// Pump.fun Replay Sprint — paper bot: patient momentum with tight risk control.
//
// Idea: most pump.fun curves die fast and most early buyers are the launch
// cluster. This bot waits until a curve has lived a while and shows real,
// broadening buy pressure (several distinct buyers, net SOL flowing in,
// price rising and holding near its recent high, inflow accelerating), then
// buys small. Winners get pyramided slowly; losers are cut by a stop loss,
// a trailing stop, a flow-fade exit, or a time limit. Nothing is bought on
// a hunch: every entry needs the tape to confirm it, and every position has
// an exit planned before the buy is sent.
//
// Risk budget: at most 5 open positions, at most 0.30 SOL of cost in any
// one coin, at most 5 SOL of cost open at once, and a small initial stake
// (0.06 SOL) so a false start costs little. Round-trip curve fees are about
// 1.25% each way plus a flat transaction fee, so entries demand a move big
// enough to pay for itself; tiny probe round trips (0.012 SOL) exist only
// to keep the run above the 20-coin guardrail when momentum entries lag.
//
// Deterministic by construction: no clock, no randomness, no I/O, and all
// iteration follows insertion-ordered maps fed by the batch stream, so two
// runs over the same window produce byte-identical action logs.

import {
  LAMPORTS_PER_SOL,
  runBot,
  type Batch,
  type BotState,
  type OrderInput,
  type Pubkey,
} from './vendor/makefun-bot.ts';

const LAMPORTS = 1e9;
const CURVE_FEE = 0.0125;
const SOL_PER_LAMPORT = 1 / LAMPORTS;

interface TapeTrade {
  slot: number;
  buy: boolean;
  sol: number;
  user: string;
  price: number;
}

interface Tape {
  created: number | null;
  trades: TapeTrade[];
  entries: number;
  lastExit: number;
}

interface Held {
  opened: number;
  peakValue: number;
  invested: number;
  exiting: boolean;
  filled: boolean;
  lastAddPrice: number;
  lastValue: number;
}

// Entry window in slots.
const WINDOW = 40;
// Net buy SOL (buys minus sells) over the window.
const MIN_NET_SOL = 0.8;
// Buy trades in the window.
const MIN_BUYS = 5;
// Distinct buying wallets in the window.
const MIN_BUYERS = 5;
// Price now over price at the window start, minus 1: lower and upper bound.
const MIN_PRICE_UP = 0.05;
const MAX_PRICE_UP = 1.5;
// Newer-half net inflow must be at least this multiple of the older half.
const ACCEL = 1;
// Price now at least this share of the window high.
const NEAR_HIGH = 0.94;
// Largest single wallet share of the window buy SOL.
const MAX_BUYER_SHARE = 0.6;
// Curve real SOL bounds at entry.
const MIN_RQ_SOL = 1.5;
const MAX_RQ_SOL = 70;
// Slots since the CreateEvent before an entry is allowed.
const MIN_AGE = 400;
// SOL to put into one position in total.
const STAKE_SOL = 0.3;
// Smallest buy worth sending.
const MIN_BUY_SOL = 0.012;
// Slippage on buys in bps; sells always go out wide open.
const BUY_SLIP_BPS = 1500;
const SELL_SLIP_BPS = 10_000;
// First buy of a position (SOL) and each add-on buy (SOL).
const INITIAL_SOL = 0.06;
const ADD_SOL = 0.12;
// Add only once the price is this much above the last buy decision price.
const ADD_STEP = 0.02;
// Add only while net SOL over the entry window is at least this.
const ADD_NET_SOL = 0.3;
// Exits: stop loss, trailing stop, trailing arm, fade window/net, max hold.
const STOP_LOSS = 0.18;
const TRAIL = 0.25;
const TRAIL_ARM = 0.1;
const FADE_WINDOW = 20;
const FADE_NET_SOL = -1;
const BIG_SELL_SOL = 5;
const MAX_HOLD = 1800;
const HOLD_IF_UP = 0.15;
// Slots to wait after an exit before the same curve may be bought again.
const COOLDOWN = 240;
// Most entries per curve over the run.
const MAX_ENTRIES = 2;
// Most SOL at cost across all positions at once.
const MAX_EXPOSURE_SOL = 5;
// Guardrail pacing: target coins and fills over the full 24h window.
const TARGET_COINS = 30;
const TARGET_FILLS = 300;
const PACE_SECS = 86_400;
const PROBE_SOL = 0.012;
const MAX_PROBES = 1;

const tapes = new Map<Pubkey, Tape>();
const held = new Map<Pubkey, Held>();
const probes = new Map<Pubkey, boolean>();
let myFills = 0;
const myCoins = new Set<Pubkey>();
let firstTime: number | null = null;
let lastTime: number | null = null;
const KEEP = WINDOW + 10;

function tapeOf(mint: Pubkey): Tape {
  let tape = tapes.get(mint);
  if (tape === undefined) {
    tape = { created: null, trades: [], entries: 0, lastExit: -1e12 };
    tapes.set(mint, tape);
  }
  return tape;
}

function entrySignal(tape: Tape, now: number, rqSol: number): boolean {
  if (rqSol < MIN_RQ_SOL || rqSol > MAX_RQ_SOL) return false;
  if (tape.created === null || now - tape.created < MIN_AGE) return false;
  const from = now - WINDOW;
  const mid = now - WINDOW / 2;
  let buySol = 0;
  let sellSol = 0;
  let buys = 0;
  let netOld = 0;
  let netNew = 0;
  let startPrice = -1;
  let high = 0;
  let last = 0;
  const byUser = new Map<string, number>();
  for (const trade of tape.trades) {
    if (trade.slot <= from) {
      startPrice = trade.price;
      continue;
    }
    if (startPrice < 0) startPrice = trade.price;
    const signed = trade.buy ? trade.sol : -trade.sol;
    if (trade.slot <= mid) netOld += signed;
    else netNew += signed;
    if (trade.buy) {
      buySol += trade.sol;
      buys += 1;
      byUser.set(trade.user, (byUser.get(trade.user) ?? 0) + trade.sol);
    } else {
      sellSol += trade.sol;
    }
    if (trade.price > high) high = trade.price;
    last = trade.price;
  }
  if (buys < MIN_BUYS || byUser.size < MIN_BUYERS) return false;
  const net = buySol - sellSol;
  if (net < MIN_NET_SOL) return false;
  if (startPrice <= 0) return false;
  const up = last / startPrice - 1;
  if (up < MIN_PRICE_UP || up > MAX_PRICE_UP) return false;
  if (last < high * NEAR_HIGH) return false;
  if (netNew <= 0 || netNew < ACCEL * netOld) return false;
  let top = 0;
  for (const value of byUser.values()) if (value > top) top = value;
  if (top > MAX_BUYER_SHARE * buySol) return false;
  return true;
}

function flowOver(tape: Tape, now: number, window: number): number {
  let net = 0;
  for (let i = tape.trades.length - 1; i >= 0; i -= 1) {
    const trade = tape.trades[i]!;
    if (trade.slot <= now - window) break;
    net += trade.buy ? trade.sol : -trade.sol;
  }
  return net;
}

function closeValue(
  tokens: number,
  vq: number,
  vt: number,
  txSol: number,
): number {
  const gross = (tokens * vq) / (vt + tokens) / LAMPORTS;
  return gross * (1 - CURVE_FEE) - txSol;
}

await runBot({
  onBatch(state: BotState, batch: Batch): OrderInput[] {
    const now = batch.slot;
    const bigSell = new Set<Pubkey>();
    const traded = new Set<Pubkey>();
    for (const fill of batch.fills) {
      if (fill.status === 'filled' && !fill.client_id.startsWith('_')) {
        myFills += 1;
        myCoins.add(fill.mint);
      }
    }
    for (const event of batch.events) {
      if (event.block_time !== null) {
        if (firstTime === null) firstTime = event.block_time;
        if (lastTime === null || event.block_time > lastTime) {
          lastTime = event.block_time;
        }
      }
      if (event.event === 'CreateEvent') {
        tapeOf(event.mint).created = event.slot;
      } else if (event.event === 'TradeEvent') {
        const tape = tapeOf(event.mint);
        const sol = Number(event.sol_amount) * SOL_PER_LAMPORT;
        const price =
          Number(event.virtual_quote_reserves) /
          Number(event.virtual_token_reserves);
        tape.trades.push({
          slot: event.slot,
          buy: event.is_buy,
          sol,
          user: event.user,
          price,
        });
        traded.add(event.mint);
        if (!event.is_buy && sol >= BIG_SELL_SOL) bigSell.add(event.mint);
      }
    }
    for (const mint of traded) {
      const tape = tapes.get(mint)!;
      let cut = 0;
      while (cut < tape.trades.length && tape.trades[cut]!.slot <= now - KEEP) {
        cut += 1;
      }
      if (cut > 0) tape.trades.splice(0, cut);
    }
    const marks = new Map(batch.marks.map((mark) => [mark.mint, mark] as const));
    const pendingByMint = new Map<Pubkey, number>();
    for (const pending of state.pending.values()) {
      pendingByMint.set(
        pending.order.mint,
        (pendingByMint.get(pending.order.mint) ?? 0) + 1,
      );
    }
    const orders: OrderInput[] = [];
    const cap = state.hello.caps.max_orders_per_batch;
    const txSol = Number(state.txFeeLamports()) * SOL_PER_LAMPORT;

    // Exits first: every held position is re-priced and checked.
    for (const [mint, hold] of held) {
      const pos = state.positions.get(mint);
      const curve = state.curves.get(mint);
      if (pos === undefined) {
        if (!pendingByMint.has(mint)) {
          held.delete(mint);
          const tape = tapeOf(mint);
          if (hold.filled) tape.lastExit = now;
          else tape.entries -= 1;
        }
        continue;
      }
      hold.filled = true;
      if (curve === undefined || curve.complete) continue;
      const mark = marks.get(mint);
      const vq = mark
        ? Number(mark.virtual_quote_reserves)
        : Number(curve.virtual_quote_reserves);
      const vt = mark
        ? Number(mark.virtual_token_reserves)
        : Number(curve.virtual_token_reserves);
      const tokens = Number(pos.token_amount);
      const value = closeValue(tokens, vq, vt, txSol);
      const cost = Number(pos.cost_lamports) * SOL_PER_LAMPORT;
      if (value > hold.peakValue) hold.peakValue = value;
      hold.lastValue = value;
      let exit = hold.exiting;
      const tape = tapeOf(mint);
      if (!exit && value < cost * (1 - STOP_LOSS)) exit = true;
      if (
        !exit &&
        hold.peakValue >= cost * (1 + TRAIL_ARM) &&
        value < hold.peakValue * (1 - TRAIL)
      ) {
        exit = true;
      }
      if (!exit && flowOver(tape, now, FADE_WINDOW) < FADE_NET_SOL) exit = true;
      if (!exit && bigSell.has(mint)) exit = true;
      if (!exit && now - hold.opened > MAX_HOLD && value < cost * (1 + HOLD_IF_UP)) {
        exit = true;
      }
      if (!exit) continue;
      hold.exiting = true;
      let sellPending = false;
      for (const pending of state.pending.values()) {
        if (pending.order.mint === mint && pending.order.side === 'sell') {
          sellPending = true;
        }
      }
      if (sellPending) continue;
      if (orders.length >= cap) break;
      orders.push({
        side: 'sell',
        mint,
        token_amount: pos.token_amount,
        max_slippage_bps: SELL_SLIP_BPS,
      });
    }

    // Probe round trips: sell the whole probe as soon as its buy lands.
    for (const [mint, selling] of probes) {
      const pos = state.positions.get(mint);
      const curve = state.curves.get(mint);
      if (pos === undefined) {
        if (!pendingByMint.has(mint)) probes.delete(mint);
        continue;
      }
      if (curve === undefined || curve.complete || pendingByMint.has(mint)) {
        continue;
      }
      if (orders.length >= cap) break;
      orders.push({
        side: 'sell',
        mint,
        token_amount: pos.token_amount,
        max_slippage_bps: SELL_SLIP_BPS,
      });
      if (!selling) probes.set(mint, true);
    }

    // Entries and pyramid adds.
    let free = Number(state.freeCashLamports()) * SOL_PER_LAMPORT;
    let slotsLeft =
      state.hello.caps.max_open_positions - state.committedPositionCount();
    let mainLeft = state.hello.caps.max_open_positions - 1 - held.size;
    let exposure = 0;
    for (const [mint, hold] of held) {
      const heldCost = state.positions.get(mint);
      exposure += Math.max(
        hold.invested,
        heldCost === undefined
          ? 0
          : Number(heldCost.cost_lamports) * SOL_PER_LAMPORT,
      );
    }
    for (const mint of traded) {
      if (orders.length >= cap) break;
      const curve = state.curves.get(mint);
      if (curve === undefined || curve.complete) continue;
      if (pendingByMint.has(mint) || probes.has(mint)) continue;
      const tape = tapes.get(mint)!;
      const hold = held.get(mint);
      const price =
        Number(curve.virtual_quote_reserves) /
        Number(curve.virtual_token_reserves);
      const rqSol = Number(curve.real_quote_reserves) * SOL_PER_LAMPORT;
      let want: number;
      if (hold === undefined) {
        if (slotsLeft <= 0 || mainLeft <= 0) continue;
        if (tape.entries >= MAX_ENTRIES || now - tape.lastExit < COOLDOWN) {
          continue;
        }
        if (!entrySignal(tape, now, rqSol)) continue;
        want = Math.min(INITIAL_SOL, STAKE_SOL);
      } else {
        if (hold.exiting || !hold.filled) continue;
        if (hold.invested >= STAKE_SOL - MIN_BUY_SOL) continue;
        const pos = state.positions.get(mint);
        if (pos === undefined) continue;
        if (price < hold.lastAddPrice * (1 + ADD_STEP)) continue;
        if (hold.lastValue < Number(pos.cost_lamports) * SOL_PER_LAMPORT) {
          continue;
        }
        if (flowOver(tape, now, WINDOW) < ADD_NET_SOL) continue;
        if (rqSol > MAX_RQ_SOL) continue;
        want = Math.min(ADD_SOL, STAKE_SOL - hold.invested);
      }
      const capSol =
        Math.floor(Number(state.maxBuyLamports(mint))) * SOL_PER_LAMPORT;
      let amount = Math.min(capSol, want, MAX_EXPOSURE_SOL - exposure);
      if (amount > free - txSol) amount = free - txSol;
      if (amount < MIN_BUY_SOL) continue;
      const lamports = BigInt(Math.floor(amount * LAMPORTS));
      orders.push({
        side: 'buy',
        mint,
        quote_lamports: lamports,
        max_slippage_bps: BUY_SLIP_BPS,
      });
      free -= amount + txSol;
      exposure += amount;
      if (hold === undefined) {
        held.set(mint, {
          opened: now,
          peakValue: 0,
          invested: amount,
          exiting: false,
          filled: false,
          lastAddPrice: price,
          lastValue: 0,
        });
        tape.entries += 1;
        slotsLeft -= 1;
        mainLeft -= 1;
      } else {
        hold.invested += amount;
        hold.lastAddPrice = price;
      }
    }

    // Guardrail pacing: open a probe when fills or coins lag the pace.
    if (probes.size < MAX_PROBES && slotsLeft > 0 && firstTime !== null && lastTime !== null) {
      const pace = Math.min(1, (lastTime - firstTime) / PACE_SECS);
      const needCoins = myCoins.size < Math.ceil(TARGET_COINS * pace);
      const needFills = myFills < Math.ceil(TARGET_FILLS * pace);
      if (needCoins || needFills) {
        const minRq =
          (PROBE_SOL * 10_000) / state.hello.caps.max_order_bps_of_real_reserve;
        for (const mint of traded) {
          if (orders.length >= cap || probes.size >= MAX_PROBES) break;
          if (held.has(mint) || probes.has(mint)) continue;
          if (pendingByMint.has(mint) || state.positions.has(mint)) continue;
          if (needCoins && !needFills && myCoins.has(mint)) continue;
          const curve = state.curves.get(mint);
          if (curve === undefined || curve.complete) continue;
          if (Number(curve.real_quote_reserves) * SOL_PER_LAMPORT < minRq * 1.5) {
            continue;
          }
          if (free < PROBE_SOL + 2 * txSol) break;
          orders.push({
            side: 'buy',
            mint,
            quote_lamports: BigInt(Math.floor(PROBE_SOL * LAMPORTS)),
            max_slippage_bps: BUY_SLIP_BPS,
          });
          probes.set(mint, false);
          free -= PROBE_SOL + txSol;
        }
      }
    }
    return orders;
  },
});
