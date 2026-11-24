// Pump.fun Replay Sprint — paper bot: early-burst scalper.
//
// What the dev data actually shows (checked on the kit's own dataset, not
// assumed): a pump.fun coin's whole life is usually minutes long, its
// price peaks a few hundred slots after creation, and by the end of the
// window it has typically fallen back to about a quarter of that peak.
// Momentum confirmation is poison here — by the time a rise is "confirmed"
// the burst is over, and stop losses and take profits only lock in the
// mean reversion. The edge, such as it is, is breadth: buy almost every
// coin that shows real money (real SOL reserves of 2 or more), buy it
// small, buy it immediately, and be out again about 500 slots later,
// win or lose. Most round trips lose a little; the few that catch a burst
// pay for all of them. So this bot is a lottery-ticket machine, not a
// stock picker: tiny stakes (0.05 SOL), at most 5 coins at once, no
// position ever big enough to hurt, and no exit rule except time.
//
// Mechanics: a coin becomes eligible on any trade once its real SOL
// reserve reaches MIN_RQ_SOL. The first buy takes up to STAKE_SOL, capped
// at 1% of the real reserve (the harness cap), and later trades of the
// same coin top the position up to the same stake while the reserve cap
// allows — winners keep growing their reserve, so they earn the top-ups.
// After HOLD_SLOTS the whole position is sold. A coin may be entered up
// to MAX_ENTRIES times, with a short cooldown after each exit, so a coin
// that keeps bursting can be ridden more than once but a dead coin cannot
// be churned forever. The first entry must come within FIRST_AGE slots
// of creation; re-entries have no age limit.
//
// Risk budget: at most 5 open positions, at most 0.05 SOL of cost in any
// one coin, at most 0.25 SOL of cost open at once. No stop loss, no take
// profit, no trailing stop: the offline sweep found every one of them
// reduced the dev score, and a 500-slot hold already bounds how wrong
// any single ticket can go. Fees are about 1.25% each way plus a flat
// transaction fee, which is why stakes stay small and buys never chase
// a price that has already run away in the latency window.
//
// Deterministic by construction: no clock, no randomness, no I/O, and all
// iteration follows insertion-ordered maps fed by the batch stream, so two
// runs over the same window produce byte-identical action logs.

import {
  runBot,
  type Batch,
  type BotState,
  type OrderInput,
  type Pubkey,
} from './vendor/makefun-bot.ts';

const LAMPORTS = 1e9;
const CURVE_FEE = 0.0125;
const SOL_PER_LAMPORT = 1 / LAMPORTS;

// Real SOL reserve a curve must hold before it may be bought.
const MIN_RQ_SOL = 2;
// SOL to put into one position in total (first buy plus top-ups).
const STAKE_SOL = 0.05;
// Smallest single buy worth paying the transaction fee for.
const MIN_BUY_SOL = 0.005;
// Slots a position is held before it is sold, win or lose.
const HOLD_SLOTS = 500;
// Slots to wait after an exit before the same coin may be bought again.
const COOLDOWN = 100;
// Most entries per coin over the run.
const MAX_ENTRIES = 6;
// First entry only within this many slots of the CreateEvent.
const FIRST_AGE = 20_000;
// Slippage allowed on buys and sells, in bps (wide open: the 2-slot
// latency is short against how sparsely most coins trade).
const BUY_SLIP_BPS = 10_000;
const SELL_SLIP_BPS = 10_000;

interface Tape {
  created: number | null;
  entries: number;
  lastExit: number;
}

interface Held {
  opened: number;
  invested: number;
  exiting: boolean;
  filled: boolean;
}

const tapes = new Map<Pubkey, Tape>();
const held = new Map<Pubkey, Held>();

function tapeOf(mint: Pubkey): Tape {
  let tape = tapes.get(mint);
  if (tape === undefined) {
    tape = { created: null, entries: 0, lastExit: -1e12 };
    tapes.set(mint, tape);
  }
  return tape;
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
    const traded = new Set<Pubkey>();
    for (const event of batch.events) {
      if (event.event === 'CreateEvent') {
        tapeOf(event.mint).created = event.slot;
      } else if (event.event === 'TradeEvent') {
        traded.add(event.mint);
      }
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

    // Exits first: any position held HOLD_SLOTS or more is sold whole.
    // A position whose curve completed is closed by the replay itself.
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
      if (now - hold.opened < HOLD_SLOTS) continue;
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

    // Entries and top-ups.
    let free = Number(state.freeCashLamports()) * SOL_PER_LAMPORT;
    let slotsLeft =
      state.hello.caps.max_open_positions - state.committedPositionCount();
    for (const mint of traded) {
      if (orders.length >= cap) break;
      const curve = state.curves.get(mint);
      if (curve === undefined || curve.complete) continue;
      if (pendingByMint.has(mint)) continue;
      const tape = tapes.get(mint)!;
      const hold = held.get(mint);
      const rqSol = Number(curve.real_quote_reserves) * SOL_PER_LAMPORT;
      if (rqSol < MIN_RQ_SOL) continue;
      let want: number;
      if (hold === undefined) {
        if (slotsLeft <= 0) continue;
        if (tape.entries >= MAX_ENTRIES) continue;
        if (now - tape.lastExit < COOLDOWN) continue;
        if (tape.entries === 0) {
          if (tape.created === null || now - tape.created > FIRST_AGE) continue;
        }
        want = STAKE_SOL;
      } else {
        // Top up a live position toward the stake while the reserve cap
        // allows: a coin whose real SOL keeps growing is earning size.
        if (hold.exiting || !hold.filled) continue;
        if (hold.invested >= STAKE_SOL - MIN_BUY_SOL) continue;
        want = STAKE_SOL - hold.invested;
      }
      const capSol =
        Math.floor(Number(state.maxBuyLamports(mint))) * SOL_PER_LAMPORT;
      let amount = Math.min(capSol, want);
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
      if (hold === undefined) {
        held.set(mint, {
          opened: now,
          invested: amount,
          exiting: false,
          filled: false,
        });
        tape.entries += 1;
        slotsLeft -= 1;
      } else {
        hold.invested += amount;
      }
    }
    return orders;
  },
});
