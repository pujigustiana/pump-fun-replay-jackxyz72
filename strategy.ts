// Selective early-burst scalper for the pump.fun bonding-curve replay.
//
// What the dev data and the house engine actually show (checked on the
// kit's own dataset with the house engine port, not assumed):
//   - A coin's whole life is usually minutes long; its price peaks a few
//     hundred slots after creation and then falls back to about a quarter
//     of that peak. Momentum confirmation is poison: by the time a rise
//     is "confirmed" the burst is over.
//   - A sell is REJECTED when its gross proceeds exceed the curve's
//     current real SOL reserve, and a closing position is valued at most
//     at that reserve. A dying coin cannot absorb a whole position, so
//     exits must be sold in chunks sized to the live reserve, and a
//     position must start exiting while the reserve is still there.
//   - The buy size cap is measured against the reserve INCLUDING the
//     bot's own paper flow, while the shim reports the recorded reserve,
//     so buys are sized with a margin under the reported cap.
//   - Most coins never spike at all. The ones that do share one early
//     signature: several DISTINCT buyers pile in fast and the curve's
//     real SOL reserve grows large while the coin is still young. Every
//     broad version of this bot lost money; every stricter version of
//     the entry filter lost less. So this bot buys FEW coins, only the
//     ones that prove themselves early, and buys them TINY (0.01 SOL):
//     at that size the curve fees and the flat transaction fee are the
//     whole cost of a ticket, and a win of one ticket pays for many.
//   - The exit is the other half of the edge: sell on the first of a
//     take profit, a tight trailing stop on the real SOL reserve (the
//     reserve peaks exactly when the price does, and it drains first
//     when the dump starts), or a short time limit.
//
// Entries: within the first FILTER_AGE slots after creation, a coin must
// show at least MIN_BUYERS distinct non-creator buyers and a real SOL
// reserve of at least MIN_RQ_SOL. One entry per coin, ever. The first
// buy takes up to STAKE_SOL, capped at a margin under the 1%-of-reserve
// size cap.
//
// Exits: the first of a take profit, a trailing stop on the reserve peak,
// or the time limit. Every exit sells in chunks whose gross proceeds fit
// under a share of the live reserve; a chunk too small to be worth its
// transaction fee waits for the reserve to recover.
//
// Risk budget: at most 5 open positions, at most 0.01 SOL of cost in any
// one coin, at most 0.05 SOL of cost open at once. The guardrail (a
// 0.01 SOL round trip on at least 20 coins, drawdown under 50%) is met
// with a wide margin on the dev window: over a hundred qualifying coins.
//
// Deterministic by construction: no clock, no randomness, no I/O, and all
// iteration follows insertion-ordered maps fed by the batch stream, so two
// runs over the same window produce byte-identical action logs.

import type { Batch, Bot, BotState, OrderInput, Pubkey } from './vendor/makefun-bot.ts';

const LAMPORTS = 1e9;
const CURVE_FEE = 0.0125;
const SOL_PER_LAMPORT = 1 / LAMPORTS;

export interface Params {
  /** SOL to put into one position (a single buy; no top-ups). */
  stakeSol: number;
  /** Smallest single buy worth paying the transaction fee for. */
  minBuySol: number;
  /** Distinct non-creator buyers a coin must show before it may be bought. */
  minBuyers: number;
  /** Real SOL reserve a coin must hold before it may be bought. */
  minRqSol: number;
  /** Entries only within this many slots of the CreateEvent. */
  filterAge: number;
  /** Slots a position is held before the time exit fires. */
  holdSlots: number;
  /** Exit once the real reserve falls this share below its peak since entry. */
  rqTrail: number;
  /** Exit once the position is worth this much over its cost. */
  takeProfit: number;
  /** Buys are sized to at most this share of the reported size cap. */
  capMargin: number;
  /** One sell chunk's gross proceeds are at most this share of the live real reserve. */
  exitChunkRq: number;
  /** Smallest net chunk worth selling (below this the exit waits). */
  minChunkSol: number;
}

export const DEFAULT_PARAMS: Params = {
  stakeSol: 0.01,
  minBuySol: 0.005,
  minBuyers: 5,
  minRqSol: 20,
  filterAge: 500,
  holdSlots: 300,
  rqTrail: 0.005,
  takeProfit: 0.2,
  capMargin: 0.85,
  exitChunkRq: 0.7,
  minChunkSol: 0.002,
};

interface Tape {
  created: number | null;
  creator: string | null;
  buyers: Set<string>;
  entered: boolean;
}

interface Held {
  opened: number;
  peakRq: number;
  exiting: boolean;
  filled: boolean;
}

export function createBot(p: Partial<Params> = {}): Bot {
  const q: Params = { ...DEFAULT_PARAMS, ...p };
  const tapes = new Map<Pubkey, Tape>();
  const held = new Map<Pubkey, Held>();

  const tapeOf = (mint: Pubkey): Tape => {
    let tape = tapes.get(mint);
    if (tape === undefined) {
      tape = { created: null, creator: null, buyers: new Set(), entered: false };
      tapes.set(mint, tape);
    }
    return tape;
  };

  return {
    onBatch(state: BotState, batch: Batch): OrderInput[] {
      const now = batch.slot;
      const traded = new Set<Pubkey>();
      for (const event of batch.events) {
        if (event.event === 'CreateEvent') {
          const tape = tapeOf(event.mint);
          tape.created = event.slot;
          tape.creator = event.creator;
        } else if (event.event === 'TradeEvent') {
          traded.add(event.mint);
          const tape = tapeOf(event.mint);
          if (event.is_buy && tape.creator !== null && event.user !== tape.creator) {
            tape.buyers.add(event.user);
          }
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

      // Reserves for a mint: the mark (offset-adjusted, what the engine
      // checks exits against) when there is one, else the curve's own.
      const reservesOf = (mint: Pubkey): { vq: number; vt: number; rq: number } | null => {
        const mark = marks.get(mint);
        if (mark !== undefined) {
          return {
            vq: Number(mark.virtual_quote_reserves),
            vt: Number(mark.virtual_token_reserves),
            rq: Number(mark.real_quote_reserves) * SOL_PER_LAMPORT,
          };
        }
        const curve = state.curves.get(mint);
        if (curve === undefined) return null;
        return {
          vq: Number(curve.virtual_quote_reserves),
          vt: Number(curve.virtual_token_reserves),
          rq: Number(curve.real_quote_reserves) * SOL_PER_LAMPORT,
        };
      };

      // Gross SOL a sale of `tokens` would move (before curve fees).
      const grossOf = (tokens: number, vq: number, vt: number): number =>
        ((tokens * vq) / (vt + tokens)) * SOL_PER_LAMPORT;

      // Exits first: re-price every held position and exit on the first
      // trigger, in chunks the curve's live reserve can absorb.
      for (const [mint, hold] of held) {
        const pos = state.positions.get(mint);
        const curve = state.curves.get(mint);
        if (pos === undefined) {
          if (!pendingByMint.has(mint)) {
            held.delete(mint);
          }
          continue;
        }
        hold.filled = true;
        if (curve === undefined || curve.complete) continue;
        const res = reservesOf(mint);
        if (res === null) continue;
        const tokens = Number(pos.token_amount);
        const cost = Number(pos.cost_lamports) * SOL_PER_LAMPORT;
        const value =
          grossOf(tokens, res.vq, res.vt) * (1 - CURVE_FEE) - txSol;
        if (res.rq > hold.peakRq) hold.peakRq = res.rq;
        let exit = hold.exiting;
        if (!exit && now - hold.opened >= q.holdSlots) exit = true;
        if (!exit && value >= cost * (1 + q.takeProfit)) exit = true;
        if (
          !exit &&
          hold.peakRq > 0 &&
          res.rq < hold.peakRq * (1 - q.rqTrail)
        ) {
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
        // Size one chunk: the whole position when the curve can absorb
        // it, else the tokens whose gross proceeds fit under the share
        // of the live reserve. A chunk too small to be worth its tx fee
        // waits for the reserve to recover.
        const wholeGross = grossOf(tokens, res.vq, res.vt);
        let sellTokens = tokens;
        if (wholeGross > q.exitChunkRq * res.rq) {
          const target = q.exitChunkRq * res.rq * LAMPORTS;
          sellTokens = Math.floor((target * res.vt) / (res.vq - target));
        }
        if (sellTokens <= 0) continue;
        if (sellTokens > tokens) sellTokens = tokens;
        const net =
          grossOf(sellTokens, res.vq, res.vt) * (1 - CURVE_FEE) - txSol;
        if (net < q.minChunkSol && sellTokens < tokens) continue;
        orders.push({
          side: 'sell',
          mint,
          token_amount: BigInt(sellTokens),
          max_slippage_bps: 10_000,
        });
      }

      // Entries: one per coin, only for coins that proved themselves
      // early (distinct buyers plus a large real reserve, while young).
      let free = Number(state.freeCashLamports()) * SOL_PER_LAMPORT;
      let slotsLeft =
        state.hello.caps.max_open_positions - state.committedPositionCount();
      for (const mint of traded) {
        if (orders.length >= cap) break;
        if (slotsLeft <= 0) break;
        const curve = state.curves.get(mint);
        if (curve === undefined || curve.complete) continue;
        if (pendingByMint.has(mint)) continue;
        const tape = tapes.get(mint)!;
        if (tape.entered || held.has(mint)) continue;
        if (tape.created === null || now - tape.created > q.filterAge) continue;
        if (tape.buyers.size < q.minBuyers) continue;
        const rqSol = Number(curve.real_quote_reserves) * SOL_PER_LAMPORT;
        if (rqSol < q.minRqSol) continue;
        const capSol =
          Math.floor(Number(state.maxBuyLamports(mint))) *
          SOL_PER_LAMPORT *
          q.capMargin;
        let amount = Math.min(capSol, q.stakeSol);
        if (amount > free - txSol) amount = free - txSol;
        if (amount < q.minBuySol) continue;
        const lamports = BigInt(Math.floor(amount * LAMPORTS));
        orders.push({
          side: 'buy',
          mint,
          quote_lamports: lamports,
          max_slippage_bps: 10_000,
        });
        tape.entered = true;
        held.set(mint, {
          opened: now,
          peakRq: rqSol,
          exiting: false,
          filled: false,
        });
        free -= amount + txSol;
        slotsLeft -= 1;
      }
      return orders;
    },
  };
}
