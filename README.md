# Pump.fun Replay Sprint — Paper Bot

[![make.fun](https://make.fun/badge/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122.svg)](https://make.fun/)
[![rank](https://make.fun/badge/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122/pujigustiana/pump-fun-replay-jackxyz72/rank.svg)](https://make.fun/)
[![reward](https://make.fun/badge/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122/pujigustiana/pump-fun-replay-jackxyz72/reward.svg)](https://make.fun/)

Paper trading bot for the [Pump.fun Replay Sprint](https://make.fun/) competition: it trades 10 SOL of paper money on a 24h replay of pump.fun bonding-curve activity. No real money is ever traded.

## Strategy

Selective early-burst scalper. Most pump.fun coins never spike; the ones that do show one early signature: several distinct buyers pile in fast and the curve's real SOL reserve grows large while the coin is still young. This bot buys few coins — only the ones that prove themselves early (at least 5 distinct non-creator buyers and at least 20 SOL of real reserves within 500 slots of creation) — and buys them tiny (0.01 SOL).

Exits are the other half: take profit at +20%, a tight trailing stop on the real SOL reserve (the reserve peaks exactly when the price does and drains first when the dump starts), or a 300-slot time limit. Every exit sells in chunks sized to the live reserve, because the replay engine rejects a sell whose proceeds exceed the curve's current real SOL reserve.

## Layout

- `main.ts` — entrypoint: wires the strategy to the replay protocol shim.
- `strategy.ts` — the strategy.
- `vendor/makefun-bot.ts` — the kit's replay protocol shim (vendored).

## License

MIT. See [LICENSE](LICENSE).
