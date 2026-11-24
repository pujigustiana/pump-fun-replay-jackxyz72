// Pump.fun Replay Sprint — paper bot entrypoint.
//
// The strategy lives in strategy.ts (createBot); this file only wires it
// to the replay protocol shim in vendor/makefun-bot.ts, which owns the
// stdin/stdout wire: one reply per batch, echoing the batch's seq.

import { runBot } from './vendor/makefun-bot.ts';
import { createBot } from './strategy.ts';

await runBot(createBot());
