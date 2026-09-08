# ErasureNone VPatch Promela model

`vpatch_erasure_none.pml` models one ErasureNone `VPATCH_DIFF` after
`START/FOUND` has found the only part. It focuses on whether the client request,
the BSQueue item, and SkeletonFront request-window accounting reach a consistent
terminal state when completion, connection failure, `PDiskError`, and restart
race.

## Scope

The model keeps DSProxy, ClientBSQueue, remote SkeletonFront, the VDisk
application, transport, lifecycle, connection failure, and watchdog as separate
processes.

SkeletonFront admits the request before starting VDisk work. A local
`TEvVDiskRequestCompleted` closes the private request window before the result
can enter transport. Local completion is reliable while the same Front
incarnation is running; it is not independently dropped.

Completion races with one request-level lifecycle outcome in the Front mailbox:

- a stable Front accounts completion and publishes the result;
- `PDiskError` before completion retires the request while the terminal Front
  retains its outstanding accounting and ignores late completion;
- restart before completion destroys the active request, creates a clean Front
  incarnation, and makes completion addressed to the old actor irrelevant;
- completion before either failure closes accounting first, after which the
  published result and connection reset race at BSQueue.

The epoch-0 `request_*_history` counters retain the old request's fate after
restart. `current_front_has_request` separately tracks the active incarnation:
completion clears it, `PDiskError` preserves an uncompleted request, and restart
clears it without rewriting history.

Production BSQueue is long-lived, but the model projects it onto one queue item
and stops observing it after that item becomes terminal. Buffered late results
represent replies rejected in production through `InterconnectSession`,
`IsReady()`, or `Queue.Expecting()`. Reconnect and a second request are outside
this one-item model.

The model also assumes terminating local work and fallback, weak process
fairness, and an eventual full quiet watchdog interval. A Front
`DropConnection` may be lost; the watchdog still resets the connection and
drains the item. Payload bytes, retries, unrelated queue traffic, fallback
internals, and the later `VPatchDyingRequest`/`VPatchDyingConfirm` handshake are
outside the model.

## Verification

Run from this directory. Assertions and invalid end states are checked without
an LTL claim:

```bash
spin -a vpatch_erasure_none.pml
cc -O2 -DMEMLIM=1024 -DSAFETY -DNOCLAIM -o pan_safety pan.c
./pan_safety -m100000
```

Spin 6.5.2 selects a named inline claim at runtime. Safety claims run without
fairness; liveness claims use weak process fairness and `NFAIR=16`:

```bash
spin -a vpatch_erasure_none.pml
cc -O2 -DMEMLIM=1024 -DNFAIR=16 -o pan pan.c

./pan -a -N safe_current_front_accounting_matches_lifecycle -m100000
./pan -a -f -N live_eventual_client_item_settlement -m100000
```

Run every `safe_*` and `live_*` claim in the model in the same respective mode.

## Verification results

Measured with Spin 6.5.2. Every row is an exhaustive search with `errors: 0`;
liveness uses weak process fairness.

| Check | Result | Stored states | Time |
| --- | --- | ---: | ---: |
| assertions/deadlocks | holds | 1,088,653 | 0.67 s |
| each safety LTL claim | holds | 1,088,653 | <=0.87 s |
| client eventually replies | holds | 626,076 (~7.26M visited) | 5.14 s |
| accepted DIFF resolves or retires | holds | 1,327,450 (~3.92M visited) | 3.36 s |
| stable Front accounts DIFF | holds | 1,167,284 (~1.50M visited) | 1.26 s |
| local completion is settled | holds | 1,455,503 (~3.51M visited) | 2.98 s |
| old completion becomes irrelevant | holds | 1,308,659 (~2.53M visited) | 2.13 s |
| BSQueue item terminates | holds | 1,438,925 (~5.30M visited) | 4.08 s |
| retired DIFF terminalizes its item | holds | 1,168,575 (~1.52M visited) | 1.23 s |
| client/item settlement and request fate | holds | 1,010,390 (~12.3M visited) | 9.35 s |

The exhaustive search distinguishes a stable Front from a retired request. If
lifecycle failure wins, the request remains historically
`accepted=1, completed=0, outstanding=1`; `PDiskError` keeps it in the terminal
Front, while restart removes it from the new active Front. If completion wins,
history becomes `completed=1, outstanding=0` before the result/reset race.
