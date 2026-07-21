# TEvSlay Recovery and YardInit Races

This directory contains two standalone Promela models for one stable `VSlotId`:

| Version | Model | Saved result |
| --- | --- | --- |
| `v0` — old NodeWarden | `slay_recovery_v0.pml` | `slay_recovery_v0.trace` — a `TEvSlay` lost during PDisk restart |
| `v1` — action-aware `SlayInFlight` | `slay_recovery_v1.pml` | `slay_recovery_v1.safety.out`, `slay_recovery_v1.liveness.out` — successful core profile |
| `v1`, additional successful profiles | `slay_recovery_v1.pml` | `slay_recovery_v1.deadlock.out`, `slay_recovery_v1_self_restart.safety.out`, `slay_recovery_v1_actor_restarts.*.out` |
| `v1`, `WIPE -> DESTROY` | `slay_recovery_v1.pml` | `slay_recovery_v1_wipe.trace` — an independent race with a live `YardInit` |

## Installation

Ubuntu:

```bash
sudo apt install spin gcc
```

macOS:

```bash
brew install spin gcc
```

## Model Scope

The model contains BSC, NodeWarden, VDisk, and PDisk for one VSlot. The VDisk generation changes from `1` to `2`, while its short identity and `VSlotId` remain unchanged. Therefore, like the production code, BSC and PDisk do not use the generation as the identity of the physical slot.

BSC stores the persistent intent (`WIPE` or `DELETE`) and whether the VSlot is allocated. NodeWarden applies `ServiceSet`, stores one `SlayInFlight` operation, and handles results and retries. VDisk starts asynchronously: it allocates `InitOwnerRound` and sends `TEvYardInit` in a later step. A subsequent `Poison` cannot retract a `YardInit` that has already been sent.

The PDisk request path is split into production-like stages:

```text
actor mailbox -> TPDisk::InputQueue -> executing request
              -> completion queue -> NodeWarden mailbox
```

`TEvSlay`, `TEvYardInit`, and an authorized restart enter the same PDisk actor FIFO. `Stop()` destroys the old incarnation's mailbox, InputQueue, executing request, pending YardInit, and completion queue. A committed owner removal and a result already delivered to the NodeWarden mailbox survive `Stop()`.

A PDisk restart creates a new `TPDisk`. The syslog restores the VDisk identity, but not `TOwnerData::OwnerRound`, so the restored owner's round is zero. The model explicitly recalculates how every live Slay round compares with that owner and separately preserves a VDisk-side `InitOwnerRound` that has been allocated but not yet sent.

BSC and NodeWarden restarts are modeled with `DOWN -> STARTING -> UP` states, connection epochs, `BSC_RECONNECTED`, and `RegisterNode`. These are logical actor/session restarts, not full host restarts.

## Differences Between v0 and v1

`v0` stores only the round for a slay. After `DESTROY`, the VDisk record has already been removed from `LocalVDisks`, so the PDisk restart callback cannot find the operation to replay. The old NOTREADY retry is also modeled precisely: its timer sends a raw `TEvSlay` directly to PDisk, bypassing NodeWarden validation.

`v1` stores `{VDiskId, Action, Round}`:

- `WIPE` can be upgraded to `DESTROY` with a fresh round;
- the identity is refreshed for the same short VDisk id;
- the restart callback replays `SlayInFlight` even after the VDisk has been removed from `LocalVDisks`;
- confirmation and NOTREADY retries perform a complete new `IssueSlay`.

## OwnerRound Without Hidden Wraparound

Production uses a monotonically increasing `ui64`. The finite model uses nominal tokens: a token can be reused only after all of its references from `SlayInFlight`, timers, mailboxes, and PDisk stages have been released. This preserves round freshness without saturation or false equality between old and new requests.

The model stores an exact ordering cut for a pending `InitOwnerRound`. PDisk consequently returns statuses in production order:

1. `NOTREADY` if there is a `PendingYardInit` for the short identity.
2. `ALREADY` if the physical owner is absent.
3. `RACE` if `SlayOwnerRound <= OwnerRound` for the existing owner.
4. `OK`, removing the owner, otherwise.

A matching `RACE` leads to `assert(false)`, corresponding to the production `Y_ABORT`. A stale result only releases its token reference.

## Verified Properties

BSC must not free the VSlot while a physical owner exists:

```promela
ltl safety_freed_vslot_has_no_owner {
    [] (!bsc_vslot_allocated -> !pdisk_owner_present)
}
```

BSC must also not free the VSlot while a live `InitOwnerRound` can create an owner later:

```promela
ltl safety_freed_vslot_has_no_live_init {
    [] (!bsc_vslot_allocated -> !init_cut_active)
}
```

With a finite number of injected faults, a pending DELETE must complete:

```promela
ltl live_delete_eventually_frees_vslot {
    [] (bsc_delete_pending -> <> !bsc_vslot_allocated)
}
```

After a restart callback, the current Slay must either target the new PDisk incarnation or already have a matching result in the current NodeWarden mailbox. In `v0`, this assertion produces the lost-request counterexample.

Every persistent receive loop has an `end_*` label. A separate verification run without a never claim can therefore check real deadlock and bounded-backpressure states without `-E`, while treating normal quiescence as valid.

## Reproducing the v0 Failure

This profile restricts restart to states where Slay is already in flight. `REQUIRE_INPUT_QUEUE_LOSS` selects a trace in which the request is lost specifically from `TPDisk::InputQueue`. These macros do not add behavior to the model.

Run the commands below from `ydb/core/blobstorage/nodewarden/spin`.

```bash
SPIN_DIR=$PWD
WORKDIR=$(mktemp -d)
cd "$WORKDIR"
spin \
  -DOPERATION_MODE=1 \
  -DPDISK_RESTART_MODE=2 \
  -DREQUIRE_SLAY_IN_FLIGHT_RESTART \
  -DREQUIRE_INPUT_QUEUE_LOSS \
  -a "$SPIN_DIR/slay_recovery_v0.pml"
cc -O2 -w -DBFS -DSAFETY -o pan_safety pan.c
./pan_safety -m2000 -N safety_freed_vslot_has_no_owner -n
./pan_safety -r slay_recovery_v0.pml.trail -S
```

Expected result: `errors: 1`. In the saved trace, Slay enters InputQueue in epoch 1, restart destroys it, PDisk is recreated with epoch 2, and the old NodeWarden does not replay the deleted VDisk.

## Successful v1 Core Profile

The core profile verifies v1 against the same failure scenario. It disables only the periodic confirmation timer; retry after `NOTREADY` remains enabled.

```bash
SPIN_DIR=$PWD
WORKDIR=$(mktemp -d)
cd "$WORKDIR"
spin \
  -DENABLE_CONFIRMATION_RETRY=0 \
  -DOPERATION_MODE=1 \
  -DPDISK_RESTART_MODE=2 \
  -DREQUIRE_SLAY_IN_FLIGHT_RESTART \
  -a "$SPIN_DIR/slay_recovery_v1.pml"
```

Safety:

```bash
cc -O2 -w -DBFS -DSAFETY -o pan_safety pan.c
./pan_safety -m2000 -N safety_freed_vslot_has_no_owner -n
```

Saved run: `9445 states stored`, `10373 transitions`, `errors: 0`.

Liveness with weak process fairness:

```bash
cc -O1 -w -DNFAIR=16 -o pan_live pan.c
./pan_live -a -f -m2000 -N live_delete_eventually_frees_vslot -n
```

Saved run: `14875 states visited`, `30817 transitions`, `errors: 0`.

Deadlock and hidden bounded backpressure:

```bash
cc -O2 -w -DNOCLAIM -DSAFETY -o pan_deadlock pan.c
./pan_deadlock -m2000 -n
```

Saved control run: `947 states stored`, `1875 transitions`, `errors: 0`.

The self-restart path (`PDisk -> AskWardenRestartPDisk -> BSC -> ServiceSet(RESTART)`) is checked with the same profile and `PDISK_RESTART_MODE=3`: `9721 states stored`, `10685 transitions`, `errors: 0`.

The following separate profile enables both BSC and NodeWarden restarts without a PDisk restart:

```bash
spin \
  -DENABLE_CONFIRMATION_RETRY=0 \
  -DOPERATION_MODE=1 \
  -DPDISK_RESTART_MODE=1 \
  -DENABLE_BSC_RESTART=1 \
  -DENABLE_NW_RESTART=1 \
  -a "$SPIN_DIR/slay_recovery_v1.pml"
```

Safety: `45858 states stored`, `54741 transitions`, `errors: 0`. Weak-fair liveness: `61687 states visited`, `163972 transitions`, `errors: 0`.

## WIPE -> DESTROY Race

Action-aware recovery prevents the lost-Slay failure, but it does not eliminate this separate race:

1. WIPE removes the owner, and BSC accepts `WIPED`.
2. The replacement VDisk allocates a new `InitOwnerRound`, but has not necessarily sent `YardInit` yet.
3. DESTROY sends Slay and receives `ALREADY` because the owner is currently absent.
4. NodeWarden reports `DESTROYED`, although the live init can still create an owner.

In the saved `slay_recovery_v1_wipe.trace`, the BSC-side `assert(!init_cut_active)` catches this condition.

```bash
spin \
  -DENABLE_CONFIRMATION_RETRY=0 \
  -DOPERATION_MODE=2 \
  -DPDISK_RESTART_MODE=1 \
  -DREQUIRE_WIPE_COMPLETION \
  -a "$SPIN_DIR/slay_recovery_v1.pml"
cc -O2 -w -DBFS -DSAFETY -o pan_safety pan.c
./pan_safety -m2000 -N safety_freed_vslot_has_no_live_init -n
./pan_safety -r slay_recovery_v1.pml.trail -S \
  -N safety_freed_vslot_has_no_live_init
```

Expected result: `errors: 1`. `REQUIRE_WIPE_COMPLETION` uses a monotonic ghost that is set only after BSC accepts the `WIPED` report; the initial `MOOD_NORMAL` state does not satisfy it.

## Confirmation Retry and Time

Safety holds with `ENABLE_CONFIRMATION_RETRY=1`, but the untimed Promela model cannot prove delete liveness using weak process fairness alone. A fair cycle is possible: the confirmation timer repeatedly overtakes the matching result, creates a fresh round, and makes the arriving result stale. Every process still runs, so weak process fairness does not rule out the cycle.

The saved successful liveness result covers only the core profile with `ENABLE_CONFIRMATION_RETRY=0`. Proving liveness with the retry enabled requires either an explicit model of time or an assumption that a matching response is handled before the next confirmation timeout.

## Limitations

- The model contains one VSlot and one injected PDisk restart.
- BSC and NodeWarden can each perform one logical session restart. A full host restart that recreates PDisk and the `LocalPDiskInitOwnerRound` allocator is not modeled.
- The model stores one concurrent `InitOwnerRound` cut and one YardInit result outbox. If the combined `WIPE + PDisk restart` profile requires a second concurrent init, the model fails with `[MODEL SCOPE]` instead of silently serializing the event. Exhaustive coverage of the full Cartesian product is not claimed.
- Permanent PDisk removal, `pdiskMissing`, `UnderlyingPDiskDestroyed`, and immediate completion of `SlayInFlight` during `DestroyLocalPDisk` are not modeled.
- Protobuf serialization, BSC journaling, metrics, and physical I/O are not modeled.
- Channels are bounded. The transport does not lose messages by itself: loss occurs only at an actor-incarnation boundary. New profiles should also be checked with adjacent capacity values.
- `REQUIRE_INPUT_QUEUE_LOSS`, `REQUIRE_WIPE_COMPLETION`, and `REQUIRE_SLAY_IN_FLIGHT_RESTART` only select subsets of traces. Success in a restricted profile is not proof for a broader profile.
