# VPatch Promela models

These models describe the distributed `VPatch` path implemented by
`dsproxy_patch.cpp` and `skeleton_vpatch_actor.cpp`.

The reliable-network baseline models intentionally omit blob bytes,
encryption, metrics, queue accounting, and the implementation of local
`VGet`/`VPut`. A part write is a single state transition. Channels still model
the important message ordering:

1. DSProxy sends `VPATCH_START` and gathers `FOUND` replies.
2. DSProxy sends one `VPATCH_DIFF` to each selected part actor.
3. Data-part actors send `XOR_DIFF` messages to parity-part actors.
4. A parity part may be written only after its own diff and all expected XORs.
5. DSProxy replies only after the erasure-specific durability condition.

Models:

- `vpatch_block_2plus1.pml`: reduced two-data/one-parity model for fast checks.
- `vpatch_block_4plus2.pml`: production block-4+2 parameters over the same core.
- `vpatch_mirror3dc.pml`: current placement-aware Mirror-3-DC completion logic.
- `vpatch_mirror3dc_legacy.pml`: pre-fix regression model using only the
  successful-result count.
- `vpatch_erasure_none.pml`: one-part ErasureNone baseline.

The baseline models use a reliable network. Two additional models isolate the
failure semantics without multiplying the production Block-4+2 state space:

- `vpatch_block_2plus1_failures.pml`: reduced model starting after successful
  `FoundParts`, with BSQueue disconnect before/after VDisk acceptance,
  synthetic errors, stale late results, parity timeout, and fallback. A finite
  sequence of pre-accept `TRYLATER` replies is a stutter step in this model.
- `vpatch_force_end_current.pml`: current ForceEnd response suppression.
- `vpatch_force_end_fixed.pml`: comparison model that forwards the ForceEnd
  result through BSQueue while DSProxy ignores it for quorum accounting.

The failure model applies an assumed BSQueue request/result contract to three
`VPATCH_DIFF` and two `XOR_DIFF` requests. It checks VPatch against that
contract; it does not derive the contract from the queue retry/session
automaton. `START/FOUND` is composed out to keep exhaustive verification
small. The model assumes a finite VPatch deadline and a terminating fallback.
It permits a VDisk accepted before disconnect to finish after fallback; both
paths write the same target value. Fallback starts at the first terminal
selected-Diff error and does not stop the other VPatch actors. The model does
not include payload bytes, handoff, topology changes, queue cost, or more than
one logical Patch request.

The focused ForceEnd models separate server-side SkeletonFront window
completion from the client BSQueue item and DSProxy result accounting. They
assume that the local `TEvVDiskRequestCompleted` reaches SkeletonFront; loss of
that local event and SkeletonFront restart are outside their scope.

Run the commands from this directory. Safety assertions and liveness are
checked separately:

```bash
# Safety and invalid end states.
spin -a vpatch_block_2plus1.pml
cc -O2 -DMEMLIM=2048 -DSAFETY -DNOCLAIM -o pan pan.c
./pan -m200000

# Liveness under weak process fairness.
spin -a vpatch_block_2plus1.pml
cc -O2 -DMEMLIM=2048 -DNFAIR=8 -o pan pan.c
./pan -a -f -N live_eventual_reply -m200000
```

Spin 6.5.2 selects an inline named claim at `pan` runtime. Generate once, then
check safety and liveness separately:

```bash
spin -a vpatch_block_2plus1_failures.pml
cc -O2 -DMEMLIM=2048 -DNFAIR=16 -o pan pan.c
./pan -a -N safe_ok_requires_correct_target -m500000
./pan -a -N safe_vpatch_ok_requires_all_writes -m500000
./pan -a -N safe_parity_requires_unique_xors -m500000
./pan -a -N safe_single_client_reply -m500000
./pan -a -N safe_queue_accepts_at_most_once -m500000
./pan -a -N safe_stale_results_not_forwarded -m500000
./pan -a -f -N live_client_eventual_reply -m500000
./pan -a -f -N live_sent_requests_terminal -m500000

# Expected counterexample for the current ForceEnd implementation.
spin -a vpatch_force_end_current.pml
cc -O2 -DMEMLIM=1024 -DNFAIR=8 -o pan pan.c
./pan -a -N safe_no_completed_request_left_inflight -m100000
spin -t -p -g -l vpatch_force_end_current.pml

# The comparison model must hold both safety and fair liveness.
spin -a vpatch_force_end_fixed.pml
cc -O2 -DMEMLIM=1024 -DNFAIR=8 -o pan pan.c
./pan -a -N safe_no_completed_request_left_inflight -m100000
./pan -a -N safe_no_collateral_queue_error -m100000

# Fairness with the watchdog rendezvous channel requires disabling POR.
cc -O2 -DMEMLIM=1024 -DNFAIR=8 -DNOREDUCE -o pan_live pan.c
./pan_live -a -f -N live_force_end_cleanly_releases_queue -m100000
```

`vpatch_mirror3dc_legacy.pml` is expected to violate
`safe_ok_requires_durable`: in 2x2 mode the old result-count rule can reply
after three successful results forming a 2+1 placement, before the fourth
write has completed. `vpatch_mirror3dc.pml` models the placement-aware quorum
now used by DSProxy.

## Baseline verification results

Measured with Spin 6.5.2. `holds` means an exhaustive search completed with no
error; `unknown` means that the search was stopped by its memory limit.

| Model | Check | Result | Stored states | Time |
| --- | --- | --- | ---: | ---: |
| Block 2+1 | safety | holds | 5,945 | 0.01 s |
| Block 2+1 | fair liveness | holds | 11,882 (53,984 visited) | 0.07 s |
| Block 4+2 | safety | unknown at 128 GiB | ~537,000,000 | 1,410 s |
| Mirror-3-DC, legacy result count | safety | violated | 2,433 before trail | 0.01 s |
| Mirror-3-DC, current placement quorum | safety | holds | 17,130 | 0.02 s |
| Mirror-3-DC, current placement quorum | fair liveness | holds | 34,237 (183,390 visited) | 0.31 s |
| ErasureNone | safety | holds | 18 | <0.01 s |
| ErasureNone | fair liveness | holds | 30 | <0.01 s |

## Failure verification results

Measured with Spin 6.5.2 on the models in this directory. Every `holds` row is
an exhaustive run with `errors: 0`; fair liveness uses weak process fairness.

| Model | Check | Result | Stored states | Time |
| --- | --- | --- | ---: | ---: |
| Block 2+1 failures | assertions/deadlocks | holds | 1,406,757 | 1.51 s |
| Block 2+1 failures | each safety LTL claim | holds | 1,406,757 | <=1.98 s |
| Block 2+1 failures | client eventually replies | holds | 2,200,669 (~10.9M visited) | 17.0 s |
| Block 2+1 failures | every sent queue item terminates | holds | 2,596,585 (~16.1M visited) | 26.1 s |
| ForceEnd, current | assertions/deadlocks | holds | 2,138 | <0.01 s |
| ForceEnd, current | stops actor / stays outside quorum | holds | 2,138 | <=0.01 s |
| ForceEnd, current | no completed item left in-flight | violated | 29 before trail | <0.01 s |
| ForceEnd, current | no collateral queue error | violated | 742 before trail | <0.01 s |
| ForceEnd, current | clean queue release | violated | 73 (159 visited) before trail | <0.01 s |
| ForceEnd, forwarded result | assertions/deadlocks | holds | 3,284 | <0.01 s |
| ForceEnd, forwarded result | each safety claim | holds | 3,284 | <=0.01 s |
| ForceEnd, forwarded result | clean queue release | holds | 4,912 (16,388 visited) | 0.04 s |

The ForceEnd trail is: DSProxy submits `TEvVPatchDiff(ForceEnd)` through
BSQueue; the VDisk actor stops and completes the server-side request window;
SkeletonFront suppresses `TEvVPatchResult`; the client queue item remains
in-flight. Successful unrelated responses can keep moving the watchdog barrier
and leave that item as a ghost indefinitely. After a complete quiet watchdog
interval, queue reset drains the item and may return a collateral error to an
unrelated waiting request. Forwarding the result closes the queue item while
`ForceStopFlags` still prevents it from counting toward VPatch quorum.

The legacy Mirror-3-DC counterexample has `mode=2`, four selected writers,
but returns success after acknowledgements distributed as DC0=2, DC1=1,
DC2=0. The fourth writer in DC1 has not completed yet. The current model waits
for either 1+1+1 or 2+2, depending on the selected mode.
