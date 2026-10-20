# Configuration versions

[distconf.pml](distconf.pml) checks recovery, FIFO metadata writes, acknowledgment,
refill and replacement of an old same-generation conflict.
[check.py](check.py) has candidate, compatibility, legacy and recovery suites; compatibility
compares participant rules with the same history under `ALL_OLD=1`, not binaries.

Run from the repository root with Python 3, SPIN and a C compiler:

```bash
python3 ydb/core/blobstorage/nodewarden/spin/config_versions/check.py --suite all --decompose --timeout 300
python3 ydb/core/blobstorage/nodewarden/spin/config_versions/check.py --suite legacy --scenario 30 31 32 --check liveness --legacy-repair 0
python3 ydb/core/blobstorage/nodewarden/spin/config_versions/check.py --suite recovery --output /tmp/distconf-recovery-fix-20261021/spin-recovery
python3 ydb/core/blobstorage/nodewarden/spin/config_versions/check.py --suite recovery --scenario 37 38 39 --conservative-recovery 0 --check safety --output /tmp/distconf-recovery-fix-20261021/spin-old-recovery
python3 ydb/core/blobstorage/nodewarden/spin/config_versions/check.py --suite recovery --scenario 44 --recovery-repropose 0 --check safety --output /tmp/distconf-recovery-fix-20261021/spin-unsafe-publication
python3 ydb/core/blobstorage/nodewarden/spin/config_versions/check.py --suite recovery --scenario 46 --recovery-fresh-generation 1 --parent-witness 0 --check safety --output /tmp/distconf-recovery-fix-20261021/spin-fresh-recovery
python3 ydb/core/blobstorage/nodewarden/spin/config_versions/check.py --suite recovery --scenario 47 --parent-witness 0 --check safety --output /tmp/distconf-recovery-fix-20261021/spin-normal-child
python3 ydb/core/blobstorage/nodewarden/spin/config_versions/check.py --suite recovery --scenario 49 --require-nonzero-recovery-base 0 --check safety --output /tmp/distconf-recovery-fix-20261021/spin-zero-base
python3 ydb/core/blobstorage/nodewarden/spin/config_versions/check_admission.py --timeout 90
python3 ydb/core/blobstorage/nodewarden/spin/config_versions/check_tree.py --decompose --timeout 90
```

The second command is a negative control: disabling legacy-conflict repair must
prevent convergence. The fourth command disables only conservative recovery; each
case must produce an `acknowledged_payload_lost_after_Advance` assertion witness.
A pass or an unrelated counterexample fails that control. The fifth command
demonstrates why a merely possible quorum cannot justify direct publication: it
must produce `configuration_visible_without_durable_choice`. The sixth command
combines a fresh child generation with disabled parent-witness recovery; interrupted
recovery must then produce `acknowledged_payload_lost_after_Advance`. The seventh disables
parent-witness recovery and must expose the same payload loss after an ordinary
child proposal. The eighth must expose `nonbootstrap_history_fell_back_to_zero_generation`.
Parent-witness recovery alone now preserves the acknowledged parent in case 46;
the combined control deliberately disables that protection. Admission controls
must expose unsafe bypasses or blocked expansion. Only explicitly declared
counterexamples are accepted. Timeout,
resource exhaustion, incomplete search or a missing witness never count as PASS.
Runners print the directory containing model snapshots, commands and traces.

Cases 1–36 retain the existing Replace abstraction: a new root returns OK only
after a committed quorum; old-root fixtures retain their earlier acknowledgment
rule. Cases 37–39, 46–47 and 49 instead model the actual non-Replace path: a proposed quorum
allows publication and OK before a committed quorum. They acknowledge a semantic
field change, delay publication to B, turn over the root, then issue an
AdvanceGeneration that copies the recovered body's field. A separate history
observer requires the acknowledged field to survive applying higher generations
and final convergence. Starting Advance does not clear this observer when it
resets the older `expected` value.

The recovery suite varies persistence of the old root's committed record. Case
37 reads B+C while A is down; case 38 restarts A and nondeterministically selects
two or three read responses; case 39 reads a formatted A as NoMetadata. Only disks
returning a committed or proposed record disprove another proposal's presence.
Missing/error and NoMetadata disks remain unknown possible acceptors. A seen
configuration can have a former quorum when its copies plus unknown disks reach
Q. Recovery requires each such candidate to extend the selected admitted base,
following only intermediate bodies actually read. Any unproved ancestry blocks
recovery, including a PrevConfig older than that base. The highest candidate must
also provably extend every other viable candidate; siblings block recovery even
when their generations differ. With all disks stateful and read, an unpublished
minority proposal remains unsupported, as in case 23.

A normal child proposal can replace accepted copies of its chosen parent before
the parent has a committed copy. A read child's immediate PrevConfig therefore
makes that parent a mandatory recovery candidate when it is newer than the
admitted base, or has a different fingerprint at the same generation. Recovery
requires the parent's full body from an actual disk read; an embedded truncated
PrevConfig cannot supply it. Missing parent bodies, unknown ancestry and competing
mandatory parents block recovery. Parent evidence never authorizes publication by
itself: the selected body must still obtain a real proposal quorum. An unrelated
unpublished minority proposal keeps case 23's existing behavior.

A possibly chosen candidate without publication or an observed durable quorum
becomes a pending recovery proposal. Recovery replays exactly that candidate: its
generation, fingerprint identity, PrevConfig and body remain unchanged. A partial
recovery therefore cannot replace an accepted identity with a different minority
proposal that a later complete read would discard.
Each replica receives its proposal RPC, persists its metadata and sends its ACK
independently. Only quorum ACKs allow publication, node-local application or a
fresh refill reply. An upgraded replica may repeat its vote for its already
applied committed body when no metadata write or proposal is pending. It persists
the unchanged metadata record before ACKing, preserving any higher proposed body.
A failed recovery proposal returns to collection after all replica responses;
a committed record that finished after the previous read can then provide
publication evidence. Case 24 combines a queued committed write with stale applied
feedback during root turnover. A root change cancels callbacks through the operation cookie;
queued replica writes survive. Existing committed/publication evidence and
actual proposed quorums retain direct recovery. The `chosen_once` observer checks
every application against durable choice/publication evidence, independently of
the recovery algorithm's possible-quorum calculation.

Cases 40–43 check the collection decision directly: an obsolete parent blocks,
an unproved skipped ancestor blocks, a read intermediate body proves the chain,
and two possibly chosen fingerprints at the same generation block. Case 45
checks two possibly chosen siblings at different generations. These snapshot
cases run safety checks; cases 37–39 also check fair final convergence.
Case 44 starts with an unpublished minority proposal, withholds recovery writes
so recovery cannot reach quorum, then turns the root over again before
delivering the remaining replica I/O. It checks that neither replacement root
exposes that body before durable choice. Explicit prefix helpers select complete
RPC/write/ACK schedules; the concurrent suffix keeps all those transitions separate.
Case 46 acknowledges X on A+B, crashes A before its committed write, then persists
only B’s recovery proposal. It restarts the nodes to discard pending writes and
volatile publication hints, reads all disks and performs AdvanceGeneration. Exact
replay preserves the original accepted quorum of X. Without parent-witness recovery,
a fresh child would leave X only on A and its child only on B, allowing the
acknowledged field to disappear.

Case 47 repeats the acknowledged-parent history using an ordinary same-root
AdvanceGeneration as the partial child. Case 48 reads every disk but lacks the
mandatory parent's full body and requires an explicit refusal. Case 49 starts
from generation zero with root C, chooses bootstrap P1 on A+B while C’s committed
writes fail, acknowledges P2 on A+B, then saves
P3 only on B. After all nodes restart, recovery must refuse the ordinary
nonbootstrap history without an admitted nonzero base. It must not fall back to
bootstrap. This case checks safety only and deliberately models metadata-write
failures by discarding C's pending committed writes.

The primary model is nonbridge: `N=3/Q=2`, one disk per node, fixed membership,
bounded fault prefixes and a stable suffix under weak process fairness. It checks
same-generation safety, convergence, publication after durable choice, and the
bounded acknowledged-payload histories above. Except for case 49's explicit bootstrap prefix, it does not prove bootstrap,
membership changes, erasure geometry or the full C++ tree/protocol.
Interrupted expansion may require an external Replace retry in the implementation.
Root turnover is abstracted by `change_root` and a local operation cookie;
C++ emergency invocation and Scepter orchestration are outside this model.
The model does not add root epochs or prove acceptor fencing against stale-root
proposal replay; that separate protocol gap remains outside these histories.

[targeted_admission.pml](targeted_admission.pml) separately checks challenge fencing
and expansion `1→2`, `3→6`, `3→7` in one DC, without full recovery or bridge.
The combined [tree model](../three_node_convergence.pml) uses `CONFIG_VERSIONS=1`.
Its default bounded cases cover stable bindings, committed propagation and an
empty intermediary; full race/disconnect/MAIN Replace searches remain incomplete.
`--decompose` requires both goal closure and fair reachability to pass.
