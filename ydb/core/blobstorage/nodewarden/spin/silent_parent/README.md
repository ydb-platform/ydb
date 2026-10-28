# Silent parent

Run from the repository root with Python 3, SPIN and a C compiler:

```bash
python3 ydb/core/blobstorage/nodewarden/spin/silent_parent/check.py --timeout 90
```

Use repeatable `--case NAME` to select cases; `--output DIR` needs an empty
directory. The runner records snapshots, hashes, commands, results and trails.
Exit codes: `0` expected results, `1` violated property or missing control witness,
`2` setup failure or incomplete search. Timeout never counts as PASS.

| Model | Checks |
|---|---|
| [silent_parent.pml](silent_parent.pml) | A stopped keeper with a live session, disconnect, rebinding, delivery and late-callback fencing. |
| [heartbeat.pml](heartbeat.pml) | Three missed deadlines, valid-reply reset, cookie/session guards and reply/deadline races. |
| [queued_old_quorum.pml](queued_old_quorum.pml) | Eight-node bridge history: partial reads, timeout, remembered generation, fresh-read retry and old queued proposal. |

Routing uses a three-node chain or six identities in two abstract piles with two
physical votes required per pile. Healthy route selection is abstracted; healthy
IO and delivery must eventually fit a scatter window. The stopped keeper need
not recover. Weak process fairness alone does not imply these timing assumptions.

Declared broken guards must yield the expected assertion or no-success cycle;
reachability controls must reach their witness. The no-cooldown control forces
reselection of the same suspect; it does not reproduce C++ queue selection.

These models isolate routing and fencing. They do not prove configuration-body
selection, complete recovery, mixed versions, arbitrary queued-message histories,
membership changes, real erasure layouts or general bridge-consensus safety.
The queued-proposal model enumerates metadata-response subsets; it no longer
assumes that the first collect succeeds or sees the old proposal. A retry after
the finite fault prefix reads the healthy quorum. Broken controls reproduce
generation reuse both with an observed proposal and with none in the partial read.
