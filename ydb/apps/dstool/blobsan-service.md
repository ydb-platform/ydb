# Continuous blob layout scans and single-DC repair

`dstool blobsan-service` runs outside `ydbd`. It discovers conventional
`mirror-3-dc` groups, scans VDisk metadata, and issues `blob_range` requests with
`mustRestoreFirst=1` for blobs whose observed local-part layout is incomplete.
It repeats the scan after repair and continues monitoring for new candidates.
The service lives in the built `dstool` binary. Workers also need the
`blobsan` executable built from `ydb/tools/blobsan`, with support for
`--write2x2-candidates`. It is invoked as a
black box; the service does not reproduce its analysis logic. No Python
installation, shared database, or new server code is required. The service uses
Python's standard library and dstool's existing connection/configuration code;
the terminal client uses Textual, as does `tools/mnc/viewer`.
Master, worker, and the console require POSIX (Linux is the deployment target).

Every successful scan produces a histogram of **all observed blobs**, before
candidate filtering, including full layouts and GC metadata. Use worker
`--scan-only` to collect these reports without issuing repair requests. Recovery
continues to use the existing `mustRestoreFirst` behavior when that option is
omitted; scans with fewer than two complete realms only publish observations.

## Start

```bash
dstool --token-file ~/.token blobsan-service master \
    -l 22570 --db ~/w2x2_master_db/ --console-sock /tmp/w2x2.sock

# On each selected storage host:
dstool --token-file ~/.token blobsan-service worker \
    --master master-host:22570 --db ~/w2x2_worker_db/ --blobsan ~/blobsan

# Observation without repair:
dstool --token-file ~/.token blobsan-service worker \
    --master master-host:22570 --db ~/w2x2_observer_db/ --blobsan ~/blobsan --scan-only
```

For this command only, an omitted `-e` defaults to `localhost`. Each process
uses its own endpoint and credentials. All workers must point to the **same
cluster** as the master. Global `-e`, `--mon-port`, TLS and token options work
as usual. `--viewer URL` overrides the HTTP Viewer URL; the master still uses
its global endpoint for BSC discovery. Global token defaults/environment
variables follow dstool conventions; `~/.token` is used only when supplied
explicitly. Tokens are read at startup; restart the process to rotate them.
`worker --blobsan PATH` defaults to `~/blobsan`. The master does not need it.

Choose separate persistent directories for master and each worker. A file lock
prevents two processes using the same directory. Run under your normal process
supervisor for restart after host/process failure. Signals SIGINT/SIGTERM stop
the service; an in-flight HTTP operation can run until its timeout.

## Deploy

From the `ydb` directory, after building:

```bash
for host in host1 host2; do scp ./apps/dstool/ydb-dstool "$host:dstool.new" && ssh "$host" 'chmod +x ~/dstool.new && mv -f ~/dstool.new ~/dstool'; done
```

The temporary file allows replacing a running executable. Restart services
separately to use the new binary. Credentials are not copied.

To copy both executables to worker hosts:

```bash
for host in host1 host2; do scp ./apps/dstool/ydb-dstool "$host:dstool.new" && scp ./tools/blobsan/blobsan "$host:blobsan.new" && ssh "$host" 'chmod +x ~/dstool.new ~/blobsan.new && mv -f ~/dstool.new ~/dstool && mv -f ~/blobsan.new ~/blobsan'; done
```

## Console

The Unix socket is created with mode **0777**, as requested. Anyone who can
access it can run console commands. No shell commands are executed by the
console. Start the built-in terminal client on the master host:

```bash
dstool blobsan-service client --console-sock /tmp/w2x2.sock
```

The client needs only the socket, without `--db`, credentials or cluster access.
It displays pools, groups, layout histograms, workers and unresolved findings.
Select a pool or group with **Enter** to open its histogram; Enter on a finding
opens its group. Local layout `?` means incomplete coverage of that realm;
`0` means all three VDisks were scanned and no local part was observed.
`Local` retains group failrealm order. `Local3` sorts its three counts descending,
with `?` last. `Local2` drops the last entry of Local3 (the smallest count or `?`).
For example, `1+2+1` becomes Local3 `2+1+1` and Local2 `2+1`; `1+?+2` becomes
Local3 `2+1+?` and also Local2 `2+1`. Failrealm permutations have equivalent
replica distributions in mirror-3-dc.

The main histogram groups by Local2 and sorts Local2 ascending, with known
counts before `?`. It shows blob counts, logical size, `phantom_like` and
`write2x2-candidate`. Enter on a Local2 row opens its original Local, Local3,
ingress/GC breakdown, replica size and missing metadata.
Sizes choose B/KiB/MiB/GiB automatically.
Ingress columns show metadata claims.

The default GC filter includes `soft_with_keep` and `no_covering_barrier`.
Choose either separately, any other GC state, or all states in the selector.
`hard_barrier`, `do_not_keep` and `soft_without_keep` have a collapsed GC summary.
Annotation columns display `yes`, `no`, or `mixed (N/total)` when only N blobs
in the Local2 row have the flag. `unknown` means the report lacks the annotation;
old reports remain readable. The flags come from blobsan, including for observation-only
workers; the client does not infer them from Local or ingress. Each refresh replaces
the displayed histogram. Each group contributes only its latest successful scan.

The active view and service stats refresh every 5 seconds (`client --refresh SEC`).
**F5** refreshes immediately, **[** / **]** switch tabs, **Ctrl+L** focuses the
command input, and **Ctrl+Q** exits. Buttons pause/resume job assignment, schedule
all groups for scanning, and refresh inventory. The command input accepts all
commands below; responses appear in the Console tab. `quit` also exits the client.

After connection loss the client retains the last displayed data, marks it stale,
and reconnects on the next refresh. Requests have a 10-second deadline
(`client --request-timeout SEC`). A failed interactive command is never retried
automatically: its effect may already have been applied by the master.

| Command | Effect |
|---|---|
| `stats` | Discovery status, scans, candidate totals, unresolved findings, live workers, stale inventory and missing VDisks |
| `groups` / `groups brief` | Per-group scan time, candidate count, coverage, repair response counts, owner, scheduling and errors; `brief` omits full reports and includes pool identity/generation |
| `layouts GROUP` | Last successful histogram with captured/missing VDisks, stream errors, scan timestamps, generation and latest attempt error |
| `pools [POOL]` | Histograms summed over the latest reports per group; select by pool name or `BoxId:StoragePoolId` (`static` for static groups) |
| `workers` | Known workers and last contact timestamps |
| `findings [GROUP]` | Up to 1000 unresolved candidates with last repair response |
| `pause` / `resume` | Stop/resume assigning new jobs; running jobs finish |
| `scan [GROUP\|all]` | Make a group/all groups eligible for scanning now |
| `interval SECONDS` | Change the clean-group rescan interval |
| `retry SECONDS` | Change the delay after candidates or scan errors |
| `refresh` | Refresh group topology now |
| `help` / `quit` | Show commands / disconnect |

Intervals and pause state persist in SQLite. `--interval` (default 3600 seconds)
and `--retry` (60) initialize a new database; subsequently use the console.
Changed intervals apply when scheduling the next completed job; `scan all`
also resets previously scheduled due times. The socket retains its plain-text
request/response protocol for other clients. Times in raw console rows are Unix
timestamps; dashboard tables display ages.

## Scheduling and persistence

One worker processes one group at a time. The master avoids concurrent jobs
sharing storage nodes. This is intentionally conservative; tune concurrency
after measuring real groups. All VDisks of one group can stream concurrently.
Repair requests are sequential; `worker --repair-delay SECONDS` adds spacing.

Master SQLite stores topology, leases, schedules, findings and the latest
summary per group. Workers persist their identity and unacknowledged report in
their local directory. Compressed snapshot files for the last job remain in
`DB/scan` and are replaced by the next job. The assembled input is `snapshot`;
the analyzer's full stderr is `blobsan.err`. `manifest.json` records the group
and successfully completed streams, including empty streams; `report.json`
contains the analyzer histogram. Resource usage depends on group
size, the external analyzer and retained findings.

Jobs have renewable leases (`master --lease`, default 120 seconds). Failed
workers' jobs become available after lease expiry. Claim retries return the
same assignment; duplicate reports are acknowledged without applying twice;
expired/reassigned reports are rejected. Worker heartbeat interval defaults to
20 seconds and is capped at one third of the granted lease. Loss of contact
stops new repair requests; an already issued request cannot be revoked in
`ydbd`. The master itself is a single coordinator, without automatic failover.

`master --refresh` controls topology discovery (300 seconds). Stale inventory
older than twice this interval is not assigned. Generation changes invalidate
old jobs. Workers verify group generation before scanning, before repairs and
after repairs. Each check makes a fresh Viewer request limited to the group's
storage nodes (`node_id`), with the response filtered by `GroupID`. No global
whiteboard query is used for these checks. The configured geometry must be
3 realms x 3 domains x 1 VDisk.
Static groups are discovered through Viewer; an unavailable static VDisk may
be absent from that inventory and is reported as missing. A static inventory
with at least one VDisk is sufficient for an observation. Dynamic virtual/blob-depot groups are excluded.

The API listens on all interfaces by default (`--bind` changes this). Use it on
a trusted network. Optional `--secret-file` on master and workers enables
shared-secret authentication. The built-in master API is HTTP, not TLS; it
should not be exposed to an untrusted network. The cluster token is never sent
to the master by a worker.

## What a successful pass means

The worker downloads `vdisk_stream` responses until the endpoint returns an
empty response. HTTP failures and explicit `ERROR` responses discard that
VDisk's stream; the next attempt creates a new session. A session request is
never retried because the server may already have advanced its cursor. The
worker adds the same group/index/length framing as `dstool group take-snapshot`,
without decoding the metadata payload.

An observation requires at least one complete VDisk stream. Partial streams
are discarded; missing indexes and errors are recorded. With no complete
streams, the attempt fails and preserves the previous report. Snapshots across
VDisk are not atomic. Empty initial streams are valid empty indexes and are
explicitly identified by the manifest. Candidate processing still requires
three complete VDisk streams in each of at least two realms.

The assembled snapshot is passed as stdin to
`blobsan --write2x2-candidates --scan-manifest manifest.json --scan-report report.json mirror-3-dc`.
Blobsan emits its usual candidate diagnostics; the worker decides whether to
act on them. Stdout goes
to `/dev/null`, as in the original scripts (this also disables verbose output
in blobsan). Stderr is saved and only `GroupId# ... BlobId# ...` lines are parsed.
The supplied executable is the sole authority on candidate selection and
snapshot payload validation. The service validates the JSON report's schema,
coverage and histogram totals. A nonzero exit, timeout, or malformed candidate
line fails the pass. The process is terminated on cancellation or lease loss.
The service itself does not check ingress, GC barriers, or replication layout
inside the snapshot. Transport completion plus analyzer success is not an
independent proof of complete or consistent metadata. The analyzer must support
`--write2x2-candidates`; the ordinary diagnostic mode selects different blobs.

`request_ok` means `blob_range` returned the requested blob with `Status=OK`;
`absent` means it returned an empty list. Neither removes a finding. A later
scan removes each finding it no longer observes only if the VDisk coverage
matches the scan that last observed that finding, or includes all nine VDisks.
Coverage is persisted per finding: repeated scans of a different set of VDisks
cannot close an older finding. Groups with unresolved findings use
the retry interval even when the latest scan has no candidates. The report's
`unresolved` count includes findings retained from earlier scans.
`disappeared` means no longer a candidate, not proof that a blob was copied
rather than garbage-collected.
`new_total` includes initially discovered and subsequently reappearing candidates.
Scan errors preserve existing findings. Repair failures remain visible and are
retried after another scan; there is no permanent processed-blob blacklist.

No direct writes of blob contents are performed by the service: actual repair
and GC/existence decisions remain in DSProxy through `mustRestoreFirst`.

## Layout reports

`layouts GROUP` includes `scan_report` version 1, with `group_id`,
`completed_vdisks`, `buckets` and `totals`. Each bucket has `local`, `ingress`,
`gc` and four counters:

| Counter | Meaning |
|---|---|
| `blobs` | Unique BlobIds within the group, after merging duplicate index records |
| `logical_bytes` | Sum of BlobSize once per blob |
| `observed_replica_bytes` | Sum of BlobSize for every observed correctly placed local part |
| `missing_metadata_blobs` | Blobs with no index record on at least one successfully captured VDisk |

New analyzers also supply boolean `phantom_like` and `write2x2_candidate` per
bucket. Buckets with different annotations remain separate in the report and
pool aggregation, even when Local, ingress and GC match. `phantom_like` is the
analyzer's `seenDisks != blob.Info.Nodes` condition: at least one captured index
has no metadata record for this BlobId. `write2x2_candidate` uses the same GC,
local completeness, metadata, restorability and failure-model conditions as the
`--write2x2-candidates` output. It is included in the report regardless of whether
the worker issues repair requests. Without `--write2x2-candidates`
this field is null, since that mode accepts different GC barriers. Missing/null
annotations in older reports mean unknown, rather than false.

Both layout strings use **group failrealm order** (0, 1, 2), rather than the
blob-dependent part/subgroup order. `local=1+1+?` means one copy in each of the
first two realms and incomplete observation of the third. A realm is `?` when
any of its three VDisks is missing, regardless of copies observed on its other
VDisk(s). `0` means all three indexes were captured and no local part was found.
`ingress` counts correctly placed parts known from merged ingress, including
claims about unavailable VDisks. It is metadata knowledge, not confirmation of
local data. Wrong-realm parts are not counted as replicas.

Local counters are collected before applying GC discards to the local layout.
GC states follow the existing analyzer's accepted barriers and merged flags,
with the following precedence:

| `gc` | Condition |
|---|---|
| `hard_barrier` | A covering hard barrier was observed |
| `do_not_keep` | DoNotKeep is set |
| `soft_without_keep` | A covering soft barrier was observed, without Keep |
| `soft_with_keep` | A covering soft barrier was observed, with Keep |
| `no_covering_barrier` | No accepted covering barrier was observed on captured indexes |

These labels describe the observed metadata, not proof of current physical
existence or completed garbage collection. In write2x2 mode the analyzer accepts
barriers without quorum. Blobs excluded by candidate filters, including missing
metadata and complete layouts, remain in the histogram. Blobs absent from every
captured index cannot be inventoried; partial reports describe observed bytes.

The master persists the latest successful report for each group independently
of its latest attempt. Failure preserves that report and is shown as
`latest_error`. Observation-only and insufficient-coverage scans do not close
previous recovery findings or report zero recovery candidates.

`pools` sums each group's latest histogram once. It reports `full_groups`,
`partial_groups`, `never_scanned_groups`, `outdated_groups`,
`stale_inventory_groups` and `failed_groups`, plus oldest/newest scan times.
Reports whose group generation or topology fingerprint has changed are excluded,
as are groups whose inventory is older than twice the discovery refresh interval.
Other old reports remain visible with their timestamps, including after a failed
attempt. Pool identity is BoxId/StoragePoolId, so identical pool names in
different boxes stay separate. Failrealm positions are retained in pool buckets;
they refer to group topology, not a global datacenter name.
Cached dynamic inventory without pool IDs from older masters is kept separate
per group under `unknown:GROUP` until topology discovery refreshes it.

Rebuild both `dstool` and `blobsan` when upgrading to layout reports. Existing
master databases gain the report table automatically; groups without reports
are listed as never scanned until a reporting worker visits them.
