# Continuous blob layout scans and restore candidates

`dstool blobsan-service` discovers conventional `mirror-3-dc` groups, scans
VDisk metadata, and collects restore candidate BlobIds on a master. Workers
run `blobsan` once per snapshot with `--restore-canidates` to collect both
`inv1_candidate` and `inv2_candidate`. Workers issue no blob repair requests.
With `master --restore`, the master continuously restores candidates through
`POST /restore_part`, prioritizing `inv1_candidate`.

`MinRedundancyPerDc` is the minimum observed replica count across fully captured
datacenters, ignoring `?` realms. Eligible, restorable blobs enter `inv1_candidate` when
`MinRedundancyPerDc < 1` and `inv2_candidate` when `MinRedundancyPerDc < 2`.
With no fully captured datacenter, neither category is selected. Within each
scan, `inv1_candidate` is a subset of `inv2_candidate`. These lists describe
captured metadata and can change as the cluster continues operating.

The service lives in the built `dstool` binary. Workers also need the `blobsan`
executable built from `ydb/tools/blobsan`, with support for `--restore-canidates`.
The analyzer is invoked as a black box: dstool does not reproduce ingress,
GC-barrier, restorability or replica-layout analysis. No Python installation
or shared database is required. The runtime uses Python's standard library and
dstool's connection/configuration code; the terminal client uses Textual.
Master, worker and console require POSIX; Linux is the deployment target.

Every successful scan also produces a histogram of all observed blobs before
candidate filtering, including complete layouts and GC metadata. Scans with
insufficient candidate coverage still publish their histogram.

## Architecture

![BlobSan service on two cluster nodes: client connects to master by Unix socket;
master persists candidates and repair attempts in SQLite, coordinates workers
by HTTP and calls ydbd POST /restore_part; each worker streams group-wide VDisk
metadata and runs its local blobsan process.](blobsan-service-architecture.svg)

The diagram shows two representative cluster nodes. A `mirror-3-dc` group has
three realms and nine VDisks; a worker streams all available VDisks of its
assigned group, including VDisks on other hosts. `blobsan` runs locally as a
worker subprocess. The master alone owns the candidate database and issues
automatic repair requests. The client connects to the master's Unix socket
and can disconnect without interrupting scans or restoration.

## Start

```bash
dstool --token-file ~/.token blobsan-service master \
    -l 22570 --db ~/blobsan_master_db/ --console-sock /tmp/blobsan.sock --restore

# On each selected storage host:
dstool --token-file ~/.token blobsan-service worker \
    --master master-host:22570 --db ~/blobsan_worker_db/ --blobsan ~/blobsan
```

Omit `master --restore` to collect candidates without automatic repair.
The `ydbd` monitoring endpoint used by the master must support
`POST /restore_part` when automatic restoration is enabled.

For this command only, an omitted `-e` defaults to `localhost`. Every process
uses its own endpoint and credentials. All workers must point to the same
cluster as the master. Global `-e`, `--mon-port`, TLS and token options work
as usual. `--viewer URL` overrides the HTTP Viewer URL; master discovery still
uses the global endpoint for BSC. Tokens are read at startup, so restart to
rotate credentials. `~/.token` is used only when explicitly supplied.
`worker --blobsan PATH` defaults to `~/blobsan`; the master does not need it.

Choose separate persistent directories for the master and each worker. A file
lock prevents two processes using the same directory. Run under a process
supervisor for recovery after host/process failure. SIGINT/SIGTERM stop the
service; an in-flight HTTP operation can run until its timeout.

## Deploy

From the `ydb` directory, after building:

```bash
for host in host1 host2; do scp ./apps/dstool/ydb-dstool "$host:dstool.new" && ssh "$host" 'chmod +x ~/dstool.new && mv -f ~/dstool.new ~/dstool'; done
```

The temporary file allows replacing a running executable. Restart services
separately to use the new binary. Credentials are not copied.

To copy both executables to workers:

```bash
for host in host1 host2; do scp ./apps/dstool/ydb-dstool "$host:dstool.new" && scp ./tools/blobsan/blobsan "$host:blobsan.new" && ssh "$host" 'chmod +x ~/dstool.new ~/blobsan.new && mv -f ~/dstool.new ~/dstool && mv -f ~/blobsan.new ~/blobsan'; done
```

Rebuild both binaries when changing analyzer protocols. The previous
`--write2x2-candidates`, `worker --scan-only` and `worker --repair-delay` options
are no longer used.

## Automatic restoration

`master --restore` starts a background scheduler using the existing persistent
`inv1_candidate` and `inv2_candidate` lists. Each batch contains at most 500
distinct `(group, BlobId)` pairs. The master chooses eligible `inv1_candidate`
entries first, then fills remaining space with `inv2_candidate` entries that
have no eligible `inv1_candidate` observation. A retained older observation
does not hide a fresh eligible observation in the other category. Selection
is random across all groups within each priority, and the request lines are
shuffled to spread load.

Candidates must match fresh group inventory and its generation and topology.
Their observation must cover at least two complete realms; a complete realm
has all three VDisk streams captured. For each complete realm, the master
chooses one random VDisk for `inv1_candidate`, or two distinct random VDisks
for `inv2_candidate`. It submits full BlobIds with PartId 0. Thus a batch of
500 blobs can contain up to 3000 target VDisk requests.

The master uses its own Viewer URL and credentials to send one
`application/x-www-form-urlencoded` `POST /restore_part` with `requests`,
`inflight` and `timeout`. Only one HTTP batch runs at a time.
`--restore-inflight` limits concurrent target VDisk requests within the batch
(default 16). `--restore-timeout` sets the server deadline in seconds
(default 60, maximum 300). The HTTP deadline is at least ten seconds longer
than the server deadline and at least `--request-timeout`.

Before sending, the master commits the exact request and selected candidates
to SQLite. It retains the HTTP status and raw response, including non-2xx
responses, before interpreting per-VDisk `Status`, `ResponseOrigin` and `ErrorReason`.
`restore_batches` stores each HTTP exchange, `restore_attempts` preserves the
history of per-blob attempts, and `restore_requests` stores their per-VDisk
outcomes. `auto_restore_work` points to the latest attempt; another attempt
never overwrites a previous attempt. Network calls run outside the database
lock, so workers can continue reporting scans and the client remains usable.

A blob enters `awaiting_rescan` only when every selected target returned `OK`
or `ALREADY`. Known failures enter `retry_wait`; an unrecorded, malformed or
missing outcome enters `unknown`. Restarting the master marks interrupted
attempts unknown. Every attempted blob must have a new scan whose **lease was
issued after its attempt finished**, and the retry delay must elapse before
it can be selected again. This gate uses `groups.claimed_at`, recorded by the
master when assigning the scan and copied into the candidate's `scan_started`.
Worker wall-clock timestamps are diagnostic only and do not control retries.
A completed batch schedules its groups for scanning. Repeated restoration in
one group does not move its rescan ahead of groups already waiting.
Candidates remain active until the analyzer's later observations remove them;
successful HTTP responses alone do not prove the desired replica layout.

`pause` stops new scan assignments and new restore batches; operations already
in progress finish. `resume` restarts both schedulers. SIGINT/SIGTERM allow the
current HTTP call to finish within its timeout. Manual restoration is disabled
while `master --restore` is active. To disable automatic restoration, restart
the master without the flag, keeping the same `--db` directory.

### Progress and completion estimates

The client summary and **Restore** tab show the automatic scheduler state and
remaining eligible blobs, split into `inv1` and `inv2 only`. The Restore tab
also shows the current batch, active candidate count, blobs in flight,
`awaiting_rescan`, `retry_wait`, unknown outcomes, and candidates excluded due
to stale inventory, changed topology or insufficient coverage. These counts
refer to distinct `(group, BlobId)` pairs, not the sum of both candidate lists.

The recent rate uses successful per-blob attempts from the last five minutes,
in blobs/s and logical bytes/s. An attempt counts as successful only when all
its target VDisk requests returned `OK` or `ALREADY`; repeated successful
attempts can count the same blob more than once. Cumulative successful totals
have the same meaning and are not a count of candidates verified as repaired.
The tab also shows recent per-VDisk status counts and up to 20 latest errors
with GroupId, BlobId, VDiskId and response origin.

The client also displays separate recent rates and ETAs for `inv1` and
`inv2 only`. For each phase, ETA divides its **currently eligible pending
queue** by that phase's successful-attempt rate. The total ETA sums both phase
estimates; it is unavailable if a phase has pending work but no measured
nonzero rate. The master does not extrapolate the `inv1` rate to `inv2`.

ETA excludes in-flight requests, rescans, retry delays, unknown outcomes and
excluded candidates. New scan results can add work or change its priority, so
ETA estimates the current queue rather than a final cluster completion time.
Track active candidates and the waiting states alongside ETA to see work that
still needs observation. The console exposes these fields, including
`phase_rates.inv1` and `phase_rates.inv2_only`, in the `automatic` object
returned by `stats` and `restore-preview`.

## Console and client

The Unix socket is created with mode 0777. Anyone who can access it can run
console commands. The console executes no shell commands. Start the terminal
client on the master host:

```bash
dstool blobsan-service client --console-sock /tmp/blobsan.sock
```

The client needs only the socket, without `--db`, credentials or cluster access.
It displays pools, groups, layout histograms, workers and candidate BlobId lists.
Enter on a pool or group opens its histogram. The **Candidates** tab shows
Group, Category, BlobId, first/last observation ages, captured VDisk indexes and outdated/stale inventory markers.
Select `inv1_candidate`, `inv2_candidate` or all categories. The default scope follows a
selected group when available; uncheck **Selected group when available** to
show all groups. **Previous** and **Next** browse pages of up to 1000 entries.
An overlapping candidate occupies one entry in each category. Enter on a
candidate opens its group histogram.

The **Restore** tab selects `inv1` (`inv1_candidate`, the default) or `inv2`
(`inv2_candidate`). Its preview shows the number of active candidate BlobIds,
their total logical size from BlobSize, and the number of groups. It covers
all groups by default; **Selected group** limits it to the
group opened from Groups or Candidates. Outdated candidates and stale group
inventory are excluded and counted separately. **Preview** or **F5** refreshes
these totals without issuing restoration requests.

With automatic restoration disabled, **Restore** starts one background job
on the master for the previewed selection. With `master --restore` enabled,
this button is disabled and the tab displays automatic progress instead.
For a manual job, if the eligible list or group generation changed, refresh
the preview before
starting. The master uses its own endpoint and credentials, verifies each
group generation, then sends one `/get_blob` request per eligible
candidate with `groupId`, the full `blob` ID, `mustRestoreFirst=1` and `binary=1`.
Requests run sequentially and are never automatically retried. The response
must be HTTP 200 with binary data of exactly BlobSize bytes; HTTP 204 is
reported as absent. The tab reports successful requests, bytes, absent blobs,
failures and candidates skipped because their inventory or membership changed.
Only one restore job can run at a time. Closing or reconnecting the client
does not stop it. Progress persists in SQLite; a master restart marks an
unfinished job interrupted, with a possibly unknown last request outcome.

After a job, its groups are scheduled for another scan. Candidate lists remain
active until scans reevaluate them. A successful MustRestoreFirst request does
not guarantee that `inv2_candidate` becomes false: `TEvGet` can succeed with one
replica in each of three realms, or two replicas in each of two realms.
Workers issue no restoration requests; with `master --restore`, the master's
automatic scheduler uses the separate `/restore_part` workflow described above.

The active view and statistics refresh every five seconds (`client --refresh
SEC`). **F5** refreshes immediately, **[** / **]** switch tabs, **Ctrl+L** focuses
the command input, and **Ctrl+Q** exits. Buttons pause/resume assignment,
schedule all groups and refresh inventory. Pause/resume also controls new
automatic restore batches. Raw command responses appear in the
Console tab; `quit` exits the client.

After connection loss the client retains displayed data, marks it stale and
reconnects on the next refresh. Requests have a ten-second deadline (`client
--request-timeout SEC`). Failed interactive commands are never retried
automatically because their effect may already have been applied.

| Command | Effect |
|---|---|
| `stats` | Discovery, scan/candidate totals, live workers, stale inventory, missing VDisks and automatic restore progress |
| `groups` / `groups brief` | Group scan time, candidate count, coverage, owner, scheduling and errors; `brief` includes pool identity/generation and omits full reports |
| `layouts GROUP` | Latest successful histogram, coverage, stream errors, timestamps, generation and latest attempt error |
| `pools [POOL]` | Histograms summed over latest group reports; select by pool name or `BoxId:StoragePoolId` (`static` for static groups) |
| `workers` | Known workers and last contact timestamps |
| `candidates CATEGORY [GROUP\|all] [OFFSET] [LIMIT]` | Candidate BlobIds; CATEGORY is `all`, `inv1_candidate` or `inv2_candidate`; default offset 0, limit 1000, maximum limit 1000 |
| `restore-preview CATEGORY [GROUP\|all]` | Eligible blob count, logical bytes, excluded candidates, a preview token and automatic progress; CATEGORY is `inv1_candidate` or `inv2_candidate` |
| `restore CATEGORY GROUP\|all PREVIEW_TOKEN` | Start a manual background restore for the unchanged preview; unavailable with `master --restore` |
| `restore-status` | Latest manual restore job progress and up to 20 errors |
| `pause` / `resume` | Stop/resume new scan assignments and automatic restore batches; running operations finish |
| `scan [GROUP\|all]` | Make a group/all groups eligible for scanning now |
| `interval SECONDS` | Set the clean-group rescan interval |
| `retry SECONDS` | Set the delay after candidates or scan errors |
| `refresh` | Refresh group topology |
| `help` / `quit` | Show commands / disconnect |

For example, `candidates inv1_candidate 2147483649 0 100` returns the first 100 `inv1_candidate`
entries for group 2147483649. Its JSON object contains `category`, `group`
(null for all groups), `offset`, `limit`, `total` and `rows`. Each row has `gid`,
`category`, `blob`, `first_seen`, `last_seen`, `coverage`, `generation`,
`outdated` and `inventory_stale`. Raw timestamps are
Unix timestamps; the dashboard displays ages. Histograms show aggregate
counts, while this command and the Candidates tab expose individual BlobIds.

Intervals and pause state persist in SQLite. `--interval` (default 3600 seconds)
and `--retry` (60) initialize a new database; use the console afterward. Changed
intervals apply when scheduling the next completed job; `scan all` also resets
previously scheduled due times. The socket retains its plain-text
request/response framing for other clients.

## Scheduling and persistence

One worker scans one group at a time. The master avoids concurrent jobs sharing
storage nodes. All VDisks of one group can stream concurrently. Master SQLite
stores topology, leases, schedules, per-category candidate lists and latest
reports, plus automatic repair requests, raw responses and attempt history.
Workers persist identity and unacknowledged results locally.

Snapshot files for the last job remain in `DB/scan` and are replaced by the
next job. `snapshot` is the assembled input, `blobsan.err` is analyzer stderr,
`manifest.json` records the group and successfully completed streams, including
empty streams, and `report.json` contains the histogram. Resource usage depends
on group size, the external analyzer and candidate lists.

Jobs have renewable leases (`master --lease`, default 120 seconds). Failed
workers' jobs become available after expiry. Claim retries return the same
assignment; duplicate results are acknowledged without applying twice;
expired/reassigned results are rejected. Worker heartbeat interval defaults
to 20 seconds and is capped at one third of the granted lease. Loss of contact
cancels the scan. The master is a single coordinator without automatic failover.

`master --refresh` controls topology discovery (300 seconds). Inventory older
than twice this interval is not assigned. Generation changes invalidate old
jobs. Workers verify group generation before and after scanning with fresh
Viewer requests limited to the group's storage nodes (`node_id`) and filtered
by `GroupID`. Geometry must be 3 realms x 3 domains x 1 VDisk.

Static groups are discovered through Viewer; unavailable VDisks may be absent
from inventory and are reported missing. At least one VDisk permits an
observation. Dynamic virtual/blob-depot groups are excluded.

The API listens on all interfaces by default (`--bind` changes this). Optional
`--secret-file` on master/workers enables shared-secret authentication. The
master API is HTTP, not TLS; use a trusted network. Workers never send cluster
tokens to the master.

## Scan and candidate semantics

Workers download each `vdisk_stream` until it returns an empty response.
HTTP failures and explicit `ERROR` responses discard that VDisk's stream.
Requests are never retried within a stream because its cursor may already have
advanced. Workers add the same group/index/length framing as `dstool group
take-snapshot`, without decoding payloads.

At least one complete stream is required for an observation. No complete
streams means failure and preservation of the previous report. Empty streams
are valid empty indexes identified explicitly by the manifest. Captures across
VDisks are not atomic. Candidate evaluation requires three complete VDisk
streams in each of at least two realms.

The assembled snapshot is stdin to:

```bash
blobsan --restore-canidates --scan-manifest manifest.json --scan-report report.json mirror-3-dc
```

One invocation supplies both categories. Stdout goes to `/dev/null`, which also
disables verbose analyzer output. Stderr is retained and classified candidate
lines are parsed. The analyzer alone decides candidate membership. The service
validates report schema, coverage and histogram totals. A nonzero exit, timeout
or malformed candidate line fails the pass. Cancellation or lease loss
terminates the analyzer. Transport completion plus analyzer success is not an
independent proof of complete or consistent metadata.

The master tracks each category independently. Failed scans preserve existing
lists. Insufficient-coverage scans publish observations without clearing
previous candidates. A later evaluated scan removes a previous candidate only
when it covers the same VDisks as that candidate's last observation or all nine
VDisks. A disappearing candidate may have been garbage-collected; disappearance
does not prove replication. Groups with candidates or missing VDisks use the
retry interval. A candidate can leave `inv1_candidate` while remaining in
`inv2_candidate`. Retained lists can reflect different observation times and
coverage; their membership need not preserve that subset relation between scans.

## Layout reports

`layouts GROUP` includes `scan_report` version 1 with `group_id`,
`completed_vdisks`, `buckets` and `totals`. Each bucket has `local`, `ingress`,
`gc` and four counters:

| Counter | Meaning |
|---|---|
| `blobs` | Unique BlobIds after merging duplicate index records |
| `logical_bytes` | BlobSize once per blob |
| `observed_replica_bytes` | BlobSize per observed correctly placed local part |
| `missing_metadata_blobs` | Blobs with no index record on at least one captured VDisk |

Annotations are `phantom_like`, `inv1_candidate` and `inv2_candidate`. Buckets with
different annotations remain separate even if Local, ingress and GC match.
`phantom_like` means a captured index lacks metadata for this BlobId.
Both candidate annotations include the analyzer's GC, metadata, restorability
and failure-model checks. `inv1_candidate` tests whether the minimum local replica count
among fully captured datacenters is below one; `inv2_candidate` tests below two.
Unknown (`?`) realms are ignored. With no known realm, both flags are false.
Older reports
without new annotations display unknown. The legacy `write2x2_candidate` flag
is not treated as either new category.

Local and ingress strings retain group failrealm order. `local=1+1+?` means
one copy in each of the first two realms and incomplete coverage of the third.
A realm is `?` when any of its three VDisks is missing. `0` means every index
was captured but no correctly placed local part was observed. Ingress reflects
merged metadata claims, including unavailable VDisks, rather than confirmation
of local data. Misplaced parts do not count as replicas.

The histogram groups by Local2. Local3 sorts the three local counts descending,
with `?` last; Local2 drops its last count. For example, `1+2+1` and `1+?+2`
both yield Local2 `2+1`. Enter opens original Local, Local3, ingress and GC
breakdowns, replica size and missing metadata. Sizes use B/KiB/MiB/GiB as needed.

The default GC filter selects `soft_with_keep` and `no_covering_barrier`.
The selector also offers individual states and all states. **Show phantom_like**
controls phantom buckets. **Show inv1_candidate only** and **Show inv2_candidate only**
select the corresponding explicit annotations; enabling both selects their
intersection. Filters apply before Local2 aggregation and to the collapsed GC
summary. Annotation cells show `yes`, `no`, `mixed (N/total)` or `unknown`.

Local counters precede GC discards. GC states have this precedence:

| `gc` | Condition |
|---|---|
| `hard_barrier` | An observed covering hard barrier |
| `do_not_keep` | DoNotKeep set |
| `soft_without_keep` | An observed covering soft barrier without Keep |
| `soft_with_keep` | An observed covering soft barrier with Keep |
| `no_covering_barrier` | No accepted covering barrier |

These labels describe metadata rather than proof of current existence or
completed garbage collection. Candidate mode accepts barriers without quorum.
Filtered blobs remain in histograms. Blobs absent from every captured index
cannot be inventoried.

Each group's latest successful report is retained independently of its latest
attempt. Failure is shown as `latest_error`. `pools` sums the latest histogram
once per group and reports full/partial, never scanned, outdated, stale and
failed group counts plus scan ages. Reports with changed generation/topology
or stale inventory are excluded. Other old reports remain visible with their
timestamps. Pool identity is BoxId/StoragePoolId, keeping duplicate names in
different boxes separate. Cached inventory without pool IDs is isolated per
group under `unknown:GROUP` until rediscovery. Failrealm positions refer to each
group's topology, rather than global datacenter names.
