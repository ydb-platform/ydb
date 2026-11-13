# Manual async-replication switchover

`manual_switchover.py` swaps the roles of two clusters that are linked by async replication without
copying the data again, one step per command, and rehearses the whole procedure back and forth under
writes on a test pair.

## Flip

A is the primary, B holds the replicas and the replication object `R` that reads A. A flip ends with
B as the primary and a new object on A that reads B. Every step is recorded in a state file and is
safe to repeat; `flip` runs the steps the state file does not record as done, so an interrupted flip
is resumed by running `flip` again.

```
manual_switchover.py --state-file sw.json init \
    --a-endpoint grpc://a:2135 --a-database /a --b-endpoint grpc://b:2135 --b-database /b \
    --primary A --replication /b/R --table t1 --table t2
manual_switchover.py --state-file sw.json flip
```

| Step | Command | What it does |
| --- | --- | --- |
| 0 | `preflight` | `R` is running without issues; no table has TTL or a vector index; the changefeed path of every target (index targets included) is recorded; the consistency level is read from `R`. |
| 1 | — | **Close writes to A with your external fence.** The write outage starts here. Nothing in the database closes them at this point, so this step is the whole guarantee that no write is accepted that the new primary will never see. |
| 2 | `drain` | Every changefeed consumer has committed its end offset: three polls in a row with unchanged offsets (ROW); for GLOBAL, three successful polls after two commit intervals, then the row digests of both sides must be equal and stay equal for one more interval (staged rows are invisible until their interval commits, so equal digests prove the last interval was committed). |
| 3 | `promote` | `ALTER ASYNC REPLICATION R SET (STATE = 'DONE', FAILOVER_MODE = 'CONSISTENT')` on B; waits until B's tables are writable, clearing replica mode by hand if `DONE` stalls; then compares row digests of both sides. B is writable from here but **not yet safe to write**: see step 5. |
| 4 | `cleanup-changefeeds` | Drops the changefeeds of the previous direction, by the names recorded at the previous flip (an index implementation table's changefeed through the Table API addressed by the index path, as the replication controller does). |
| 5 | `create-reverse` | `CREATE ASYNC REPLICATION R<n> FOR <B tables> AS <A tables> WITH (ADOPT_EXISTING = TRUE, ...)` on A; waits for `running`. One statement demotes every table of A, and the implementation tables of its synchronous and unique indexes, to a read-only replica in place and starts reading B -- nothing is copied, and the mode, consistency level and import state come from the replication's own configuration. It needs a cluster administrator on A. **This is what closes the loss window**: until it reports running there is no changefeed on B, so a write accepted there would reach A by no path at all. It cannot be done earlier -- a changefeed cannot be created on a table that is still a replica. |
| 6 | `probe` | One row per table written on B and read on A, over a connection that bypasses the fence. Doing it before traffic resumes means a broken reverse direction costs a few more seconds of outage instead of silent divergence under load. |
| 7 | — | **Reopen writes on the new primary.** The write outage ends here. |
| 8 | `drop-old` | `DROP ASYNC REPLICATION R FORCE` on B; the state file now says B is the primary. Its feeds stay on A's tables, to be cleaned at the next flip. |

The write outage is step 1 through step 6, and `create-reverse` reports it. The promote is in the
middle of it, not the end: a changefeed captures only changes made after it is created and an
adopted replica takes no initial scan, so anything written on the new primary before the reverse
replication is running is in neither the feed nor the replica's data, and no later step carries it.

Nothing in either database changes before `promote`, so abandoning a flip before it means simply
reopening the fence. After the promote you must go forward: keep writes closed and finish
`create-reverse` and `probe`. `status` prints the state file and the clusters' view.

## Seeding from a backup

The replica side can be created without a CDC initial scan, from an S3 backup, which is the
documented procedure for large tables (`INITIAL_SCAN = FALSE`, export on the primary, import into
the placeholders, `STANDBY`):

```
manual_switchover.py --state-file sw.json --s3-endpoint https://storage.example.net --s3-bucket mybucket \
    --s3-access-key ... --s3-secret-key ... seed [--prefix migration-2026-11]
```

`seed` creates the replication object on the replica side, waits until every changefeed and
placeholder exists (the backup must be taken after the changefeeds, or the changes in between are
lost), runs `ydb export s3` on the primary and `ydb import s3 --index-population-mode Build` on the
replica side, attaches with `STANDBY` and waits until the replica equals the primary. With the S3
options given, `reseed` after a failover uses the same path instead of an initial scan, and
`rehearse --seed backup` rehearses the whole procedure from a backup.

## Unplanned failover

```
manual_switchover.py --state-file sw.json failover     # on the surviving replica side: DONE, DROP FORCE
manual_switchover.py --state-file sw.json reseed       # drop its tables, re-create them with an initial scan
```

Everything the lost primary had committed that the replica had not applied is lost. The lost side
cannot be attached as it is: it holds writes the new primary never saw, and the new primary has been
taking writes with no changefeed for it. `reseed` is the only way back, after which `flip` works again.

## Rehearsal

```
YDBD=.../ydbd ydb-clusters exec -- python3 manual_switchover.py --state-file /tmp/sw.json rehearse \
    --tables 3 --partitions 2 --index sync --rows 200 --flips 4 --failover-after 2 [--consistency GLOBAL]
```

Creates the tables (every other one with the chosen index kind) and the replication, then flips back
and forth with a writer running until each fence, checks that every acknowledged row is on the new
primary and that writes on the new primary reach the new replica, and optionally rehearses the
unplanned failover after a flip. The downtime of each flip (fence to promote) is printed. On the
sandbox pair it is 10-13 s for small tables; the drain dominates, and a flip whose consumer commits
late (observed once at 53 s, GLOBAL, on the changefeed of a replication attached with
`INITIAL_SCAN = FALSE`) is bounded only by the drain's polling limit.

## Requirements

- Both clusters on a build with `TRemoteTopicReaderSettings.SourceIdPrefix`: a table that becomes a
  replica for the second time keeps the deduplication offsets of the earlier stream and would skip
  the new stream's records without it.
- A cluster administrator token (`--token` or `YDB_TOKEN`) for the side being adopted:
  `ADOPT_EXISTING` demotes tables in place, which SchemeShard admits only from a SID in
  `administration_allowed_sids`. Over a there-and-back cycle that means both clusters.
- An external write fence. The tool never closes writes itself; it assumes they are closed from
  step 1 to step 6 and cannot verify it.
- `--extra-create-settings` passes credentials into the `CREATE ASYNC REPLICATION` statements. The
  `ydbd` binary is used read-only, for `db schema describe`. Seeding from a backup needs an S3
  bucket reachable from both clusters (`--s3-*` options, or `S3_ENDPOINT`, `S3_BUCKET`,
  `S3_ACCESS_KEY`, `S3_SECRET_KEY`).
- Row tables; synchronous, unique and async indexes are fine; vector indexes and TTL are refused.
- No DDL on the tables during a flip.
- `STATE = 'DONE'` does not wait for anything by itself. The fence and the drain are what make it
  safe. With `EnableConsistentReplicationDone` on, a consistent `DONE` of a GLOBAL replication also
  waits for the controller's commit loop to drain (`rehearse --skip-global-tx-wait` relies on it).
