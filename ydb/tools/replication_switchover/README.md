# Manual async-replication switchover

`manual_switchover.py` swaps the roles of two clusters that are linked by async replication without
copying the data again, and rehearses it: it flips a pair back and forth under writes and checks that
every acknowledged write is on the new primary and that writes on the new primary reach the new
replica.

One flip, with A the primary and B the replica holding the replication object `R`:

1. Read `R`'s description on B and record the changefeed path of every target, index targets included.
2. Demote every replicated table on A, index implementation tables included, with an internal
   `AlterTable` (`ReplicationConfig { Mode: READ_ONLY, ImportState: IMPORTED }`) sent through
   `ydbd db schema exec`. Writes on A are rejected from here on; reads work.
3. Wait until the `replicationConsumer` of every changefeed has committed the end offset, stable
   across three polls (GLOBAL consistency additionally waits for the replication's transaction list
   to be empty).
4. `ALTER ASYNC REPLICATION R SET (STATE = 'DONE')` on B and wait until the tables are writable.
5. Compare row digests of both sides.
6. Drop the changefeeds of the previous direction on B, by the names recorded at the previous flip.
7. `CREATE ASYNC REPLICATION R2 FOR <B table> AS <A table> WITH (INITIAL_SCAN = FALSE, ...)` on A.
8. `ALTER ASYNC REPLICATION R2 SET (STATE = 'STANDBY')` on A: the demoted tables are attached.
9. Write a probe row on B and read it on A.
10. `DROP ASYNC REPLICATION R FORCE` on B.

Requirements: both clusters on a build with `TRemoteTopicReaderSettings.SourceIdPrefix` (a table that
becomes a replica again would otherwise skip the new stream's records), the legacy gRPC service on at
least one node of each cluster, row tables with at most synchronous and unique indexes, no TTL, no
DDL on the tables during the flip. `STATE = 'DONE'` does not wait for anything by itself; the fence
and the drain of steps 2 and 3 are what make it safe.

Example against the sandbox pair started by `ydb-clusters`:

```
YDBD=.../ydb/apps/ydbd/ydbd ydb-clusters exec -- python3 manual_switchover.py run --flips 4 --index
```
