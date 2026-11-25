# Stand experiments and evidence

Use this workflow for requested YDB workloads under Nemesis. Read operations.md for the stock application's API; the checks below also apply to custom injectors.

## Reuse or prepare the stand

1. Read the existing run manifest, config, launcher, and result index before changing a saved stand. Record the current source SHA, binary identity, endpoints, database paths, service names, ports, storage layout, and fault profile in the new run's artifacts. Treat old addresses and process IDs as historical observations.
2. Distinguish a standard slice from several ydbd processes on one VM. Stock runners use conventions such as kikimr and kikimr-multi@ services; a custom service layout needs a matching adapter or explicitly scoped process injector. See ydb/tests/stability/nemesis/internal/nemesis/runners/local_kikimr.py and target_payload.py in the same directory.
3. For a custom adapter, enumerate only the intended actions and validate inventory without injecting faults. Use actual endpoints and runtime IDs; unknown dynamic node IDs must not become the static node ID. Keep the adapter's additional API separate from the stock API.
4. Record shared failure domains. A stand with one static node and four compute processes on one VM is five processes, not five independent hosts. A shared PDisk with erasure=none cannot establish multi-host storage-failure tolerance.
5. Before a long run, complete a fault-free write/read baseline, one scoped fault with recovery, then a short campaign. Verify that the intended fault happened using execution results and observed tablet generation or process PID changes. Do not count successful HTTP requests as fault coverage.

## Workload and oracle

1. Use a unique run namespace and deterministic keys/values. Keep an append-only client journal with transaction/batch ID, affected keys, expected values, timestamps, and acknowledged commit outcome. Separate acknowledged, aborted, and uncertain operations; a timeout does not prove rollback.
2. Define the contract before interpreting mismatches: eventual catch-up, final equality, or transaction visibility. For replication, distinguish initial scan from CDC and ROW from GLOBAL consistency. Record source and destination table types; row-table results do not cover ColumnShard.
3. For a finite dataset, replay the journal independently and compare complete ordered keys and values, including deletions and unexpected rows. Row counts and green wardens do not establish content equality. For larger workloads, specify what a digest or sample does and does not prove.
4. To test GLOBAL visibility, write a known batch in one source transaction and read the destination in a transactional snapshot. Preserve the exact query, raw rows, truncation indicator, batch membership, and ACK/read timestamps for a partial result. Later convergence disproves permanent loss for those rows, but does not erase the earlier visibility failure.
5. Stop writes and faults, allow bounded catch-up, then audit through fresh reads/connections. Retain the original failure even if a retry succeeds. A failed checker query or resource error is not itself evidence of lost data.

## Fault scope and completion

1. Map tablet IDs back to this run's tables before targeted restarts, and refresh after splits or schema changes. A type-wide tablet killer can hit old tables or another database. Report distinct targets actually affected, not only a total restart count.
2. For a process fault on a shared VM, select the exact stand-owned service and verify its PID before and after recovery. Broad process-name kills can affect unrelated builds and stands. Count ordinary restart and SIGKILL separately.
3. Apply the deadline, cleanup, and recovery sequence in [operations](operations.md) to the stand's supported control API.
4. Keep run state and logs durable enough to resume observation after a disconnected client. When asked to run to completion, continue checking until the requested duration, shutdown, and final audit finish; a detached process is only a launch result.
5. On an invariant failure, stop further injections, retain affected objects and raw evidence, and diagnose the checker and system separately. Cleanup may remove only this run's objects and services after evidence is saved and cleanup is within scope.

## What to preserve and report

1. Keep a result index linking configuration, exact commands, workload journal, fault history, raw failed responses, YDB/Nemesis logs, health snapshots, and the independent audit.
2. Report duration actually completed, acknowledged operations, successful faults by kind and target, recovery time, audit result, and unfinished coverage. Separate harness errors, unresolved observations, and reproduced product failures.
3. State the workload and topology limits of a successful run; it does not prove all failure modes safe.
