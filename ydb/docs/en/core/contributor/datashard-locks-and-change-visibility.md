# DataShard: locks and transaction change visibility

When a long-running YQL transaction writes data to tables, it may try to read the same table later. To support observing data consistent with transaction changes [DataShard](https://github.com/ydb-platform/ydb/tree/main/ydb/core/tx/datashard) tablets support writing [uncommitted changes](localdb-uncommitted-txs.md) as part of a transaction, include these changes in subsequent queries by the same transaction, and allow atomic commits of these changes as long as [serializable isolation](../concepts/transactions.md#modes) is not violated.

The underlying LocalDB support also allows very large transaction commits, not limited by the size of a single message between distributed actors.

## High level overview

Complex YQL transactions (either interactive, i.e. when client begins a transaction and uses it to perform queries without committing in the same query, or that involve multiple sub-queries) are split into multiple "phases" by [KQP](https://github.com/ydb-platform/ydb/tree/main/ydb/core/kqp), where output of one phase potentially acts as input to the next phase. For example, when a YQL query contains a `JOIN`, the first phase may be reading the first table, and the second phase may use the output of the first table to perform lookup queries in the second table.

For read-only queries KQP uses global [MVCC](https://en.wikipedia.org/wiki/Multiversion_concurrency_control) snapshots to ensure consistency between sub-queries. But when transaction also writes, it needs to ensure [serializable](https://en.wikipedia.org/wiki/Serializability) isolation is not violated at commit time. Currently, this is achieved using [Optimistic concurrency control](https://en.wikipedia.org/wiki/Optimistic_concurrency_control), where reads add optimistic "locks" to observed ranges, and writes by other transactions "break" those locks at their commit time. Transaction may successfully commit as long as none of those locks are broken at commit time, otherwise it fails with a "transaction locks invalidated" error.

There's another way to look at optimistic locks. A single transaction may read from multiple shards using read timestamps (this may be a single global MVCC snapshot timestamp, or multiple timestamps, different for each read), while other transactions concurrently write to the same tables or shards. When transaction commits, it is assigned a single commit timestamp in the global serializable order of execution. As long as all of those reads could be repeated at the commit timestamp, without any change to observed results, the transaction might as well have executed (in its entirety) at the commit timestamp. The optimistic lock, as long as it's not broken, tells the transaction that it is possible to move all reads to the commit timestamp.

Uncommitted changes are not too different from reads in that regard. As long as DataShard can store those changes and then "move" them to the final commit timestamp without conflicts, the transaction may commit, otherwise it must abort. The main difference is that unlike read locks (which are stored in-memory), uncommitted changes are persistent, must be tracked across reboots, and must be cleaned up correctly when no longer needed.

## How locks are used for reads

Operations proposed to DataShards are assigned a globally unique 64-bit `TxId`, which are allocated in large batches from global [TxAllocator](https://github.com/ydb-platform/ydb/tree/main/ydb/core/tx/tx_allocator) tablets. When KQP performs the first read in a multi-phase transaction, it also uses this `TxId` as a lock identifier (historically named `LockTxId`), which is then used in all subsequent queries in the same YQL transaction. DataShard will add new locks when operation has `LockTxId` specified (it is not zero):

* See [LockTxId](https://github.com/ydb-platform/ydb/blob/3444af692d32224288c41ba8c21e416d5fd4996c/ydb/core/protos/tx_datashard.proto#L1612) field in `TEvRead` read requests
* See [LockTxId](https://github.com/ydb-platform/ydb/blob/bcf764f1aa71683e3871616abe6f16b47cec42e4/ydb/core/protos/data_events.proto#L83) field in `TEvWrite` write requests
* See [LockTxId](https://github.com/ydb-platform/ydb/blob/3444af692d32224288c41ba8c21e416d5fd4996c/ydb/core/protos/tx_datashard.proto#L282) field in `TDataTransaction` messages (used for encoding data transaction body)

You may also see the `LockNodeId` field, which specifies the originating node id of the lock, which is used by DataShard for subscribing to lock status, and cleaning up locks when they are no longer needed.

Note that `LockTxId` is just a unique number, that is used across multiple operations in the same YQL transaction, while `TxId` is unique for every operation for a single DataShard. The use of the first `TxId` as `LockTxId` is not required, but since KQP already has a globally unique number, and `LockTxId` is unrelated to `TxId`, it elides an extra allocation.

Locks table (see [datashard_locks.h](https://github.com/ydb-platform/ydb/blob/main/ydb/core/tx/datashard/datashard_locks.h) and [datashard_locks.cpp](https://github.com/ydb-platform/ydb/blob/main/ydb/core/tx/datashard/datashard_locks.cpp)) indexes locks by their primary key ranges in a range tree, allowing finding and "breaking" them by point keys. In the simplest case when read operation reads a range it is added using a [SetLock](https://github.com/ydb-platform/ydb/blob/207ac81618e05ade724a8a8193bc9125d466bd06/ydb/core/tx/datashard/datashard_locks.h#L831) method, and when write operation writes a key it breaks other locks using a [BreakLocks](https://github.com/ydb-platform/ydb/blob/207ac81618e05ade724a8a8193bc9125d466bd06/ydb/core/tx/datashard/datashard_locks.h#L834) method.

When the lock is added for the first time, it is assigned a monotonically increasing `Counter` in the current tablet's `Generation` (see [TLock](https://github.com/ydb-platform/ydb/blob/207ac81618e05ade724a8a8193bc9125d466bd06/ydb/core/protos/data_events.proto#L8) message), and a row with these numbers is added to the virtual `/sys/locks` table (which is no longer used). These locks are then returned in result messages (e.g. see [TEvReadResult](https://github.com/ydb-platform/ydb/blob/b07264456a2e8b5929901f258ad60399bb64678a/ydb/core/protos/tx_datashard.proto#L1702)). 

In the successful scenario, the lock exists and is not broken, `Generation` and `Counter` fields do not change. 

In the unsuccessful scenarios, previously acquired locks are broken. For example, when changing the `Generation` of the lock (disabling the lock on restart) or the `Counter` (on an explicit error status, or when disabling and recreating the lock in the same generation).

The first `Generation` and `Counter` for each shard are remembered by KQP, and changes during transaction lifetime are indicative of possible inconsistencies and serializable isolation violations. Read-Write transactions, or transactions that did not use global MVCC snapshots for efficiency reasons, perform the final commit that validates `Generation`/`Counter` values and commit only succeeds when all of them match.

Reads using global MVCC snapshots are already consistent. Nevertheless, they still acquire locks in case transaction might perform a write later in the lifecycle.

When acquiring locks DataShard performs additional checks, on whether or not conflicting changes have been committed "above" the snapshot. When the conflict is detected, the read succeeds, however the lock will have `Counter` equal to [ErrorAlreadyBroken](https://github.com/ydb-platform/ydb/blob/b07264456a2e8b5929901f258ad60399bb64678a/ydb/core/tx/datashard/sys_tables.h#L107), to signal that even though the read is consistent, writes will never succeed. Such transactions may stop trying to add new locks, and succeed when it turns out the full YQL transaction is read-only. When such transactions try to write, however, they are aborted early as it would be impossible to commit them anyway.

## How locks are used for writes

When KQP needs to make uncommitted changes in a YQL transaction, it uses DataShard write transactions with a non-zero `LockTxId`. DataShard will then use this `LockTxId` as `TxId` for persisting uncommitted changes, available as long as the lock is valid. Internally locks that have uncommitted writes are called write locks. Such locks also become persistent (see the [Locks](https://github.com/ydb-platform/ydb/blob/5aecdb67595db1a47c933b7d8da2cb662a50e185/ydb/core/tx/datashard/datashard_impl.h#L879) table and related tables below), surviving DataShard restarts.

There are some limitations to such uncommitted write transactions:

* Transaction must run in an immediate transaction mode (i.e. uncommitted writes cannot be distributed, uncommitted writes to different shards are performed independently instead)
* Transaction must have a valid `LockNodeId` specified, DataShard subscribes to lock status using this node and automatically rolls back uncommitted changes when the lock expires (e.g. when transaction aborts unexpectedly, node is restarted and transaction state is lost, etc.)
* Transaction must have a valid MVCC snapshot specified, which is used as the conflict detection baseline (and reads when needed), and expected to be used across all reads and writes in the same YQL transaction.
* The specified lock must be valid and non-broken, otherwise the specified `LockTxId` must not have any uncompacted data in LocalDB. This protects against edge cases where transaction rolls back due to lock status failure, and KQP tries writing to the shard again.

When the YQL transaction later reads using the same `LockTxId`, reads will use a per-query transaction map with [LocalDB](localdb-uncommitted-txs.md), where the `LockTxId` appears as if it's already committed, allowing transaction to observe its own changes, but not other uncommitted changes. Since reads are performed using an MVCC snapshot, the transaction map will have a special entry `[LockTxId] => v{min}`, so the uncommitted change is visible in all snapshots.

Uncommitted writes need additional conflict detection (see [CheckWriteConflicts](https://github.com/ydb-platform/ydb/blob/efe5b5f8d2da503eda4d172f6f2e85aac64ba6a6/ydb/core/tx/datashard/datashard__engine_host.cpp#L802) in the MiniKQL engine host implementation). When multiple uncommitted transactions write to the same key, DataShard needs to ensure correctness by breaking conflicting transactions. Transaction observer objects (e.g. [TLockedWriteTxObserver](https://github.com/ydb-platform/ydb/blob/efe5b5f8d2da503eda4d172f6f2e85aac64ba6a6/ydb/core/tx/datashard/datashard__engine_host.cpp#L872)) are used to detect these conflicts, where LocalDB calls back various interface methods whenever a change is skipped or applied during reads, and a special read is used before each write to detect other uncommitted writes to the same key. Whenever a conflict is detected, it is added to conflict sets between locks, so each lock remembers which other locks must be broken when it commits, and which locks will break this lock when they commit.

Reads also need additional checks for conflicts when uncommitted writes are involved. Because reads need to not only be internally consistent, but also match the eventual state at the commit timestamp. Transaction observers are used to gather uncommitted writes from other transactions (reads need to be able to eventually move to the commit timestamp), which introduce conflict graph edges from write locks to read locks.

Even more complicated is the case where change visibility writes happen over committed changes after the transaction MVCC read snapshot, and transaction reads rows including those uncommitted changes. For example, let's consider this case:

1. Key K initially has `A = 1` at version `v4000/100` (the two numbers in version are `Step=4000` and `TxId=100` of the commit timestamp)
2. Tx1 is started with an MVCC snapshot `v5000/max`
3. Tx2 commits a blind `UPSERT` with `B = 2` at version `v6000/102`
4. Tx1 performs an uncommitted blind `UPSERT` with `C = 3` and `TxId` 101
5. At this point Tx1 may still commit successfully, because it didn't read key K and change with `C = 3` may still move to some future commit timestamp
6. Tx1 performs a read of key K, which happens at snapshot `v5000/max`:
    * This read will have `[101] => v{min}` in its custom transaction map
    * The iteration will be positioned at the first delta with `C = 3` (since `v{min} <= v5000/max`), which will be applied to the row state
    * All other committed deltas and rows will also be applied, i.e. the row state would include `B = 2` which is currently committed
    * However, there is a conflict: `B = 2` is committed above the MVCC read snapshot (`v6000/102 > v5000/max`)
    * This will be detected in [OnApplyCommitted](https://github.com/ydb-platform/ydb/blob/efe5b5f8d2da503eda4d172f6f2e85aac64ba6a6/ydb/core/tx/datashard/datashard__engine_host.cpp#L698) callback, which calls [CheckReadConflict](https://github.com/ydb-platform/ydb/blob/efe5b5f8d2da503eda4d172f6f2e85aac64ba6a6/ydb/core/tx/datashard/datashard__engine_host.cpp#L751)
    * Since this introduces a read inconsistency, the lock will be immediately broken, and an inconsistent read flag will be raised
7. Not only the above read would fail, Tx1 would not be able to commit since serializable isolation can no longer be provided
8. The application will get a "transaction locks invalidated" error and retry the transaction from the beginning

## Interaction with change collectors

DataShards may have [change collectors](https://github.com/ydb-platform/ydb/blob/bcf764f1aa71683e3871616abe6f16b47cec42e4/ydb/core/tx/datashard/change_collector.h#L48), which log table changes and stream them to other subsystems. This is used to support [Change Data Capture](https://en.wikipedia.org/wiki/Change_data_capture) and [Asynchronous secondary indexes](../concepts/secondary_indexes.md#async). Depending on the mode, change collector may need to know the previous row state, and may perform a row read before each write.

When transaction has uncommitted changes, change collectors need to process those as well, but those must not be streamed until they are committed, and moreover streamed changes must match the eventual commit order of those changes. Changes are first accumulated in a separate [LockChangeRecords](https://github.com/ydb-platform/ydb/blob/c5284478af104d82c4d84a5d99bf0a51ecd2ca63/ydb/core/tx/datashard/datashard_impl.h#L911) table, and when transaction eventually commits they are added to the output stream in bulk using a single record in a [ChangeRecordCommits](https://github.com/ydb-platform/ydb/blob/c5284478af104d82c4d84a5d99bf0a51ecd2ca63/ydb/core/tx/datashard/datashard_impl.h#L947) table. This way all change processing is done gradually, it matches the size of each individual write, and there is no expensive post-processing at commit time.

## Interaction with distributed transactions

When distributed transaction starts execution, it first validates locks and sends its validation result to other participants. When all successful validation results are received, the transaction body may execute and apply its intended side-effects. In other words, when all reads from all involved shards may successfully move to the commit timestamp, the transaction may execute all buffered UPSERTs, otherwise transaction body is not executed at all participants, and all accumulated changes are rolled back. Normally, validation result is persisted, and correctness is preserved by runtime key conflicts between transactions. However, uncommitted writes add a serious complication, since when the write lock is committed DataShard doesn't even know which keys are involved (it would need to keep all keys in memory, which is prohibitively expensive). Left unchecked, DataShard might validate the write lock, send a successful result to other participants, while another conflicting transaction breaks this lock and rolls back all changes. It is preferred to optimize for the case where transactions don't conflict, and stopping the pipeline when uncommitted writes are involved would be prohibitively expensive.

Instead, when the write lock is validated, it becomes "frozen", and cannot be broken anymore. When a conflicting transaction tries to break a frozen lock, it is temporarily paused, and waits until the transaction with that frozen lock is first resolved. DataShard tracks which locks break which other locks on commit, and to avoid potential deadlocks it also tracks when commit of a transaction A may break validation results for transaction B and vice versa. When A must go before B in the global order from coordinators, DataShard won't start transaction B validation until A completes, but as long as transactions don't conflict they may be executed out-of-order.

## Committing changes

When KQP needs to commit previously uncommitted changes, it proposes a transaction that commits previously acquired locks. Specifically this transaction must not have a `LockTxId` specified (the commit is not setting any new locks), and must include a previously set lock in [Locks](https://github.com/ydb-platform/ydb/blob/b07264456a2e8b5929901f258ad60399bb64678a/ydb/core/protos/tx_datashard.proto#L219) transaction field, and with Op set to [Commit](https://github.com/ydb-platform/ydb/blob/d31477e5ed13679dd3b409b100623cc81a5e0964/ydb/core/protos/data_events.proto#L26). The commit transaction may either be immediate (when the YQL transaction involves a single shard, whether or not multiple phases have been used), or prepared as a distributed transaction with multiple participants.

To support distributed transactions all shards that validate locks must be included in [SendingShards](https://github.com/ydb-platform/ydb/blob/d31477e5ed13679dd3b409b100623cc81a5e0964/ydb/core/protos/data_events.proto#L20), and all shards that have side-effects must be included in [ReceivingShards](https://github.com/ydb-platform/ydb/blob/d31477e5ed13679dd3b409b100623cc81a5e0964/ydb/core/protos/data_events.proto#L21). During validation sending shards will generate persistent ReadSets and send them to all receiving shards, and receiving shards will wait for all expected ReadSets before executing the transaction. When transaction is executed with all successful validation results it will commit the lock by calling [KqpCommitLocks](https://github.com/ydb-platform/ydb/blob/d31477e5ed13679dd3b409b100623cc81a5e0964/ydb/core/tx/datashard/execute_kqp_data_tx_unit.cpp#L228). Otherwise, transaction body will not be executed, and lock is erased with all uncommitted changes rolled back by calling [KqpEraseLocks](https://github.com/ydb-platform/ydb/blob/d31477e5ed13679dd3b409b100623cc81a5e0964/ydb/core/tx/datashard/execute_kqp_data_tx_unit.cpp#L190), and it cannot be retried later on commit failure.

{% note info %}

Note that all uncommitted changes with the same `LockTxId` must be included in a commit transaction, and transaction must never try to partially commit. For example, when a transaction involves multiple writes, and one of those writes fails with an error, it would not be correct to skip the failed write and partially commit. Shards might merge later and non-matching transaction status would lead to consistency anomalies.

{% endnote %}

The commit transaction may also have additional side-effects, which are atomically executed after the lock is committed. KQP will try to accumulate side-effects in memory until the same table is read in the same transaction, or until transaction commits, to reduce latency and fuse side-effects with commit as much as possible. When it is possible to accumulate all side-effects in memory, no uncommitted changes are persisted, and only read locks are optionally acquired.

When YQL transaction needs to rollback it runs an empty transactions with the [Rollback](https://github.com/ydb-platform/ydb/blob/d31477e5ed13679dd3b409b100623cc81a5e0964/ydb/core/protos/data_events.proto#L27) lock op. Even if it didn't, when the [TLockHandle](https://github.com/ydb-platform/ydb/blob/0db14d1168517ecacab106e7abfe7af663020829/ydb/core/tx/long_tx_service/public/lock_handle.h#L15) is destroyed, subscribed DataShards will clean it up automatically in their [TxRemoveLock](https://github.com/ydb-platform/ydb/blob/0db14d1168517ecacab106e7abfe7af663020829/ydb/core/tx/datashard/remove_locks.cpp#L24) internal transaction. The explicit removal is preferred, since uncommitted changes is a finite resource and asynchronous cleanup via `TLockHandle` would not ensure that resource is freed before new transactions try to write more uncommitted changes.

## Limitations

Currently, uncommitted changes have a downside that all new writes have to search for conflicts. A single uncommitted write is enough to cause all new writes at a specific DataShard to switch to become non-blind, i.e. every write will have to perform a read first, which makes them more expensive, increases latency and decreases throughput. For maximum performance it is recommended to execute transactions where all reads are performed first, and a small amount of blind writes are performed last. This way uncommitted writes will not be used and DataShard performance will be optimal.

Due to LocalDB limitations DataShard also needs to ensure it doesn't accumulate too many open transactions, and the locks table is already limited to ~10k locks, which includes write locks. DataShard also has to count the number of uncommitted changes before each uncommitted write, which is implemented by counting skips in the transaction observer, and throwing [TLockedWriteLimitException](https://github.com/ydb-platform/ydb/blob/d31477e5ed13679dd3b409b100623cc81a5e0964/ydb/core/tx/datashard/datashard__engine_host.cpp#L867) when the limit is exceeded.

Persistent locks survive DataShard reboots and restore the last state. Even though it is possible to persist specific ranges, this is not used in practice (DataShard would need to persist ranges during reads, which is expensive), and read locks are restored in the worst case "whole shard" as their range.
