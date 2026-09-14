# LocalDB: persistent uncommitted changes

Tablets may need to save a large number of changes over a long period and then commit or abort them entirely. To support this scenario, [LocalDB](../concepts/glossary.md#local-database) allows marking uncommitted changes in tables with a unique 64-bit transaction identifier (`TxId`). This data is stored in the table alongside committed data but is not visible to other queries until commit. Committing or aborting a transaction is atomic and cheap, and over time this data is integrated into the table in the normal committed form. Thus, uncommitted data is not stored in memory for a long time and is not limited by its capacity.

This mechanism is used as the basis for the following functionality:

* Saving uncommitted changes in long YQL transactions and the ability to see these changes in the same transaction before they are committed.
* Saving effects in volatile distributed transactions with the ability to roll back.
* Cross-cluster consistent replication, where changes are streamed in small batches and periodically committed, making the next consistent snapshot of the source database visible.

## Limitations

This mechanism has the following limitations:

1. For a single key, changes can be made in parallel within different `TxId`, but these transactions must be committed in the same order in which the changes were written. For example, if there is a change for key K from transaction tx1 and then from transaction tx2, you can commit transaction tx1 and then tx2, and both changes will be visible. However, if you commit tx2 first, then tx1 must be aborted.
2. The number of uncommitted transactions and the amount of data in them for a specific key must be limited by higher-level layers.
3. `TxId` must be globally unique and must not be reused after a transaction commit or abort, including across different shards.

## Writing uncommitted changes

In the recovery log (see [flat_redo_writer.h](https://github.com/ydb-platform/ydb/blob/main/ydb/core/tablet_flat/flat_redo_writer.h)), there are the following events:

* [EvUpdateTx](https://github.com/ydb-platform/ydb/blob/ab6222f2deabf1a12b50db13728b68cbd6b59604/ydb/core/tablet_flat/flat_redo_writer.h#L160) saves changes to the table with the specified uncommitted `TxId`. This event is created by the [TDatabase::UpdateTx](https://github.com/ydb-platform/ydb/blob/ab6222f2deabf1a12b50db13728b68cbd6b59604/ydb/core/tablet_flat/flat_database.h#L123) method of the local database (LocalDB).
* [EvRemoveTx](https://github.com/ydb-platform/ydb/blob/ab6222f2deabf1a12b50db13728b68cbd6b59604/ydb/core/tablet_flat/flat_redo_writer.h#L169) is used to remove the specified `TxId` when a transaction is aborted. This event is created by the [TDatabase::RemoveTx](https://github.com/ydb-platform/ydb/blob/ab6222f2deabf1a12b50db13728b68cbd6b59604/ydb/core/tablet_flat/flat_database.h#L124) method of the local database.
* [EvCommitTx](https://github.com/ydb-platform/ydb/blob/ab6222f2deabf1a12b50db13728b68cbd6b59604/ydb/core/tablet_flat/flat_redo_writer.h#L183) is used to commit the specified `TxId` with the specified [MVCC](../concepts/query_execution/mvcc.md) commit version. This event is created by the [TDatabase::CommitTx](https://github.com/ydb-platform/ydb/blob/ab6222f2deabf1a12b50db13728b68cbd6b59604/ydb/core/tablet_flat/flat_database.h#L125) method of the local database.

## Storing uncommitted changes in MemTable

[MemTable](../concepts/glossary.md#memtable) in LocalDB is a small key-sorted tree of recently made changes that is stored in memory. The key in this tree is the table key, and the value is a pointer to a chain of changes for the corresponding key. For each change, the MVCC version of that change is specified (a pair `Step`/`TxId` with the global commit time). The rows in the change chain are merged within a single MemTable. For example, suppose we had the following operations for key K:

| Version | Operation |
| --- | --- |
| `v1000/10` | `UPDATE ... SET A = 1` |
| `v2000/11` | `UPDATE ... SET B = 2` |
| `v3000/12` | `UPDATE ... SET C = 3` |

Then the chain of rows with changes for this key in a single MemTable would look as follows:

| Version | Row |
| --- | --- |
| `v3000/12` | `SET A = 1, B = 2, C = 3` |
| `v2000/11` | `SET A = 1, B = 2` |
| `v1000/10` | `SET A = 1` |

In some cases, a MemTable may split between operations, which happens, for example, at the start of compaction. Then the MemTable from the example above would look like this:

| MemTable | Version | Row |
| --- | --- | --- |
| Epoch 2 | `v3000/12` | `SET B = 2, C = 3` |
| Epoch 2 | `v2000/11` | `SET B = 2` |
| Epoch 1 | `v1000/10` | `SET A = 1` |

New changes are applied to the current MemTable, even if they have not yet been committed. When writing, uncommitted changes are marked with a special version, specifying the maximum value for `Step` (as if implying a distant future) and the `TxId` of the transaction as `TxId`. Merging of lower-level changes is not performed. For example, suppose we then performed the following uncommitted operations for key K:

| TxId | Operation |
| --- | --- |
| 15 | `UPDATE ... SET C = 10` |
| 13 | `UPDATE ... SET B = 20` |

The chain of changes in the MemTable will now look like this:

| Version | Row |
| --- | --- |
| `v{max}/13` | `SET B = 20` |
| `v{max}/15` | `SET C = 10` |
| `v3000/12` | `SET A = 1, B = 2, C = 3` |
| `v2000/11` | `SET A = 1, B = 2` |
| `v1000/10` | `SET A = 1` |

During reading, for changes with version `Step == max`, the [iterator](https://github.com/ydb-platform/ydb/blob/main/ydb/core/tablet_flat/flat_mem_iter.h) checks their `TxId` against the [transaction table](https://github.com/ydb-platform/ydb/blob/0e69bf615395fdd48ecee032faaec81bc468b0b8/ydb/core/tablet_flat/flat_table.h#L359). For committed transactions, the MVCC commit version is indicated there. Then it applies all committed changes until it finds and applies the first merged record with version `Step != max`.

Suppose we committed a transaction with `TxId = 13` at point `v4000/20`. At that moment, a record `[13] => v4000/20` appears in the transaction table, and this transaction becomes committed. All subsequent reads will see changes with `TxId = 13`, skipping the transaction with `TxId = 15` because it was not committed before `TxId = 13`. The chain of changes for key K does not change.

Now suppose we performed `SET A = 30` in version `v5000/21`. The resulting chain of changes will look as follows:

| Version | Row |
| --- | --- |
| `v5000/21` | `SET A = 30, B = 20, C = 3` |
| `v{max}/13` | `SET B = 20` |
| `v{max}/15` | `SET C = 10` |
| `v3000/12` | `SET A = 1, B = 2, C = 3` |
| `v2000/11` | `SET A = 1, B = 2` |
| `v1000/10` | `SET A = 1` |

A new row was added in the merged state, including changes for `TxId = 13`. Since transaction `TxId = 15` is not committed, its changes were skipped, which is reflected in the merged state for `v5000/21`. It is important that the upstream code does not commit `TxId = 15` after this, because such a commit would lead to an anomaly: some versions during reading will see the transaction as committed, and some will not.

## Compaction of uncommitted changes

Compaction takes a part of the table data, merges them in sorted order, and writes them as a new [SST](../concepts/glossary.md#sst) that replaces the compacted data. If a MemTable participates in compaction, this also implies compaction of a part of the change log. In the compacted log, there may be `EvRemoveTx`/`EvCommitTx` messages that modify the committed transaction table — they must also end up in persistent storage. This is implemented by writing `TxStatus` blobs (see [flat_page_txstatus.h](https://github.com/ydb-platform/ydb/blob/main/ydb/core/tablet_flat/flat_page_txstatus.h)), which LocalDB saves along with writing the SST with data. These blobs store the current list of committed and deleted transactions, which replaces the entire previously written log in terms of `EvRemoveTx`/`EvCommitTx` events. The compaction receives a reference to the entire current table, but during writing, the table is filtered, preserving only those transactions that were mentioned in the compacting MemTable or previous `TxStatus` pages.

At the level of data pages (see [flat_page_data.h](https://github.com/ydb-platform/ydb/blob/main/ydb/core/tablet_flat/flat_page_data.h)), uncommitted changes from MemTable (or other SSTs) are aggregated by `TxId` while preserving their relative order and are added before the main record as uncommitted delta records. A record has MVCC flags (`HasHistory`, `IsVersioned`, `IsErased`) that indicate the presence of MVCC history for the key and the presence of fields with the MVCC version of the row. For delta records, a special [IsDelta flag](https://github.com/ydb-platform/ydb/blob/0adff98ae52cb826f7fb9705503e430b9812994f/ydb/core/tablet_flat/flat_page_data.h#L98) is used, which is a `HasHistory` flag without specifying other MVCC flags. For rows with the delta flag, after the fixed part, the [TDelta](https://github.com/ydb-platform/ydb/blob/0adff98ae52cb826f7fb9705503e430b9812994f/ydb/core/tablet_flat/flat_page_data.h#L66) structure is stored in the extension fields, which specifies the `TxId` of the transaction that was uncommitted at the time of compaction.

There can be multiple delta records for a single key, and optionally the most recent committed record. Since only one record per key can exist on a data page, the offset table points to the first delta record, and the remaining records are accessible through the alternative offsets table for that record:

| Offset | Description |
| --- | --- |
| -X*8 | Main offset |
| ... | ... |
| -16 | Delta 2 offset |
| -8 | Delta 1 offset |
| 0 | Delta 0 header |
| ... | ... |
| Delta 1 offset | Delta 1 header |
| ... | ... |
| Main offset | Main header |

Having a pointer to the Delta 0 record, the remaining records for the key can be obtained via the `GetAltRecord(size_t index)` method, where `index` is the record number in the chain (1 for Delta 1). The chain ends either with a pointer to the main record (without the delta flag) or 0 if the main record is absent.

Continuing the example started above, suppose that after writing the transaction with `TxId = 13`, the current MemTable was fully compacted. The record for the 32-bit key K may look as follows (offsets are relative to the pointer in the record list on the page):

| Offset | Value | Description |
| --- | --- | --- |
| -16 | 58 | Main offset |
| -8 | 29 | Delta 1 offset |
| 0 | 0x21 | Delta 0: IsDelta + ERowOp::Upsert |
| 1 | 0x00 | .. key column non-NULL |
| 2 | K | .. key column (32-bit) |
| 6 | 0x00 | .. column A empty |
| 7 | 0 | .. column A (32-bit) |
| 11 | 0x01 | .. column B = ECellOp::Set |
| 12 | 20 | .. column B (32-bit) |
| 16 | 0x00 | .. column C empty |
| 17 | 0 | .. column C (32-bit) |
| 21 | 13 | .. TDelta::TxId |
| 29 | 0x21 | Delta 1: IsDelta + ERowOp::Upsert |
| 30 | 0x00 | .. key column non-NULL |
| 31 | K | .. key column (32-bit) |
| 35 | 0x00 | .. column A empty |
| 36 | 0 | .. column A (32-bit) |
| 40 | 0x00 | .. column B empty |
| 41 | 0 | .. column B (32-bit) |
| 45 | 0x01 | .. column C = ECellOp::Set |
| 46 | 10 | .. column C (32-bit) |
| 50 | 15 | .. TDelta::TxId |
| 58 | 0x61 | Main: HasHistory + IsVersioned + ERowOp::Upsert |
| 59 | 0x00 | .. key column non-NULL |
| 60 | K | .. key column (32-bit) |
| 64 | 0x01 | .. column A = ECellOp::Set |
| 65 | 1 | .. column A (32-bit) |
| 69 | 0x01 | .. column B = ECellOp::Set |
| 70 | 2 | .. column B (32-bit) |
| 74 | 0x01 | .. column C = ECellOp::Set |
| 75 | 3 | .. column C (32-bit) |
| 79 | 3000 | .. RowVersion.Step |
| 87 | 12 | .. RowVersion.TxId |
| 95 | - | End of record |

The remaining two records will be in the historical data at keys `(RowId, 2000, 11)` and `(RowId, 1000, 10)`, respectively. Their presence is indicated by the `HasHistory` flag in the main record.

During compaction, the iterator operates in a special mode, iterating over all deltas and all versions for each key. The compaction scan implementation (see [flat_ops_compact.h](https://github.com/ydb-platform/ydb/blob/main/ydb/core/tablet_flat/flat_ops_compact.h)) first aggregates the deltas that were uncommitted at the start of compaction by `TxId`, preserving their relative order. If changes from different `TxId` overlap, their order may change arbitrarily. Such reordering is valid because such transactions overlap in write order and the upper level is not allowed to commit them simultaneously. Once all uncommitted deltas are aggregated, they are written in the correct order to the resulting SST (see [flat_part_writer.h](https://github.com/ydb-platform/ydb/blob/main/ydb/core/tablet_flat/flat_part_writer.h) and [flat_page_writer.h](https://github.com/ydb-platform/ydb/blob/main/ydb/core/tablet_flat/flat_page_writer.h)). After that, iteration over row versions for the key begins, which are written to the SST in decreasing version order.

When the iterator encounters the first committed delta, the commit version of that delta is taken as the row version. All committed deltas below, including the first committed row version from each [LSM](https://en.wikipedia.org/wiki/Log-structured_merge-tree) level participating in the compaction, are merged into the row state. To move to the next version, the iterator skips committed deltas with a version greater than or equal to the last version, and the process repeats.

If a large number of deltas are written and compacted for a single key, and then quickly committed with different versions, the formation of all row versions will grow quadratically with the number of deltas. This is one reason why upper levels must control the number of deltas per key and prevent their uncontrolled growth. Another current limitation is that all uncommitted deltas must be kept in memory, because they can be committed at any moment and we must check this during search. Additionally, all deltas for a key must reside on the same page.

## Statistics on uncommitted transactions

Optimistically, most write transactions eventually commit successfully, but sometimes transactions have to be aborted, even after compaction. Since after their abort a large amount of data may actually be deleted, for garbage collection purposes an `TxIdStats` page is saved in SST (see [flat_page_txidstat.h](https://github.com/ydb-platform/ydb/blob/main/ydb/core/tablet_flat/flat_page_txidstat.h)) with statistics on the number of rows and bytes occupied by each `TxId`. As transactions are aborted, the compaction strategy aggregates the amount of freed bytes and may decide to run additional compactions for garbage collection.

`TxIdStats` pages are also used to gradually reduce the commit table. After a transaction is committed, a record of it is stored in an in-memory hash table as long as there is at least one reference to the corresponding `TxId`. As SST is compacted, uncommitted deltas are overwritten as committed records and references on `TxIdStats` pages disappear. If the corresponding `TxId` was committed or aborted, and there are no references to it in either MemTable or SST, then this record is removed from the commit table and frees memory.

The size of the in-memory commit table is limited at the datashard level by the number of open transactions at any given time. Open transactions are those that have not yet been committed or aborted. During compaction, only deltas for transactions that were open at the start of compaction can be written to SST, because committed deltas are rewritten as regular committed rows, and aborted transactions leave no traces. Accordingly, the size of the commit table is limited by the number of open transactions multiplied by the number of SSTs.

## Borrowing SSTs with uncommitted changes

When copying tables, as well as during DataShard splits and merges, the mechanism of "borrowing" SSTs in LocalDB is used. In this case, SSTs from the source shard are merged into the corresponding table on the destination shard. With the addition of uncommitted changes, there may be both changes from still open transactions (relevant in case of replication) and committed but not yet compacted changes. To ensure that data on the destination shards is read in the same form as on the source shard, `TxStatus` blobs are also borrowed and merged. Information from them is added to the commit table.

Transaction information may differ on different shards. Consider a new example:

1. A transaction writes a change for key K with `TxId` to shard S, while the data is compacted and the changes end up in a large SST.
2. Shard S starts exceeding the size threshold and splits into two shards L and R.
3. SSTs that contain changes for key K end up on both shards with a filter applied.
4. Key K ends up in shard L, but the transaction also exists on shard R because there is a reference to it from a borrowed SST.
5. The transaction commits the change on shard L, but no commit occurs on shard R, since logically and from the writer's perspective there were no changes on shard R.
6. Since the transaction completes, `TxId` on shard R is rolled back after some time, because in reality nothing changes in the shard's data.
7. Suppose later shards L and R are merged again.
8. In such a situation, `TxStatus` from shard L will have a commit, and `TxStatus` from shard R will have an abort for the same `TxId`. Since transactions successfully commit all changes, and an abort may be due to a filter applied, in case of merging conflicting information for `TxId`, the commit "wins" over the abort.

In practice, DataShard is fully compacted before each split or merge, so conflicting commit status information should not arise. For example, this means that transactions cannot decide to commit only part of the changes; all changes for a specific `TxId` must be committed together. It also means that transaction IDs cannot be reused, including across different shards. Only globally unique identifiers can also be used as `TxId` for uncommitted changes.

{% include [career](./_includes/career.md) %}
