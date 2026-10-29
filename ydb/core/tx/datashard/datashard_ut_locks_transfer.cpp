#include "datashard_ut_common_kqp.h"
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common_tx.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/protos/query_stats.pb.h>

namespace NKikimr {

using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NKikimr::NDataShard::NTxHelpers;
using namespace NSchemeShard;
using namespace Tests;

Y_UNIT_TEST_SUITE(DataShardLocksTransfer) {

struct TTestEnv {
    TPortManager PortManager;
    TServer::TPtr Server;
    TActorId Sender;

    TDisableDataShardLogBatching DisableDataShardLogBatching;

    TTableId TableId;
    TVector<ui64> Shards;

    TTestActorRuntime& GetRuntime() { return *Server->GetRuntime(); }

    TTestEnv(const TVector<ui32>& splitPoints) {
        NKikimrConfig::TAppConfig app;
        TServerSettings serverSettings(PortManager.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetAppConfig(app);
        serverSettings.FeatureFlags.SetEnableDataShardLocksTransferOnSplit(true);

        Server = new TServer(serverSettings);
        auto& runtime = GetRuntime();
        Sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(Server, Sender);

        SetSplitMergePartCountLimit(&GetRuntime(), -1);

        TStringBuilder partitionAtKeys;
        if (!splitPoints.empty()) {
            partitionAtKeys << " WITH (PARTITION_AT_KEYS = (" << JoinSeq(", ", splitPoints) << "))";
        }

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key UInt32, value int, PRIMARY KEY (key))
            )" + partitionAtKeys),
            "SUCCESS"
        );

        TableId = ResolveTableId(Server, Sender, "/Root/table");
        UNIT_ASSERT(TableId);
        Shards = GetTableShards(Server, Sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(Shards.size(), splitPoints.size() + 1);
    }

    auto GetAll() {
        const auto& self = *this;
        return std::tie(Server, GetRuntime(), self.Sender, self.TableId, self.Shards);
    }

    void Split(size_t shardIndex, ui32 splitKey) {
        ui64 txId = AsyncSplitTable(Server, Sender, "/Root/table", Shards.at(shardIndex), splitKey);
        WaitTxNotification(Server, Sender, txId);
        Shards = GetTableShards(Server, Sender, "/Root/table");
    }

    void Merge(size_t fromIdx, size_t toIdx) {
        TVector<ui64> tablets(Shards.begin() + fromIdx, Shards.begin() + toIdx + 1);
        ui64 txId = AsyncMergeTable(Server, Sender, "/Root/table", tablets);
        WaitTxNotification(Server, Sender, txId);
        Shards = GetTableShards(Server, Sender, "/Root/table");
    }
};

Y_UNIT_TEST(LocksTransferSimple) {
    // Create a table with 3 shards
    TTestEnv env({10, 20});
    auto [server, runtime, sender, tableId, shards] = env.GetAll();

    TTransactionState tx(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);

    // Lock and upsert a row in shards 0 and 1
    tx.LockRows(tableId, shards.at(0), {1});
    tx.Write(tableId, shards.at(0), TWriteOperation::Upsert(1, 100));

    tx.LockRows(tableId, shards.at(1), {15});
    tx.Write(tableId, shards.at(1), TWriteOperation::Upsert(15, 1500));

    // After a split-merge we end up with 3 shards as well, shard 0 got
    // data from old shards 0 and 1, and shard 1 from old shard 1
    auto oldShards = shards;
    env.Split(1, 15);
    env.Merge(0, 1);

    tx.MapAncestorShard(shards.at(0), oldShards.at(0));
    tx.MapAncestorShard(shards.at(0), oldShards.at(1));
    tx.MapAncestorShard(shards.at(1), oldShards.at(1));

    // Reboot one of the tablets to check loading locks from localdb
    RebootTablet(runtime, shards.at(1), sender);

    // Check that we can still read uncommitted data on new shards
    UNIT_ASSERT_VALUES_EQUAL(
        tx.ReadKey(tableId, shards.at(0), 1),
        "1, 100\n");
    UNIT_ASSERT_VALUES_EQUAL(
        tx.ReadKey(tableId, shards.at(1), 15),
        "15, 1500\n");

    // Commit the transaction and check that the results are readable
    tx.InitCommit({shards.at(0), shards.at(1)});
    auto prepare1 = tx.PrepareCommit(tableId, shards.at(0));
    auto prepare2 = tx.PrepareCommit(tableId, shards.at(1));
    tx.SendPlan();
    UNIT_ASSERT_VALUES_EQUAL(prepare1.NextString(), "OK");
    UNIT_ASSERT_VALUES_EQUAL(prepare2.NextString(), "OK");

    UNIT_ASSERT_VALUES_EQUAL(
        KqpSimpleExec(runtime, R"(
            SELECT key, value FROM `/Root/table` ORDER BY key;
        )"),
        "{ items { uint32_value: 1 } items { int32_value: 100 } }, "
        "{ items { uint32_value: 15 } items { int32_value: 1500 } }");
}

Y_UNIT_TEST(WriteSeqNums) {
    TTestEnv env({});
    auto [server, runtime, sender, tableId, shards] = env.GetAll();

    // Start with a table with 2 shards
    env.Split(0, 10);

    // Use write seqnums in this tx
    TTransactionState tx(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
    tx.WriterIndex = 123;

    // Lock and upsert a row on each of the shards, but for shard 0 let the write go
    // through and block the result, and for shard 1 block the write request itself.
    auto shard0Actor = ResolveTablet(runtime, shards.at(0));
    TBlockEvents<NEvents::TDataEvents::TEvWriteResult> blockWriteResToShard0(runtime, [&](auto& ev) {
        return ev->Sender == shard0Actor;
    });
    auto shard1Actor = ResolveTablet(runtime, shards.at(1));
    TBlockEvents<NEvents::TDataEvents::TEvWrite> blockWritesToShard1(runtime, [&](auto& ev) {
        return ev->GetRecipientRewrite() == shard1Actor;
    });

    UNIT_ASSERT_VALUES_EQUAL(tx.LockRows(tableId, shards.at(0), {1}), "OK");
    auto write1Fut = tx.SendWrite(tableId, shards.at(0), TWriteOperation::Upsert(1, 100));
    UNIT_ASSERT_VALUES_EQUAL(
        write1Fut.NextString(TDuration::Seconds(1)),
        "<timeout>");

    UNIT_ASSERT_VALUES_EQUAL(tx.LockRows(tableId, shards.at(1), {15}), "OK");
    auto write2Fut = tx.SendWrite(tableId, shards.at(1), TWriteOperation::Upsert(15, 1500));
    UNIT_ASSERT_VALUES_EQUAL(
        write2Fut.NextString(TDuration::Seconds(1)),
        "<timeout>");

    runtime.WaitFor("Blocked writes", [&] {
        return !blockWriteResToShard0.empty() && !blockWritesToShard1.empty();
    });
    blockWriteResToShard0.Stop();
    blockWritesToShard1.Stop();

    // Merge into a single shard
    auto oldShards = shards;
    env.Merge(0, 1);

    tx.MapAncestorShard(shards.at(0), oldShards.at(0));
    tx.MapAncestorShard(shards.at(0), oldShards.at(1));

    // Re-send the write to old shard 0. It is a duplicate.
    UNIT_ASSERT_VALUES_EQUAL(
        tx.RetryWriteToAnotherShard(
            tableId, oldShards.at(0), shards.at(0), TWriteOperation::Upsert(1, 100)),
        "OK");
    // Re-send the write to old shard 1.
    UNIT_ASSERT_VALUES_EQUAL(
        tx.RetryWriteToAnotherShard(
            tableId, oldShards.at(1), shards.at(0), TWriteOperation::Upsert(15, 1500)),
        "OK");

    // Stale request is an error now that the old shard 1 is merged
    blockWritesToShard1.Unblock();
    UNIT_ASSERT_VALUES_EQUAL(
        write2Fut.NextString(TDuration::Seconds(1)),
        "ERROR: STATUS_WRONG_SHARD_STATE");

    // Write something to the new shard as well.
    UNIT_ASSERT_VALUES_EQUAL(tx.LockRows(tableId, shards.at(0), {20}), "OK");
    UNIT_ASSERT_VALUES_EQUAL(
        tx.Write(tableId, shards.at(0), TWriteOperation::Upsert(20, 2000)),
        "OK");

    // Check that the uncommitted data is readable.
    UNIT_ASSERT_VALUES_EQUAL(
        tx.ReadKey(tableId, shards.at(0), 1),
        "1, 100\n");
    UNIT_ASSERT_VALUES_EQUAL(
        tx.ReadKey(tableId, shards.at(0), 15),
        "15, 1500\n");
    UNIT_ASSERT_VALUES_EQUAL(
        tx.ReadKey(tableId, shards.at(0), 20),
        "20, 2000\n");

    // Do an immediate commit and check that the data was committed.
    UNIT_ASSERT_VALUES_EQUAL(
        tx.WriteCommit(tableId, shards.at(0)),
        "OK");

    UNIT_ASSERT_VALUES_EQUAL(
        KqpSimpleExec(runtime, R"(
            SELECT key, value FROM `/Root/table` ORDER BY key;
        )"),
        "{ items { uint32_value: 1 } items { int32_value: 100 } }, "
        "{ items { uint32_value: 15 } items { int32_value: 1500 } }, "
        "{ items { uint32_value: 20 } items { int32_value: 2000 } }");
}

// A re-routed batch after a split may merge two pending positions of the removed
// shard's write chain in a single EvWrite: the already applied, unacknowledged
// position 1 (its result was lost, its rows were moved by the split) and the still
// pending position 2 that was queued behind it. Such a mixed group starts at an
// already applied position: the leading position is a duplicate (its ops are
// skipped, the rows are not written again) and the tail is applied as a
// continuation of the transferred ancestor chain. Before the DataShard accepted
// mixed groups, every covering shard answered the merged batch with
// STATUS_BAD_REQUEST ("must continue the writer chain at 2").
Y_UNIT_TEST(WriteSeqNumsRerouteMergesAppliedAndPending) {
    TTestEnv env({});
    auto [server, runtime, sender, tableId, initialShards] = env.GetAll();
    UNIT_ASSERT_VALUES_EQUAL(initialShards.size(), 1u);

    // Start with a table with 2 shards
    env.Split(0, 10);
    TVector<ui64> shards = env.Shards;
    UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);

    // Use write seqnums in this tx
    TTransactionState tx(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
    tx.WriterIndex = 123;

    // The first write (WriteSeqNum 1) is applied on the shard that is about to be
    // split, but its result is blocked: the batch stays unacknowledged, like a lost
    // reply of an in-flight re-routed batch.
    auto splitShardActor = ResolveTablet(runtime, shards.at(0));
    TBlockEvents<NEvents::TDataEvents::TEvWriteResult> blockWriteResToSplitShard(runtime, [&](auto& ev) {
        return ev->Sender == splitShardActor;
    });

    UNIT_ASSERT_VALUES_EQUAL(tx.LockRows(tableId, shards.at(0), {1}), "OK");
    auto write1Fut = tx.SendWrite(tableId, shards.at(0), TWriteOperation::Upsert(1, 100));
    UNIT_ASSERT_VALUES_EQUAL(
        write1Fut.NextString(TDuration::Seconds(1)),
        "<timeout>");

    runtime.WaitFor("Blocked write result", [&] {
        return !blockWriteResToSplitShard.empty();
    });

    // Release the blocked reply of the original write: it is a stale duplicate of
    // position 1 once the re-routed batches arrive; nothing must break. Draining it
    // before the re-routes also keeps the harness' lock echo bookkeeping fresh.
    blockWriteResToSplitShard.Stop();
    blockWriteResToSplitShard.Unblock();
    UNIT_ASSERT_VALUES_EQUAL(
        write1Fut.NextString(TDuration::Seconds(1)),
        "OK");

    // Quiet split of the shard: the lock with its WriteSeqNum chain (at 1) and the
    // rows transfer to the two new covering shards.
    auto oldShards = shards;
    env.Split(0, 5);
    shards = env.Shards;
    UNIT_ASSERT_VALUES_EQUAL(shards.size(), 3u);

    tx.MapAncestorShard(shards.at(0), oldShards.at(0));
    tx.MapAncestorShard(shards.at(1), oldShards.at(0));

    // The drained reply advanced the test's write seq num bookkeeping for the
    // removed shard; reset it so the re-routed batch is rebuilt with the pending
    // positions [1, 2] of the removed shard's chain: the applied position 1 (a
    // duplicate now, its result was lost) and the queued position 2.
    tx.Shard2SeqNum[oldShards.at(0)] = 0;

    // The re-routed batch merges the applied position 1 with the pending position 2
    // of the removed shard's chain. The op of the duplicate position carries a
    // different value (999): if it were applied again, key 1 would be overwritten.
    {
        auto promise = tx.SendWriteToAnotherShard(
            tableId, oldShards.at(0), shards.at(0),
            TWriteOperation::Upsert(1, 999), TWriteOperation::Upsert(2, 200));
        auto result = promise.NextResult();
        UNIT_ASSERT_VALUES_EQUAL(
            result->Record.GetStatus(),
            NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        // The reply reports the sum of the batch's statistics: the duplicate
        // position's original delivery (1 written row, its reply was lost) plus
        // the fresh tail op for key 2.
        UNIT_ASSERT_VALUES_EQUAL(
            result->Record.GetTxStats().GetTableAccessStats(0).GetUpdateRow().GetRows(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetTxLocks().size(), 2u);
        bool chainAdvanced = false;
        for (const auto& lock : result->Record.GetTxLocks()) {
            if (lock.WriteSeqNumsSize() == 1
                    && lock.GetWriteSeqNums(0).GetWriterIndex() == 123u
                    && lock.GetWriteSeqNums(0).GetWriteSeqNum() == 2u) {
                chainAdvanced = true;
            }
        }
        UNIT_ASSERT_C(chainAdvanced,
            "the transferred chain did not advance to position 2");
    }

    // The second covering shard receives the same merged positions without data (the
    // empty re-routed batch): the duplicate position is skipped, the empty tail
    // advances the transferred chain. The reply echoes both the shard's own lock and
    // the transferred ancestor lock carrying the advanced chain position. The first
    // reply advanced the test's write seq num bookkeeping for the removed shard;
    // reset it so the same merged positions are rebuilt for the empty batch.
    tx.Shard2SeqNum[oldShards.at(0)] = 0;
    {
        const TVector<TWriteOperation::TKeyValue> noRows;
        auto promise = tx.SendWriteToAnotherShard(
            tableId, oldShards.at(0), shards.at(1),
            TWriteOperation::Upsert(noRows), TWriteOperation::Upsert(noRows));
        auto result = promise.NextResult();
        UNIT_ASSERT_VALUES_EQUAL(
            result->Record.GetStatus(),
            NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        // The duplicate position's stored statistics are reported even when the
        // batch's own ops are empty: the writer still never saw the lost delivery.
        UNIT_ASSERT_VALUES_EQUAL(
            result->Record.GetTxStats().GetTableAccessStats(0).GetUpdateRow().GetRows(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetTxLocks().size(), 2u);
        bool chainAdvanced = false;
        for (const auto& lock : result->Record.GetTxLocks()) {
            if (lock.WriteSeqNumsSize() == 1
                    && lock.GetWriteSeqNums(0).GetWriterIndex() == 123u
                    && lock.GetWriteSeqNums(0).GetWriteSeqNum() == 2u) {
                chainAdvanced = true;
            }
        }
        UNIT_ASSERT_C(chainAdvanced,
            "the echoed transferred chain did not advance to position 2");
    }

    // The duplicate position was not applied again: key 1 keeps its value; the tail
    // is applied as a continuation.
    UNIT_ASSERT_VALUES_EQUAL(
        tx.ReadKey(tableId, shards.at(0), 1),
        "1, 100\n");
    UNIT_ASSERT_VALUES_EQUAL(
        tx.ReadKey(tableId, shards.at(0), 2),
        "2, 200\n");

    // Commit the transaction and check that the data is committed.
    UNIT_ASSERT_VALUES_EQUAL(
        tx.WriteCommit(tableId, shards.at(0)),
        "OK");

    UNIT_ASSERT_VALUES_EQUAL(
        KqpSimpleExec(runtime, R"(
            SELECT key, value FROM `/Root/table` WHERE key = 1 OR key = 2 ORDER BY key;
        )"),
        "{ items { uint32_value: 1 } items { int32_value: 100 } }, "
        "{ items { uint32_value: 2 } items { int32_value: 200 } }");
}

// The writer acknowledges deliveries in order and resends only unacknowledged
// operations, so already applied (duplicate) positions must always form a
// prefix of an EvWrite request: a duplicate positioned after a fresh operation
// is a writer bug and must be rejected.
Y_UNIT_TEST(WriteSeqNumsDuplicateAfterFreshRejected) {
    TTestEnv env({});
    auto [server, runtime, sender, tableId, initialShards] = env.GetAll();
    UNIT_ASSERT_VALUES_EQUAL(initialShards.size(), 1u);

    TTransactionState tx(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
    tx.WriterIndex = 123;

    // Apply positions 1 and 2 of the writer chain on the shard: the delivery is
    // acknowledged normally, so the shard's chain is at 2.
    UNIT_ASSERT_VALUES_EQUAL(tx.LockRows(tableId, env.Shards.at(0), {1}), "OK");
    UNIT_ASSERT_VALUES_EQUAL(
        tx.Write(tableId, env.Shards.at(0), TWriteOperation::Upsert(1, 100), TWriteOperation::Upsert(2, 200)),
        "OK");

    // Split the shard: the lock with its WriteSeqNum chain (at 2) transfers to
    // the new covering shards as an ancestor state.
    const ui64 oldShard = env.Shards.at(0);
    env.Split(0, 5);
    auto shards = env.Shards;
    UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);
    tx.MapAncestorShard(shards.at(0), oldShard);

    tx.Shard2SeqNum[oldShard] = 0;
    {
        auto req = tx.CreateWrite(tableId, oldShard, shards.at(0),
            TWriteOperation::Upsert(3, 300), TWriteOperation::Upsert(4, 400));
        // The first op is re-labeled as a fresh position of the current shard's
        // own chain, the second stays on the transferred ancestor chain as its
        // already applied position 2.
        req->Record.MutableOperations(0)->SetOriginalShard(0);

        auto reqSender = runtime.AllocateEdgeActor();
        ui32 nodeIndex = reqSender.NodeId() - runtime.GetNodeId(0);
        runtime.SendToPipe(shards.at(0), reqSender, req.release(), nodeIndex);

        TTransactionState::TWritePromise promise{tx, reqSender};
        auto result = promise.NextResult();
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(
            result->Record.GetStatus(),
            NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
        UNIT_ASSERT(result->Record.GetIssues(0).message().Contains(
            "duplicates must form a prefix of the request"));
    }

    // Neither row of the rejected request was applied.
    UNIT_ASSERT_VALUES_EQUAL(
        KqpSimpleExec(runtime, R"(
            SELECT key, value FROM `/Root/table` WHERE key = 3 OR key = 4 ORDER BY key;
        )"),
        "");
}

Y_UNIT_TEST(RollbackSimple) {
    TTestEnv env({});
    auto [server, runtime, sender, tableId, shards] = env.GetAll();

    // Start with a table with 2 shards
    env.Split(0, 10);

    TTransactionState tx(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
    tx.WriterIndex = 123;

    // Lock and upsert a row to each of the shards.
    UNIT_ASSERT_VALUES_EQUAL(tx.LockRows(tableId, shards.at(0), {1}), "OK");
    UNIT_ASSERT_VALUES_EQUAL(
        tx.Write(tableId, shards.at(0), TWriteOperation::Upsert(1, 100)),
        "OK");

    UNIT_ASSERT_VALUES_EQUAL(tx.LockRows(tableId, shards.at(1), {15}), "OK");
    UNIT_ASSERT_VALUES_EQUAL(
        tx.Write(tableId, shards.at(1), TWriteOperation::Upsert(15, 1500)),
        "OK");

    // Merge into a single shard.
    auto oldShards = shards;
    env.Merge(0, 1);

    tx.MapAncestorShard(shards.at(0), oldShards.at(0));
    tx.MapAncestorShard(shards.at(0), oldShards.at(1));

    // Check that the uncommitted data is there.
    UNIT_ASSERT_VALUES_EQUAL(
        tx.ReadKey(tableId, shards.at(0), 1),
        "1, 100\n");
    UNIT_ASSERT_VALUES_EQUAL(
        tx.ReadKey(tableId, shards.at(0), 15),
        "15, 1500\n");

    // Rollback the transaction and check that everything gets rolled back.
    UNIT_ASSERT_VALUES_EQUAL(
        tx.Rollback(shards.at(0)),
        "OK");

    UNIT_ASSERT_VALUES_EQUAL(
        KqpSimpleExec(runtime, R"(
            SELECT key, value FROM `/Root/table` ORDER BY key;
        )"),
        "");

    runtime.SendToPipe(shards.at(0), sender, new TEvDataShard::TEvGetOpenTxs(tableId.PathId));
    auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvGetOpenTxsResult>(sender);
    UNIT_ASSERT(ev->Get()->OpenTxs.empty());
}

Y_UNIT_TEST_TWIN(ConflictsSimple, CommitInWriteOrder) {
    TTestEnv env({});
    auto [server, runtime, sender, tableId, shards] = env.GetAll();

    // Start with a table with 2 shards
    env.Split(0, 10);

    TTransactionState tx1(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
    tx1.WriterIndex = 123;
    TTransactionState tx2(runtime, NKikimrDataEvents::OPTIMISTIC);
    tx2.WriterIndex = 234;

    // Tx1: lock and upsert a row to each of the shards.
    UNIT_ASSERT_VALUES_EQUAL(tx1.LockRows(tableId, shards.at(0), {1}), "OK");
    UNIT_ASSERT_VALUES_EQUAL(
        tx1.Write(tableId, shards.at(0), TWriteOperation::Upsert(1, 100)),
        "OK");

    UNIT_ASSERT_VALUES_EQUAL(tx1.LockRows(tableId, shards.at(1), {15}), "OK");
    UNIT_ASSERT_VALUES_EQUAL(
        tx1.Write(tableId, shards.at(1), TWriteOperation::Upsert(15, 1500)),
        "OK");

    // Tx2: upsert the same key on the second shard.
    UNIT_ASSERT_VALUES_EQUAL(
        tx2.Write(tableId, shards.at(1), TWriteOperation::Upsert(15, 1501)),
        "OK");

    // Merge into a single shard.
    auto oldShards = shards;
    env.Merge(0, 1);

    for (auto tx : {&tx1, &tx2}) {
        tx->MapAncestorShard(shards.at(0), oldShards.at(0));
        tx->MapAncestorShard(shards.at(0), oldShards.at(1));
    }

    if (CommitInWriteOrder) {
        // Commit tx1 first, tx2 should still be live.
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.WriteCommit(tableId, shards.at(0)),
            "OK");

        UNIT_ASSERT_VALUES_EQUAL(
            tx2.Write(tableId, shards.at(0), TWriteOperation::Upsert(15, 1502)),
            "OK");

        // Commit tx2 and observe the results.
        UNIT_ASSERT_VALUES_EQUAL(
            tx2.WriteCommit(tableId, shards.at(0)),
            "OK");

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table` ORDER BY key;
            )"),
            "{ items { uint32_value: 1 } items { int32_value: 100 } }, "
            "{ items { uint32_value: 15 } items { int32_value: 1502 } }");
    } else {
        // Commit tx2 first, this should abort tx1.
        UNIT_ASSERT_VALUES_EQUAL(
            tx2.WriteCommit(tableId, shards.at(0)),
            "OK");

        // tx1 should be aborted, so additional updates should fail.
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.Write(tableId, shards.at(0), TWriteOperation::Upsert(1, 101)),
            "ERROR: STATUS_LOCKS_BROKEN");

        UNIT_ASSERT_VALUES_EQUAL(
            tx1.WriteCommit(tableId, shards.at(0)),
            "ERROR: STATUS_LOCKS_BROKEN");

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table` ORDER BY key;
            )"),
            "{ items { uint32_value: 15 } items { int32_value: 1501 } }");
    }
}

} // Y_UNIT_TEST_SUITE(DataShardLocksTransfer)

} // namespace NKikimr
