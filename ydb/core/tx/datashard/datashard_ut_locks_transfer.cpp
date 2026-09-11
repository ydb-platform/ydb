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
        "OK (duplicate)");
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

} // Y_UNIT_TEST_SUITE(DataShardLocksTransfer)

} // namespace NKikimr
