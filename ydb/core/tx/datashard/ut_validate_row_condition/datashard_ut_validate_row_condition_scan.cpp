#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/protos/index_builder.pb.h>

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NSchemeShard;
using namespace Tests;

Y_UNIT_TEST_SUITE(DataShardCheckConstraintScan) {

    Y_UNIT_TEST(SimpleCheck) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "test", 1);
        auto shards = GetTableShards(server, sender, "/Root/test");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        ExecSQL(server, sender, "UPSERT INTO `/Root/test` (key, value) VALUES (1, 1), (2, 2);");

        auto snapshot = CreateVolatileSnapshot(server, { "/Root/test" });
        auto tableId = ResolveTableId(server, sender, "/Root/test");

        auto request = MakeHolder<TEvDataShard::TEvValidateRowConditionRequest>();
        request->Record.SetId(100);
        request->Record.SetTabletId(shards[0]);
        request->Record.SetOwnerId(tableId.PathId.OwnerId);
        request->Record.SetPathId(tableId.PathId.LocalPathId);
        request->Record.AddNotNullColumns("value");
        request->Record.SetSnapshotStep(snapshot.Step);
        request->Record.SetSnapshotTxId(snapshot.TxId);

        runtime.SendToPipe(shards[0], sender, request.Release(), 0, GetPipeConfigWithRetries());

        {
            TAutoPtr<IEventHandle> handle;
            auto reply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvValidateRowConditionResponse>(handle);
            UNIT_ASSERT_VALUES_EQUAL((ui32)reply->Record.GetStatus(), (ui32)NKikimrIndexBuilder::EBuildStatus::DONE);
            UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetIsValid(), true);
        }
    }

    Y_UNIT_TEST(CheckWithNulls) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "test_nulls", 1);
        auto shards = GetTableShards(server, sender, "/Root/test_nulls");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        ExecSQL(server, sender, "UPSERT INTO `/Root/test_nulls` (key, value) VALUES (1, 1), (2, NULL);");

        auto snapshot = CreateVolatileSnapshot(server, { "/Root/test_nulls" });
        auto tableId = ResolveTableId(server, sender, "/Root/test_nulls");

        auto request = MakeHolder<TEvDataShard::TEvValidateRowConditionRequest>();
        request->Record.SetId(101);
        request->Record.SetTabletId(shards[0]);
        request->Record.SetOwnerId(tableId.PathId.OwnerId);
        request->Record.SetPathId(tableId.PathId.LocalPathId);
        request->Record.AddNotNullColumns("value");
        request->Record.SetSnapshotStep(snapshot.Step);
        request->Record.SetSnapshotTxId(snapshot.TxId);

        runtime.SendToPipe(shards[0], sender, request.Release(), 0, GetPipeConfigWithRetries());

        {
            TAutoPtr<IEventHandle> handle;
            auto reply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvValidateRowConditionResponse>(handle);
            UNIT_ASSERT_VALUES_EQUAL((ui32)reply->Record.GetStatus(), (ui32)NKikimrIndexBuilder::EBuildStatus::DONE);
            UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetIsValid(), false);
        }
    }

    Y_UNIT_TEST(CheckSpecificColumn) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "test_cols",
            TShardedTableOptions().Columns({
                {"key", "Uint32", true, false},
                {"col2", "Uint32", false, false},
                {"col3", "Uint32", false, false}
            }));
        auto shards = GetTableShards(server, sender, "/Root/test_cols");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        ExecSQL(server, sender, "UPSERT INTO `/Root/test_cols` (key, col2, col3) VALUES (1, NULL, 10);");

        auto snapshot = CreateVolatileSnapshot(server, { "/Root/test_cols" });
        auto tableId = ResolveTableId(server, sender, "/Root/test_cols");

        auto request = MakeHolder<TEvDataShard::TEvValidateRowConditionRequest>();
        request->Record.SetId(102);
        request->Record.SetTabletId(shards[0]);
        request->Record.SetOwnerId(tableId.PathId.OwnerId);
        request->Record.SetPathId(tableId.PathId.LocalPathId);
        request->Record.AddNotNullColumns("col3");
        request->Record.SetSnapshotStep(snapshot.Step);
        request->Record.SetSnapshotTxId(snapshot.TxId);

        runtime.SendToPipe(shards[0], sender, request.Release(), 0, GetPipeConfigWithRetries());

        {
            TAutoPtr<IEventHandle> handle;
            auto reply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvValidateRowConditionResponse>(handle);
            UNIT_ASSERT_VALUES_EQUAL((ui32)reply->Record.GetStatus(), (ui32)NKikimrIndexBuilder::EBuildStatus::DONE);
            UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetIsValid(), true);
        }
    }

    Y_UNIT_TEST(CheckSpecificColumnWithNull) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "test_cols_null",
            TShardedTableOptions().Columns({
                {"key", "Uint32", true, false},
                {"col2", "Uint32", false, false},
                {"col3", "Uint32", false, false}
            }));
        auto shards = GetTableShards(server, sender, "/Root/test_cols_null");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        ExecSQL(server, sender, "UPSERT INTO `/Root/test_cols_null` (key, col2, col3) VALUES (1, NULL, 10);");

        auto snapshot = CreateVolatileSnapshot(server, { "/Root/test_cols_null" });
        auto tableId = ResolveTableId(server, sender, "/Root/test_cols_null");

        auto request = MakeHolder<TEvDataShard::TEvValidateRowConditionRequest>();
        request->Record.SetId(103);
        request->Record.SetTabletId(shards[0]);
        request->Record.SetOwnerId(tableId.PathId.OwnerId);
        request->Record.SetPathId(tableId.PathId.LocalPathId);
        request->Record.AddNotNullColumns("col2");
        request->Record.SetSnapshotStep(snapshot.Step);
        request->Record.SetSnapshotTxId(snapshot.TxId);

        runtime.SendToPipe(shards[0], sender, request.Release(), 0, GetPipeConfigWithRetries());

        {
            TAutoPtr<IEventHandle> handle;
            auto reply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvValidateRowConditionResponse>(handle);
            UNIT_ASSERT_VALUES_EQUAL((ui32)reply->Record.GetStatus(), (ui32)NKikimrIndexBuilder::EBuildStatus::DONE);
            UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetIsValid(), false);
        }
    }
}

} // namespace NKikimr