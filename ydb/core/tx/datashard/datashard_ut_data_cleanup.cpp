#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>

namespace NKikimr {

using namespace Tests;

Y_UNIT_TEST_SUITE(DataCleanup) {
    std::tuple<Tests::TServer::TPtr, TActorId, TVector<ui64>> SetupWithTable() {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = MakeIntrusive<TServer>(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        InitRoot(server, sender);

        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", 1);
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 100), (2, 200), (3, 300), (4, 400);");
        ExecSQL(server, sender, "DELETE FROM `/Root/table-1` WHERE key IN (1, 2, 4);");

        return {server, sender, shards};
    }

    void CheckResultEvent(const TEvDataShard::TEvForceDataCleanupResult& ev, ui64 tabletId, ui64 generation) {
        UNIT_ASSERT_EQUAL(ev.Record.GetStatus(), NKikimrTxDataShard::TEvForceDataCleanupResult::OK);
        UNIT_ASSERT_VALUES_EQUAL(ev.Record.GetTabletId(), tabletId);
        UNIT_ASSERT_VALUES_EQUAL(ev.Record.GetDataCleanupGeneration(), generation);
    }

    void CheckTableData(Tests::TServer::TPtr server) {
        auto result = ReadShardedTable(server, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(result, "key = 3, value = 300\n");
    }

    Y_UNIT_TEST(ForceDataCleanup) {
        auto [server, sender, tableShards] = SetupWithTable();
        auto& runtime = *server->GetRuntime();

        auto cleanupAndCheck = [&runtime, &sender, &tableShards](ui64 expectedDataCleanupGeneration) {
            auto request = MakeHolder<TEvDataShard::TEvForceDataCleanup>(expectedDataCleanupGeneration);

            runtime.SendToPipe(tableShards.at(0), sender, request.Release(), 0, GetPipeConfigWithRetries());

            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvForceDataCleanupResult>(sender);
            CheckResultEvent(*ev->Get(), tableShards.at(0), expectedDataCleanupGeneration);
        };

        cleanupAndCheck(24);
        cleanupAndCheck(24);
        cleanupAndCheck(25);

        CheckTableData(server);
    }

    Y_UNIT_TEST(MultipleDataCleanups) {
        auto [server, sender, tableShards] = SetupWithTable();
        auto& runtime = *server->GetRuntime();

        ui64 expectedGenFirst = 42;
        ui64 expectedGenLast = 43;
        auto request1 = MakeHolder<TEvDataShard::TEvForceDataCleanup>(expectedGenFirst);
        auto request2 = MakeHolder<TEvDataShard::TEvForceDataCleanup>(expectedGenLast);

        runtime.SendToPipe(tableShards.at(0), sender, request1.Release(), 0, GetPipeConfigWithRetries());
        runtime.SendToPipe(tableShards.at(0), sender, request2.Release(), 0, GetPipeConfigWithRetries());

        {
            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvForceDataCleanupResult>(sender);
            CheckResultEvent(*ev->Get(), tableShards.at(0), expectedGenLast);
        }

        {
            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvForceDataCleanupResult>(sender);
            CheckResultEvent(*ev->Get(), tableShards.at(0), expectedGenLast);
        }

        CheckTableData(server);
    }

    Y_UNIT_TEST(MultipleDataCleanupsWithOldGenerations) {
        auto [server, sender, tableShards] = SetupWithTable();
        auto& runtime = *server->GetRuntime();

        ui64 expectedGenFirst = 42;
        ui64 expectedGenOld = 10;
        auto request1 = MakeHolder<TEvDataShard::TEvForceDataCleanup>(expectedGenFirst);
        auto request2 = MakeHolder<TEvDataShard::TEvForceDataCleanup>(expectedGenOld);

        runtime.SendToPipe(tableShards.at(0), sender, request1.Release(), 0, GetPipeConfigWithRetries());
        runtime.SendToPipe(tableShards.at(0), sender, request2.Release(), 0, GetPipeConfigWithRetries());

        {
            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvForceDataCleanupResult>(sender);
            CheckResultEvent(*ev->Get(), tableShards.at(0), expectedGenFirst);
        }

        {
            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvForceDataCleanupResult>(sender);
            CheckResultEvent(*ev->Get(), tableShards.at(0), expectedGenFirst);
        }

        CheckTableData(server);
    }

    Y_UNIT_TEST(ForceDataCleanupWithRestart) {
        auto [server, sender, tableShards] = SetupWithTable();
        auto& runtime = *server->GetRuntime();

        ui64 cleanupGeneration = 33;
        ui64 oldGeneration = 10;
        ui64 olderGeneration = 5;

        {
            auto request = MakeHolder<TEvDataShard::TEvForceDataCleanup>(cleanupGeneration);

            runtime.SendToPipe(tableShards.at(0), sender, request.Release(), 0, GetPipeConfigWithRetries());

            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvForceDataCleanupResult>(sender);
            CheckResultEvent(*ev->Get(), tableShards.at(0), cleanupGeneration);
        }

        {
            auto request = MakeHolder<TEvDataShard::TEvForceDataCleanup>(oldGeneration);

            runtime.SendToPipe(tableShards.at(0), sender, request.Release(), 0, GetPipeConfigWithRetries());

            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvForceDataCleanupResult>(sender);
            CheckResultEvent(*ev->Get(), tableShards.at(0), cleanupGeneration);
        }

        // restart tablet
        SendViaPipeCache(runtime, tableShards.at(0), sender, std::make_unique<TEvents::TEvPoison>());
        runtime.SimulateSleep(TDuration::Seconds(1));

        {
            auto request = MakeHolder<TEvDataShard::TEvForceDataCleanup>(olderGeneration);

            runtime.SendToPipe(tableShards.at(0), sender, request.Release(), 0, GetPipeConfigWithRetries());

            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvForceDataCleanupResult>(sender);
            // more recent generation should be persisted
            CheckResultEvent(*ev->Get(), tableShards.at(0), cleanupGeneration);
        }

        CheckTableData(server);
    }
}

} // namespace NKikimr
