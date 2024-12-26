#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>

namespace NKikimr {

using namespace Tests;

Y_UNIT_TEST_SUITE(DataCleanup) {
    Y_UNIT_TEST(ForceDataCleanup) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        InitRoot(server, sender);

        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", 1);

        ui64 expectedDataCleanupGeneration = 42;
        auto request = MakeHolder<TEvDataShard::TEvForceDataCleanup>(expectedDataCleanupGeneration);

        runtime.SendToPipe(shards.at(0), sender, request.Release(), 0, GetPipeConfigWithRetries());

        auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvForceDataCleanupResult>(sender);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetDataCleanupGeneration(), expectedDataCleanupGeneration);
    }
}

} // namespace NKikimr
