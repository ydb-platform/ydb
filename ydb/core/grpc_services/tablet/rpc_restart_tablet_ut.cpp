#include "rpc_restart_tablet.h"
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NGRpcService {

using namespace Tests;

Y_UNIT_TEST_SUITE(TabletService_Restart) {

    NThreading::TFuture<Ydb::Tablet::RestartTabletResponse> RestartRpc(
            TTestActorRuntime& runtime, ui64 tabletId,
            const TString& token = {})
    {
        Ydb::Tablet::RestartTabletRequest request;
        request.set_tablet_id(tabletId);
        return NRpcService::DoLocalRpc<TEvRestartTabletRequest>(
            std::move(request), "/Root", token, runtime.GetActorSystem(0));
    }

    Y_UNIT_TEST(Basics) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();

        auto sender = runtime.AllocateEdgeActor();
        server->SetupRootStoragePools(sender);

        ui64 schemeShardId = ChangeStateStorage(Tests::SchemeRoot, server->GetSettings().Domain);
        auto actorBefore = ResolveTablet(runtime, schemeShardId);

        Cerr << "... restarting tablet " << schemeShardId << Endl;
        auto future = RestartRpc(runtime, schemeShardId);
        auto result = runtime.WaitFuture(std::move(future));
        UNIT_ASSERT_VALUES_EQUAL_C(result.status(), Ydb::StatusIds::SUCCESS, result.DebugString());

        runtime.SimulateSleep(TDuration::Seconds(1));
        InvalidateTabletResolverCache(runtime, schemeShardId);
        auto actorAfter = ResolveTablet(runtime, schemeShardId);

        UNIT_ASSERT_C(actorBefore != actorAfter, "SchemeShard actor " << actorBefore << " didn't change");
    }

    Y_UNIT_TEST(OnlyAdminsAllowed) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        runtime.GetAppData().AdministrationAllowedSIDs.push_back("root@builtin");

        auto sender = runtime.AllocateEdgeActor();
        server->SetupRootStoragePools(sender);

        ui64 schemeShardId = ChangeStateStorage(Tests::SchemeRoot, server->GetSettings().Domain);
        auto actorBefore = ResolveTablet(runtime, schemeShardId);

        Cerr << "... restarting tablet " << schemeShardId << " (without token)" << Endl;
        auto future = RestartRpc(runtime, schemeShardId);
        auto result = runtime.WaitFuture(std::move(future));
        UNIT_ASSERT_VALUES_EQUAL_C(result.status(), Ydb::StatusIds::UNAUTHORIZED, result.DebugString());

        Cerr << "... restarting tablet " << schemeShardId << " (non-admin token)" << Endl;
        future = RestartRpc(runtime, schemeShardId, NACLib::TUserToken("user@builtin", {}).SerializeAsString());
        result = runtime.WaitFuture(std::move(future));
        UNIT_ASSERT_VALUES_EQUAL_C(result.status(), Ydb::StatusIds::UNAUTHORIZED, result.DebugString());

        runtime.SimulateSleep(TDuration::Seconds(1));
        InvalidateTabletResolverCache(runtime, schemeShardId);
        auto actorNoRestart = ResolveTablet(runtime, schemeShardId);

        UNIT_ASSERT_C(actorBefore == actorNoRestart, "SchemeShard actor " << actorBefore << " changed to " << actorNoRestart);

        Cerr << "... restarting tablet " << schemeShardId << " (admin token)" << Endl;
        future = RestartRpc(runtime, schemeShardId, NACLib::TUserToken("root@builtin", {}).SerializeAsString());
        result = runtime.WaitFuture(std::move(future));
        UNIT_ASSERT_VALUES_EQUAL_C(result.status(), Ydb::StatusIds::SUCCESS, result.DebugString());

        runtime.SimulateSleep(TDuration::Seconds(1));
        InvalidateTabletResolverCache(runtime, schemeShardId);
        auto actorAfter = ResolveTablet(runtime, schemeShardId);

        UNIT_ASSERT_C(actorBefore != actorAfter, "SchemeShard actor " << actorBefore << " didn't change");
    }

} // Y_UNIT_TEST_SUITE(TabletService_Restart)

} // namespace NKikimr::NGRpcService
