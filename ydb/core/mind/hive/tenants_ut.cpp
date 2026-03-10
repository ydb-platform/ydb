#include <ydb/core/cms/cms.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/public/api/grpc/ydb_cms_v1.grpc.pb.h>
#include <ydb/public/api/grpc/draft/ydb_maintenance_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_scripting_v1.grpc.pb.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/async.h>

#include "ut_common.h"

using namespace NKikimr;

Y_UNIT_TEST_SUITE(THiveTestWithTenants) {
    Y_UNIT_TEST(TestDrain) {
        TPortManager pm;
        Tests::TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetNodeCount(1)
            .SetDynamicNodeCount(5)
            .SetUseRealThreads(true)
            .AddStoragePoolType("ssd");

        Tests::TServer::TPtr server = new Tests::TServer(serverSettings);

        auto& runtime = *server->GetRuntime();
        if (ENABLE_DETAILED_HIVE_LOG) {
            runtime.SetLogPriority(NKikimrServices::HIVE, NActors::NLog::PRI_TRACE);
        }
        const auto sender = runtime.AllocateEdgeActor();
        const ui64 hiveTablet = Tests::ChangeStateStorage(Tests::Hive, serverSettings.Domain);

        Tests::TTenants tenants(server);

        Cerr << "1.Create tenant" << Endl;
        {
            Ydb::Cms::CreateDatabaseRequest request;
            request.set_path("/Root/db1");
            auto* resources = request.mutable_resources();
            auto* storage = resources->add_storage_units();
            storage->set_unit_kind("ssd");
            storage->set_count(1);
            tenants.CreateTenant(request, 5, TDuration::Minutes(1));
        }

        Cerr << "2.Create table" << Endl;

        {
            using TEvExecuteYqlRequest = NGRpcService::TGrpcRequestOperationCall<
                Ydb::Scripting::ExecuteYqlRequest,
                Ydb::Scripting::ExecuteYqlResponse>;

            const TString createTable = R"(
                CREATE TABLE `Root/db1/table` (
                    Key Uint64,
                    Value Uint64,
                    PRIMARY KEY (Key)
                ) WITH (
                    UNIFORM_PARTITIONS = 10,
                    AUTO_PARTITIONING_BY_SIZE = DISABLED
                );
            )";

            Ydb::Scripting::ExecuteYqlRequest request;
            request.set_script(createTable);

            auto future = NRpcService::DoLocalRpc<TEvExecuteYqlRequest>(
                std::move(request), "", "", runtime.GetActorSystem(0));
            auto result = runtime.WaitFuture(std::move(future));
            Cerr << "Result: " << result.operation().ShortDebugString() << Endl;
        }

        Cerr << "3.Drain" << Endl;

        const auto nodeId = runtime.GetNodeId(tenants.List("/Root/db1").front());

        {
            runtime.SendToPipe(hiveTablet, sender, new TEvHive::TEvDrainNode(nodeId));
            TAutoPtr<IEventHandle> handle;
            auto drainResponse = runtime.GrabEdgeEventRethrow<TEvHive::TEvDrainNodeResult>(handle, TDuration::Seconds(30));
            UNIT_ASSERT_VALUES_EQUAL(drainResponse->Record.GetStatus(), NKikimrProto::EReplyStatus::OK);
        }

        Cerr << "4.Check whiteboard" << Endl;

        {
            TAutoPtr<IEventHandle> handle;
            TActorId whiteboard = NNodeWhiteboard::MakeNodeWhiteboardServiceId(nodeId);
            runtime.Send(new IEventHandle(whiteboard, sender, new NNodeWhiteboard::TEvWhiteboard::TEvTabletStateRequest()));
            NNodeWhiteboard::TEvWhiteboard::TEvTabletStateResponse* wbResponse = runtime.GrabEdgeEventRethrow<NNodeWhiteboard::TEvWhiteboard::TEvTabletStateResponse>(handle);
            ui64 aliveTablets = 0;
            for (const NKikimrWhiteboard::TTabletStateInfo& tabletInfo : wbResponse->Record.GetTabletStateInfo()) {
                if (tabletInfo.GetType() != NKikimrTabletBase::TTabletTypes::DataShard) {
                    continue;
                }
                if (tabletInfo.GetState() != NKikimrWhiteboard::TTabletStateInfo::Dead) {
                    Cerr << "Tablet " << tabletInfo.GetTabletId() << "." << tabletInfo.GetFollowerId()
                        << " is not dead yet (" << NKikimrWhiteboard::TTabletStateInfo::ETabletState_Name(tabletInfo.GetState()) << ")" << Endl;
                    ++aliveTablets;
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(aliveTablets, 0);
        }
    }

    Y_UNIT_TEST(TestSetDown) {
        TPortManager pm;
        Tests::TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetNodeCount(1)
            .SetDynamicNodeCount(2)
            .SetUseRealThreads(true)
            .AddStoragePoolType("ssd");

        Tests::TServer::TPtr server = new Tests::TServer(serverSettings);

        auto& runtime = *server->GetRuntime();
        auto cms = CreateTestBootstrapper(runtime, CreateTestTabletInfo(MakeCmsID(), TTabletTypes::Cms), &NCms::CreateCms);
        runtime.EnableScheduleForActor(cms);
        if (ENABLE_DETAILED_HIVE_LOG) {
            runtime.SetLogPriority(NKikimrServices::HIVE, NActors::NLog::PRI_TRACE);
            runtime.SetLogPriority(NKikimrServices::CMS, NActors::NLog::PRI_TRACE);
        }
        const auto sender = runtime.AllocateEdgeActor();
        const ui64 hiveTablet = Tests::ChangeStateStorage(Tests::Hive, serverSettings.Domain);

        Tests::TTenants tenants(server);

        Cerr << "1.Create tenant" << Endl;
        {
            Ydb::Cms::CreateDatabaseRequest request;
            request.set_path("/Root/db1");
            auto* resources = request.mutable_resources();
            auto* storage = resources->add_storage_units();
            storage->set_unit_kind("ssd");
            storage->set_count(1);
            tenants.CreateTenant(request, 2, TDuration::Minutes(1));
        }

        const auto nodeId = runtime.GetNodeId(tenants.List("/Root/db1").front());

        {
            Ydb::Maintenance::CreateMaintenanceTaskRequest request;
            request.mutable_task_options()->set_availability_mode(Ydb::Maintenance::AVAILABILITY_MODE_WEAK);
            request.mutable_task_options()->set_task_uid("cordon-task");
            auto* cordon = request.add_action_groups()->add_actions()->mutable_cordon_action();
            cordon->mutable_scope()->set_node_id(nodeId);

            auto ev = std::make_unique<NCms::TEvCms::TEvCreateMaintenanceTaskRequest>();
            ev->Record.SetUserSID("test-user");
            *ev->Record.MutableRequest() = std::move(request);
            runtime.SendToPipe(MakeCmsID(), sender, ev.release(), 0, GetPipeConfigWithRetries());
            TAutoPtr<IEventHandle> handle;
            auto reply = runtime.GrabEdgeEventRethrow<NCms::TEvCms::TEvMaintenanceTaskResponse>(handle);
            Cerr << "Cordon result: " << reply->Record.ShortDebugString() << Endl;
        }

        UNIT_ASSERT_VALUES_EQUAL(GetSimpleCounter(runtime, hiveTablet, NHive::COUNTER_NODES_DOWN), 1);

        {
            Ydb::Maintenance::DropMaintenanceTaskRequest request;
            request.set_task_uid("cordon-task");

            auto ev = std::make_unique<NCms::TEvCms::TEvDropMaintenanceTaskRequest>();
            *ev->Record.MutableRequest() = std::move(request);
            runtime.SendToPipe(MakeCmsID(), sender, ev.release(), 0, GetPipeConfigWithRetries());
            TAutoPtr<IEventHandle> handle;
            auto reply = runtime.GrabEdgeEventRethrow<NCms::TEvCms::TEvManageMaintenanceTaskResponse>(handle);
            Cerr << "Uncordon result: " << reply->Record.ShortDebugString() << Endl;
        }

        UNIT_ASSERT_VALUES_EQUAL(GetSimpleCounter(runtime, hiveTablet, NHive::COUNTER_NODES_DOWN), 0);
    }
}
