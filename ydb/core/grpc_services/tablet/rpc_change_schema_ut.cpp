#include "rpc_change_schema.h"
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NGRpcService {

using namespace Tests;

Y_UNIT_TEST_SUITE(TabletService_ChangeSchema) {

    NThreading::TFuture<Ydb::Tablet::ChangeTabletSchemaResponse> ChangeSchema(
            TTestActorRuntime& runtime, ui64 tabletId, const TString& changes,
            const TString& token = {},
            bool dryRun = false)
    {
        // Cerr << "ChangeSchema: <<<" << changes << ">>>" << Endl;
        Ydb::Tablet::ChangeTabletSchemaRequest request;
        request.set_tablet_id(tabletId);
        request.set_schema_changes(changes);
        request.set_dry_run(dryRun);
        return NRpcService::DoLocalRpc<TEvChangeTabletSchemaRequest>(
            std::move(request), "/Root", token, runtime.GetActorSystem(0));
    }

    TString MakeSchemaChange() {
        return R"__(
            {"delta": [
                {"delta_type": "AddTable",
                 "table_id": 5555,
                 "table_name": "MyAwesomeTable"},
                {"delta_type": "AddColumn",
                 "table_id": 5555,
                 "column_id": 1,
                 "column_name": "MyAwesomeKey",
                 "column_type": 4},
                {"delta_type": "AddColumnToKey",
                 "table_id": 5555,
                 "column_id": 1}
            ]}
        )__";
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

        Cerr << "... reading schema" << Endl;
        auto future = ChangeSchema(runtime, schemeShardId, "");
        auto result = runtime.WaitFuture(std::move(future));
        // Cerr << "Got result:\n" << result.DebugString();
        UNIT_ASSERT_VALUES_EQUAL_C(result.status(), Ydb::StatusIds::SUCCESS, result.DebugString());
        UNIT_ASSERT_C(result.schema().StartsWith(R"__({"delta":[{"delta_type":"AddTable","table_id":1,"table_name":"Paths"},)__"), result.schema());

        Cerr << "... changing schema (dry run)" << Endl;
        future = ChangeSchema(runtime, schemeShardId, MakeSchemaChange(), {}, /* dryRun */ true);
        result = runtime.WaitFuture(std::move(future));
        UNIT_ASSERT_VALUES_EQUAL_C(result.status(), Ydb::StatusIds::SUCCESS, result.DebugString());
        UNIT_ASSERT_C(result.schema().Contains(R"__({"delta_type":"AddTable","table_id":5555,"table_name":"MyAwesomeTable"})__"), result.schema());

        Cerr << "... reading schema" << Endl;
        future = ChangeSchema(runtime, schemeShardId, "");
        result = runtime.WaitFuture(std::move(future));
        // Cerr << "Got result:\n" << result.DebugString();
        UNIT_ASSERT_VALUES_EQUAL_C(result.status(), Ydb::StatusIds::SUCCESS, result.DebugString());
        UNIT_ASSERT_C(!result.schema().Contains("MyAwesomeTable"), result.schema());

        Cerr << "... changing schema" << Endl;
        future = ChangeSchema(runtime, schemeShardId, MakeSchemaChange());
        result = runtime.WaitFuture(std::move(future));
        UNIT_ASSERT_VALUES_EQUAL_C(result.status(), Ydb::StatusIds::SUCCESS, result.DebugString());
        UNIT_ASSERT_C(result.schema().Contains(R"__({"delta_type":"AddTable","table_id":5555,"table_name":"MyAwesomeTable"})__"), result.schema());

        Cerr << "... reading schema" << Endl;
        future = ChangeSchema(runtime, schemeShardId, "");
        result = runtime.WaitFuture(std::move(future));
        // Cerr << "Got result:\n" << result.DebugString();
        UNIT_ASSERT_VALUES_EQUAL_C(result.status(), Ydb::StatusIds::SUCCESS, result.DebugString());
        UNIT_ASSERT_C(result.schema().Contains(R"__({"delta_type":"AddTable","table_id":5555,"table_name":"MyAwesomeTable"})__"), result.schema());
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

        Cerr << "... reading schema (without token)" << Endl;
        auto future = ChangeSchema(runtime, schemeShardId, "");
        auto result = runtime.WaitFuture(std::move(future));
        // Cerr << "Got result:\n" << result.DebugString();
        UNIT_ASSERT_VALUES_EQUAL_C(result.status(), Ydb::StatusIds::UNAUTHORIZED, result.DebugString());

        Cerr << "... reading schema (non-admin token)" << Endl;
        future = ChangeSchema(runtime, schemeShardId, "", NACLib::TUserToken("user@builtin", {}).SerializeAsString());
        result = runtime.WaitFuture(std::move(future));
        // Cerr << "Got result:\n" << result.DebugString();
        UNIT_ASSERT_VALUES_EQUAL_C(result.status(), Ydb::StatusIds::UNAUTHORIZED, result.DebugString());

        Cerr << "... reading schema (admin token)" << Endl;
        future = ChangeSchema(runtime, schemeShardId, "", NACLib::TUserToken("root@builtin", {}).SerializeAsString());
        result = runtime.WaitFuture(std::move(future));
        // Cerr << "Got result:\n" << result.DebugString();
        UNIT_ASSERT_VALUES_EQUAL_C(result.status(), Ydb::StatusIds::SUCCESS, result.DebugString());
    }

} // Y_UNIT_TEST_SUITE(TabletService_ChangeSchema)

} // namespace NKikimr::NGRpcService
