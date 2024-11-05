#include "rpc_execute_mkql.h"
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NGRpcService {

using namespace Tests;

Y_UNIT_TEST_SUITE(TabletService_ExecuteMiniKQL) {

    Ydb::TypedValue MakeUint64(ui64 value) {
        Ydb::TypedValue ret;
        ret.mutable_type()->set_type_id(Ydb::Type::UINT64);
        ret.mutable_value()->set_uint64_value(value);
        return ret;
    }

    NThreading::TFuture<Ydb::Tablet::ExecuteTabletMiniKQLResponse> ExecuteMiniKQL(
            TTestActorRuntime& runtime, ui64 tabletId, const TString& program,
            const std::unordered_map<TString, Ydb::TypedValue>& params = {},
            const TString& token = {},
            bool dryRun = false)
    {
        // Cerr << "ExecuteMiniKQL: <<<" << program << ">>>" << Endl;
        Ydb::Tablet::ExecuteTabletMiniKQLRequest request;
        request.set_tablet_id(tabletId);
        request.set_program(program);
        for (const auto& pr : params) {
            (*request.mutable_parameters())[pr.first] = pr.second;
        }
        request.set_dry_run(dryRun);
        return NRpcService::DoLocalRpc<TEvExecuteTabletMiniKQLRequest>(
            std::move(request), "/Root", token, runtime.GetActorSystem(0));
    }

    Y_UNIT_TEST(BasicMiniKQLRead) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();

        auto sender = runtime.AllocateEdgeActor();
        server->SetupRootStoragePools(sender);

        ui64 schemeShardId = ChangeStateStorage(Tests::SchemeRoot, server->GetSettings().Domain);
        auto future = ExecuteMiniKQL(runtime, schemeShardId, R"___((
            (let key '('('Id (Uint64 '1))))
            (let select '('Id 'Name))
            (return (AsList
                (SetResult 'row (SelectRow 'Paths key select))
            ))
        ))___");
        auto result = runtime.WaitFuture(std::move(future));
        // Cerr << "Got result:\n" << result.DebugString();
        UNIT_ASSERT_VALUES_EQUAL_C(result.status(), Ydb::StatusIds::SUCCESS, result.DebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(
            result.result().value().ShortDebugString(),
            "items { items { uint64_value: 1 } items { text_value: \"Root\" } }",
            result.DebugString());
    }

    Y_UNIT_TEST(ParamsMiniKQLRead) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();

        auto sender = runtime.AllocateEdgeActor();
        server->SetupRootStoragePools(sender);

        ui64 schemeShardId = ChangeStateStorage(Tests::SchemeRoot, server->GetSettings().Domain);
        auto future = ExecuteMiniKQL(runtime, schemeShardId, R"___((
            (let p (Parameter 'p (DataType 'Uint64)))
            (let key '('('Id p)))
            (let select '('Id 'Name))
            (return (AsList
                (SetResult 'p p)
                (SetResult 'row (SelectRow 'Paths key select))
            ))
        ))___", {{"p", MakeUint64(1)}});
        auto result = runtime.WaitFuture(std::move(future));
        // Cerr << "Got result:\n" << result.DebugString();
        UNIT_ASSERT_VALUES_EQUAL_C(result.status(), Ydb::StatusIds::SUCCESS, result.DebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(
            result.result().value().ShortDebugString(),
            "items { uint64_value: 1 } items { items { uint64_value: 1 } items { text_value: \"Root\" } }",
            result.DebugString());
    }

    Ydb::TypedValue MakeMalformedValue() {
        Ydb::TypedValue ret;
        // Type is a struct with 2 members
        Ydb::StructType* s = ret.mutable_type()->mutable_struct_type();
        Ydb::StructMember* m1 = s->add_members();
        m1->set_name("m1");
        m1->mutable_type()->set_type_id(Ydb::Type::UINT64);
        Ydb::StructMember* m2 = s->add_members();
        m2->set_name("m2");
        m2->mutable_type()->set_type_id(Ydb::Type::UINT64);
        // Value has only one member: malformed
        ret.mutable_value()->add_items()->set_uint64_value(42);
        return ret;
    }

    Y_UNIT_TEST(MalformedParams) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();

        auto sender = runtime.AllocateEdgeActor();
        server->SetupRootStoragePools(sender);

        ui64 schemeShardId = ChangeStateStorage(Tests::SchemeRoot, server->GetSettings().Domain);
        auto future = ExecuteMiniKQL(runtime, schemeShardId, R"___((
            (let p (Parameter 'p (DataType 'Uint64)))
            (let key '('('Id p)))
            (let select '('Id 'Name))
            (return (AsList
                (SetResult 'p p)
                (SetResult 'row (SelectRow 'Paths key select))
            ))
        ))___", {{"p", MakeMalformedValue()}});
        auto result = runtime.WaitFuture(std::move(future));
        // Cerr << "Got result:\n" << result.DebugString();
        UNIT_ASSERT_VALUES_EQUAL_C(result.status(), Ydb::StatusIds::BAD_REQUEST, result.DebugString());
    }

    Y_UNIT_TEST(MalformedProgram) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();

        auto sender = runtime.AllocateEdgeActor();
        server->SetupRootStoragePools(sender);

        ui64 schemeShardId = ChangeStateStorage(Tests::SchemeRoot, server->GetSettings().Domain);
        auto future = ExecuteMiniKQL(runtime, schemeShardId, R"___((
            (let key '('('Id (Uint64 '1))))
            (let select '('Id 'Name))
            (return (AsList
                (SetResult 'row (SelectRow 'NoSuchTable key select))
            ))
        ))___");
        auto result = runtime.WaitFuture(std::move(future));
        // Cerr << "Got result:\n" << result.DebugString();
        UNIT_ASSERT_VALUES_EQUAL_C(result.status(), Ydb::StatusIds::GENERIC_ERROR, result.DebugString());
    }

    Y_UNIT_TEST(DryRunEraseRow) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();

        auto sender = runtime.AllocateEdgeActor();
        server->SetupRootStoragePools(sender);

        ui64 schemeShardId = ChangeStateStorage(Tests::SchemeRoot, server->GetSettings().Domain);
        auto future = ExecuteMiniKQL(runtime, schemeShardId, R"___((
            (let key '('('Id (Uint64 '1))))
            (return (AsList
                (EraseRow 'Paths key)
            ))
        ))___", {}, {}, /* dryRun */ true);
        auto result = runtime.WaitFuture(std::move(future));
        // Cerr << "Got result (dry run EraseRow):\n" << result.DebugString();
        UNIT_ASSERT_VALUES_EQUAL_C(result.status(), Ydb::StatusIds::SUCCESS, result.DebugString());

        future = ExecuteMiniKQL(runtime, schemeShardId, R"___((
            (let key '('('Id (Uint64 '1))))
            (let select '('Id 'Name))
            (return (AsList
                (SetResult 'row (SelectRow 'Paths key select))
            ))
        ))___");
        result = runtime.WaitFuture(std::move(future));
        // Cerr << "Got result (SelectRow):\n" << result.DebugString();
        UNIT_ASSERT_VALUES_EQUAL_C(result.status(), Ydb::StatusIds::SUCCESS, result.DebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(
            result.result().value().ShortDebugString(),
            "items { items { uint64_value: 1 } items { text_value: \"Root\" } }",
            result.DebugString());

        // Repeat request without dry_run
        future = ExecuteMiniKQL(runtime, schemeShardId, R"___((
            (let key '('('Id (Uint64 '1))))
            (return (AsList
                (EraseRow 'Paths key)
            ))
        ))___");
        result = runtime.WaitFuture(std::move(future));
        // Cerr << "Got result (EraseRow):\n" << result.DebugString();
        UNIT_ASSERT_VALUES_EQUAL_C(result.status(), Ydb::StatusIds::SUCCESS, result.DebugString());

        future = ExecuteMiniKQL(runtime, schemeShardId, R"___((
            (let key '('('Id (Uint64 '1))))
            (let select '('Id 'Name))
            (return (AsList
                (SetResult 'row (SelectRow 'Paths key select))
            ))
        ))___");
        result = runtime.WaitFuture(std::move(future));
        // Cerr << "Got result (SelectRow):\n" << result.DebugString();
        UNIT_ASSERT_VALUES_EQUAL_C(result.status(), Ydb::StatusIds::SUCCESS, result.DebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(
            result.result().value().ShortDebugString(),
            "items { nested_value { null_flag_value: NULL_VALUE } }",
            result.DebugString());
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
        auto future = ExecuteMiniKQL(runtime, schemeShardId, R"___((
            (let key '('('Id (Uint64 '1))))
            (let select '('Id 'Name))
            (return (AsList
                (SetResult 'row (SelectRow 'Paths key select))
            ))
        ))___");
        auto result = runtime.WaitFuture(std::move(future));
        // Cerr << "Got result:\n" << result.DebugString();
        UNIT_ASSERT_VALUES_EQUAL_C(result.status(), Ydb::StatusIds::UNAUTHORIZED, result.DebugString());

        future = ExecuteMiniKQL(runtime, schemeShardId, R"___((
            (let key '('('Id (Uint64 '1))))
            (let select '('Id 'Name))
            (return (AsList
                (SetResult 'row (SelectRow 'Paths key select))
            ))
        ))___", {}, NACLib::TUserToken("user@builtin", {}).SerializeAsString());
        result = runtime.WaitFuture(std::move(future));
        // Cerr << "Got result:\n" << result.DebugString();
        UNIT_ASSERT_VALUES_EQUAL_C(result.status(), Ydb::StatusIds::UNAUTHORIZED, result.DebugString());

        future = ExecuteMiniKQL(runtime, schemeShardId, R"___((
            (let key '('('Id (Uint64 '1))))
            (let select '('Id 'Name))
            (return (AsList
                (SetResult 'row (SelectRow 'Paths key select))
            ))
        ))___", {}, NACLib::TUserToken("root@builtin", {}).SerializeAsString());
        result = runtime.WaitFuture(std::move(future));
        // Cerr << "Got result:\n" << result.DebugString();
        UNIT_ASSERT_VALUES_EQUAL_C(result.status(), Ydb::StatusIds::SUCCESS, result.DebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(
            result.result().value().ShortDebugString(),
            "items { items { uint64_value: 1 } items { text_value: \"Root\" } }",
            result.DebugString());
    }

} // Y_UNIT_TEST_SUITE(TabletService_ExecuteMiniKQL)

} // namespace NKikimr::NGRpcService
