#include "rpc_change_schema.h"
#include <ydb/core/base/localdb.h>
#include <ydb/core/protos/scheme_log.pb.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>

#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/protobuf/json/proto2json.h>
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

    Y_UNIT_TEST(ValidateCompactionPolicyChangeInDryRunAndApply) {
        TPortManager pm;
        TServerSettings settings(pm.GetPort(2134));
        settings.SetDomainName("Root").SetUseRealThreads(false);
        Tests::TServer::TPtr server = new TServer(settings);
        auto& runtime = *server->GetRuntime();
        server->SetupRootStoragePools(runtime.AllocateEdgeActor());
        const ui64 tabletId = ChangeStateStorage(Tests::SchemeRoot, server->GetSettings().Domain);

        using NTabletFlatScheme::TAlterRecord;
        using NTabletFlatScheme::TSchemeChanges;
        const NProtobufJson::TJson2ProtoConfig reader = {
            .FieldNameMode = NProtobufJson::TJson2ProtoConfig::FieldNameSnakeCaseDense,
            .MapAsObject = true,
            .EnumValueMode = NProtobufJson::TJson2ProtoConfig::EnumSnakeCaseInsensitive,
        };
        auto create = NProtobufJson::Json2Proto<TSchemeChanges>(MakeSchemaChange(), reader);
        const ui32 tableId = create.GetDelta(0).GetTableId();
        for (ui32 tier : {42u, 7u}) {
            auto* family = create.AddDelta();
            family->SetDeltaType(TAlterRecord::AddFamily);
            family->SetTableId(tableId);
            family->SetFamilyId(tier);
        }
        const auto policyChange = [&](const TVector<ui32>& tiers) {
            TSchemeChanges changes;
            auto* record = changes.AddDelta();
            record->SetTableId(tableId);
            record->SetDeltaType(TAlterRecord::SetCompactionPolicy);
            auto policy = NLocalDb::CreateDefaultUserTablePolicy();
            policy->Generations.resize(tiers.size(), policy->Generations.back());
            for (ui32 i = 0; i < tiers.size(); ++i) {
                policy->Generations[i].TierId = tiers[i];
            }
            policy->Serialize(*record->MutableCompactionPolicy());
            return changes;
        };
        const auto run = [&](const TSchemeChanges& changes, bool dryRun = false) {
            auto json = NProtobufJson::Proto2Json(changes, {
                .EnumMode = NProtobufJson::TProto2JsonConfig::EnumName,
                .FieldNameMode = NProtobufJson::TProto2JsonConfig::FieldNameSnakeCaseDense,
            });
            return runtime.WaitFuture(ChangeSchema(runtime, tabletId, json, {}, dryRun));
        };
        const auto checkTiers = [&](const Ydb::Tablet::ChangeTabletSchemaResponse& result, const TVector<ui32>& expected) {
            UNIT_ASSERT_VALUES_EQUAL_C(result.status(), Ydb::StatusIds::SUCCESS, result.DebugString());
            const auto snapshot = NProtobufJson::Json2Proto<TSchemeChanges>(result.schema(), reader);
            for (const auto& record : snapshot.GetDelta()) {
                if (record.GetTableId() == tableId && record.HasCompactionPolicy()) {
                    TVector<ui32> tiers;
                    for (const auto& generation : record.GetCompactionPolicy().GetGeneration()) {
                        tiers.push_back(generation.GetTierId());
                    }
                    UNIT_ASSERT_VALUES_EQUAL(tiers, expected);
                    return;
                }
            }
            UNIT_FAIL("Missing compaction policy for table " << tableId);
        };

        const TVector<ui32> original{0, 42, 7};
        create.MergeFrom(policyChange(original));
        checkTiers(run(create), original);
        const auto reordered = policyChange({0, 7, 42});
        for (bool dryRun : {true, false}) {
            const auto result = run(reordered, dryRun);
            UNIT_ASSERT_VALUES_EQUAL_C(result.status(), Ydb::StatusIds::GENERIC_ERROR, result.DebugString());
            UNIT_ASSERT_VALUES_EQUAL(result.issues_size(), 1);
            UNIT_ASSERT_STRING_CONTAINS(result.issues(0).message(), "Existing compaction tier slots must not be removed or reordered");
            checkTiers(run({}), original);
        }

        const TVector<ui32> extended{0, 42, 7, 7};
        const auto append = policyChange(extended);
        checkTiers(run(append, true), extended);
        checkTiers(run({}), original);
        checkTiers(run(append), extended);
        checkTiers(run({}), extended);
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
