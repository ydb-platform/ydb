#include <ydb/core/client/minikql_compile/mkql_compile_service.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/gateway/kqp_metadata_loader.h>
#include <ydb/core/kqp/host/kqp_host.h>
#include <ydb/core/kqp/opt/rbo/verification/semantic_snapshot.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <yql/essentials/core/services/mounts/yql_mounts.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <yql/essentials/public/langver/yql_langver.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/system/env.h>
#include <util/system/shellcommand.h>

#include <mutex>

namespace NKikimr::NKqp {
namespace {

constexpr const char* TestCluster = "local_ut";

class TRecordingSemanticSnapshotSink final : public IRBOSemanticSnapshotSink {
public:
    void OnSemanticSnapshot(TRBOSemanticSnapshotBoundaryResultV1 result) override {
        std::lock_guard guard(Mutex);
        Results.push_back(std::move(result));
    }

    TVector<TRBOSemanticSnapshotBoundaryResultV1> Extract() {
        std::lock_guard guard(Mutex);
        return std::move(Results);
    }

private:
    std::mutex Mutex;
    TVector<TRBOSemanticSnapshotBoundaryResultV1> Results;
};

TIntrusivePtr<IKqpGateway> MakeGateway(Tests::TServer& server) {
    auto counters = MakeIntrusive<TKqpRequestCounters>();
    counters->Counters = new TKqpCounters(server.GetRuntime()->GetAppData(0).Counters);
    counters->TxProxyMon = new NTxProxy::TTxProxyMon(server.GetRuntime()->GetAppData(0).Counters);
    auto loader = std::make_shared<TKqpTableMetadataLoader>(
        TestCluster,
        server.GetRuntime()->GetAnyNodeActorSystem(),
        TIntrusivePtr<NYql::TKikimrConfiguration>(),
        false);
    return CreateKikimrIcGateway(
        TestCluster,
        NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY,
        "/Root",
        "/Root",
        std::move(loader),
        server.GetRuntime()->GetAnyNodeActorSystem(),
        server.GetRuntime()->GetNodeId(0),
        counters,
        server.GetSettings().AppConfig->GetQueryServiceConfig());
}

NYql::TKikimrConfiguration::TPtr MakeConfiguration() {
    auto config = MakeIntrusive<NYql::TKikimrConfiguration>();
    const auto defaultsData = NResource::Find("kqp_default_settings.txt");
    TStringInput defaultsStream(defaultsData);
    NKikimrKqp::TKqpDefaultSettings defaults;
    UNIT_ASSERT(TryParseFromTextFormat(defaultsStream, defaults));
    config->Init(
        defaults.GetDefaultSettings(),
        TestCluster,
        TVector<NKikimrKqp::TKqpSetting>{},
        true);
    config->SetEnableNewRBO(true);
    config->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    config->SetDefaultLangVer(NYql::GetMaxLangVersion());
    config->_ResultRowsLimit.Clear();
    return config;
}

TIntrusivePtr<IKqpHost> MakeHost(
    Tests::TServer& server,
    NYql::IModuleResolver::TPtr moduleResolver,
    std::shared_ptr<IRBOSemanticSnapshotSink> sink)
{
    return CreateKqpHost(
        MakeGateway(server),
        TestCluster,
        "/Root",
        MakeConfiguration(),
        std::move(moduleResolver),
        std::nullopt,
        nullptr,
        nullptr,
        server.GetSettings().AppConfig->GetQueryServiceConfig(),
        {},
        server.GetFunctionRegistry(),
        true,  // keepConfigChanges: preserve the cleared result-row cap.
        false, // isInternalCall
        nullptr,
        server.GetRuntime()->GetAnyNodeActorSystem(),
        nullptr,
        nullptr,
        false,
        std::move(sink));
}

NJson::TJsonValue ParseSnapshot(const TRBOSemanticSnapshotBoundaryResultV1& result) {
    UNIT_ASSERT_C(result.IsSupported(), result.UnsupportedReason);
    NJson::TJsonValue snapshot;
    UNIT_ASSERT_C(NJson::ReadJsonTree(result.Json, &snapshot, true), result.Json);
    return snapshot;
}

TVector<const NJson::TJsonValue*> PlanNodes(
    const NJson::TJsonValue& snapshot,
    const TString& op)
{
    TVector<const NJson::TJsonValue*> matches;
    for (const auto& node : snapshot["plan"]["nodes"].GetArraySafe()) {
        if (node["op"].GetStringSafe() == op) {
            matches.push_back(&node);
        }
    }
    return matches;
}

const NJson::TJsonValue& OnlyPlanNode(
    const NJson::TJsonValue& snapshot,
    const TString& op)
{
    const auto matches = PlanNodes(snapshot, op);
    UNIT_ASSERT_VALUES_EQUAL_C(matches.size(), 1, op);
    return *matches.front();
}

void AssertLimit(const NJson::TJsonValue& limit, const TString& phase) {
    UNIT_ASSERT_VALUES_EQUAL(limit["phase"].GetStringSafe(), phase);
    const auto& count = limit["count"];
    UNIT_ASSERT_VALUES_EQUAL(count["kind"].GetStringSafe(), "literal");
    UNIT_ASSERT_VALUES_EQUAL(count["type"].GetStringSafe(), "Uint64");
    UNIT_ASSERT_VALUES_EQUAL(count["value"].GetUIntegerSafe(), 1);
    UNIT_ASSERT(limit["offset"].IsNull());
}

void AssertSort(
    const NJson::TJsonValue& sort,
    const TString& phase,
    bool hasLimit)
{
    UNIT_ASSERT_VALUES_EQUAL(sort["phase"].GetStringSafe(), phase);
    const auto& order = sort["order"].GetArraySafe();
    UNIT_ASSERT_VALUES_EQUAL(order.size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(order[0]["ascending"].GetBooleanSafe(), false);
    UNIT_ASSERT_VALUES_EQUAL(order[0]["nulls_first"].GetBooleanSafe(), true);
    UNIT_ASSERT_VALUES_EQUAL(order[1]["ascending"].GetBooleanSafe(), true);
    UNIT_ASSERT_VALUES_EQUAL(order[1]["nulls_first"].GetBooleanSafe(), true);
    if (!hasLimit) {
        UNIT_ASSERT(sort["limit"].IsNull());
        return;
    }
    const auto& limit = sort["limit"];
    UNIT_ASSERT_VALUES_EQUAL(limit["kind"].GetStringSafe(), "literal");
    UNIT_ASSERT_VALUES_EQUAL(limit["type"].GetStringSafe(), "Uint64");
    UNIT_ASSERT_VALUES_EQUAL(limit["value"].GetUIntegerSafe(), 1);
}

void CreateOrderedColumnTable(TKikimrRunner& kikimr) {
    auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
    const auto result = session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/RboOrdered` (
            Id Uint64 NOT NULL,
            A Int64,
            B Int64,
            Payload Uint64,
            PRIMARY KEY (Id)
        ) WITH (STORE = COLUMN);
    )").GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

NJson::TJsonValue BuildVerificationProblem(
    const TRBOSemanticSnapshotBoundaryResultV1& initial,
    const TRBOSemanticSnapshotBoundaryResultV1& final)
{
    TTempDir tempDir;
    const auto initialPath = tempDir.Path() / "initial.json";
    const auto finalPath = tempDir.Path() / "final.json";
    const auto formulaPath = tempDir.Path() / "problem.smt2";
    TFileOutput(initialPath.GetPath()).Write(initial.Json);
    TFileOutput(finalPath.GetPath()).Write(final.Json);

    TShellCommand command(BinaryPath(
        "ydb/core/kqp/opt/rbo/verification/bin/kqp_rbo_verify"));
    command
        << initialPath.GetPath()
        << finalPath.GetPath()
        << "--rows" << "2"
        << "--timeout-ms" << "10000"
        << "--emit-smt" << formulaPath.GetPath();

    const auto solver = TryGetEnv("RBO_Z3");
    if (solver) {
        command << "--solver" << *solver;
    }
    command.Run();
    UNIT_ASSERT_C(
        command.GetExitCode().Defined() && command.GetExitCode().GetRef() == 0,
        command.GetError() << command.GetOutput());
    UNIT_ASSERT(formulaPath.Exists());

    NJson::TJsonValue verdict;
    UNIT_ASSERT_C(
        NJson::ReadJsonTree(command.GetOutput(), &verdict, true),
        command.GetOutput());
    return verdict;
}

} // namespace

Y_UNIT_TEST_SUITE(TRBOSemanticSnapshotIntegration) {
    Y_UNIT_TEST(RealHostProducesInitialAndFinalSnapshots) {
        TKikimrRunner kikimr;

        NYql::TExprContext moduleContext;
        NYql::IModuleResolver::TPtr moduleResolver;
        UNIT_ASSERT(NYql::GetYqlDefaultModuleResolver(moduleContext, moduleResolver));

        auto sink = std::make_shared<TRecordingSemanticSnapshotSink>();
        auto host = MakeHost(kikimr.GetTestServer(), std::move(moduleResolver), sink);
        const TString query = R"(--!syntax_v1
                SELECT Key FROM `/Root/KeyValue` LIMIT 1;
            )";
        IKqpHost::TPrepareSettings settings;
        settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;
        const auto prepared = host->SyncPrepareDataQuery(query, settings);
        UNIT_ASSERT_C(prepared.Success(), prepared.Issues().ToString());

        const auto results = sink->Extract();
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 2);
        UNIT_ASSERT(results[0].Boundary == ERBOSemanticSnapshotBoundaryV1::Initial);
        UNIT_ASSERT(results[1].Boundary == ERBOSemanticSnapshotBoundaryV1::Final);

        const auto initial = ParseSnapshot(results[0]);
        const auto final = ParseSnapshot(results[1]);
        UNIT_ASSERT_VALUES_EQUAL(initial["format"].GetStringSafe(), "ydb-rbo-semantic-snapshot");
        UNIT_ASSERT_VALUES_EQUAL(final["format"].GetStringSafe(), "ydb-rbo-semantic-snapshot");
        UNIT_ASSERT_VALUES_EQUAL(initial["version"].GetIntegerSafe(), 1);
        UNIT_ASSERT_VALUES_EQUAL(final["version"].GetIntegerSafe(), 1);
        UNIT_ASSERT(initial["stage_graph"].IsNull());
        UNIT_ASSERT(final["stage_graph"].IsMap());

        const auto& initialScan = OnlyPlanNode(initial, "scan");
        const auto& initialProject = OnlyPlanNode(initial, "project");
        const auto& initialLimit = OnlyPlanNode(initial, "limit");
        UNIT_ASSERT(initialScan["pushed_limit"].IsNull());
        AssertLimit(initialLimit, "undefined");
        UNIT_ASSERT_VALUES_EQUAL(
            initialProject["input"].GetStringSafe(),
            initialScan["id"].GetStringSafe());
        UNIT_ASSERT_VALUES_EQUAL(
            initialLimit["input"].GetStringSafe(),
            initialProject["id"].GetStringSafe());

        const auto& finalScan = OnlyPlanNode(final, "scan");
        UNIT_ASSERT(finalScan["pushed_limit"].IsNull());
        const auto finalLimits = PlanNodes(final, "limit");
        UNIT_ASSERT_VALUES_EQUAL(finalLimits.size(), 2);
        const NJson::TJsonValue* intermediateLimit = nullptr;
        const NJson::TJsonValue* finalLimit = nullptr;
        for (const auto* limit : finalLimits) {
            const auto phase = (*limit)["phase"].GetStringSafe();
            if (phase == "intermediate") {
                intermediateLimit = limit;
            } else if (phase == "final") {
                finalLimit = limit;
            }
        }
        UNIT_ASSERT(intermediateLimit);
        UNIT_ASSERT(finalLimit);
        AssertLimit(*intermediateLimit, "intermediate");
        AssertLimit(*finalLimit, "final");
        UNIT_ASSERT_VALUES_EQUAL(
            (*intermediateLimit)["input"].GetStringSafe(),
            finalScan["id"].GetStringSafe());
        UNIT_ASSERT_VALUES_EQUAL(
            (*finalLimit)["input"].GetStringSafe(),
            (*intermediateLimit)["id"].GetStringSafe());
        UNIT_ASSERT_VALUES_EQUAL(
            final["plan"]["root"].GetStringSafe(),
            (*finalLimit)["id"].GetStringSafe());

        const auto& stages = final["stage_graph"]["stages"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(stages.size(), 2);
        const NJson::TJsonValue* rowSource = nullptr;
        for (const auto& stage : stages) {
            if (!stage["source_storage"].IsNull() &&
                stage["source_storage"].GetStringSafe() == "row")
            {
                UNIT_ASSERT(!rowSource);
                rowSource = &stage;
            }
        }
        UNIT_ASSERT(rowSource);
        const auto& edges = final["stage_graph"]["edges"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(edges.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(edges[0]["kind"].GetStringSafe(), "union_all");
        UNIT_ASSERT_VALUES_EQUAL(edges[0]["parallel"].GetBooleanSafe(), false);
        UNIT_ASSERT_VALUES_EQUAL(
            edges[0]["producer"].GetStringSafe(),
            (*rowSource)["id"].GetStringSafe());
        UNIT_ASSERT_VALUES_EQUAL(
            edges[0]["consumer"].GetStringSafe(),
            final["stage_graph"]["root_stage"].GetStringSafe());

        const auto verdict = BuildVerificationProblem(results[0], results[1]);
        UNIT_ASSERT_VALUES_EQUAL(
            verdict["status"].GetStringSafe(),
            TryGetEnv("RBO_Z3") ? "VERIFIED_BOUNDED" : "FORMULA_EMITTED");
        UNIT_ASSERT_VALUES_EQUAL(verdict["row_bound"].GetIntegerSafe(), 2);
        UNIT_ASSERT_VALUES_EQUAL(verdict["task_bound"].GetIntegerSafe(), 2);
    }

    Y_UNIT_TEST(RealHostProducesTopSortAndMergeSnapshots) {
        TKikimrRunner kikimr;
        CreateOrderedColumnTable(kikimr);

        NYql::TExprContext moduleContext;
        NYql::IModuleResolver::TPtr moduleResolver;
        UNIT_ASSERT(NYql::GetYqlDefaultModuleResolver(moduleContext, moduleResolver));

        auto sink = std::make_shared<TRecordingSemanticSnapshotSink>();
        auto host = MakeHost(kikimr.GetTestServer(), std::move(moduleResolver), sink);
        const TString query = R"(--!syntax_v1
                SELECT A, B
                FROM `/Root/RboOrdered`
                ORDER BY A DESC, B ASC
                LIMIT 1;
            )";
        IKqpHost::TPrepareSettings settings;
        settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;
        const auto prepared = host->SyncPrepareDataQuery(query, settings);
        UNIT_ASSERT_C(prepared.Success(), prepared.Issues().ToString());

        const auto results = sink->Extract();
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 2);
        UNIT_ASSERT(results[0].Boundary == ERBOSemanticSnapshotBoundaryV1::Initial);
        UNIT_ASSERT(results[1].Boundary == ERBOSemanticSnapshotBoundaryV1::Final);
        const auto initial = ParseSnapshot(results[0]);
        const auto final = ParseSnapshot(results[1]);

        const auto& initialSort = OnlyPlanNode(initial, "sort");
        AssertSort(initialSort, "undefined", false);
        const auto& initialLimit = OnlyPlanNode(initial, "limit");
        AssertLimit(initialLimit, "undefined");
        const auto initialProjects = PlanNodes(initial, "project");
        UNIT_ASSERT_VALUES_EQUAL(initialProjects.size(), 2);
        const NJson::TJsonValue* orderedProject = nullptr;
        for (const auto* project : initialProjects) {
            if ((*project)["input"].GetStringSafe() ==
                initialSort["id"].GetStringSafe())
            {
                orderedProject = project;
            }
        }
        UNIT_ASSERT(orderedProject);
        UNIT_ASSERT_VALUES_EQUAL(
            (*orderedProject)["ordered"].GetBooleanSafe(),
            true);
        UNIT_ASSERT_VALUES_EQUAL(
            initialLimit["input"].GetStringSafe(),
            (*orderedProject)["id"].GetStringSafe());

        const auto& finalSort = OnlyPlanNode(final, "sort");
        AssertSort(finalSort, "intermediate", true);
        const auto& finalProject = OnlyPlanNode(final, "project");
        UNIT_ASSERT_VALUES_EQUAL(
            finalProject["input"].GetStringSafe(),
            finalSort["id"].GetStringSafe());
        UNIT_ASSERT_VALUES_EQUAL(finalProject["ordered"].GetBooleanSafe(), false);
        const auto& finalLimit = OnlyPlanNode(final, "limit");
        AssertLimit(finalLimit, "final");
        UNIT_ASSERT_VALUES_EQUAL(
            finalLimit["input"].GetStringSafe(),
            finalProject["id"].GetStringSafe());
        UNIT_ASSERT_VALUES_EQUAL(
            final["plan"]["root"].GetStringSafe(),
            finalLimit["id"].GetStringSafe());

        const auto& edges = final["stage_graph"]["edges"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(edges.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(edges[0]["kind"].GetStringSafe(), "merge");
        UNIT_ASSERT_VALUES_EQUAL(
            edges[0]["order"].GetArraySafe(),
            finalSort["order"].GetArraySafe());

        const auto verdict = BuildVerificationProblem(results[0], results[1]);
        UNIT_ASSERT_VALUES_EQUAL(
            verdict["status"].GetStringSafe(),
            TryGetEnv("RBO_Z3") ? "VERIFIED_BOUNDED" : "FORMULA_EMITTED");
        UNIT_ASSERT_VALUES_EQUAL(verdict["row_bound"].GetIntegerSafe(), 2);
        UNIT_ASSERT_VALUES_EQUAL(verdict["task_bound"].GetIntegerSafe(), 2);
    }
}

} // namespace NKikimr::NKqp
