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
#include <regex>

namespace NKikimr::NKqp {
namespace {

constexpr const char* TestCluster = "local_ut";

class TRecordingSemanticSnapshotSink final : public IRBOSemanticSnapshotSink {
public:
    explicit TRecordingSemanticSnapshotSink(
        std::optional<ui64> transformationPrefixTarget = std::nullopt)
        : TransformationPrefixTarget(transformationPrefixTarget)
    {
    }

    void OnSemanticSnapshot(TRBOSemanticSnapshotBoundaryResultV1 result) override {
        std::lock_guard guard(Mutex);
        Results.push_back(std::move(result));
    }

    std::optional<ui64> GetTransformationPrefixTarget() const override {
        return TransformationPrefixTarget;
    }

    TVector<TRBOSemanticSnapshotBoundaryResultV1> Extract() {
        std::lock_guard guard(Mutex);
        return std::move(Results);
    }

private:
    const std::optional<ui64> TransformationPrefixTarget;
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
    config->SetEnableFallbackToYqlOptimizer(false);
    config->SetAllowOlapDataQuery(true);
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

void CollectConjuncts(
    const NJson::TJsonValue& expression,
    TVector<const NJson::TJsonValue*>& conjuncts)
{
    if (expression["kind"].GetStringSafe() != "and") {
        conjuncts.push_back(&expression);
        return;
    }
    for (const auto& argument : expression["args"].GetArraySafe()) {
        CollectConjuncts(argument, conjuncts);
    }
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

void CreateSqlInColumnTable(TKikimrRunner& kikimr) {
    auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
    const auto result = session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/RboSqlIn` (
            Id Uint64 NOT NULL,
            S String,
            N Int64,
            PRIMARY KEY (Id)
        ) WITH (STORE = COLUMN);
    )").GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

TKikimrRunner MakeTpcdsRunner() {
    NKikimrConfig::TAppConfig appConfig;
    auto* service = appConfig.MutableTableServiceConfig();
    service->SetEnableNewRBO(true);
    service->SetEnableFallbackToYqlOptimizer(false);
    service->SetAllowOlapDataQuery(true);
    service->SetDefaultLangVer(NYql::GetMaxLangVersion());
    service->SetBackportMode(
        NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    return TKikimrRunner(TKikimrSettings(appConfig).SetWithSampleTables(false));
}

void CreateTpcdsColumnTables(TKikimrRunner& kikimr) {
    const TString path = ArcadiaSourceRoot() +
        "/ydb/core/kqp/ut/rbo/data/schema/tpcds.sql";
    std::string schema = TFileInput(path).ReadAll();
    const std::regex table(R"(CREATE TABLE [^\(]+ \([^;]*\))", std::regex::multiline);
    schema = std::regex_replace(
        schema,
        table,
        "$& WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 16);");

    auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
    const auto result = session.ExecuteSchemeQuery(TString(schema)).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

TString TpcdsQuery96() {
    const TString prelude = R"(
$to_decimal = ($x) -> { return cast($x as Decimal(12, 2)); };
$to_decimal_max_precision = ($x) -> { return cast($x as Decimal(35, 2)); };
$round = ($x,$y) -> { return $x; };
)";
    return prelude + TFileInput(
        ArcadiaSourceRoot() +
        "/ydb/core/kqp/ut/rbo/data/yql-tpcds/q96.yql").ReadAll();
}

NJson::TJsonValue BuildVerificationProblem(
    const TRBOSemanticSnapshotBoundaryResultV1& initial,
    const TRBOSemanticSnapshotBoundaryResultV1& final,
    ui64 timeoutMs = 10'000,
    bool diagnosticTransformationPrefix = false)
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
        << "--timeout-ms" << ToString(timeoutMs)
        << "--emit-smt" << formulaPath.GetPath();
    if (diagnosticTransformationPrefix) {
        command << "--diagnostic-transformation-prefix";
    }

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
    Y_UNIT_TEST(RealHostStopsAtNoOpConstantFoldingAtomicCheckpoint) {
        TKikimrRunner kikimr;

        NYql::TExprContext moduleContext;
        NYql::IModuleResolver::TPtr moduleResolver;
        UNIT_ASSERT(NYql::GetYqlDefaultModuleResolver(moduleContext, moduleResolver));

        auto sink = std::make_shared<TRecordingSemanticSnapshotSink>(1);
        auto host = MakeHost(kikimr.GetTestServer(), std::move(moduleResolver), sink);
        const TString query = R"(--!syntax_v1
                SELECT Key FROM `/Root/KeyValue` LIMIT 1;
            )";
        IKqpHost::TPrepareSettings settings;
        settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;
        const auto prepared = host->SyncPrepareDataQuery(query, settings);
        UNIT_ASSERT(!prepared.Success());

        const auto results = sink->Extract();
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 2);
        UNIT_ASSERT(results[0].Boundary == ERBOSemanticSnapshotBoundaryV1::Initial);
        UNIT_ASSERT(
            results[1].Boundary ==
            ERBOSemanticSnapshotBoundaryV1::TransformationPrefix);
        UNIT_ASSERT(results[0].TransformationEvents.empty());
        UNIT_ASSERT_VALUES_EQUAL(results[1].TransformationEvents.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(results[1].TransformationEvents[0].Ordinal, 1);
        UNIT_ASSERT(
            results[1].TransformationEvents[0].Kind ==
            ERBOTransformationEventKindV1::AtomicStageCommit);
        UNIT_ASSERT_VALUES_EQUAL(
            results[1].TransformationEvents[0].Stage,
            "Constant folding stage");
        UNIT_ASSERT_VALUES_EQUAL(
            results[1].TransformationEvents[0].Name,
            "Fold constant expressions");

        const auto initial = ParseSnapshot(results[0]);
        const auto prefix = ParseSnapshot(results[1]);
        UNIT_ASSERT(initial["stage_graph"].IsNull());
        UNIT_ASSERT(prefix["stage_graph"].IsNull());
        const auto verdict = BuildVerificationProblem(
            results[0],
            results[1],
            10'000,
            true);
        UNIT_ASSERT_VALUES_EQUAL(
            verdict["status"].GetStringSafe(),
            TryGetEnv("RBO_Z3") ? "VERIFIED_BOUNDED" : "FORMULA_EMITTED");
        UNIT_ASSERT_VALUES_EQUAL(
            verdict["comparison_scope"].GetStringSafe(),
            "OPTIMIZER_TRANSFORMATION_PREFIX");
    }

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
        UNIT_ASSERT(results[0].TransformationEvents.empty());
        UNIT_ASSERT(results[1].TransformationEvents.empty());

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

    Y_UNIT_TEST(RealHostCompletionAndHashPrefixIncludeAtomicStages) {
        TKikimrRunner kikimr;
        const TString query = R"(--!syntax_v1
                SELECT Key FROM `/Root/KeyValue` LIMIT 1;
            )";
        IKqpHost::TPrepareSettings settings;
        settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;

        TVector<TRBOSemanticSnapshotBoundaryResultV1> complete;
        {
            NYql::TExprContext moduleContext;
            NYql::IModuleResolver::TPtr moduleResolver;
            UNIT_ASSERT(NYql::GetYqlDefaultModuleResolver(moduleContext, moduleResolver));

            auto sink = std::make_shared<TRecordingSemanticSnapshotSink>(10'000);
            auto host = MakeHost(kikimr.GetTestServer(), std::move(moduleResolver), sink);
            const auto prepared = host->SyncPrepareDataQuery(query, settings);
            UNIT_ASSERT_C(prepared.Success(), prepared.Issues().ToString());
            complete = sink->Extract();
        }

        UNIT_ASSERT_VALUES_EQUAL(complete.size(), 2);
        UNIT_ASSERT(complete[0].Boundary == ERBOSemanticSnapshotBoundaryV1::Initial);
        UNIT_ASSERT(complete[1].Boundary == ERBOSemanticSnapshotBoundaryV1::Final);
        UNIT_ASSERT(complete[0].TransformationEvents.empty());
        UNIT_ASSERT(!complete[1].TransformationEvents.empty());
        UNIT_ASSERT(ParseSnapshot(complete[1])["stage_graph"].IsMap());

        std::optional<ui64> constantFoldingOrdinal;
        std::optional<ui64> hashPropagationOrdinal;
        const auto& events = complete[1].TransformationEvents;
        for (ui64 index = 0; index < events.size(); ++index) {
            const auto& event = events[index];
            UNIT_ASSERT_VALUES_EQUAL(event.Ordinal, index + 1);
            UNIT_ASSERT(!event.Stage.empty());
            UNIT_ASSERT(!event.Name.empty());

            if (event.Stage == "Constant folding stage") {
                UNIT_ASSERT(!constantFoldingOrdinal);
                UNIT_ASSERT(
                    event.Kind ==
                    ERBOTransformationEventKindV1::AtomicStageCommit);
                UNIT_ASSERT_VALUES_EQUAL(event.Name, "Fold constant expressions");
                constantFoldingOrdinal = event.Ordinal;
            } else if (event.Stage == "Hash function propagation") {
                UNIT_ASSERT(!hashPropagationOrdinal);
                UNIT_ASSERT(
                    event.Kind ==
                    ERBOTransformationEventKindV1::AtomicStageCommit);
                UNIT_ASSERT_VALUES_EQUAL(event.Name, "Propagate hash functions");
                hashPropagationOrdinal = event.Ordinal;
            }
        }

        UNIT_ASSERT(constantFoldingOrdinal);
        UNIT_ASSERT(hashPropagationOrdinal);
        UNIT_ASSERT(*constantFoldingOrdinal < *hashPropagationOrdinal);
        UNIT_ASSERT_VALUES_EQUAL(*hashPropagationOrdinal, events.size());

        TVector<TRBOSemanticSnapshotBoundaryResultV1> stopped;
        {
            NYql::TExprContext moduleContext;
            NYql::IModuleResolver::TPtr moduleResolver;
            UNIT_ASSERT(NYql::GetYqlDefaultModuleResolver(moduleContext, moduleResolver));

            auto sink = std::make_shared<TRecordingSemanticSnapshotSink>(
                *hashPropagationOrdinal);
            auto host = MakeHost(kikimr.GetTestServer(), std::move(moduleResolver), sink);
            const auto prepared = host->SyncPrepareDataQuery(query, settings);
            UNIT_ASSERT(!prepared.Success());
            stopped = sink->Extract();
        }

        UNIT_ASSERT_VALUES_EQUAL(stopped.size(), 2);
        UNIT_ASSERT(stopped[0].Boundary == ERBOSemanticSnapshotBoundaryV1::Initial);
        UNIT_ASSERT(
            stopped[1].Boundary ==
            ERBOSemanticSnapshotBoundaryV1::TransformationPrefix);
        UNIT_ASSERT_VALUES_EQUAL(stopped[1].TransformationEvents.size(), events.size());
        for (ui64 index = 0; index < events.size(); ++index) {
            const auto& expected = events[index];
            const auto& actual = stopped[1].TransformationEvents[index];
            UNIT_ASSERT_VALUES_EQUAL(actual.Ordinal, expected.Ordinal);
            UNIT_ASSERT(actual.Kind == expected.Kind);
            UNIT_ASSERT_VALUES_EQUAL(actual.Stage, expected.Stage);
            UNIT_ASSERT_VALUES_EQUAL(actual.Name, expected.Name);
        }
        UNIT_ASSERT(ParseSnapshot(stopped[1])["stage_graph"].IsMap());
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

    Y_UNIT_TEST(RealHostVerifiesExplicitIntegerArithmetic) {
        TKikimrRunner kikimr;
        CreateOrderedColumnTable(kikimr);

        NYql::TExprContext moduleContext;
        NYql::IModuleResolver::TPtr moduleResolver;
        UNIT_ASSERT(NYql::GetYqlDefaultModuleResolver(moduleContext, moduleResolver));

        auto sink = std::make_shared<TRecordingSemanticSnapshotSink>();
        auto host = MakeHost(kikimr.GetTestServer(), std::move(moduleResolver), sink);
        const TString query = R"(--!syntax_v1
                SELECT A + 1l AS Adjusted
                FROM `/Root/RboOrdered`;
            )";
        IKqpHost::TPrepareSettings settings;
        settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;
        const auto prepared = host->SyncPrepareDataQuery(query, settings);
        UNIT_ASSERT_C(prepared.Success(), prepared.Issues().ToString());

        const auto results = sink->Extract();
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 2);
        const auto initial = ParseSnapshot(results[0]);
        const auto final = ParseSnapshot(results[1]);

        const auto additions = [](const NJson::TJsonValue& snapshot) {
            TVector<const NJson::TJsonValue*> result;
            for (const auto* project : PlanNodes(snapshot, "project")) {
                for (const auto& column : (*project)["columns"].GetArraySafe()) {
                    const auto& expression = column["expression"];
                    if (expression["kind"].GetStringSafe() == "add") {
                        result.push_back(&expression);
                    }
                }
            }
            return result;
        };

        const auto initialAdditions = additions(initial);
        const auto finalAdditions = additions(final);
        UNIT_ASSERT_VALUES_EQUAL(initialAdditions.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(finalAdditions.size(), 1);
        const auto assertAddition = [](const NJson::TJsonValue& expression) {
            UNIT_ASSERT_VALUES_EQUAL(expression["type"].GetStringSafe(), "Int64");
            UNIT_ASSERT(expression["nullable"].GetBooleanSafe());
            UNIT_ASSERT_VALUES_EQUAL(expression["left"]["kind"].GetStringSafe(), "column");
            UNIT_ASSERT_VALUES_EQUAL(expression["right"]["kind"].GetStringSafe(), "literal");
            UNIT_ASSERT_VALUES_EQUAL(expression["right"]["type"].GetStringSafe(), "Int64");
            UNIT_ASSERT_VALUES_EQUAL(expression["right"]["value"].GetIntegerSafe(), 1);
        };
        assertAddition(*initialAdditions[0]);
        assertAddition(*finalAdditions[0]);

        const auto verdict = BuildVerificationProblem(results[0], results[1]);
        UNIT_ASSERT_VALUES_EQUAL(
            verdict["status"].GetStringSafe(),
            TryGetEnv("RBO_Z3") ? "VERIFIED_BOUNDED" : "FORMULA_EMITTED");
        UNIT_ASSERT_VALUES_EQUAL(verdict["row_bound"].GetIntegerSafe(), 2);
        UNIT_ASSERT_VALUES_EQUAL(verdict["task_bound"].GetIntegerSafe(), 2);
    }

    Y_UNIT_TEST(RealHostVerifiesStaticSqlInWithNullableLookups) {
        TKikimrRunner kikimr;
        CreateSqlInColumnTable(kikimr);

        NYql::TExprContext moduleContext;
        NYql::IModuleResolver::TPtr moduleResolver;
        UNIT_ASSERT(NYql::GetYqlDefaultModuleResolver(moduleContext, moduleResolver));

        auto sink = std::make_shared<TRecordingSemanticSnapshotSink>();
        auto host = MakeHost(kikimr.GetTestServer(), std::move(moduleResolver), sink);
        const TString query = R"(--!syntax_v1
                SELECT
                    S IN ('first', 'second') AS StringMatched,
                    N IN (1, 2) AS IntegerMatched
                FROM `/Root/RboSqlIn`;
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
        ParseSnapshot(results[1]);

        TVector<const NJson::TJsonValue*> memberships;
        for (const auto* project : PlanNodes(initial, "project")) {
            for (const auto& column : (*project)["columns"].GetArraySafe()) {
                const auto& expression = column["expression"];
                if (expression["kind"].GetStringSafe() == "in") {
                    memberships.push_back(&expression);
                }
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(memberships.size(), 2);
        THashMap<TString, const NJson::TJsonValue*> byItemType;
        for (const auto* membership : memberships) {
            UNIT_ASSERT_VALUES_EQUAL(membership->GetMapSafe().size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(
                (*membership)["lookup"]["kind"].GetStringSafe(),
                "column");
            const auto& items = (*membership)["items"].GetArraySafe();
            UNIT_ASSERT_VALUES_EQUAL(items.size(), 2);
            byItemType.emplace(items[0]["type"].GetStringSafe(), membership);
        }
        UNIT_ASSERT_VALUES_EQUAL(byItemType.size(), 2);
        const auto& stringItems = (*byItemType.at("String"))["items"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(stringItems[0]["value"].GetStringSafe(), "first");
        UNIT_ASSERT_VALUES_EQUAL(stringItems[1]["value"].GetStringSafe(), "second");
        const auto& integerItems = (*byItemType.at("Int32"))["items"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(integerItems[0]["value"].GetIntegerSafe(), 1);
        UNIT_ASSERT_VALUES_EQUAL(integerItems[1]["value"].GetIntegerSafe(), 2);

        const auto verdict = BuildVerificationProblem(results[0], results[1]);
        UNIT_ASSERT_VALUES_EQUAL(
            verdict["status"].GetStringSafe(),
            TryGetEnv("RBO_Z3") ? "VERIFIED_BOUNDED" : "FORMULA_EMITTED");
        UNIT_ASSERT_VALUES_EQUAL(verdict["row_bound"].GetIntegerSafe(), 2);
        UNIT_ASSERT_VALUES_EQUAL(verdict["task_bound"].GetIntegerSafe(), 2);
    }

    Y_UNIT_TEST(RealHostVerifiesPushedOlapFilter) {
        TKikimrRunner kikimr;
        CreateOrderedColumnTable(kikimr);

        NYql::TExprContext moduleContext;
        NYql::IModuleResolver::TPtr moduleResolver;
        UNIT_ASSERT(NYql::GetYqlDefaultModuleResolver(moduleContext, moduleResolver));

        auto sink = std::make_shared<TRecordingSemanticSnapshotSink>();
        auto host = MakeHost(kikimr.GetTestServer(), std::move(moduleResolver), sink);
        const TString query = R"(--!syntax_v1
                SELECT Id, A, B, Payload
                FROM `/Root/RboOrdered`
                WHERE A >= 30 AND B == 5;
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

        const auto& initialScan = OnlyPlanNode(initial, "scan");
        UNIT_ASSERT(initialScan["predicate"].IsNull());
        const auto& initialFilter = OnlyPlanNode(initial, "filter");
        UNIT_ASSERT_VALUES_EQUAL(
            initialFilter["predicate"]["kind"].GetStringSafe(),
            "and");

        const auto& finalScan = OnlyPlanNode(final, "scan");
        UNIT_ASSERT(!finalScan["predicate"].IsNull());
        UNIT_ASSERT(finalScan["pushed_limit"].IsNull());
        UNIT_ASSERT_VALUES_EQUAL(
            finalScan["predicate"]["kind"].GetStringSafe(),
            "and");
        const auto& conjuncts = finalScan["predicate"]["args"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(conjuncts.size(), 2);
        THashSet<TString> kinds;
        THashSet<TString> columns;
        for (const auto& conjunct : conjuncts) {
            const TString kind = conjunct["kind"].GetStringSafe();
            const TString column = conjunct["left"]["column"].GetStringSafe();
            kinds.insert(kind);
            columns.insert(column);
            UNIT_ASSERT_VALUES_EQUAL(
                conjunct["right"]["kind"].GetStringSafe(),
                "literal");
            UNIT_ASSERT_VALUES_EQUAL(
                conjunct["right"]["type"].GetStringSafe(),
                "Int32");
            if (column == "A") {
                UNIT_ASSERT_VALUES_EQUAL(kind, "gte");
                UNIT_ASSERT_VALUES_EQUAL(
                    conjunct["right"]["value"].GetIntegerSafe(),
                    30);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(column, "B");
                UNIT_ASSERT_VALUES_EQUAL(kind, "eq");
                UNIT_ASSERT_VALUES_EQUAL(
                    conjunct["right"]["value"].GetIntegerSafe(),
                    5);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(kinds, THashSet<TString>({"eq", "gte"}));
        UNIT_ASSERT_VALUES_EQUAL(columns, THashSet<TString>({"A", "B"}));
        UNIT_ASSERT(PlanNodes(final, "filter").empty());

        const auto verdict = BuildVerificationProblem(results[0], results[1]);
        UNIT_ASSERT_VALUES_EQUAL(
            verdict["status"].GetStringSafe(),
            TryGetEnv("RBO_Z3") ? "VERIFIED_BOUNDED" : "FORMULA_EMITTED");
        UNIT_ASSERT_VALUES_EQUAL(verdict["row_bound"].GetIntegerSafe(), 2);
        UNIT_ASSERT_VALUES_EQUAL(verdict["task_bound"].GetIntegerSafe(), 2);
    }

    Y_UNIT_TEST(RealHostVerifiesTpcdsQuery96) {
        auto kikimr = MakeTpcdsRunner();
        CreateTpcdsColumnTables(kikimr);

        NYql::TExprContext moduleContext;
        NYql::IModuleResolver::TPtr moduleResolver;
        UNIT_ASSERT(NYql::GetYqlDefaultModuleResolver(moduleContext, moduleResolver));

        auto sink = std::make_shared<TRecordingSemanticSnapshotSink>();
        auto host = MakeHost(kikimr.GetTestServer(), std::move(moduleResolver), sink);
        IKqpHost::TPrepareSettings settings;
        settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;
        const auto prepared = host->SyncPrepareDataQuery(TpcdsQuery96(), settings);
        UNIT_ASSERT_C(prepared.Success(), prepared.Issues().ToString());

        const auto results = sink->Extract();
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 2);
        UNIT_ASSERT(results[0].Boundary == ERBOSemanticSnapshotBoundaryV1::Initial);
        UNIT_ASSERT(results[1].Boundary == ERBOSemanticSnapshotBoundaryV1::Final);
        const auto initial = ParseSnapshot(results[0]);
        const auto final = ParseSnapshot(results[1]);

        UNIT_ASSERT_VALUES_EQUAL(PlanNodes(initial, "scan").size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(PlanNodes(final, "scan").size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(PlanNodes(initial, "join").size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(PlanNodes(final, "join").size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(PlanNodes(final, "aggregate").size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(PlanNodes(final, "sort").size(), 1);

        for (const auto* scan : PlanNodes(initial, "scan")) {
            UNIT_ASSERT((*scan)["predicate"].IsNull());
        }
        const auto initialFilters = PlanNodes(initial, "filter");
        UNIT_ASSERT_VALUES_EQUAL(initialFilters.size(), 1);
        TVector<const NJson::TJsonValue*> initialConjuncts;
        CollectConjuncts((*initialFilters.front())["predicate"], initialConjuncts);
        UNIT_ASSERT_VALUES_EQUAL(initialConjuncts.size(), 7);

        UNIT_ASSERT(PlanNodes(final, "filter").empty());
        TVector<const NJson::TJsonValue*> pushedConjuncts;
        size_t scansWithPredicate = 0;
        for (const auto* scan : PlanNodes(final, "scan")) {
            const auto& predicate = (*scan)["predicate"];
            if (!predicate.IsNull()) {
                ++scansWithPredicate;
                CollectConjuncts(predicate, pushedConjuncts);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(scansWithPredicate, 3);
        UNIT_ASSERT_VALUES_EQUAL(pushedConjuncts.size(), 4);
        THashSet<TString> pushedColumns;
        for (const auto* predicate : pushedConjuncts) {
            UNIT_ASSERT_VALUES_EQUAL((*predicate)["left"]["kind"].GetStringSafe(), "column");
            UNIT_ASSERT_VALUES_EQUAL((*predicate)["right"]["kind"].GetStringSafe(), "literal");
            const TString column = (*predicate)["left"]["column"].GetStringSafe();
            UNIT_ASSERT(pushedColumns.insert(column).second);
            const TString kind = (*predicate)["kind"].GetStringSafe();
            const auto& literal = (*predicate)["right"];
            if (column == "time_dim.t_hour") {
                UNIT_ASSERT_VALUES_EQUAL(kind, "eq");
                UNIT_ASSERT_VALUES_EQUAL(literal["type"].GetStringSafe(), "Int32");
                UNIT_ASSERT_VALUES_EQUAL(literal["value"].GetIntegerSafe(), 8);
            } else if (column == "time_dim.t_minute") {
                UNIT_ASSERT_VALUES_EQUAL(kind, "gte");
                UNIT_ASSERT_VALUES_EQUAL(literal["type"].GetStringSafe(), "Int32");
                UNIT_ASSERT_VALUES_EQUAL(literal["value"].GetIntegerSafe(), 30);
            } else if (column == "household_demographics.hd_dep_count") {
                UNIT_ASSERT_VALUES_EQUAL(kind, "eq");
                UNIT_ASSERT_VALUES_EQUAL(literal["type"].GetStringSafe(), "Int32");
                UNIT_ASSERT_VALUES_EQUAL(literal["value"].GetIntegerSafe(), 5);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(column, "store.s_store_name");
                UNIT_ASSERT_VALUES_EQUAL(kind, "eq");
                UNIT_ASSERT_VALUES_EQUAL(literal["type"].GetStringSafe(), "String");
                UNIT_ASSERT_VALUES_EQUAL(literal["value"].GetStringSafe(), "ese");
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(pushedColumns, THashSet<TString>({
            "time_dim.t_hour",
            "time_dim.t_minute",
            "household_demographics.hd_dep_count",
            "store.s_store_name",
        }));

        bool sawDate = false;
        bool sawDecimal = false;
        for (const auto& table : initial["schema"]["tables"].GetArraySafe()) {
            for (const auto& column : table["columns"].GetArraySafe()) {
                const TString type = column["type"].GetStringSafe();
                sawDate = sawDate || type == "Date";
                sawDecimal = sawDecimal || type.StartsWith("Decimal(");
            }
        }
        UNIT_ASSERT(sawDate);
        UNIT_ASSERT(sawDecimal);

        const auto countVoidExpressions = [](const NJson::TJsonValue& snapshot) {
            size_t count = 0;
            for (const auto* project : PlanNodes(snapshot, "project")) {
                for (const auto& column : (*project)["columns"].GetArraySafe()) {
                    count += column["expression"]["kind"].GetStringSafe() == "void";
                }
            }
            return count;
        };
        UNIT_ASSERT_VALUES_EQUAL(countVoidExpressions(initial), 1);
        UNIT_ASSERT_VALUES_EQUAL(countVoidExpressions(final), 1);

        THashMap<TString, size_t> edgeKinds;
        for (const auto& edge : final["stage_graph"]["edges"].GetArraySafe()) {
            ++edgeKinds[edge["kind"].GetStringSafe()];
        }
        UNIT_ASSERT_VALUES_EQUAL(edgeKinds["map"], 3);
        UNIT_ASSERT_VALUES_EQUAL(edgeKinds["broadcast"], 3);
        UNIT_ASSERT_VALUES_EQUAL(edgeKinds["union_all"], 1);
        UNIT_ASSERT_VALUES_EQUAL(edgeKinds["merge"], 1);

        const auto verdict = BuildVerificationProblem(results[0], results[1], 60'000);
        UNIT_ASSERT_VALUES_EQUAL(
            verdict["status"].GetStringSafe(),
            TryGetEnv("RBO_Z3") ? "VERIFIED_BOUNDED" : "FORMULA_EMITTED");
        UNIT_ASSERT_VALUES_EQUAL(verdict["row_bound"].GetIntegerSafe(), 2);
        UNIT_ASSERT_VALUES_EQUAL(verdict["task_bound"].GetIntegerSafe(), 2);
    }
}

} // namespace NKikimr::NKqp
