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
#include <library/cpp/json/json_writer.h>
#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/system/shellcommand.h>

#include <mutex>
#include <regex>

namespace NKikimr::NKqp {
namespace {

constexpr const char* TestCluster = "local_ut";
constexpr const char* HermeticSolverTarget = "contrib/tools/z3/z3";

TString HermeticSolver() {
    return BinaryPath(HermeticSolverTarget);
}

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

const NJson::TJsonValue& PlanNode(
    const NJson::TJsonValue& snapshot,
    const TString& id)
{
    const NJson::TJsonValue* match = nullptr;
    for (const auto& node : snapshot["plan"]["nodes"].GetArraySafe()) {
        if (node["id"].GetStringSafe() == id) {
            UNIT_ASSERT_C(!match, id);
            match = &node;
        }
    }
    UNIT_ASSERT_C(match, id);
    return *match;
}

const NJson::TJsonValue& WitnessRows(
    const NJson::TJsonValue& verdict,
    TStringBuf tablePath)
{
    const TString identityPart = TStringBuilder()
        << "path:" << tablePath.size() << ":" << tablePath << ";";
    const NJson::TJsonValue* match = nullptr;
    for (const auto& [table, rows] :
        verdict["witness"].GetMapSafe())
    {
        if (table.Contains(identityPart)) {
            UNIT_ASSERT_C(!match, tablePath);
            match = &rows;
        }
    }
    UNIT_ASSERT_C(match, tablePath);
    return *match;
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

void CollectExpressions(
    const NJson::TJsonValue& value,
    TStringBuf kind,
    TVector<const NJson::TJsonValue*>& matches)
{
    if (value.IsMap()) {
        const auto& fields = value.GetMapSafe();
        const auto kindIt = fields.find("kind");
        if (kindIt != fields.end() && kindIt->second.IsString() &&
            kindIt->second.GetStringSafe() == kind)
        {
            matches.push_back(&value);
        }
        for (const auto& [name, field] : fields) {
            Y_UNUSED(name);
            CollectExpressions(field, kind, matches);
        }
    } else if (value.IsArray()) {
        for (const auto& item : value.GetArraySafe()) {
            CollectExpressions(item, kind, matches);
        }
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

void CreateTextOrderedColumnTable(TKikimrRunner& kikimr) {
    auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
    const auto result = session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/RboTextOrdered` (
            Id Uint64 NOT NULL,
            Bytes String,
            Text Utf8,
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

void CreateDecimalColumnTable(TKikimrRunner& kikimr) {
    auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
    const auto result = session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/RboDecimal` (
            Id Uint64 NOT NULL,
            D Decimal(7, 2),
            PRIMARY KEY (Id)
        ) WITH (STORE = COLUMN);
    )").GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

void CreateDateColumnTable(TKikimrRunner& kikimr) {
    auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
    const auto result = session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/RboDate` (
            Id Uint64 NOT NULL,
            D Date,
            PRIMARY KEY (Id)
        ) WITH (STORE = COLUMN);
    )").GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

void CreateExistsColumnTables(TKikimrRunner& kikimr) {
    auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
    const auto result = session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/RboExistsOuter` (
            Id Int64 NOT NULL,
            MatchKey Int64 NOT NULL,
            Payload Int64,
            PRIMARY KEY (Id)
        ) WITH (STORE = COLUMN);

        CREATE TABLE `/Root/RboExistsInner` (
            Id Int64 NOT NULL,
            MatchKey Int64,
            Payload Int64,
            Amount Decimal(7, 2),
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
    command << "--solver" << HermeticSolver();
    command.Run();
    UNIT_ASSERT_C(
        command.GetExitCode().Defined(),
        command.GetError() << command.GetOutput());

    NJson::TJsonValue verdict;
    UNIT_ASSERT_C(
        NJson::ReadJsonTree(command.GetOutput(), &verdict, true),
        command.GetOutput());
    const TString status = verdict["status"].GetStringSafe();
    const int expectedExitCode =
        status == "VERIFIED_BOUNDED"
            ? 0
            : status == "COUNTEREXAMPLE"
                ? 1
                : status == "UNKNOWN"
                    ? 2
                    : -1;
    UNIT_ASSERT_C(expectedExitCode >= 0, command.GetOutput());
    UNIT_ASSERT_VALUES_EQUAL_C(
        command.GetExitCode().GetRef(),
        expectedExitCode,
        command.GetError() << command.GetOutput());
    UNIT_ASSERT_C(formulaPath.Exists(), command.GetOutput());
    return verdict;
}

struct TSnapshotPair {
    NJson::TJsonValue Initial;
    NJson::TJsonValue Final;
    NJson::TJsonValue Verdict;
};

TSnapshotPair CaptureRealHostSnapshotPair(
    TKikimrRunner& kikimr,
    const TString& query)
{
    NYql::TExprContext moduleContext;
    NYql::IModuleResolver::TPtr moduleResolver;
    UNIT_ASSERT(NYql::GetYqlDefaultModuleResolver(moduleContext, moduleResolver));

    auto sink = std::make_shared<TRecordingSemanticSnapshotSink>();
    auto host = MakeHost(
        kikimr.GetTestServer(),
        std::move(moduleResolver),
        sink);
    IKqpHost::TPrepareSettings settings;
    settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;
    const auto prepared = host->SyncPrepareDataQuery(query, settings);
    UNIT_ASSERT_C(prepared.Success(), prepared.Issues().ToString());

    const auto results = sink->Extract();
    UNIT_ASSERT_VALUES_EQUAL(results.size(), 2);
    UNIT_ASSERT(results[0].Boundary == ERBOSemanticSnapshotBoundaryV1::Initial);
    UNIT_ASSERT(results[1].Boundary == ERBOSemanticSnapshotBoundaryV1::Final);

    auto initial = ParseSnapshot(results[0]);
    auto final = ParseSnapshot(results[1]);
    UNIT_ASSERT(initial["stage_graph"].IsNull());
    UNIT_ASSERT(final["stage_graph"].IsMap());

    auto verdict = BuildVerificationProblem(results[0], results[1]);
    return {
        .Initial = std::move(initial),
        .Final = std::move(final),
        .Verdict = std::move(verdict),
    };
}

TSnapshotPair VerifyRealHostSnapshotPair(
    TKikimrRunner& kikimr,
    const TString& query)
{
    auto pair = CaptureRealHostSnapshotPair(kikimr, query);
    UNIT_ASSERT_VALUES_EQUAL_C(
        pair.Verdict["status"].GetStringSafe(),
        "VERIFIED_BOUNDED",
        NJson::WriteJson(pair.Verdict, false));
    UNIT_ASSERT_VALUES_EQUAL(pair.Verdict["row_bound"].GetIntegerSafe(), 2);
    UNIT_ASSERT_VALUES_EQUAL(pair.Verdict["task_bound"].GetIntegerSafe(), 2);
    return pair;
}

} // namespace

Y_UNIT_TEST_SUITE(TRBOSemanticSnapshotIntegration) {
    Y_UNIT_TEST(HermeticSolverIsPinned) {
        TShellCommand command(HermeticSolver());
        command << "--version";
        command.Run();
        UNIT_ASSERT_C(
            command.GetExitCode().Defined() && command.GetExitCode().GetRef() == 0,
            command.GetError() << command.GetOutput());
        UNIT_ASSERT_STRING_CONTAINS(command.GetOutput(), "Z3 version 4.16.0");
    }

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
            "VERIFIED_BOUNDED");
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
            "VERIFIED_BOUNDED");
        UNIT_ASSERT_VALUES_EQUAL(verdict["row_bound"].GetIntegerSafe(), 2);
        UNIT_ASSERT_VALUES_EQUAL(verdict["task_bound"].GetIntegerSafe(), 2);
    }

    Y_UNIT_TEST(RealHostVerifiesUncorrelatedExists) {
        TKikimrRunner kikimr;
        CreateExistsColumnTables(kikimr);

        const auto pair = VerifyRealHostSnapshotPair(kikimr, R"(--!syntax_v1
            SELECT outer_row.Id
            FROM `/Root/RboExistsOuter` AS outer_row
            WHERE EXISTS (
                SELECT inner_row.Id
                FROM `/Root/RboExistsInner` AS inner_row
                WHERE inner_row.Payload > 0
            );
        )");

        const auto& subplans = pair.Initial["plan"]["subplans"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(subplans.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(subplans[0]["kind"].GetStringSafe(), "exists");
        UNIT_ASSERT(subplans[0]["predicate"].IsNull());
        UNIT_ASSERT(subplans[0]["dependencies"].GetArraySafe().empty());
        UNIT_ASSERT(pair.Final["plan"]["subplans"].GetArraySafe().empty());
    }

    Y_UNIT_TEST(RealHostVerifiesEqualityCorrelatedExists) {
        TKikimrRunner kikimr;
        CreateExistsColumnTables(kikimr);

        const auto pair = VerifyRealHostSnapshotPair(kikimr, R"(--!syntax_v1
            SELECT outer_row.Id
            FROM `/Root/RboExistsOuter` AS outer_row
            WHERE EXISTS (
                SELECT inner_row.Id
                FROM `/Root/RboExistsInner` AS inner_row
                WHERE inner_row.MatchKey == outer_row.MatchKey
            );
        )");

        const auto& subplans = pair.Initial["plan"]["subplans"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(subplans.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(subplans[0]["kind"].GetStringSafe(), "exists");
        UNIT_ASSERT_VALUES_EQUAL(
            subplans[0]["predicate"]["kind"].GetStringSafe(),
            "eq");
        UNIT_ASSERT_VALUES_EQUAL(
            subplans[0]["dependencies"].GetArraySafe().size(),
            1);
        UNIT_ASSERT(pair.Final["plan"]["subplans"].GetArraySafe().empty());
    }

    Y_UNIT_TEST(RealHostVerifiesEqualityCorrelatedScalarAvgLeftJoin) {
        TKikimrRunner kikimr;
        CreateExistsColumnTables(kikimr);

        const auto pair = VerifyRealHostSnapshotPair(kikimr, R"(--!syntax_v1
            SELECT
                outer_row.Id,
                (
                    SELECT Avg(inner_row.Amount)
                    FROM `/Root/RboExistsInner` AS inner_row
                    WHERE inner_row.MatchKey == outer_row.MatchKey
                ) AS MeanAmount
            FROM `/Root/RboExistsOuter` AS outer_row;
        )");

        const auto& subplans = pair.Initial["plan"]["subplans"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(subplans.size(), 1);
        const auto& subplan = subplans[0];
        UNIT_ASSERT_VALUES_EQUAL(subplan.GetMapSafe().size(), 8);
        UNIT_ASSERT_VALUES_EQUAL(subplan["kind"].GetStringSafe(), "scalar");
        UNIT_ASSERT_VALUES_EQUAL(subplan["type"].GetStringSafe(), "Decimal(7,2)");
        UNIT_ASSERT(subplan["nullable"].GetBooleanSafe());
        UNIT_ASSERT_VALUES_EQUAL(
            subplan["output"]["type"].GetStringSafe(),
            "Decimal(7,2)");
        UNIT_ASSERT(subplan["output"]["nullable"].GetBooleanSafe());
        UNIT_ASSERT_VALUES_EQUAL(
            subplan["dependencies"].GetArraySafe().size(),
            1);
        UNIT_ASSERT_VALUES_EQUAL(subplan["consumers"].GetArraySafe().size(), 1);
        const TString dependency =
            subplan["dependencies"][0].GetStringSafe();

        const auto outerBindings = PlanNodes(pair.Initial, "outer_bind");
        UNIT_ASSERT_VALUES_EQUAL(outerBindings.size(), 1);
        const auto& outerBinding = *outerBindings[0];
        UNIT_ASSERT_VALUES_EQUAL(
            outerBinding["dependency"].GetStringSafe(),
            dependency);
        UNIT_ASSERT_VALUES_EQUAL(
            outerBinding["type"].GetStringSafe(),
            "Int64");
        UNIT_ASSERT(!outerBinding["nullable"].GetBooleanSafe());

        const auto* shape = &PlanNode(
            pair.Initial,
            subplan["root"].GetStringSafe());
        const NJson::TJsonValue* scalarAggregate = nullptr;
        while ((*shape)["op"].GetStringSafe() == "project" ||
               (*shape)["op"].GetStringSafe() == "aggregate")
        {
            if ((*shape)["op"].GetStringSafe() == "aggregate") {
                UNIT_ASSERT(!scalarAggregate);
                scalarAggregate = shape;
            }
            shape = &PlanNode(
                pair.Initial,
                (*shape)["input"].GetStringSafe());
        }
        UNIT_ASSERT(scalarAggregate);
        UNIT_ASSERT((*scalarAggregate)["keys"].GetArraySafe().empty());
        UNIT_ASSERT_VALUES_EQUAL(
            (*scalarAggregate)["aggregates"].GetArraySafe().size(),
            1);
        UNIT_ASSERT_VALUES_EQUAL(
            (*scalarAggregate)["aggregates"][0]["function"].GetStringSafe(),
            "avg");

        const auto& correlationFilter = *shape;
        UNIT_ASSERT_VALUES_EQUAL(
            correlationFilter["op"].GetStringSafe(),
            "filter");
        UNIT_ASSERT_VALUES_EQUAL(
            correlationFilter["input"].GetStringSafe(),
            outerBinding["id"].GetStringSafe());
        UNIT_ASSERT_VALUES_EQUAL(
            correlationFilter["predicate"]["kind"].GetStringSafe(),
            "eq");
        const auto& left = correlationFilter["predicate"]["left"];
        const auto& right = correlationFilter["predicate"]["right"];
        UNIT_ASSERT_VALUES_EQUAL(left["kind"].GetStringSafe(), "column");
        UNIT_ASSERT_VALUES_EQUAL(right["kind"].GetStringSafe(), "column");
        UNIT_ASSERT(
            left["column"].GetStringSafe() == dependency ||
            right["column"].GetStringSafe() == dependency);

        UNIT_ASSERT(pair.Final["plan"]["subplans"].GetArraySafe().empty());
        UNIT_ASSERT(PlanNodes(pair.Final, "outer_bind").empty());
        const auto joins = PlanNodes(pair.Final, "join");
        UNIT_ASSERT_VALUES_EQUAL(joins.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            (*joins[0])["kind"].GetStringSafe(),
            "left");
    }

    Y_UNIT_TEST(RealHostFindsCorrelatedScalarCountEmptyInputBug) {
        TKikimrRunner kikimr;
        CreateExistsColumnTables(kikimr);

        const auto pair = CaptureRealHostSnapshotPair(kikimr, R"(--!syntax_v1
            SELECT
                outer_row.Id,
                (
                    SELECT COUNT(*)
                    FROM `/Root/RboExistsInner` AS inner_row
                    WHERE inner_row.MatchKey == outer_row.MatchKey
                ) AS Matches
            FROM `/Root/RboExistsOuter` AS outer_row;
        )");

        const auto& subplans = pair.Initial["plan"]["subplans"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(subplans.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            subplans[0]["kind"].GetStringSafe(),
            "scalar");
        UNIT_ASSERT_VALUES_EQUAL(
            subplans[0]["dependencies"].GetArraySafe().size(),
            1);
        UNIT_ASSERT(subplans[0]["nullable"].GetBooleanSafe());
        UNIT_ASSERT(!subplans[0]["output"]["nullable"].GetBooleanSafe());
        const auto& aggregate = OnlyPlanNode(pair.Initial, "aggregate");
        UNIT_ASSERT(aggregate["keys"].GetArraySafe().empty());
        UNIT_ASSERT_VALUES_EQUAL(
            aggregate["aggregates"].GetArraySafe().size(),
            1);
        UNIT_ASSERT_VALUES_EQUAL(
            aggregate["aggregates"][0]["function"].GetStringSafe(),
            "count");

        UNIT_ASSERT(pair.Final["plan"]["subplans"].GetArraySafe().empty());
        const auto joins = PlanNodes(pair.Final, "join");
        UNIT_ASSERT_VALUES_EQUAL(joins.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            (*joins[0])["kind"].GetStringSafe(),
            "left");

        UNIT_ASSERT_VALUES_EQUAL_C(
            pair.Verdict["status"].GetStringSafe(),
            "COUNTEREXAMPLE",
            NJson::WriteJson(pair.Verdict, false));
        UNIT_ASSERT_VALUES_EQUAL(
            pair.Verdict["row_bound"].GetIntegerSafe(),
            2);
        UNIT_ASSERT_VALUES_EQUAL(
            pair.Verdict["task_bound"].GetIntegerSafe(),
            2);

        const auto& outerRows =
            WitnessRows(pair.Verdict, "/Root/RboExistsOuter").GetArraySafe();
        const auto& innerRows =
            WitnessRows(pair.Verdict, "/Root/RboExistsInner").GetArraySafe();
        UNIT_ASSERT(!outerRows.empty());
        bool hasUnmatchedOuter = false;
        for (const auto& outer : outerRows) {
            const i64 key = outer["MatchKey"].GetIntegerSafe();
            bool matched = false;
            for (const auto& inner : innerRows) {
                const auto& innerKey = inner["MatchKey"];
                matched = matched ||
                    (!innerKey.IsNull() &&
                     innerKey.GetIntegerSafe() == key);
            }
            hasUnmatchedOuter = hasUnmatchedOuter || !matched;
        }
        UNIT_ASSERT_C(
            hasUnmatchedOuter,
            NJson::WriteJson(pair.Verdict, false));
    }

    Y_UNIT_TEST(RealHostVerifiesScalarAndEqualityCorrelatedNotExists) {
        TKikimrRunner kikimr;
        CreateExistsColumnTables(kikimr);

        const auto pair = VerifyRealHostSnapshotPair(kikimr, R"(--!syntax_v1
            SELECT outer_row.Id
            FROM `/Root/RboExistsOuter` AS outer_row
            WHERE outer_row.Payload == (
                SELECT scalar_row.Payload
                FROM `/Root/RboExistsInner` AS scalar_row
                WHERE scalar_row.MatchKey == 0
            ) AND NOT EXISTS (
                SELECT inner_row.Id
                FROM `/Root/RboExistsInner` AS inner_row
                WHERE inner_row.MatchKey == outer_row.MatchKey
            );
        )");

        const auto& subplans = pair.Initial["plan"]["subplans"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(subplans.size(), 2);
        THashSet<TString> kinds;
        const NJson::TJsonValue* exists = nullptr;
        TString consumerId;
        for (const auto& subplan : subplans) {
            const TString kind = subplan["kind"].GetStringSafe();
            UNIT_ASSERT(kinds.insert(kind).second);
            const auto& consumers = subplan["consumers"].GetArraySafe();
            UNIT_ASSERT_VALUES_EQUAL(consumers.size(), 1);
            if (consumerId.empty()) {
                consumerId = consumers[0].GetStringSafe();
            } else {
                UNIT_ASSERT_VALUES_EQUAL(
                    consumers[0].GetStringSafe(),
                    consumerId);
            }
            if (kind == "scalar") {
                UNIT_ASSERT(subplan["dependencies"].GetArraySafe().empty());
            } else {
                UNIT_ASSERT_VALUES_EQUAL(kind, "exists");
                exists = &subplan;
                UNIT_ASSERT_VALUES_EQUAL(
                    subplan["predicate"]["kind"].GetStringSafe(),
                    "eq");
                UNIT_ASSERT_VALUES_EQUAL(
                    subplan["dependencies"].GetArraySafe().size(),
                    1);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(
            kinds,
            THashSet<TString>({"exists", "scalar"}));

        UNIT_ASSERT(exists);
        const auto& consumer = PlanNode(pair.Initial, consumerId);
        TVector<const NJson::TJsonValue*> negations;
        CollectExpressions(consumer["predicate"], "not", negations);
        UNIT_ASSERT_VALUES_EQUAL(negations.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            (*negations[0])["arg"]["kind"].GetStringSafe(),
            "column");
        UNIT_ASSERT_VALUES_EQUAL(
            (*negations[0])["arg"]["column"].GetStringSafe(),
            (*exists)["binding"].GetStringSafe());
        UNIT_ASSERT(pair.Final["plan"]["subplans"].GetArraySafe().empty());
    }

    Y_UNIT_TEST(RealHostVerifiesWorkloadShapedCorrelatedExists) {
        TKikimrRunner kikimr;
        CreateExistsColumnTables(kikimr);

        const TVector<TString> queries = {
            R"(--!syntax_v1
                SELECT outer_row.Id
                FROM `/Root/RboExistsOuter` AS outer_row
                WHERE EXISTS (
                    SELECT a.Id
                    FROM `/Root/RboExistsInner` AS a
                    WHERE a.MatchKey == outer_row.MatchKey
                      AND a.MatchKey > 0
                      AND a.Payload > 0
                ) AND (
                    EXISTS (
                        SELECT b.Id
                        FROM `/Root/RboExistsInner` AS b
                        WHERE b.MatchKey == outer_row.MatchKey
                          AND b.MatchKey > 10
                          AND b.Payload > 10
                    ) OR EXISTS (
                        SELECT c.Id
                        FROM `/Root/RboExistsInner` AS c
                        WHERE c.MatchKey == outer_row.MatchKey
                          AND c.MatchKey > 20
                          AND c.Payload > 20
                    )
                );
            )",
            R"(--!syntax_v1
                SELECT outer_row.Id
                FROM `/Root/RboExistsOuter` AS outer_row
                WHERE EXISTS (
                    SELECT a.Id
                    FROM `/Root/RboExistsInner` AS a
                    WHERE a.MatchKey == outer_row.MatchKey
                      AND a.MatchKey > 0
                      AND a.Payload > 0
                ) AND NOT EXISTS (
                    SELECT b.Id
                    FROM `/Root/RboExistsInner` AS b
                    WHERE b.MatchKey == outer_row.MatchKey
                      AND b.MatchKey > 10
                      AND b.Payload > 10
                ) AND NOT EXISTS (
                    SELECT c.Id
                    FROM `/Root/RboExistsInner` AS c
                    WHERE c.MatchKey == outer_row.MatchKey
                      AND c.MatchKey > 20
                      AND c.Payload > 20
                );
            )",
        };

        for (const auto& query : queries) {
            const auto pair = VerifyRealHostSnapshotPair(kikimr, query);
            const auto& subplans =
                pair.Initial["plan"]["subplans"].GetArraySafe();
            UNIT_ASSERT_VALUES_EQUAL(subplans.size(), 3);

            THashSet<TString> bindings;
            THashSet<TString> roots;
            THashSet<TString> consumers;
            for (const auto& subplan : subplans) {
                UNIT_ASSERT_VALUES_EQUAL(
                    subplan["kind"].GetStringSafe(),
                    "exists");
                UNIT_ASSERT(bindings.insert(
                    subplan["binding"].GetStringSafe()).second);
                UNIT_ASSERT(roots.insert(
                    subplan["root"].GetStringSafe()).second);
                UNIT_ASSERT_VALUES_EQUAL(
                    subplan["dependencies"].GetArraySafe().size(),
                    1);
                const auto& subplanConsumers =
                    subplan["consumers"].GetArraySafe();
                UNIT_ASSERT_VALUES_EQUAL(subplanConsumers.size(), 1);
                consumers.insert(subplanConsumers[0].GetStringSafe());

                TVector<const NJson::TJsonValue*> conjuncts;
                CollectConjuncts(subplan["predicate"], conjuncts);
                UNIT_ASSERT_VALUES_EQUAL(conjuncts.size(), 3);
                TVector<const NJson::TJsonValue*> equalities;
                TVector<const NJson::TJsonValue*> comparisons;
                CollectExpressions(
                    subplan["predicate"],
                    "eq",
                    equalities);
                CollectExpressions(
                    subplan["predicate"],
                    "gt",
                    comparisons);
                UNIT_ASSERT_VALUES_EQUAL(equalities.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(comparisons.size(), 2);
                UNIT_ASSERT_VALUES_EQUAL(
                    PlanNode(
                        pair.Initial,
                        subplan["root"].GetStringSafe())["op"].GetStringSafe(),
                    "scan");
            }
            UNIT_ASSERT_VALUES_EQUAL(bindings.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(roots.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(consumers.size(), 1);
            UNIT_ASSERT(pair.Final["plan"]["subplans"].GetArraySafe().empty());

            bool sawNullableInnerKey = false;
            for (const auto& table :
                pair.Initial["schema"]["tables"].GetArraySafe())
            {
                if (!table["name"].GetStringSafe().Contains(
                        "/Root/RboExistsInner"))
                {
                    continue;
                }
                for (const auto& column :
                    table["columns"].GetArraySafe())
                {
                    if (column["name"].GetStringSafe() == "MatchKey") {
                        sawNullableInnerKey =
                            column["nullable"].GetBooleanSafe();
                    }
                }
            }
            UNIT_ASSERT(sawNullableInnerKey);
        }
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
            "VERIFIED_BOUNDED");
        UNIT_ASSERT_VALUES_EQUAL(verdict["row_bound"].GetIntegerSafe(), 2);
        UNIT_ASSERT_VALUES_EQUAL(verdict["task_bound"].GetIntegerSafe(), 2);
    }

    Y_UNIT_TEST(RealHostVerifiesStringAndUtf8TopSortAndMerge) {
        TKikimrRunner kikimr;
        CreateTextOrderedColumnTable(kikimr);

        NYql::TExprContext moduleContext;
        NYql::IModuleResolver::TPtr moduleResolver;
        UNIT_ASSERT(NYql::GetYqlDefaultModuleResolver(moduleContext, moduleResolver));

        auto sink = std::make_shared<TRecordingSemanticSnapshotSink>();
        auto host = MakeHost(kikimr.GetTestServer(), std::move(moduleResolver), sink);
        const TString query = R"(--!syntax_v1
                SELECT Bytes, Text
                FROM `/Root/RboTextOrdered`
                WHERE Bytes >= "binary\x00tail"
                    AND Text >= Utf8("Cafe\xCC\x81/привет")
                ORDER BY Bytes DESC, Text ASC
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

        const auto assertTextSchema = [](const NJson::TJsonValue& snapshot) {
            THashMap<TString, TString> types;
            for (const auto& table : snapshot["schema"]["tables"].GetArraySafe()) {
                if (!table["name"].GetStringSafe().Contains("/Root/RboTextOrdered")) {
                    continue;
                }
                for (const auto& column : table["columns"].GetArraySafe()) {
                    types[column["name"].GetStringSafe()] =
                        column["type"].GetStringSafe();
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(types["Bytes"], "String");
            UNIT_ASSERT_VALUES_EQUAL(types["Text"], "Utf8");
        };
        assertTextSchema(initial);
        assertTextSchema(final);

        TVector<const NJson::TJsonValue*> literals;
        CollectExpressions(initial["plan"], "literal", literals);
        THashMap<TString, TString> textLiterals;
        for (const auto* literal : literals) {
            const TString type = (*literal)["type"].GetStringSafe();
            if (type == "String" || type == "Utf8") {
                textLiterals[type] = (*literal)["value"].GetStringSafe();
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(
            textLiterals["String"],
            TString("binary\0tail", 11));
        UNIT_ASSERT_VALUES_EQUAL(
            textLiterals["Utf8"],
            TString("Cafe\xCC\x81/привет"));

        const auto& initialSort = OnlyPlanNode(initial, "sort");
        AssertSort(initialSort, "undefined", false);
        AssertLimit(OnlyPlanNode(initial, "limit"), "undefined");

        const auto& finalSort = OnlyPlanNode(final, "sort");
        AssertSort(finalSort, "intermediate", true);
        AssertLimit(OnlyPlanNode(final, "limit"), "final");
        const auto& edges = final["stage_graph"]["edges"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(edges.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(edges[0]["kind"].GetStringSafe(), "merge");
        UNIT_ASSERT_VALUES_EQUAL(
            edges[0]["order"].GetArraySafe(),
            finalSort["order"].GetArraySafe());

        const auto verdict = BuildVerificationProblem(results[0], results[1]);
        UNIT_ASSERT_VALUES_EQUAL(
            verdict["status"].GetStringSafe(),
            "VERIFIED_BOUNDED");
        UNIT_ASSERT_VALUES_EQUAL(verdict["row_bound"].GetIntegerSafe(), 2);
        UNIT_ASSERT_VALUES_EQUAL(verdict["task_bound"].GetIntegerSafe(), 2);
    }

    Y_UNIT_TEST(RealHostVerifiesRestrictedSubstring) {
        TKikimrRunner kikimr;
        CreateTextOrderedColumnTable(kikimr);

        NYql::TExprContext moduleContext;
        NYql::IModuleResolver::TPtr moduleResolver;
        UNIT_ASSERT(NYql::GetYqlDefaultModuleResolver(moduleContext, moduleResolver));

        auto sink = std::make_shared<TRecordingSemanticSnapshotSink>();
        auto host = MakeHost(kikimr.GetTestServer(), std::move(moduleResolver), sink);
        const TString query = R"(--!syntax_v1
                SELECT Id, Substring(Bytes, 1, 5) AS Prefix
                FROM `/Root/RboTextOrdered`;
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

        const auto substringExpressions = [](const NJson::TJsonValue& snapshot) {
            TVector<const NJson::TJsonValue*> opaque;
            CollectExpressions(snapshot["plan"], "opaque", opaque);
            TVector<const NJson::TJsonValue*> result;
            for (const auto* expression : opaque) {
                if ((*expression)["fingerprint"].GetStringSafe().Contains("Substring")) {
                    result.push_back(expression);
                }
            }
            return result;
        };
        const auto initialSubstrings = substringExpressions(initial);
        const auto finalSubstrings = substringExpressions(final);
        UNIT_ASSERT_VALUES_EQUAL(initialSubstrings.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(finalSubstrings.size(), 1);
        const auto assertSubstring = [](const NJson::TJsonValue& expression) {
            UNIT_ASSERT_VALUES_EQUAL(expression["type"].GetStringSafe(), "String");
            UNIT_ASSERT(expression["nullable"].GetBooleanSafe());
            const auto& arguments = expression["args"].GetArraySafe();
            UNIT_ASSERT_VALUES_EQUAL(arguments.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(
                arguments[0]["kind"].GetStringSafe(),
                "column");
        };
        assertSubstring(*initialSubstrings[0]);
        assertSubstring(*finalSubstrings[0]);
        UNIT_ASSERT_VALUES_EQUAL(
            (*initialSubstrings[0])["fingerprint"].GetStringSafe(),
            (*finalSubstrings[0])["fingerprint"].GetStringSafe());

        const auto verdict = BuildVerificationProblem(results[0], results[1]);
        UNIT_ASSERT_VALUES_EQUAL(
            verdict["status"].GetStringSafe(),
            "VERIFIED_BOUNDED");
        UNIT_ASSERT_VALUES_EQUAL(verdict["row_bound"].GetIntegerSafe(), 2);
        UNIT_ASSERT_VALUES_EQUAL(verdict["task_bound"].GetIntegerSafe(), 2);
    }

    Y_UNIT_TEST(RealHostVerifiesBoundedStoredStringConcat) {
        TKikimrRunner kikimr;
        CreateTextOrderedColumnTable(kikimr);

        NYql::TExprContext moduleContext;
        NYql::IModuleResolver::TPtr moduleResolver;
        UNIT_ASSERT(NYql::GetYqlDefaultModuleResolver(moduleContext, moduleResolver));

        auto sink = std::make_shared<TRecordingSemanticSnapshotSink>();
        auto host = MakeHost(kikimr.GetTestServer(), std::move(moduleResolver), sink);
        const TString query = R"(--!syntax_v1
                SELECT
                    Id,
                    Coalesce(Bytes, '') || ':' AS Label
                FROM `/Root/RboTextOrdered`;
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

        const auto concatExpression = [](const NJson::TJsonValue& snapshot) {
            TVector<const NJson::TJsonValue*> opaque;
            CollectExpressions(snapshot["plan"], "opaque", opaque);
            const NJson::TJsonValue* result = nullptr;
            for (const auto* expression : opaque) {
                if ((*expression)["fingerprint"].GetStringSafe().Contains("Concat")) {
                    UNIT_ASSERT(!result);
                    result = expression;
                }
            }
            UNIT_ASSERT(result);
            return result;
        };
        const auto* initialConcat = concatExpression(initial);
        const auto* finalConcat = concatExpression(final);
        const auto assertConcat = [](const NJson::TJsonValue& expression) {
            UNIT_ASSERT_VALUES_EQUAL(expression["type"].GetStringSafe(), "String");
            UNIT_ASSERT(!expression["nullable"].GetBooleanSafe());
            const auto& arguments = expression["args"].GetArraySafe();
            UNIT_ASSERT_VALUES_EQUAL(arguments.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(arguments[0]["kind"].GetStringSafe(), "column");
        };
        assertConcat(*initialConcat);
        assertConcat(*finalConcat);
        UNIT_ASSERT_VALUES_EQUAL(
            (*initialConcat)["fingerprint"].GetStringSafe(),
            (*finalConcat)["fingerprint"].GetStringSafe());

        const auto verdict = BuildVerificationProblem(results[0], results[1]);
        UNIT_ASSERT_VALUES_EQUAL(
            verdict["status"].GetStringSafe(),
            "VERIFIED_BOUNDED");
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
            "VERIFIED_BOUNDED");
        UNIT_ASSERT_VALUES_EQUAL(verdict["row_bound"].GetIntegerSafe(), 2);
        UNIT_ASSERT_VALUES_EQUAL(verdict["task_bound"].GetIntegerSafe(), 2);
    }

    Y_UNIT_TEST(RealHostVerifiesMixedSignedUnsignedComparison) {
        TKikimrRunner kikimr;
        CreateOrderedColumnTable(kikimr);

        NYql::TExprContext moduleContext;
        NYql::IModuleResolver::TPtr moduleResolver;
        UNIT_ASSERT(NYql::GetYqlDefaultModuleResolver(moduleContext, moduleResolver));

        auto sink = std::make_shared<TRecordingSemanticSnapshotSink>();
        auto host = MakeHost(kikimr.GetTestServer(), std::move(moduleResolver), sink);
        const TString query = R"(--!syntax_v1
                SELECT COUNT(*) > 1 AS HasMultipleRows
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

        const auto assertComparison = [](const NJson::TJsonValue& snapshot) {
            TVector<const NJson::TJsonValue*> comparisons;
            CollectExpressions(snapshot["plan"], "gt", comparisons);
            UNIT_ASSERT_VALUES_EQUAL(comparisons.size(), 1);
            const auto& comparison = *comparisons.front();
            UNIT_ASSERT_VALUES_EQUAL(
                comparison["left"]["kind"].GetStringSafe(),
                "column");
            UNIT_ASSERT_VALUES_EQUAL(
                comparison["right"]["type"].GetStringSafe(),
                "Int32");
            UNIT_ASSERT_VALUES_EQUAL(
                comparison["right"]["value"].GetIntegerSafe(),
                1);

            const TString leftColumn =
                comparison["left"]["column"].GetStringSafe();
            bool foundUint64AggregateOutput = false;
            for (const auto* aggregate : PlanNodes(snapshot, "aggregate")) {
                for (const auto& trait : (*aggregate)["aggregates"].GetArraySafe()) {
                    foundUint64AggregateOutput =
                        foundUint64AggregateOutput ||
                        (trait["output"].GetStringSafe() == leftColumn &&
                         trait["type"].GetStringSafe() == "Uint64");
                }
            }
            UNIT_ASSERT(foundUint64AggregateOutput);
        };
        assertComparison(initial);
        assertComparison(final);

        const auto verdict = BuildVerificationProblem(results[0], results[1]);
        UNIT_ASSERT_VALUES_EQUAL(
            verdict["status"].GetStringSafe(),
            "VERIFIED_BOUNDED");
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
            "VERIFIED_BOUNDED");
        UNIT_ASSERT_VALUES_EQUAL(verdict["row_bound"].GetIntegerSafe(), 2);
        UNIT_ASSERT_VALUES_EQUAL(verdict["task_bound"].GetIntegerSafe(), 2);
    }

    Y_UNIT_TEST(RealHostVerifiesDecimalComparison) {
        auto kikimr = MakeTpcdsRunner();
        CreateDecimalColumnTable(kikimr);

        NYql::TExprContext moduleContext;
        NYql::IModuleResolver::TPtr moduleResolver;
        UNIT_ASSERT(NYql::GetYqlDefaultModuleResolver(moduleContext, moduleResolver));

        auto sink = std::make_shared<TRecordingSemanticSnapshotSink>();
        auto host = MakeHost(kikimr.GetTestServer(), std::move(moduleResolver), sink);
        const TString query = R"(--!syntax_v1
                SELECT Id
                FROM `/Root/RboDecimal`
                WHERE D BETWEEN Decimal("100", 12, 2)
                    AND Decimal("150", 12, 2);
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

        const auto assertDecimalPredicate = [](const NJson::TJsonValue& predicate) {
            TVector<const NJson::TJsonValue*> comparisons;
            CollectConjuncts(predicate, comparisons);
            UNIT_ASSERT_VALUES_EQUAL(comparisons.size(), 2);

            THashSet<TString> kinds;
            THashSet<TString> scaledValues;
            for (const auto* comparison : comparisons) {
                kinds.insert((*comparison)["kind"].GetStringSafe());
                UNIT_ASSERT_VALUES_EQUAL(
                    (*comparison)["left"]["kind"].GetStringSafe(),
                    "column");
                UNIT_ASSERT_VALUES_EQUAL(
                    (*comparison)["left"]["column"].GetStringSafe(),
                    "/Root/RboDecimal.D");
                const auto& literal = (*comparison)["right"];
                UNIT_ASSERT_VALUES_EQUAL(literal["kind"].GetStringSafe(), "literal");
                UNIT_ASSERT_VALUES_EQUAL(
                    literal["type"].GetStringSafe(),
                    "Decimal(12,2)");
                UNIT_ASSERT_VALUES_EQUAL(
                    literal["value"]["kind"].GetStringSafe(),
                    "finite");
                scaledValues.insert(literal["value"]["scaled"].GetStringSafe());
            }
            UNIT_ASSERT_VALUES_EQUAL(kinds, THashSet<TString>({"gte", "lte"}));
            UNIT_ASSERT_VALUES_EQUAL(
                scaledValues,
                THashSet<TString>({"10000", "15000"}));
        };

        const auto& initialScan = OnlyPlanNode(initial, "scan");
        UNIT_ASSERT(initialScan["predicate"].IsNull());
        assertDecimalPredicate(OnlyPlanNode(initial, "filter")["predicate"]);

        const auto finalFilters = PlanNodes(final, "filter");
        UNIT_ASSERT_VALUES_EQUAL(finalFilters.size(), 1);
        assertDecimalPredicate((*finalFilters.front())["predicate"]);
        UNIT_ASSERT(OnlyPlanNode(final, "scan")["predicate"].IsNull());

        const auto verdict = BuildVerificationProblem(results[0], results[1]);
        UNIT_ASSERT_VALUES_EQUAL(
            verdict["status"].GetStringSafe(),
            "VERIFIED_BOUNDED");
        UNIT_ASSERT_VALUES_EQUAL(verdict["row_bound"].GetIntegerSafe(), 2);
        UNIT_ASSERT_VALUES_EQUAL(verdict["task_bound"].GetIntegerSafe(), 2);
    }

    Y_UNIT_TEST(RealHostVerifiesDecimalArithmetic) {
        auto kikimr = MakeTpcdsRunner();
        CreateDecimalColumnTable(kikimr);

        NYql::TExprContext moduleContext;
        NYql::IModuleResolver::TPtr moduleResolver;
        UNIT_ASSERT(NYql::GetYqlDefaultModuleResolver(moduleContext, moduleResolver));

        auto sink = std::make_shared<TRecordingSemanticSnapshotSink>();
        auto host = MakeHost(kikimr.GetTestServer(), std::move(moduleResolver), sink);
        const TString query = R"(--!syntax_v1
                SELECT
                    D + Decimal("1.25", 7, 2) AS DecimalSum,
                    D - Decimal("0.50", 7, 2) AS DecimalDifference,
                    D * Decimal("2.00", 7, 2) AS DecimalProduct,
                    D * 3 AS IntegerProduct,
                    3 * D AS ReversedIntegerProduct,
                    D / Decimal("2.00", 7, 2) AS DecimalQuotient,
                    D / 4 AS IntegerQuotient
                FROM `/Root/RboDecimal`;
            )";
        IKqpHost::TPrepareSettings settings;
        settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;
        const auto prepared = host->SyncPrepareDataQuery(query, settings);
        UNIT_ASSERT_C(prepared.Success(), prepared.Issues().ToString());

        const auto results = sink->Extract();
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 2);
        UNIT_ASSERT(results[0].Boundary == ERBOSemanticSnapshotBoundaryV1::Initial);
        UNIT_ASSERT(results[1].Boundary == ERBOSemanticSnapshotBoundaryV1::Final);

        const auto assertArithmetic = [](const NJson::TJsonValue& snapshot) {
            TVector<const NJson::TJsonValue*> expressions;
            for (const auto* project : PlanNodes(snapshot, "project")) {
                for (const auto& column : (*project)["columns"].GetArraySafe()) {
                    const auto& expression = column["expression"];
                    const TString kind = expression["kind"].GetStringSafe();
                    if ((kind == "add" || kind == "sub" || kind == "mul" ||
                         kind == "div") &&
                        expression["type"].GetStringSafe() == "Decimal(7,2)")
                    {
                        expressions.push_back(&expression);
                    }
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(expressions.size(), 7);

            THashMap<TString, ui32> kindCounts;
            THashSet<TString> rightTypes;
            THashSet<TString> scaledValues;
            THashSet<i64> integerValues;
            for (const auto* expression : expressions) {
                ++kindCounts[(*expression)["kind"].GetStringSafe()];
                UNIT_ASSERT((*expression)["nullable"].GetBooleanSafe());
                UNIT_ASSERT_VALUES_EQUAL(
                    (*expression)["left"]["kind"].GetStringSafe(),
                    "column");
                const auto& right = (*expression)["right"];
                UNIT_ASSERT_VALUES_EQUAL(right["kind"].GetStringSafe(), "literal");
                const TString rightType = right["type"].GetStringSafe();
                rightTypes.insert(rightType);
                if (rightType == "Decimal(7,2)") {
                    UNIT_ASSERT_VALUES_EQUAL(
                        right["value"]["kind"].GetStringSafe(),
                        "finite");
                    scaledValues.insert(
                        right["value"]["scaled"].GetStringSafe());
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(rightType, "Int32");
                    integerValues.insert(right["value"].GetIntegerSafe());
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(kindCounts["add"], 1);
            UNIT_ASSERT_VALUES_EQUAL(kindCounts["sub"], 1);
            UNIT_ASSERT_VALUES_EQUAL(kindCounts["mul"], 3);
            UNIT_ASSERT_VALUES_EQUAL(kindCounts["div"], 2);
            UNIT_ASSERT_VALUES_EQUAL(
                rightTypes,
                THashSet<TString>({"Decimal(7,2)", "Int32"}));
            UNIT_ASSERT_VALUES_EQUAL(
                scaledValues,
                THashSet<TString>({"50", "125", "200"}));
            UNIT_ASSERT_VALUES_EQUAL(
                integerValues,
                THashSet<i64>({3, 4}));
        };

        assertArithmetic(ParseSnapshot(results[0]));
        assertArithmetic(ParseSnapshot(results[1]));

        const auto verdict = BuildVerificationProblem(results[0], results[1]);
        UNIT_ASSERT_VALUES_EQUAL(
            verdict["status"].GetStringSafe(),
            "VERIFIED_BOUNDED");
        UNIT_ASSERT_VALUES_EQUAL(verdict["row_bound"].GetIntegerSafe(), 2);
        UNIT_ASSERT_VALUES_EQUAL(verdict["task_bound"].GetIntegerSafe(), 2);
    }

    Y_UNIT_TEST(RealHostVerifiesIntegralSafeCastToDecimal) {
        auto kikimr = MakeTpcdsRunner();
        CreateDecimalColumnTable(kikimr);

        NYql::TExprContext moduleContext;
        NYql::IModuleResolver::TPtr moduleResolver;
        UNIT_ASSERT(NYql::GetYqlDefaultModuleResolver(moduleContext, moduleResolver));

        auto sink = std::make_shared<TRecordingSemanticSnapshotSink>();
        auto host = MakeHost(kikimr.GetTestServer(), std::move(moduleResolver), sink);
        const TString query = R"(--!syntax_v1
                SELECT
                    CAST(COUNT(*) AS Decimal(15, 4)) /
                    CAST(1 + COUNT(*) AS Decimal(15, 4)) AS Ratio
                FROM `/Root/RboDecimal`;
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

        const auto assertCasts = [](const NJson::TJsonValue& snapshot) {
            TVector<const NJson::TJsonValue*> casts;
            CollectExpressions(snapshot["plan"], "cast_decimal", casts);
            UNIT_ASSERT_VALUES_EQUAL(casts.size(), 2);
            for (const auto* cast : casts) {
                UNIT_ASSERT_VALUES_EQUAL(cast->GetMapSafe().size(), 4);
                UNIT_ASSERT_VALUES_EQUAL(
                    (*cast)["type"].GetStringSafe(),
                    "Decimal(15,4)");
                UNIT_ASSERT(!(*cast)["nullable"].GetBooleanSafe());
                const auto& argument = (*cast)["arg"];
                const TString argumentKind = argument["kind"].GetStringSafe();
                UNIT_ASSERT_C(
                    argumentKind == "column" || argumentKind == "add" ||
                        argumentKind == "opaque",
                    NJson::WriteJson(argument, false, true));
                if (argumentKind != "column") {
                    UNIT_ASSERT_VALUES_EQUAL(
                        argument["type"].GetStringSafe(),
                        "Uint64");
                    UNIT_ASSERT(!argument["nullable"].GetBooleanSafe());
                }
            }

            TVector<const NJson::TJsonValue*> divisions;
            CollectExpressions(snapshot["plan"], "div", divisions);
            UNIT_ASSERT_VALUES_EQUAL(divisions.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(
                (*divisions.front())["type"].GetStringSafe(),
                "Decimal(15,4)");
            UNIT_ASSERT(!(*divisions.front())["nullable"].GetBooleanSafe());
        };
        assertCasts(initial);
        assertCasts(final);

        UNIT_ASSERT_VALUES_EQUAL(PlanNodes(initial, "aggregate").size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(PlanNodes(final, "aggregate").size(), 2);

        const auto verdict = BuildVerificationProblem(results[0], results[1]);
        UNIT_ASSERT_VALUES_EQUAL(
            verdict["status"].GetStringSafe(),
            "VERIFIED_BOUNDED");
        UNIT_ASSERT_VALUES_EQUAL(verdict["row_bound"].GetIntegerSafe(), 2);
        UNIT_ASSERT_VALUES_EQUAL(verdict["task_bound"].GetIntegerSafe(), 2);
    }

    Y_UNIT_TEST(RealHostVerifiesDecimalSumPhases) {
        auto kikimr = MakeTpcdsRunner();
        CreateDecimalColumnTable(kikimr);

        NYql::TExprContext moduleContext;
        NYql::IModuleResolver::TPtr moduleResolver;
        UNIT_ASSERT(NYql::GetYqlDefaultModuleResolver(moduleContext, moduleResolver));

        auto sink = std::make_shared<TRecordingSemanticSnapshotSink>();
        auto host = MakeHost(kikimr.GetTestServer(), std::move(moduleResolver), sink);
        const TString query = R"(--!syntax_v1
                SELECT SUM(D) AS Total
                FROM `/Root/RboDecimal`;
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

        const auto assertSum = [](
            const NJson::TJsonValue& aggregate,
            TStringBuf phase,
            TStringBuf input)
        {
            UNIT_ASSERT_VALUES_EQUAL(
                aggregate["phase"].GetStringSafe(),
                phase);
            const auto& traits = aggregate["aggregates"].GetArraySafe();
            UNIT_ASSERT_VALUES_EQUAL(traits.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(traits[0]["input"].GetStringSafe(), input);
            UNIT_ASSERT_VALUES_EQUAL(
                traits[0]["function"].GetStringSafe(),
                "sum");
            UNIT_ASSERT_VALUES_EQUAL(
                traits[0]["type"].GetStringSafe(),
                "Decimal(35,2)");
            UNIT_ASSERT(traits[0]["nullable"].GetBooleanSafe());
        };

        const auto initialAggregates = PlanNodes(initial, "aggregate");
        UNIT_ASSERT_VALUES_EQUAL(initialAggregates.size(), 1);
        assertSum(*initialAggregates.front(), "undefined", "/Root/RboDecimal.D");

        const auto finalAggregates = PlanNodes(final, "aggregate");
        UNIT_ASSERT_VALUES_EQUAL(finalAggregates.size(), 2);
        const NJson::TJsonValue* intermediate = nullptr;
        const NJson::TJsonValue* finalAggregate = nullptr;
        for (const auto* aggregate : finalAggregates) {
            const TString phase = (*aggregate)["phase"].GetStringSafe();
            if (phase == "intermediate") {
                intermediate = aggregate;
            } else if (phase == "final") {
                finalAggregate = aggregate;
            }
        }
        UNIT_ASSERT(intermediate);
        UNIT_ASSERT(finalAggregate);
        assertSum(*intermediate, "intermediate", "/Root/RboDecimal.D");
        const TString partialOutput =
            (*intermediate)["aggregates"][0]["output"].GetStringSafe();
        assertSum(*finalAggregate, "final", partialOutput);

        const auto& edges = final["stage_graph"]["edges"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(edges.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(edges[0]["kind"].GetStringSafe(), "union_all");
        UNIT_ASSERT(!edges[0]["parallel"].GetBooleanSafe());

        const auto verdict = BuildVerificationProblem(results[0], results[1]);
        UNIT_ASSERT_VALUES_EQUAL(
            verdict["status"].GetStringSafe(),
            "VERIFIED_BOUNDED");
        UNIT_ASSERT_VALUES_EQUAL(verdict["row_bound"].GetIntegerSafe(), 2);
        UNIT_ASSERT_VALUES_EQUAL(verdict["task_bound"].GetIntegerSafe(), 2);
    }

    Y_UNIT_TEST(RealHostVerifiesDecimalTopSortAndMerge) {
        auto kikimr = MakeTpcdsRunner();
        CreateDecimalColumnTable(kikimr);

        NYql::TExprContext moduleContext;
        NYql::IModuleResolver::TPtr moduleResolver;
        UNIT_ASSERT(NYql::GetYqlDefaultModuleResolver(moduleContext, moduleResolver));

        auto sink = std::make_shared<TRecordingSemanticSnapshotSink>();
        auto host = MakeHost(kikimr.GetTestServer(), std::move(moduleResolver), sink);
        const TString query = R"(--!syntax_v1
                SELECT Id, D
                FROM `/Root/RboDecimal`
                ORDER BY D DESC
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

        const auto assertDecimalSort = [](const NJson::TJsonValue& sort) {
            const auto& order = sort["order"].GetArraySafe();
            UNIT_ASSERT_VALUES_EQUAL(order.size(), 1);
            UNIT_ASSERT(!order[0]["ascending"].GetBooleanSafe());
            UNIT_ASSERT(order[0]["nulls_first"].GetBooleanSafe());
        };
        const auto& initialSort = OnlyPlanNode(initial, "sort");
        const auto& finalSort = OnlyPlanNode(final, "sort");
        assertDecimalSort(initialSort);
        assertDecimalSort(finalSort);
        UNIT_ASSERT(initialSort["limit"].IsNull());
        UNIT_ASSERT_VALUES_EQUAL(
            finalSort["limit"]["value"].GetUIntegerSafe(),
            1);

        const auto& edges = final["stage_graph"]["edges"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(edges.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(edges[0]["kind"].GetStringSafe(), "merge");
        UNIT_ASSERT_VALUES_EQUAL(
            edges[0]["order"].GetArraySafe(),
            finalSort["order"].GetArraySafe());

        bool sawDecimal = false;
        for (const auto& table : initial["schema"]["tables"].GetArraySafe()) {
            for (const auto& column : table["columns"].GetArraySafe()) {
                sawDecimal = sawDecimal ||
                    column["type"].GetStringSafe() == "Decimal(7,2)";
            }
        }
        UNIT_ASSERT(sawDecimal);

        const auto verdict = BuildVerificationProblem(results[0], results[1]);
        UNIT_ASSERT_VALUES_EQUAL(
            verdict["status"].GetStringSafe(),
            "VERIFIED_BOUNDED");
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
                WHERE A >= 30 AND B == 5 AND Payload IS NULL;
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
        TVector<const NJson::TJsonValue*> initialExists;
        CollectExpressions(initialFilter["predicate"], "exists", initialExists);
        UNIT_ASSERT_VALUES_EQUAL(initialExists.size(), 1);
        const TString initialPresenceColumn =
            (*initialExists.front())["arg"]["column"].GetStringSafe();
        UNIT_ASSERT(
            initialPresenceColumn == "Payload" ||
            initialPresenceColumn.EndsWith(".Payload"));

        const auto& finalScan = OnlyPlanNode(final, "scan");
        UNIT_ASSERT(!finalScan["predicate"].IsNull());
        UNIT_ASSERT(finalScan["pushed_limit"].IsNull());
        UNIT_ASSERT_VALUES_EQUAL(
            finalScan["predicate"]["kind"].GetStringSafe(),
            "and");
        const auto& conjuncts = finalScan["predicate"]["args"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(conjuncts.size(), 3);
        THashSet<TString> kinds;
        THashSet<TString> columns;
        for (const auto& conjunct : conjuncts) {
            const TString kind = conjunct["kind"].GetStringSafe();
            if (kind == "not") {
                const auto& exists = conjunct["arg"];
                UNIT_ASSERT_VALUES_EQUAL(exists["kind"].GetStringSafe(), "exists");
                UNIT_ASSERT_VALUES_EQUAL(exists["arg"]["kind"].GetStringSafe(), "column");
                UNIT_ASSERT_VALUES_EQUAL(exists["arg"]["column"].GetStringSafe(), "Payload");
                UNIT_ASSERT(kinds.insert("empty").second);
                UNIT_ASSERT(columns.insert("Payload").second);
                continue;
            }
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
        UNIT_ASSERT_VALUES_EQUAL(kinds, THashSet<TString>({"empty", "eq", "gte"}));
        UNIT_ASSERT_VALUES_EQUAL(columns, THashSet<TString>({"A", "B", "Payload"}));
        UNIT_ASSERT(PlanNodes(final, "filter").empty());

        const auto verdict = BuildVerificationProblem(results[0], results[1]);
        UNIT_ASSERT_VALUES_EQUAL(
            verdict["status"].GetStringSafe(),
            "VERIFIED_BOUNDED");
        UNIT_ASSERT_VALUES_EQUAL(verdict["row_bound"].GetIntegerSafe(), 2);
        UNIT_ASSERT_VALUES_EQUAL(verdict["task_bound"].GetIntegerSafe(), 2);
    }

    Y_UNIT_TEST(RealHostVerifiesConstantDateIntervalPushedOlapFilter) {
        TKikimrRunner kikimr;
        CreateDateColumnTable(kikimr);

        NYql::TExprContext moduleContext;
        NYql::IModuleResolver::TPtr moduleResolver;
        UNIT_ASSERT(NYql::GetYqlDefaultModuleResolver(moduleContext, moduleResolver));

        auto sink = std::make_shared<TRecordingSemanticSnapshotSink>();
        auto host = MakeHost(kikimr.GetTestServer(), std::move(moduleResolver), sink);
        const TString query = R"(--!syntax_v1
                SELECT Id, D
                FROM `/Root/RboDate`
                WHERE D BETWEEN
                    CAST('1998-08-04' AS Date)
                    AND
                    (CAST('1998-08-04' AS Date) + DateTime::IntervalFromDays(14));
            )";
        IKqpHost::TPrepareSettings settings;
        settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;
        // Date literal folding calls MiniKQL code that requires an active actor
        // context, just as normal query preparation has inside the server.
        const auto prepared = kikimr.GetTestServer().GetRuntime()->RunCall([
            host,
            query,
            settings
        ] {
            return host->SyncPrepareDataQuery(query, settings);
        });
        UNIT_ASSERT_C(prepared.Success(), prepared.Issues().ToString());

        const auto results = sink->Extract();
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 2);
        UNIT_ASSERT(results[0].Boundary == ERBOSemanticSnapshotBoundaryV1::Initial);
        UNIT_ASSERT(results[1].Boundary == ERBOSemanticSnapshotBoundaryV1::Final);
        const auto initial = ParseSnapshot(results[0]);
        const auto final = ParseSnapshot(results[1]);

        const auto assertBounds = [](const NJson::TJsonValue& predicate) {
            TVector<const NJson::TJsonValue*> literals;
            CollectExpressions(predicate, "literal", literals);
            THashSet<ui64> days;
            for (const auto* literal : literals) {
                if ((*literal)["type"].GetStringSafe() == "Date") {
                    UNIT_ASSERT(days.insert(
                        (*literal)["value"].GetUIntegerSafe()).second);
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(days, THashSet<ui64>({10'442, 10'456}));
        };

        const auto& initialScan = OnlyPlanNode(initial, "scan");
        UNIT_ASSERT(initialScan["predicate"].IsNull());
        assertBounds(OnlyPlanNode(initial, "filter")["predicate"]);

        const auto& finalScan = OnlyPlanNode(final, "scan");
        UNIT_ASSERT(!finalScan["predicate"].IsNull());
        assertBounds(finalScan["predicate"]);
        UNIT_ASSERT(PlanNodes(final, "filter").empty());

        const auto verdict = BuildVerificationProblem(results[0], results[1]);
        UNIT_ASSERT_VALUES_EQUAL(
            verdict["status"].GetStringSafe(),
            "VERIFIED_BOUNDED");
        UNIT_ASSERT_VALUES_EQUAL(verdict["row_bound"].GetIntegerSafe(), 2);
        UNIT_ASSERT_VALUES_EQUAL(verdict["task_bound"].GetIntegerSafe(), 2);
    }

    Y_UNIT_TEST(RealHostVerifiesDirectDateIntervalPushedOlapFilter) {
        TKikimrRunner kikimr;
        CreateDateColumnTable(kikimr);

        NYql::TExprContext moduleContext;
        NYql::IModuleResolver::TPtr moduleResolver;
        UNIT_ASSERT(NYql::GetYqlDefaultModuleResolver(moduleContext, moduleResolver));

        auto sink = std::make_shared<TRecordingSemanticSnapshotSink>();
        auto host = MakeHost(kikimr.GetTestServer(), std::move(moduleResolver), sink);
        const TString query = R"(--!syntax_v1
                SELECT Id, D
                FROM `/Root/RboDate`
                WHERE D <= Date('1998-12-01') - Interval('P90D');
            )";
        IKqpHost::TPrepareSettings settings;
        settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;
        // Query preparation evaluates the Interval literal through the KQP
        // literal executor, which requires an active actor context.
        const auto prepared = kikimr.GetTestServer().GetRuntime()->RunCall([
            host,
            query,
            settings
        ] {
            return host->SyncPrepareDataQuery(query, settings);
        });
        UNIT_ASSERT_C(prepared.Success(), prepared.Issues().ToString());

        const auto results = sink->Extract();
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 2);
        UNIT_ASSERT(results[0].Boundary == ERBOSemanticSnapshotBoundaryV1::Initial);
        UNIT_ASSERT(results[1].Boundary == ERBOSemanticSnapshotBoundaryV1::Final);
        const auto initial = ParseSnapshot(results[0]);
        const auto final = ParseSnapshot(results[1]);

        const auto assertBound = [](const NJson::TJsonValue& predicate) {
            TVector<const NJson::TJsonValue*> literals;
            CollectExpressions(predicate, "literal", literals);
            TVector<ui64> dates;
            for (const auto* literal : literals) {
                if ((*literal)["type"].GetStringSafe() == "Date") {
                    dates.push_back((*literal)["value"].GetUIntegerSafe());
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(dates, TVector<ui64>({10'471}));
        };

        UNIT_ASSERT(OnlyPlanNode(initial, "scan")["predicate"].IsNull());
        assertBound(OnlyPlanNode(initial, "filter")["predicate"]);

        const auto& finalScan = OnlyPlanNode(final, "scan");
        UNIT_ASSERT(!finalScan["predicate"].IsNull());
        assertBound(finalScan["predicate"]);
        UNIT_ASSERT(PlanNodes(final, "filter").empty());

        const auto verdict = BuildVerificationProblem(results[0], results[1]);
        UNIT_ASSERT_VALUES_EQUAL(
            verdict["status"].GetStringSafe(),
            "VERIFIED_BOUNDED");
        UNIT_ASSERT_VALUES_EQUAL(verdict["row_bound"].GetIntegerSafe(), 2);
        UNIT_ASSERT_VALUES_EQUAL(verdict["task_bound"].GetIntegerSafe(), 2);
    }

    Y_UNIT_TEST(RealHostVerifiesShiftedDateConstantsInPushedOlapFilter) {
        TKikimrRunner kikimr;
        CreateDateColumnTable(kikimr);

        NYql::TExprContext moduleContext;
        NYql::IModuleResolver::TPtr moduleResolver;
        UNIT_ASSERT(NYql::GetYqlDefaultModuleResolver(moduleContext, moduleResolver));

        auto sink = std::make_shared<TRecordingSemanticSnapshotSink>();
        auto host = MakeHost(kikimr.GetTestServer(), std::move(moduleResolver), sink);
        const TString query = R"(--!syntax_v1
                SELECT Id, D
                FROM `/Root/RboDate`
                WHERE D >= DateTime::MakeDate(
                    DateTime::ShiftMonths(Date('1993-10-01'), 3))
                  AND D < DateTime::MakeDate(
                    DateTime::ShiftYears(Date('1994-01-01'), 1));
            )";
        IKqpHost::TPrepareSettings settings;
        settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;
        // Query preparation evaluates the DateTime UDF constants in an actor
        // context, just as it does for normal server-side preparation.
        const auto prepared = kikimr.GetTestServer().GetRuntime()->RunCall([
            host,
            query,
            settings
        ] {
            return host->SyncPrepareDataQuery(query, settings);
        });
        UNIT_ASSERT_C(prepared.Success(), prepared.Issues().ToString());

        const auto results = sink->Extract();
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 2);
        UNIT_ASSERT(results[0].Boundary == ERBOSemanticSnapshotBoundaryV1::Initial);
        UNIT_ASSERT(results[1].Boundary == ERBOSemanticSnapshotBoundaryV1::Final);
        const auto initial = ParseSnapshot(results[0]);
        const auto final = ParseSnapshot(results[1]);

        const auto assertBounds = [](const NJson::TJsonValue& predicate) {
            TVector<const NJson::TJsonValue*> literals;
            CollectExpressions(predicate, "literal", literals);
            THashSet<ui64> days;
            for (const auto* literal : literals) {
                if ((*literal)["type"].GetStringSafe() == "Date") {
                    UNIT_ASSERT(days.insert(
                        (*literal)["value"].GetUIntegerSafe()).second);
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(days, THashSet<ui64>({8'766, 9'131}));
        };

        UNIT_ASSERT(OnlyPlanNode(initial, "scan")["predicate"].IsNull());
        assertBounds(OnlyPlanNode(initial, "filter")["predicate"]);

        const auto& finalScan = OnlyPlanNode(final, "scan");
        UNIT_ASSERT(!finalScan["predicate"].IsNull());
        assertBounds(finalScan["predicate"]);
        UNIT_ASSERT(PlanNodes(final, "filter").empty());

        const auto verdict = BuildVerificationProblem(results[0], results[1]);
        UNIT_ASSERT_VALUES_EQUAL(
            verdict["status"].GetStringSafe(),
            "VERIFIED_BOUNDED");
        UNIT_ASSERT_VALUES_EQUAL(verdict["row_bound"].GetIntegerSafe(), 2);
        UNIT_ASSERT_VALUES_EQUAL(verdict["task_bound"].GetIntegerSafe(), 2);
    }

    Y_UNIT_TEST(RealHostVerifiesPushedOlapPresencePredicates) {
        TKikimrRunner kikimr;
        CreateOrderedColumnTable(kikimr);

        NYql::TExprContext moduleContext;
        NYql::IModuleResolver::TPtr moduleResolver;
        UNIT_ASSERT(NYql::GetYqlDefaultModuleResolver(moduleContext, moduleResolver));

        auto sink = std::make_shared<TRecordingSemanticSnapshotSink>();
        auto host = MakeHost(kikimr.GetTestServer(), std::move(moduleResolver), sink);
        const TString query = R"(--!syntax_v1
                SELECT Id, A, B
                FROM `/Root/RboOrdered`
                WHERE A IS NULL OR B IS NOT NULL;
            )";
        IKqpHost::TPrepareSettings settings;
        settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;
        const auto prepared = host->SyncPrepareDataQuery(query, settings);
        UNIT_ASSERT_C(prepared.Success(), prepared.Issues().ToString());

        const auto results = sink->Extract();
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 2);
        const auto initial = ParseSnapshot(results[0]);
        const auto final = ParseSnapshot(results[1]);

        auto assertPresencePredicate = [](const NJson::TJsonValue& predicate) {
            UNIT_ASSERT_VALUES_EQUAL(predicate["kind"].GetStringSafe(), "or");
            const auto& alternatives = predicate["args"].GetArraySafe();
            UNIT_ASSERT_VALUES_EQUAL(alternatives.size(), 2);

            THashSet<TString> columns;
            for (const auto& alternative : alternatives) {
                const NJson::TJsonValue* presence = &alternative;
                bool negated = false;
                size_t notCount = 0;
                while ((*presence)["kind"].GetStringSafe() == "not") {
                    negated = !negated;
                    presence = &(*presence)["arg"];
                    UNIT_ASSERT(++notCount <= 2);
                }
                UNIT_ASSERT_VALUES_EQUAL((*presence)["kind"].GetStringSafe(), "exists");
                const TString column = (*presence)["arg"]["column"].GetStringSafe();
                if (negated) {
                    UNIT_ASSERT(column == "A" || column.EndsWith(".A"));
                    UNIT_ASSERT(columns.insert("empty:A").second);
                } else {
                    UNIT_ASSERT(column == "B" || column.EndsWith(".B"));
                    UNIT_ASSERT(columns.insert("exists:B").second);
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(
                columns,
                THashSet<TString>({"empty:A", "exists:B"}));
        };

        const auto& initialScan = OnlyPlanNode(initial, "scan");
        UNIT_ASSERT(initialScan["predicate"].IsNull());
        assertPresencePredicate(OnlyPlanNode(initial, "filter")["predicate"]);

        UNIT_ASSERT(PlanNodes(final, "filter").empty());
        assertPresencePredicate(OnlyPlanNode(final, "scan")["predicate"]);

        const auto verdict = BuildVerificationProblem(results[0], results[1]);
        UNIT_ASSERT_VALUES_EQUAL(
            verdict["status"].GetStringSafe(),
            "VERIFIED_BOUNDED");
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
            "VERIFIED_BOUNDED");
        UNIT_ASSERT_VALUES_EQUAL(verdict["row_bound"].GetIntegerSafe(), 2);
        UNIT_ASSERT_VALUES_EQUAL(verdict["task_bound"].GetIntegerSafe(), 2);
    }
}

} // namespace NKikimr::NKqp
