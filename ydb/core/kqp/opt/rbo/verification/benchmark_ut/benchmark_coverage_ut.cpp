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
#include <library/cpp/json/writer/json.h>
#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/folder/tempdir.h>
#include <util/generic/map.h>
#include <util/stream/file.h>
#include <util/string/cast.h>
#include <util/string/split.h>
#include <util/system/env.h>
#include <util/system/shellcommand.h>

#include <mutex>
#include <regex>
#include <set>
#include <utility>

namespace NKikimr::NKqp {
namespace {

constexpr const char* TestCluster = "local_ut";
constexpr ui64 RowBound = 2;
constexpr ui64 TaskBound = 2;
constexpr ui64 DefaultTimeoutMs = 10'000;

struct TSuite {
    TString Name;
    TString Slug;
    TString Schema;
    TString QueryPrefix;
    ui32 QueryCount;
};

const TSuite Tpch{
    "TPCH_YQL", "tpch", "schema/tpch.sql", "yql-tpch/q", 22};
const TSuite Tpcds{
    "TPCDS_YQL", "tpcds", "schema/tpcds.sql", "yql-tpcds/q", 99};

class TRecordingSink final : public IRBOSemanticSnapshotSink {
public:
    void OnSemanticSnapshot(TRBOSemanticSnapshotBoundaryResultV1 result) override {
        std::lock_guard guard(Mutex);
        Results.push_back(std::move(result));
    }

    TVector<TRBOSemanticSnapshotBoundaryResultV1> Take() {
        std::lock_guard guard(Mutex);
        TVector<TRBOSemanticSnapshotBoundaryResultV1> result;
        result.swap(Results);
        return result;
    }

private:
    std::mutex Mutex;
    TVector<TRBOSemanticSnapshotBoundaryResultV1> Results;
};

TString DataPath(TStringBuf relative) {
    return ArcadiaSourceRoot() +
        "/ydb/core/kqp/ut/rbo/data/" + TString(relative);
}

TIntrusivePtr<IKqpGateway> MakeGateway(Tests::TServer& server) {
    auto counters = MakeIntrusive<TKqpRequestCounters>();
    counters->Counters = new TKqpCounters(
        server.GetRuntime()->GetAppData(0).Counters);
    counters->TxProxyMon = new NTxProxy::TTxProxyMon(
        server.GetRuntime()->GetAppData(0).Counters);
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
    config->SetDefaultLangVer(NYql::GetMaxLangVersion());
    config->SetBackportMode(
        NKikimrConfig::TTableServiceConfig_EBackportMode_All);
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
        true,
        false,
        nullptr,
        server.GetRuntime()->GetAnyNodeActorSystem(),
        nullptr,
        nullptr,
        false,
        std::move(sink));
}

TKikimrRunner MakeRunner() {
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

void CreateTables(TKikimrRunner& kikimr, const TSuite& suite) {
    std::string schema = TFileInput(DataPath(suite.Schema)).ReadAll();
    const std::regex table(
        R"(CREATE TABLE [^\(]+ \([^;]*\))",
        std::regex::multiline);
    schema = std::regex_replace(
        schema,
        table,
        "$& WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 16);");
    auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
    const auto result = session.ExecuteSchemeQuery(TString(schema)).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

TString Query(const TSuite& suite, ui32 queryId) {
    const TString prelude = R"(
$to_decimal = ($x) -> { return cast($x as Decimal(12, 2)); };
$to_decimal_max_precision = ($x) -> { return cast($x as Decimal(35, 2)); };
$round = ($x,$y) -> { return $x; };
)";
    return prelude + TFileInput(DataPath(
        suite.QueryPrefix + ToString(queryId) + ".yql")).ReadAll();
}

ui64 TimeoutMs() {
    const auto value = TryGetEnv("RBO_COVERAGE_TIMEOUT_MS");
    if (!value) {
        return DefaultTimeoutMs;
    }
    ui64 result = 0;
    UNIT_ASSERT_C(
        TryFromString<ui64>(*value, result) && result > 0,
        "RBO_COVERAGE_TIMEOUT_MS must be a positive integer");
    return result;
}

ui32 ParseQueryId(TStringBuf text, const TSuite& suite) {
    ui32 result = 0;
    UNIT_ASSERT_C(
        TryFromString<ui32>(text, result) &&
            result >= 1 && result <= suite.QueryCount,
        "Invalid query id " << text << " for " << suite.Name);
    return result;
}

std::set<ui32> SelectedQueries(const TSuite& suite) {
    const auto value = TryGetEnv("RBO_COVERAGE_QUERIES");
    if (!value || value->empty()) {
        std::set<ui32> result;
        for (ui32 queryId = 1; queryId <= suite.QueryCount; ++queryId) {
            result.insert(queryId);
        }
        return result;
    }

    std::set<ui32> result;
    for (const TStringBuf token : StringSplitter(*value).Split(',').SkipEmpty()) {
        const size_t dash = token.find('-');
        if (dash == TStringBuf::npos) {
            result.insert(ParseQueryId(token, suite));
            continue;
        }
        UNIT_ASSERT_C(
            token.find('-', dash + 1) == TStringBuf::npos,
            "Invalid query range " << token);
        const ui32 first = ParseQueryId(token.SubStr(0, dash), suite);
        const ui32 last = ParseQueryId(token.SubStr(dash + 1), suite);
        UNIT_ASSERT_C(first <= last, "Descending query range " << token);
        for (ui32 queryId = first; queryId <= last; ++queryId) {
            result.insert(queryId);
        }
    }
    UNIT_ASSERT_C(!result.empty(), "RBO_COVERAGE_QUERIES selected no queries");
    return result;
}

NJson::TJsonValue JsonIds(const TVector<ui32>& ids) {
    NJson::TJsonValue result(NJson::JSON_ARRAY);
    for (const ui32 id : ids) {
        result.AppendValue(id);
    }
    return result;
}

bool ParseJson(const TString& text, NJson::TJsonValue& value) {
    return !text.empty() && NJson::ReadJsonTree(text, &value, true);
}

int ExpectedExit(TStringBuf status) {
    if (status == "FORMULA_EMITTED" || status == "VERIFIED_BOUNDED") {
        return 0;
    }
    if (status == "COUNTEREXAMPLE" || status == "SCHEMA_MISMATCH") {
        return 1;
    }
    return 2;
}

struct TOutcome {
    NJson::TJsonValue Json{NJson::JSON_MAP};
    TString Status;
    TString Layer;
    TString Reason;
    bool Fatal = false;
};

TOutcome HarnessError(
    ui32 queryId,
    ui64 prepareMs,
    size_t captureCount,
    const TString& reason)
{
    TOutcome outcome;
    outcome.Status = "HARNESS_ERROR";
    outcome.Layer = "harness";
    outcome.Reason = reason;
    outcome.Fatal = true;
    outcome.Json["query_id"] = queryId;
    outcome.Json["status"] = outcome.Status;
    outcome.Json["layer"] = outcome.Layer;
    outcome.Json["reason"] = reason;
    outcome.Json["prepare_ms"] = prepareMs;
    outcome.Json["verify_ms"] = 0;
    outcome.Json["capture_count"] = captureCount;
    return outcome;
}

TOutcome RunVerifier(
    ui32 queryId,
    ui64 prepareMs,
    const TRBOSemanticSnapshotBoundaryResultV1& initial,
    const TRBOSemanticSnapshotBoundaryResultV1& final,
    ui64 timeoutMs,
    const TMaybe<TString>& solver)
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
        << "--rows" << ToString(RowBound)
        << "--timeout-ms" << ToString(timeoutMs)
        << "--emit-smt" << formulaPath.GetPath();
    if (solver) {
        command << "--solver" << *solver;
    }

    const TInstant started = TInstant::Now();
    command.Run();
    const ui64 verifyMs = (TInstant::Now() - started).MilliSeconds();

    NJson::TJsonValue verdict;
    if (!ParseJson(command.GetOutput(), verdict) &&
        !ParseJson(command.GetError(), verdict))
    {
        return HarnessError(
            queryId,
            prepareMs,
            2,
            TStringBuilder()
                << "verifier returned no JSON; exit="
                << command.GetExitCode().GetOrElse(-1)
                << "; stdout=" << command.GetOutput()
                << "; stderr=" << command.GetError());
    }
    if (!verdict.IsMap() || !verdict.Has("status") ||
        !verdict["status"].IsString())
    {
        return HarnessError(queryId, prepareMs, 2, "verifier JSON has no string status");
    }

    const TString status = verdict["status"].GetStringSafe();
    static const THashSet<TString> Statuses = {
        "VERIFIED_BOUNDED",
        "FORMULA_EMITTED",
        "UNKNOWN",
        "UNSUPPORTED",
        "COUNTEREXAMPLE",
        "SCHEMA_MISMATCH",
        "SOLVER_ERROR",
    };
    const auto exitCode = command.GetExitCode();
    if (!Statuses.contains(status) || !exitCode.Defined() ||
        exitCode.GetRef() != ExpectedExit(status))
    {
        return HarnessError(
            queryId,
            prepareMs,
            2,
            TStringBuilder()
                << "verifier protocol mismatch: status=" << status
                << ", exit=" << exitCode.GetOrElse(-1));
    }

    TOutcome outcome;
    outcome.Status = status;
    outcome.Layer = "verifier";
    if (verdict.Has("reason") && verdict["reason"].IsString()) {
        outcome.Reason = verdict["reason"].GetStringSafe();
    }
    outcome.Fatal = status == "COUNTEREXAMPLE" ||
        status == "SCHEMA_MISMATCH" || status == "SOLVER_ERROR";
    outcome.Json["query_id"] = queryId;
    outcome.Json["status"] = status;
    outcome.Json["layer"] = outcome.Layer;
    outcome.Json["reason"] = outcome.Reason;
    outcome.Json["prepare_ms"] = prepareMs;
    outcome.Json["verify_ms"] = verifyMs;
    outcome.Json["capture_count"] = 2;
    outcome.Json["verdict"] = std::move(verdict);
    return outcome;
}

TOutcome ClassifyQuery(
    TKikimrRunner& kikimr,
    const NYql::IModuleResolver::TPtr& moduleResolver,
    const TSuite& suite,
    ui32 queryId,
    ui64 timeoutMs,
    const TMaybe<TString>& solver)
{
    auto sink = std::make_shared<TRecordingSink>();
    auto host = MakeHost(kikimr.GetTestServer(), moduleResolver, sink);
    IKqpHost::TPrepareSettings settings;
    settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;

    const TInstant started = TInstant::Now();
    const auto prepared = host->SyncPrepareDataQuery(Query(suite, queryId), settings);
    const ui64 prepareMs = (TInstant::Now() - started).MilliSeconds();
    const auto captures = sink->Take();

    if (!prepared.Success()) {
        TOutcome outcome;
        outcome.Status = "OPTIMIZER_FAILURE";
        outcome.Layer = "optimizer";
        outcome.Reason = prepared.Issues().ToString();
        outcome.Json["query_id"] = queryId;
        outcome.Json["status"] = outcome.Status;
        outcome.Json["layer"] = outcome.Layer;
        outcome.Json["reason"] = outcome.Reason;
        outcome.Json["prepare_ms"] = prepareMs;
        outcome.Json["verify_ms"] = 0;
        outcome.Json["capture_count"] = captures.size();
        return outcome;
    }
    if (captures.size() != 2 ||
        captures[0].Boundary != ERBOSemanticSnapshotBoundaryV1::Initial ||
        captures[1].Boundary != ERBOSemanticSnapshotBoundaryV1::Final)
    {
        return HarnessError(
            queryId,
            prepareMs,
            captures.size(),
            "snapshot callback count or order is invalid");
    }

    const auto& initial = captures[0];
    const auto& final = captures[1];
    if (!initial.IsSupported() || !final.IsSupported()) {
        TOutcome outcome;
        outcome.Status = "UNSUPPORTED";
        outcome.Layer = !initial.IsSupported() ? "initial_export" : "final_export";
        outcome.Reason = !initial.IsSupported()
            ? initial.UnsupportedReason
            : final.UnsupportedReason;
        outcome.Json["query_id"] = queryId;
        outcome.Json["status"] = outcome.Status;
        outcome.Json["layer"] = outcome.Layer;
        outcome.Json["reason"] = outcome.Reason;
        outcome.Json["initial_reason"] = initial.UnsupportedReason;
        outcome.Json["final_reason"] = final.UnsupportedReason;
        outcome.Json["prepare_ms"] = prepareMs;
        outcome.Json["verify_ms"] = 0;
        outcome.Json["capture_count"] = captures.size();
        return outcome;
    }

    return RunVerifier(
        queryId,
        prepareMs,
        initial,
        final,
        timeoutMs,
        solver);
}

void RunCoverage(const TSuite& suite) {
    auto kikimr = MakeRunner();
    CreateTables(kikimr, suite);

    NYql::TExprContext moduleContext;
    NYql::IModuleResolver::TPtr moduleResolver;
    UNIT_ASSERT(NYql::GetYqlDefaultModuleResolver(moduleContext, moduleResolver));

    const auto solver = TryGetEnv("RBO_Z3");
    const ui64 timeoutMs = TimeoutMs();
    const auto selected = SelectedQueries(suite);
    NJson::TJsonValue rows(NJson::JSON_ARRAY);
    TMap<TString, ui32> summary;
    TMap<std::pair<TString, TString>, TVector<ui32>> unsupported;
    TMap<TString, TVector<ui32>> optimizerFailures;
    bool fatal = false;

    for (const ui32 queryId : selected) {
        Cerr << "Checking " << suite.Name << " q" << queryId << Endl;
        TOutcome outcome = ClassifyQuery(
            kikimr,
            moduleResolver,
            suite,
            queryId,
            timeoutMs,
            solver);
        ++summary[outcome.Status];
        if (outcome.Status == "UNSUPPORTED") {
            unsupported[{outcome.Layer, outcome.Reason}].push_back(queryId);
        } else if (outcome.Status == "OPTIMIZER_FAILURE") {
            optimizerFailures[outcome.Reason].push_back(queryId);
        }
        fatal = fatal || outcome.Fatal;
        outcome.Json["suite"] = suite.Name;
        outcome.Json["source"] = suite.QueryPrefix + ToString(queryId) + ".yql";
        outcome.Json["timeout_ms"] = timeoutMs;
        rows.AppendValue(std::move(outcome.Json));
    }

    NJson::TJsonValue summaryJson(NJson::JSON_MAP);
    for (const auto& [status, count] : summary) {
        summaryJson[status] = count;
    }

    NJson::TJsonValue unsupportedJson(NJson::JSON_ARRAY);
    for (const auto& [key, ids] : unsupported) {
        NJson::TJsonValue item(NJson::JSON_MAP);
        item["layer"] = key.first;
        item["reason"] = key.second;
        item["query_ids"] = JsonIds(ids);
        unsupportedJson.AppendValue(std::move(item));
    }

    NJson::TJsonValue optimizerJson(NJson::JSON_ARRAY);
    for (const auto& [reason, ids] : optimizerFailures) {
        NJson::TJsonValue item(NJson::JSON_MAP);
        item["reason"] = reason;
        item["query_ids"] = JsonIds(ids);
        optimizerJson.AppendValue(std::move(item));
    }

    NJson::TJsonValue report(NJson::JSON_MAP);
    report["format"] = "ydb-rbo-benchmark-coverage";
    report["version"] = 1;
    report["suite"] = suite.Name;
    report["row_bound"] = RowBound;
    report["task_bound"] = TaskBound;
    report["solver_present"] = solver.Defined();
    report["timeout_ms"] = timeoutMs;
    report["summary"] = std::move(summaryJson);
    report["queries"] = std::move(rows);
    report["unsupported_inventory"] = std::move(unsupportedJson);
    report["optimizer_failure_inventory"] = std::move(optimizerJson);

    const auto reportPath = GetOutputPath() / (suite.Slug + "_coverage.json");
    TFileOutput(reportPath.GetPath()).Write(NJson::WriteJson(
        report,
        true,
        true));
    Cout << suite.Name << " summary: "
         << NJson::WriteJson(report["summary"], false, true) << Endl
         << "Coverage report: " << reportPath.GetPath() << Endl;
    UNIT_ASSERT_C(!fatal, "correctness or harness failure; see " << reportPath.GetPath());
}

} // namespace

Y_UNIT_TEST_SUITE(TRBOBenchmarkCoverage) {
    Y_UNIT_TEST(TPCH) {
        RunCoverage(Tpch);
    }

    Y_UNIT_TEST(TPCDS) {
        RunCoverage(Tpcds);
    }
}

} // namespace NKikimr::NKqp
