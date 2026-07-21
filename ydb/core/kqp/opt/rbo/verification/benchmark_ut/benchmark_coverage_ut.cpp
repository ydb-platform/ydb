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
#include <library/cpp/testing/common/scope.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/folder/tempdir.h>
#include <util/generic/map.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/string/cast.h>
#include <util/string/split.h>
#include <util/system/env.h>
#include <util/system/shellcommand.h>

#include <exception>
#include <initializer_list>
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
constexpr const char* CoverageReportFormat =
    "ydb-rbo-benchmark-coverage";
constexpr ui64 CoverageReportVersion = 2;
constexpr const char* CoveragePolicyFormat =
    "ydb-rbo-benchmark-coverage-policy";

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

struct TSuiteCoveragePolicy {
    ui32 QueryCount = 0;
    std::set<ui32> RequiredFormulaQueries;
};

struct TCoveragePolicy {
    TMap<TString, TSuiteCoveragePolicy> Suites;
};

struct TPolicyEvaluation {
    bool Valid = true;
    bool FullSelection = false;
    bool Enforced = false;
    std::set<ui32> RequiredFormulaQueries;
    std::set<ui32> FormulaEmittedQueries;
    TVector<TString> Violations;
};

TString CoveragePolicyPath() {
    return ArcadiaSourceRoot() +
        "/ydb/core/kqp/opt/rbo/verification/benchmark_ut/coverage_policy.json";
}

void RequirePolicyKeys(
    const NJson::TJsonValue& value,
    std::initializer_list<TStringBuf> required,
    TStringBuf context)
{
    if (!value.IsMap()) {
        ythrow yexception() << context << " must be an object";
    }
    std::set<TString> expected;
    for (const TStringBuf key : required) {
        expected.emplace(key);
    }
    const auto& fields = value.GetMapSafe();
    for (const auto& [key, field] : fields) {
        Y_UNUSED(field);
        if (!expected.contains(key)) {
            ythrow yexception()
                << context << " has unexpected field " << key;
        }
    }
    for (const auto& key : expected) {
        if (!fields.contains(key)) {
            ythrow yexception()
                << context << " is missing field " << key;
        }
    }
}

ui64 PolicyUint(
    const NJson::TJsonValue& value,
    TStringBuf context)
{
    if (!value.IsUInteger()) {
        ythrow yexception() << context << " must be an unsigned integer";
    }
    return value.GetUIntegerSafe();
}

TCoveragePolicy DecodeCoveragePolicy(TStringBuf text) {
    NJson::TJsonValue root;
    if (!NJson::ReadJsonTree(text, &root, false)) {
        ythrow yexception() << "coverage policy is not valid JSON";
    }
    RequirePolicyKeys(
        root,
        {"format", "version", "row_bound", "task_bound", "suites"},
        "coverage policy");
    if (!root["format"].IsString() ||
        root["format"].GetStringSafe() != CoveragePolicyFormat)
    {
        ythrow yexception()
            << "coverage policy has unsupported format";
    }
    if (PolicyUint(root["version"], "coverage policy version") != 1) {
        ythrow yexception()
            << "coverage policy has unsupported version";
    }
    if (PolicyUint(root["row_bound"], "coverage policy row_bound") != RowBound ||
        PolicyUint(root["task_bound"], "coverage policy task_bound") != TaskBound)
    {
        ythrow yexception()
            << "coverage policy bounds do not match the dashboard";
    }

    const auto& suites = root["suites"];
    RequirePolicyKeys(suites, {Tpch.Name, Tpcds.Name}, "coverage policy suites");

    TCoveragePolicy policy;
    for (const TSuite* suite : {&Tpch, &Tpcds}) {
        const auto& encoded = suites[suite->Name];
        const TString context = TStringBuilder()
            << "coverage policy suite " << suite->Name;
        RequirePolicyKeys(
            encoded,
            {"query_count", "required_formula_queries"},
            context);
        if (PolicyUint(encoded["query_count"], context + " query_count") !=
            suite->QueryCount)
        {
            ythrow yexception()
                << context << " query_count does not match the corpus";
        }
        const auto& required = encoded["required_formula_queries"];
        if (!required.IsArray()) {
            ythrow yexception()
                << context << " required_formula_queries must be an array";
        }

        TSuiteCoveragePolicy suitePolicy;
        suitePolicy.QueryCount = suite->QueryCount;
        ui32 previous = 0;
        for (const auto& encodedId : required.GetArraySafe()) {
            const ui64 id = PolicyUint(
                encodedId,
                context + " required query id");
            if (id < 1 || id > suite->QueryCount) {
                ythrow yexception()
                    << context << " required query id " << id
                    << " is outside the corpus";
            }
            if (id <= previous) {
                ythrow yexception()
                    << context
                    << " required query ids must be strictly increasing";
            }
            previous = static_cast<ui32>(id);
            suitePolicy.RequiredFormulaQueries.insert(previous);
        }
        policy.Suites.emplace(suite->Name, std::move(suitePolicy));
    }
    return policy;
}

TCoveragePolicy LoadCoveragePolicy() {
    return DecodeCoveragePolicy(
        TFileInput(CoveragePolicyPath()).ReadAll());
}

bool IsFullSelection(
    const TSuite& suite,
    const std::set<ui32>& selected)
{
    if (selected.size() != suite.QueryCount) {
        return false;
    }
    for (ui32 queryId = 1; queryId <= suite.QueryCount; ++queryId) {
        if (!selected.contains(queryId)) {
            return false;
        }
    }
    return true;
}

TPolicyEvaluation EvaluateCoveragePolicy(
    const TCoveragePolicy& policy,
    const TSuite& suite,
    const std::set<ui32>& selected,
    const TMap<ui32, TString>& statuses,
    bool solverPresent)
{
    const auto suitePolicy = policy.Suites.find(suite.Name);
    if (suitePolicy == policy.Suites.end() ||
        suitePolicy->second.QueryCount != suite.QueryCount)
    {
        ythrow yexception()
            << "coverage policy does not match suite " << suite.Name;
    }

    TPolicyEvaluation result;
    result.FullSelection = IsFullSelection(suite, selected);
    result.Enforced = !solverPresent && result.FullSelection;
    result.RequiredFormulaQueries =
        suitePolicy->second.RequiredFormulaQueries;
    for (const auto& [queryId, status] : statuses) {
        if (status == "FORMULA_EMITTED") {
            result.FormulaEmittedQueries.insert(queryId);
        }
    }
    if (!result.Enforced) {
        return result;
    }
    for (const ui32 queryId : result.RequiredFormulaQueries) {
        const auto status = statuses.find(queryId);
        if (status == statuses.end()) {
            result.Violations.push_back(TStringBuilder()
                << suite.Name << " q" << queryId
                << " has no coverage outcome; expected FORMULA_EMITTED");
        } else if (status->second != "FORMULA_EMITTED") {
            result.Violations.push_back(TStringBuilder()
                << suite.Name << " q" << queryId
                << " regressed from FORMULA_EMITTED to " << status->second);
        }
    }
    return result;
}

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
    if (!TryParseFromTextFormat(defaultsStream, defaults)) {
        ythrow yexception() << "Cannot parse embedded KQP default settings";
    }
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
    if (!result.IsSuccess()) {
        ythrow yexception()
            << "Cannot create " << suite.Name << " tables: "
            << result.GetIssues().ToString();
    }
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

TMaybe<TString> CoverageSolver() {
    const auto enabled = TryGetEnv("RBO_COVERAGE_USE_SOLVER");
    if (!enabled || enabled->empty() || *enabled == "0") {
        return Nothing();
    }
    if (*enabled != "1") {
        ythrow yexception()
            << "RBO_COVERAGE_USE_SOLVER must be 0 or 1; got " << *enabled;
    }
    const auto solver = TryGetEnv("RBO_Z3");
    if (!solver || solver->empty()) {
        ythrow yexception()
            << "RBO_COVERAGE_USE_SOLVER=1 requires a non-empty RBO_Z3";
    }
    return solver;
}

ui64 TimeoutMs() {
    const auto value = TryGetEnv("RBO_COVERAGE_TIMEOUT_MS");
    if (!value) {
        return DefaultTimeoutMs;
    }
    ui64 result = 0;
    if (!TryFromString<ui64>(*value, result) || result == 0) {
        ythrow yexception()
            << "RBO_COVERAGE_TIMEOUT_MS must be a positive integer; got "
            << *value;
    }
    return result;
}

ui32 ParseQueryId(TStringBuf text, const TSuite& suite) {
    ui32 result = 0;
    if (!TryFromString<ui32>(text, result) ||
        result < 1 || result > suite.QueryCount)
    {
        ythrow yexception()
            << "Invalid query id " << text << " for " << suite.Name;
    }
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
        if (token.find('-', dash + 1) != TStringBuf::npos) {
            ythrow yexception() << "Invalid query range " << token;
        }
        const ui32 first = ParseQueryId(token.SubStr(0, dash), suite);
        const ui32 last = ParseQueryId(token.SubStr(dash + 1), suite);
        if (first > last) {
            ythrow yexception() << "Descending query range " << token;
        }
        for (ui32 queryId = first; queryId <= last; ++queryId) {
            result.insert(queryId);
        }
    }
    if (result.empty()) {
        ythrow yexception() << "RBO_COVERAGE_QUERIES selected no queries";
    }
    return result;
}

NJson::TJsonValue JsonIds(const TVector<ui32>& ids) {
    NJson::TJsonValue result(NJson::JSON_ARRAY);
    for (const ui32 id : ids) {
        result.AppendValue(id);
    }
    return result;
}

NJson::TJsonValue JsonIds(const std::set<ui32>& ids) {
    return JsonIds(TVector<ui32>(ids.begin(), ids.end()));
}

NJson::TJsonValue PolicyJson(
    const TPolicyEvaluation& evaluation,
    bool solverPresent)
{
    NJson::TJsonValue result(NJson::JSON_MAP);
    result["format"] = CoveragePolicyFormat;
    result["version"] = 1;
    result["valid"] = evaluation.Valid;
    result["mode"] = solverPresent ? "solver" : "formula_only";
    result["full_selection"] = evaluation.FullSelection;
    result["enforced"] = evaluation.Enforced;
    result["required_formula_queries"] =
        JsonIds(evaluation.RequiredFormulaQueries);
    result["formula_emitted_queries"] =
        JsonIds(evaluation.FormulaEmittedQueries);
    NJson::TJsonValue violations(NJson::JSON_ARRAY);
    for (const auto& violation : evaluation.Violations) {
        violations.AppendValue(violation);
    }
    result["violations"] = std::move(violations);
    return result;
}

NJson::TJsonValue CoverageReportHeader(const TSuite& suite) {
    NJson::TJsonValue result(NJson::JSON_MAP);
    result["format"] = CoverageReportFormat;
    result["version"] = CoverageReportVersion;
    result["suite"] = suite.Name;
    result["row_bound"] = RowBound;
    result["task_bound"] = TaskBound;
    return result;
}

bool ParseJson(const TString& text, NJson::TJsonValue& value) {
    return !text.empty() && NJson::ReadJsonTree(text, &value, false);
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
    TVector<std::pair<TString, TString>> UnsupportedReasons;
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

NJson::TJsonValue PreserveArtifacts(
    TStringBuf suiteSlug,
    ui32 queryId,
    const TRBOSemanticSnapshotBoundaryResultV1& initial,
    const TRBOSemanticSnapshotBoundaryResultV1& final,
    const TFsPath& formulaPath)
{
    const TString stem = TStringBuilder()
        << suiteSlug << "_q" << queryId;
    NJson::TJsonValue artifacts(NJson::JSON_MAP);

    const TString initialName = stem + ".initial.json";
    const TString finalName = stem + ".final.json";
    TFileOutput((GetOutputPath() / initialName).GetPath()).Write(initial.Json);
    TFileOutput((GetOutputPath() / finalName).GetPath()).Write(final.Json);
    artifacts["initial_snapshot"] = initialName;
    artifacts["final_snapshot"] = finalName;

    if (formulaPath.Exists()) {
        const TString formulaName = stem + ".smt2";
        formulaPath.CopyTo((GetOutputPath() / formulaName).GetPath(), true);
        artifacts["formula"] = formulaName;
    }
    return artifacts;
}

TOutcome RunVerifier(
    TStringBuf suiteSlug,
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

    NJson::TJsonValue stdoutVerdict;
    NJson::TJsonValue stderrVerdict;
    const bool stdoutJson = ParseJson(command.GetOutput(), stdoutVerdict);
    const bool stderrJson = ParseJson(command.GetError(), stderrVerdict);
    if (!stdoutJson && !stderrJson) {
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
    if (stdoutJson && stderrJson) {
        return HarnessError(
            queryId,
            prepareMs,
            2,
            "verifier returned JSON on both stdout and stderr");
    }
    NJson::TJsonValue verdict = stdoutJson
        ? std::move(stdoutVerdict)
        : std::move(stderrVerdict);
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
    if (status == "UNSUPPORTED") {
        outcome.UnsupportedReasons.emplace_back(
            outcome.Layer, outcome.Reason);
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
    if (status == "COUNTEREXAMPLE" || status == "UNKNOWN" ||
        status == "SCHEMA_MISMATCH" || status == "SOLVER_ERROR")
    {
        try {
            outcome.Json["artifacts"] = PreserveArtifacts(
                suiteSlug, queryId, initial, final, formulaPath);
        } catch (const std::exception& error) {
            outcome.Json["artifact_error"] = error.what();
        }
    }
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
    // Literal folding and other preparation paths assume a real actor
    // activation context, just as the production KQP request path provides.
    const auto prepared = kikimr.GetTestServer().GetRuntime()->RunCall([
        host,
        query = Query(suite, queryId),
        settings
    ] {
        return host->SyncPrepareDataQuery(query, settings);
    });
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
        if (!initial.IsSupported()) {
            outcome.UnsupportedReasons.emplace_back(
                "initial_export", initial.UnsupportedReason);
        }
        if (!final.IsSupported()) {
            outcome.UnsupportedReasons.emplace_back(
                "final_export", final.UnsupportedReason);
        }
        outcome.Layer = outcome.UnsupportedReasons.front().first;
        outcome.Reason = outcome.UnsupportedReasons.front().second;
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
        suite.Slug,
        queryId,
        prepareMs,
        initial,
        final,
        timeoutMs,
        solver);
}

void RunCoverage(const TSuite& suite) {
    TMaybe<TString> solver;
    TMaybe<TCoveragePolicy> policy;
    std::set<ui32> selected;
    TMap<ui32, TString> statuses;
    TVector<TString> policyLoadViolations;
    ui64 timeoutMs = DefaultTimeoutMs;
    bool timeoutResolved = false;
    NJson::TJsonValue rows(NJson::JSON_ARRAY);
    TMap<TString, ui32> summary;
    TMap<std::pair<TString, TString>, TVector<ui32>> unsupported;
    TMap<TString, TVector<ui32>> optimizerFailures;
    bool fatal = false;

    const auto record = [&](ui32 queryId, TString source, TOutcome outcome) {
        ++summary[outcome.Status];
        if (outcome.Status == "UNSUPPORTED") {
            for (const auto& reason : outcome.UnsupportedReasons) {
                unsupported[reason].push_back(queryId);
            }
        } else if (outcome.Status == "OPTIMIZER_FAILURE") {
            optimizerFailures[outcome.Reason].push_back(queryId);
        }
        if (queryId >= 1 && queryId <= suite.QueryCount) {
            statuses[queryId] = outcome.Status;
        }
        fatal = fatal || outcome.Fatal;
        outcome.Json["suite"] = suite.Name;
        outcome.Json["source"] = std::move(source);
        outcome.Json["timeout_ms"] = timeoutResolved
            ? NJson::TJsonValue(timeoutMs)
            : NJson::TJsonValue(NJson::JSON_NULL);
        rows.AppendValue(std::move(outcome.Json));
    };

    try {
        policy = LoadCoveragePolicy();
        timeoutMs = TimeoutMs();
        timeoutResolved = true;
        selected = SelectedQueries(suite);
        solver = CoverageSolver();
        auto kikimr = MakeRunner();
        CreateTables(kikimr, suite);

        NYql::TExprContext moduleContext;
        NYql::IModuleResolver::TPtr moduleResolver;
        if (!NYql::GetYqlDefaultModuleResolver(moduleContext, moduleResolver)) {
            ythrow yexception() << "Cannot construct the default YQL module resolver";
        }

        for (const ui32 queryId : selected) {
            Cerr << "Checking " << suite.Name << " q" << queryId << Endl;
            TOutcome outcome;
            try {
                outcome = ClassifyQuery(
                    kikimr,
                    moduleResolver,
                    suite,
                    queryId,
                    timeoutMs,
                    solver);
            } catch (const std::exception& error) {
                outcome = HarnessError(
                    queryId,
                    0,
                    0,
                    TStringBuilder() << "classification threw: " << error.what());
            } catch (...) {
                outcome = HarnessError(
                    queryId, 0, 0, "classification threw a non-standard exception");
            }
            record(
                queryId,
                suite.QueryPrefix + ToString(queryId) + ".yql",
                std::move(outcome));
        }
    } catch (const std::exception& error) {
        if (!policy) {
            policyLoadViolations.push_back(TStringBuilder()
                << "coverage policy is invalid: " << error.what());
        }
        record(
            0,
            "",
            HarnessError(
                0, 0, 0, TStringBuilder() << "suite execution threw: " << error.what()));
    } catch (...) {
        if (!policy) {
            policyLoadViolations.push_back(
                "coverage policy is invalid: non-standard exception");
        }
        record(
            0,
            "",
            HarnessError(0, 0, 0, "suite execution threw a non-standard exception"));
    }

    TPolicyEvaluation policyEvaluation;
    if (!policy) {
        policyEvaluation.Valid = false;
        policyEvaluation.Enforced = true;
        policyEvaluation.Violations = policyLoadViolations;
    } else {
        try {
            policyEvaluation = EvaluateCoveragePolicy(
                *policy,
                suite,
                selected,
                statuses,
                solver.Defined());
        } catch (const std::exception& error) {
            policyEvaluation.Valid = false;
            policyEvaluation.Enforced = true;
            policyEvaluation.Violations.push_back(TStringBuilder()
                << "coverage policy evaluation failed: " << error.what());
        } catch (...) {
            policyEvaluation.Valid = false;
            policyEvaluation.Enforced = true;
            policyEvaluation.Violations.push_back(
                "coverage policy evaluation failed: non-standard exception");
        }
    }
    fatal = fatal || !policyEvaluation.Violations.empty();

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

    NJson::TJsonValue report = CoverageReportHeader(suite);
    report["solver_present"] = solver.Defined();
    report["timeout_ms"] = timeoutResolved
        ? NJson::TJsonValue(timeoutMs)
        : NJson::TJsonValue(NJson::JSON_NULL);
    report["summary"] = std::move(summaryJson);
    report["queries"] = std::move(rows);
    report["unsupported_inventory"] = std::move(unsupportedJson);
    report["optimizer_failure_inventory"] = std::move(optimizerJson);
    report["policy"] = PolicyJson(policyEvaluation, solver.Defined());

    const auto reportPath = GetOutputPath() / (suite.Slug + "_coverage.json");
    TFileOutput(reportPath.GetPath()).Write(NJson::WriteJson(
        report,
        true,
        true));
    Cout << suite.Name << " summary: "
         << NJson::WriteJson(report["summary"], false, true) << Endl
         << "Coverage policy: "
         << NJson::WriteJson(report["policy"], false, true) << Endl
         << "Coverage report: " << reportPath.GetPath() << Endl;
    UNIT_ASSERT_C(
        !fatal,
        "correctness, harness, or coverage policy failure; see "
            << reportPath.GetPath());
}

} // namespace

Y_UNIT_TEST_SUITE(TRBOBenchmarkCoverage) {
    Y_UNIT_TEST(PolicyFileMatchesFixedContract) {
        const auto policy = LoadCoveragePolicy();
        UNIT_ASSERT_VALUES_EQUAL(policy.Suites.size(), 2);
        UNIT_ASSERT(policy.Suites.at(Tpch.Name).RequiredFormulaQueries.empty());
        UNIT_ASSERT(
            policy.Suites.at(Tpcds.Name).RequiredFormulaQueries ==
            std::set<ui32>({88, 96}));

        const auto report = CoverageReportHeader(Tpcds);
        UNIT_ASSERT_VALUES_EQUAL(
            report["format"].GetStringSafe(),
            CoverageReportFormat);
        UNIT_ASSERT_VALUES_EQUAL(
            report["version"].GetUIntegerSafe(),
            CoverageReportVersion);
    }

    Y_UNIT_TEST(PolicyAllowsMonotonicCoverageImprovements) {
        const auto policy = LoadCoveragePolicy();
        std::set<ui32> selected;
        for (ui32 queryId = 1; queryId <= Tpcds.QueryCount; ++queryId) {
            selected.insert(queryId);
        }
        const TMap<ui32, TString> statuses = {
            {1, "FORMULA_EMITTED"},
            {88, "FORMULA_EMITTED"},
            {96, "FORMULA_EMITTED"},
        };
        const auto evaluation = EvaluateCoveragePolicy(
            policy,
            Tpcds,
            selected,
            statuses,
            false);
        UNIT_ASSERT(evaluation.Enforced);
        UNIT_ASSERT(evaluation.Violations.empty());
        UNIT_ASSERT(
            evaluation.FormulaEmittedQueries ==
            std::set<ui32>({1, 88, 96}));
    }

    Y_UNIT_TEST(PolicyReportsEveryFloorRegression) {
        const auto policy = LoadCoveragePolicy();
        std::set<ui32> selected;
        for (ui32 queryId = 1; queryId <= Tpcds.QueryCount; ++queryId) {
            selected.insert(queryId);
        }
        const TMap<ui32, TString> statuses = {
            {88, "UNSUPPORTED"},
        };
        const auto evaluation = EvaluateCoveragePolicy(
            policy,
            Tpcds,
            selected,
            statuses,
            false);
        UNIT_ASSERT(evaluation.Enforced);
        UNIT_ASSERT_VALUES_EQUAL(evaluation.Violations.size(), 2);
        UNIT_ASSERT(evaluation.Violations[0].Contains(
            "q88 regressed from FORMULA_EMITTED to UNSUPPORTED"));
        UNIT_ASSERT(evaluation.Violations[1].Contains(
            "q96 has no coverage outcome"));

        const auto report = PolicyJson(evaluation, false);
        UNIT_ASSERT(report["enforced"].GetBooleanSafe());
        UNIT_ASSERT_VALUES_EQUAL(
            report["violations"].GetArraySafe().size(),
            2);
    }

    Y_UNIT_TEST(PolicyDoesNotGateFocusedOrSolverRuns) {
        const auto policy = LoadCoveragePolicy();
        const TMap<ui32, TString> statuses;
        const auto focused = EvaluateCoveragePolicy(
            policy,
            Tpcds,
            {88, 96},
            statuses,
            false);
        UNIT_ASSERT(!focused.Enforced);
        UNIT_ASSERT(focused.Violations.empty());

        std::set<ui32> selected;
        for (ui32 queryId = 1; queryId <= Tpcds.QueryCount; ++queryId) {
            selected.insert(queryId);
        }
        const auto solver = EvaluateCoveragePolicy(
            policy,
            Tpcds,
            selected,
            statuses,
            true);
        UNIT_ASSERT(!solver.Enforced);
        UNIT_ASSERT(solver.Violations.empty());
    }

    Y_UNIT_TEST(PolicySolverRequiresExplicitOptIn) {
        {
            NTesting::TScopedEnvironment environment{{
                {"RBO_COVERAGE_USE_SOLVER", ""},
                {"RBO_Z3", "/ambient/z3"},
            }};
            UNIT_ASSERT(!CoverageSolver());
        }
        {
            NTesting::TScopedEnvironment environment{{
                {"RBO_COVERAGE_USE_SOLVER", "1"},
                {"RBO_Z3", ""},
            }};
            UNIT_ASSERT_EXCEPTION_CONTAINS(
                CoverageSolver(),
                yexception,
                "requires a non-empty RBO_Z3");
        }
        {
            NTesting::TScopedEnvironment environment{{
                {"RBO_COVERAGE_USE_SOLVER", "1"},
                {"RBO_Z3", "/explicit/z3"},
            }};
            const auto solver = CoverageSolver();
            UNIT_ASSERT(solver);
            UNIT_ASSERT_VALUES_EQUAL(*solver, "/explicit/z3");
        }
    }

    Y_UNIT_TEST(PolicyRejectsMalformedDocuments) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            DecodeCoveragePolicy("{"),
            yexception,
            "not valid JSON");

        NJson::TJsonValue encoded;
        UNIT_ASSERT(NJson::ReadJsonTree(
            TFileInput(CoveragePolicyPath()).ReadAll(),
            &encoded,
            true));
        encoded["unexpected"] = true;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            DecodeCoveragePolicy(NJson::WriteJson(encoded, false, true)),
            yexception,
            "unexpected field unexpected");

        encoded.EraseValue("unexpected");
        NJson::TJsonValue unordered(NJson::JSON_ARRAY);
        unordered.AppendValue(96);
        unordered.AppendValue(88);
        encoded["suites"][Tpcds.Name]["required_formula_queries"] =
            std::move(unordered);
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            DecodeCoveragePolicy(NJson::WriteJson(encoded, false, true)),
            yexception,
            "strictly increasing");
    }

    Y_UNIT_TEST(TPCH) {
        RunCoverage(Tpch);
    }

    Y_UNIT_TEST(TPCDS) {
        RunCoverage(Tpcds);
    }
}

} // namespace NKikimr::NKqp
