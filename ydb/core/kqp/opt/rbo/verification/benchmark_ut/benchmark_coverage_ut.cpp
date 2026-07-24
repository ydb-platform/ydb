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

#include <openssl/sha.h>

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
#include <util/string/hex.h>
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
constexpr ui64 ProofFloorTimeoutMs = 60'000;
constexpr const char* CoverageReportFormat =
    "ydb-rbo-benchmark-coverage";
constexpr ui64 CoverageReportVersion = 4;
constexpr const char* CoveragePolicyFormat =
    "ydb-rbo-benchmark-coverage-policy";
constexpr ui64 CoveragePolicyVersion = 3;
constexpr const char* CoveragePolicyEvaluationFormat =
    "ydb-rbo-benchmark-coverage-policy-evaluation";
constexpr ui64 CoveragePolicyEvaluationVersion = 2;

enum class ECoverageRun {
    Environment,
    ProofFloor,
};

enum class ECoverageMode {
    FormulaDashboard,
    SolverExperiment,
    ProofFloor,
};

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
    std::set<ui32> RequiredVerifierEntryQueries;
    std::set<ui32> RequiredFormulaQueries;
    std::set<ui32> RequiredVerifiedQueries;
};

struct TCoveragePolicy {
    TMap<TString, TSuiteCoveragePolicy> Suites;
};

struct TPolicyEvaluation {
    bool Valid = true;
    ECoverageMode Mode = ECoverageMode::FormulaDashboard;
    bool FullSelection = false;
    bool VerifierEntryFloorEnforced = false;
    bool FormulaFloorEnforced = false;
    bool ProofFloorEnforced = false;
    std::set<ui32> SelectedQueries;
    std::set<ui32> RequiredVerifierEntryQueries;
    std::set<ui32> VerifierEntryQueries;
    std::set<ui32> RequiredFormulaQueries;
    std::set<ui32> FormulaEmittedQueries;
    std::set<ui32> RequiredVerifiedQueries;
    std::set<ui32> VerifiedBoundedQueries;
    TVector<TString> Violations;
};

struct TCoverageRunConfig {
    ECoverageMode Mode = ECoverageMode::FormulaDashboard;
    std::set<ui32> Selected;
    TMaybe<TString> Solver;
    ui64 TimeoutMs = DefaultTimeoutMs;
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

std::set<ui32> PolicyQueryIds(
    const NJson::TJsonValue& value,
    const TSuite& suite,
    TStringBuf field,
    TStringBuf context)
{
    if (!value.IsArray()) {
        ythrow yexception()
            << context << " " << field << " must be an array";
    }

    std::set<ui32> result;
    ui32 previous = 0;
    for (const auto& encodedId : value.GetArraySafe()) {
        const ui64 id = PolicyUint(
            encodedId,
            TStringBuilder() << context << " " << field << " query id");
        if (id < 1 || id > suite.QueryCount) {
            ythrow yexception()
                << context << " " << field << " query id " << id
                << " is outside the corpus";
        }
        if (id <= previous) {
            ythrow yexception()
                << context << " " << field
                << " query ids must be strictly increasing";
        }
        previous = static_cast<ui32>(id);
        result.insert(previous);
    }
    return result;
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
    if (PolicyUint(root["version"], "coverage policy version") !=
        CoveragePolicyVersion)
    {
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
            {
                "query_count",
                "required_verifier_entry_queries",
                "required_formula_queries",
                "required_verified_queries",
            },
            context);
        if (PolicyUint(encoded["query_count"], context + " query_count") !=
            suite->QueryCount)
        {
            ythrow yexception()
                << context << " query_count does not match the corpus";
        }
        TSuiteCoveragePolicy suitePolicy;
        suitePolicy.QueryCount = suite->QueryCount;
        suitePolicy.RequiredVerifierEntryQueries = PolicyQueryIds(
            encoded["required_verifier_entry_queries"],
            *suite,
            "required_verifier_entry_queries",
            context);
        suitePolicy.RequiredFormulaQueries = PolicyQueryIds(
            encoded["required_formula_queries"],
            *suite,
            "required_formula_queries",
            context);
        suitePolicy.RequiredVerifiedQueries = PolicyQueryIds(
            encoded["required_verified_queries"],
            *suite,
            "required_verified_queries",
            context);
        if (suitePolicy.RequiredVerifiedQueries.empty()) {
            ythrow yexception()
                << context << " required_verified_queries must not be empty";
        }
        for (const ui32 queryId : suitePolicy.RequiredVerifiedQueries) {
            if (!suitePolicy.RequiredFormulaQueries.contains(queryId)) {
                ythrow yexception()
                    << context << " required verified query q" << queryId
                    << " is not a required formula query";
            }
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

std::set<ui32> OutcomeIds(const TMap<ui32, TString>& statuses) {
    std::set<ui32> result;
    for (const auto& [queryId, status] : statuses) {
        Y_UNUSED(status);
        result.insert(queryId);
    }
    return result;
}

TMap<ui32, TString> FormulaFloorStatuses(
    const TCoveragePolicy& policy,
    const TSuite& suite)
{
    TMap<ui32, TString> result;
    for (const ui32 queryId :
         policy.Suites.at(suite.Name).RequiredFormulaQueries)
    {
        result[queryId] = "FORMULA_EMITTED";
    }
    return result;
}

TPolicyEvaluation EvaluateCoveragePolicy(
    const TCoveragePolicy& policy,
    const TSuite& suite,
    const std::set<ui32>& selected,
    const TMap<ui32, TString>& statuses,
    const std::set<ui32>& verifierEntryQueries,
    ECoverageMode mode)
{
    const auto suitePolicy = policy.Suites.find(suite.Name);
    if (suitePolicy == policy.Suites.end() ||
        suitePolicy->second.QueryCount != suite.QueryCount)
    {
        ythrow yexception()
            << "coverage policy does not match suite " << suite.Name;
    }

    TPolicyEvaluation result;
    result.Mode = mode;
    result.SelectedQueries = selected;
    result.FullSelection = IsFullSelection(suite, selected);
    result.RequiredVerifierEntryQueries =
        suitePolicy->second.RequiredVerifierEntryQueries;
    result.VerifierEntryQueries = verifierEntryQueries;
    result.RequiredFormulaQueries =
        suitePolicy->second.RequiredFormulaQueries;
    result.RequiredVerifiedQueries =
        suitePolicy->second.RequiredVerifiedQueries;
    for (const auto& [queryId, status] : statuses) {
        if (status == "FORMULA_EMITTED") {
            result.FormulaEmittedQueries.insert(queryId);
        } else if (status == "VERIFIED_BOUNDED") {
            result.VerifiedBoundedQueries.insert(queryId);
        }
    }

    if (mode == ECoverageMode::SolverExperiment) {
        return result;
    }

    if (mode == ECoverageMode::FormulaDashboard) {
        result.VerifierEntryFloorEnforced = result.FullSelection;
        result.FormulaFloorEnforced = result.FullSelection;
        if (!result.FormulaFloorEnforced) {
            return result;
        }
        for (const ui32 queryId : result.RequiredVerifierEntryQueries) {
            const auto status = statuses.find(queryId);
            if (status == statuses.end()) {
                result.Violations.push_back(TStringBuilder()
                    << suite.Name << " q" << queryId
                    << " has no coverage outcome; expected verifier entry");
            } else if (!result.VerifierEntryQueries.contains(queryId)) {
                result.Violations.push_back(TStringBuilder()
                    << suite.Name << " q" << queryId
                    << " regressed before verifier entry with status "
                    << status->second);
            }
        }
        for (const ui32 queryId : result.RequiredFormulaQueries) {
            const auto status = statuses.find(queryId);
            if (status == statuses.end()) {
                result.Violations.push_back(TStringBuilder()
                    << suite.Name << " q" << queryId
                    << " has no coverage outcome; expected FORMULA_EMITTED");
            } else if (
                status->second != "FORMULA_EMITTED" &&
                status->second != "VERIFIED_BOUNDED")
            {
                result.Violations.push_back(TStringBuilder()
                    << suite.Name << " q" << queryId
                    << " regressed from FORMULA_EMITTED to " << status->second);
            }
        }
        return result;
    }

    result.ProofFloorEnforced = true;
    if (selected != result.RequiredVerifiedQueries) {
        result.Violations.push_back(TStringBuilder()
            << suite.Name
            << " proof floor did not select exactly its required verified queries");
    }
    for (const ui32 queryId : result.RequiredVerifiedQueries) {
        const auto status = statuses.find(queryId);
        if (status == statuses.end()) {
            result.Violations.push_back(TStringBuilder()
                << suite.Name << " q" << queryId
                << " has no proof outcome; expected VERIFIED_BOUNDED");
        } else if (status->second != "VERIFIED_BOUNDED") {
            result.Violations.push_back(TStringBuilder()
                << suite.Name << " q" << queryId
                << " regressed from VERIFIED_BOUNDED to " << status->second);
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
    return BinaryPath("contrib/tools/z3/z3");
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

TCoverageRunConfig ResolveCoverageRun(
    const TCoveragePolicy& policy,
    const TSuite& suite,
    ECoverageRun run)
{
    TCoverageRunConfig result;
    if (run == ECoverageRun::ProofFloor) {
        const auto suitePolicy = policy.Suites.find(suite.Name);
        if (suitePolicy == policy.Suites.end()) {
            ythrow yexception()
                << "coverage policy does not match suite " << suite.Name;
        }
        result.Mode = ECoverageMode::ProofFloor;
        result.Selected = suitePolicy->second.RequiredVerifiedQueries;
        result.Solver = BinaryPath("contrib/tools/z3/z3");
        result.TimeoutMs = ProofFloorTimeoutMs;
        return result;
    }

    result.Selected = SelectedQueries(suite);
    result.Solver = CoverageSolver();
    result.TimeoutMs = TimeoutMs();
    result.Mode = result.Solver
        ? ECoverageMode::SolverExperiment
        : ECoverageMode::FormulaDashboard;
    return result;
}

TStringBuf CoverageModeName(ECoverageMode mode) {
    switch (mode) {
        case ECoverageMode::FormulaDashboard:
            return "formula_dashboard";
        case ECoverageMode::SolverExperiment:
            return "solver_experiment";
        case ECoverageMode::ProofFloor:
            return "proof_floor";
    }
    ythrow yexception() << "unknown coverage mode";
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

NJson::TJsonValue PolicyEvaluationJson(
    const TPolicyEvaluation& evaluation)
{
    NJson::TJsonValue result(NJson::JSON_MAP);
    result["format"] = CoveragePolicyEvaluationFormat;
    result["version"] = CoveragePolicyEvaluationVersion;
    result["valid"] = evaluation.Valid;
    result["mode"] = CoverageModeName(evaluation.Mode);
    result["full_selection"] = evaluation.FullSelection;
    result["verifier_entry_floor_enforced"] =
        evaluation.VerifierEntryFloorEnforced;
    result["formula_floor_enforced"] = evaluation.FormulaFloorEnforced;
    result["proof_floor_enforced"] = evaluation.ProofFloorEnforced;
    result["selected_queries"] = JsonIds(evaluation.SelectedQueries);
    result["required_verifier_entry_queries"] =
        JsonIds(evaluation.RequiredVerifierEntryQueries);
    result["verifier_entry_queries"] =
        JsonIds(evaluation.VerifierEntryQueries);
    result["required_formula_queries"] =
        JsonIds(evaluation.RequiredFormulaQueries);
    result["formula_emitted_queries"] =
        JsonIds(evaluation.FormulaEmittedQueries);
    result["required_verified_queries"] =
        JsonIds(evaluation.RequiredVerifiedQueries);
    result["verified_bounded_queries"] =
        JsonIds(evaluation.VerifiedBoundedQueries);
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

NJson::TJsonValue VerdictForCoverageReport(
    NJson::TJsonValue verdict,
    TStringBuf status)
{
    // NJson represents integer tokens outside ui64 as doubles. Keep the exact
    // counterexample witness only in the byte-for-byte verifier artifact.
    if (status == "COUNTEREXAMPLE") {
        verdict.EraseValue("witness");
    }
    return verdict;
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
    TStringBuf query,
    TStringBuf verifierVerdict,
    const TRBOSemanticSnapshotBoundaryResultV1& initial,
    const TRBOSemanticSnapshotBoundaryResultV1& final,
    const TFsPath& formulaPath)
{
    const TString stem = TStringBuilder()
        << suiteSlug << "_q" << queryId;
    NJson::TJsonValue artifacts(NJson::JSON_MAP);

    const auto preserveText = [&](
        TStringBuf key,
        const TString& name,
        TStringBuf text)
    {
        unsigned char digest[SHA256_DIGEST_LENGTH];
        if (!SHA256(
                reinterpret_cast<const unsigned char*>(text.data()),
                text.size(),
                digest))
        {
            ythrow yexception() << "cannot hash diagnostic artifact " << name;
        }
        TFileOutput((GetOutputPath() / name).GetPath()).Write(text);
        artifacts[TString(key)] = name;
        artifacts[TStringBuilder() << key << "_sha256"] =
            to_lower(HexEncode(digest, sizeof(digest)));
    };

    preserveText("initial_snapshot", stem + ".initial.json", initial.Json);
    preserveText("final_snapshot", stem + ".final.json", final.Json);
    preserveText("query", stem + ".query.yql", query);
    preserveText(
        "verifier_verdict",
        stem + ".verdict.json",
        verifierVerdict);

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
    TStringBuf query,
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

    const TString stdoutText = command.GetOutput();
    const TString stderrText = command.GetError();
    NJson::TJsonValue stdoutVerdict;
    NJson::TJsonValue stderrVerdict;
    const bool stdoutJson = ParseJson(stdoutText, stdoutVerdict);
    const bool stderrJson = ParseJson(stderrText, stderrVerdict);
    if (!stdoutJson && !stderrJson) {
        return HarnessError(
            queryId,
            prepareMs,
            2,
            TStringBuilder()
                << "verifier returned no JSON; exit="
                << command.GetExitCode().GetOrElse(-1)
                << "; stdout=" << stdoutText
                << "; stderr=" << stderrText);
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
    const TString& verifierVerdict = stdoutJson ? stdoutText : stderrText;
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
    outcome.Json["verdict"] = VerdictForCoverageReport(
        std::move(verdict), status);
    if (status == "COUNTEREXAMPLE" || status == "UNKNOWN" ||
        status == "SCHEMA_MISMATCH" || status == "SOLVER_ERROR")
    {
        try {
            outcome.Json["artifacts"] = PreserveArtifacts(
                suiteSlug,
                queryId,
                query,
                verifierVerdict,
                initial,
                final,
                formulaPath);
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
    const TString query = Query(suite, queryId);
    const TInstant started = TInstant::Now();
    // Literal folding and other preparation paths assume a real actor
    // activation context, just as the production KQP request path provides.
    const auto prepared = kikimr.GetTestServer().GetRuntime()->RunCall([
        host,
        query,
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
        query,
        prepareMs,
        initial,
        final,
        timeoutMs,
        solver);
}

void RunCoverage(const TSuite& suite, ECoverageRun run) {
    ECoverageMode mode = run == ECoverageRun::ProofFloor
        ? ECoverageMode::ProofFloor
        : ECoverageMode::FormulaDashboard;
    TMaybe<TString> solver;
    TMaybe<TCoveragePolicy> policy;
    std::set<ui32> selected;
    TMap<ui32, TString> statuses;
    std::set<ui32> verifierEntryQueries;
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
            if (outcome.Layer == "verifier") {
                verifierEntryQueries.insert(queryId);
            }
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
        const auto config = ResolveCoverageRun(*policy, suite, run);
        mode = config.Mode;
        timeoutMs = config.TimeoutMs;
        timeoutResolved = true;
        selected = config.Selected;
        solver = config.Solver;

        NYql::IModuleResolver::TPtr moduleResolver;
        // RunCall work may retain the host, and therefore the resolver, until
        // the runner's thread pool is torn down. Give the resolver ownership
        // of its expression context and construct it before the runner, so
        // both outlive that teardown.
        if (!NYql::GetYqlDefaultModuleResolverWithContext(moduleResolver)) {
            ythrow yexception() << "Cannot construct the default YQL module resolver";
        }

        auto kikimr = MakeRunner();
        CreateTables(kikimr, suite);

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
    policyEvaluation.Mode = mode;
    if (!policy) {
        policyEvaluation.Valid = false;
        policyEvaluation.VerifierEntryFloorEnforced =
            mode == ECoverageMode::FormulaDashboard;
        policyEvaluation.FormulaFloorEnforced =
            mode == ECoverageMode::FormulaDashboard;
        policyEvaluation.ProofFloorEnforced =
            mode == ECoverageMode::ProofFloor;
        policyEvaluation.Violations = policyLoadViolations;
    } else {
        try {
            policyEvaluation = EvaluateCoveragePolicy(
                *policy,
                suite,
                selected,
                statuses,
                verifierEntryQueries,
                mode);
        } catch (const std::exception& error) {
            policyEvaluation.Valid = false;
            policyEvaluation.VerifierEntryFloorEnforced =
                mode == ECoverageMode::FormulaDashboard;
            policyEvaluation.FormulaFloorEnforced =
                mode == ECoverageMode::FormulaDashboard;
            policyEvaluation.ProofFloorEnforced =
                mode == ECoverageMode::ProofFloor;
            policyEvaluation.Violations.push_back(TStringBuilder()
                << "coverage policy evaluation failed: " << error.what());
        } catch (...) {
            policyEvaluation.Valid = false;
            policyEvaluation.VerifierEntryFloorEnforced =
                mode == ECoverageMode::FormulaDashboard;
            policyEvaluation.FormulaFloorEnforced =
                mode == ECoverageMode::FormulaDashboard;
            policyEvaluation.ProofFloorEnforced =
                mode == ECoverageMode::ProofFloor;
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
    report["policy"] = PolicyEvaluationJson(policyEvaluation);

    const TString reportName = run == ECoverageRun::ProofFloor
        ? suite.Slug + "_proof_floor.json"
        : suite.Slug + "_coverage.json";
    const auto reportPath = GetOutputPath() / reportName;
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
        UNIT_ASSERT(
            policy.Suites.at(Tpch.Name).RequiredVerifierEntryQueries ==
            std::set<ui32>({1}));
        UNIT_ASSERT(
            policy.Suites.at(Tpch.Name).RequiredFormulaQueries ==
            std::set<ui32>({1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 18, 19, 22}));
        UNIT_ASSERT(
            policy.Suites.at(Tpch.Name).RequiredVerifiedQueries ==
            std::set<ui32>({3, 4, 6, 11, 12, 14, 15, 18, 19, 22}));
        UNIT_ASSERT(
            policy.Suites.at(Tpcds.Name).RequiredVerifierEntryQueries ==
            std::set<ui32>({5, 65, 80}));
        UNIT_ASSERT(
            policy.Suites.at(Tpcds.Name).RequiredFormulaQueries ==
            std::set<ui32>({
                3, 5, 6, 10, 15, 19, 25, 29, 37, 38, 40, 42, 43, 46, 48,
                50, 52, 55, 56, 60, 61, 62, 65, 68, 69, 71, 76, 77, 79, 80,
                82, 87, 88, 90, 91, 93, 95, 96, 99,
            }));
        UNIT_ASSERT(
            policy.Suites.at(Tpcds.Name).RequiredVerifiedQueries ==
            std::set<ui32>({
                3, 38, 42, 48, 52, 55, 69, 87, 90, 93, 95, 96,
            }));

        const auto report = CoverageReportHeader(Tpcds);
        UNIT_ASSERT_VALUES_EQUAL(
            report["format"].GetStringSafe(),
            CoverageReportFormat);
        UNIT_ASSERT_VALUES_EQUAL(
            report["version"].GetUIntegerSafe(),
            4);
    }

    Y_UNIT_TEST(DiagnosticArtifactsPreserveExactBytes) {
        const TString initialJson = "{\"boundary\":\"initial\"}\n";
        const TString finalJson = "{\"boundary\":\"final\"}\n";
        const TString query = "SELECT 1;\r\n";
        const TString verifierVerdict =
            "{\"row_bound\":2,\"status\":\"COUNTEREXAMPLE\",\"task_bound\":2,"
            "\"witness\":{\"table\":[{\"amount\":"
            "99999999999999999999999999999999999}]}}\n";
        const TString formula = "(check-sat)\n";
        const TRBOSemanticSnapshotBoundaryResultV1 initial{
            ERBOSemanticSnapshotBoundaryV1::Initial,
            initialJson,
            {},
            {},
        };
        const TRBOSemanticSnapshotBoundaryResultV1 final{
            ERBOSemanticSnapshotBoundaryV1::Final,
            finalJson,
            {},
            {},
        };
        TTempDir tempDir;
        const auto formulaPath = tempDir.Path() / "problem.smt2";
        TFileOutput(formulaPath.GetPath()).Write(formula);

        const auto artifacts = PreserveArtifacts(
            "artifact_contract",
            7,
            query,
            verifierVerdict,
            initial,
            final,
            formulaPath);

        UNIT_ASSERT_VALUES_EQUAL(artifacts.GetMapSafe().size(), 9);
        UNIT_ASSERT_VALUES_EQUAL(
            artifacts["initial_snapshot"].GetStringSafe(),
            "artifact_contract_q7.initial.json");
        UNIT_ASSERT_VALUES_EQUAL(
            artifacts["final_snapshot"].GetStringSafe(),
            "artifact_contract_q7.final.json");
        UNIT_ASSERT_VALUES_EQUAL(
            artifacts["query"].GetStringSafe(),
            "artifact_contract_q7.query.yql");
        UNIT_ASSERT_VALUES_EQUAL(
            artifacts["verifier_verdict"].GetStringSafe(),
            "artifact_contract_q7.verdict.json");
        UNIT_ASSERT_VALUES_EQUAL(
            artifacts["formula"].GetStringSafe(),
            "artifact_contract_q7.smt2");
        UNIT_ASSERT_VALUES_EQUAL(
            artifacts["query_sha256"].GetStringSafe(),
            "d3cd5042f97738960d802ad6b3a548dfa18152215118ba18f04493bc6944b0e4");
        UNIT_ASSERT_VALUES_EQUAL(
            artifacts["initial_snapshot_sha256"].GetStringSafe(),
            "8d8c42b4c53466a92ec001719137bef5542ccab67674ffd9aa6285ef5d67b444");
        UNIT_ASSERT_VALUES_EQUAL(
            artifacts["final_snapshot_sha256"].GetStringSafe(),
            "a9cc8a8d51463f3e4e115a4bde9e6e592f39c19cc3d1880ba1aeadd20f8b4d27");
        UNIT_ASSERT_VALUES_EQUAL(
            artifacts["verifier_verdict_sha256"].GetStringSafe(),
            "20e32d803d936b5149286a66dcde8f6841cff1ab1e2a3aad849fbe96334d199d");
        UNIT_ASSERT_VALUES_EQUAL(
            TFileInput((GetOutputPath() /
                artifacts["initial_snapshot"].GetStringSafe()).GetPath()).ReadAll(),
            initialJson);
        UNIT_ASSERT_VALUES_EQUAL(
            TFileInput((GetOutputPath() /
                artifacts["final_snapshot"].GetStringSafe()).GetPath()).ReadAll(),
            finalJson);
        UNIT_ASSERT_VALUES_EQUAL(
            TFileInput((GetOutputPath() /
                artifacts["query"].GetStringSafe()).GetPath()).ReadAll(),
            query);
        UNIT_ASSERT_VALUES_EQUAL(
            TFileInput((GetOutputPath() /
                artifacts["verifier_verdict"].GetStringSafe()).GetPath()).ReadAll(),
            verifierVerdict);
        UNIT_ASSERT_VALUES_EQUAL(
            TFileInput((GetOutputPath() /
                artifacts["formula"].GetStringSafe()).GetPath()).ReadAll(),
            formula);

        NJson::TJsonValue parsedVerdict;
        UNIT_ASSERT(ParseJson(verifierVerdict, parsedVerdict));
        UNIT_ASSERT(parsedVerdict.Has("witness"));
        const auto reportVerdict = VerdictForCoverageReport(
            std::move(parsedVerdict), "COUNTEREXAMPLE");
        UNIT_ASSERT(!reportVerdict.Has("witness"));
        UNIT_ASSERT_VALUES_EQUAL(
            reportVerdict["status"].GetStringSafe(),
            "COUNTEREXAMPLE");
    }

    Y_UNIT_TEST(PolicyAllowsMonotonicCoverageImprovements) {
        const auto policy = LoadCoveragePolicy();
        std::set<ui32> selected;
        for (ui32 queryId = 1; queryId <= Tpcds.QueryCount; ++queryId) {
            selected.insert(queryId);
        }
        auto statuses = FormulaFloorStatuses(policy, Tpcds);
        ui32 improvementQuery = 0;
        for (ui32 queryId = 1; queryId <= Tpcds.QueryCount; ++queryId) {
            if (!policy.Suites.at(Tpcds.Name)
                     .RequiredFormulaQueries.contains(queryId))
            {
                improvementQuery = queryId;
                break;
            }
        }
        UNIT_ASSERT(improvementQuery);
        statuses[improvementQuery] = "FORMULA_EMITTED";
        const auto evaluation = EvaluateCoveragePolicy(
            policy,
            Tpcds,
            selected,
            statuses,
            OutcomeIds(statuses),
            ECoverageMode::FormulaDashboard);
        UNIT_ASSERT(evaluation.VerifierEntryFloorEnforced);
        UNIT_ASSERT(evaluation.FormulaFloorEnforced);
        UNIT_ASSERT(!evaluation.ProofFloorEnforced);
        UNIT_ASSERT(evaluation.Violations.empty());
        for (const ui32 queryId : {5, 65, 80}) {
            UNIT_ASSERT(evaluation.VerifierEntryQueries.contains(queryId));
        }
        UNIT_ASSERT(evaluation.FormulaEmittedQueries.contains(5));
        UNIT_ASSERT(evaluation.FormulaEmittedQueries.contains(65));
        UNIT_ASSERT(evaluation.FormulaEmittedQueries.contains(80));
        UNIT_ASSERT(
            evaluation.FormulaEmittedQueries ==
            OutcomeIds(statuses));
        UNIT_ASSERT(
            evaluation.FormulaEmittedQueries.contains(improvementQuery));

        const auto report = PolicyEvaluationJson(evaluation);
        UNIT_ASSERT(report["verifier_entry_floor_enforced"].GetBooleanSafe());
        UNIT_ASSERT_VALUES_EQUAL(
            report["required_verifier_entry_queries"].GetArraySafe().size(),
            3);
        size_t index = 0;
        for (const ui32 queryId :
             policy.Suites.at(Tpcds.Name).RequiredVerifierEntryQueries)
        {
            UNIT_ASSERT_VALUES_EQUAL(
                report["required_verifier_entry_queries"][index++].GetUIntegerSafe(),
                queryId);
        }
        UNIT_ASSERT_VALUES_EQUAL(
            report["verifier_entry_queries"].GetArraySafe().size(),
            statuses.size());
    }

    Y_UNIT_TEST(PolicyPinsTpchQ1AtFormulaConstruction) {
        const auto policy = LoadCoveragePolicy();
        std::set<ui32> selected;
        for (ui32 queryId = 1; queryId <= Tpch.QueryCount; ++queryId) {
            selected.insert(queryId);
        }
        const auto statuses = FormulaFloorStatuses(policy, Tpch);

        auto verifierEntries = OutcomeIds(statuses);
        const auto current = EvaluateCoveragePolicy(
            policy,
            Tpch,
            selected,
            statuses,
            verifierEntries,
            ECoverageMode::FormulaDashboard);
        UNIT_ASSERT(current.VerifierEntryFloorEnforced);
        UNIT_ASSERT(current.FormulaFloorEnforced);
        UNIT_ASSERT(current.Violations.empty());
        UNIT_ASSERT(current.VerifierEntryQueries.contains(1));
        UNIT_ASSERT(current.FormulaEmittedQueries.contains(1));

        auto regressedStatuses = statuses;
        regressedStatuses[1] = "UNSUPPORTED";
        const auto regressed = EvaluateCoveragePolicy(
            policy,
            Tpch,
            selected,
            regressedStatuses,
            verifierEntries,
            ECoverageMode::FormulaDashboard);
        UNIT_ASSERT_VALUES_EQUAL(regressed.Violations.size(), 1);
        UNIT_ASSERT(regressed.Violations.front().Contains(
            "q1 regressed from FORMULA_EMITTED to UNSUPPORTED"));
    }

    Y_UNIT_TEST(PolicyPinsTpchFormulaFloor) {
        const auto policy = LoadCoveragePolicy();
        std::set<ui32> selected;
        for (ui32 queryId = 1; queryId <= Tpch.QueryCount; ++queryId) {
            selected.insert(queryId);
        }
        const auto statuses = FormulaFloorStatuses(policy, Tpch);
        const auto current = EvaluateCoveragePolicy(
            policy,
            Tpch,
            selected,
            statuses,
            OutcomeIds(statuses),
            ECoverageMode::FormulaDashboard);
        UNIT_ASSERT(current.VerifierEntryFloorEnforced);
        UNIT_ASSERT(current.FormulaFloorEnforced);
        UNIT_ASSERT(current.Violations.empty());
        UNIT_ASSERT(
            current.FormulaEmittedQueries ==
            policy.Suites.at(Tpch.Name).RequiredFormulaQueries);

        for (const ui32 queryId :
             policy.Suites.at(Tpch.Name).RequiredFormulaQueries)
        {
            auto regressedStatuses = statuses;
            regressedStatuses[queryId] = "UNSUPPORTED";
            const auto regressed = EvaluateCoveragePolicy(
                policy,
                Tpch,
                selected,
                regressedStatuses,
                OutcomeIds(regressedStatuses),
                ECoverageMode::FormulaDashboard);
            UNIT_ASSERT_VALUES_EQUAL(regressed.Violations.size(), 1);
            const TString expected = TStringBuilder()
                << "q" << queryId
                << " regressed from FORMULA_EMITTED to UNSUPPORTED";
            UNIT_ASSERT(regressed.Violations.front().Contains(expected));
        }
    }

    Y_UNIT_TEST(PolicyVerifierEntryFloorAcceptsDeeperOutcomes) {
        const auto policy = LoadCoveragePolicy();
        std::set<ui32> selected;
        for (ui32 queryId = 1; queryId <= Tpcds.QueryCount; ++queryId) {
            selected.insert(queryId);
        }

        TMap<ui32, TString> statuses;
        for (const ui32 queryId :
             policy.Suites.at(Tpcds.Name).RequiredFormulaQueries)
        {
            statuses[queryId] = "FORMULA_EMITTED";
        }

        for (const TString status : {"FORMULA_EMITTED", "VERIFIED_BOUNDED"}) {
            statuses[65] = status;
            const auto evaluation = EvaluateCoveragePolicy(
                policy,
                Tpcds,
                selected,
                statuses,
                OutcomeIds(statuses),
                ECoverageMode::FormulaDashboard);
            UNIT_ASSERT(evaluation.VerifierEntryFloorEnforced);
            UNIT_ASSERT(evaluation.Violations.empty());
            for (const ui32 queryId :
                 policy.Suites.at(Tpcds.Name).RequiredVerifierEntryQueries)
            {
                UNIT_ASSERT(evaluation.VerifierEntryQueries.contains(queryId));
            }
            UNIT_ASSERT(evaluation.FormulaEmittedQueries.contains(5));
            UNIT_ASSERT(evaluation.FormulaEmittedQueries.contains(80));
            if (status == "FORMULA_EMITTED") {
                UNIT_ASSERT(evaluation.FormulaEmittedQueries.contains(65));
            } else {
                UNIT_ASSERT(evaluation.VerifiedBoundedQueries.contains(65));
            }
        }
    }

    Y_UNIT_TEST(PolicyReportsVerifierEntryRegressions) {
        const auto policy = LoadCoveragePolicy();
        std::set<ui32> selected;
        for (ui32 queryId = 1; queryId <= Tpcds.QueryCount; ++queryId) {
            selected.insert(queryId);
        }

        TMap<ui32, TString> statuses;
        for (const ui32 queryId :
             policy.Suites.at(Tpcds.Name).RequiredFormulaQueries)
        {
            statuses[queryId] = "FORMULA_EMITTED";
        }
        statuses[65] = "UNSUPPORTED";
        auto verifierEntries = OutcomeIds(statuses);
        verifierEntries.erase(65);

        const auto beforeVerifier = EvaluateCoveragePolicy(
            policy,
            Tpcds,
            selected,
            statuses,
            verifierEntries,
            ECoverageMode::FormulaDashboard);
        UNIT_ASSERT(beforeVerifier.VerifierEntryFloorEnforced);
        UNIT_ASSERT_VALUES_EQUAL(beforeVerifier.Violations.size(), 2);
        UNIT_ASSERT(beforeVerifier.Violations[0].Contains(
            "q65 regressed before verifier entry with status UNSUPPORTED"));
        UNIT_ASSERT(beforeVerifier.Violations[1].Contains(
            "q65 regressed from FORMULA_EMITTED to UNSUPPORTED"));

        statuses[65] = "OPTIMIZER_FAILURE";
        const auto optimizerFailure = EvaluateCoveragePolicy(
            policy,
            Tpcds,
            selected,
            statuses,
            verifierEntries,
            ECoverageMode::FormulaDashboard);
        UNIT_ASSERT_VALUES_EQUAL(optimizerFailure.Violations.size(), 2);
        UNIT_ASSERT(optimizerFailure.Violations[0].Contains(
            "q65 regressed before verifier entry with status OPTIMIZER_FAILURE"));
        UNIT_ASSERT(optimizerFailure.Violations[1].Contains(
            "q65 regressed from FORMULA_EMITTED to OPTIMIZER_FAILURE"));

        statuses.erase(65);
        const auto missing = EvaluateCoveragePolicy(
            policy,
            Tpcds,
            selected,
            statuses,
            verifierEntries,
            ECoverageMode::FormulaDashboard);
        UNIT_ASSERT_VALUES_EQUAL(missing.Violations.size(), 2);
        UNIT_ASSERT(missing.Violations[0].Contains(
            "q65 has no coverage outcome; expected verifier entry"));
        UNIT_ASSERT(missing.Violations[1].Contains(
            "q65 has no coverage outcome; expected FORMULA_EMITTED"));
    }

    Y_UNIT_TEST(PolicyReportsEveryFloorRegression) {
        const auto policy = LoadCoveragePolicy();
        std::set<ui32> selected;
        for (ui32 queryId = 1; queryId <= Tpcds.QueryCount; ++queryId) {
            selected.insert(queryId);
        }
        auto statuses = FormulaFloorStatuses(policy, Tpcds);
        statuses[88] = "UNSUPPORTED";
        statuses.erase(96);
        auto verifierEntries = OutcomeIds(statuses);
        verifierEntries.erase(88);
        const auto evaluation = EvaluateCoveragePolicy(
            policy,
            Tpcds,
            selected,
            statuses,
            verifierEntries,
            ECoverageMode::FormulaDashboard);
        UNIT_ASSERT(evaluation.VerifierEntryFloorEnforced);
        UNIT_ASSERT(evaluation.FormulaFloorEnforced);
        UNIT_ASSERT(!evaluation.ProofFloorEnforced);
        UNIT_ASSERT_VALUES_EQUAL(evaluation.Violations.size(), 2);
        UNIT_ASSERT(evaluation.Violations[0].Contains(
            "q88 regressed from FORMULA_EMITTED to UNSUPPORTED"));
        UNIT_ASSERT(evaluation.Violations[1].Contains(
            "q96 has no coverage outcome"));

        const auto report = PolicyEvaluationJson(evaluation);
        UNIT_ASSERT(report["formula_floor_enforced"].GetBooleanSafe());
        UNIT_ASSERT(!report["proof_floor_enforced"].GetBooleanSafe());
        UNIT_ASSERT_VALUES_EQUAL(
            report["violations"].GetArraySafe().size(),
            2);
    }

    Y_UNIT_TEST(PolicyEnforcesCuratedProofFloor) {
        const auto policy = LoadCoveragePolicy();
        const std::set<ui32> selected = {
            3, 38, 42, 48, 52, 55, 69, 87, 90, 93, 95, 96};
        const TMap<ui32, TString> statuses = {
            {3, "VERIFIED_BOUNDED"},
            {38, "VERIFIED_BOUNDED"},
            {42, "VERIFIED_BOUNDED"},
            {48, "VERIFIED_BOUNDED"},
            {52, "VERIFIED_BOUNDED"},
            {55, "VERIFIED_BOUNDED"},
            {69, "VERIFIED_BOUNDED"},
            {87, "VERIFIED_BOUNDED"},
            {90, "VERIFIED_BOUNDED"},
            {93, "VERIFIED_BOUNDED"},
            {95, "VERIFIED_BOUNDED"},
            {96, "VERIFIED_BOUNDED"},
        };
        const auto evaluation = EvaluateCoveragePolicy(
            policy,
            Tpcds,
            selected,
            statuses,
            {},
            ECoverageMode::ProofFloor);
        UNIT_ASSERT(!evaluation.VerifierEntryFloorEnforced);
        UNIT_ASSERT(!evaluation.FormulaFloorEnforced);
        UNIT_ASSERT(evaluation.ProofFloorEnforced);
        UNIT_ASSERT(evaluation.Violations.empty());
        UNIT_ASSERT(
            evaluation.VerifiedBoundedQueries == selected);

        const auto report = PolicyEvaluationJson(evaluation);
        UNIT_ASSERT_VALUES_EQUAL(
            report["format"].GetStringSafe(),
            CoveragePolicyEvaluationFormat);
        UNIT_ASSERT_VALUES_EQUAL(
            report["version"].GetUIntegerSafe(),
            CoveragePolicyEvaluationVersion);
        UNIT_ASSERT_VALUES_EQUAL(
            report["mode"].GetStringSafe(),
            "proof_floor");
        UNIT_ASSERT(report["proof_floor_enforced"].GetBooleanSafe());
        UNIT_ASSERT_VALUES_EQUAL(
            report["verified_bounded_queries"].GetArraySafe().size(),
            selected.size());
    }

    Y_UNIT_TEST(PolicyReportsEveryProofFloorRegression) {
        const auto policy = LoadCoveragePolicy();
        const TMap<ui32, TString> statuses = {
            {3, "VERIFIED_BOUNDED"},
            {38, "VERIFIED_BOUNDED"},
            {42, "VERIFIED_BOUNDED"},
            {48, "VERIFIED_BOUNDED"},
            {52, "UNKNOWN"},
            {55, "FORMULA_EMITTED"},
            {69, "VERIFIED_BOUNDED"},
            {87, "VERIFIED_BOUNDED"},
            {90, "VERIFIED_BOUNDED"},
            {93, "UNSUPPORTED"},
            {95, "VERIFIED_BOUNDED"},
        };
        const auto evaluation = EvaluateCoveragePolicy(
            policy,
            Tpcds,
            {3, 38, 42, 48, 52, 55, 69, 87, 90, 93, 95, 96},
            statuses,
            {},
            ECoverageMode::ProofFloor);
        UNIT_ASSERT(evaluation.ProofFloorEnforced);
        UNIT_ASSERT_VALUES_EQUAL(evaluation.Violations.size(), 4);
        UNIT_ASSERT(evaluation.Violations[0].Contains(
            "q52 regressed from VERIFIED_BOUNDED to UNKNOWN"));
        UNIT_ASSERT(evaluation.Violations[1].Contains(
            "q55 regressed from VERIFIED_BOUNDED to FORMULA_EMITTED"));
        UNIT_ASSERT(evaluation.Violations[2].Contains(
            "q93 regressed from VERIFIED_BOUNDED to UNSUPPORTED"));
        UNIT_ASSERT(evaluation.Violations[3].Contains(
            "q96 has no proof outcome"));

        auto optimizerFailureStatuses = statuses;
        optimizerFailureStatuses[96] = "OPTIMIZER_FAILURE";
        const auto optimizerFailure = EvaluateCoveragePolicy(
            policy,
            Tpcds,
            {3, 38, 42, 48, 52, 55, 69, 87, 90, 93, 95, 96},
            optimizerFailureStatuses,
            {},
            ECoverageMode::ProofFloor);
        UNIT_ASSERT(optimizerFailure.Violations.back().Contains(
            "q96 regressed from VERIFIED_BOUNDED to OPTIMIZER_FAILURE"));

        const auto wrongSelection = EvaluateCoveragePolicy(
            policy,
            Tpcds,
            {3, 42, 48, 52, 55, 69, 90, 93},
            statuses,
            {},
            ECoverageMode::ProofFloor);
        UNIT_ASSERT(wrongSelection.Violations.front().Contains(
            "did not select exactly"));
    }

    Y_UNIT_TEST(PolicyDoesNotGateFocusedOrSolverExperiments) {
        const auto policy = LoadCoveragePolicy();
        const TMap<ui32, TString> statuses;
        const auto focused = EvaluateCoveragePolicy(
            policy,
            Tpcds,
            {3, 42, 48, 50, 52, 55, 61, 71, 76, 88, 90, 93, 96},
            statuses,
            {},
            ECoverageMode::FormulaDashboard);
        UNIT_ASSERT(!focused.VerifierEntryFloorEnforced);
        UNIT_ASSERT(!focused.FormulaFloorEnforced);
        UNIT_ASSERT(!focused.ProofFloorEnforced);
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
            {},
            ECoverageMode::SolverExperiment);
        UNIT_ASSERT(!solver.VerifierEntryFloorEnforced);
        UNIT_ASSERT(!solver.FormulaFloorEnforced);
        UNIT_ASSERT(!solver.ProofFloorEnforced);
        UNIT_ASSERT(solver.Violations.empty());

        const auto coincidentalProofSelection = EvaluateCoveragePolicy(
            policy,
            Tpcds,
            {3, 38, 42, 48, 52, 55, 69, 87, 90, 93, 95, 96},
            statuses,
            {},
            ECoverageMode::SolverExperiment);
        UNIT_ASSERT(!coincidentalProofSelection.ProofFloorEnforced);
        UNIT_ASSERT(coincidentalProofSelection.Violations.empty());
    }

    Y_UNIT_TEST(PolicySolverUsesHermeticBinaryAfterExplicitOptIn) {
        {
            NTesting::TScopedEnvironment environment{{
                {"RBO_COVERAGE_USE_SOLVER", ""},
                {"RBO_Z3", "/ambient/z3"},
            }};
            UNIT_ASSERT(!CoverageSolver());
        }
        {
            NTesting::TScopedEnvironment environment{{
                {"RBO_COVERAGE_USE_SOLVER", "2"},
            }};
            UNIT_ASSERT_EXCEPTION_CONTAINS(
                CoverageSolver(),
                yexception,
                "must be 0 or 1");
        }
        {
            NTesting::TScopedEnvironment environment{{
                {"RBO_COVERAGE_USE_SOLVER", "1"},
                {"RBO_Z3", "/ambient/z3"},
            }};
            const auto solver = CoverageSolver();
            UNIT_ASSERT(solver);
            UNIT_ASSERT_STRING_CONTAINS(*solver, "contrib/tools/z3/z3");
            UNIT_ASSERT_VALUES_UNEQUAL(*solver, "/ambient/z3");
        }
    }

    Y_UNIT_TEST(ProofFloorConfigurationIsHermetic) {
        const auto policy = LoadCoveragePolicy();
        NTesting::TScopedEnvironment environment{{
            {"RBO_COVERAGE_USE_SOLVER", "2"},
            {"RBO_COVERAGE_QUERIES", "999"},
            {"RBO_COVERAGE_TIMEOUT_MS", "0"},
            {"RBO_Z3", "/ambient/z3"},
        }};
        const auto config = ResolveCoverageRun(
            policy,
            Tpcds,
            ECoverageRun::ProofFloor);
        UNIT_ASSERT(config.Mode == ECoverageMode::ProofFloor);
        UNIT_ASSERT(
            config.Selected ==
            std::set<ui32>({
                3, 38, 42, 48, 52, 55, 69, 87, 90, 93, 95, 96,
            }));
        UNIT_ASSERT(config.Solver);
        UNIT_ASSERT_STRING_CONTAINS(
            *config.Solver,
            "contrib/tools/z3/z3");
        UNIT_ASSERT_VALUES_UNEQUAL(*config.Solver, "/ambient/z3");
        UNIT_ASSERT_VALUES_EQUAL(config.TimeoutMs, ProofFloorTimeoutMs);
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
        const auto baseline = encoded;

        encoded["version"] = 2;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            DecodeCoveragePolicy(NJson::WriteJson(encoded, false, true)),
            yexception,
            "unsupported version");

        encoded = baseline;
        encoded["suites"][Tpcds.Name].EraseValue(
            "required_verifier_entry_queries");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            DecodeCoveragePolicy(NJson::WriteJson(encoded, false, true)),
            yexception,
            "is missing field required_verifier_entry_queries");

        encoded = baseline;
        encoded["suites"][Tpcds.Name]["required_verifier_entry_queries"] = true;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            DecodeCoveragePolicy(NJson::WriteJson(encoded, false, true)),
            yexception,
            "required_verifier_entry_queries must be an array");

        encoded = baseline;
        NJson::TJsonValue duplicateVerifierEntry(NJson::JSON_ARRAY);
        duplicateVerifierEntry.AppendValue(65);
        duplicateVerifierEntry.AppendValue(65);
        encoded["suites"][Tpcds.Name]["required_verifier_entry_queries"] =
            std::move(duplicateVerifierEntry);
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            DecodeCoveragePolicy(NJson::WriteJson(encoded, false, true)),
            yexception,
            "required_verifier_entry_queries query ids must be strictly increasing");

        encoded = baseline;
        NJson::TJsonValue outsideVerifierEntry(NJson::JSON_ARRAY);
        outsideVerifierEntry.AppendValue(100);
        encoded["suites"][Tpcds.Name]["required_verifier_entry_queries"] =
            std::move(outsideVerifierEntry);
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            DecodeCoveragePolicy(NJson::WriteJson(encoded, false, true)),
            yexception,
            "required_verifier_entry_queries query id 100 is outside the corpus");

        encoded = baseline;
        encoded["suites"][Tpcds.Name].EraseValue(
            "required_verified_queries");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            DecodeCoveragePolicy(NJson::WriteJson(encoded, false, true)),
            yexception,
            "is missing field required_verified_queries");

        encoded = baseline;
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

        encoded = baseline;
        NJson::TJsonValue duplicate(NJson::JSON_ARRAY);
        duplicate.AppendValue(3);
        duplicate.AppendValue(52);
        duplicate.AppendValue(52);
        encoded["suites"][Tpcds.Name]["required_verified_queries"] =
            std::move(duplicate);
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            DecodeCoveragePolicy(NJson::WriteJson(encoded, false, true)),
            yexception,
            "strictly increasing");

        encoded = baseline;
        NJson::TJsonValue outside(NJson::JSON_ARRAY);
        outside.AppendValue(100);
        encoded["suites"][Tpcds.Name]["required_verified_queries"] =
            std::move(outside);
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            DecodeCoveragePolicy(NJson::WriteJson(encoded, false, true)),
            yexception,
            "outside the corpus");

        encoded = baseline;
        NJson::TJsonValue notFormula(NJson::JSON_ARRAY);
        notFormula.AppendValue(3);
        notFormula.AppendValue(48);
        encoded["suites"][Tpcds.Name]["required_verified_queries"] =
            std::move(notFormula);
        encoded["suites"][Tpcds.Name]["required_formula_queries"] =
            NJson::TJsonValue(NJson::JSON_ARRAY);
        encoded["suites"][Tpcds.Name]["required_formula_queries"]
            .AppendValue(3);
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            DecodeCoveragePolicy(NJson::WriteJson(encoded, false, true)),
            yexception,
            "is not a required formula query");

        encoded = baseline;
        encoded["suites"][Tpcds.Name]["required_verified_queries"] =
            NJson::TJsonValue(NJson::JSON_ARRAY);
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            DecodeCoveragePolicy(NJson::WriteJson(encoded, false, true)),
            yexception,
            "must not be empty");
    }

    Y_UNIT_TEST(TPCH) {
        RunCoverage(Tpch, ECoverageRun::Environment);
    }

    Y_UNIT_TEST(TPCDS) {
        RunCoverage(Tpcds, ECoverageRun::Environment);
    }

    Y_UNIT_TEST(ProofFloorTpchCorpus) {
        RunCoverage(Tpch, ECoverageRun::ProofFloor);
    }

    Y_UNIT_TEST(ProofFloorTpcdsCorpus) {
        RunCoverage(Tpcds, ECoverageRun::ProofFloor);
    }
}

} // namespace NKikimr::NKqp
