#include <ydb/core/kqp/opt/rbo/verification/benchmark_policy/coverage_policy.h>

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
#include <library/cpp/json/json_writer.h>
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
#include <mutex>
#include <regex>
#include <set>
#include <utility>

namespace NKikimr::NKqp {
namespace {

using namespace NVerificationCoverage;

constexpr const char* TestCluster = "local_ut";
constexpr ui64 DefaultTimeoutMs = 10'000;
constexpr ui64 ProofFloorTimeoutMs = 60'000;
constexpr const char* CoverageReportFormat =
    "ydb-rbo-benchmark-coverage";
constexpr ui64 CoverageReportVersion = 5;

enum class ECoverageRun {
    Environment,
    ProofFloor,
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

TCoveragePolicy LoadCoveragePolicy() {
    return DecodeCoveragePolicy(
        TFileInput(CoveragePolicyPath()).ReadAll());
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

bool IsExactSnapshotPair(
    const TVector<TRBOSemanticSnapshotBoundaryResultV1>& captures)
{
    return captures.size() == 2 &&
        captures[0].Boundary == ERBOSemanticSnapshotBoundaryV1::Initial &&
        captures[1].Boundary == ERBOSemanticSnapshotBoundaryV1::Final;
}

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
    TString PrepareStatus = "NOT_RUN";
    TString PrepareReason;
    TVector<std::pair<TString, TString>> UnsupportedReasons;
    bool SnapshotPairCaptured = false;
    bool Fatal = false;
};

void SetPreparationOutcome(
    TOutcome& outcome,
    bool succeeded,
    TString reason = {})
{
    outcome.PrepareStatus = succeeded ? "SUCCEEDED" : "FAILED";
    outcome.PrepareReason = succeeded ? TString() : std::move(reason);
    outcome.Json["prepare_status"] = outcome.PrepareStatus;
    outcome.Json["prepare_reason"] = outcome.PrepareReason;
}

void SetUnknownPreparationOutcome(
    TOutcome& outcome,
    TString reason)
{
    outcome.PrepareStatus = "UNKNOWN";
    outcome.PrepareReason = reason.empty()
        ? "preparation outcome is unavailable"
        : std::move(reason);
    outcome.Json["prepare_status"] = outcome.PrepareStatus;
    outcome.Json["prepare_reason"] = outcome.PrepareReason;
}

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
    outcome.Json["prepare_status"] = outcome.PrepareStatus;
    outcome.Json["prepare_reason"] = outcome.PrepareReason;
    return outcome;
}

void PreserveTextArtifact(
    NJson::TJsonValue& artifacts,
    TStringBuf key,
    const TString& name,
    TStringBuf content)
{
    unsigned char digest[SHA256_DIGEST_LENGTH];
    if (!SHA256(
            reinterpret_cast<const unsigned char*>(content.data()),
            content.size(),
            digest))
    {
        ythrow yexception() << "cannot hash diagnostic artifact " << name;
    }
    TFileOutput((GetOutputPath() / name).GetPath()).Write(content);
    artifacts[TString(key)] = name;
    artifacts[TStringBuilder() << key << "_sha256"] =
        to_lower(HexEncode(digest, sizeof(digest)));
}

NJson::TJsonValue PreserveCaptureArtifacts(
    TStringBuf suiteSlug,
    ui32 queryId,
    TStringBuf query,
    const TRBOSemanticSnapshotBoundaryResultV1& initial,
    const TRBOSemanticSnapshotBoundaryResultV1& final)
{
    const TString stem = TStringBuilder()
        << suiteSlug << "_q" << queryId;
    NJson::TJsonValue artifacts(NJson::JSON_MAP);

    const auto preserveCapture = [&](
        TStringBuf side,
        const TRBOSemanticSnapshotBoundaryResultV1& capture)
    {
        if (capture.IsSupported()) {
            PreserveTextArtifact(
                artifacts,
                TStringBuilder() << side << "_snapshot",
                TStringBuilder() << stem << "." << side << ".json",
                capture.Json);
        } else {
            PreserveTextArtifact(
                artifacts,
                TStringBuilder() << side << "_unsupported",
                TStringBuilder() << stem << "." << side << ".unsupported.txt",
                capture.UnsupportedReason);
        }
    };

    preserveCapture("initial", initial);
    preserveCapture("final", final);
    PreserveTextArtifact(
        artifacts,
        "query",
        stem + ".query.yql",
        query);
    return artifacts;
}

NJson::TJsonValue PreserveArtifacts(
    TStringBuf suiteSlug,
    ui32 queryId,
    TStringBuf query,
    TStringBuf verifierVerdict,
    const TRBOSemanticSnapshotBoundaryResultV1& initial,
    const TRBOSemanticSnapshotBoundaryResultV1& final,
    const NJson::TJsonValue& processArtifacts)
{
    const TString stem = TStringBuilder()
        << suiteSlug << "_q" << queryId;
    NJson::TJsonValue artifacts = PreserveCaptureArtifacts(
        suiteSlug,
        queryId,
        query,
        initial,
        final);
    PreserveTextArtifact(
        artifacts,
        "verifier_verdict",
        stem + ".verdict.json",
        verifierVerdict);

    if (processArtifacts.Has("formula")) {
        // Both manifests refer to the one already copied and SHA-bound file.
        // Keep the legacy candidate map shape consumed by confirmation.
        artifacts["formula"] = processArtifacts["formula"];
    }
    return artifacts;
}

// Capture process output before interpreting any verdict. The raw streams are
// evidence even when neither stream is valid JSON or the exit/status disagree.
struct TVerifierProcess {
    TVector<TString> Arguments;
    TMaybe<int> ExitCode;
    TString Stdout;
    TString Stderr;
    TString Error;
    ui64 ElapsedMs = 0;
};

void PreserveFormulaArtifact(
    NJson::TJsonValue& artifacts,
    const TString& name,
    const TFsPath& source)
{
    if (!source.Exists()) {
        return;
    }
    SHA256_CTX hash;
    if (!SHA256_Init(&hash)) {
        ythrow yexception() << "cannot initialize formula artifact digest";
    }
    TFileInput input(source.GetPath());
    TFileOutput output((GetOutputPath() / name).GetPath());
    char buffer[64 * 1024];
    while (const size_t count = input.Read(buffer, sizeof(buffer))) {
        output.Write(buffer, count);
        if (!SHA256_Update(&hash, buffer, count)) {
            ythrow yexception() << "cannot hash formula artifact";
        }
    }
    unsigned char digest[SHA256_DIGEST_LENGTH];
    if (!SHA256_Final(digest, &hash)) {
        ythrow yexception() << "cannot finalize formula artifact digest";
    }
    artifacts["formula"] = name;
    artifacts["formula_sha256"] = to_lower(HexEncode(digest, sizeof(digest)));
}

NJson::TJsonValue PreserveVerifierProcess(
    TStringBuf suiteSlug,
    ui32 queryId,
    const TVerifierProcess& process,
    const TFsPath& formulaPath)
{
    const TString stem = TStringBuilder() << suiteSlug << "_q" << queryId;
    NJson::TJsonValue artifacts(NJson::JSON_MAP);
    PreserveTextArtifact(
        artifacts, "stdout", stem + ".verifier.stdout", process.Stdout);
    PreserveTextArtifact(
        artifacts, "stderr", stem + ".verifier.stderr", process.Stderr);
    NJson::TJsonValue command(NJson::JSON_MAP);
    command["arguments"] = NJson::TJsonValue(NJson::JSON_ARRAY);
    for (const auto& argument : process.Arguments) {
        command["arguments"].AppendValue(argument);
    }
    command["exit_code"] = process.ExitCode
        ? NJson::TJsonValue(*process.ExitCode)
        : NJson::TJsonValue(NJson::JSON_NULL);
    command["elapsed_ms"] = process.ElapsedMs;
    command["error"] = process.Error;
    PreserveTextArtifact(
        artifacts,
        "command",
        stem + ".verifier.command.json",
        NJson::WriteJson(command, true, true));
    PreserveFormulaArtifact(artifacts, stem + ".smt2", formulaPath);
    return artifacts;
}

TOutcome ClassifyVerifierProcess(
    TStringBuf suiteSlug,
    ui32 queryId,
    TStringBuf query,
    ui64 prepareMs,
    const TRBOSemanticSnapshotBoundaryResultV1& initial,
    const TRBOSemanticSnapshotBoundaryResultV1& final,
    const TVerifierProcess& process,
    const TFsPath& formulaPath,
    bool preserveArtifacts)
{
    const auto harnessError = [&](TString reason) {
        auto outcome = HarnessError(queryId, prepareMs, 2, reason);
        // A protocol failure is itself a diagnostic outcome, even when query
        // preparation succeeded. Never drop evidence merely because decoding
        // did not produce one of the normal verifier statuses.
        try {
            outcome.Json["process_artifacts"] = PreserveVerifierProcess(
                suiteSlug, queryId, process, formulaPath);
            outcome.Json["artifacts"] = PreserveCaptureArtifacts(
                suiteSlug, queryId, query, initial, final);
        } catch (const std::exception& error) {
            outcome.Json["artifact_error"] = error.what();
        }
        return outcome;
    };
    if (!process.Error.empty()) {
        return harnessError(process.Error);
    }

    NJson::TJsonValue stdoutVerdict;
    NJson::TJsonValue stderrVerdict;
    const bool stdoutJson = ParseJson(process.Stdout, stdoutVerdict);
    const bool stderrJson = ParseJson(process.Stderr, stderrVerdict);
    if (!stdoutJson && !stderrJson) {
        return harnessError(TStringBuilder()
            << "verifier returned no JSON; exit=" << process.ExitCode.GetOrElse(-1));
    }
    if (stdoutJson && stderrJson) {
        return harnessError("verifier returned JSON on both stdout and stderr");
    }
    NJson::TJsonValue verdict = stdoutJson
        ? std::move(stdoutVerdict)
        : std::move(stderrVerdict);
    const TString& verifierVerdict = stdoutJson ? process.Stdout : process.Stderr;
    if (!verdict.IsMap() || !verdict.Has("status") ||
        !verdict["status"].IsString())
    {
        return harnessError("verifier JSON has no string status");
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
    if (!Statuses.contains(status) || !process.ExitCode.Defined() ||
        process.ExitCode.GetRef() != ExpectedExit(status))
    {
        return harnessError(TStringBuilder()
            << "verifier protocol mismatch: status=" << status
            << ", exit=" << process.ExitCode.GetOrElse(-1));
    }

    TOutcome outcome;
    outcome.Status = status;
    outcome.Layer = "verifier";
    if (verdict.Has("reason") && verdict["reason"].IsString()) {
        outcome.Reason = verdict["reason"].GetStringSafe();
    }
    if (status == "UNSUPPORTED") {
        outcome.UnsupportedReasons.emplace_back(outcome.Layer, outcome.Reason);
    }
    outcome.Fatal = status == "COUNTEREXAMPLE" ||
        status == "SCHEMA_MISMATCH" || status == "SOLVER_ERROR";
    outcome.Json["query_id"] = queryId;
    outcome.Json["status"] = status;
    outcome.Json["layer"] = outcome.Layer;
    outcome.Json["reason"] = outcome.Reason;
    outcome.Json["prepare_ms"] = prepareMs;
    outcome.Json["verify_ms"] = process.ElapsedMs;
    outcome.Json["capture_count"] = 2;
    outcome.Json["verdict"] = VerdictForCoverageReport(std::move(verdict), status);
    if (preserveArtifacts || status == "COUNTEREXAMPLE" || status == "UNKNOWN" ||
        status == "SCHEMA_MISMATCH" || status == "SOLVER_ERROR")
    {
        try {
            outcome.Json["process_artifacts"] = PreserveVerifierProcess(
                suiteSlug, queryId, process, formulaPath);
            outcome.Json["artifacts"] = PreserveArtifacts(
                suiteSlug,
                queryId,
                query,
                verifierVerdict,
                initial,
                final,
                outcome.Json["process_artifacts"]);
        } catch (const std::exception& error) {
            outcome.Json["artifact_error"] = error.what();
            outcome.Fatal = true;
        }
    }
    return outcome;
}

TOutcome RunVerifier(
    TStringBuf suiteSlug,
    ui32 queryId,
    TStringBuf query,
    ui64 prepareMs,
    const TRBOSemanticSnapshotBoundaryResultV1& initial,
    const TRBOSemanticSnapshotBoundaryResultV1& final,
    ui64 timeoutMs,
    const TMaybe<TString>& solver,
    bool preserveArtifacts)
{
    TTempDir tempDir;
    const auto initialPath = tempDir.Path() / "initial.json";
    const auto finalPath = tempDir.Path() / "final.json";
    const auto formulaPath = tempDir.Path() / "problem.smt2";
    TFileOutput(initialPath.GetPath()).Write(initial.Json);
    TFileOutput(finalPath.GetPath()).Write(final.Json);
    TVerifierProcess process;
    process.Arguments = {
        BinaryPath("ydb/core/kqp/opt/rbo/verification/bin/kqp_rbo_verify"),
        initialPath.GetPath(),
        finalPath.GetPath(),
        "--rows", ToString(RowBound),
        "--timeout-ms", ToString(timeoutMs),
        "--emit-smt", formulaPath.GetPath(),
    };
    if (solver) {
        process.Arguments.push_back("--solver");
        process.Arguments.push_back(*solver);
    }
    TShellCommand command(process.Arguments.front());
    for (size_t index = 1; index < process.Arguments.size(); ++index) {
        command << process.Arguments[index];
    }
    const TInstant started = TInstant::Now();
    try {
        command.Run();
    } catch (const std::exception& error) {
        process.Error = TStringBuilder() << "cannot run verifier: " << error.what();
    } catch (...) {
        process.Error = "cannot run verifier: non-standard exception";
    }
    process.ElapsedMs = (TInstant::Now() - started).MilliSeconds();
    process.ExitCode = command.GetExitCode();
    process.Stdout = command.GetOutput();
    process.Stderr = command.GetError();
    return ClassifyVerifierProcess(
        suiteSlug, queryId, query, prepareMs, initial, final,
        process, formulaPath, preserveArtifacts);
}

TOutcome OptimizerFailure(
    ui32 queryId,
    ui64 prepareMs,
    size_t captureCount,
    TString reason)
{
    TOutcome outcome;
    outcome.Status = "OPTIMIZER_FAILURE";
    outcome.Layer = "optimizer";
    outcome.Reason = std::move(reason);
    outcome.Json["query_id"] = queryId;
    outcome.Json["status"] = outcome.Status;
    outcome.Json["layer"] = outcome.Layer;
    outcome.Json["reason"] = outcome.Reason;
    outcome.Json["prepare_ms"] = prepareMs;
    outcome.Json["verify_ms"] = 0;
    outcome.Json["capture_count"] = captureCount;
    return outcome;
}

TOutcome ClassifyCapturedPair(
    const TSuite& suite,
    ui32 queryId,
    TStringBuf query,
    ui64 prepareMs,
    const TRBOSemanticSnapshotBoundaryResultV1& initial,
    const TRBOSemanticSnapshotBoundaryResultV1& final,
    ui64 timeoutMs,
    const TMaybe<TString>& solver,
    bool preserveArtifacts)
{
    if (initial.Boundary != ERBOSemanticSnapshotBoundaryV1::Initial ||
        final.Boundary != ERBOSemanticSnapshotBoundaryV1::Final)
    {
        return HarnessError(
            queryId,
            prepareMs,
            2,
            "snapshot callback count or order is invalid");
    }

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
        outcome.Json["capture_count"] = 2;
        if (preserveArtifacts) {
            try {
                outcome.Json["artifacts"] = PreserveCaptureArtifacts(
                    suite.Slug,
                    queryId,
                    query,
                    initial,
                    final);
            } catch (const std::exception& error) {
                outcome.Json["artifact_error"] = error.what();
                outcome.Fatal = true;
            }
        }
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
        solver,
        preserveArtifacts);
}

TOutcome ClassifyQuery(
    TKikimrRunner& kikimr,
    const NYql::IModuleResolver::TPtr& moduleResolver,
    const TSuite& suite,
    ui32 queryId,
    ui64 timeoutMs,
    const TMaybe<TString>& solver,
    bool preserveArtifacts)
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
    const bool snapshotPairCaptured = IsExactSnapshotPair(captures);
    const bool prepareSucceeded = prepared.Success();
    TString prepareReason = prepareSucceeded
        ? TString()
        : prepared.Issues().ToString();
    if (!prepareSucceeded && prepareReason.empty()) {
        prepareReason = "query preparation failed without diagnostic issues";
    }

    TOutcome outcome;
    const auto preserveExceptionalPair = [&] {
        if (prepareSucceeded || captures.size() != 2 ||
            captures[0].Boundary != ERBOSemanticSnapshotBoundaryV1::Initial ||
            captures[1].Boundary != ERBOSemanticSnapshotBoundaryV1::Final)
        {
            return;
        }
        try {
            outcome.Json["artifacts"] = PreserveCaptureArtifacts(
                suite.Slug,
                queryId,
                query,
                captures[0],
                captures[1]);
        } catch (const std::exception& artifactError) {
            outcome.Json["artifact_error"] = artifactError.what();
        }
    };
    try {
        if (captures.size() == 2) {
            outcome = ClassifyCapturedPair(
                suite,
                queryId,
                query,
                prepareMs,
                captures[0],
                captures[1],
                timeoutMs,
                solver,
                !prepareSucceeded || preserveArtifacts);
        } else if (prepareSucceeded) {
            outcome = HarnessError(
                queryId,
                prepareMs,
                captures.size(),
                "snapshot callback count is invalid");
        } else {
            outcome = OptimizerFailure(
                queryId,
                prepareMs,
                captures.size(),
                prepareReason);
        }
    } catch (const std::exception& error) {
        outcome = HarnessError(
            queryId,
            prepareMs,
            captures.size(),
            TStringBuilder()
                << "captured-pair classification threw: " << error.what());
        preserveExceptionalPair();
    } catch (...) {
        outcome = HarnessError(
            queryId,
            prepareMs,
            captures.size(),
            "captured-pair classification threw a non-standard exception");
        preserveExceptionalPair();
    }
    SetPreparationOutcome(outcome, prepareSucceeded, prepareReason);
    outcome.SnapshotPairCaptured = snapshotPairCaptured;
    return outcome;
}

void RunCoverage(const TSuite& suite, ECoverageRun run) {
    ECoverageMode mode = run == ECoverageRun::ProofFloor
        ? ECoverageMode::ProofFloor
        : ECoverageMode::FormulaDashboard;
    TMaybe<TString> solver;
    TMaybe<TCoveragePolicy> policy;
    std::set<ui32> selected;
    TMap<ui32, TString> statuses;
    std::set<ui32> prepareSuccessQueries;
    std::set<ui32> snapshotPairQueries;
    std::set<ui32> verifierEntryQueries;
    TVector<TString> policyLoadViolations;
    ui64 timeoutMs = DefaultTimeoutMs;
    bool timeoutResolved = false;
    NJson::TJsonValue rows(NJson::JSON_ARRAY);
    TMap<TString, ui32> summary;
    TMap<TString, ui32> prepareSummary;
    TMap<std::pair<TString, TString>, TVector<ui32>> unsupported;
    TMap<TString, TVector<ui32>> optimizerFailures;
    bool fatal = false;

    const auto record = [&](ui32 queryId, TString source, TOutcome outcome) {
        ++summary[outcome.Status];
        ++prepareSummary[outcome.PrepareStatus];
        if (outcome.Status == "UNSUPPORTED") {
            for (const auto& reason : outcome.UnsupportedReasons) {
                unsupported[reason].push_back(queryId);
            }
        }
        if (outcome.PrepareStatus == "FAILED") {
            optimizerFailures[outcome.PrepareReason].push_back(queryId);
        }
        if (queryId >= 1 && queryId <= suite.QueryCount) {
            statuses[queryId] = outcome.Status;
            if (outcome.PrepareStatus == "SUCCEEDED") {
                prepareSuccessQueries.insert(queryId);
            }
            if (outcome.SnapshotPairCaptured) {
                snapshotPairQueries.insert(queryId);
            }
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
                    solver,
                    mode == ECoverageMode::ProofFloor);
            } catch (const std::exception& error) {
                outcome = HarnessError(
                    queryId,
                    0,
                    0,
                    TStringBuilder() << "classification threw: " << error.what());
                SetUnknownPreparationOutcome(
                    outcome,
                    "classification failed before a preparation outcome was recorded");
            } catch (...) {
                outcome = HarnessError(
                    queryId, 0, 0, "classification threw a non-standard exception");
                SetUnknownPreparationOutcome(
                    outcome,
                    "classification failed before a preparation outcome was recorded");
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
        policyEvaluation.PrepareSuccessFloorEnforced =
            mode == ECoverageMode::FormulaDashboard ||
            mode == ECoverageMode::ProofFloor;
        policyEvaluation.SnapshotPairFloorEnforced =
            mode == ECoverageMode::FormulaDashboard;
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
                snapshotPairQueries,
                verifierEntryQueries,
                prepareSuccessQueries,
                mode);
        } catch (const std::exception& error) {
            policyEvaluation.Valid = false;
            policyEvaluation.PrepareSuccessFloorEnforced =
                mode == ECoverageMode::FormulaDashboard ||
                mode == ECoverageMode::ProofFloor;
            policyEvaluation.SnapshotPairFloorEnforced =
                mode == ECoverageMode::FormulaDashboard;
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
            policyEvaluation.PrepareSuccessFloorEnforced =
                mode == ECoverageMode::FormulaDashboard ||
                mode == ECoverageMode::ProofFloor;
            policyEvaluation.SnapshotPairFloorEnforced =
                mode == ECoverageMode::FormulaDashboard;
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
    NJson::TJsonValue prepareSummaryJson(NJson::JSON_MAP);
    for (const auto& [status, count] : prepareSummary) {
        prepareSummaryJson[status] = count;
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
    report["prepare_summary"] = std::move(prepareSummaryJson);
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
         << "Preparation summary: "
         << NJson::WriteJson(report["prepare_summary"], false, true) << Endl
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
            policy.Suites.at(Tpch.Name).RequiredPrepareSuccessQueries ==
            std::set<ui32>({
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18, 19,
                21, 22,
            }));
        UNIT_ASSERT(
            policy.Suites.at(Tpch.Name).RequiredSnapshotPairQueries.empty());
        UNIT_ASSERT(
            policy.Suites.at(Tpch.Name).RequiredVerifierEntryQueries ==
            std::set<ui32>({1, 13, 16}));
        UNIT_ASSERT(
            policy.Suites.at(Tpch.Name).RequiredFormulaQueries ==
            std::set<ui32>({
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18, 19,
                21, 22,
            }));
        UNIT_ASSERT(
            policy.Suites.at(Tpch.Name).RequiredVerifiedQueries ==
            std::set<ui32>({
                3, 4, 6, 7, 11, 12, 13, 14, 15, 16, 18, 19, 21, 22,
            }));
        UNIT_ASSERT_VALUES_EQUAL(
            SnapshotPairFloorQueries(policy.Suites.at(Tpch.Name)).size(),
            20);
        UNIT_ASSERT(
            policy.Suites.at(Tpcds.Name).RequiredPrepareSuccessQueries ==
            std::set<ui32>({
                2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15, 16, 18, 19, 21, 22, 24,
                25, 26, 28, 29, 31,
                33, 34, 35, 37, 38, 40, 41, 42, 43, 45, 46, 48, 50, 52, 54, 55,
                56,
                58, 59,
                60, 61, 62, 64, 65, 66, 68, 69, 71, 72, 73, 74, 75, 76, 77, 78,
                79, 80, 82, 83, 84, 85, 87, 88, 90, 91, 93, 94, 95, 96, 97, 99,
            }));
        UNIT_ASSERT(
            policy.Suites.at(Tpcds.Name).RequiredSnapshotPairQueries.empty());
        UNIT_ASSERT(
            policy.Suites.at(Tpcds.Name).RequiredVerifierEntryQueries ==
            std::set<ui32>({5, 8, 9, 59, 65, 72, 78, 80}));
        UNIT_ASSERT(
            policy.Suites.at(Tpcds.Name).RequiredFormulaQueries ==
            std::set<ui32>({
                2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16, 18, 19, 20, 21,
                22, 24, 25, 26, 28, 29, 31, 33, 34, 35, 37, 38, 40, 41, 42, 43,
                45, 46, 48, 49, 50, 51, 52, 53, 54, 55, 56, 58, 59, 60, 61, 62,
                63,
                64, 65, 66, 68, 69, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 82,
                83, 84,
                85, 87, 88, 89, 90, 91, 93, 94, 95, 96, 97, 98, 99,
            }));
        UNIT_ASSERT(
            policy.Suites.at(Tpcds.Name).RequiredVerifiedQueries ==
            std::set<ui32>({
                3, 8, 9, 15, 16, 19, 21, 28, 34, 38, 41, 42, 43, 48, 52, 55,
                62, 69, 73, 87, 88, 90, 93, 94, 95, 96, 97, 99,
            }));
        UNIT_ASSERT_VALUES_EQUAL(
            SnapshotPairFloorQueries(policy.Suites.at(Tpcds.Name)).size(),
            82);
        UNIT_ASSERT_VALUES_EQUAL(
            policy.Suites.at(Tpcds.Name).RequiredFormulaQueries.size(),
            82);

        const auto report = CoverageReportHeader(Tpcds);
        UNIT_ASSERT_VALUES_EQUAL(
            report["format"].GetStringSafe(),
            CoverageReportFormat);
        UNIT_ASSERT_VALUES_EQUAL(
            report["version"].GetUIntegerSafe(),
            CoverageReportVersion);
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

        TVerifierProcess process;
        process.Arguments = {"/verifier"};
        process.ExitCode = 1;
        process.Stdout = verifierVerdict;
        const auto processArtifacts = PreserveVerifierProcess(
            "artifact_contract", 7, process, formulaPath);
        const auto artifacts = PreserveArtifacts(
            "artifact_contract",
            7,
            query,
            verifierVerdict,
            initial,
            final,
            processArtifacts);

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
            artifacts["formula"].GetStringSafe(),
            processArtifacts["formula"].GetStringSafe());
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

    Y_UNIT_TEST(VerifierProtocolFailuresRetainRawEvidence) {
        const TRBOSemanticSnapshotBoundaryResultV1 initial{
            ERBOSemanticSnapshotBoundaryV1::Initial, "{\"before\":1}\n", {}, {}};
        const TRBOSemanticSnapshotBoundaryResultV1 final{
            ERBOSemanticSnapshotBoundaryV1::Final, "{\"after\":1}\n", {}, {}};
        struct TCase {
            TString Stdout;
            TString Stderr;
            int ExitCode;
            TString Error;
            bool HasFormula;
        };
        const TVector<TCase> cases = {
            {"not JSON\r\n", "diagnostic\n", 2, {}, true},
            {"{\"status\":\"UNKNOWN\"}\n", "{\"status\":\"UNKNOWN\"}\n", 2, {}, true},
            {"{\"unexpected\":1}\n", "", 2, {}, true},
            {"{\"status\":\"VERIFIED_BOUNDED\"}\n", "", 2, {}, true},
            {"partial output\n", "startup diagnostic\n", -1, "cannot start verifier", false},
        };
        ui32 queryId = 0;
        for (const auto& test : cases) {
            ++queryId;
            TTempDir temporary;
            const auto formulaPath = temporary.Path() / "problem.smt2";
            const TString formula = "(assert false)\n(check-sat)\n";
            if (test.HasFormula) {
                TFileOutput(formulaPath.GetPath()).Write(formula);
            }
            TVerifierProcess process;
            process.Arguments = {"/verifier", "--rows", "2"};
            process.ExitCode = test.ExitCode;
            process.Stdout = test.Stdout;
            process.Stderr = test.Stderr;
            process.Error = test.Error;
            const auto outcome = ClassifyVerifierProcess(
                "protocol_evidence", queryId, "SELECT 1;\r\n", 0,
                initial, final, process, formulaPath, false);
            UNIT_ASSERT_VALUES_EQUAL(outcome.Status, "HARNESS_ERROR");
            UNIT_ASSERT(outcome.Fatal);
            UNIT_ASSERT(!outcome.Json.Has("artifact_error"));
            const auto& evidence = outcome.Json["process_artifacts"];
            const auto read = [&](TStringBuf key) {
                return TFileInput((GetOutputPath() /
                    evidence[key].GetStringSafe()).GetPath()).ReadAll();
            };
            UNIT_ASSERT_VALUES_EQUAL(read("stdout"), test.Stdout);
            UNIT_ASSERT_VALUES_EQUAL(read("stderr"), test.Stderr);
            UNIT_ASSERT_VALUES_EQUAL(evidence["stdout_sha256"].GetStringSafe().size(), 64);
            UNIT_ASSERT_VALUES_EQUAL(evidence["stderr_sha256"].GetStringSafe().size(), 64);
            NJson::TJsonValue command;
            UNIT_ASSERT(ParseJson(read("command"), command));
            UNIT_ASSERT_VALUES_EQUAL(command["exit_code"].GetIntegerSafe(), test.ExitCode);
            UNIT_ASSERT_VALUES_EQUAL(command["error"].GetStringSafe(), test.Error);
            UNIT_ASSERT_VALUES_EQUAL(command["arguments"][0].GetStringSafe(), "/verifier");
            UNIT_ASSERT_VALUES_EQUAL(evidence.Has("formula"), test.HasFormula);
            if (test.HasFormula) {
                UNIT_ASSERT_VALUES_EQUAL(read("formula"), formula);
                UNIT_ASSERT_VALUES_EQUAL(
                    evidence["formula_sha256"].GetStringSafe().size(), 64);
            }
            UNIT_ASSERT(outcome.Json["artifacts"].Has("initial_snapshot_sha256"));
            UNIT_ASSERT(outcome.Json["artifacts"].Has("final_snapshot_sha256"));
            UNIT_ASSERT(outcome.Json["artifacts"].Has("query_sha256"));
        }
    }

    Y_UNIT_TEST(SuccessEvidenceRetentionIsExplicit) {
        const TRBOSemanticSnapshotBoundaryResultV1 initial{
            ERBOSemanticSnapshotBoundaryV1::Initial, "{\"before\":1}\n", {}, {}};
        const TRBOSemanticSnapshotBoundaryResultV1 final{
            ERBOSemanticSnapshotBoundaryV1::Final, "{\"after\":1}\n", {}, {}};
        TTempDir temporary;
        const auto formulaPath = temporary.Path() / "problem.smt2";
        TFileOutput(formulaPath.GetPath()).Write("(check-sat)\n");
        for (const TString status : {"FORMULA_EMITTED", "VERIFIED_BOUNDED"}) {
            TVerifierProcess process;
            process.Arguments = {"/verifier"};
            process.ExitCode = 0;
            process.Stdout = TStringBuilder() << "{\"status\":\"" << status << "\"}\n";
            for (const bool retain : {false, true}) {
                const auto outcome = ClassifyVerifierProcess(
                    "success_evidence", retain ? 2 : 1, "SELECT 1;\n", 0,
                    initial, final, process, formulaPath, retain);
                UNIT_ASSERT_VALUES_EQUAL(outcome.Status, status);
                UNIT_ASSERT(!outcome.Fatal);
                UNIT_ASSERT_VALUES_EQUAL(outcome.Json.Has("process_artifacts"), retain);
                UNIT_ASSERT_VALUES_EQUAL(outcome.Json.Has("artifacts"), retain);
                if (retain) {
                    UNIT_ASSERT(outcome.Json["process_artifacts"].Has("formula_sha256"));
                    UNIT_ASSERT(outcome.Json["artifacts"].Has("verifier_verdict_sha256"));
                    UNIT_ASSERT_VALUES_EQUAL(
                        outcome.Json["artifacts"]["formula"].GetStringSafe(),
                        outcome.Json["process_artifacts"]["formula"].GetStringSafe());
                }
            }
        }
    }

    Y_UNIT_TEST(CapturedPairOutcomeIsIndependentOfPreparation) {
        const TRBOSemanticSnapshotBoundaryResultV1 initial{
            ERBOSemanticSnapshotBoundaryV1::Initial,
            {},
            "Unsupported scalar callable YqlAggWin",
            {},
        };
        const TRBOSemanticSnapshotBoundaryResultV1 final{
            ERBOSemanticSnapshotBoundaryV1::Final,
            "{\"supported\":true}\n",
            {},
            {},
        };

        auto outcome = ClassifyCapturedPair(
            Tpcds,
            12,
            "SELECT 1;\n",
            17,
            initial,
            final,
            1'000,
            Nothing(),
            true);
        SetPreparationOutcome(
            outcome,
            false,
            "physical query compilation rejected YqlAggWin");

        UNIT_ASSERT_VALUES_EQUAL(outcome.Status, "UNSUPPORTED");
        UNIT_ASSERT_VALUES_EQUAL(outcome.Layer, "initial_export");
        UNIT_ASSERT_VALUES_EQUAL(outcome.PrepareStatus, "FAILED");
        UNIT_ASSERT_VALUES_EQUAL(
            outcome.Json["status"].GetStringSafe(),
            "UNSUPPORTED");
        UNIT_ASSERT_VALUES_EQUAL(
            outcome.Json["prepare_status"].GetStringSafe(),
            "FAILED");
        UNIT_ASSERT_STRING_CONTAINS(
            outcome.Json["prepare_reason"].GetStringSafe(),
            "physical query compilation");
        UNIT_ASSERT_VALUES_EQUAL(outcome.UnsupportedReasons.size(), 1);
        UNIT_ASSERT(outcome.Json.Has("artifacts"));
        const auto& artifacts = outcome.Json["artifacts"];
        UNIT_ASSERT_VALUES_EQUAL(artifacts.GetMapSafe().size(), 6);
        UNIT_ASSERT_VALUES_EQUAL(
            artifacts["initial_unsupported"].GetStringSafe(),
            "tpcds_q12.initial.unsupported.txt");
        UNIT_ASSERT_VALUES_EQUAL(
            artifacts["final_snapshot"].GetStringSafe(),
            "tpcds_q12.final.json");
        UNIT_ASSERT_VALUES_EQUAL(
            TFileInput((GetOutputPath() /
                artifacts["initial_unsupported"].GetStringSafe()).GetPath()).ReadAll(),
            initial.UnsupportedReason);
    }

    Y_UNIT_TEST(CapturedPairRequiresInitialThenFinalOrder) {
        const TRBOSemanticSnapshotBoundaryResultV1 first{
            ERBOSemanticSnapshotBoundaryV1::Final,
            {},
            "unsupported final",
            {},
        };
        const TRBOSemanticSnapshotBoundaryResultV1 second{
            ERBOSemanticSnapshotBoundaryV1::Initial,
            {},
            "unsupported initial",
            {},
        };
        const auto outcome = ClassifyCapturedPair(
            Tpcds,
            39,
            "SELECT 1;\n",
            17,
            first,
            second,
            1'000,
            Nothing(),
            true);
        UNIT_ASSERT_VALUES_EQUAL(outcome.Status, "HARNESS_ERROR");
        UNIT_ASSERT(outcome.Fatal);
        UNIT_ASSERT_STRING_CONTAINS(outcome.Reason, "count or order");
    }

    Y_UNIT_TEST(PolicySnapshotPairPredicateRequiresExactlyInitialThenFinal) {
        const TRBOSemanticSnapshotBoundaryResultV1 initial{
            ERBOSemanticSnapshotBoundaryV1::Initial, {}, {}, {}};
        const TRBOSemanticSnapshotBoundaryResultV1 final{
            ERBOSemanticSnapshotBoundaryV1::Final, {}, {}, {}};

        UNIT_ASSERT(!IsExactSnapshotPair({}));
        UNIT_ASSERT(!IsExactSnapshotPair({initial}));
        UNIT_ASSERT(!IsExactSnapshotPair({final, initial}));
        UNIT_ASSERT(IsExactSnapshotPair({initial, final}));
        UNIT_ASSERT(!IsExactSnapshotPair({initial, final, final}));
    }

    Y_UNIT_TEST(OptimizerFailureRetainsCaptureMetadata) {
        const auto zeroCapture = OptimizerFailure(
            51, 17, 0, "window metadata references a missing member");
        UNIT_ASSERT(!zeroCapture.SnapshotPairCaptured);
        UNIT_ASSERT_VALUES_EQUAL(
            zeroCapture.Json["capture_count"].GetUIntegerSafe(), 0);
        UNIT_ASSERT_VALUES_EQUAL(zeroCapture.Status, "OPTIMIZER_FAILURE");
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
        // Membership is locked once in PolicyFileMatchesFixedContract.
        UNIT_ASSERT(
            config.Selected == policy.Suites.at(Tpcds.Name).RequiredVerifiedQueries);
        UNIT_ASSERT(config.Solver);
        UNIT_ASSERT_STRING_CONTAINS(
            *config.Solver,
            "contrib/tools/z3/z3");
        UNIT_ASSERT_VALUES_UNEQUAL(*config.Solver, "/ambient/z3");
        UNIT_ASSERT_VALUES_EQUAL(config.TimeoutMs, ProofFloorTimeoutMs);
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
