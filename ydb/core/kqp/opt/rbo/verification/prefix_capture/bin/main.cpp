#include "../capture.h"

#include <ydb/core/client/minikql_compile/mkql_compile_service.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/gateway/kqp_metadata_loader.h>
#include <ydb/core/kqp/host/kqp_host.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/protos/subdomains.pb.h>

#include <yql/essentials/core/services/mounts/yql_mounts.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <yql/essentials/public/langver/yql_langver.h>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/resource/resource.h>

#include <util/folder/path.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/stream/output.h>
#include <util/stream/str.h>

#include <algorithm>
#include <cctype>
#include <mutex>
#include <utility>

namespace NKikimr::NKqp::NRBOPrefixCapture {
namespace {

constexpr const char* TestCluster = "local_rbo_prefix_capture";

struct TOptions {
    TString SchemaPath;
    TString QueryPath;
    TString SchemaDirectory;
    TString QueryDirectory;
    TString SchemeLimitsPath;
    ui32 LanguageVersion = NYql::GetMaxLangVersion();
    TString OutputPath;
    ui64 TransformationOrdinal = 0;
    bool BenchmarkColumnStore = false;
    TString QueryMode = "data";
};

class TRecordingSink final : public IRBOSemanticSnapshotSink {
public:
    explicit TRecordingSink(ui64 transformationOrdinal)
        : TransformationOrdinal(transformationOrdinal)
    {
    }

    void OnSemanticSnapshot(TRBOSemanticSnapshotBoundaryResultV1 result) override {
        std::lock_guard guard(Mutex);
        Results.push_back(std::move(result));
    }

    std::optional<ui64> GetTransformationPrefixTarget() const override {
        return TransformationOrdinal;
    }

    TVector<TRBOSemanticSnapshotBoundaryResultV1> Take() {
        std::lock_guard guard(Mutex);
        return std::move(Results);
    }

private:
    const ui64 TransformationOrdinal;
    std::mutex Mutex;
    TVector<TRBOSemanticSnapshotBoundaryResultV1> Results;
};

TOptions ParseOptions(int argc, char** argv) {
    TOptions result;
    NLastGetopt::TOpts options;
    options.SetTitle("Capture one real new-RBO transformation prefix");
    options.SetFreeArgsNum(0);
    options.AddHelpOption('h');
    options.AddLongOption("schema", "YQL schema file to execute")
        .RequiredArgument("PATH")
        .StoreResult(&result.SchemaPath);
    options.AddLongOption("query", "YQL query file to prepare exactly once")
        .RequiredArgument("PATH")
        .StoreResult(&result.QueryPath);
    options.AddLongOption("schema-dir", "Execute immediate .sql/.yql schema files in filename order")
        .RequiredArgument("DIR")
        .StoreResult(&result.SchemaDirectory);
    options.AddLongOption("query-dir", "Capture immediate .sql/.yql queries serially, continuing after failures")
        .RequiredArgument("DIR")
        .StoreResult(&result.QueryDirectory);
    options.AddLongOption("scheme-limits", "Root domain TSchemeLimits protobuf text file")
        .RequiredArgument("PATH")
        .StoreResult(&result.SchemeLimitsPath);
    options.AddLongOption("lang-version", "SQL language version (defaults to the newest supported version)")
        .RequiredArgument("VERSION")
        .StoreResult(&result.LanguageVersion);
    options.AddLongOption("query-mode", "Preparation mode: data, generic, or generic-spilling")
        .RequiredArgument("MODE")
        .StoreResult(&result.QueryMode);
    options.AddLongOption(
            "benchmark-column-store",
            "Apply benchmark_ut's exact TPCH/TPCDS column-store schema rewrite and query prelude")
        .NoArgument()
        .SetFlag(&result.BenchmarkColumnStore);
    options.AddLongOption(
            "rbo-transformation-prefix-ordinal",
            "One-based optimizer transformation event to capture")
        .Required()
        .RequiredArgument("N")
        .StoreResult(&result.TransformationOrdinal);
    options.AddLongOption(
            "rbo-transformation-prefix-output",
            "New or empty output directory for strict capture artifacts")
        .Required()
        .RequiredArgument("DIR")
        .StoreResult(&result.OutputPath);
    NLastGetopt::TOptsParseResult parsed(&options, argc, argv);
    if (result.SchemaPath.empty() == result.SchemaDirectory.empty() ||
        result.QueryPath.empty() == result.QueryDirectory.empty())
    {
        ythrow yexception() << "specify exactly one of --schema/--schema-dir and --query/--query-dir";
    }
    if (result.QueryMode != "data" && result.QueryMode != "generic" && result.QueryMode != "generic-spilling") {
        ythrow yexception() << "--query-mode must be data, generic, or generic-spilling";
    }
    if (result.TransformationOrdinal == 0) {
        ythrow yexception()
            << "--rbo-transformation-prefix-ordinal must be a positive integer";
    }
    return result;
}

bool HasNonWhitespace(TStringBuf text) {
    return std::any_of(text.begin(), text.end(), [](char value) {
        return !std::isspace(static_cast<unsigned char>(value));
    });
}

TString ReadInput(const TString& name, TStringBuf description) {
    const TFsPath path(name);
    if (!path.IsFile() || path.IsSymlink()) {
        ythrow yexception() << description << " is not a regular file: " << name;
    }
    TString value = TFileInput(path.GetPath()).ReadAll();
    if (!HasNonWhitespace(value) || value.find('\0') != TString::npos) {
        ythrow yexception() << description << " is empty or binary: " << name;
    }
    return value;
}

TVector<TString> InputPaths(const TString& file, const TString& directory) {
    if (!file.empty()) {
        return {file};
    }
    const TFsPath path(directory);
    if (!path.IsDirectory() || path.IsSymlink()) {
        ythrow yexception() << "input is not a regular directory: " << directory;
    }
    TVector<TString> names;
    path.ListNames(names);
    std::sort(names.begin(), names.end());
    TVector<TString> result;
    for (const auto& name : names) {
        const TFsPath child = path / name;
        if ((name.EndsWith(".sql") || name.EndsWith(".yql")) && child.IsFile()) {
            // ReadInput rejects symlinks rather than following external inputs.
            ReadInput(child.GetPath(), "directory input");
            result.push_back(child.GetPath());
        }
    }
    if (result.empty()) {
        ythrow yexception() << "input directory has no .sql/.yql files: " << directory;
    }
    return result;
}

TFsPath PrepareOutputDirectory(const TString& name) {
    TFsPath output(name);
    if (output.Exists()) {
        if (!output.IsDirectory() || output.IsSymlink()) {
            ythrow yexception() << "capture output is not a regular directory: " << name;
        }
        TVector<TString> children;
        output.ListNames(children);
        if (!children.empty()) {
            ythrow yexception() << "capture output directory is not empty: " << name;
        }
    } else {
        output.MkDirs();
    }
    return output;
}

TIntrusivePtr<IKqpGateway> MakeGateway(
    Tests::TServer& server,
    const NYql::TKikimrConfiguration::TPtr& config)
{
    auto counters = MakeIntrusive<TKqpRequestCounters>();
    counters->Counters = new TKqpCounters(
        server.GetRuntime()->GetAppData(0).Counters);
    counters->TxProxyMon = new NTxProxy::TTxProxyMon(
        server.GetRuntime()->GetAppData(0).Counters);
    auto loader = std::make_shared<TKqpTableMetadataLoader>(
        TestCluster,
        server.GetRuntime()->GetAnyNodeActorSystem(),
        config,
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

NYql::TKikimrConfiguration::TPtr MakeConfiguration(Tests::TServer& server, TStringBuf queryMode) {
    auto config = MakeIntrusive<NYql::TKikimrConfiguration>();
    const TString defaultsData = NResource::Find("kqp_default_settings.txt");
    TStringInput defaultsStream(defaultsData);
    NKikimrKqp::TKqpDefaultSettings defaults;
    if (!TryParseFromTextFormat(defaultsStream, defaults)) {
        ythrow yexception() << "cannot parse embedded KQP default settings";
    }
    config->Init(
        defaults.GetDefaultSettings(),
        TestCluster,
        TVector<NKikimrKqp::TKqpSetting>{},
        false);
    // Match the production compile actor's database-relative SQL resolution.
    config->_KqpTablePathPrefix = "/Root";
    config->ApplyServiceConfig(server.GetSettings().AppConfig->GetTableServiceConfig());
    // This helper runs outside actor activation context; use the test runtime
    // rather than the TLS-based AppData() accessor used by the compile actor.
    config->FeatureFlags = server.GetRuntime()->GetAppData(0).FeatureFlags;
    if (queryMode != "data") {
        const auto rowsLimit = server.GetSettings().AppConfig->GetQueryServiceConfig().GetScriptResultRowsLimit();
        if (rowsLimit > 0) {
            config->_ResultRowsLimit = rowsLimit;
        } else {
            config->_ResultRowsLimit.Clear();
        }
        config->_KqpYqlConstraintsTransformerEnabled = config->FeatureFlags.GetEnableKqpConstraintsTransformer();
    }
    config->OptValidateStreamingCheckpoints = false;
    if (queryMode == "generic-spilling") {
        config->_KqpEnableSpilling = true;
    }
    config->FreezeDefaults();
    return config;
}

TKikimrRunner MakeRunner(ui32 languageVersion) {
    NKikimrConfig::TAppConfig appConfig;
    auto* service = appConfig.MutableTableServiceConfig();
    service->SetEnableNewRBO(true);
    service->SetEnableFallbackToYqlOptimizer(false);
    service->SetAllowOlapDataQuery(true);
    service->SetDefaultLangVer(languageVersion);
    service->SetBackportMode(
        NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    return TKikimrRunner(TKikimrSettings(appConfig).SetWithSampleTables(false));
}

TIntrusivePtr<IKqpHost> MakeHost(
    Tests::TServer& server,
    NYql::IModuleResolver::TPtr moduleResolver,
    std::shared_ptr<IRBOSemanticSnapshotSink> sink,
    const NYql::TKikimrConfiguration::TPtr& config)
{
    return CreateKqpHost(
        MakeGateway(server, config),
        TestCluster,
        "/Root",
        config,
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

void ExecuteSchema(TKikimrRunner& runner, const TString& schema) {
    auto session = runner.GetTableClient()
        .CreateSession()
        .GetValueSync()
        .GetSession();
    const auto result = session.ExecuteSchemeQuery(schema).GetValueSync();
    if (!result.IsSuccess()) {
        ythrow yexception()
            << "schema execution failed: " << result.GetIssues().ToString();
    }
}

void WriteNew(const TFsPath& path, const TString& contents) {
    if (path.Exists()) {
        ythrow yexception() << "refusing to overwrite capture artifact " << path;
    }
    TFileOutput(path.GetPath()).Write(contents);
}

void WriteCapture(const TFsPath& output, const TCaptureOutput& capture, const TOptions& options) {
    WriteNew(output / "initial.json", capture.InitialSnapshot);
    switch (capture.Status) {
        case ECaptureStatus::PrefixCaptured:
            WriteNew(output / "prefix.json", capture.CandidateSnapshot);
            break;
        case ECaptureStatus::OptimizerComplete:
            WriteNew(output / "final.json", capture.CandidateSnapshot);
            break;
        case ECaptureStatus::PrefixUnsupported:
        case ECaptureStatus::FinalUnsupported:
            break;
    }
    NJson::TJsonValue preparation(NJson::JSON_MAP);
    preparation["query_mode"] = options.QueryMode;
    preparation["language_version"] = options.LanguageVersion;
    preparation["benchmark_column_store"] = options.BenchmarkColumnStore;
    preparation["new_rbo"] = true;
    preparation["fallback_to_yql_optimizer"] = false;
    WriteNew(output / "preparation.json", NJson::WriteJson(preparation, true, true));
    // The manifest is the commit marker and is deliberately written last.
    WriteNew(output / "capture.json", RenderManifest(capture));
}

void CaptureQuery(TKikimrRunner& runner, const TOptions& options, const TFsPath& output) {
    NJson::TJsonValue started(NJson::JSON_MAP);
    started["query_path"] = options.QueryPath;
    WriteNew(output / "started.json", NJson::WriteJson(started, true, true));
    TString query = ReadInput(options.QueryPath, "query input");
    if (options.BenchmarkColumnStore) {
        query = AddBenchmarkQueryPrelude(query);
    }

    NYql::IModuleResolver::TPtr moduleResolver;
    if (!NYql::GetYqlDefaultModuleResolverWithContext(moduleResolver)) {
        ythrow yexception() << "cannot create the default YQL module resolver";
    }
    auto sink = std::make_shared<TRecordingSink>(options.TransformationOrdinal);
    auto config = MakeConfiguration(runner.GetTestServer(), options.QueryMode);
    auto host = MakeHost(
        runner.GetTestServer(),
        std::move(moduleResolver),
        sink,
        config);
    IKqpHost::TPrepareSettings settings;
    settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;
    if (options.QueryMode != "data") {
        // QUERY_TYPE_SQL_GENERIC_QUERY serializes SQL result blocks.
        settings.ConcurrentResults = false;
    }
    // Constant-expression evaluation expects the actor activation context used
    // by a production KQP request and by the benchmark coverage harness.
    const auto prepared = runner.GetTestServer().GetRuntime()->RunCall([
        host,
        query = std::move(query),
        settings,
        generic = options.QueryMode != "data"
    ] {
        if (!generic) {
            return host->SyncPrepareDataQuery(query, settings);
        }
        auto preparing = host->PrepareGenericQuery(query, settings);
        while (!preparing->HasResult()) {
            preparing->Continue().GetValueSync();
        }
        return preparing->GetResult();
    });

    auto boundaries = sink->Take();
    NJson::TJsonValue attempt(NJson::JSON_MAP);
    attempt["query_path"] = options.QueryPath;
    attempt["query_mode"] = options.QueryMode;
    attempt["language_version"] = options.LanguageVersion;
    attempt["preparation_succeeded"] = prepared.Success();
    attempt["preparation_issues"] = prepared.Issues().ToString();
    attempt["boundaries"] = NJson::TJsonValue(NJson::JSON_ARRAY);
    for (const auto& boundary : boundaries) {
        NJson::TJsonValue item(NJson::JSON_MAP);
        item["boundary"] = boundary.Boundary == ERBOSemanticSnapshotBoundaryV1::Initial
            ? "Initial" : boundary.Boundary == ERBOSemanticSnapshotBoundaryV1::Final ? "Final" : "Prefix";
        item["root_ordinal"] = boundary.RootOrdinal;
        item["root_count"] = boundary.RootCount;
        item["result_count"] = boundary.ResultCount;
        if (boundary.ResultOrdinal) {
            item["result_ordinal"] = *boundary.ResultOrdinal;
        } else {
            item["result_ordinal"] = NJson::TJsonValue(NJson::JSON_NULL);
        }
        item["supported"] = boundary.IsSupported();
        item["unsupported_reason"] = boundary.UnsupportedReason;
        attempt["boundaries"].AppendValue(std::move(item));
    }
    // Keep preparation and exporter failures distinct even when no valid
    // capture manifest can be produced (e.g. an unsupported initial boundary).
    WriteNew(output / "attempt.json", NJson::WriteJson(attempt, true, true));
    auto capture = ClassifyCapture(
        options.TransformationOrdinal,
        prepared.Success(),
        std::move(boundaries),
        prepared.Issues().ToString());
    WriteCapture(output, capture, options);
    if (!options.QueryDirectory.empty()) {
        Cout << options.QueryPath << ": ";
    }
    Cout << StatusName(capture.Status) << Endl;
}

int Run(const TOptions& options) {
    const auto schemas = InputPaths(options.SchemaPath, options.SchemaDirectory);
    const auto queries = InputPaths(options.QueryPath, options.QueryDirectory);
    const TFsPath output = PrepareOutputDirectory(options.OutputPath);
    auto runner = MakeRunner(options.LanguageVersion);
    if (!options.SchemeLimitsPath.empty()) {
        NKikimrSubDomains::TSubDomainSettings subdomain;
        subdomain.SetName("Root");
        const TString limits = ReadInput(options.SchemeLimitsPath, "scheme limits");
        TStringInput text(limits);
        if (!TryParseFromTextFormat(text, *subdomain.MutableSchemeLimits())) {
            ythrow yexception() << "cannot parse root scheme limits";
        }
        if (runner.GetTestClient().AlterSubdomain("/", subdomain) != NMsgBusProxy::MSTATUS_OK) {
            ythrow yexception() << "cannot apply root scheme limits";
        }
    }
    for (const auto& path : schemas) {
        TString schema = ReadInput(path, "schema input");
        if (options.BenchmarkColumnStore) {
            const TString rewritten = RewriteBenchmarkSchemaToColumnStore(schema);
            if (rewritten == schema) {
                ythrow yexception() << "benchmark schema rewrite found no CREATE TABLE statement";
            }
            schema = rewritten;
        }
        ExecuteSchema(runner, schema);
    }
    WriteNew(output / "schema-ready.json", "{\"ready\":true}\n");
    for (const auto& path : queries) {
        auto selected = options;
        selected.QueryPath = path;
        if (options.QueryDirectory.empty()) {
            CaptureQuery(runner, selected, output);
        } else {
            const auto child = PrepareOutputDirectory((output / TFsPath(path).GetName()).GetPath());
            try {
                CaptureQuery(runner, selected, child);
            } catch (const std::exception& error) {
                NJson::TJsonValue failure(NJson::JSON_MAP);
                failure["error"] = TString(error.what());
                WriteNew(child / "error.json", NJson::WriteJson(failure, true, true));
                Cout << path << ": CAPTURE_ERROR: " << error.what() << Endl;
            }
        }
    }
    return 0;
}

} // namespace
} // namespace NKikimr::NKqp::NRBOPrefixCapture

int main(int argc, char** argv) {
    try {
        const auto options =
            NKikimr::NKqp::NRBOPrefixCapture::ParseOptions(argc, argv);
        return NKikimr::NKqp::NRBOPrefixCapture::Run(options);
    } catch (const std::exception& error) {
        Cerr << "kqp_rbo_prefix_capture: " << error.what() << Endl;
        return 2;
    }
}
