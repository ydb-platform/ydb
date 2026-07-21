#include "../capture.h"

#include <ydb/core/client/minikql_compile/mkql_compile_service.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/gateway/kqp_metadata_loader.h>
#include <ydb/core/kqp/host/kqp_host.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <yql/essentials/core/services/mounts/yql_mounts.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <yql/essentials/public/langver/yql_langver.h>

#include <library/cpp/getopt/last_getopt.h>
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
    TString OutputPath;
    ui64 Ordinal = 0;
    bool BenchmarkColumnStore = false;
};

class TRecordingSink final : public IRBOSemanticSnapshotSink {
public:
    explicit TRecordingSink(ui64 ordinal)
        : Ordinal(ordinal)
    {
    }

    void OnSemanticSnapshot(TRBOSemanticSnapshotBoundaryResultV1 result) override {
        std::lock_guard guard(Mutex);
        Results.push_back(std::move(result));
    }

    std::optional<ui64> GetRuleApplicationPrefixTarget() const override {
        return Ordinal;
    }

    TVector<TRBOSemanticSnapshotBoundaryResultV1> Take() {
        std::lock_guard guard(Mutex);
        return std::move(Results);
    }

private:
    const ui64 Ordinal;
    std::mutex Mutex;
    TVector<TRBOSemanticSnapshotBoundaryResultV1> Results;
};

TOptions ParseOptions(int argc, char** argv) {
    TOptions result;
    NLastGetopt::TOpts options;
    options.SetTitle("Capture one real new-RBO rule-application prefix");
    options.SetFreeArgsNum(0);
    options.AddHelpOption('h');
    options.AddLongOption("schema", "YQL schema file to execute")
        .Required()
        .RequiredArgument("PATH")
        .StoreResult(&result.SchemaPath);
    options.AddLongOption("query", "YQL query file to prepare exactly once")
        .Required()
        .RequiredArgument("PATH")
        .StoreResult(&result.QueryPath);
    options.AddLongOption(
            "benchmark-column-store",
            "Apply benchmark_ut's exact TPCH/TPCDS column-store schema rewrite and query prelude")
        .NoArgument()
        .SetFlag(&result.BenchmarkColumnStore);
    options.AddLongOption(
            "rbo-rule-prefix-ordinal",
            "One-based committed dynamic rule application to capture")
        .Required()
        .RequiredArgument("N")
        .StoreResult(&result.Ordinal);
    options.AddLongOption(
            "rbo-rule-prefix-output",
            "New or empty output directory for strict capture artifacts")
        .Required()
        .RequiredArgument("DIR")
        .StoreResult(&result.OutputPath);
    NLastGetopt::TOptsParseResult parsed(&options, argc, argv);
    if (result.Ordinal == 0) {
        ythrow yexception()
            << "--rbo-rule-prefix-ordinal must be a positive integer";
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

void WriteCapture(const TFsPath& output, const TCaptureOutput& capture) {
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
    // The manifest is the commit marker and is deliberately written last.
    WriteNew(output / "capture.json", RenderManifest(capture));
}

int Run(const TOptions& options) {
    const TFsPath output = PrepareOutputDirectory(options.OutputPath);
    TString schema = ReadInput(options.SchemaPath, "schema input");
    TString query = ReadInput(options.QueryPath, "query input");
    if (options.BenchmarkColumnStore) {
        const TString rewritten = RewriteBenchmarkSchemaToColumnStore(schema);
        if (rewritten == schema) {
            ythrow yexception()
                << "benchmark schema rewrite found no CREATE TABLE statement";
        }
        schema = rewritten;
        query = AddBenchmarkQueryPrelude(query);
    }

    auto runner = MakeRunner();
    ExecuteSchema(runner, schema);

    NYql::TExprContext moduleContext;
    NYql::IModuleResolver::TPtr moduleResolver;
    if (!NYql::GetYqlDefaultModuleResolver(moduleContext, moduleResolver)) {
        ythrow yexception() << "cannot create the default YQL module resolver";
    }
    auto sink = std::make_shared<TRecordingSink>(options.Ordinal);
    auto host = MakeHost(
        runner.GetTestServer(),
        std::move(moduleResolver),
        sink);
    IKqpHost::TPrepareSettings settings;
    settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;
    // Constant-expression evaluation expects the actor activation context used
    // by a production KQP request and by the benchmark coverage harness.
    const auto prepared = runner.GetTestServer().GetRuntime()->RunCall([
        host,
        query = std::move(query),
        settings
    ] {
        return host->SyncPrepareDataQuery(query, settings);
    });

    auto capture = ClassifyCapture(
        options.Ordinal,
        prepared.Success(),
        sink->Take(),
        prepared.Issues().ToString());
    WriteCapture(output, capture);
    Cout << StatusName(capture.Status) << Endl;
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
