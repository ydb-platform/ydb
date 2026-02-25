#pragma once

#include <yql/essentials/sql/settings/translation_settings.h>
#include <yql/essentials/core/file_storage/defs/downloader.h>
#include <yql/essentials/core/file_storage/file_storage.h>
#include <yql/essentials/core/credentials/yql_credentials.h>
#include <yql/essentials/core/url_lister/interface/url_lister.h>
#include <yql/essentials/core/yql_data_provider.h>
#include <yql/essentials/core/yql_user_data.h>
#include <yql/essentials/core/facade/yql_facade.h>
#include <yql/essentials/core/qplayer/storage/interface/yql_qstorage.h>
#include <yql/essentials/core/layers/remote_layer_provider.h>
#include <yql/essentials/public/langver/yql_langver.h>
#include <yql/essentials/utils/log/log.h>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/yson/public.h>
#include <library/cpp/logger/priority.h>

#include <util/generic/hash_set.h>
#include <util/generic/hash.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>

#include <functional>

namespace NKikimr::NMiniKQL {
class IFunctionRegistry;
} // namespace NKikimr::NMiniKQL

namespace NYql {
class TFileStorageConfig;
class TGatewaysConfig;
} // namespace NYql

namespace NYql::NProto {
class TPgExtensions;
} // namespace NYql::NProto

namespace NYqlMountConfig {
class TMountConfig;
} // namespace NYqlMountConfig

namespace NYql {

enum class ERunMode {
    Parse /* "parse" */,
    Compile /* "compile" */,
    Validate /* "validate" */,
    Optimize /* "optimize" */,
    Peephole /* "peephole" */,
    Lineage /* "lineage" */,
    Discover /* "discover" */,
    Run /* "run" */,
};

enum class EProgramType {
    SExpr /* "s-expr" */,
    Sql /* "sql" */,
    Pg /* "pg" */,
};

enum class EQPlayerMode {
    None /* "none" */,
    Capture /* "capture" */,
    Replay /* "replay" */,
};

class TFacadeRunOptions {
public:
    TFacadeRunOptions();
    ~TFacadeRunOptions();

    EProgramType ProgramType = EProgramType::SExpr;
    TLangVersion LangVer = MinLangVersion;
    TLangVersion MaxLangVer = GetMaxLangVersion();
    NYson::EYsonFormat ResultsFormat = NYson::EYsonFormat::Text;
    ERunMode Mode = ERunMode::Run;
    TString ProgramFile;
    TString ProgramText;
    TString User;
    TString Token;
    ui64 MemLimit = 0;
    EQPlayerMode QPlayerMode = EQPlayerMode::None;
    EQPlayerCaptureMode QPlayerCaptureMode = EQPlayerCaptureMode::None;
    TString OperationId;
    TQContext QPlayerContext;

    THashSet<TString> SqlFlags;
    ui16 SyntaxVersion = 1;
    bool AnsiLexer = false;
    bool TestAntlr4 = false;
    bool AssumeYdbOnClusterWithSlash = false;
    bool TestSqlFormat = false;
    bool TestLexers = false;
    bool TestComplete = false;
    bool TestSyntaxAmbiguities = false;
    bool TestPartialTypecheck = false;
    THashMap<TString, NSQLTranslation::TTableBindingSettings> Bindings;

    bool PrintAst = false;
    bool FullExpr = false;
    bool WithTypes = false;
    bool FullStatistics = false;
    bool PrintDiagnostics = false;
    int Verbosity = TLOG_ERR;
    bool ShowLog = false;
    bool WithFinalIssues = false;
    bool ValidateResultFormat = false;

    IOutputStream* TraceOptStream = nullptr;

    IOutputStream* ErrStream = &Cerr;
    IOutputStream* PlanStream = nullptr;
    IOutputStream* ExprStream = nullptr;
    IOutputStream* ResultStream = nullptr;
    IOutputStream* StatStream = nullptr;

    NYql::TUserDataTable DataTable;
    TVector<TString> UdfsPaths;
    TString Params;
    TString YsonAttrs;
    NUdf::EValidateMode ValidateMode = NUdf::EValidateMode::Greedy;
    TCredentials::TPtr Credentials = MakeIntrusive<TCredentials>();

    THashSet<TString> GatewayTypes;
    TString UdfResolverPath;
    TString UdfResolverLog;
    bool UdfResolverFilterSyscalls = false;
    bool ScanUdfs = false;
    THolder<NYqlMountConfig::TMountConfig> MountConfig;
    THolder<TGatewaysConfig> GatewaysConfig;
    THolder<TFileStorageConfig> FsConfig;
    THolder<NProto::TPgExtensions> PgExtConfig;
    TMaybe<TString> GatewaysPatch;

    // No command line options for these settings. Should be configured in the inherited class
    bool NoDebug = false;
    bool PgSupport = true;
    bool FailureInjectionSupport = false;
    bool UseRepeatableRandomAndTimeProviders = false;
    bool UseMetaFromGrpah = false;
    bool EnableResultPosition = false;
    bool EnableCredentials = false;
    bool EnableQPlayer = false;
    bool OptimizeLibs = true;
    bool CustomTests = false;
    bool EnableLineage = false;

    void Parse(int argc, const char** argv);

    void AddOptExtension(std::function<void(NLastGetopt::TOpts& opts)> optExtender) {
        OptExtenders_.push_back(std::move(optExtender));
    }
    void AddOptHandler(std::function<void(const NLastGetopt::TOptsParseResult& res)> optHandler) {
        OptHandlers_.push_back(std::move(optHandler));
    }
    void SetSupportedGateways(std::initializer_list<TString> gateways) {
        SupportedGateways_.insert(gateways);
    }

    void InitLogger();

    void PrintInfo(const TString& msg);

    static void ParseProtoConfig(const TString& cfgFile, google::protobuf::Message* config);

    template <typename TMessage>
    static THolder<TMessage> ParseProtoConfig(const TString& cfgFile) {
        auto config = MakeHolder<TMessage>();
        ParseProtoConfig(cfgFile, config.Get());
        return config;
    }

private:
    std::vector<std::function<void(NLastGetopt::TOpts&)>> OptExtenders_;
    std::vector<std::function<void(const NLastGetopt::TOptsParseResult&)>> OptHandlers_;
    THashSet<TString> SupportedGateways_;
    THolder<IOutputStream> ErrStreamHolder_;
    THolder<IOutputStream> PlanStreamHolder_;
    THolder<IOutputStream> ExprStreamHolder_;
    THolder<IOutputStream> ResultStreamHolder_;
    THolder<IOutputStream> StatStreamHolder_;
    IQStoragePtr QPlayerStorage_;
};

class TFacadeRunner {
public:
    explicit TFacadeRunner(TString name);
    ~TFacadeRunner();

    int Main(int argc, const char** argv);

    void AddFsDownloadFactory(std::function<NFS::IDownloaderPtr()> factory) {
        FsDownloadFactories_.push_back(std::move(factory));
    }
    void AddProviderFactory(std::function<NYql::TDataProviderInitializer()> factory) {
        ProviderFactories_.push_back(std::move(factory));
    }
    void AddUrlListerFactory(std::function<IUrlListerPtr()> factory) {
        UrlListerFactories_.push_back(std::move(factory));
    }
    void AddClusterMapping(TString name, TString provider) {
        ClusterMapping_[name] = std::move(provider);
    }
    template <class TPbConfig>
    void FillClusterMapping(const TPbConfig& config, const TString& provider) {
        for (auto& cluster : config.GetClusterMapping()) {
            ClusterMapping_.emplace(to_lower(cluster.GetName()), provider);
        }
    }
    void SetOperationProgressWriter(TOperationProgressWriter writer) {
        ProgressWriter_ = std::move(writer);
    }
    void SetOptPipelineConfigurator(IPipelineConfigurator* configurator) {
        OptPipelineConfigurator_ = configurator;
    }
    void SetPeepholePipelineConfigurator(IPipelineConfigurator* configurator) {
        PeepholePipelineConfigurator_ = configurator;
    }

    TFileStoragePtr GetFileStorage() const {
        return FileStorage_;
    }
    TIntrusivePtr<NKikimr::NMiniKQL::IFunctionRegistry> GetFuncRegistry();
    TFacadeRunOptions& GetRunOptions() {
        return RunOptions_;
    }

    void AddRemoteLayersFactory(std::function<std::pair<TString, NLayers::IRemoteLayerProviderPtr>()>&& factory) {
        RemoteLayersFactories_.emplace_back(std::move(factory));
    }

protected:
    virtual int DoMain(int argc, const char** argv);
    virtual int DoRun(TProgramFactory& factory);
    virtual TProgram::TStatus DoRunProgram(TProgramPtr program);

private:
    TString Name_;
    std::vector<std::function<NFS::IDownloaderPtr()>> FsDownloadFactories_;
    std::vector<std::function<TDataProviderInitializer()>> ProviderFactories_;
    std::vector<std::function<IUrlListerPtr()>> UrlListerFactories_;
    std::vector<std::function<std::pair<TString, NLayers::IRemoteLayerProviderPtr>()>> RemoteLayersFactories_;
    THashMap<TString, TString> ClusterMapping_;
    THolder<TFileStorageConfig> FileStorageConfig_;
    TFileStoragePtr FileStorage_;
    TIntrusivePtr<NKikimr::NMiniKQL::IFunctionRegistry> FuncRegistry_;
    TOperationProgressWriter ProgressWriter_;
    IPipelineConfigurator* OptPipelineConfigurator_ = nullptr;
    IPipelineConfigurator* PeepholePipelineConfigurator_ = nullptr;
    TFacadeRunOptions RunOptions_;
    std::unique_ptr<NYql::NLog::YqlLoggerScope> YqlLogger_;
};

} // namespace NYql
