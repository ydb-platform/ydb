#pragma once

#include <yql/essentials/core/file_storage/defs/downloader.h>
#include <yql/essentials/core/file_storage/file_storage.h>
#include <yql/essentials/core/credentials/yql_credentials.h>
#include <yql/essentials/core/url_lister/interface/url_lister.h>
#include <yql/essentials/core/yql_data_provider.h>
#include <yql/essentials/core/yql_user_data.h>

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
}

namespace NYql {
    class TFileStorageConfig;
    class TGatewaysConfig;
    class TProgramFactory;
}

namespace NYql::NProto {
    class TPgExtensions;
}

namespace NYqlMountConfig {
    class TMountConfig;
}

namespace NYql {

enum class ERunMode {
    Parse       /* "parse" */,
    Compile     /* "compile" */,
    Validate    /* "validate" */,
    Optimize    /* "optimize" */,
    Peephole    /* "peephole" */,
    Lineage     /* "lineage" */,
    Discover    /* "discover" */,
    Run         /* "run" */,
};

enum class EProgramType {
    SExpr   /* "s-expr" */,
    Sql     /* "sql" */,
    Pg      /* "pg" */,
};

class TFacadeRunOptions {
public:
    TFacadeRunOptions();
    ~TFacadeRunOptions();

    EProgramType ProgramType = EProgramType::SExpr;
    NYson::EYsonFormat ResultsFormat = NYson::EYsonFormat::Text;
    ERunMode Mode = ERunMode::Run;
    TString ProgramFile;
    TString ProgramText;
    TString User;
    ui64 MemLimit = 0;

    THashSet<TString> SqlFlags;
    ui16 SyntaxVersion = 1;
    bool AnsiLexer = false;
    bool TestAntlr4 = false;
    bool AssumeYdbOnClusterWithSlash = false;

    bool PrintAst = false;
    bool FullExpr = false;
    bool WithTypes = false;
    bool FullStatistics = false;
    int Verbosity = TLOG_ERR;
    bool ShowLog = false;
    bool WithFinalIssues = false;

    IOutputStream* TraceOptStream = nullptr;

    IOutputStream* ErrStream = &Cerr;
    THolder<IOutputStream> ErrStreamHolder;
    IOutputStream* PlanStream = nullptr;
    THolder<IOutputStream> PlanStreamHolder;
    IOutputStream* ExprStream = nullptr;
    THolder<IOutputStream> ExprStreamHolder;
    IOutputStream* ResultStream = nullptr;
    THolder<IOutputStream> ResultStreamHolder;
    IOutputStream* StatStream = nullptr;
    THolder<IOutputStream> StatStreamHolder;

    NYql::TUserDataTable DataTable;
    TVector<TString> UdfsPaths;
    TString Params;
    NUdf::EValidateMode ValidateMode = NUdf::EValidateMode::Greedy;

    THashSet<TString> GatewayTypes;
    TString UdfResolverPath;
    bool UdfResolverFilterSyscalls = false;
    bool ScanUdfs = false;
    THolder<NYqlMountConfig::TMountConfig> MountConfig;
    THolder<TGatewaysConfig> GatewaysConfig;
    THolder<TFileStorageConfig> FsConfig;
    THolder<NProto::TPgExtensions> PgExtConfig;

    // No command line options for these settings. Should be configured in the inherited class
    bool NoDebug = false;
    bool PgSupport = true;
    bool FailureInjectionSupport = false;
    bool UseRepeatableRandomAndTimeProviders = false;
    bool UseMetaFromGrpah = false;
    bool TestSqlFormat = false;
    bool ValidateResultFormat = false;
    bool EnableResultPosition = false;

    void Parse(int argc, const char *argv[]);

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

private:
    std::vector<std::function<void(NLastGetopt::TOpts&)>> OptExtenders_;
    std::vector<std::function<void(const NLastGetopt::TOptsParseResult&)>> OptHandlers_;
    THashSet<TString> SupportedGateways_;
};

class TFacadeRunner {
public:
    TFacadeRunner(TString name)
        : Name_(std::move(name))
    {
    }
    int Main(int argc, const char *argv[]);

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
        for (auto& cluster: config.GetClusterMapping()) {
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
    TIntrusivePtr<NKikimr::NMiniKQL::IFunctionRegistry> GetFuncRegistry() {
        return FuncRegistry_;
    }
    TCredentials::TPtr GetCredentials() {
        return Credentials_;
    }
    TFacadeRunOptions& GetRunOptions() {
        return RunOptions_;
    }

private:
    int DoMain(int argc, const char *argv[]);
    int RunProgram(TProgramFactory& factory);

private:
    TString Name_;
    std::vector<std::function<NFS::IDownloaderPtr()>> FsDownloadFactories_;
    std::vector<std::function<TDataProviderInitializer()>> ProviderFactories_;
    std::vector<std::function<IUrlListerPtr()>> UrlListerFactories_;
    THashMap<TString, TString> ClusterMapping_;
    THolder<TFileStorageConfig> FileStorageConfig_;
    TFileStoragePtr FileStorage_;
    TIntrusivePtr<NKikimr::NMiniKQL::IFunctionRegistry> FuncRegistry_;
    TCredentials::TPtr Credentials_ = MakeIntrusive<TCredentials>();
    TOperationProgressWriter ProgressWriter_;
    IPipelineConfigurator* OptPipelineConfigurator_ = nullptr;
    IPipelineConfigurator* PeepholePipelineConfigurator_ = nullptr;
    TFacadeRunOptions RunOptions_;
};

} // NYql
