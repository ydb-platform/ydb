#include "ytrun_lib.h"

#include <util/system/env.h>
#include <yt/yql/providers/yt/provider/yql_yt_provider_impl.h>
#include <yt/yql/providers/yt/provider/yql_yt_provider.h>
#include <yt/yql/providers/yt/lib/config_clusters/config_clusters.h>
#include <yt/yql/providers/yt/lib/yt_download/yt_download.h>
#include <yt/yql/providers/yt/lib/yt_url_lister/yt_url_lister.h>
#include <yt/yql/providers/yt/lib/log/yt_logger.h>
#include <yt/yql/providers/yt/lib/secret_masker/dummy/dummy_secret_masker.h>
#include <yt/yql/providers/yt/gateway/native/yql_yt_native.h>
#include <yt/yql/providers/yt/gateway/fmr/yql_yt_fmr.h>
#include <yt/yql/providers/yt/fmr/fmr_tool_lib/yql_yt_fmr_initializer.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/core/peephole_opt/yql_opt_peephole_physical.h>
#include <yql/essentials/core/services/yql_transform_pipeline.h>
#include <yql/essentials/core/cbo/simple/cbo_simple.h>
#include <yql/essentials/utils/backtrace/backtrace.h>

#include <yt/cpp/mapreduce/client/init.h>
#include <yt/cpp/mapreduce/interface/config.h>

#include <library/cpp/digest/md5/md5.h>
#include <library/cpp/malloc/api/malloc.h>
#include <library/cpp/sighandler/async_signals_handler.h>

#include <util/folder/path.h>
#include <util/stream/file.h>

namespace {

class TPeepHolePipelineConfigurator : public NYql::IPipelineConfigurator {
public:
    TPeepHolePipelineConfigurator() = default;

    void AfterCreate(NYql::TTransformationPipeline* pipeline) const final {
        Y_UNUSED(pipeline);
    }

    void AfterTypeAnnotation(NYql::TTransformationPipeline* pipeline) const final {
        Y_UNUSED(pipeline);
    }

    void AfterOptimize(NYql::TTransformationPipeline* pipeline) const final {
        pipeline->Add(NYql::CreateYtWideFlowTransformer(nullptr), "WideFlow");
        pipeline->Add(NYql::CreateYtBlockInputTransformer(nullptr), "BlockInput");
        pipeline->Add(NYql::MakePeepholeOptimization(pipeline->GetTypeAnnotationContext()), "PeepHole");
        pipeline->Add(NYql::CreateYtBlockOutputTransformer(nullptr), "BlockOutput");
    }
};

TPeepHolePipelineConfigurator PEEPHOLE_CONFIG_INSTANCE;

} // unnamed

namespace NYql {

TYtRunTool::TYtRunTool(TString name)
    : TFacadeRunner(std::move(name))
{
    GetRunOptions().EnableResultPosition = true;
    GetRunOptions().EnableCredentials = true;
    GetRunOptions().EnableQPlayer = true;
    GetRunOptions().ResultStream = &Cout;

    GetRunOptions().AddOptExtension([this](NLastGetopt::TOpts& opts) {
        opts.AddLongOption("user", "MR user")
            .Optional()
            .RequiredArgument("USER")
            .StoreResult(&GetRunOptions().User);

        opts.AddLongOption("mrjob-bin", "Path to mrjob binary")
            .Optional()
            .StoreResult(&MrJobBin_);
        opts.AddLongOption("mrjob-udfsdir", "Path to udfs for mr jobs")
            .Optional()
            .StoreResult(&MrJobUdfsDir_);
        opts.AddLongOption("show-progress", "Report operation progress").NoArgument()
            .Handler0([&]() {
                SetOperationProgressWriter([](const TOperationProgress& progress) {
                    TStringBuilder remoteId;
                    if (progress.RemoteId) {
                        remoteId << ", remoteId: " << progress.RemoteId;
                    }
                    TStringBuilder counters;
                    if (progress.Counters) {
                        if (progress.Counters->Running) {
                            counters << ' ' << progress.Counters->Running;
                        }
                        if (progress.Counters->Total) {
                            counters << TStringBuf(" (") << (100ul * progress.Counters->Completed / progress.Counters->Total) << TStringBuf("%)");
                        }
                    }
                    Cerr << "Operation: [" << progress.Category << "] " << progress.Id
                        << ", state: " << progress.State << remoteId << counters
                        << ", current stage: " << progress.Stage.first << Endl;
                });
            });
        opts.AddLongOption("yt-threads", "YT gateway threads")
            .Optional()
            .RequiredArgument("COUNT")
            .StoreResult(&NumYtThreads_);
        opts.AddLongOption("keep-temp", "keep temporary tables")
            .Optional()
            .NoArgument()
            .SetFlag(&KeepTemp_);
        opts.AddLongOption("use-graph-meta", "Use tables metadata from graph")
            .Optional()
            .NoArgument()
            .SetFlag(&GetRunOptions().UseMetaFromGrpah);
        opts.AddLongOption("disable-local-fmr-worker", "Disable local fast map reduce worker")
            .Optional()
            .NoArgument()
            .SetFlag(&DisableLocalFmrWorker_);

        opts.AddLongOption( "fmr-operation-spec-path", "Path to file with fmr operation spec settings")
            .Optional()
            .StoreResult(&FmrOperationSpecFilePath_);
        opts.AddLongOption( "table-data-service-discovery-file-path", "Table data service discovery file path")
            .Optional()
            .StoreResult(&TableDataServiceDiscoveryFilePath_);
        opts.AddLongOption( "fmrjob-bin", "Path to fmrjob binary")
            .Optional()
            .StoreResult(&FmrJobBin_);
        opts.AddLongOption( "fmr-pool-name", "Fmr pool name")
            .Optional()
            .StoreResult(&FmrPoolName_);
    });

    GetRunOptions().AddOptHandler([this](const NLastGetopt::TOptsParseResult& res) {
        Y_UNUSED(res);

        if (!GetRunOptions().GatewaysConfig) {
            GetRunOptions().GatewaysConfig = MakeHolder<TGatewaysConfig>();
        }

        auto ytConfig = GetRunOptions().GatewaysConfig->MutableYt();
        ytConfig->SetGatewayThreads(NumYtThreads_);
        if (MrJobBin_.empty()) {
            ytConfig->ClearMrJobBin();
        } else {
            ytConfig->SetMrJobBin(MrJobBin_);
            ytConfig->SetMrJobBinMd5(MD5::File(MrJobBin_));
        }

        if (MrJobUdfsDir_.empty()) {
            ytConfig->ClearMrJobUdfsDir();
        } else {
            ytConfig->SetMrJobUdfsDir(MrJobUdfsDir_);
        }
        auto attr = ytConfig->MutableDefaultSettings()->Add();
        attr->SetName("KeepTempTables");
        attr->SetValue(KeepTemp_ ? "yes" : "no");

        FillClusterMapping(*ytConfig, TString{YtProviderName});

        DefYtServer_ = NYql::TConfigClusters::GetDefaultYtServer(*ytConfig);

        if (GetRunOptions().GatewayTypes.contains(NFmr::FastMapReduceGatewayName)) {
            GetRunOptions().GatewayTypes.emplace(YtProviderName);
        }
    });

    GetRunOptions().SetSupportedGateways({TString{YtProviderName}, TString{NFmr::FastMapReduceGatewayName}});
    GetRunOptions().GatewayTypes.emplace(YtProviderName);

    AddFsDownloadFactory([this]() -> NFS::IDownloaderPtr {
        return MakeYtDownloader(*GetRunOptions().FsConfig, DefYtServer_);
    });

    AddUrlListerFactory([]() -> IUrlListerPtr {
        return MakeYtUrlLister();
    });

    AddProviderFactory([this]() -> NYql::TDataProviderInitializer {
        if (GetRunOptions().GatewayTypes.contains(YtProviderName) && GetRunOptions().GatewaysConfig->HasYt()) {
            return GetYtNativeDataProviderInitializer(CreateYtGateway(), CreateCboFactory(), CreateDqHelper());
        }
        return {};
    });

    SetPeepholePipelineConfigurator(&PEEPHOLE_CONFIG_INSTANCE);
}

IYtGateway::TPtr TYtRunTool::CreateYtGateway() {
    TYtNativeServices services;
    services.FunctionRegistry = GetFuncRegistry().Get();
    services.FileStorage = GetFileStorage();
    services.Config = std::make_shared<TYtGatewayConfig>(GetRunOptions().GatewaysConfig->GetYt());
    services.SecretMasker = CreateSecretMasker();
    auto ytGateway = CreateYtNativeGateway(services);
    if (!GetRunOptions().GatewayTypes.contains(NFmr::FastMapReduceGatewayName)) {
        return ytGateway;
    }

    bool fmrConfigurationFound = false;
    NFmr::TFmrInitializationOptions fmrInitializationOpts;

    if (FmrPoolName_.empty()) {
        throw yexception() << "Pool should be specified for fmr gateway";
    }

    for (const auto& fmrConfiguration: GetRunOptions().GatewaysConfig->GetFmr().GetFmrConfigurations()) {
        if (fmrConfiguration.GetName() == FmrPoolName_) {
            fmrConfigurationFound = true;
            fmrInitializationOpts = NFmr::GetFmrInitializationInfoFromConfig(fmrConfiguration, GetRunOptions().GatewaysConfig->GetFmr().GetFileCacheConfigurations());
            break;
        }
    }

    if (!fmrConfigurationFound) {
        throw yexception() << "Fmr configuration was not found for pool " << FmrPoolName_;
    }

    NFmr::TFmrServices fmrServices;
    fmrServices.FunctionRegistry = GetFuncRegistry().Get();
    fmrServices.FileStorage = GetFileStorage();
    fmrServices.Config = std::make_shared<TYtGatewayConfig>(GetRunOptions().GatewaysConfig->GetYt());
    fmrServices.DisableLocalFmrWorker = DisableLocalFmrWorker_;
    fmrServices.CoordinatorServerUrl = *fmrInitializationOpts.FmrCoordinatorUrl;
    fmrServices.TableDataServiceDiscoveryFilePath = TableDataServiceDiscoveryFilePath_;
    fmrServices.YtJobService = NFmr::MakeYtJobSerivce();
    fmrServices.YtCoordinatorService = NFmr::MakeYtCoordinatorService();
    fmrServices.FmrOperationSpecFilePath = FmrOperationSpecFilePath_;
    fmrServices.JobLauncher = MakeIntrusive<NFmr::TFmrUserJobLauncher>(NFmr::TFmrUserJobLauncherOptions{
        .RunInSeparateProcess = true,
        .FmrJobBinaryPath = FmrJobBin_,
        .TableDataServiceDiscoveryFilePath = TableDataServiceDiscoveryFilePath_,
        .GatewayType = "native"
    });

    fmrServices.FileUploadService = fmrInitializationOpts.FmrFileUploadService;
    fmrServices.FileMetadataService = fmrInitializationOpts.FmrFileMetadataService;
    fmrServices.TvmSettings = fmrInitializationOpts.FmrTvmSettings;

    if (!DisableLocalFmrWorker_) {
        auto jobPreparer = NFmr::MakeFmrJobPreparer(GetFileStorage(), TableDataServiceDiscoveryFilePath_);
        auto fmrDistCacheSettings = fmrInitializationOpts.FmrDistributedCacheSettings;
        TString distFileCacheBaseUrl = "yt://" + fmrDistCacheSettings.YtServerName + "/" + fmrDistCacheSettings.Path;
        jobPreparer->InitalizeDistributedCache(distFileCacheBaseUrl, fmrDistCacheSettings.YtToken);

        fmrServices.JobPreparer = jobPreparer;
    }

    auto [fmrGateway, worker] = NFmr::InitializeFmrGateway(ytGateway, MakeIntrusive<NFmr::TFmrServices>(fmrServices));
    FmrWorker_ = std::move(worker);
    return fmrGateway;
}

IOptimizerFactory::TPtr TYtRunTool::CreateCboFactory() {
    return MakeSimpleCBOOptimizerFactory();
}

IDqHelper::TPtr TYtRunTool::CreateDqHelper() {
    return {};
}

ISecretMasker::TPtr TYtRunTool::CreateSecretMasker() {
    return CreateDummySecretMasker();
}

int TYtRunTool::DoMain(int argc, const char *argv[]) {
    // Init MR/YT for proper work of embedded agent
    NYT::Initialize(argc, argv);

    if (NYT::TConfig::Get()->Prefix.empty()) {
        NYT::TConfig::Get()->Prefix = "//";
    }

    return TFacadeRunner::DoMain(argc, argv);
}

TProgram::TStatus TYtRunTool::DoRunProgram(TProgramPtr program) {
    auto sigHandler = [program](int) {
        Cerr << "Aborting..." << Endl;
        try {
            program->Abort().GetValueSync();
        } catch (...) {
            Cerr << CurrentExceptionMessage();
        }
    };
    SetAsyncSignalFunction(SIGINT, sigHandler);
    SetAsyncSignalFunction(SIGTERM, sigHandler);

    TProgram::TStatus status = TFacadeRunner::DoRunProgram(program);

    auto dummySigHandler = [](int) { };
    SetAsyncSignalFunction(SIGINT, dummySigHandler);
    SetAsyncSignalFunction(SIGTERM, dummySigHandler);

    return status;
}

} // NYql
