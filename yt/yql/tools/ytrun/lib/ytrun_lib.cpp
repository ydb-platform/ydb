#include "ytrun_lib.h"

#include <yt/yql/providers/yt/provider/yql_yt_provider_impl.h>
#include <yt/yql/providers/yt/provider/yql_yt_provider.h>
#include <yt/yql/providers/yt/lib/config_clusters/config_clusters.h>
#include <yt/yql/providers/yt/lib/yt_download/yt_download.h>
#include <yt/yql/providers/yt/lib/yt_url_lister/yt_url_lister.h>
#include <yt/yql/providers/yt/lib/log/yt_logger.h>
#include <yt/yql/providers/yt/gateway/native/yql_yt_native.h>
#include <yt/yql/providers/yt/gateway/fmr/yql_yt_fmr.h>
#include <yt/yql/providers/yt/fmr/coordinator/client/yql_yt_coordinator_client.h>
#include <yt/yql/providers/yt/fmr/coordinator/impl/yql_yt_coordinator_impl.h>
#include <yt/yql/providers/yt/fmr/job_factory/impl/yql_yt_job_factory_impl.h>
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

void FlushYtDebugLogOnSignal() {
    if (!NMalloc::IsAllocatorCorrupted) {
        NYql::FlushYtDebugLog();
    }
}

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
        opts.AddLongOption("threads", "gateway threads")
            .Optional()
            .RequiredArgument("COUNT")
            .StoreResult(&NumThreads_);
        opts.AddLongOption("keep-temp", "keep temporary tables")
            .Optional()
            .NoArgument()
            .SetFlag(&KeepTemp_);
        opts.AddLongOption("use-graph-meta", "Use tables metadata from graph")
            .Optional()
            .NoArgument()
            .SetFlag(&GetRunOptions().UseMetaFromGrpah);
        opts.AddLongOption("fmr-coordinator-server-url", "Fast map reduce coordinator server url")
            .Optional()
            .StoreResult(&FmrCoordinatorServerUrl_);
        opts.AddLongOption("disable-local-fmr-worker", "Disable local fast map reduce worker")
            .Optional()
            .NoArgument()
            .SetFlag(&DisableLocalFmrWorker_);
    });

    GetRunOptions().AddOptHandler([this](const NLastGetopt::TOptsParseResult& res) {
        Y_UNUSED(res);

        if (!GetRunOptions().GatewaysConfig) {
            GetRunOptions().GatewaysConfig = MakeHolder<TGatewaysConfig>();
        }

        auto ytConfig = GetRunOptions().GatewaysConfig->MutableYt();
        ytConfig->SetGatewayThreads(NumThreads_);
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
    });

    GetRunOptions().SetSupportedGateways({TString{YtProviderName}});
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
    auto ytGateway = CreateYtNativeGateway(services);
    if (!GetRunOptions().GatewayTypes.contains(FastMapReduceGatewayName)) {
        return ytGateway;
    }

    auto coordinator = NFmr::MakeFmrCoordinator();
    if (!FmrCoordinatorServerUrl_.empty()) {
        NFmr::TFmrCoordinatorClientSettings coordinatorClientSettings;
        THttpURL parsedUrl;
        if (parsedUrl.Parse(FmrCoordinatorServerUrl_) != THttpURL::ParsedOK) {
            ythrow yexception() << "Invalid fast map reduce coordinator server url passed in parameters";
        }
        coordinatorClientSettings.Port = parsedUrl.GetPort();
        coordinatorClientSettings.Host = parsedUrl.GetHost();
        coordinator = NFmr::MakeFmrCoordinatorClient(coordinatorClientSettings);
    }

    if (!DisableLocalFmrWorker_) {
        auto func = [&] (NFmr::TTask::TPtr /*task*/, std::shared_ptr<std::atomic<bool>> cancelFlag) {
            while (!cancelFlag->load()) {
                Sleep(TDuration::Seconds(3));
                return NFmr::ETaskStatus::Completed;
            }
            return NFmr::ETaskStatus::Aborted;
        }; // TODO - use function which actually calls Downloader/Uploader based on task params

        NFmr::TFmrJobFactorySettings settings{.Function=func};
        auto jobFactory = MakeFmrJobFactory(settings);
        NFmr::TFmrWorkerSettings workerSettings{.WorkerId = 0, .RandomProvider = CreateDefaultRandomProvider(),
            .TimeToSleepBetweenRequests=TDuration::Seconds(1)};
        FmrWorker_ = MakeFmrWorker(coordinator, jobFactory, workerSettings);
        FmrWorker_->Start();
    }
    return NFmr::CreateYtFmrGateway(ytGateway, coordinator);
}

IOptimizerFactory::TPtr TYtRunTool::CreateCboFactory() {
    return MakeSimpleCBOOptimizerFactory();
}

IDqHelper::TPtr TYtRunTool::CreateDqHelper() {
    return {};
}

int TYtRunTool::DoMain(int argc, const char *argv[]) {
    // Init MR/YT for proper work of embedded agent
    NYT::Initialize(argc, argv);

    NYql::NBacktrace::AddAfterFatalCallback([](int){ FlushYtDebugLogOnSignal(); });
    NYql::SetYtLoggerGlobalBackend(LOG_DEF_PRIORITY);

    if (NYT::TConfig::Get()->Prefix.empty()) {
        NYT::TConfig::Get()->Prefix = "//";
    }

    int res = TFacadeRunner::DoMain(argc, argv);
    if (0 == res) {
        NYql::DropYtDebugLog();
    }
    return res;
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
