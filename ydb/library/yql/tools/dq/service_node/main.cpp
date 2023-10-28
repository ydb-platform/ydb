#include <ydb/library/yql/providers/dq/metrics/metrics_printer.h>
#include <ydb/library/yql/providers/dq/actors/yt/resource_manager.h>
#include <ydb/library/yql/providers/dq/actors/dynamic_nameserver.h>
#include <ydb/library/yql/providers/dq/stats_collector/pool_stats_collector.h>
#include <ydb/library/yql/providers/dq/service/interconnect_helpers.h>
#include <ydb/library/yql/providers/dq/service/service_node.h>
#include <ydb/library/yql/providers/dq/global_worker_manager/coordination_helper.h>
#include <ydb/library/yql/providers/dq/worker_manager/interface/events.h>
#include <ydb/library/yql/providers/yt/dq_task_preprocessor/yql_yt_dq_task_preprocessor.h>
#include <ydb/library/yql/utils/log/proto/logger_config.pb.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/log/tls_backend.h>
#include <ydb/library/yql/utils/failure_injector/failure_injector.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/utils/backtrace/backtrace.h>

#include <yt/yt/core/misc/shutdown.h>
#include <yt/yt/client/api/client.h>

#include <library/cpp/getopt/small/last_getopt.h>
#include <library/cpp/threading/future/future.h>
#include <library/cpp/digest/md5/md5.h>


#if 0
#  include <yt/yt/core/logging/config.h>
#  include <yt/yt/core/logging/log_manager.h>
#endif

constexpr ui32 THREAD_PER_NODE = 16;

using namespace NYql;
using namespace NYql::NDqs;
using TFileResource = Yql::DqsProto::TFile;

static NThreading::TPromise<void> ShouldContinue = NThreading::NewPromise<void>();

static void OnTerminate(int) {
    ShouldContinue.TrySetValue();
}

// TODO: Merge with ydb/library/yql/providers/dq/provider/yql_dq_gateway.cpp#L28
THashMap<TString, TString> Md5Cache;

TString GetMd5(const TString& localPath) {
    if (Md5Cache.contains(localPath)) {
        return Md5Cache[localPath];
    } else {
        auto blob = ::TBlob::FromFile(localPath);
        TString digest;

        char buf[33] = {0};
        digest = MD5::Data(blob.Data(), blob.Size(), buf);
        Md5Cache[localPath] = digest;
        return digest;
    }
}

TVector<TFileResource> GetFiles(TString udfsPath, TString vanillaLitePath) {
    TVector<TFileResource> files;

    if (!vanillaLitePath.empty()) {
        TFileResource vanillaLite;
        vanillaLite.SetLocalPath(vanillaLitePath);
        vanillaLite.SetName(vanillaLitePath.substr(vanillaLitePath.rfind('/') + 1));
        vanillaLite.SetObjectType(Yql::DqsProto::TFile_EFileType_EEXE_FILE);
        vanillaLite.SetObjectId(GetProgramCommitId());
        files.push_back(vanillaLite);
    }

    if (!udfsPath.empty()) {
        TVector<TString> tmp;
        NKikimr::NMiniKQL::FindUdfsInDir(udfsPath, &tmp);
        for (const auto& f : tmp) {
            TFileResource r;
            r.SetLocalPath(f);
            r.SetObjectType(Yql::DqsProto::TFile_EFileType_EUDF_FILE);
            r.SetObjectId(GetMd5(f));
            files.push_back(r);
        }
    }
    return files;
}

/* crutch */
void* GetAppData() {
    return nullptr;
}
/* */

// Simple usage: ./service_node --id 1 --port 31337 --grpcport 8080
int main(int argc, char** argv) {
    using namespace NLastGetopt;

    auto loggerConfig = NYql::NProto::TLoggingConfig();

#if 0
    auto logManager = NYT::NLogging::TLogManager::Get();

    TString logConfig = " \
    { \
        \"rules\" = [ \
            { \
                \"min_level\" = \"debug\"; \
                \"writers\" = [ \
                    \"debug\"; \
                ]; \
                \"exclude_categories\" = [ \
                    \"Bus\"; \
                ]; \
            }; \
        ]; \
        \"writers\" = { \
            \"debug\" = { \
                \"type\" = \"stderr\"; \
            } \
        } \
    }";

    auto ytLogConfigNode = NYT::NYTree::ConvertTo<NYT::NYTree::INodePtr>(
        NYT::NYson::TYsonString(
            logConfig.Data(), logConfig.Size(), NYT::NYson::EYsonType::Node));
    auto logManagerConfig = NYT::New<NYT::NLogging::TLogManagerConfig>();
    logManagerConfig->Load(ytLogConfigNode);
    logManager->Configure(logManagerConfig);
#endif
    TOpts opts = TOpts::Default();
    opts.AddHelpOption();
    opts.AddLongOption("id", "Entry node for service");
    opts.AddLongOption("workers", "Worker actors per worker node");

    opts.AddLongOption("ytprefix", "Yt prefix");
    opts.AddLongOption("proxy", "Yt proxy");
    opts.AddLongOption("yttoken", "Yt token");
    opts.AddLongOption("ytuser", "Yt user");

    opts.AddLongOption("port", "Port");
    opts.AddLongOption("grpcport", "Grpc Port");
    opts.AddLongOption("mbusport", "Yql worker mbus port");

    opts.AddLongOption("remote_jobs", "Start YtRM with jobs");
    opts.AddLongOption("jobs_per_op", "Start YtRM with jobs");
    opts.AddLongOption("vanilla_job", "Vanilla job biary");

    opts.AddLongOption('u', "udfs", "UdfsPath");
    opts.AddLongOption("enabled_failure_injector", "Enabled failure injections").NoArgument();
    opts.AddLongOption("dump_stats", "Dump Statitics").NoArgument();

    opts.AddLongOption("revision", "Revision for debug");
    opts.AddLongOption("force_leader", "Disable leader election").NoArgument();
    opts.AddLongOption("log_level", "Log Level");
    opts.AddLongOption("ipv4", "Use ipv4").NoArgument();

    TOptsParseResult res(&opts, argc, argv);

    TString hostName, localAddress;

    ui16 interconnectPort = res.Get<ui16>("port");
    ui16 grpcPort = res.Get<ui16>("grpcport");
    ui16 mbusPort = res.GetOrElse<ui16>("mbusport", 0);

    auto logLevel = NYql::NProto::TLoggingConfig::INFO;
    if (res.Has("log_level")) {
        auto str = res.Get<TString>("log_level");
        if (str == "TRACE") {
            logLevel = NYql::NProto::TLoggingConfig::TRACE;
        }
    }

    loggerConfig.SetAllComponentsLevel(logLevel);
    NYql::NLog::InitLogger(loggerConfig, false);

    NProto::TDqConfig::TYtCoordinator coordinatorConfig;

    bool useYtCoordination = false;
    if (res.Has("proxy")) {
        useYtCoordination = true;
        coordinatorConfig.SetPrefix(res.Get<TString>("ytprefix"));
        coordinatorConfig.SetClusterName(res.Get<TString>("proxy"));
    }

    if (res.Has("yttoken")) {
        coordinatorConfig.SetToken(res.Get<TString>("yttoken"));
    }

    if (res.Has("ytuser")) {
        coordinatorConfig.SetUser(res.Get<TString>("ytuser"));
    }

    if (res.Has("enabled_failure_injector")) {
        YQL_LOG(INFO) << "Enabled failure injector";
        TFailureInjector::Activate();
    }

    if (res.Has("revision")) {
        coordinatorConfig.SetRevision(res.Get<TString>("revision"));
    }

    if (useYtCoordination == false || res.Has("force_leader")) {
        coordinatorConfig.SetLockType("dummy");
    }

    std::tie(hostName, localAddress) = NYql::NDqs::GetLocalAddress(
        coordinatorConfig.HasHostName() ? &coordinatorConfig.GetHostName() : nullptr,
        res.Has("ipv4") ? AF_INET : AF_INET6
    );

    auto coordinator = CreateCoordiantionHelper(coordinatorConfig, NProto::TDqConfig::TScheduler(), "service_node", interconnectPort, hostName, localAddress);

    Cerr << hostName + ":" + ToString(localAddress) << Endl;

    TMaybe<ui32> maybeNodeId;
    if (useYtCoordination == false && !res.Has("id")) {
        Cerr << "--id required!\n"; return -1;
    }
    if (res.Has("id")) {
        maybeNodeId = res.Get<ui32>("id");
    }
    auto nodeId = coordinator->GetNodeId(
        maybeNodeId,
        {ToString(grpcPort)},
        static_cast<ui32>(NDqs::ENodeIdLimits::MinServiceNodeId),
        static_cast<ui32>(NDqs::ENodeIdLimits::MinServiceNodeId)+200,
        {}
    );

    Cerr << "My nodeId: " << nodeId << Endl;

    TLocalProcessKeyState<NActors::TActorActivityTag>& key = TLocalProcessKeyState<NActors::TActorActivityTag>::GetInstance();
    Cerr << "ActorNames: " << key.GetCount() << Endl;
    for (ui64 i = 0; i < key.GetCount(); i++) {
        auto name = key.GetNameByIndex(i);
        if (name && !name.StartsWith("Activity_")) {
            Cerr << "  " << name << Endl;
        }
    }

    NYql::NDqs::TServiceNodeConfig config;
    config.NodeId = nodeId;
    config.InterconnectAddress = localAddress;
    config.GrpcHostname = hostName;
    config.Port = interconnectPort;
    config.GrpcPort = grpcPort;
    config.MbusPort = mbusPort;
    config.NameserverFactory = [](const TIntrusivePtr<NActors::TTableNameserverSetup>& setup) {
        return NYql::NDqs::CreateDynamicNameserver(setup);
    };

    YQL_LOG(INFO) << "Interconnect addr/port " << config.InterconnectAddress << ":" << config.Port;
    YQL_LOG(INFO) << "GRPC addr/port " << config.GrpcHostname << ":" << config.GrpcPort;
    YQL_LOG(INFO) << "MBus port " << config.MbusPort;

    auto metricsRegistry = CreateMetricsRegistry(GetSensorsGroupFor(NSensorComponent::kDq));
    auto serviceNode = TServiceNode(config, THREAD_PER_NODE, metricsRegistry);

    auto statsCollector = CreateStatsCollector(5, *serviceNode.GetSetup(), metricsRegistry->GetSensors());

    auto* actorSystem = serviceNode.StartActorSystem(GetAppData());

    // push metrics from root group
    auto  metricsPusherId = NActors::TActorId() ;// actorSystem->Register(CreateMetricsPusher(CreateMetricsRegistry(GetSensorsRootGroup()),mbusPort));

    if (res.Has("dump_stats")) {
        metricsPusherId = actorSystem->Register(CreateMetricsPrinter(metricsRegistry->GetSensors()));
        actorSystem->Register(statsCollector);
    }

    if (!maybeNodeId) {
        coordinator->StartRegistrator(actorSystem);
        coordinator->StartCleaner(actorSystem, {});
    } else {
        // just create yt wrapper
        if (useYtCoordination) {
            (void)coordinator->GetWrapper(actorSystem);
        }
    }

    auto funcRegistry = CreateFunctionRegistry(&NYql::NBacktrace::KikimrBackTrace, NKikimr::NMiniKQL::CreateBuiltinRegistry(), false, TVector<TString>());
    TDqTaskPreprocessorFactoryCollection dqTaskPreprocessorFactories = {
        NDq::CreateYtDqTaskPreprocessorFactory(false, funcRegistry)
    };
    serviceNode.StartService(dqTaskPreprocessorFactories);

    TVector<NActors::TActorId> ids;
    TVector<TResourceManagerOptions> uploadResourcesOptions;
    int jobs = res.GetOrElse<int>("remote_jobs", 0);
    int jobsPerOperation = res.GetOrElse<int>("jobs_per_op", 2);
    if (useYtCoordination) {
        coordinatorConfig = coordinator->GetConfig();
        TResourceManagerOptions options;
        options.YtBackend.SetClusterName(coordinatorConfig.GetClusterName());
        options.YtBackend.SetUser(coordinatorConfig.GetUser());
        options.YtBackend.SetToken(coordinatorConfig.GetToken());
        options.YtBackend.SetMemoryLimit(16000000000LL);
        options.YtBackend.SetPrefix(coordinatorConfig.GetPrefix());
        options.YtBackend.SetUploadPrefix(coordinatorConfig.GetPrefix());
        options.YtBackend.SetMinNodeId(static_cast<ui32>(NDqs::ENodeIdLimits::MinWorkerNodeId));
        options.YtBackend.SetMaxNodeId(static_cast<ui32>(NDqs::ENodeIdLimits::MinWorkerNodeId)+100);


        options.YtBackend.SetMaxJobs(jobs);
        if (jobsPerOperation > 0) {
            options.YtBackend.SetJobsPerOperation(jobsPerOperation);
        }

        if (jobs > 0) {
            TResourceFile vanilla(res.Get<TString>("vanilla_job"));
            vanilla.RemoteFileName = "bin/" + ToString(GetProgramCommitId()) + "/" + vanilla.LocalFileName.substr(vanilla.LocalFileName.rfind('/')+1);
            options.Files.push_back(vanilla);
            for (const auto& r : GetFiles(res.GetOrElse("udfs", ""), res.Get<TString>("vanilla_job") + ".lite")) {
                if (r.GetObjectType() == Yql::DqsProto::TFile::EEXE_FILE) {
                    TResourceFile f(r.GetLocalPath());
                    f.RemoteFileName = "bin/" + r.GetObjectId() + "/" + r.GetName();
                    options.Files.push_back(f);
                } else {
                    TResourceFile f(r.GetLocalPath());
                    f.RemoteFileName = "udfs/" + r.GetObjectId();
                    options.Files.push_back(f);
                }
            }

            // uploader
            options.UploadPrefix = options.YtBackend.GetUploadPrefix();
            options.LockName = TString("ytuploader.") + options.YtBackend.GetClusterName();
            // don't start uploader for local-yt
            if (options.YtBackend.GetClusterName().find("localhost") != 0) {
                ids.push_back(actorSystem->Register(CreateResourceUploader(options, coordinator)));
            }
            {
                // bin cleaner
                options.KeepFirst = 5;
                options.UploadPrefix = options.YtBackend.GetUploadPrefix() + "/bin";
                ids.push_back(actorSystem->Register(CreateResourceCleaner(options, coordinator)));
            }
            {
                // udf cleaner
                options.KeepFirst = 500;
                options.DropBefore = TDuration::Days(7);
                options.UploadPrefix = options.YtBackend.GetUploadPrefix() + "/udfs";
                ids.push_back(actorSystem->Register(CreateResourceCleaner(options, coordinator)));
            }
            {
                // temporary locks
                options.KeepFilter = options.YtBackend.GetClusterName(); // don't remove locks with `ClusterName` in LockName
                options.DropBefore = TDuration::Hours(1);
                options.UploadPrefix = options.YtBackend.GetPrefix() + "/locks";
                ids.push_back(actorSystem->Register(CreateResourceCleaner(options, coordinator)));
            }

            // rm manager
            options.Files.clear();
            vanilla.RemoteFileName = vanilla.LocalFileName.substr(vanilla.LocalFileName.rfind('/')+1);
            options.Files.push_back(vanilla);
            options.UploadPrefix = options.YtBackend.GetUploadPrefix() + "/bin/" + ToString(GetProgramCommitId());
            options.LockName = TString("ytrm.") + options.YtBackend.GetClusterName();
            options.Counters = metricsRegistry->GetSensors()->GetSubgroup("counters", "ytrm");
            ids.push_back(actorSystem->Register(CreateResourceManager(options, coordinator)));
        }

        options.UploadPrefix = options.YtBackend.GetUploadPrefix();
        options.Files.clear();
        uploadResourcesOptions.push_back(options);
    }

    coordinator->StartGlobalWorker(actorSystem, uploadResourcesOptions, metricsRegistry);

    signal(SIGINT, &OnTerminate);
    signal(SIGTERM, &OnTerminate);

    auto future = ShouldContinue.GetFuture();
    future.Wait();

    for (auto id : ids) {
        actorSystem->Send(id, new NActors::TEvents::TEvPoison);
    }
    actorSystem->Send(NDqs::MakeWorkerManagerActorID(nodeId), new NActors::TEvents::TEvPoison);
    actorSystem->Send(metricsPusherId, new NActors::TEvents::TEvPoison);

    coordinator->Stop(actorSystem);

    // TODO: remove this
    Sleep(TDuration::Seconds(5));

    serviceNode.Stop();
    NYT::Shutdown();

    return 0;
}
