#include "dq_manager.h"

#include <ydb/library/yql/providers/dq/actors/yt/resource_manager.h>
#include <ydb/library/yql/providers/dq/actors/dynamic_nameserver.h>
#include <ydb/library/yql/providers/dq/metrics/metrics_printer.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_control.h>
#include <ydb/library/yql/providers/dq/service/interconnect_helpers.h>
#include <ydb/library/yql/providers/dq/stats_collector/pool_stats_collector.h>
#include <ydb/library/yql/providers/dq/worker_manager/interface/events.h>

#include <ydb/library/yql/providers/yt/dq_task_preprocessor/yql_yt_dq_task_preprocessor.h>

#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>

#include <ydb/library/yql/utils/backtrace/backtrace.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/log/proto/logger_config.pb.h>

#include <library/cpp/digest/md5/md5.h>

namespace NYT::NYqlPlugin {

using namespace NYql;
using namespace NYql::NDqs;

//////////////////////////////////////////////////////////////////////////////

void TDqManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("interconnect_port", &TThis::InterconnectPort)
        .GreaterThan(0);
    registrar.Parameter("grpc_port", &TThis::GrpcPort)
        .GreaterThan(0);
    registrar.Parameter("actor_threads", &TThis::ActorThreads)
        .GreaterThan(0);
    registrar.Parameter("use_ipv4", &TThis::UseIPv4)
        .Default(false);
    registrar.Parameter("address_resolver", &TThis::AddressResolver)
        .Default();

    registrar.Parameter("yt_backends", &TThis::YtBackends)
        .NonEmpty();

    registrar.Parameter("yt_coordinator", &TThis::YtCoordinator)
        .Default();
    registrar.Parameter("scheduler", &TThis::Scheduler)
        .Default();
    registrar.Parameter("interconnect_settings", &TThis::ICSettings)
        .Default();
}

//////////////////////////////////////////////////////////////////////////////

TDqManager::TDqManager(const TDqManagerConfigPtr& config)
    : Config_(config)
{ }

void TDqManager::Start()
{
    NYson::TProtobufWriterOptions protobufWriterOptions;
    protobufWriterOptions.ConvertSnakeToCamelCase = true;

    if (!Config_->YtCoordinator) {
        YQL_LOG(FATAL) << "YtCoordinator config is not specified";
        exit(1);
    }
    NYql::NProto::TDqConfig::TYtCoordinator coordinatorConfig;
    coordinatorConfig.ParseFromStringOrThrow(NYson::YsonStringToProto(
        ConvertToYsonString(Config_->YtCoordinator),
        NYson::ReflectProtobufMessageType<NYql::NProto::TDqConfig::TYtCoordinator>(),
        protobufWriterOptions));

    auto threads = Config_->ActorThreads;
    auto interconnectPort = Config_->InterconnectPort;
    auto grpcPort = Config_->GrpcPort;
    if (interconnectPort == grpcPort) {
        YQL_LOG(FATAL) << "Interconnect and grpc ports must be different";
        exit(1);
    }

    TString hostName, localAddress;
    std::tie(hostName, localAddress) = NYql::NDqs::GetLocalAddress(
        coordinatorConfig.HasHostName() ? &coordinatorConfig.GetHostName() : nullptr,
        Config_->UseIPv4 ? AF_INET : AF_INET6
    );
    YQL_LOG(INFO) << hostName + ":" + ToString(localAddress) << Endl;

    NYql::NProto::TDqConfig::TScheduler schedulerConfig;
    if (Config_->Scheduler) {
        schedulerConfig.ParseFromStringOrThrow(NYson::YsonStringToProto(
            ConvertToYsonString(Config_->Scheduler),
            NYson::ReflectProtobufMessageType<NYql::NProto::TDqConfig::TScheduler>(),
            protobufWriterOptions));
    }

    if (!coordinatorConfig.HasToken()) {
        if (!coordinatorConfig.HasTokenFile()) {
            YQL_LOG(FATAL) << "Either token or token file must be specified in the Coordinator config";
            exit(1);
        }

        TFsPath path(coordinatorConfig.GetTokenFile());
        auto token = TIFStream(path).ReadAll();
        coordinatorConfig.SetToken(token);
    }
    auto Coordinator_ = CreateCoordiantionHelper(coordinatorConfig, schedulerConfig, "service_node", interconnectPort, hostName, localAddress);

    auto nodeId = Coordinator_->GetNodeId(
        Nothing(),
        {ToString(grpcPort)},
        static_cast<ui32>(NDqs::ENodeIdLimits::MinServiceNodeId),
        static_cast<ui32>(NDqs::ENodeIdLimits::MaxServiceNodeId),
        {}
    );
    YQL_LOG(INFO) << "DQ manager nodeId: " << nodeId << Endl;

    NYql::NDqs::TServiceNodeConfig serviceNodeConfig;
    serviceNodeConfig.NodeId = nodeId;
    serviceNodeConfig.InterconnectAddress = localAddress;
    serviceNodeConfig.GrpcHostname = hostName;
    serviceNodeConfig.Port = interconnectPort;
    serviceNodeConfig.GrpcPort = grpcPort;

    NYql::NProto::TDqConfig::TICSettings iCSettings;
    if (Config_->ICSettings) {
        iCSettings.ParseFromStringOrThrow(NYson::YsonStringToProto(
            ConvertToYsonString(Config_->ICSettings),
            NYson::ReflectProtobufMessageType<NYql::NProto::TDqConfig::TICSettings >(),
            protobufWriterOptions));
    }

    serviceNodeConfig.ICSettings = iCSettings;
    serviceNodeConfig.NameserverFactory = [](const auto setup) {
        return NYql::NDqs::CreateDynamicNameserver(setup);
    };

    YQL_LOG(INFO) << "Interconnect addr/port " << serviceNodeConfig.InterconnectAddress << ":" << serviceNodeConfig.Port;
    YQL_LOG(INFO) << "GRPC addr/port " << serviceNodeConfig.GrpcHostname << ":" << serviceNodeConfig.GrpcPort;

    MetricsRegistry_ = CreateMetricsRegistry(GetSensorsGroupFor(NSensorComponent::kDq));
    ServiceNode_ = MakeHolder<TServiceNode>(serviceNodeConfig, threads, MetricsRegistry_);

    StatsCollector_ = CreateStatsCollector(5, *ServiceNode_->GetSetup(), MetricsRegistry_->GetSensors());
    ActorSystem_ = ServiceNode_->StartActorSystem();
    ActorSystem_->Register(StatsCollector_);
    Coordinator_->StartRegistrator(ActorSystem_);
    Coordinator_->StartCleaner(ActorSystem_, {});

    auto funcRegistry = CreateFunctionRegistry(&NYql::NBacktrace::KikimrBackTrace, NKikimr::NMiniKQL::CreateBuiltinRegistry(), false, TVector<TString>());
    TDqTaskPreprocessorFactoryCollection dqTaskPreprocessorFactories = {
        NDq::CreateYtDqTaskPreprocessorFactory(false, funcRegistry)
    };
    ServiceNode_->StartService(dqTaskPreprocessorFactories);

    TVector<NActors::TActorId> ytRMs;
    ytRMs.reserve(Config_->YtBackends.size());

    TVector<TResourceManagerOptions> uploadResourcesOptions;
    auto nodesPerCluster = static_cast<ui32>(NDqs::ENodeIdLimits::MaxWorkerNodeId) - static_cast<ui32>(NDqs::ENodeIdLimits::MinWorkerNodeId);
    nodesPerCluster /= Config_->YtBackends.size();
    auto startNodeId = static_cast<ui32>(NDqs::ENodeIdLimits::MinWorkerNodeId);

    for (const auto& ytBackendConfig : Config_->YtBackends) {
        TResourceManagerOptions rmOptions;
        rmOptions.YtBackend.ParseFromStringOrThrow(NYson::YsonStringToProto(
            ConvertToYsonString(ytBackendConfig),
            NYson::ReflectProtobufMessageType<NYql::NProto::TDqConfig::TYtBackend>(),
            protobufWriterOptions));

        if (!rmOptions.YtBackend.HasICSettings()) {
            *rmOptions.YtBackend.MutableICSettings() = iCSettings;
        }

        if (!rmOptions.YtBackend.HasPrefix()) {
            YQL_LOG(FATAL) << "At least one of YtBackends does not have a prefix specified";
            exit(1);
        }
        if (!rmOptions.YtBackend.HasUploadPrefix()) {
            rmOptions.YtBackend.SetUploadPrefix(rmOptions.YtBackend.GetPrefix() + "/tmp");
        }
        if (!rmOptions.YtBackend.HasMinNodeId() || !rmOptions.YtBackend.HasMaxNodeId()) {
            rmOptions.YtBackend.SetMinNodeId(startNodeId);
            rmOptions.YtBackend.SetMaxNodeId(startNodeId + nodesPerCluster);
            startNodeId += nodesPerCluster;
        }

        if (!rmOptions.YtBackend.HasToken()) {
            if (!rmOptions.YtBackend.HasTokenFile()) {
                YQL_LOG(FATAL) << "Either token or token file must be specified in all YtBackends configs";
                exit(1);
            }

            TFsPath path(rmOptions.YtBackend.GetTokenFile());
            auto token = TIFStream(path).ReadAll();
            rmOptions.YtBackend.SetToken(token);
        }

        // Add vanilla job starter and other required files for upload
        for (const auto& jobFile : rmOptions.YtBackend.GetVanillaJobFile()) {
            rmOptions.Files.push_back(jobFile.GetLocalPath());
        }
        rmOptions.Files.push_back(rmOptions.YtBackend.GetVanillaJobLite());

        {
            // uploader
            rmOptions.UploadPrefix = rmOptions.YtBackend.GetUploadPrefix() + "/bin/" + ToString(GetProgramCommitId());
            rmOptions.LockName = TString("ytuploader.") + rmOptions.YtBackend.GetClusterName();
            rmOptions.Counters = MetricsRegistry_->GetSensors()->GetSubgroup("counters", "uploader");
            ActorSystem_->Register(CreateResourceUploader(rmOptions, Coordinator_));
        }

        {
            // bin cleaner
            rmOptions.KeepFirst = 10;
            rmOptions.UploadPrefix = rmOptions.YtBackend.GetUploadPrefix() + "/bin";
            ActorSystem_->Register(CreateResourceCleaner(rmOptions, Coordinator_));
        }

        {
            // udf cleaner
            rmOptions.KeepFirst = 500;
            rmOptions.DropBefore = TDuration::Days(7);
            rmOptions.UploadPrefix = rmOptions.YtBackend.GetUploadPrefix() + "/udfs";
            ActorSystem_->Register(CreateResourceCleaner(rmOptions, Coordinator_));
        }

        {
            // temporary locks
            rmOptions.KeepFilter = rmOptions.YtBackend.GetClusterName(); // don't remove locks with `ClusterName` in LockName
            rmOptions.DropBefore = TDuration::Hours(1);
            rmOptions.UploadPrefix = rmOptions.YtBackend.GetPrefix() + "/locks";
            ActorSystem_->Register(CreateResourceCleaner(rmOptions, Coordinator_));
        }

        {
            // resource manager
            rmOptions.Files.pop_back(); // don't need lite verstion for operation start
            rmOptions.LockName = TString("ytrm.") + rmOptions.YtBackend.GetClusterName();
            rmOptions.UploadPrefix = rmOptions.YtBackend.GetUploadPrefix() + "/bin/" + ToString(GetProgramCommitId());
            rmOptions.Counters = MetricsRegistry_->GetSensors()->GetSubgroup("counters", "ytrm")->GetSubgroup("ytname", rmOptions.YtBackend.GetClusterName());
            rmOptions.ForceIPv4 = Config_->UseIPv4;
            rmOptions.AddressResolverConfig = ConvertToYsonString(Config_->AddressResolver, EYsonFormat::Text);
            ActorSystem_->Register(CreateResourceManager(rmOptions, Coordinator_));
        }
        rmOptions.UploadPrefix = rmOptions.YtBackend.GetUploadPrefix();
        rmOptions.Files.clear();
        uploadResourcesOptions.push_back(rmOptions);
    }

    Coordinator_->StartGlobalWorker(ActorSystem_, uploadResourcesOptions, MetricsRegistry_);

    // Wait here until the DQ component is ready
    auto dqControlFactory = CreateDqControlFactory(grpcPort, uploadResourcesOptions[0].YtBackend.GetVanillaJobLite(), MD5::File(uploadResourcesOptions[0].YtBackend.GetVanillaJobLite()), true, {}, Config_->UdfsWithMd5, Config_->FileStorage);
    auto dqControl = dqControlFactory->GetControl();
    auto isDqReady = dqControl->IsReady({});
    YQL_LOG(INFO) << "DQ warmup initiated, current status: " << isDqReady;
    while (!isDqReady) {
        YQL_LOG(INFO) << "Waiting DQ warmup";
        Sleep(TDuration::Seconds(3));
        isDqReady = dqControl->IsReady();
    }
    YQL_LOG(INFO) << "DQ component is ready";
}

//////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
