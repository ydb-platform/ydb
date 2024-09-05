#include "dq_worker.h"

#include <ydb/library/yql/utils/signals/signals.h>
#include <ydb/library/yql/utils/bind_in_range.h>

#include <ydb/library/yql/providers/dq/stats_collector/pool_stats_collector.h>
#include <ydb/library/yql/providers/dq/actors/yt/nodeid_assigner.h>
#include <ydb/library/yql/providers/dq/actors/dynamic_nameserver.h>
#include <ydb/library/yql/providers/dq/service/interconnect_helpers.h>
#include <ydb/library/yql/providers/dq/global_worker_manager/coordination_helper.h>

#include <ydb/library/yql/providers/dq/runtime/file_cache.h>
#include <ydb/library/yql/providers/dq/runtime/runtime_data.h>
#include <ydb/library/yql/providers/dq/worker_manager/local_worker_manager.h>

#include <ydb/library/yql/dq/actors/spilling/spilling_file.h>

#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/log/tls_backend.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/utils/range_walker.h>

#include <yt/yt/core/actions/invoker.h>
#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/config.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <util/system/fs.h>
#include <util/stream/file.h>
#include <util/system/env.h>
#include <util/system/shellcommand.h>
#include <util/string/type.h>

using namespace NYql::NDqs;

namespace {
    template <typename TMessage>
    THolder<TMessage> ParseProtoConfig(const TString& cfgFile) {
        auto config = MakeHolder<TMessage>();
        TString configData = TFileInput(cfgFile).ReadAll();;

        using ::google::protobuf::TextFormat;
        if (!TextFormat::ParseFromString(configData, config.Get())) {
            YQL_LOG(ERROR) << "Bad format of dq_vanilla_job configuration";
            return {};
        }

        return config;
    }

    static NThreading::TPromise<void> ShouldContinue = NThreading::NewPromise<void>();

    static void OnTerminate(int) {
        ShouldContinue.SetValue();
    }

    class TSerializedTaskRunnerInvoker: public ITaskRunnerInvoker {
    public:
        TSerializedTaskRunnerInvoker(const NYT::IInvokerPtr& invoker)
            : Invoker(NYT::NConcurrency::CreateSerializedInvoker(invoker))
        { }

        void Invoke(const std::function<void(void)>& f) override {
            Invoker->Invoke(BIND(f));
        }

    private:
        const NYT::IInvokerPtr Invoker;
    };

    class TConcurrentInvokerFactory: public ITaskRunnerInvokerFactory {
    public:
        TConcurrentInvokerFactory(int capacity)
            : ThreadPool(NYT::NConcurrency::CreateThreadPool(capacity, "WorkerActor"))
        { }

        ITaskRunnerInvoker::TPtr Create() override {
            return new TSerializedTaskRunnerInvoker(ThreadPool->GetInvoker());
        }

        NYT::NConcurrency::IThreadPoolPtr ThreadPool;
    };

    void ConfigurePorto(const NYql::NProto::TDqConfig::TYtBackend& config, const TString portoCtl) {
        TString settings[][2] = {
            {"enable_porto", "isolate"},
            {"respawn", "true"}
        };
        int nSettings = 2;
        {
            TShellCommand cmd(portoCtl, {"create", "Outer"});
            cmd.Run().Wait();
        }
        for (int i = 0; i < nSettings; i++) {
            TShellCommand cmd(portoCtl, {"set", "Outer", settings[i][0], settings[i][1]});
            cmd.Run().Wait();
        }
        for (const auto& attr : config.GetPortoSettings().GetSetting()) {
            TShellCommand cmd(portoCtl, {"set", "Outer", attr.GetName(), attr.GetValue()});
            cmd.Run().Wait();
        }
        {
            TShellCommand cmd(portoCtl, {"start", "Outer"});
            cmd.Run().Wait();
        }
        {
            TShellCommand cmd(portoCtl, {"wait", "Outer"});
            cmd.Run().Wait();
        }
    }
}

namespace NYql::NDq::NWorker {

    void TDefaultWorkerConfigurator::ConfigureMetrics(const THolder<NYql::NProto::TLoggingConfig>& /*loggerConfig*/, const THolder<NActors::TActorSystem>& /*actorSystem*/, const NProto::TDqConfig::TYtBackend& /*backendConfig*/, const TResourceManagerOptions& /*rmOptions*/, ui32 /*nodeId*/) const {
    }

    NDq::IDqAsyncIoFactory::TPtr TDefaultWorkerConfigurator::CreateAsyncIoFactory() const {
        return MakeIntrusive<NYql::NDq::TDqAsyncIoFactory>();
    }

    void TDefaultWorkerConfigurator::OnWorkerFinish() {
    }

    TWorkerJob::TWorkerJob()
        : WorkerConfigurator(MakeHolder<TDefaultWorkerConfigurator>(TDefaultWorkerConfigurator()))
    { }

    void TWorkerJob::SetConfigFile(const TString& configFile) {
        ConfigFile = configFile;
    }

    void TWorkerJob::SetWorkerConfigurator(THolder<IWorkerConfigurator> workerConfigurator) {
        WorkerConfigurator = std::move(workerConfigurator);
    }

    void TWorkerJob::Do() {

        auto loggerConfig = MakeHolder<NYql::NProto::TLoggingConfig>();

        ui16 startPort = 0;

        auto deterministicMode = !!GetEnv("YQL_DETERMINISTIC_MODE");

        YQL_ENSURE(TryFromString<ui16>(GetEnv(NCommonJobVars::ACTOR_PORT), startPort),
                    "Invalid service config port env var empty");

        ui32 tryNodeId;
        YQL_ENSURE(TryFromString<ui32>(GetEnv(NCommonJobVars::ACTOR_NODE_ID, "0"), tryNodeId),
                    "Invalid nodeId env var");

        if (!ConfigFile.empty()) {
            loggerConfig = ParseProtoConfig<NYql::NProto::TLoggingConfig>(ConfigFile);

            for (auto& logDest : *loggerConfig->MutableLogDest()) {
                if (logDest.GetType() == NYql::NProto::TLoggingConfig::FILE) {
                    TString logFile = logDest.GetTarget() + "." + ToString(tryNodeId);
                    logDest.SetTarget(logFile);
                }
            }

            loggerConfig->SetAllComponentsLevel(NYql::NProto::TLoggingConfig::TRACE);
        } else {
            loggerConfig->SetAllComponentsLevel(NYql::NProto::TLoggingConfig::DEBUG);
        }
        NYql::NLog::InitLogger(*loggerConfig, false);
        InitSignals();

        TString fileCacheDir = GetEnv(NCommonJobVars::UDFS_PATH);
        TString ytCoordinatorStr = GetEnv(TString("YT_SECURE_VAULT_") + NCommonJobVars::YT_COORDINATOR);
        TString ytBackendStr = GetEnv(TString("YT_SECURE_VAULT_") + NCommonJobVars::YT_BACKEND);

        TString operationId = GetEnv("YT_OPERATION_ID");
        TString jobId = GetEnv("YT_JOB_ID");

        TString operationSize = GetEnv(NCommonJobVars::OPERATION_SIZE);

        NProto::TDqConfig::TYtCoordinator coordinatorConfig;
        TStringInput inputStream1(ytCoordinatorStr);
        ParseFromTextFormat(inputStream1, coordinatorConfig, EParseFromTextFormatOption::AllowUnknownField);

        NProto::TDqConfig::TYtBackend backendConfig;
        TStringInput inputStream2(ytBackendStr);
        ParseFromTextFormat(inputStream2, backendConfig, EParseFromTextFormatOption::AllowUnknownField);

        TRangeWalker<int> portWalker(startPort, startPort+100);
        auto ports = BindInRange(portWalker);

        auto forceIPv4 = IsTrue(GetEnv(TString("YT_SECURE_VAULT_") + NCommonJobVars::YT_FORCE_IPV4, ""));

        auto addressResolverStr = GetEnv(NCommonJobVars::ADDRESS_RESOLVER_CONFIG, "");
        if (!addressResolverStr.Empty()) {
            auto addressResolverConfig = NYT::NYTree::ConvertTo<NYT::NNet::TAddressResolverConfigPtr>(NYT::NYson::TYsonString(addressResolverStr));
            NYT::NNet::TAddressResolver::Get()->Configure(addressResolverConfig);
        } else if (forceIPv4) {
            // Keep the previous behavior for compatibility.
            auto config = NYT::New<NYT::NNet::TAddressResolverConfig>();
            config->EnableIPv4 = true;
            config->EnableIPv6 = false;
            NYT::NNet::TAddressResolver::Get()->Configure(config);
        }

        auto [host, ip] = NYql::NDqs::GetLocalAddress(
            coordinatorConfig.HasHostName() ? &coordinatorConfig.GetHostName() : nullptr,
            forceIPv4 ? AF_INET : AF_INET6
        );

        auto coordinator = CreateCoordiantionHelper(coordinatorConfig, NProto::TDqConfig::TScheduler(), "worker_node", ports[forceIPv4 ? 0 : 1].Addr.GetPort(), host, ip);
        i64 cacheSize = backendConfig.HasCacheSize()
            ? backendConfig.GetCacheSize()
            : 16000000000L;
        TIntrusivePtr<IFileCache> fileCache = new TFileCache(fileCacheDir + "/cache", cacheSize);
        NFs::SymLink(fileCacheDir, "file_cache"); // COMPAT
        TString layerDir = fileCacheDir + "/layer";
        if (backendConfig.GetPortoLayer().size() > 0) {
            NFs::MakeDirectoryRecursive(layerDir + "/mnt/work");
            for (const auto& layerPath : backendConfig.GetPortoLayer()) {
                auto pos = layerPath.rfind('/');
                auto archive = layerPath.substr(pos+1);
                TShellCommand cmd("tar", {"xf", archive, "-C", layerDir});
                cmd.Run().Wait();
            }
        } else {
            NFs::MakeDirectoryRecursive("mnt/work");
            NFs::MakeDirectoryRecursive("usr/local/bin");
        }

        int capacity = backendConfig.GetWorkerCapacity()
                ? backendConfig.GetWorkerCapacity()
                : 1;

        NYql::NTaskRunnerProxy::TPipeFactoryOptions pfOptions;
        pfOptions.ExecPath = GetExecPath();
        pfOptions.FileCache = fileCache;
        if (deterministicMode) {
            YQL_LOG(DEBUG) << "deterministicMode On";
            pfOptions.Env["YQL_DETERMINISTIC_MODE"] = "1";
        }
        if (backendConfig.GetEnforceJobUtc()) {
            pfOptions.Env["TZ"] = "UTC0";
        }
        if (backendConfig.GetEnforceJobYtIsolation()) {
            pfOptions.Env["YT_ALLOW_HTTP_REQUESTS_TO_YT_FROM_JOB"] = "0";
            pfOptions.Env["YT_FORBID_REQUESTS_FROM_JOB"] = "1";
        }
        pfOptions.EnablePorto = backendConfig.GetEnablePorto() == "isolate";
        pfOptions.PortoLayer = backendConfig.GetPortoLayer().size() == 0 ? "" : layerDir;
        pfOptions.MaxProcesses = capacity*1.5;
        pfOptions.ContainerName = "Outer";

        TResourceManagerOptions rmOptions;
        rmOptions.YtBackend = backendConfig;
        rmOptions.FileCache = fileCache;
        rmOptions.TmpDir =  fileCacheDir + "/tmp";

        if (NFs::Exists(layerDir + "/usr/bin/portoctl")) {
            TString dst = fileCache->GetDir() + "/portoctl";
            NFs::Copy(layerDir + "/usr/bin/portoctl", dst);
            NFs::HardLink(layerDir + "/usr/bin/portoctl", layerDir + "/usr/sbin/portoctl"); // workaround PORTO-997
            chmod(dst.c_str(), 0755);
            pfOptions.PortoCtlPath = dst;
            rmOptions.DieOnFileAbsence = dst; // die on file absence
        }

        Cerr << host + ":" + ip << Endl;

        THashMap<TString, TString> attributes;
        attributes[NCommonAttrs::OPERATIONID_ATTR] = operationId;
        attributes[NCommonAttrs::OPERATIONSIZE_ATTR] = operationSize;
        attributes[NCommonAttrs::JOBID_ATTR] = jobId;
        attributes[NCommonAttrs::CLUSTERNAME_ATTR] = backendConfig.GetClusterName();

        auto nodeIdOpt = (tryNodeId == 0)
            ? TMaybe<ui32>()
            : TMaybe<ui32>(tryNodeId);
        auto nodeId = coordinator->GetNodeId(
            nodeIdOpt,
            {},
            static_cast<ui32>(NDqs::ENodeIdLimits::MinWorkerNodeId),
            static_cast<ui32>(NDqs::ENodeIdLimits::MaxWorkerNodeId),
            attributes);

        Y_ABORT_UNLESS(
            static_cast<ui32>(NDqs::ENodeIdLimits::MinWorkerNodeId) <= nodeId &&
            nodeId < static_cast<ui32>(NDqs::ENodeIdLimits::MaxWorkerNodeId));

        Cerr << "My nodeId: " << nodeId << Endl;

        Cerr << "Configure porto" << Endl;
        if (backendConfig.GetEnablePorto() == "isolate") {
            ConfigurePorto(backendConfig, pfOptions.PortoCtlPath);
        }
        Cerr << "Configure porto done" << Endl;

        auto dqSensors = GetSensorsGroupFor(NSensorComponent::kDq);
        THolder<NActors::TActorSystemSetup> setup;
        TIntrusivePtr<NActors::NLog::TSettings> logSettings;
        std::tie(setup, logSettings) = BuildActorSetup(
            nodeId,
            ip,
            ports[forceIPv4 ? 0 : 1].Addr.GetPort(),
            ports[forceIPv4 ? 0 : 1].Socket->Release(),
            {},
            dqSensors,
            [](const TIntrusivePtr<NActors::TTableNameserverSetup>& setup) {
                return NYql::NDqs::CreateDynamicNameserver(setup);
            },
            Nothing(),
            backendConfig.GetICSettings());

        auto statsCollector = CreateStatsCollector(5, *setup.Get(), dqSensors);

        auto actorSystem = MakeHolder<NActors::TActorSystem>(setup, nullptr, logSettings);

        actorSystem->Start();

        actorSystem->Register(statsCollector);

        TVector<TString> hostPortPairs;
        for (auto hostPortPair : coordinatorConfig.GetServiceNodeHostPort()) {
            hostPortPairs.emplace_back(hostPortPair);
            // tests
            if (hostPortPair.StartsWith("localhost")) {
                rmOptions.ExitOnPingFail = true;
            }
        }

        WorkerConfigurator->ConfigureMetrics(loggerConfig, actorSystem, backendConfig, rmOptions, nodeId);

        // rmOptions.MetricsRegistry = CreateMetricsRegistry(dqSensors); // send metrics to gwm, unsupported
        auto resolver = coordinator->CreateServiceNodeResolver(actorSystem.Get(), hostPortPairs);
        actorSystem->Register(coordinator->CreateServiceNodePinger(resolver, rmOptions, attributes));

        NLog::YqlLogger().UpdateProcInfo(jobId + "/" + GetGuidAsString(coordinator->GetRuntimeData()->WorkerId));

        // For testing only
        THashMap<TString, TString> clusterMapping;
        clusterMapping["plato"] = backendConfig.GetClusterName();

        auto proxyFactory = NTaskRunnerProxy::CreatePipeFactory(pfOptions);
        ITaskRunnerInvokerFactory::TPtr invokerFactory = new TConcurrentInvokerFactory(2*capacity);
        auto taskRunnerActorFactory = NTaskRunnerActor::CreateTaskRunnerActorFactory(proxyFactory, invokerFactory, coordinator->GetRuntimeData());

        TLocalWorkerManagerOptions lwmOptions;
        lwmOptions.Factory = proxyFactory;
        lwmOptions.TaskRunnerActorFactory = taskRunnerActorFactory;
        lwmOptions.AsyncIoFactory = WorkerConfigurator->CreateAsyncIoFactory();
        lwmOptions.RuntimeData = coordinator->GetRuntimeData();
        lwmOptions.TaskRunnerInvokerFactory = invokerFactory;
        lwmOptions.ClusterNamesMapping = clusterMapping;
        lwmOptions.ComputeActorOwnsCounters = true;

        auto resman = NDqs::CreateLocalWorkerManager(lwmOptions);

        auto workerManagerActorId = actorSystem->Register(resman);
        actorSystem->RegisterLocalService(MakeWorkerManagerActorID(nodeId), workerManagerActorId);

        if (backendConfig.HasSpillingSettings()) {
            auto spilling = NDq::CreateDqLocalFileSpillingService(
                NDq::TFileSpillingServiceConfig {
                    .Root = backendConfig.GetSpillingSettings().GetRoot(),
                    .MaxTotalSize = backendConfig.GetSpillingSettings().GetMaxTotalSize(),
                    .MaxFileSize = backendConfig.GetSpillingSettings().GetMaxFileSize(),
                    .MaxFilePartSize = backendConfig.GetSpillingSettings().GetMaxFilePartSize(),
                    .IoThreadPoolWorkersCount = backendConfig.GetSpillingSettings().GetIoThreadPoolWorkersCount(),
                    .IoThreadPoolQueueSize = backendConfig.GetSpillingSettings().GetIoThreadPoolQueueSize(),
                    .CleanupOnShutdown = backendConfig.GetSpillingSettings().GetCleanupOnShutdown()
                },
                MakeIntrusive<NDq::TSpillingCounters>(dqSensors)
            );
            auto spillingActor = actorSystem->Register(spilling);
            actorSystem->RegisterLocalService(NDq::MakeDqLocalFileSpillingServiceID(nodeId), spillingActor);
        }

        auto endFuture = ShouldContinue.GetFuture();

        signal(SIGINT, &OnTerminate);
        signal(SIGTERM, &OnTerminate);
        signal(SIGPIPE, SIG_IGN);

        // run forever

        endFuture.Wait();
        WorkerConfigurator->OnWorkerFinish();
        actorSystem->Stop();
        dqSensors->OutputHtml(Cerr);
    }

    REGISTER_VANILLA_JOB(TWorkerJob);

} // namespace NYql::NDq::NWorker
