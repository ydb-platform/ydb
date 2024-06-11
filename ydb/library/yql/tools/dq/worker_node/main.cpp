#include <ydb/library/yql/providers/dq/worker_manager/local_worker_manager.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/comp_factory.h>
#include <ydb/library/yql/providers/dq/global_worker_manager/service_node_resolver.h>
#include <ydb/library/yql/providers/dq/global_worker_manager/coordination_helper.h>
#include <ydb/library/yql/providers/dq/actors/yt/yt_wrapper.h>
#include <ydb/library/yql/providers/dq/actors/yt/worker_registrator.h>
#include <ydb/library/yql/providers/dq/actors/yt/nodeid_cleaner.h>
#include <ydb/library/yql/providers/dq/metrics/metrics_printer.h>
#include <ydb/library/yql/providers/dq/stats_collector/pool_stats_collector.h>
#include <ydb/library/yql/providers/dq/actors/dynamic_nameserver.h>
#include <ydb/library/yql/providers/dq/actors/execution_helpers.h>
#include <ydb/library/yql/utils/bind_in_range.h>
#include <library/cpp/digest/md5/md5.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/dq/actors/spilling/spilling_file.h>

#include <ydb/library/yql/providers/dq/service/interconnect_helpers.h>
#include <ydb/library/yql/providers/dq/runtime/file_cache.h>
#include <ydb/library/yql/providers/dq/runtime/task_command_executor.h>
#include <ydb/library/yql/providers/dq/task_runner/tasks_runner_pipe.h>
#include <ydb/library/yql/providers/dq/task_runner/tasks_runner_local.h>
#include <ydb/library/yql/dq/integration/transform/yql_dq_task_transform.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_read_actor.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_write_actor.h>
#include <ydb/library/yql/providers/ydb/actors/yql_ydb_source_factory.h>
#include <ydb/library/yql/providers/yt/comp_nodes/dq/dq_yt_factory.h>
#include <ydb/library/yql/providers/yt/mkql_dq/yql_yt_dq_transform.h>
#include <ydb/library/yql/providers/common/comp_nodes/yql_factory.h>
#include <ydb/library/yql/providers/ydb/comp_nodes/yql_ydb_factory.h>
#include <ydb/library/yql/providers/ydb/comp_nodes/yql_ydb_dq_transform.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_actors_factory_impl.h>

#include <ydb/library/yql/providers/ydb/actors/yql_ydb_source_factory.h>
#include <ydb/library/yql/providers/clickhouse/actors/yql_ch_source_factory.h>

#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/mkql_stats_registry.h>
#include <ydb/library/yql/utils/backtrace/backtrace.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/log/proto/logger_config.pb.h>
#include <ydb/library/yql/utils/log/tls_backend.h>
#include <ydb/library/yql/utils/failure_injector/failure_injector.h>
#include <ydb/library/yql/dq/comp_nodes/yql_common_dq_factory.h>
#include <ydb/library/yql/dq/transform/yql_common_dq_transform.h>

#include <yt/yt/core/misc/shutdown.h>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/threading/future/future.h>
#include <library/cpp/svnversion/svnversion.h>

#include <yt/yt/core/actions/invoker.h>
#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <util/generic/scope.h>
#include <util/folder/path.h>
#include <util/system/env.h>
#include <util/system/getpid.h>
#include <util/system/fs.h>
#include <util/folder/dirut.h>

constexpr ui32 THREAD_PER_NODE = 8;

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NDqs;

using namespace NActors;

static NThreading::TPromise<void> ShouldContinue = NThreading::NewPromise<void>();

namespace {
void OnTerminate(int) {
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

NDq::IDqAsyncIoFactory::TPtr CreateAsyncIoFactory(const NYdb::TDriver& driver, IHTTPGateway::TPtr httpGateway) {
    auto factory = MakeIntrusive<NYql::NDq::TDqAsyncIoFactory>();
    RegisterDqPqReadActorFactory(*factory, driver, nullptr);
    RegisterYdbReadActorFactory(*factory, driver, nullptr);
    RegisterClickHouseReadActorFactory(*factory, nullptr, httpGateway);
    RegisterDqPqWriteActorFactory(*factory, driver, nullptr);

    auto s3ActorsFactory = NYql::NDq::CreateS3ActorsFactory();
    auto retryPolicy = GetHTTPDefaultRetryPolicy();
    s3ActorsFactory->RegisterS3WriteActorFactory(*factory, nullptr, httpGateway, retryPolicy);
    s3ActorsFactory->RegisterS3ReadActorFactory(*factory, nullptr, httpGateway, retryPolicy);

    return factory;
}

}

int main(int argc, char** argv) {

    const auto driverConfig = NYdb::TDriverConfig().SetLog(CreateLogBackend("cerr"));
    NYdb::TDriver driver(driverConfig);

    Y_DEFER {
        driver.Stop(true);
    };

    NKikimr::NMiniKQL::IStatsRegistryPtr statsRegistry = NKikimr::NMiniKQL::CreateDefaultStatsRegistry();

    auto dqCompFactory = NKikimr::NMiniKQL::GetCompositeWithBuiltinFactory({
        NYql::GetCommonDqFactory(),
        NYql::GetDqYtFactory(statsRegistry.Get()),
        NYql::GetDqYdbFactory(driver),
        NKikimr::NMiniKQL::GetYqlFactory(),
        NYql::GetPgFactory()
    });

    auto dqTaskTransformFactory = NYql::CreateCompositeTaskTransformFactory({
        NYql::CreateCommonDqTaskTransformFactory(),
        NYql::CreateYtDqTaskTransformFactory(),
        NYql::CreateYdbDqTaskTransformFactory()
    });

    auto patternCache = std::make_shared<NKikimr::NMiniKQL::TComputationPatternLRUCache>(NKikimr::NMiniKQL::TComputationPatternLRUCache::Config(200_MB, 200_MB));

    if (argc > 1 && !strcmp(argv[1], "tasks_runner_proxy")) {
        NYql::NBacktrace::RegisterKikimrFatalActions();
        //NYql::NBacktrace::EnableKikimrSymbolize(); // symbolize in gateway

        return NTaskRunnerProxy::CreateTaskCommandExecutor(dqCompFactory, dqTaskTransformFactory, statsRegistry.Get(), true);
    }

    using namespace NLastGetopt;
    TOpts opts = TOpts::Default();
    opts.AddHelpOption();
    opts.AddLongOption('i', "id", "Node ID");
    opts.AddLongOption('p', "port", "Port");
    opts.AddLongOption('u', "udfs", "UdfsPath");
    opts.AddLongOption("config", "Config");
    opts.AddLongOption("service_addr", "Service Addr (host/port pair)");

    opts.AddLongOption("ytprefix", "Yt prefix");
    opts.AddLongOption("proxy", "Yt proxy");
    opts.AddLongOption("workers", "Workers");
    opts.AddLongOption("threads", "Threads");
    opts.AddLongOption("yttoken", "Yt token");
    opts.AddLongOption("ytuser", "Yt user");
    opts.AddLongOption("enabled_failure_injector", "Enabled failure injections").NoArgument();
    opts.AddLongOption("revision", "Revision");
    opts.AddLongOption("heartbeat", "HeartbeatPeriod");
    opts.AddLongOption("solomon", "Solomon Token");
    opts.AddLongOption("print_metrics", "Print Metrics").NoArgument();
    opts.AddLongOption("announce_cluster_name", "Send this name in pings");
    opts.AddLongOption("disable_pipe", "Disable pipe").NoArgument();
    opts.AddLongOption("log_level", "Log Level");
    opts.AddLongOption("ipv4", "Use ipv4").NoArgument();
    opts.AddLongOption("enable-spilling", "Enable disk spilling").NoArgument();

    ui32 threads = THREAD_PER_NODE;
    TString host;
    TString ip;
    TString solomonToken;
    int capacity = 1;
    int heartbeatPeriodMs = 100;

    TOptsParseResult res(&opts, argc, argv);

    auto loggerConfig = NYql::NProto::TLoggingConfig();
    auto logLevel = NYql::NProto::TLoggingConfig::INFO;
    if (res.Has("log_level")) {
        auto str = res.Get<TString>("log_level");
        if (str == "TRACE") {
            logLevel = NYql::NProto::TLoggingConfig::TRACE;
        }
    }

    loggerConfig.SetAllComponentsLevel(logLevel);

    NYql::NLog::InitLogger(loggerConfig, false);

    ui16 startPort = res.Get<ui16>("port");
    if (res.Has("heartbeat")) {
        heartbeatPeriodMs = res.Get<int>("heartbeat");
    }
    if (res.Has("threads")) {
        threads = res.Get<int>("threads");
    }

    NProto::TDqConfig::TYtCoordinator coordinatorConfig;
    bool useYtCoordination = false;
    if (res.Has("proxy")) {
        coordinatorConfig.SetPrefix(res.Get<TString>("ytprefix"));
        coordinatorConfig.SetClusterName(res.Get<TString>("proxy"));
        useYtCoordination = true;
    }
    coordinatorConfig.SetHeartbeatPeriodMs(heartbeatPeriodMs);

    if (!useYtCoordination) {
        coordinatorConfig.SetLockType("dummy");
    }

    if (res.Has("yttoken")) {
        coordinatorConfig.SetToken(res.Get<TString>("yttoken"));
    }

    if (res.Has("workers")) {
        capacity = res.Get<int>("workers");
    }

    if (res.Has("ytuser")) {
        coordinatorConfig.SetUser(res.Get<TString>("ytuser"));
    }

    if (res.Has("revision")) {
        coordinatorConfig.SetRevision(res.Get<TString>("revision"));
        YQL_LOG(INFO) << "Set revision '" << coordinatorConfig.GetRevision() << "'";
    }

    if (res.Has("solomon")) {
        solomonToken = res.Get<TString>("solomon");
    }

    if (res.Has("enabled_failure_injector")) {
        YQL_LOG(INFO) << "Enabled failure injector";
        TFailureInjector::Activate();
    }

    int portIndex = res.Has("ipv4") ? 0 : 1;
    TRangeWalker<int> portWalker(startPort, startPort+100);
    auto ports = BindInRange(portWalker);

    std::tie(host, ip) = NYql::NDqs::GetLocalAddress(
        coordinatorConfig.HasHostName() ? &coordinatorConfig.GetHostName() : nullptr,
        res.Has("ipv4") ? AF_INET : AF_INET6
    );

    auto coordinator = CreateCoordiantionHelper(coordinatorConfig, NProto::TDqConfig::TScheduler(), "worker_node", ports[portIndex].Addr.GetPort(), host, ip);
    coordinatorConfig = coordinator->GetConfig();

    NProto::TDqConfig::TYtBackend backendConfig;
    backendConfig.SetUploadPrefix(coordinatorConfig.GetPrefix());
    backendConfig.SetUser(coordinatorConfig.GetUser());
    backendConfig.SetToken(coordinatorConfig.GetToken());
    backendConfig.SetClusterName(coordinatorConfig.GetClusterName());

    Cerr << host + ":" + ToString(ip) << Endl;

    TMaybe<ui32> maybeNodeId;
    if (res.Has("id")) {
        maybeNodeId = res.Get<ui32>("id");
    }
    auto nodeId = coordinator->GetNodeId(
        maybeNodeId,
        {},
        static_cast<ui32>(NDqs::ENodeIdLimits::MinWorkerNodeId),
        static_cast<ui32>(NDqs::ENodeIdLimits::MaxWorkerNodeId),
        {}
    );

    Cerr << "My nodeId: " << nodeId << Endl;
    TString fileCacheDir = "./file_cache/" + ToString(nodeId);
    IFileCache::TPtr fileCache = new TFileCache(fileCacheDir, 16000000000L);


    TString udfsDir = res.GetOrElse("udfs", "");

    try {
        auto dqSensors = GetSensorsGroupFor(NSensorComponent::kDq);
        THolder<NActors::TActorSystemSetup> setup;
        TIntrusivePtr<NActors::NLog::TSettings> logSettings;
        std::tie(setup, logSettings) = BuildActorSetup(
            nodeId,
            ip,
            ports[portIndex].Addr.GetPort(),
            ports[portIndex].Socket->Release(),
            {threads},
            dqSensors,
            [](const TIntrusivePtr<NActors::TTableNameserverSetup>& setup) {
                return NYql::NDqs::CreateDynamicNameserver(setup);
            },
            Nothing());

        auto statsCollector = CreateStatsCollector(5, *setup.Get(), dqSensors);

        TVector<TString> UDFsPaths;
        if (!udfsDir.empty()) {
            NKikimr::NMiniKQL::FindUdfsInDir(udfsDir, &UDFsPaths);
        }
        auto functionRegistry = NKikimr::NMiniKQL::CreateFunctionRegistry(
            &NYql::NBacktrace::KikimrBackTrace, NKikimr::NMiniKQL::CreateBuiltinRegistry(), false, UDFsPaths)->Clone();

        for (auto& m : functionRegistry->GetAllModuleNames()) {
            auto path = *functionRegistry->FindUdfPath(m);
            TString objectId = MD5::Calc(path); // Production env uses MD5::File as an Id. For testing purpose we use fast version.
            Cout << m << '\t' << path << "\t" << objectId << Endl;
            if (!fileCache->Contains(objectId)) {
                TString newPath = fileCacheDir + "/" + objectId;
                NFs::Copy(path, newPath);
                Cout << "Add " << newPath << " " << objectId << "\n";
                fileCache->AddFile(newPath, objectId);
            }
        }

        NKikimr::NMiniKQL::FillStaticModules(*functionRegistry);

        auto actorSystem = MakeHolder<NActors::TActorSystem>(setup, nullptr, logSettings);

        actorSystem->Start();
        actorSystem->Register(statsCollector);

        if (!maybeNodeId) {
            coordinator->StartRegistrator(actorSystem.Get());
        }

        TVector<TString> hostPort;
        if (res.Has("service_addr")) {
            TString addresses = res.Get<TString>("service_addr");
            Split(addresses, ",", hostPort);
        }

/*
        if (solomonToken) {
            TSolomonAgentConfig config = TSolomonAgentConfig()
                .WithServer("https://solomon.yandex.net")
                .WithPath("/api/v2/push")
                .WithPort(443)
                .WithProject("yql")
                .WithService("dq_vanilla")
                .WithCluster("test")
                .WithHost("NodeId-" + ToString(nodeId))
                .WithCalcDerivs(true)
                .WithAuthorizaton("OAuth " + solomonToken)
                .WithCommonLabels({{"ytcluster", "test_cluster"}})
                ;
            actorSystem->Register(CreateMetricsPusher(dqSensors, config));
        }
*/

        if (res.Has("print_metrics")) {
            actorSystem->Register(CreateMetricsPrinter(dqSensors));
        }

        auto resolver = coordinator->CreateServiceNodeResolver(actorSystem.Get(), hostPort);

        backendConfig.SetWorkerCapacity(capacity);
        TResourceManagerOptions rmOptions;
        rmOptions.Capabilities = Yql::DqsProto::RegisterNodeRequest::ECAP_COMPUTE_ACTOR;
        rmOptions.YtBackend = backendConfig;
        rmOptions.FileCache = fileCache;
        rmOptions.TmpDir = "./tmp";

        if (res.Has("announce_cluster_name")) {
            rmOptions.AnnounceClusterName = res.Get<TString>("announce_cluster_name");
            Cerr << "Announce as '" << backendConfig.GetClusterName() << "'\n";
        }

        actorSystem->Register(coordinator->CreateServiceNodePinger(resolver, rmOptions));

        NYql::NTaskRunnerProxy::TPipeFactoryOptions pfOptions;
        pfOptions.ExecPath = TFsPath(argv[0]).RealPath().GetPath();
        pfOptions.FileCache = fileCache;
        if (res.Has("revision")) {
            pfOptions.Revision = coordinatorConfig.GetRevision();
        }

        NYql::NDqs::TLocalWorkerManagerOptions lwmOptions;
        bool disablePipe = res.Has("disable_pipe");
        NKikimr::NMiniKQL::IStatsRegistryPtr statsRegistry = NKikimr::NMiniKQL::CreateDefaultStatsRegistry();
        lwmOptions.Factory = disablePipe
            ? NTaskRunnerProxy::CreateFactory(functionRegistry.Get(), dqCompFactory, dqTaskTransformFactory, patternCache, true)
            : NTaskRunnerProxy::CreatePipeFactory(pfOptions);
        lwmOptions.AsyncIoFactory = CreateAsyncIoFactory(driver, IHTTPGateway::Make());
        lwmOptions.FunctionRegistry = functionRegistry.Get();
        lwmOptions.RuntimeData = coordinator->GetRuntimeData();
        lwmOptions.TaskRunnerInvokerFactory = disablePipe
            ? TTaskRunnerInvokerFactory::TPtr(new NDqs::TTaskRunnerInvokerFactory())
            : TTaskRunnerInvokerFactory::TPtr(new TConcurrentInvokerFactory(2*capacity));
        YQL_ENSURE(functionRegistry);
        lwmOptions.TaskRunnerActorFactory = disablePipe
            ? NDq::NTaskRunnerActor::CreateLocalTaskRunnerActorFactory([=](std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc, const NDq::TDqTaskSettings& task, NDqProto::EDqStatsMode statsMode, const NDq::TLogFunc& )
                {
                    return lwmOptions.Factory->Get(alloc, task, statsMode);
                })
            : NTaskRunnerActor::CreateTaskRunnerActorFactory(lwmOptions.Factory, lwmOptions.TaskRunnerInvokerFactory);
        lwmOptions.ComputeActorOwnsCounters = true;
        bool enableSpilling = res.Has("enable-spilling");
        auto resman = NDqs::CreateLocalWorkerManager(lwmOptions);

        auto workerManagerActorId = actorSystem->Register(resman);
        actorSystem->RegisterLocalService(MakeWorkerManagerActorID(nodeId), workerManagerActorId);

        if (enableSpilling) {
            auto spillingActor = actorSystem->Register(
                NDq::CreateDqLocalFileSpillingService(
                    NDq::TFileSpillingServiceConfig{
                        .Root = "./spilling",
                        .CleanupOnShutdown = true
                    },
                    MakeIntrusive<NDq::TSpillingCounters>(dqSensors)
                )
            );

            actorSystem->RegisterLocalService(NDq::MakeDqLocalFileSpillingServiceID(nodeId), spillingActor);
        }

        auto endFuture = ShouldContinue.GetFuture();

        signal(SIGINT, &OnTerminate);
        signal(SIGTERM, &OnTerminate);


        endFuture.Wait();

        actorSystem->Stop();
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return -1;
    }

    NYT::Shutdown();
    return 0;
}
