#include "test_runtime.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/pool_stats_collector.h>
#include <ydb/core/mon/sync_http_mon.h>
#include <ydb/core/mon/async_http_mon.h>
#include <ydb/core/mon_alloc/profiler.h>
#include <ydb/core/tablet/tablet_impl.h>

#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/executor_pool_io.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/interconnect/interconnect_impl.h>

#include <ydb/core/protos/datashard_config.pb.h>
#include <ydb/core/protos/key.pb.h>
#include <ydb/core/protos/netclassifier.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/protos/stream.pb.h>

/**** ACHTUNG: Do not make here any new dependecies on kikimr ****/

namespace NActors {

    void TTestActorRuntime::TNodeData::Stop() {
        if (Mon) {
            Mon->Stop();
            GetAppData<NKikimr::TAppData>()->Mon = nullptr;
        }
        TNodeDataBase::Stop();
    }

    TTestActorRuntime::TNodeData::~TNodeData() {
        Stop();
    }

    ui64 TTestActorRuntime::TNodeData::GetLoggerPoolId() const {
        return GetAppData<NKikimr::TAppData>()->IOPoolId;
    }

    void TTestActorRuntime::Initialize() {
        SetScheduledEventFilter(&TTestActorRuntime::DefaultScheduledFilterFunc);
        NodeFactory = MakeHolder<TNodeFactory>();
        InitNodes();
    }

    void TTestActorRuntime::SetupStatsCollectors() {
        NeedStatsCollectors = true;
    }

    void TTestActorRuntime::SetupActorSystemConfig(const TActorSystemSetupConfig& config) {
        ActorSystemSetupConfig = config;
    }

    TTestActorRuntime::TTestActorRuntime(THeSingleSystemEnv d)
        : TPortManager(false)
        , TTestActorRuntimeBase{d}
    {
        /* How it is possible to do initilization without these components? */
        NKikimr::TAppData::RandomProvider = RandomProvider;
        NKikimr::TAppData::TimeProvider = TimeProvider;

        Initialize();
    }

    TTestActorRuntime::TTestActorRuntime(ui32 nodeCount, ui32 dataCenterCount, bool useRealThreads)
        : TPortManager(false)
        , TTestActorRuntimeBase{nodeCount, dataCenterCount, useRealThreads}
    {
        Initialize();
    }

    TTestActorRuntime::TTestActorRuntime(ui32 nodeCount, ui32 dataCenterCount)
        : TPortManager(false)
        , TTestActorRuntimeBase{nodeCount, dataCenterCount}
    {
        Initialize();
    }

    TTestActorRuntime::TTestActorRuntime(ui32 nodeCount, bool useRealThreads)
        : TPortManager(false)
        , TTestActorRuntimeBase{nodeCount, useRealThreads}
    {
        Initialize();
    }

    TTestActorRuntime::~TTestActorRuntime() {
        if (!UseRealThreads) {
            NKikimr::TAppData::RandomProvider = CreateDefaultRandomProvider();
            NKikimr::TAppData::TimeProvider = CreateDefaultTimeProvider();
        }

        SetObserverFunc(&TTestActorRuntimeBase::DefaultObserverFunc);
        SetScheduledEventsSelectorFunc(&CollapsedTimeScheduledEventsSelector);
        SetEventFilter(&TTestActorRuntimeBase::DefaultFilterFunc);
        SetScheduledEventFilter(&TTestActorRuntimeBase::NopFilterFunc);
        SetRegistrationObserverFunc(&TTestActorRuntimeBase::DefaultRegistrationObserver);

        CleanupNodes();
    }

    void TTestActorRuntime::AddAppDataInit(std::function<void(ui32, NKikimr::TAppData&)> callback) {
        Y_ABORT_UNLESS(!IsInitialized, "Actor system is already initialized");
        AppDataInit_.push_back(std::move(callback));
    }

    void TTestActorRuntime::Initialize(TEgg egg) {
        IsInitialized = true;

        Opaque = std::move(egg.Opaque);
        App0.Reset(egg.App0);
        KeyConfigGenerator = std::move(egg.KeyConfigGenerator);

        if (!UseRealThreads) {
            NKikimr::TAppData::RandomProvider = RandomProvider;
            NKikimr::TAppData::TimeProvider = TimeProvider;
        }

        MonPorts.clear();
        for (ui32 nodeIndex = 0; nodeIndex < NodeCount; ++nodeIndex) {
            ui32 nodeId = FirstNodeId + nodeIndex;
            auto* node = GetNodeById(nodeId);
            const auto* app0 = App0.Get();
            if (!SingleSysEnv) {
                const TIntrusivePtr<::NMonitoring::TDynamicCounters> profilerCounters = NKikimr::GetServiceCounters(node->DynamicCounters, "utils");
                TTestActorSetupCmd profilerSetup{CreateProfilerActor(profilerCounters, "."), TMailboxType::Simple, 0};
                node->LocalServices.push_back(std::pair<TActorId, TTestActorSetupCmd>(MakeProfilerID(FirstNodeId + nodeIndex), profilerSetup));
            }

            if (!UseRealThreads) {
                node->AppData0.reset(new NKikimr::TAppData(0, 0, 0, 0, { }, App0->TypeRegistry, App0->FunctionRegistry, App0->FormatFactory, nullptr));
                node->SchedulerPool.Reset(CreateExecutorPoolStub(this, nodeIndex, node, 0));
                node->MailboxTable.Reset(new TMailboxTable());
                node->ActorSystem = MakeActorSystem(nodeIndex, node);
                node->ExecutorThread.Reset(new TExecutorThread(0, 0, node->ActorSystem.Get(), node->SchedulerPool.Get(), node->MailboxTable.Get(), "TestExecutor"));
            } else {
                node->AppData0.reset(new NKikimr::TAppData(0, 1, 2, 3, { }, app0->TypeRegistry, app0->FunctionRegistry, app0->FormatFactory, nullptr));
                node->ActorSystem = MakeActorSystem(nodeIndex, node);
            }
            node->LogSettings->MessagePrefix = " node " + ToString(nodeId);

            auto* nodeAppData = node->GetAppData<NKikimr::TAppData>();
            if (!UseRealThreads) {
                nodeAppData->MonotonicTimeProvider = MonotonicTimeProvider;
            }
            nodeAppData->DataShardExportFactory = app0->DataShardExportFactory;
            nodeAppData->DomainsInfo = app0->DomainsInfo;
            nodeAppData->ChannelProfiles = app0->ChannelProfiles;
            nodeAppData->Counters = node->DynamicCounters;
            nodeAppData->PollerThreads = node->Poller;
            nodeAppData->StreamingConfig.SetEnableOutputStreams(true);
            nodeAppData->PQConfig = app0->PQConfig;
            nodeAppData->NetClassifierConfig.CopyFrom(app0->NetClassifierConfig);
            nodeAppData->EnableKqpSpilling = app0->EnableKqpSpilling;
            nodeAppData->FeatureFlags = app0->FeatureFlags;
            nodeAppData->CompactionConfig = app0->CompactionConfig;
            nodeAppData->HiveConfig.SetWarmUpBootWaitingPeriod(10);
            nodeAppData->HiveConfig.SetMaxNodeUsageToKick(100);
            nodeAppData->HiveConfig.SetMinCounterScatterToBalance(100);
            nodeAppData->HiveConfig.SetMinScatterToBalance(100);
            nodeAppData->HiveConfig.SetObjectImbalanceToBalance(100);
            nodeAppData->HiveConfig.CopyFrom(app0->HiveConfig);
            nodeAppData->SchemeShardConfig = app0->SchemeShardConfig;
            nodeAppData->DataShardConfig = app0->DataShardConfig;
            nodeAppData->ColumnShardConfig = app0->ColumnShardConfig;
            nodeAppData->MeteringConfig = app0->MeteringConfig;
            nodeAppData->AwsCompatibilityConfig = app0->AwsCompatibilityConfig;
            nodeAppData->S3ProxyResolverConfig = app0->S3ProxyResolverConfig;
            nodeAppData->GraphConfig = app0->GraphConfig;
            nodeAppData->EnableMvccSnapshotWithLegacyDomainRoot = app0->EnableMvccSnapshotWithLegacyDomainRoot;
            nodeAppData->IoContextFactory = app0->IoContextFactory;
            if (nodeIndex < egg.Icb.size()) {
                nodeAppData->Icb = std::move(egg.Icb[nodeIndex]);
                nodeAppData->InFlightLimiterRegistry.Reset(new NKikimr::NGRpcService::TInFlightLimiterRegistry(nodeAppData->Icb));
            }
            if (KeyConfigGenerator) {
                nodeAppData->KeyConfig = KeyConfigGenerator(nodeIndex);
            } else {
                nodeAppData->KeyConfig.CopyFrom(app0->KeyConfig);
            }

            for (auto& callback : AppDataInit_) {
                callback(nodeIndex, *nodeAppData);
            }

            if (NeedMonitoring && !SingleSysEnv) {
                ui16 port = MonitoringPortOffset ? MonitoringPortOffset + nodeIndex : GetPortManager().GetPort();
                if (MonitoringTypeAsync) {
                    node->Mon.Reset(new NActors::TAsyncHttpMon({
                        .Port = port,
                        .Threads = 10,
                        .Title = "KIKIMR monitoring"
                    }));
                } else {
                    node->Mon.Reset(new NActors::TSyncHttpMon({
                        .Port = port,
                        .Threads = 10,
                        .Title = "KIKIMR monitoring"
                    }));
                }
                nodeAppData->Mon = node->Mon.Get();
                node->Mon->RegisterCountersPage("counters", "Counters", node->DynamicCounters);
                auto actorsMonPage = node->Mon->RegisterIndexPage("actors", "Actors");
                node->Mon->RegisterActorPage(actorsMonPage, "profiler", "Profiler", false, node->ActorSystem.Get(), MakeProfilerID(FirstNodeId + nodeIndex));
                const NActors::TActorId loggerActorId = NActors::TActorId(FirstNodeId + nodeIndex, "logger");
                node->Mon->RegisterActorPage(actorsMonPage, "logger", "Logger", false, node->ActorSystem.Get(), loggerActorId);
                MonPorts.push_back(port);
            }

            node->ActorSystem->Start();
            if (nodeAppData->Mon) {
                nodeAppData->Mon->Start(node->ActorSystem.Get());
            }
        }

        AppDataInit_.clear();
    }

    ui16 TTestActorRuntime::GetMonPort(ui32 nodeIndex) const {
        Y_ABORT_UNLESS(nodeIndex < MonPorts.size(), "Unknown MonPort for nodeIndex = %" PRIu32, nodeIndex);
        return MonPorts[nodeIndex];
    }

    void TTestActorRuntime::InitActorSystemSetup(TActorSystemSetup& setup, TNodeDataBase* node) {
        if (ActorSystemSetupConfig) {
            setup.Executors.Reset();
            setup.ExecutorsCount = 0;

            setup.CpuManager = ActorSystemSetupConfig->CpuManagerConfig;
            setup.MonitorStuckActors = ActorSystemSetupConfig->MonitorStuckActors;

            auto schedulerConfig = ActorSystemSetupConfig->SchedulerConfig;
            schedulerConfig.MonCounters = NKikimr::GetServiceCounters(node->DynamicCounters, "utils");
            setup.Scheduler.Reset(CreateSchedulerThread(schedulerConfig));
        }

        if (NeedMonitoring && NeedStatsCollectors) {
            NActors::IActor* statsCollector = NKikimr::CreateStatsCollector(1, setup, node->DynamicCounters);
            setup.LocalServices.push_back({
                TActorId(),
                NActors::TActorSetupCmd(statsCollector, NActors::TMailboxType::HTSwap, node->GetAppData<NKikimr::TAppData>()->SystemPoolId)
            });
        }
    }

    NKikimr::TAppData& TTestActorRuntime::GetAppData(ui32 nodeIndex) {
        TGuard<TMutex> guard(Mutex);
        Y_ABORT_UNLESS(nodeIndex < NodeCount);
        ui32 nodeId = FirstNodeId + nodeIndex;
        auto* node = GetNodeById(nodeId);
        return *node->GetAppData<NKikimr::TAppData>();
    }

    ui32 TTestActorRuntime::GetFirstNodeId() {
        return FirstNodeId;
    }

    bool TTestActorRuntime::DefaultScheduledFilterFunc(TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event, TDuration delay, TInstant& deadline) {
        Y_UNUSED(delay);
        Y_UNUSED(deadline);

        switch (event->GetTypeRewrite()) {
        case NKikimr::TEvBlobStorage::EvConfigureQueryTimeout:
        case NKikimr::TEvBlobStorage::EvEstablishingSessionTimeout:
            return true;
        case NKikimr::TEvBlobStorage::EvNotReadyRetryTimeout:
        case NKikimr::TEvTabletPipe::EvClientRetry:
        case NKikimr::TEvTabletPipe::EvClientCheckDelay:
        case NKikimr::TEvTabletBase::EvFollowerRetry:
        case NKikimr::TEvTabletBase::EvTryBuildFollowerGraph:
        case NKikimr::TEvTabletBase::EvTrySyncFollower:
            return false;
        case NKikimr::TEvents::TEvFlushLog::EventType:
            return false;
        default:
            break;
        }

        if (runtime.IsScheduleForActorEnabled(event->GetRecipientRewrite())) {
            return false;
        }

        return true;
    }

    void TTestActorRuntime::SimulateSleep(TDuration duration) {
        if (!SleepEdgeActor) {
            SleepEdgeActor = AllocateEdgeActor();
        }
        Schedule(new IEventHandle(SleepEdgeActor, SleepEdgeActor, new TEvents::TEvWakeup()), duration);
        GrabEdgeEventRethrow<TEvents::TEvWakeup>(SleepEdgeActor);
    }

    void TTestActorRuntime::SendToPipe(ui64 tabletId, const TActorId& sender, IEventBase* payload, ui32 nodeIndex, const NKikimr::NTabletPipe::TClientConfig& pipeConfig, TActorId clientId, ui64 cookie, NWilson::TTraceId traceId) {
        bool newPipe = (clientId == TActorId());
        if (newPipe) {
            clientId = ConnectToPipe(tabletId, sender, nodeIndex, pipeConfig);
        }

        SendToPipe(clientId, sender, payload, nodeIndex, cookie, std::move(traceId));

        if (newPipe) {
            ClosePipe(clientId, sender, nodeIndex);
        }
    }

    void TTestActorRuntime::SendToPipe(TActorId clientId, const TActorId& sender, IEventBase* payload,
                                       ui32 nodeIndex, ui64 cookie, NWilson::TTraceId traceId) {
        auto pipeEv = new IEventHandle(clientId, sender, payload, 0, cookie, nullptr, std::move(traceId));
        pipeEv->Rewrite(NKikimr::TEvTabletPipe::EvSend, clientId);
        Send(pipeEv, nodeIndex, true);
    }

    TActorId TTestActorRuntime::ConnectToPipe(ui64 tabletId, const TActorId& sender, ui32 nodeIndex, const NKikimr::NTabletPipe::TClientConfig& pipeConfig) {
        TActorId clientId = Register(NKikimr::NTabletPipe::CreateClient(sender, tabletId, pipeConfig), nodeIndex);
        if (!UseRealThreads) {
            EnableScheduleForActor(clientId, true);
        }
        return clientId;
    }

    void TTestActorRuntime::ClosePipe(TActorId clientId, const TActorId& sender, ui32 nodeIndex) {
        Send(new IEventHandle(clientId, sender, new NKikimr::TEvTabletPipe::TEvShutdown()), nodeIndex, true);
    }

    void TTestActorRuntime::DisconnectNodes(ui32 fromNodeIndex, ui32 toNodeIndex, bool async) {
        Send(new IEventHandle(
            GetInterconnectProxy(fromNodeIndex, toNodeIndex),
            TActorId(),
            new TEvInterconnect::TEvDisconnect()),
            fromNodeIndex, async);
    }

    TIntrusivePtr<::NMonitoring::TDynamicCounters> TTestActorRuntime::GetCountersForComponent(TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, const char* component) {
        return NKikimr::GetServiceCounters(counters, component);
    }

    void TTestActorRuntime::InitNodeImpl(TNodeDataBase* node, size_t) {
        node->LogSettings->Append(
            NActorsServices::EServiceCommon_MIN,
            NActorsServices::EServiceCommon_MAX,
            NActorsServices::EServiceCommon_Name
        );
        node->LogSettings->Append(
            NKikimrServices::EServiceKikimr_MIN,
            NKikimrServices::EServiceKikimr_MAX,
            NKikimrServices::EServiceKikimr_Name
        );
        // turn off some noisy components
        TString explanation;
        node->LogSettings->SetLevel(NLog::PRI_CRIT, NKikimrServices::BS_PROXY_DISCOVER, explanation);
        node->LogSettings->SetLevel(NLog::PRI_ERROR, NKikimrServices::TABLET_EXECUTOR, explanation);
        node->LogSettings->SetLevel(NLog::PRI_ERROR, NKikimrServices::BS_PROXY, explanation);
        node->LogSettings->SetLevel(NLog::PRI_CRIT, NKikimrServices::TABLET_MAIN, explanation);

    }
} // namespace NActors
