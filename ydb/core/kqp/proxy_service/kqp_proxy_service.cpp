#include "kqp_proxy_service.h"
#include "kqp_proxy_service_impl.h"
#include "kqp_script_executions.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/location.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/kqp/common/events/workload_service.h>
#include <ydb/core/kqp/common/kqp_lwtrace_probes.h>
#include <ydb/core/kqp/common/kqp_timeouts.h>
#include <ydb/core/kqp/compile_service/kqp_compile_service.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/kqp/session_actor/kqp_worker_common.h>
#include <ydb/core/kqp/node_service/kqp_node_service.h>
#include <ydb/core/kqp/workload_service/kqp_workload_service.h>
#include <ydb/core/resource_pools/resource_pool_settings.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/library/yql/dq/actors/spilling/spilling_file.h>
#include <ydb/library/yql/dq/actors/spilling/spilling.h>
#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/public/lib/operation_id/operation_id.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_actor.h>
#include <ydb/core/mon/mon.h>
#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/core/sys_view/common/schema.h>

#include <ydb/library/yql/utils/actor_log/log.h>
#include <ydb/library/yql/core/services/mounts/yql_mounts.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <library/cpp/lwtrace/mon/mon_lwtrace.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/resource/resource.h>

#include <util/folder/dirut.h>

namespace NKikimr::NKqp {

namespace {

#define KQP_PROXY_LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, stream)
#define KQP_PROXY_LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, stream)
#define KQP_PROXY_LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, stream)
#define KQP_PROXY_LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, stream)
#define KQP_PROXY_LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, stream)
#define KQP_PROXY_LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, stream)
#define KQP_PROXY_LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, stream)

TString MakeKqpProxyBoardPath(const TString& database) {
    return "kqpprx+" + database;
}


static constexpr TDuration DEFAULT_KEEP_ALIVE_TIMEOUT = TDuration::MilliSeconds(5000);
static constexpr TDuration DEFAULT_EXTRA_TIMEOUT_WAIT = TDuration::MilliSeconds(50);
static constexpr TDuration DEFAULT_CREATE_SESSION_TIMEOUT = TDuration::MilliSeconds(5000);

using VSessions = NKikimr::NSysView::Schema::QuerySessions;
using namespace NKikimrConfig;


std::optional<ui32> TryDecodeYdbSessionId(const TString& sessionId) {
    if (sessionId.empty()) {
        return std::nullopt;
    }

    try {
        NOperationId::TOperationId opId(sessionId);
        ui32 nodeId;
        const auto& nodeIds = opId.GetValue("node_id");
        if (nodeIds.size() != 1) {
            return std::nullopt;
        }

        if (!TryFromString(*nodeIds[0], nodeId)) {
            return std::nullopt;
        }

        return nodeId;
    } catch (...) {
        return std::nullopt;
    }

    return std::nullopt;
}

TString EncodeSessionId(ui32 nodeId, const TString& id) {
    Ydb::TOperationId opId;
    opId.SetKind(NOperationId::TOperationId::SESSION_YQL);
    NOperationId::AddOptionalValue(opId, "node_id", ToString(nodeId));
    NOperationId::AddOptionalValue(opId, "id", Base64Encode(id));
    return NOperationId::ProtoToString(opId);
}

class TKqpTempTablesAgentActor: public TActorBootstrapped<TKqpTempTablesAgentActor> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_PROXY_ACTOR;
    }

    explicit TKqpTempTablesAgentActor()
    {}

    void Bootstrap() {
        Become(&TKqpTempTablesAgentActor::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NSchemeShard::TEvSchemeShard::TEvOwnerActorAck, HandleNoop)
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    template<typename T>
    void HandleNoop(T&) {
    }
};

class TKqpProxyService : public TActorBootstrapped<TKqpProxyService> {
    struct TEvPrivate {
        enum EEv {
            EvReadyToPublishResources = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvCollectPeerProxyData,
            EvOnRequestTimeout,
            EvCloseIdleSessions,
            EvResourcesSnapshot,
        };

        struct TEvReadyToPublishResources : public TEventLocal<TEvReadyToPublishResources, EEv::EvReadyToPublishResources> {};
        struct TEvCollectPeerProxyData: public TEventLocal<TEvCollectPeerProxyData, EEv::EvCollectPeerProxyData> {};

        struct TEvOnRequestTimeout: public TEventLocal<TEvOnRequestTimeout, EEv::EvOnRequestTimeout> {
            ui64 RequestId;
            TDuration Timeout;
            NYql::NDqProto::StatusIds::StatusCode Status;
            int Round;

            TEvOnRequestTimeout(ui64 requestId, TDuration timeout, NYql::NDqProto::StatusIds::StatusCode status, int round)
                : RequestId(requestId)
                , Timeout(timeout)
                , Status(status)
                , Round(round)
            {}

            void TickNextRound() {
                ++Round;
                Timeout = DEFAULT_EXTRA_TIMEOUT_WAIT;
            }
        };

        struct TEvCloseIdleSessions : public TEventLocal<TEvCloseIdleSessions, EEv::EvCloseIdleSessions> {};

        struct TEvResourcesSnapshot : public TEventLocal<TEvResourcesSnapshot, EEv::EvResourcesSnapshot> {
            TVector<NKikimrKqp::TKqpNodeResources> Snapshot;

            TEvResourcesSnapshot(TVector<NKikimrKqp::TKqpNodeResources>&& snapshot)
                : Snapshot(std::move(snapshot)) {}
        };
    };

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_PROXY_ACTOR;
    }

    TKqpProxyService(const NKikimrConfig::TLogConfig& logConfig,
        const NKikimrConfig::TTableServiceConfig& tableServiceConfig,
        const NKikimrConfig::TQueryServiceConfig& queryServiceConfig,
        TVector<NKikimrKqp::TKqpSetting>&& settings,
        std::shared_ptr<IQueryReplayBackendFactory> queryReplayFactory,
        std::shared_ptr<TKqpProxySharedResources>&& kqpProxySharedResources,
        IKqpFederatedQuerySetupFactory::TPtr federatedQuerySetupFactory,
        std::shared_ptr<NYql::NDq::IS3ActorsFactory> s3ActorsFactory
        ): LogConfig(logConfig)
        , TableServiceConfig(tableServiceConfig)
        , QueryServiceConfig(queryServiceConfig)
        , FeatureFlags()
        , KqpSettings(std::make_shared<const TKqpSettings>(std::move(settings)))
        , FederatedQuerySetupFactory(federatedQuerySetupFactory)
        , QueryReplayFactory(std::move(queryReplayFactory))
        , PendingRequests()
        , ModuleResolverState()
        , KqpProxySharedResources(std::move(kqpProxySharedResources))
        , S3ActorsFactory(std::move(s3ActorsFactory))
    {}

    void Bootstrap(const TActorContext &ctx) {
        NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(KQP_PROVIDER));
        Counters = MakeIntrusive<TKqpCounters>(AppData()->Counters, &TlsActivationContext->AsActorContext());
        FeatureFlags = AppData()->FeatureFlags;
        // NOTE: some important actors are constructed within next call
        FederatedQuerySetup = FederatedQuerySetupFactory->Make(ctx.ActorSystem());
        AsyncIoFactory = CreateKqpAsyncIoFactory(Counters, FederatedQuerySetup, S3ActorsFactory);
        ModuleResolverState = MakeIntrusive<TModuleResolverState>();

        LocalSessions = std::make_unique<TLocalSessionsRegistry>(AppData()->RandomProvider);
        RandomProvider = AppData()->RandomProvider;
        if (!GetYqlDefaultModuleResolver(ModuleResolverState->ExprCtx, ModuleResolverState->ModuleResolver)) {
            TStringStream errorStream;
            ModuleResolverState->ExprCtx.IssueManager.GetIssues().PrintTo(errorStream);

            KQP_PROXY_LOG_E("Failed to load default YQL libraries: " << errorStream.Str());
            PassAway();
        }

        ModuleResolverState->FreezeGuardHolder =
            MakeHolder<NYql::TExprContext::TFreezeGuard>(ModuleResolverState->ExprCtx);

        UpdateYqlLogLevels();

        // Subscribe for TableService & Logger & FeatureFlags config changes
        ui32 tableServiceConfigKind = (ui32)NKikimrConsole::TConfigItem::TableServiceConfigItem;
        ui32 logConfigKind = (ui32)NKikimrConsole::TConfigItem::LogConfigItem;
        ui32 featureFlagsKind = (ui32)NKikimrConsole::TConfigItem::FeatureFlagsItem;
        Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
            new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest(
                {tableServiceConfigKind, logConfigKind, featureFlagsKind}),
            IEventHandle::FlagTrackDelivery);

        WhiteBoardService = NNodeWhiteboard::MakeNodeWhiteboardServiceId(SelfId().NodeId());

        if (auto& cfg = TableServiceConfig.GetSpillingServiceConfig().GetLocalFileConfig(); cfg.GetEnable()) {
            TString spillingRoot = cfg.GetRoot();
            if (spillingRoot.empty()) {
                spillingRoot = NYql::NDq::GetTmpSpillingRootForCurrentUser();
                MakeDirIfNotExist(spillingRoot);
            }

            SpillingService = TlsActivationContext->ExecutorThread.RegisterActor(NYql::NDq::CreateDqLocalFileSpillingService(
                NYql::NDq::TFileSpillingServiceConfig{
                    .Root = spillingRoot,
                    .MaxTotalSize = cfg.GetMaxTotalSize(),
                    .MaxFileSize = cfg.GetMaxFileSize(),
                    .MaxFilePartSize = cfg.GetMaxFilePartSize(),
                    .IoThreadPoolWorkersCount = cfg.GetIoThreadPool().GetWorkersCount(),
                    .IoThreadPoolQueueSize = cfg.GetIoThreadPool().GetQueueSize(),
                    .CleanupOnShutdown = false
                },
                Counters));
            TlsActivationContext->ExecutorThread.ActorSystem->RegisterLocalService(
                NYql::NDq::MakeDqLocalFileSpillingServiceID(SelfId().NodeId()), SpillingService);

            if (NActors::TMon* mon = AppData()->Mon) {
                NMonitoring::TIndexMonPage* actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
                mon->RegisterActorPage(actorsMonPage, "kqp_spilling_file", "KQP Local File Spilling Service", false,
                    TlsActivationContext->ExecutorThread.ActorSystem, SpillingService);
            }
        }

        // Create compile service
        CompileService = TlsActivationContext->ExecutorThread.RegisterActor(CreateKqpCompileService(TableServiceConfig, QueryServiceConfig,
            KqpSettings, ModuleResolverState, Counters, std::move(QueryReplayFactory), FederatedQuerySetup));
        TlsActivationContext->ExecutorThread.ActorSystem->RegisterLocalService(
            MakeKqpCompileServiceID(SelfId().NodeId()), CompileService);

        if (TableServiceConfig.GetEnableAsyncComputationPatternCompilation()) {
            IActor* ComputationPatternServiceActor = CreateKqpCompileComputationPatternService(TableServiceConfig, Counters);
            ui32 batchPoolId = AppData(ctx)->BatchPoolId;
            CompileComputationPatternService = ctx.Register(ComputationPatternServiceActor, TMailboxType::HTSwap, batchPoolId);
            TlsActivationContext->ExecutorThread.ActorSystem->RegisterLocalService(
                MakeKqpCompileComputationPatternServiceID(SelfId().NodeId()), CompileComputationPatternService);
        }

        ResourceManager_ = GetKqpResourceManager();
        CaFactory_ = NComputeActor::MakeKqpCaFactory(
            TableServiceConfig.GetResourceManager(), ResourceManager_, AsyncIoFactory, FederatedQuerySetup);

        KqpNodeService = TlsActivationContext->ExecutorThread.RegisterActor(CreateKqpNodeService(TableServiceConfig, ResourceManager_, CaFactory_, Counters, AsyncIoFactory, FederatedQuerySetup));
        TlsActivationContext->ExecutorThread.ActorSystem->RegisterLocalService(
            MakeKqpNodeServiceID(SelfId().NodeId()), KqpNodeService);

        KqpWorkloadService = TlsActivationContext->ExecutorThread.RegisterActor(CreateKqpWorkloadService(Counters->GetWorkloadManagerCounters()));
        TlsActivationContext->ExecutorThread.ActorSystem->RegisterLocalService(
            MakeKqpWorkloadServiceId(SelfId().NodeId()), KqpWorkloadService);

        NActors::TMon* mon = AppData()->Mon;
        if (mon) {
            NMonitoring::TIndexMonPage* actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
            mon->RegisterActorPage(actorsMonPage, "kqp_proxy", "KQP Proxy", false,
                TlsActivationContext->ExecutorThread.ActorSystem, SelfId());
        }

        KqpRmServiceActor = MakeKqpRmServiceID(SelfId().NodeId());

        KqpTempTablesAgentActor = Register(new TKqpTempTablesAgentActor());

        Become(&TKqpProxyService::MainState);
        StartCollectPeerProxyData();
        PublishResourceUsage();
        AskSelfNodeInfo();
        SendWhiteboardRequest();
        ScheduleIdleSessionCheck(TDuration::Seconds(2));
    }

    TDuration GetSessionIdleDuration() const {
        return TDuration::Seconds(TableServiceConfig.GetSessionIdleDurationSeconds());
    }

    void ScheduleIdleSessionCheck(const TDuration& scheduleInterval) {
        if (!ShutdownState) {
            Schedule(scheduleInterval, new TEvPrivate::TEvCloseIdleSessions());
        }
    }

    void Handle(TEvPrivate::TEvCloseIdleSessions::TPtr&) {
        bool hasMoreToShutdown = CheckIdleSessions();
        if (hasMoreToShutdown) {
            // we already performed several session shutdowns, but there are many sessions to
            // be shutdowned. so we need to speadup the process.
            static const TDuration quickIdleCheckInterval = TDuration::MilliSeconds(10);
            ScheduleIdleSessionCheck(quickIdleCheckInterval);
        } else {
            static const TDuration defaultIdleCheckInterval = TDuration::Seconds(2);
            ScheduleIdleSessionCheck(defaultIdleCheckInterval);
            SendWhiteboardStats();
        }
    }

    bool CheckIdleSessions(const ui32 maxSessionsToClose = 10) {
        ui32 closedIdleSessions = 0;
        const NActors::TMonotonic now = TActivationContext::Monotonic();
        while(true) {
            const TKqpSessionInfo* sessionInfo = LocalSessions->GetIdleSession(now);
            if (sessionInfo == nullptr)
                return false;

            Counters->ReportSessionActorClosedIdle(sessionInfo->DbCounters);
            LocalSessions->StopIdleCheck(sessionInfo);
            SendSessionClose(sessionInfo);
            ++closedIdleSessions;

            if (closedIdleSessions > maxSessionsToClose) {
                return true;
            }
        }
    }

    void SendSessionClose(const TKqpSessionInfo* sessionInfo) {
        auto closeSessionEv = std::make_unique<TEvKqp::TEvCloseSessionRequest>();
        closeSessionEv->Record.MutableRequest()->SetSessionId(sessionInfo->SessionId);
        Send(sessionInfo->WorkerId, closeSessionEv.release());
    }

    void AskSelfNodeInfo() {
        Send(GetNameserviceActorId(), new TEvInterconnect::TEvGetNode(SelfId().NodeId()));
    }

    void Handle(TEvInterconnect::TEvNodeInfo::TPtr& ev) {
        if (const auto& node = ev->Get()->Node) {
            SelfDataCenterId = node->Location.GetDataCenterId();
        } else {
            SelfDataCenterId = TString();
        }

        NodeResources.SetNodeId(SelfId().NodeId());
        NodeResources.SetDataCenterNumId(DataCenterFromString(*SelfDataCenterId));
        NodeResources.SetDataCenterId(*SelfDataCenterId);
        PublishResourceUsage();
    }

    void StartCollectPeerProxyData() {
        Send(SelfId(), new TEvPrivate::TEvCollectPeerProxyData());
    }

    void SendBoardPublishPoison(){
        if (BoardPublishActor) {
            Send(BoardPublishActor, new TEvents::TEvPoison);
            BoardPublishActor = TActorId();
            PublishBoardPath = TString();
        }
    }

    void SendWhiteboardRequest() {
        auto ev = std::make_unique<NNodeWhiteboard::TEvWhiteboard::TEvSystemStateRequest>();
        Send(WhiteBoardService, ev.release(), IEventHandle::FlagTrackDelivery, SelfId().NodeId());
    }

    void Handle(NNodeWhiteboard::TEvWhiteboard::TEvSystemStateResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (record.SystemStateInfoSize() != 1)  {
            KQP_PROXY_LOG_C("Unexpected whiteboard info");
            return;
        }

        const auto& info = record.GetSystemStateInfo(0);
        if (AppData()->UserPoolId >= info.PoolStatsSize()) {
            KQP_PROXY_LOG_D("Unexpected whiteboard info: pool size is smaller than user pool id"
                << ", pool size: " << info.PoolStatsSize()
                << ", user pool id: " << AppData()->UserPoolId);
            return;
        }

        const auto& pool = info.GetPoolStats(AppData()->UserPoolId);

        KQP_PROXY_LOG_D("Received node white board pool stats: " << pool.usage());
        NodeResources.SetCpuUsage(pool.usage());
        NodeResources.SetThreads(pool.threads());

        PublishResourceUsage();
    }

    void DoPublishResources() {
        SendWhiteboardRequest();

        if (AppData()->TenantName.empty() || !SelfDataCenterId) {
            KQP_PROXY_LOG_I("Cannot start publishing usage, tenants: " << AppData()->TenantName << ", " <<  SelfDataCenterId.value_or("empty"));
            return;
        }

        SendBoardPublishPoison();

        if (TableServiceConfig.GetEnablePublishKqpProxyByRM()) {
            LastPublishResourcesAt = TAppData::TimeProvider->Now();
            Send(KqpRmServiceActor, std::make_unique<TEvKqp::TEvKqpProxyPublishRequest>());
            return;
        }

        NodeResources.SetActiveWorkersCount(LocalSessions->size());
        PublishBoardPath = MakeKqpProxyBoardPath(AppData()->TenantName);
        auto actor = CreateBoardPublishActor(PublishBoardPath, NodeResources.SerializeAsString(), SelfId(), 0, true);
        BoardPublishActor = Register(actor);
        LastPublishResourcesAt = TAppData::TimeProvider->Now();
    }

    void PublishResourceUsage() {
        if (ResourcesPublishScheduled) {
            return;
        }

        const auto& sbs = TableServiceConfig.GetSessionBalancerSettings();
        auto now = TAppData::TimeProvider->Now();
        TDuration batchingInterval = TDuration::MilliSeconds(sbs.GetBoardPublishIntervalMs());

        if (LastPublishResourcesAt && now - *LastPublishResourcesAt < batchingInterval) {
            ResourcesPublishScheduled = true;
            Schedule(batchingInterval, new TEvPrivate::TEvReadyToPublishResources());
            return;
        }

        DoPublishResources();
    }

    void Handle(TEvPrivate::TEvReadyToPublishResources::TPtr&) {
        ResourcesPublishScheduled = false;
        DoPublishResources();
    }

    void PassAway() override {
        Send(CompileService, new TEvents::TEvPoisonPill());

        Send(KqpTempTablesAgentActor, new TEvents::TEvPoisonPill());

        if (TableServiceConfig.GetEnableAsyncComputationPatternCompilation()) {
            Send(CompileComputationPatternService, new TEvents::TEvPoisonPill());
        }

        Send(SpillingService, new TEvents::TEvPoison);
        Send(KqpNodeService, new TEvents::TEvPoison);
        if (BoardPublishActor) {
            Send(BoardPublishActor, new TEvents::TEvPoison);
        }

        Send(KqpWorkloadService, new TEvents::TEvPoison());

        LocalSessions->ForEachNode([this](TNodeId node) {
            Send(TActivationContext::InterconnectProxy(node), new TEvents::TEvUnsubscribe);
        });

        return TActor::PassAway();
    }

    void Handle(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr&) {
        KQP_PROXY_LOG_D("Subscribed for config changes.");
    }

    void Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
        auto &event = ev->Get()->Record;

        TableServiceConfig.Swap(event.MutableConfig()->MutableTableServiceConfig());
        KQP_PROXY_LOG_D("Updated table service config.");

        LogConfig.Swap(event.MutableConfig()->MutableLogConfig());
        UpdateYqlLogLevels();

        FeatureFlags.Swap(event.MutableConfig()->MutableFeatureFlags());

        auto responseEv = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationResponse>(event);
        Send(ev->Sender, responseEv.Release(), IEventHandle::FlagTrackDelivery, ev->Cookie);
        PublishResourceUsage();
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        switch (ev->Get()->SourceType) {
            case NConsole::TEvConfigsDispatcher::EvSetConfigSubscriptionRequest:
                KQP_PROXY_LOG_C("Failed to deliver subscription request to config dispatcher.");
                break;

            case NConsole::TEvConsole::EvConfigNotificationResponse:
                KQP_PROXY_LOG_E("Failed to deliver config notification response.");
                break;

            case NNodeWhiteboard::TEvWhiteboard::EvSystemStateRequest:
                KQP_PROXY_LOG_D("Failed to get system details");
                break;

            case TKqpEvents::EvCreateSessionRequest: {
                KQP_PROXY_LOG_D("Remote create session request failed");
                ReplyProcessError(Ydb::StatusIds::UNAVAILABLE, "Session not found.", ev->Cookie);
                break;
            }

            case TKqpEvents::EvQueryRequest:
            case TKqpEvents::EvPingSessionRequest: {
                KQP_PROXY_LOG_D("Session not found, targetId: " << ev->Sender << " requestId: " << ev->Cookie);

                ReplyProcessError(Ydb::StatusIds::BAD_SESSION, "Session not found.", ev->Cookie);
                RemoveSession("", ev->Sender);
                break;
            }

            default:
                KQP_PROXY_LOG_E("Undelivered event with unexpected source type: " << ev->Get()->SourceType);
                break;
        }
    }

    void Handle(TEvKqp::TEvInitiateShutdownRequest::TPtr& ev) {
        KQP_PROXY_LOG_N("KQP proxy shutdown requested.");
        ShutdownRequested = true;
        ShutdownState.Reset(ev->Get()->ShutdownState.Get());
        ShutdownState->Update(LocalSessions->size());
        auto& shs = TableServiceConfig.GetShutdownSettings();
        ui32 hardTimeout = shs.GetHardTimeoutMs();
        ui32 softTimeout = shs.GetSoftTimeoutMs();
        for(auto& [idx, sessionInfo] : *LocalSessions) {
            Send(sessionInfo.WorkerId, new TEvKqp::TEvInitiateSessionShutdown(softTimeout, hardTimeout));
        }
    }

    bool CreateRemoteSession(TEvKqp::TEvCreateSessionRequest::TPtr& ev, ui64 requestId) {
        auto& event = ev->Get()->Record;
        if (!event.GetCanCreateRemoteSession() || LocalDatacenterProxies.empty()) {
            return false;
        }

        const auto& sbs = TableServiceConfig.GetSessionBalancerSettings();
        if (!sbs.GetSupportRemoteSessionCreation()) {
            return false;
        }

        ui64 randomNumber = RandomProvider->GenRand();
        ui32 nodeId = LocalDatacenterProxies[randomNumber % LocalDatacenterProxies.size()];
        if (nodeId == SelfId().NodeId()){
            return false;
        }

        std::unique_ptr<TEvKqp::TEvCreateSessionRequest> remoteRequest = std::make_unique<TEvKqp::TEvCreateSessionRequest>();
        remoteRequest->Record.SetDeadlineUs(event.GetDeadlineUs());
        remoteRequest->Record.SetTraceId(event.GetTraceId());
        remoteRequest->Record.SetSupportsBalancing(event.GetSupportsBalancing());
        remoteRequest->Record.MutableRequest()->SetDatabase(event.GetRequest().GetDatabase());

        Send(MakeKqpProxyID(nodeId), remoteRequest.release(), IEventHandle::FlagTrackDelivery, requestId);
        TDuration timeout = DEFAULT_CREATE_SESSION_TIMEOUT;
        StartQueryTimeout(requestId, timeout);
        return true;
    }

    void Handle(TEvKqp::TEvCreateSessionRequest::TPtr& ev) {
        auto& event = ev->Get()->Record;
        auto& request = event.GetRequest();
        TKqpRequestInfo requestInfo(event.GetTraceId());
        ui64 requestId = PendingRequests.RegisterRequest(ev->Sender, ev->Cookie, event.GetTraceId(), TKqpEvents::EvCreateSessionRequest);
        if (CreateRemoteSession(ev, requestId)) {
            return;
        }

        auto responseEv = MakeHolder<TEvKqp::TEvCreateSessionResponse>();
        // If we create many sessions per second, it might be ok to check and close
        // several idle sessions
        CheckIdleSessions(3);

        TProcessResult<TKqpSessionInfo*> result;
        TKqpDbCountersPtr dbCounters;

        const auto deadline = TInstant::MicroSeconds(event.GetDeadlineUs());

        if (CheckRequestDeadline(requestInfo, deadline, result) &&
            CreateNewSessionWorker(requestInfo, TString(DefaultKikimrPublicClusterName), true, request.GetDatabase(),
                event.GetSupportsBalancing(), event.GetPgWire(),
                event.GetClientAddress(), event.GetUserSID(), event.GetClientUserAgent(), event.GetClientSdkBuildInfo(),
                event.GetClientPID(),
                event.GetApplicationName(), event.GetUserName(), result))
        {
            auto& response = *responseEv->Record.MutableResponse();
            response.SetSessionId(result.Value->SessionId);
            response.SetNodeId(SelfId().NodeId());
            dbCounters = result.Value->DbCounters;
        } else {
            dbCounters = Counters->GetDbCounters(request.GetDatabase());
        }

        Counters->ReportCreateSession(dbCounters, request.ByteSize());
        KQP_PROXY_LOG_D("Received create session request, trace_id: " << event.GetTraceId());

        responseEv->Record.SetResourceExhausted(result.ResourceExhausted);
        responseEv->Record.SetYdbStatus(result.YdbStatus);
        responseEv->Record.SetError(result.Error);

        PendingRequests.Erase(requestId);
        LogResponse(event.GetTraceId(), responseEv->Record, dbCounters);
        Send(ev->Sender, responseEv.Release(), 0, ev->Cookie);
    }

    void Handle(TEvKqp::TEvQueryRequest::TPtr& ev) {
        const TString& database = ev->Get()->GetDatabase();
        const TString& traceId = ev->Get()->GetTraceId();
        const auto queryType = ev->Get()->GetType();
        const auto queryAction = ev->Get()->GetAction();
        TKqpRequestInfo requestInfo(traceId);
        ui64 requestId = PendingRequests.RegisterRequest(ev->Sender, ev->Cookie, traceId, TKqpEvents::EvQueryRequest);
        bool explicitSession = true;
        if (ev->Get()->GetSessionId().empty()) {
            TProcessResult<TKqpSessionInfo*> result;
            if (!CreateNewSessionWorker(requestInfo, TString(DefaultKikimrPublicClusterName), false,
                database, false, false, "", "", "", "", "", "", Nothing(), result))
            {
                ReplyProcessError(result.YdbStatus, result.Error, requestId);
                return;
            }
            explicitSession = false;
            ev->Get()->SetSessionId(result.Value->SessionId);
        }

        const TString& sessionId = ev->Get()->GetSessionId();

        if (!ev->Get()->GetUserRequestContext()) {
            ev->Get()->SetUserRequestContext(MakeIntrusive<TUserRequestContext>(traceId, database, sessionId));
        }

        const TKqpSessionInfo* sessionInfo = LocalSessions->FindPtr(sessionId);
        auto dbCounters = sessionInfo ? sessionInfo->DbCounters : nullptr;
        if (!dbCounters) {
            dbCounters = Counters->GetDbCounters(database);
        }

        if (queryType == NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY ||
            queryType == NKikimrKqp::QUERY_TYPE_SQL_GENERIC_CONCURRENT_QUERY) {

            if (explicitSession &&
                sessionInfo &&
                !sessionInfo->PgWire && // pg wire bypasses rpc layer and doesn't perform attach
                !sessionInfo->AttachedRpcId)
            {
                TString error = "Attempt to execute query on explicit session without attach";
                ReplyProcessError(Ydb::StatusIds::BAD_REQUEST, error, requestId);
                return;
            }
        }

        PendingRequests.SetSessionId(requestId, sessionId, dbCounters);
        Counters->ReportQueryRequest(dbCounters, ev->Get()->GetRequestSize(), ev->Get()->GetParametersSize(), ev->Get()->GetQuerySize());
        Counters->ReportQueryAction(dbCounters, queryAction);
        Counters->ReportQueryType(dbCounters, queryType);

        auto queryLimitBytes = TableServiceConfig.GetQueryLimitBytes();
        if (queryLimitBytes && IsSqlQuery(queryType) && ev->Get()->GetQuerySize() > queryLimitBytes) {
            TString error = TStringBuilder() << "Query text size exceeds limit ("
                << ev->Get()->GetQuerySize() << "b > " << queryLimitBytes << "b)";
            ReplyProcessError(Ydb::StatusIds::BAD_REQUEST, error, requestId);
            return;
        }

        auto paramsLimitBytes = TableServiceConfig.GetParametersLimitBytes();
        if (paramsLimitBytes && ev->Get()->GetParametersSize() > paramsLimitBytes) {
            TString error = TStringBuilder() << "Parameters size exceeds limit ("
                << ev->Get()->GetParametersSize() << "b > " << paramsLimitBytes << "b)";
            ReplyProcessError(Ydb::StatusIds::BAD_REQUEST, error, requestId);
            return;
        }

        if (sessionInfo) {
            LocalSessions->AttachQueryText(sessionInfo, ev->Get()->GetQuery());
        }

        if (!TryGetPoolInfoFromCache(ev, requestId)) {
            return;
        }

        TActorId targetId;
        if (sessionInfo) {
            targetId = sessionInfo->WorkerId;
            LocalSessions->StopIdleCheck(sessionInfo);
        } else {
            targetId = TryGetSessionTargetActor(sessionId, requestInfo, requestId);
            if (!targetId) {
                return;
            }
        }

        auto cancelAfter = ev->Get()->GetCancelAfter();
        auto timeout = ev->Get()->GetOperationTimeout();
        auto timerDuration = GetQueryTimeout(queryType, timeout.MilliSeconds(), TableServiceConfig, QueryServiceConfig);
        if (cancelAfter) {
            timerDuration = Min(timerDuration, cancelAfter);
        }
        KQP_PROXY_LOG_D("Ctx: " << *ev->Get()->GetUserRequestContext() << ". TEvQueryRequest, set timer for: " << timerDuration
            << " timeout: " << timeout << " cancelAfter: " << cancelAfter
            << ". " << "Send request to target, requestId: " << requestId << ", targetId: " << targetId);
        auto status = timerDuration == cancelAfter ? NYql::NDqProto::StatusIds::CANCELLED : NYql::NDqProto::StatusIds::TIMEOUT;
        StartQueryTimeout(requestId, timerDuration, status);
        Send(targetId, ev->Release().Release(), IEventHandle::FlagTrackDelivery, requestId, std::move(ev->TraceId));
    }

    void Handle(TEvKqp::TEvScriptRequest::TPtr& ev) {
        if (CheckScriptExecutionsTablesReady<TEvKqp::TEvScriptResponse>(ev)) {
            auto req = ev->Get()->Record.MutableRequest();
            auto maxRunTime = GetQueryTimeout(req->GetType(), req->GetTimeoutMs(), TableServiceConfig, QueryServiceConfig);
            req->SetTimeoutMs(maxRunTime.MilliSeconds());
            if (req->GetCancelAfterMs()) {
                maxRunTime = TDuration::MilliSeconds(Min(req->GetCancelAfterMs(), maxRunTime.MilliSeconds()));
            }
            Register(CreateScriptExecutionCreatorActor(std::move(ev), QueryServiceConfig, Counters, maxRunTime), TMailboxType::HTSwap, AppData()->SystemPoolId);
        }
    }

    void Handle(TEvKqp::TEvCloseSessionRequest::TPtr& ev) {
        auto& event = ev->Get()->Record;
        auto& request = event.GetRequest();

        TKqpRequestInfo requestInfo(event.GetTraceId());

        TString sessionId = request.GetSessionId();
        const TKqpSessionInfo* sessionInfo = LocalSessions->FindPtr(sessionId);
        auto dbCounters = sessionInfo ? sessionInfo->DbCounters : nullptr;

        Counters->ReportCloseSession(dbCounters, request.ByteSize());

        if (LocalSessions->IsPendingShutdown(sessionId) && dbCounters) {
            Counters->ReportSessionGracefulShutdownHit(dbCounters);
        }

        if (sessionInfo) {
            Send(sessionInfo->WorkerId, ev->Release().Release());
        } else {
            if (!sessionId.empty()) {
                TActorId targetId = TryGetSessionTargetActor(sessionId, requestInfo, 0);
                if (targetId) {
                    Send(targetId, ev->Release().Release());
                }
            }
        }
    }

    void Handle(TEvKqp::TEvPingSessionRequest::TPtr& ev) {
        auto& event = ev->Get()->Record;
        auto& request = event.GetRequest();

        const auto traceId = event.GetTraceId();
        TKqpRequestInfo requestInfo(traceId);
        const auto sessionId = request.GetSessionId();
        // If set rpc layer will controll session lifetime
        const TActorId ctrlActor = request.HasExtSessionCtrlActorId()
            ? ActorIdFromProto(request.GetExtSessionCtrlActorId())
            : TActorId();
        const TKqpSessionInfo* sessionInfo = LocalSessions->FindPtr(sessionId);
        auto dbCounters = sessionInfo ? sessionInfo->DbCounters : nullptr;
        Counters->ReportPingSession(dbCounters, request.ByteSize());

        // Local session
        if (sessionInfo) {
            const bool sameNode = ev->Sender.NodeId() == SelfId().NodeId();
            KQP_PROXY_LOG_D("Received ping session request, has local session: " << sessionId
                << ", rpc ctrl: " << ctrlActor
                << ", sameNode: " << sameNode
                << ", trace_id: " << traceId);

            const bool isIdle = LocalSessions->IsSessionIdle(sessionInfo);
            if (isIdle) {
                LocalSessions->StopIdleCheck(sessionInfo);
                if (!ctrlActor) {
                    LocalSessions->StartIdleCheck(sessionInfo, GetSessionIdleDuration());
                }
            }

            auto result = std::make_unique<TEvKqp::TEvPingSessionResponse>();
            auto& record = result->Record;
            record.SetStatus(Ydb::StatusIds::SUCCESS);
            auto sessionStatus = isIdle
                ? Ydb::Table::KeepAliveResult::SESSION_STATUS_READY
                : Ydb::Table::KeepAliveResult::SESSION_STATUS_BUSY;
            record.MutableResponse()->SetSessionStatus(sessionStatus);
            if (ctrlActor && isIdle) {
                //TODO: fix
                ui32 flags = IEventHandle::FlagTrackDelivery;
                if (sameNode) {
                    KQP_PROXY_LOG_T("Attach local session: " << sessionInfo->WorkerId
                        << " to rpc: " << ctrlActor << " on same node");

                    LocalSessions->AttachSession(sessionInfo, 0, ctrlActor);
                } else {
                    const TNodeId nodeId = ev->Sender.NodeId();
                    KQP_PROXY_LOG_T("Subscribe local session: " << sessionInfo->WorkerId
                        << " to remote: " << ev->Sender << " , nodeId: " << nodeId << ", with rpc: " << ctrlActor);

                    LocalSessions->AttachSession(sessionInfo, nodeId, ctrlActor);

                    flags |= IEventHandle::FlagSubscribeOnSession;
                }
                Send(ev->Sender, result.release(), flags, ev->Cookie);
            } else {
                Send(ev->Sender, result.release(), 0, ev->Cookie);
            }
            return;
        }

        // Forward request to another proxy
        ui64 requestId = PendingRequests.RegisterRequest(ev->Sender, ev->Cookie, traceId, TKqpEvents::EvPingSessionRequest);

        KQP_PROXY_LOG_D("Received ping session request, request_id: " << requestId
            << ", sender: " << ev->Sender
            << ", trace_id: " << traceId);

        const TActorId targetId = TryGetSessionTargetActor(sessionId, requestInfo, requestId);
        if (!targetId) {
            return;
        }

        TDuration timeout = DEFAULT_KEEP_ALIVE_TIMEOUT;
        if (request.GetTimeoutMs() > 0) {
            timeout = TDuration::MilliSeconds(Min(timeout.MilliSeconds(), (ui64)request.GetTimeoutMs()));
        }

        PendingRequests.SetSessionId(requestId, sessionId, dbCounters);
        StartQueryTimeout(requestId, timeout);
        Send(targetId, ev->Release().Release(), IEventHandle::FlagTrackDelivery, requestId);
    }

    void Handle(TEvKqp::TEvCancelQueryRequest::TPtr& ev) {
        auto& event = ev->Get()->Record;
        auto& request = event.GetRequest();

        auto traceId = event.GetTraceId();
        TKqpRequestInfo requestInfo(traceId);
        auto sessionId = request.GetSessionId();
        ui64 requestId = PendingRequests.RegisterRequest(ev->Sender, ev->Cookie, traceId, TKqpEvents::EvCancelQueryRequest);
        const TKqpSessionInfo* sessionInfo = LocalSessions->FindPtr(sessionId);
        auto dbCounters = sessionInfo ? sessionInfo->DbCounters : nullptr;
        KQP_PROXY_LOG_D("Received cancel query request, request_id: " << requestId << ", trace_id: " << traceId);
        Counters->ReportCancelQuery(dbCounters, request.ByteSize());

        PendingRequests.SetSessionId(requestId, sessionId, dbCounters);

        TActorId targetId;
        if (sessionInfo) {
            targetId = sessionInfo->WorkerId;
            LocalSessions->StopIdleCheck(sessionInfo);
        } else {
            targetId = TryGetSessionTargetActor(sessionId, requestInfo, requestId);
            if (!targetId) {
                return;
            }
        }

        Send(targetId, ev->Release().Release(), IEventHandle::FlagTrackDelivery, requestId);
        KQP_PROXY_LOG_D("Sent request to target, requestId: " << requestId
            << ", targetId: " << targetId << ", sessionId: " << sessionId);
    }

    template<typename TEvent>
    void ForwardEvent(TEvent ev) {
        ui64 requestId = ev->Cookie;

        StopQueryTimeout(requestId);
        auto proxyRequest = PendingRequests.FindPtr(requestId);
        if (!proxyRequest) {
            KQP_PROXY_LOG_E("Unknown sender for proxy response, requestId: " << requestId);
            return;
        }

        const TKqpSessionInfo* info = LocalSessions->FindPtr(proxyRequest->SessionId);
        if (info && !info->AttachedRpcId) {
            LocalSessions->StartIdleCheck(info, GetSessionIdleDuration());
        }

        Send(proxyRequest->Sender, ev->Release().Release(), 0, proxyRequest->SenderCookie);

        if (info && proxyRequest->EventType == TKqpEvents::EvQueryRequest) {
            LocalSessions->DetachQueryText(info);
        }

        TKqpRequestInfo requestInfo(proxyRequest->TraceId);
        KQP_PROXY_LOG_D(requestInfo << "Forwarded response to sender actor, requestId: " << requestId
            << ", sender: " << proxyRequest->Sender << ", selfId: " << SelfId() << ", source: " << ev->Sender);

        PendingRequests.Erase(requestId);
    }

    void ForwardProgress(TEvKqpExecuter::TEvExecuterProgress::TPtr& ev) {
        ui64 requestId = ev->Cookie;

        auto proxyRequest = PendingRequests.FindPtr(requestId);
        if (!proxyRequest) {
            KQP_PROXY_LOG_E("Unknown sender for proxy response, requestId: " << requestId);
            return;
        }

        Send(proxyRequest->Sender, ev->Release().Release(), 0, proxyRequest->SenderCookie);

        TKqpRequestInfo requestInfo(proxyRequest->TraceId);
        KQP_PROXY_LOG_D(requestInfo << "Forwarded response to sender actor, requestId: " << requestId
            << ", sender: " << proxyRequest->Sender << ", selfId: " << SelfId() << ", source: " << ev->Sender);
    }

    void LookupPeerProxyData() {
        if (!SelfDataCenterId || BoardLookupActor || AppData()->TenantName.empty()) {
            return;
        }

        if (PublishBoardPath) {
            auto actor = CreateBoardLookupActor(PublishBoardPath, SelfId(), EBoardLookupMode::Majority);
            BoardLookupActor = Register(actor);
        }
    }

    void Handle(TEvPrivate::TEvCollectPeerProxyData::TPtr&) {
        if (!TableServiceConfig.GetEnablePublishKqpProxyByRM()) {
            LookupPeerProxyData();
        } else {
            if (SelfDataCenterId && !AppData()->TenantName.empty() && !IsLookupByRmScheduled) {
                IsLookupByRmScheduled = true;
                GetKqpResourceManager()->RequestClusterResourcesInfo(
                    [as = TlsActivationContext->ActorSystem(), self = SelfId()](TVector<NKikimrKqp::TKqpNodeResources>&& resources) {
                        TAutoPtr<IEventHandle> eh = new IEventHandle(self, self, new TEvPrivate::TEvResourcesSnapshot(std::move(resources)));
                        as->Send(eh);
                    });
            }
        }
        if (!ShutdownRequested) {
            const auto& sbs = TableServiceConfig.GetSessionBalancerSettings();
            ui64 millis = sbs.GetBoardLookupIntervalMs();
            TDuration d = TDuration::MilliSeconds(millis + (RandomProvider->GenRand() % millis));
            Schedule(d, new TEvPrivate::TEvCollectPeerProxyData());
        }
    }

    void Handle(TEvStateStorage::TEvBoardInfo::TPtr& ev) {
        auto boardInfo = ev->Get();
        BoardLookupActor = TActorId();

        if (boardInfo->Status != TEvStateStorage::TEvBoardInfo::EStatus::Ok || PublishBoardPath != boardInfo->Path) {
            PeerProxyNodeResources.clear();
            KQP_PROXY_LOG_D("Received unexpected data from board: " << boardInfo->Path << ", current board path "
                << PublishBoardPath << ", status: " << (int) boardInfo->Status);
            return;
        }

        Y_ABORT_UNLESS(SelfDataCenterId);
        PeerProxyNodeResources.resize(boardInfo->InfoEntries.size());
        size_t idx = 0;
        auto getDataCenterId = [](const auto& entry) {
            return entry.HasDataCenterId() ? entry.GetDataCenterId() : DataCenterToString(entry.GetDataCenterNumId());
        };

        LocalDatacenterProxies.clear();
        for(auto& [ownerId, entry] : boardInfo->InfoEntries) {
            Y_PROTOBUF_SUPPRESS_NODISCARD PeerProxyNodeResources[idx].ParseFromString(entry.Payload);
            if (getDataCenterId(PeerProxyNodeResources[idx]) == *SelfDataCenterId) {
                LocalDatacenterProxies.emplace_back(PeerProxyNodeResources[idx].GetNodeId());
            }
            ++idx;
        }

        PeerStats = CalcPeerStats(PeerProxyNodeResources, *SelfDataCenterId);
        TryKickSession();
    }

    void Handle(TEvPrivate::TEvResourcesSnapshot::TPtr& ev) {
        IsLookupByRmScheduled = false;

        TVector<NKikimrKqp::TKqpProxyNodeResources> proxyResources;
        std::vector<ui64> localDatacenterProxies;
        proxyResources.reserve(ev->Get()->Snapshot.size());

        auto getDataCenterId = [](const auto& entry) {
            return entry.HasDataCenterId() ? entry.GetDataCenterId() : DataCenterToString(entry.GetDataCenterNumId());
        };

        for(auto& nodeResources : ev->Get()->Snapshot) {
            auto* proxyNodeResources = nodeResources.MutableKqpProxyNodeResources();

            if (proxyNodeResources->HasNodeId()) {
                proxyResources.push_back(std::move(*proxyNodeResources));
                if (getDataCenterId(proxyResources.back()) == *SelfDataCenterId) {
                    localDatacenterProxies.emplace_back(proxyResources.back().GetNodeId());
                }
            }
        }

        if (proxyResources.empty()) {
            PeerProxyNodeResources.clear();
            KQP_PROXY_LOG_D("Received unexpected data from rm for database " <<
                AppData()->TenantName);
            return;
        }

        Y_ABORT_UNLESS(SelfDataCenterId);
        PeerProxyNodeResources = std::move(proxyResources);
        LocalDatacenterProxies = std::move(localDatacenterProxies);

        PeerStats = CalcPeerStats(PeerProxyNodeResources, *SelfDataCenterId);
        TryKickSession();
    }

    bool ShouldStartBalancing(const TSimpleResourceStats& stats, const double minResourceThreshold, const double currentResourceUsage) const {
        const auto& sbs = TableServiceConfig.GetSessionBalancerSettings();
        if (stats.CV < sbs.GetMinCVTreshold()) {
            return false;
        }

        if (stats.CV < sbs.GetMaxCVTreshold() && ServerWorkerBalancerComplete) {
            return false;
        }

        if (stats.Mean < currentResourceUsage && minResourceThreshold < currentResourceUsage) {
            return true;
        }

        return false;
    }

    std::pair<bool, ui32> GetBalancerEnableSettings() const {
        const auto& sbs = TableServiceConfig.GetSessionBalancerSettings();
        ui32 maxInFlightSize = sbs.GetMaxSessionsShutdownInFlightSize();
        bool force = false;

        auto tier = sbs.GetEnableTier();
        if (sbs.GetEnabled()) {
            // it's legacy configuration.
            tier = TTableServiceConfig_TSessionBalancerSettings::TIER_ENABLED_FOR_ALL;
        }

        switch(tier) {
            case TTableServiceConfig_TSessionBalancerSettings::TIER_DISABLED:
                return {false, 0};
            case TTableServiceConfig_TSessionBalancerSettings::TIER_ENABLED_FOR_ALL:
                return {true, maxInFlightSize};
            case TTableServiceConfig_TSessionBalancerSettings::TIER_ENABLED_FOR_SESSIONS_WITH_SUPPORT:
                return {false, maxInFlightSize};
            default:
                return {false, 0};
        }

        return {force, maxInFlightSize};
    }

    void TryKickSession() {

        const auto& sbs = TableServiceConfig.GetSessionBalancerSettings();
        const std::pair<bool, ui32> settings = GetBalancerEnableSettings();

        Y_ABORT_UNLESS(PeerStats);

        bool isReasonableToKick = false;

        ui32 strategy = static_cast<ui32>(sbs.GetStrategy());
        ui32 balanceByCpu = strategy & TTableServiceConfig_TSessionBalancerSettings::BALANCE_BY_CPU;
        ui32 balanceByCount = strategy & TTableServiceConfig_TSessionBalancerSettings::BALANCE_BY_COUNT;

        if (sbs.GetLocalDatacenterPolicy()) {
            if (balanceByCount) {
                isReasonableToKick |= ShouldStartBalancing(PeerStats->LocalSessionCount, static_cast<double>(sbs.GetMinNodeSessions()), static_cast<double>(LocalSessions->size()));
            }

            if (balanceByCpu) {
                isReasonableToKick |= ShouldStartBalancing(PeerStats->LocalCpu, sbs.GetMinCpuBalancerThreshold(), NodeResources.GetCpuUsage());
            }

        } else {
            if (balanceByCount) {
                isReasonableToKick |= ShouldStartBalancing(PeerStats->CrossAZSessionCount, static_cast<double>(sbs.GetMinNodeSessions()), static_cast<double>(LocalSessions->size()));
            }

            if (balanceByCpu) {
                isReasonableToKick |= ShouldStartBalancing(PeerStats->CrossAZCpu, sbs.GetMinCpuBalancerThreshold(), NodeResources.GetCpuUsage());
            }
        }

        if (!isReasonableToKick) {
            // Start balancing
            ServerWorkerBalancerComplete = true;
            return;
        } else {
            ServerWorkerBalancerComplete = false;
        }

        while(LocalSessions->GetShutdownInFlightSize() < settings.second) {
            auto sessionInfo = LocalSessions->PickSessionToShutdown(settings.first, sbs.GetMinNodeSessions());
            if (!sessionInfo) {
                break;
            }

            StartSessionGraceShutdown(sessionInfo);
        }
    }

    void StartSessionGraceShutdown(const TKqpSessionInfo* sessionInfo) {
        if (!sessionInfo)
            return;

        const auto& sbs = TableServiceConfig.GetSessionBalancerSettings();
        KQP_PROXY_LOG_D("Started grace shutdown of session, session id: " << sessionInfo->SessionId);
        ui32 hardTimeout = sbs.GetHardSessionShutdownTimeoutMs();
        ui32 softTimeout = sbs.GetSoftSessionShutdownTimeoutMs();
        Counters->ReportSessionShutdownRequest(sessionInfo->DbCounters);
        Send(sessionInfo->WorkerId, new TEvKqp::TEvInitiateSessionShutdown(softTimeout, hardTimeout));
    }

    void ProcessMonShutdownQueue(ui32 wantsToShutdown) {
        for(ui32 i = 0; i < wantsToShutdown; ++i) {
            const TKqpSessionInfo* candidate = LocalSessions->PickSessionToShutdown(true, 0);
            if (!candidate)
                break;

            StartSessionGraceShutdown(candidate);
        }
    }

    void Handle(NMon::TEvHttpInfo::TPtr& ev) {
        TStringStream str;

        auto& sbs = TableServiceConfig.GetSessionBalancerSettings();
        const TCgiParameters& cgi = ev->Get()->Request.GetParams();

        if (cgi.Has("force_shutdown")) {
            const TString& forceShutdown = cgi.Get("force_shutdown");
            ui32 wantsToShutdown = 0;
            if (forceShutdown == "all") {
                wantsToShutdown = LocalSessions->size();
            } else {
                wantsToShutdown = FromStringWithDefault<ui32>(forceShutdown, 0);
            }

            ProcessMonShutdownQueue(wantsToShutdown);
            str << "{\"status\": \"OK\", \"queueSize\": " << wantsToShutdown << "}";
            Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
            return;
        }

        HTML(str) {
            PRE() {
                str << "Self:" << Endl;
                str << "  - NodeId: " << SelfId().NodeId() << Endl;
                if (SelfDataCenterId) {
                    str << "  - DataCenterId: " << *SelfDataCenterId << Endl;
                }

                str << "Serving tenant: " << AppData()->TenantName << Endl;

                {
                    auto cgiTmp = cgi;
                    cgiTmp.InsertUnescaped("force_shutdown", "all");
                    str << "Force shutdown all sessions: <a href=\"kqp_proxy?" << cgiTmp.Print() << "\">Execute</a>" << Endl;
                }

                const std::pair<bool, ui32> sbsSettings = GetBalancerEnableSettings();
                str << "Allow shutdown all sessions: " << (sbsSettings.first ? "true": "false") << Endl;
                str << "MaxSessionsShutdownInFlightSize: " << sbsSettings.second << Endl;
                str << "LocalDatacenterPolicy: " << (sbs.GetLocalDatacenterPolicy() ? "true" : "false") << Endl;
                str << "MaxCVTreshold: " << sbs.GetMaxCVTreshold() << Endl;
                str << "MinCVTreshold: " << sbs.GetMinCVTreshold() << Endl;
                str << "Balance strategy: " << TTableServiceConfig_TSessionBalancerSettings_EBalancingStrategy_Name(sbs.GetStrategy()) << Endl;

                str << Endl;

                if (BoardPublishActor) {
                    str << "Publish status: " << Endl;
                    if (LastPublishResourcesAt) {
                        str << "Last published resources at " << *LastPublishResourcesAt << Endl;
                    }

                    if (PublishBoardPath) {
                        str << "Publish board path: " << PublishBoardPath << Endl;
                    }
                }

                str << Endl;

                str << "EnableSessionActor: always on" << Endl;
                str << "Active session_actors count on node: " << LocalSessions->size() << Endl;

                const auto& sessionsShutdownInFlight = LocalSessions->GetShutdownInFlight();
                if (!sessionsShutdownInFlight.empty()) {
                    str << Endl;
                    str << "Sessions shutdown in flight: " << Endl;
                    auto now = TAppData::TimeProvider->Now();
                    for(const auto& sessionId : sessionsShutdownInFlight) {
                        auto session = LocalSessions->FindPtr(sessionId);
                        str << "Session " << sessionId << " is under shutdown for " << (now - session->ShutdownStartedAt).SecondsFloat() << " seconds. " << Endl;
                    }

                    str << Endl;
                }

                if (!PeerStats) {
                    str << "No peer proxy data available." << Endl;
                } else {
                    str << Endl << "Peer Proxy data: " << Endl;
                    str << "Session count stats: " << Endl;
                    str << "Local: " << PeerStats->LocalSessionCount << Endl;
                    str << "Cross AZ: " << PeerStats->CrossAZSessionCount << Endl;

                    str << Endl << "CPU usage stats:" << Endl;
                    str << "Local: " << PeerStats->LocalCpu << Endl;
                    str << "Cross AZ: " << PeerStats->CrossAZCpu << Endl;

                    str << Endl;
                    for(const auto& entry : PeerProxyNodeResources) {
                        str << "Peer(NodeId: " << entry.GetNodeId() << ", DataCenter: " << entry.GetDataCenterId() << "): active workers: "
                            << entry.GetActiveWorkersCount() << "): cpu usage: " << entry.GetCpuUsage() << ", threads count: " << entry.GetThreads() << Endl;
                    }
                 }
            }
        }

        Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
    }

    void StartQueryTimeout(ui64 requestId, TDuration timeout, NYql::NDqProto::StatusIds::StatusCode status = NYql::NDqProto::StatusIds::TIMEOUT) {
        TActorId timeoutTimer = CreateLongTimer(
            TlsActivationContext->AsActorContext(), timeout,
            new IEventHandle(SelfId(), SelfId(), new TEvPrivate::TEvOnRequestTimeout{requestId, timeout, status, 0})
        );

        KQP_PROXY_LOG_D("Scheduled timeout timer for requestId: " << requestId << " timeout: " << timeout << " actor id: " << timeoutTimer);
        if (timeoutTimer) {
            TimeoutTimers.emplace(requestId, timeoutTimer);
        }
   }

    void StopQueryTimeout(ui64 requestId) {
        auto it = TimeoutTimers.find(requestId);
        if (it != TimeoutTimers.end()) {
            Send(it->second, new TEvents::TEvPoison);
            TimeoutTimers.erase(it);
        }
    }

    void Handle(TEvPrivate::TEvOnRequestTimeout::TPtr& ev) {
        auto* msg = ev->Get();
        ui64 requestId = ev->Get()->RequestId;
        TimeoutTimers.erase(requestId);

        KQP_PROXY_LOG_D("Handle TEvPrivate::TEvOnRequestTimeout(" << requestId << ")");
        const TKqpProxyRequest* reqInfo = PendingRequests.FindPtr(requestId);
        if (!reqInfo) {
            KQP_PROXY_LOG_D("Invalid request info while on request timeout handle. RequestId: " <<  requestId);
            return;
        }

        KQP_PROXY_LOG_D("Reply timeout: requestId " << requestId << " sessionId: " << reqInfo->SessionId
            << " status: " << NYql::NDq::DqStatusToYdbStatus(msg->Status) << " round: " << msg->Round);

        const TKqpSessionInfo* info = LocalSessions->FindPtr(reqInfo->SessionId);
        if (msg->Round == 0 && info) {
            TString message = TStringBuilder()
                << "request's " << (msg->Status == NYql::NDqProto::StatusIds::TIMEOUT ? "timeout" : "cancelAfter")
                << " exceeded";

            Send(info->WorkerId, new TEvKqp::TEvAbortExecution(msg->Status, message));

            // We must not reply before session actor in case of CANCEL AFTER settings
            if (msg->Status != NYql::NDqProto::StatusIds::CANCELLED) {
                auto newEv = ev->Release().Release();
                newEv->TickNextRound();
                Schedule(newEv->Timeout, newEv);
            }
        } else {
            TString message = TStringBuilder()
                << "Query did not complete within specified timeout, session id " << reqInfo->SessionId;
            ReplyProcessError(NYql::NDq::DqStatusToYdbStatus(msg->Status), message, requestId);
        }
    }

    void Handle(TEvKqp::TEvCloseSessionResponse::TPtr& ev) {
        const auto &event = ev->Get()->Record;
        if (event.GetStatus() == Ydb::StatusIds::SUCCESS && event.GetResponse().GetClosed()) {
            auto sessionId = event.GetResponse().GetSessionId();
            TActorId workerId = ev->Sender;

            RemoveSession(sessionId, workerId);

            KQP_PROXY_LOG_D("Session closed, sessionId: " << event.GetResponse().GetSessionId()
                << ", workerId: " << workerId << ", local sessions count: " << LocalSessions->size());
        }
    }

    void SendWhiteboardStats() {
        TActorId whiteboardId = NNodeWhiteboard::MakeNodeWhiteboardServiceId(SelfId().NodeId());
        Send(whiteboardId, NNodeWhiteboard::TEvWhiteboard::CreateTotalSessionsUpdateRequest(LocalSessions->size()));
    }

    STATEFN(MainState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvInterconnect::TEvNodeInfo, Handle);
            hFunc(NMon::TEvHttpInfo, Handle);
            hFunc(TEvStateStorage::TEvBoardInfo, Handle);
            hFunc(TEvPrivate::TEvCollectPeerProxyData, Handle);
            hFunc(TEvPrivate::TEvReadyToPublishResources, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
            hFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, Handle);
            hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, Handle);
            hFunc(TEvKqp::TEvQueryRequest, Handle);
            hFunc(TEvKqp::TEvScriptRequest, Handle);
            hFunc(TEvKqp::TEvCloseSessionRequest, Handle);
            hFunc(TEvKqp::TEvQueryResponse, ForwardEvent);
            hFunc(TEvKqpExecuter::TEvExecuterProgress, ForwardProgress);
            hFunc(TEvKqp::TEvCreateSessionRequest, Handle);
            hFunc(TEvKqp::TEvPingSessionRequest, Handle);
            hFunc(TEvKqp::TEvCancelQueryRequest, Handle);
            hFunc(TEvKqp::TEvCancelQueryResponse, ForwardEvent);
            hFunc(TEvKqp::TEvCloseSessionResponse, Handle);
            hFunc(TEvKqp::TEvPingSessionResponse, ForwardEvent);
            hFunc(TEvKqp::TEvInitiateShutdownRequest, Handle);
            hFunc(TEvPrivate::TEvOnRequestTimeout, Handle);
            hFunc(TEvPrivate::TEvResourcesSnapshot, Handle);
            hFunc(NNodeWhiteboard::TEvWhiteboard::TEvSystemStateResponse, Handle);
            hFunc(TEvKqp::TEvCreateSessionResponse, ForwardEvent);
            hFunc(TEvPrivate::TEvCloseIdleSessions, Handle);
            hFunc(TEvScriptExecutionsTablesCreationFinished, Handle);
            hFunc(NKqp::TEvForgetScriptExecutionOperation, Handle);
            hFunc(NKqp::TEvGetScriptExecutionOperation, Handle);
            hFunc(NKqp::TEvListScriptExecutionOperations, Handle);
            hFunc(NKqp::TEvCancelScriptExecutionOperation, Handle);
            hFunc(TEvInterconnect::TEvNodeConnected, Handle);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
            hFunc(TEvKqp::TEvListSessionsRequest, Handle);
            hFunc(TEvKqp::TEvListProxyNodesRequest, Handle);
            hFunc(NWorkload::TEvUpdatePoolInfo, Handle);
        default:
            Y_ABORT("TKqpProxyService: unexpected event type: %" PRIx32 " event: %s",
                ev->GetTypeRewrite(), ev->ToString().data());
        }
    }

private:

    void LogResponse(const TKqpRequestInfo&,
        const NKikimrKqp::TEvCreateSessionResponse& event, TKqpDbCountersPtr dbCounters)
    {
        Counters->ReportResponseStatus(dbCounters, event.ByteSize(),
            event.GetYdbStatus());
    }

    void LogResponse(const TKqpRequestInfo&,
        const NKikimrKqp::TEvPingSessionResponse& event, TKqpDbCountersPtr dbCounters)
    {
        Counters->ReportResponseStatus(dbCounters, event.ByteSize(), event.GetStatus());
    }

    bool ReplyProcessError(Ydb::StatusIds::StatusCode ydbStatus, const TString& message, ui64 requestId)
    {
        auto issue = NKikimr::MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, message);
        NYql::TIssues issues;
        issues.AddIssue(issue);
        const auto request = PendingRequests.FindPtr(requestId);
        if (!request) {
            return true;
        }

        if (request->EventType == TKqpEvents::EvPingSessionRequest) {
            auto response = std::make_unique<TEvKqp::TEvPingSessionResponse>();
            response->Record.SetStatus(ydbStatus);
            NYql::IssuesToMessage(issues, response->Record.MutableIssues());
            return Send(SelfId(), response.release(), 0, requestId);
        } else if (request->EventType == TKqpEvents::EvCreateSessionRequest) {
            auto response = std::make_unique<TEvKqp::TEvCreateSessionResponse>();
            response->Record.SetYdbStatus(ydbStatus);
            response->Record.SetError(message);
            return Send(SelfId(), response.release(), 0, requestId);
        }

        auto response = std::make_unique<TEvKqp::TEvQueryResponse>();
        response->Record.GetRef().SetYdbStatus(ydbStatus);

        NYql::IssuesToMessage(issues, response->Record.GetRef().MutableResponse()->MutableQueryIssues());
        return Send(SelfId(), response.release(), 0, requestId);
    }

    bool CheckRequestDeadline(const TKqpRequestInfo& requestInfo, const TInstant deadline, TProcessResult<TKqpSessionInfo*>& result)
    {
        if (!deadline) {
            return true;
        }
        auto now = TInstant::Now();
        if (now >= deadline) {
            TString error = TStringBuilder() << "Request deadline has expired for " << now - deadline << " seconds";
            KQP_PROXY_LOG_E(requestInfo << error);

            // In theory client should not see this status due to internal grpc deadline accounting.
            result.YdbStatus = Ydb::StatusIds::TIMEOUT;
            result.Error = error;
            return false;
        } else {
            KQP_PROXY_LOG_D(requestInfo << "Request has " << deadline - now << " seconds to be completed");
            return true;
        }
    }

    bool CreateNewSessionWorker(const TKqpRequestInfo& requestInfo, const TString& cluster, bool longSession,
        const TString& database, bool supportsBalancing, bool pgWire,
        const TString& clientHost, const TString& clientSid, const TString& userAgent,
        const TString& sdkBuildInfo,
        const TString& clientPid,
        const TString& clientApplicationName,
        const TMaybe<TString>& clientUserName,
        TProcessResult<TKqpSessionInfo*>& result)
    {
        if (!database.empty() && AppData()->TenantName.empty()) {
            TString error = TStringBuilder() << "Node isn't ready to serve database requests.";

            KQP_PROXY_LOG_E(requestInfo << error);

            result.YdbStatus = Ydb::StatusIds::UNAVAILABLE;
            result.Error = error;
            return false;
        }

        if (ShutdownRequested) {
            TString error = TStringBuilder() << "Cannot create session: system shutdown requested.";

            KQP_PROXY_LOG_N(requestInfo << error);

            result.ResourceExhausted = true;
            result.YdbStatus = Ydb::StatusIds::OVERLOADED;
            result.Error = error;
            return false;
        }

        auto sessionsLimitPerNode = TableServiceConfig.GetSessionsLimitPerNode();
        if (sessionsLimitPerNode && !LocalSessions->CheckDatabaseLimits(database, sessionsLimitPerNode)) {
            TString error = TStringBuilder() << "Active sessions limit exceeded, maximum allowed: "
                << sessionsLimitPerNode;
            KQP_PROXY_LOG_W(requestInfo << error);

            result.YdbStatus = Ydb::StatusIds::OVERLOADED;
            result.Error = error;
            return false;
        }

        auto sessionId = EncodeSessionId(SelfId().NodeId(), CreateGuidAsString());

        auto dbCounters = Counters->GetDbCounters(database);

        TKqpWorkerSettings workerSettings(cluster, database, clientApplicationName, clientUserName, TableServiceConfig, QueryServiceConfig, dbCounters);
        workerSettings.LongSession = longSession;

        auto config = CreateConfig(KqpSettings, workerSettings);

        IActor* sessionActor = CreateKqpSessionActor(SelfId(), ResourceManager_, CaFactory_, sessionId, KqpSettings, workerSettings,
            FederatedQuerySetup, AsyncIoFactory, ModuleResolverState, Counters,
            QueryServiceConfig, KqpTempTablesAgentActor);
        auto workerId = TlsActivationContext->ExecutorThread.RegisterActor(sessionActor, TMailboxType::HTSwap, AppData()->UserPoolId);
        TKqpSessionInfo* sessionInfo = LocalSessions->Create(
            sessionId, workerId, database, dbCounters, supportsBalancing, GetSessionIdleDuration(), pgWire);
        KqpProxySharedResources->AtomicLocalSessionCount.store(LocalSessions->size());

        sessionInfo->ClientSID = clientSid;
        sessionInfo->ClientHost = clientHost;
        sessionInfo->UserAgent = userAgent;
        sessionInfo->SdkBuildInfo = sdkBuildInfo;
        sessionInfo->ClientPID = clientPid;
        sessionInfo->ClientApplicationName = clientApplicationName;

        KQP_PROXY_LOG_D(requestInfo << "Created new session"
            << ", sessionId: " << sessionInfo->SessionId
            << ", workerId: " << sessionInfo->WorkerId
            << ", database: " << sessionInfo->Database
            << ", longSession: " << longSession
            << ", local sessions count: " << LocalSessions->size());

        result.YdbStatus = Ydb::StatusIds::SUCCESS;
        result.Error.clear();
        result.Value = sessionInfo;
        PublishResourceUsage();
        return true;
    }

    TActorId TryGetSessionTargetActor(const TString& sessionId, const TKqpRequestInfo& requestInfo, ui64 requestId)
    {
        auto nodeId = TryDecodeYdbSessionId(sessionId);
        if (!nodeId) {
            TString error = TStringBuilder() << "Failed to parse session id: " << sessionId;
            KQP_PROXY_LOG_W(requestInfo << error);
            ReplyProcessError(Ydb::StatusIds::BAD_REQUEST, error, requestId);
            return TActorId();
        }

        if (*nodeId == SelfId().NodeId()) {
            TString error = TStringBuilder() << "Session not found: " << sessionId;
            KQP_PROXY_LOG_N(requestInfo << error);
            ReplyProcessError(Ydb::StatusIds::BAD_SESSION, error, requestId);
            return TActorId();
        }

        if (!AppData()->TenantName.empty()) {
            auto counters = Counters->GetDbCounters(AppData()->TenantName);
            Counters->ReportProxyForwardedRequest(counters);
        }

        return MakeKqpProxyID(*nodeId);
    }

    void RemoveSession(const TString& sessionId, const TActorId& workerId) {
        if (!sessionId.empty()) {
            auto [nodeId, rpcActor] = LocalSessions->Erase(sessionId);
            KqpProxySharedResources->AtomicLocalSessionCount.store(LocalSessions->size());
            PublishResourceUsage();
            if (ShutdownRequested) {
                ShutdownState->Update(LocalSessions->size());
            }

            // No more session with kqp proxy on this node
            if (nodeId) {
                Send(TActivationContext::InterconnectProxy(nodeId), new TEvents::TEvUnsubscribe);
            }

            if (rpcActor) {
                auto closeEv = MakeHolder<TEvKqp::TEvCloseSessionResponse>();
                closeEv->Record.SetStatus(Ydb::StatusIds::SUCCESS);
                closeEv->Record.MutableResponse()->SetSessionId(sessionId);
                closeEv->Record.MutableResponse()->SetClosed(true);
                Send(rpcActor, closeEv.Release());
            }

            return;
        }

        LocalSessions->Erase(workerId);
        KqpProxySharedResources->AtomicLocalSessionCount.store(LocalSessions->size());
        PublishResourceUsage();
        if (ShutdownRequested) {
            ShutdownState->Update(LocalSessions->size());
        }
    }

    bool TryGetPoolInfoFromCache(TEvKqp::TEvQueryRequest::TPtr& ev, ui64 requestId) {
        if (!FeatureFlags.GetEnableResourcePools()) {
            ev->Get()->SetPoolId("");
            return true;
        }

        if (!ev->Get()->GetPoolId()) {
            ev->Get()->SetPoolId(NResourcePool::DEFAULT_POOL_ID);
        }

        const auto& poolId = ev->Get()->GetPoolId();
        const auto& poolInfo = ResourcePoolsCache.GetPoolInfo(ev->Get()->GetDatabase(), poolId);
        if (!poolInfo) {
            return true;
        }

        if (const auto& securityObject = poolInfo->SecurityObject) {
            const auto& userToken = ev->Get()->GetUserToken();
            if (!userToken || !securityObject->CheckAccess(NACLib::EAccessRights::DescribeSchema, *userToken)) {
                ReplyProcessError(Ydb::StatusIds::NOT_FOUND, TStringBuilder() << "Resource pool " << poolId << " not found or you don't have access permissions", requestId);
                return false;
            }
            if (!securityObject->CheckAccess(NACLib::EAccessRights::SelectRow, *userToken)) {
                ReplyProcessError(Ydb::StatusIds::UNAUTHORIZED, TStringBuilder() << "You don't have access permissions for resource pool " << poolId, requestId);
                return false;
            }
        }

        const auto& poolConfig = poolInfo->Config;
        if (poolConfig.ConcurrentQueryLimit == -1 && poolConfig.DatabaseLoadCpuThreshold < 0.0 && !poolConfig.QueryCancelAfter) {
            ev->Get()->SetPoolConfig(poolConfig);
        }

        return true;
    }

    void UpdateYqlLogLevels() {
        const auto& kqpYqlName = NKikimrServices::EServiceKikimr_Name(NKikimrServices::KQP_YQL);
        for (auto &entry : LogConfig.GetEntry()) {
            if (entry.GetComponent() == kqpYqlName && entry.HasLevel()) {
                auto yqlPriority = static_cast<NActors::NLog::EPriority>(entry.GetLevel());
                NYql::NDq::SetYqlLogLevels(yqlPriority);
                KQP_PROXY_LOG_D("Updated YQL logs priority: " << (ui32)yqlPriority);
                return;
            }
        }

        // Set log level based on current logger settings
        ui8 currentLevel = TlsActivationContext->LoggerSettings()->GetComponentSettings(NKikimrServices::KQP_YQL).Raw.X.Level;
        auto yqlPriority = static_cast<NActors::NLog::EPriority>(currentLevel);

        KQP_PROXY_LOG_D("Updated YQL logs priority to current level: " << (ui32)yqlPriority);
        NYql::NDq::SetYqlLogLevels(yqlPriority);
    }

    template<typename TResponse, typename TEvent>
    bool CheckScriptExecutionsTablesReady(TEvent& ev) {
        if (!AppData()->FeatureFlags.GetEnableScriptExecutionOperations()) {
            NYql::TIssues issues;
            issues.AddIssue("ExecuteScript feature is not enabled");
            Send(ev->Sender, new TResponse(Ydb::StatusIds::UNSUPPORTED, std::move(issues)));
            return false;
        }

        switch (ScriptExecutionsCreationStatus) {
            case EScriptExecutionsCreationStatus::NotStarted:
                ScriptExecutionsCreationStatus = EScriptExecutionsCreationStatus::Pending;
                Register(CreateScriptExecutionsTablesCreator(), TMailboxType::HTSwap, AppData()->SystemPoolId);
                [[fallthrough]];
            case EScriptExecutionsCreationStatus::Pending:
                if (DelayedEventsQueue.size() < 10000) {
                    DelayedEventsQueue.push_back({
                        .Event = std::move(ev),
                        .ResponseBuilder = [](Ydb::StatusIds::StatusCode status, NYql::TIssues issues) {
                            return new TResponse(status, std::move(issues));
                        }
                    });
                } else {
                    NYql::TIssues issues;
                    issues.AddIssue("Too many queued requests");
                    Send(ev->Sender, new TResponse(Ydb::StatusIds::OVERLOADED, std::move(issues)));
                }
                return false;
            case EScriptExecutionsCreationStatus::Finished:
                return true;
        }
    }

    void Handle(TEvScriptExecutionsTablesCreationFinished::TPtr& ev) {
        ScriptExecutionsCreationStatus = EScriptExecutionsCreationStatus::Finished;

        NYql::TIssue rootIssue;
        if (!ev->Get()->Success) {
            ScriptExecutionsCreationStatus = EScriptExecutionsCreationStatus::NotStarted;
            rootIssue.SetMessage("Failed to create script execution tables");
            for (const NYql::TIssue& issue : ev->Get()->Issues) {
                rootIssue.AddSubIssue(MakeIntrusive<NYql::TIssue>(issue));
            }
        }

        while (!DelayedEventsQueue.empty()) {
            auto delayedEvent = std::move(DelayedEventsQueue.front());
            if (ev->Get()->Success) {
                Send(std::move(delayedEvent.Event));
            } else {
                Send(delayedEvent.Event->Sender, delayedEvent.ResponseBuilder(Ydb::StatusIds::INTERNAL_ERROR, {rootIssue}));
            }
            DelayedEventsQueue.pop_front();
        }
    }

    void Handle(NKqp::TEvForgetScriptExecutionOperation::TPtr& ev) {
        if (CheckScriptExecutionsTablesReady<TEvForgetScriptExecutionOperationResponse>(ev)) {
            Register(CreateForgetScriptExecutionOperationActor(std::move(ev)), TMailboxType::HTSwap, AppData()->SystemPoolId);
        }
    }

    void Handle(NKqp::TEvGetScriptExecutionOperation::TPtr& ev) {
        if (CheckScriptExecutionsTablesReady<TEvGetScriptExecutionOperationResponse>(ev)) {
            Register(CreateGetScriptExecutionOperationActor(std::move(ev)), TMailboxType::HTSwap, AppData()->SystemPoolId);
        }
    }

    void Handle(NKqp::TEvListScriptExecutionOperations::TPtr& ev) {
        if (CheckScriptExecutionsTablesReady<TEvListScriptExecutionOperationsResponse>(ev)) {
            Register(CreateListScriptExecutionOperationsActor(std::move(ev)), TMailboxType::HTSwap, AppData()->SystemPoolId);
        }
    }

    void Handle(NKqp::TEvCancelScriptExecutionOperation::TPtr& ev) {
        if (CheckScriptExecutionsTablesReady<TEvCancelScriptExecutionOperationResponse>(ev)) {
            Register(CreateCancelScriptExecutionOperationActor(std::move(ev)), TMailboxType::HTSwap, AppData()->SystemPoolId);
        }
    }

    void Handle(TEvInterconnect::TEvNodeConnected::TPtr& ev) {
        TNodeId nodeId = ev->Get()->NodeId;
        auto sessions = LocalSessions->FindSessions(nodeId);
        if (sessions) {
            KQP_PROXY_LOG_T("Got TEvNodeConnected event from node: " << nodeId
                << ", has " << sessions.size() << " sessions");
        } else {
            KQP_PROXY_LOG_E("Got TEvNodeConnected event from node without sessions: " << nodeId);
        }
    }

    void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        TNodeId nodeId = ev->Get()->NodeId;
        auto sessions = LocalSessions->FindSessions(nodeId);
        KQP_PROXY_LOG_D("Node: " << nodeId << " disconnected, had " << sessions.size() << " sessions.");
        const static auto IdleDurationAfterDisconnect = TDuration::Seconds(1);
        // Just start standard idle check with small timeout
        // It allows to use common code to close and delete expired session
        for (const auto sessionInfo : sessions) {
            LocalSessions->StartIdleCheck(sessionInfo, IdleDurationAfterDisconnect);
        }
    }

    void Handle(TEvKqp::TEvListSessionsRequest::TPtr& ev) {
        KQP_PROXY_LOG_D("incoming list sessions request " << ev->Get()->Record.ShortUtf8DebugString());

        auto result = std::make_unique<TEvKqp::TEvListSessionsResponse>();
        auto startIt = LocalSessions->GetOrderedLowerBound(ev->Get()->Record.GetSessionIdStart());
        auto endIt = LocalSessions->GetOrderedEnd();
        i32 freeSpace = ev->Get()->Record.GetFreeSpace();

        TKqpSessionInfo::TFieldsMap fieldsMap(ev->Get()->Record.GetColumns());

        const TString until = ev->Get()->Record.GetSessionIdEnd();
        bool finished = false;

        while(startIt != endIt && freeSpace > 0) {
            auto* sessionInfo = startIt->second;

            if (!until.empty()) {
                if (sessionInfo->SessionId > until) {
                    finished = true;
                    break;
                }

                if (!ev->Get()->Record.GetSessionIdEndInclusive() && until == sessionInfo->SessionId) {
                    finished = true;
                    break;
                }
            }

            auto* sessionProto = result->Record.AddSessions();
            sessionInfo->SerializeTo(sessionProto, fieldsMap);
            freeSpace -= sessionProto->ByteSizeLong();
            ++startIt;
        }

        if (startIt == endIt) {
            finished = true;
        }

        result->Record.SetNodeId(SelfId().NodeId());
        if (finished) {
            result->Record.SetFinished(true);
        } else {
            result->Record.SetContinuationToken(startIt->first);
            result->Record.SetFinished(false);
        }

        Send(ev->Sender, result.release(), 0, ev->Cookie);
    }

    void Handle(TEvKqp::TEvListProxyNodesRequest::TPtr& ev) {
        auto result = std::make_unique<TEvKqp::TEvListProxyNodesResponse>();
        result->ProxyNodes.reserve(PeerProxyNodeResources.size());
        for(const auto& resource: PeerProxyNodeResources) {
            result->ProxyNodes.push_back(resource.GetNodeId());
        }

        if (result->ProxyNodes.size() < 1) {
            result->ProxyNodes.push_back(SelfId().NodeId());
        }

        Send(ev->Sender, result.release(), 0, ev->Cookie);
    }

    void Handle(NWorkload::TEvUpdatePoolInfo::TPtr& ev) {
        ResourcePoolsCache.UpdatePoolInfo(ev->Get()->Database, ev->Get()->PoolId, ev->Get()->Config, ev->Get()->SecurityObject);
    }

private:
    NKikimrConfig::TLogConfig LogConfig;
    NKikimrConfig::TTableServiceConfig TableServiceConfig;
    NKikimrConfig::TQueryServiceConfig QueryServiceConfig;
    NKikimrConfig::TFeatureFlags FeatureFlags;
    TKqpSettings::TConstPtr KqpSettings;
    IKqpFederatedQuerySetupFactory::TPtr FederatedQuerySetupFactory;
    std::optional<TKqpFederatedQuerySetup> FederatedQuerySetup;
    std::shared_ptr<IQueryReplayBackendFactory> QueryReplayFactory;
    NYql::NConnector::IClient::TPtr ConnectorClient;

    std::optional<TPeerStats> PeerStats;
    TKqpProxyRequestTracker PendingRequests;
    bool ShutdownRequested = false;
    THashMap<ui64, NKikimrConsole::TConfigItem::EKind> ConfigSubscriptions;
    THashMap<ui64, TActorId> TimeoutTimers;

    std::shared_ptr<NRm::IKqpResourceManager> ResourceManager_;
    std::shared_ptr<NComputeActor::IKqpNodeComputeActorFactory> CaFactory_;
    TIntrusivePtr<TKqpShutdownState> ShutdownState;
    TIntrusivePtr<TModuleResolverState> ModuleResolverState;

    TIntrusivePtr<TKqpCounters> Counters;
    std::unique_ptr<TLocalSessionsRegistry> LocalSessions;
    std::shared_ptr<TKqpProxySharedResources> KqpProxySharedResources;
    std::shared_ptr<NYql::NDq::IS3ActorsFactory> S3ActorsFactory;

    bool ServerWorkerBalancerComplete = false;
    std::optional<TString> SelfDataCenterId;
    TIntrusivePtr<IRandomProvider> RandomProvider;
    std::vector<ui64> LocalDatacenterProxies;
    TVector<NKikimrKqp::TKqpProxyNodeResources> PeerProxyNodeResources;
    bool ResourcesPublishScheduled = false;
    TString PublishBoardPath;
    std::optional<TInstant> LastPublishResourcesAt;

    TActorId KqpRmServiceActor;
    TActorId BoardLookupActor;
    TActorId BoardPublishActor;
    TActorId CompileService;
    TActorId CompileComputationPatternService;
    TActorId KqpNodeService;
    TActorId SpillingService;
    TActorId WhiteBoardService;
    TActorId KqpWorkloadService;
    NKikimrKqp::TKqpProxyNodeResources NodeResources;
    NYql::NDq::IDqAsyncIoFactory::TPtr AsyncIoFactory;

    enum class EScriptExecutionsCreationStatus {
        NotStarted,
        Pending,
        Finished,
    };
    struct TDelayedEvent {
        THolder<IEventHandle> Event;
        std::function<IEventBase*(Ydb::StatusIds::StatusCode, NYql::TIssues)> ResponseBuilder;
    };
    EScriptExecutionsCreationStatus ScriptExecutionsCreationStatus = EScriptExecutionsCreationStatus::NotStarted;
    std::deque<TDelayedEvent> DelayedEventsQueue;
    bool IsLookupByRmScheduled = false;
    TActorId KqpTempTablesAgentActor;

    TResourcePoolsCache ResourcePoolsCache;
};

} // namespace

IActor* CreateKqpProxyService(const NKikimrConfig::TLogConfig& logConfig,
    const NKikimrConfig::TTableServiceConfig& tableServiceConfig,
    const NKikimrConfig::TQueryServiceConfig& queryServiceConfig,
    TVector<NKikimrKqp::TKqpSetting>&& settings,
    std::shared_ptr<IQueryReplayBackendFactory> queryReplayFactory,
    std::shared_ptr<TKqpProxySharedResources> kqpProxySharedResources,
    IKqpFederatedQuerySetupFactory::TPtr federatedQuerySetupFactory,
    std::shared_ptr<NYql::NDq::IS3ActorsFactory> s3ActorsFactory
    )
{
    return new TKqpProxyService(logConfig, tableServiceConfig, queryServiceConfig, std::move(settings),
        std::move(queryReplayFactory), std::move(kqpProxySharedResources), std::move(federatedQuerySetupFactory), std::move(s3ActorsFactory));
}

} // namespace NKikimr::NKqp
