#include "kqp_workload_service.h"
#include "kqp_workload_service_impl.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/path.h>

#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>

#include <ydb/core/kqp/workload_service/actors/actors.h>
#include <ydb/core/kqp/workload_service/common/helpers.h>
#include <ydb/core/kqp/workload_service/tables/table_queries.h>

#include <ydb/core/protos/console_config.pb.h>

#include <ydb/library/actors/interconnect/interconnect.h>


namespace NKikimr::NKqp {

namespace NWorkload {

namespace {

using namespace NActors;


class TKqpWorkloadService : public TActorBootstrapped<TKqpWorkloadService> {
    struct TCounters {
        const NMonitoring::TDynamicCounterPtr Counters;

        NMonitoring::TDynamicCounters::TCounterPtr ActivePools;

        TCounters(NMonitoring::TDynamicCounterPtr counters)
            : Counters(counters)
        {
            Register();
        }

    private:
        void Register() {
            ActivePools = Counters->GetCounter("ActivePools", false);
        }
    };

    enum class ETablesCreationStatus {
        Cleanup,
        NotStarted,
        Pending,
        Finished,
    };

    enum class EWakeUp {
        IdleCheck,
        StartCpuLoadRequest,
        StartNodeInfoRequest
    };

public:
    explicit TKqpWorkloadService(NMonitoring::TDynamicCounterPtr counters)
        : Counters(counters)
    {}

    void Bootstrap() {
        Become(&TKqpWorkloadService::MainState);

        // Subscribe for FeatureFlags
        Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()), new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({
            (ui32)NKikimrConsole::TConfigItem::FeatureFlagsItem
        }), IEventHandle::FlagTrackDelivery);

        CpuQuotaManager = std::make_unique<TCpuQuotaManagerState>(ActorContext(), Counters.Counters->GetSubgroup("subcomponent", "CpuQuotaManager"));

        EnabledResourcePools = AppData()->FeatureFlags.GetEnableResourcePools();
        EnabledResourcePoolsOnServerless = AppData()->FeatureFlags.GetEnableResourcePoolsOnServerless();
        if (EnabledResourcePools) {
            InitializeWorkloadService();
        }
    }

    void HandlePoison() {
        LOG_W("Got poison, stop workload service");

        for (const auto& [_, poolState] : PoolIdToState) {
            Send(poolState.PoolHandler, new TEvents::TEvPoison());
            if (poolState.NewPoolHandler) {
                Send(*poolState.NewPoolHandler, new TEvents::TEvPoison());
            }
        }

        PassAway();
    }

    void HandleSetConfigSubscriptionResponse() const {
        LOG_D("Subscribed for config changes");
    }

    void Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
        const auto& event = ev->Get()->Record;

        EnabledResourcePools = event.GetConfig().GetFeatureFlags().GetEnableResourcePools();
        EnabledResourcePoolsOnServerless = event.GetConfig().GetFeatureFlags().GetEnableResourcePoolsOnServerless();
        if (EnabledResourcePools) {
            LOG_I("Resource pools was enanbled");
            InitializeWorkloadService();
        } else {
            LOG_I("Resource pools was disabled");
        }

        auto responseEvent = std::make_unique<NConsole::TEvConsole::TEvConfigNotificationResponse>(event);
        Send(ev->Sender, responseEvent.release(), IEventHandle::FlagTrackDelivery, ev->Cookie);
    }

    void Handle(TEvInterconnect::TEvNodesInfo::TPtr& ev) {
        NodeCount = ev->Get()->Nodes.size();
        ScheduleNodeInfoRequest();

        LOG_T("Updated node info, noode count: " << NodeCount);
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) const {
        switch (ev->Get()->SourceType) {
            case NConsole::TEvConfigsDispatcher::EvSetConfigSubscriptionRequest:
                LOG_C("Failed to deliver subscription request to config dispatcher");
                break;

            case NConsole::TEvConsole::EvConfigNotificationResponse:
                LOG_E("Failed to deliver config notification response");
                break;

            case TEvInterconnect::EvListNodes:
                LOG_W("Failed to deliver list nodes request");
                ScheduleNodeInfoRequest();
                break;

            default:
                LOG_E("Undelivered event with unexpected source type: " << ev->Get()->SourceType);
                break;
        }
    }

    void Handle(TEvSubscribeOnPoolChanges::TPtr& ev) {
        const TString& database = ev->Get()->Database;
        const TString& poolId = ev->Get()->PoolId;
        if (!EnabledResourcePools) {
            Send(ev->Sender, new TEvUpdatePoolInfo(database, poolId, std::nullopt, std::nullopt));
            return;
        }

        LOG_D("Recieved subscription request, Database: " << database << ", PoolId: " << poolId);
        Register(CreatePoolFetcherActor(SelfId(), database, poolId, nullptr));
    }

    void Handle(TEvPlaceRequestIntoPool::TPtr& ev) {
        const TActorId& workerActorId = ev->Sender;
        if (!EnabledResourcePools) {
            ReplyContinueError(workerActorId, Ydb::StatusIds::UNSUPPORTED, "Workload manager is disabled. Please contact your system administrator to enable it");
            return;
        }

        const TString& database = ev->Get()->Database;
        LOG_D("Recieved new request from " << workerActorId << ", Database: " << database << ", PoolId: " << ev->Get()->PoolId << ", SessionId: " << ev->Get()->SessionId);
        GetOrCreateDatabaseState(database)->DoPlaceRequest(std::move(ev));
    }

    void Handle(TEvCleanupRequest::TPtr& ev) {
        const TString& database = ev->Get()->Database;
        const TString& poolId = ev->Get()->PoolId;
        auto poolState = GetPoolState(database, poolId);
        if (!poolState) {
            ReplyCleanupError(ev->Sender, Ydb::StatusIds::NOT_FOUND, TStringBuilder() << "Pool " << poolId << " not found");
            return;
        }

        LOG_D("Finished request with worker actor " << ev->Sender << ", Database: " << database << ", PoolId: " << poolId << ", SessionId: " << ev->Get()->SessionId);
        Send(ev->Forward(poolState->PoolHandler));
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        switch (static_cast<EWakeUp>(ev->Get()->Tag)) {
            case EWakeUp::IdleCheck:
                RunIdleCheck();
                break;

            case EWakeUp::StartCpuLoadRequest:
                RunCpuLoadRequest();
                break;

            case EWakeUp::StartNodeInfoRequest:
                RunNodeInfoRequest();
                break;
        }
    }

    STRICT_STFUNC(MainState,
        sFunc(TEvents::TEvPoison, HandlePoison);
        sFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, HandleSetConfigSubscriptionResponse);
        hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, Handle);
        hFunc(TEvInterconnect::TEvNodesInfo, Handle);
        hFunc(TEvents::TEvUndelivered, Handle);

        hFunc(TEvSubscribeOnPoolChanges, Handle);
        hFunc(TEvPlaceRequestIntoPool, Handle);
        hFunc(TEvCleanupRequest, Handle);
        hFunc(TEvents::TEvWakeup, Handle);

        hFunc(TEvPrivate::TEvFetchDatabaseResponse, Handle);
        hFunc(TEvPrivate::TEvFetchPoolResponse, Handle);
        hFunc(TEvPrivate::TEvResolvePoolResponse, Handle);
        hFunc(TEvPrivate::TEvPlaceRequestIntoPoolResponse, Handle);
        hFunc(TEvPrivate::TEvNodesInfoRequest, Handle);
        hFunc(TEvPrivate::TEvRefreshPoolState, Handle);
        hFunc(TEvPrivate::TEvCpuQuotaRequest, Handle);
        hFunc(TEvPrivate::TEvFinishRequestInPool, Handle);
        hFunc(TEvPrivate::TEvPrepareTablesRequest, Handle);
        hFunc(TEvPrivate::TEvCleanupTablesFinished, Handle);
        hFunc(TEvPrivate::TEvTablesCreationFinished, Handle);
        hFunc(TEvPrivate::TEvCpuLoadResponse, Handle);
        hFunc(TEvPrivate::TEvResignPoolHandler, Handle);
    )

private:
    void Handle(TEvPrivate::TEvFetchDatabaseResponse::TPtr& ev) {
        GetOrCreateDatabaseState(ev->Get()->Database)->UpdateDatabaseInfo(ev);
    }

    void Handle(TEvPrivate::TEvFetchPoolResponse::TPtr& ev) {
        const TString& database = ev->Get()->Database;
        const TString& poolId = ev->Get()->PoolId;
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            Send(MakeKqpProxyID(SelfId().NodeId()), new TEvUpdatePoolInfo(database, poolId, std::nullopt, std::nullopt));
            return;
        }

        LOG_D("Successfully fetched pool " << poolId << ", Database: " << database);

        auto poolState = GetOrCreatePoolState(database, poolId, ev->Get()->PoolConfig);
        Send(poolState->PoolHandler, new TEvPrivate::TEvUpdateSchemeBoardSubscription(ev->Get()->PathId));
    }

    void Handle(TEvPrivate::TEvResolvePoolResponse::TPtr& ev) {
        const auto& event = ev->Get()->Event;
        const TString& database = event->Get()->Database;
        if (ev->Get()->DefaultPoolCreated) {
            GetOrCreateDatabaseState(database)->HasDefaultPool = true;
        }

        const TString& poolId = event->Get()->PoolId;
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            ReplyContinueError(event->Sender, ev->Get()->Status, ev->Get()->Issues);
            return;
        }

        LOG_D("Successfully fetched pool " << poolId << ", Database: " << database << ", SessionId: " << event->Get()->SessionId);

        auto poolState = GetOrCreatePoolState(database, poolId, ev->Get()->PoolConfig);
        poolState->PendingRequests.emplace(std::move(ev));
        poolState->StartPlaceRequest();
    }

    void Handle(TEvPrivate::TEvPlaceRequestIntoPoolResponse::TPtr& ev) {
        const TString& database = ev->Get()->Database;
        const TString& poolId = ev->Get()->PoolId;
        LOG_T("Request placed into pool, Database: " << database << ", PoolId: " << poolId);

        if (auto poolState = GetPoolState(database, poolId)) {
            poolState->PlaceRequestRunning = false;
            poolState->UpdateHandler();
            poolState->StartPlaceRequest();
        }
    }

    void Handle(TEvPrivate::TEvNodesInfoRequest::TPtr& ev) const {
        Send(ev->Sender, new TEvPrivate::TEvNodesInfoResponse(NodeCount));
    }

    void Handle(TEvPrivate::TEvRefreshPoolState::TPtr& ev) {
        const auto& event = ev->Get()->Record;
        const TString& database = event.GetDatabase();
        const TString& poolId = event.GetPoolId();
        LOG_T("Got remote refresh request, Database: " << database << ", PoolId: " << poolId << ", NodeId: " << ev->Sender.NodeId());

        if (auto poolState = GetPoolState(database, poolId)) {
            Send(ev->Forward(poolState->PoolHandler));
        }
    }

    void Handle(TEvPrivate::TEvCpuQuotaRequest::TPtr& ev) {
        const TActorId& poolHandler = ev->Sender;
        const double maxClusterLoad = ev->Get()->MaxClusterLoad;
        LOG_T("Requested cpu quota from handler " << poolHandler << ", MaxClusterLoad: " << maxClusterLoad);

        CpuQuotaManager->RequestCpuQuota(poolHandler, maxClusterLoad, ev->Cookie);
        ScheduleCpuLoadRequest();
    }

    void Handle(TEvPrivate::TEvFinishRequestInPool::TPtr& ev) {
        const TString& database = ev->Get()->Database;
        const TString& poolId = ev->Get()->PoolId;
        LOG_T("Request finished in pool, Database: " << database << ", PoolId: " << poolId << ", Duration: " << ev->Get()->Duration << ", CpuConsumed: " << ev->Get()->CpuConsumed << ", AdjustCpuQuota: " << ev->Get()->AdjustCpuQuota);

        if (auto poolState = GetPoolState(database, poolId)) {
            poolState->OnRequestFinished();
        }
        if (ev->Get()->AdjustCpuQuota) {
            CpuQuotaManager->AdjustCpuQuota(ev->Get()->Duration, ev->Get()->CpuConsumed.SecondsFloat());
            ScheduleCpuLoadRequest();
        }
    }

    void Handle(TEvPrivate::TEvPrepareTablesRequest::TPtr& ev) {
        const TString& database = ev->Get()->Database;
        const TString& poolId = ev->Get()->PoolId;
        LOG_T("Got create teables request, Database: " << database << ", PoolId: " << poolId);

        auto poolState = GetPoolState(database, poolId);
        if (!poolState) {
            return;
        }

        if (TablesCreationStatus == ETablesCreationStatus::Finished) {
            Send(poolState->PoolHandler, new TEvPrivate::TEvTablesCreationFinished(true, {}));
        } else {
            poolState->WaitingInitialization = true;
            PendingHandlers.emplace(GetPoolKey(database, poolId));
            PrepareWorkloadServiceTables();
        }
    }

    void Handle(TEvPrivate::TEvCleanupTablesFinished::TPtr& ev) {
        if (ev->Get()->Success) {
            LOG_D("Cleanup completed, tables exists: " << ev->Get()->TablesExists);
        } else {
            LOG_C("Failed to cleanup tables, issues: " << ev->Get()->Issues.ToOneLineString());
        }

        TablesCreationStatus = ETablesCreationStatus::NotStarted;
        if (ev->Get()->TablesExists) {
            TablesCreationStatus = ETablesCreationStatus::Finished;
            OnTabelsCreated(true);
        } else {
            PrepareWorkloadServiceTables();
        }
    }

    void Handle(TEvPrivate::TEvTablesCreationFinished::TPtr& ev) {
        TablesCreationStatus = ev->Get()->Success ? ETablesCreationStatus::Finished : ETablesCreationStatus::NotStarted;

        if (ev->Get()->Success) {
            LOG_D("Succefully created tables, send response to handlers");
            OnTabelsCreated(true);
            return;
        }

        LOG_E("Failed to create tables, issues: " << ev->Get()->Issues.ToOneLineString());
        NYql::TIssues issues = GroupIssues(ev->Get()->Issues, "Failed to create workload service tables");
        OnTabelsCreated(false, issues);
    }

    void Handle(TEvPrivate::TEvCpuLoadResponse::TPtr& ev) {
        const bool success = ev->Get()->Status == Ydb::StatusIds::SUCCESS;
        if (!success) {
            LOG_E("Failed to fetch cpu load " << ev->Get()->Status << ", issues: " << ev->Get()->Issues.ToOneLineString());
        } else {
            LOG_T("Succesfully fetched cpu load: " << 100.0 * ev->Get()->InstantLoad << "%, cpu number: " << ev->Get()->CpuNumber);
        }

        CpuQuotaManager->CpuLoadRequestRunning = false;
        CpuQuotaManager->UpdateCpuLoad(ev->Get()->InstantLoad, ev->Get()->CpuNumber, success);
        ScheduleCpuLoadRequest();
    }

    void Handle(TEvPrivate::TEvResignPoolHandler::TPtr& ev) {
        const TString& database = ev->Get()->Database;
        const TString& poolId = ev->Get()->PoolId;
        LOG_T("Got resign request, Database: " << database << ", PoolId: " << poolId);

        if (auto poolState = GetPoolState(database, poolId)) {
            if (poolState->NewPoolHandler) {
                Send(*poolState->NewPoolHandler, new TEvPrivate::TEvStopPoolHandler());
            }
            poolState->NewPoolHandler = ev->Get()->NewHandler;
            poolState->UpdateHandler();
        }
    }

private:
    void InitializeWorkloadService() {
        if (ServiceInitialized) {
            return;
        }
        ServiceInitialized = true;

        LOG_I("Started workload service initialization");
        Register(CreateCleanupTablesActor());
        RunNodeInfoRequest();
    }

    void PrepareWorkloadServiceTables() {
        if (PendingHandlers.empty()) {
            return;
        }

        if (TablesCreationStatus == ETablesCreationStatus::NotStarted) {
            LOG_I("Started workload service tables creation");
            TablesCreationStatus = ETablesCreationStatus::Pending;
            Register(CreateTablesCreator());
        }
    }

    void OnTabelsCreated(bool success, NYql::TIssues issues = {}) {
        for (const TString& poolKey : PendingHandlers) {
            auto poolState = GetPoolState(poolKey);
            if (!poolState) {
                continue;
            }

            Send(poolState->PoolHandler, new TEvPrivate::TEvTablesCreationFinished(success, issues));
            poolState->WaitingInitialization = false;
            poolState->UpdateHandler();
        }
        PendingHandlers.clear();
    }

    void ScheduleIdleCheck() {
        if (IdleChecksStarted) {
            return;
        }
        IdleChecksStarted = true;

        Schedule(IDLE_DURATION / 2, new TEvents::TEvWakeup(static_cast<ui64>(EWakeUp::IdleCheck)));
    }

    void RunIdleCheck() {
        IdleChecksStarted = false;

        std::vector<TString> poolsToDelete;
        poolsToDelete.reserve(PoolIdToState.size());
        for (const auto& [poolKey, poolState] : PoolIdToState) {
            if (!poolState.InFlightRequests && TInstant::Now() - poolState.LastUpdateTime > IDLE_DURATION) {
                CpuQuotaManager->CleanupHandler(poolState.PoolHandler);
                Send(poolState.PoolHandler, new TEvPrivate::TEvStopPoolHandler());
                poolsToDelete.emplace_back(poolKey);
            }
        }
        for (const auto& poolKey : poolsToDelete) {
            PoolIdToState.erase(poolKey);
            Counters.ActivePools->Dec();
        }

        if (!PoolIdToState.empty()) {
            ScheduleIdleCheck();
        }
    }

    void ScheduleCpuLoadRequest() const {
        auto delay = CpuQuotaManager->GetCpuLoadRequestDelay();
        if (!delay) {
            return;
        }

        if (*delay) {
            Schedule(*delay, new TEvents::TEvWakeup(static_cast<ui64>(EWakeUp::StartCpuLoadRequest)));
        } else {
            RunCpuLoadRequest();
        }
    }

    void RunCpuLoadRequest() const {
        if (CpuQuotaManager->CpuLoadRequestRunning) {
            return;
        }

        CpuQuotaManager->CpuLoadRequestTime = TInstant::Zero();
        if (CpuQuotaManager->CpuQuotaManager.GetMonitoringRequestDelay()) {
            ScheduleCpuLoadRequest();
            return;
        }

        CpuQuotaManager->CpuLoadRequestRunning = true;
        Register(CreateCpuLoadFetcherActor(SelfId()));
    }

    void ScheduleNodeInfoRequest() const {
        Schedule(IDLE_DURATION * 2, new TEvents::TEvWakeup(static_cast<ui64>(EWakeUp::StartNodeInfoRequest)));
    }

    void RunNodeInfoRequest() const {
        Send(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes(), IEventHandle::FlagTrackDelivery);
    }

private:
    void ReplyContinueError(const TActorId& replyActorId, Ydb::StatusIds::StatusCode status, const TString& message) const {
        ReplyContinueError(replyActorId, status, {NYql::TIssue(message)});
    }

    void ReplyContinueError(const TActorId& replyActorId, Ydb::StatusIds::StatusCode status, NYql::TIssues issues) const {
        if (status == Ydb::StatusIds::UNSUPPORTED) {
            LOG_T("Reply unsupported to " << replyActorId << ": " << issues.ToOneLineString());
        } else {
            LOG_W("Reply continue error " << status << " to " << replyActorId << ": " << issues.ToOneLineString());
        }
        Send(replyActorId, new TEvContinueRequest(status, {}, {}, std::move(issues)));
    }

    void ReplyCleanupError(const TActorId& replyActorId, Ydb::StatusIds::StatusCode status, const TString& message) const {
        LOG_W("Reply cleanup error " << status << " to " << replyActorId << ": " << message);
        Send(replyActorId, new TEvCleanupResponse(status, {NYql::TIssue(message)}));
    }

    TDatabaseState* GetOrCreateDatabaseState(const TString& database) {
        auto databaseIt = DatabaseToState.find(database);
        if (databaseIt != DatabaseToState.end()) {
            return &databaseIt->second;
        }
        return &DatabaseToState.insert({database, TDatabaseState{.ActorContext = ActorContext(), .EnabledResourcePoolsOnServerless = EnabledResourcePoolsOnServerless}}).first->second;
    }

    TPoolState* GetOrCreatePoolState(const TString& database, const TString& poolId, const NResourcePool::TPoolSettings& poolConfig) {
        const auto& poolKey = GetPoolKey(database, poolId);
        if (auto poolState = GetPoolState(poolKey)) {
            return poolState;
        }

        LOG_I("Creating new handler for pool " << poolKey);

        const auto poolHandler = Register(CreatePoolHandlerActor(database, poolId, poolConfig, Counters.Counters));
        const auto poolState = &PoolIdToState.insert({poolKey, TPoolState{.PoolHandler = poolHandler, .ActorContext = ActorContext()}}).first->second;

        Counters.ActivePools->Inc();
        ScheduleIdleCheck();

        return poolState;
    }

    TPoolState* GetPoolState(const TString& database, const TString& poolId) {
        return GetPoolState(GetPoolKey(database, poolId));
    }

    TPoolState* GetPoolState(const TString& key) {
        auto poolIt = PoolIdToState.find(key);
        if (poolIt != PoolIdToState.end()) {
            return &poolIt->second;
        }
        return nullptr;
    }

    static TString GetPoolKey(const TString& database, const TString& poolId) {
        return CanonizePath(TStringBuilder() << database << "/" << poolId);
    }

    TString LogPrefix() const {
        return "[Service] ";
    }

private:
    TCounters Counters;

    bool EnabledResourcePools = false;
    bool EnabledResourcePoolsOnServerless = false;
    bool ServiceInitialized = false;
    bool IdleChecksStarted = false;
    ETablesCreationStatus TablesCreationStatus = ETablesCreationStatus::Cleanup;
    std::unordered_set<TString> PendingHandlers;

    std::unordered_map<TString, TDatabaseState> DatabaseToState;
    std::unordered_map<TString, TPoolState> PoolIdToState;
    std::unique_ptr<TCpuQuotaManagerState> CpuQuotaManager;
    ui32 NodeCount = 0;
};

}  // anonymous namespace

bool IsWorkloadServiceRequired(const NResourcePool::TPoolSettings& config) {
    return config.ConcurrentQueryLimit != -1 || config.DatabaseLoadCpuThreshold >= 0.0 || config.QueryCancelAfter;
}

}  // namespace NWorkload

IActor* CreateKqpWorkloadService(NMonitoring::TDynamicCounterPtr counters) {
    return new NWorkload::TKqpWorkloadService(counters);
}

}  // namespace NKikimr::NKqp
