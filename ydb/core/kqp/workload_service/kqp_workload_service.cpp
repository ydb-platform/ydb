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

#include <ydb/core/mind/tenant_node_enumeration.h>

#include <ydb/core/protos/console_config.pb.h>
#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/core/protos/workload_manager_config.pb.h>

#include <ydb/library/actors/interconnect/interconnect.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KQP_WORKLOAD_SERVICE


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

        // Subscribe for FeatureFlags and WorkloadManagerConfig
        Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()), new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({
            (ui32)NKikimrConsole::TConfigItem::FeatureFlagsItem, (ui32)NKikimrConsole::TConfigItem::WorkloadManagerConfigItem
        }), IEventHandle::FlagTrackDelivery);

        CpuQuotaManager = std::make_unique<TCpuQuotaManagerState>(Counters.Counters->GetSubgroup("subcomponent", "CpuQuotaManager"));

        WorkloadManagerConfig = AppData()->WorkloadManagerConfig;
        EnabledResourcePools = AppData()->FeatureFlags.GetEnableResourcePools() || WorkloadManagerConfig.GetEnabled();
        EnabledResourcePoolsOnServerless = AppData()->FeatureFlags.GetEnableResourcePoolsOnServerless() || WorkloadManagerConfig.GetEnabled();
        EnableResourcePoolsCounters = AppData()->FeatureFlags.GetEnableResourcePoolsCounters();
        if (EnabledResourcePools) {
            InitializeWorkloadService();
        }
    }

    void HandlePoison() {
        YDB_LOG_WARN("[WorkloadService] Got poison, stop workload service",
            {"logPrefix", LogPrefix()});

        // Unsubscribe from all scheme cache watches
        for (const auto& [_, databaseState] : DatabaseToState) {
            if (databaseState.WatchKey) {
                Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchRemove(databaseState.WatchKey));
            }
        }

        for (const auto& [_, poolState] : PoolIdToState) {
            Send(poolState.PoolHandler, new TEvents::TEvPoison());
            if (poolState.NewPoolHandler) {
                Send(*poolState.NewPoolHandler, new TEvents::TEvPoison());
            }
        }

        PassAway();
    }

    void HandleSetConfigSubscriptionResponse() const {
        YDB_LOG_DEBUG("[WorkloadService] Subscribed for config changes",
            {"logPrefix", LogPrefix()});
    }

    void Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
        const auto& event = ev->Get()->Record;

        WorkloadManagerConfig = event.GetConfig().GetWorkloadManagerConfig();
        EnabledResourcePools = event.GetConfig().GetFeatureFlags().GetEnableResourcePools() || WorkloadManagerConfig.GetEnabled();
        EnabledResourcePoolsOnServerless = event.GetConfig().GetFeatureFlags().GetEnableResourcePoolsOnServerless() || WorkloadManagerConfig.GetEnabled();
        EnableResourcePoolsCounters = event.GetConfig().GetFeatureFlags().GetEnableResourcePoolsCounters();
        if (EnabledResourcePools) {
            YDB_LOG_INFO("[WorkloadService] Resource pools was enabled",
                {"logPrefix", LogPrefix()});
            InitializeWorkloadService();
        } else {
            YDB_LOG_INFO("[WorkloadService] Resource pools was disabled",
                {"logPrefix", LogPrefix()});
        }

        auto responseEvent = std::make_unique<NConsole::TEvConsole::TEvConfigNotificationResponse>(event);
        Send(ev->Sender, responseEvent.release(), IEventHandle::FlagTrackDelivery, ev->Cookie);
    }

    void Handle(TEvTenantNodeEnumerator::TEvLookupResult::TPtr& ev) {
        if (!ev->Get()->Success) {
            YDB_LOG_WARN("[WorkloadService] Failed to discover tenant nodes",
                {"logPrefix", LogPrefix()});
            return;
        }

        NodeCount = ev->Get()->AssignedNodes.size();
        ScheduleNodeInfoRequest();

        YDB_LOG_TRACE("[WorkloadService] Updated node info, node",
            {"logPrefix", LogPrefix()},
            {"count", NodeCount});
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) const {
        switch (ev->Get()->SourceType) {
            case NConsole::TEvConfigsDispatcher::EvSetConfigSubscriptionRequest:
                YDB_LOG_CRIT("[WorkloadService] Failed to deliver subscription request to config dispatcher",
                    {"logPrefix", LogPrefix()});
                break;

            case NConsole::TEvConsole::EvConfigNotificationResponse:
                YDB_LOG_ERROR("[WorkloadService] Failed to deliver config notification response",
                    {"logPrefix", LogPrefix()});
                break;

            case TEvInterconnect::EvListNodes:
                YDB_LOG_WARN("[WorkloadService] Failed to deliver list nodes request",
                    {"logPrefix", LogPrefix()});
                ScheduleNodeInfoRequest();
                break;

            default:
                YDB_LOG_ERROR("[WorkloadService] Undelivered event with unexpected source",
                    {"logPrefix", LogPrefix()},
                    {"type", ev->Get()->SourceType});
                break;
        }
    }

    void Handle(TEvSubscribeOnPoolChanges::TPtr& ev) {
        const TString& databaseId = ev->Get()->DatabaseId;
        const TString& poolId = ev->Get()->PoolId;
        if (!EnabledResourcePools) {
            Send(ev->Sender, new TEvUpdatePoolInfo(databaseId, poolId, std::nullopt, std::nullopt));
            return;
        }

        YDB_LOG_DEBUG("[WorkloadService] Received subscription request,",
            {"logPrefix", LogPrefix()},
            {"databaseId", databaseId},
            {"poolId", poolId});
        GetOrCreateDatabaseState(databaseId)->DoSubscribeRequest(std::move(ev));
    }

    void Handle(TEvPlaceRequestIntoPool::TPtr& ev) {
        const TActorId& workerActorId = ev->Sender;
        if (!EnabledResourcePools) {
            ReplyContinueError(workerActorId, Ydb::StatusIds::UNSUPPORTED, "Workload manager is disabled. Please contact your system administrator to enable it");
            return;
        }

        const TString& databaseId = ev->Get()->DatabaseId;
        YDB_LOG_DEBUG("[WorkloadService] Received new request",
            {"logPrefix", LogPrefix()},
            {"workerActorId", workerActorId},
            {"databaseId", databaseId},
            {"poolId", ev->Get()->PoolId},
            {"sessionId", ev->Get()->SessionId});
        GetOrCreateDatabaseState(databaseId)->DoPlaceRequest(std::move(ev));
    }

    void Handle(TEvCleanupRequest::TPtr& ev) {
        const TString& databaseId = ev->Get()->DatabaseId;
        const TString& poolId = ev->Get()->PoolId;
        const TString& sessionId = ev->Get()->SessionId;
        if (GetOrCreateDatabaseState(databaseId)->PendingSessionIds.contains(sessionId)) {
            YDB_LOG_DEBUG("[WorkloadService] Finished request with worker actor wait for place request,",
                {"logPrefix", LogPrefix()},
                {"sender", ev->Sender},
                {"databaseId", databaseId},
                {"poolId", poolId},
                {"sessionId", ev->Get()->SessionId});
            GetOrCreateDatabaseState(databaseId)->PendingCancelRequests[sessionId].emplace_back(std::move(ev));
            return;
        }

        auto poolState = GetPoolState(databaseId, poolId);
        if (!poolState) {
            ReplyCleanupError(ev->Sender, Ydb::StatusIds::NOT_FOUND, TStringBuilder() << "Pool " << poolId << " not found");
            return;
        }

        YDB_LOG_DEBUG("[WorkloadService] Finished request with worker actor",
            {"logPrefix", LogPrefix()},
            {"sender", ev->Sender},
            {"databaseId", databaseId},
            {"poolId", poolId},
            {"sessionId", ev->Get()->SessionId});
        poolState->DoCleanupRequest(std::move(ev));
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
        hFunc(TEvTenantNodeEnumerator::TEvLookupResult, Handle);
        hFunc(TEvents::TEvUndelivered, Handle);

        hFunc(TEvSubscribeOnPoolChanges, Handle);
        hFunc(TEvPlaceRequestIntoPool, Handle);
        hFunc(TEvCleanupRequest, Handle);
        hFunc(TEvents::TEvWakeup, Handle);

        hFunc(TEvFetchDatabaseResponse, Handle);
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
        hFunc(TEvPrivate::TEvStopPoolHandlerResponse, Handle);
        hFunc(TEvTxProxySchemeCache::TEvWatchNotifyDeleted, Handle);
        IgnoreFunc(TEvTxProxySchemeCache::TEvWatchNotifyUpdated);
        IgnoreFunc(TEvTxProxySchemeCache::TEvWatchNotifyUnavailable);
    )

private:
    ///
    /// Fetches database metadata and subscribes to Scheme Cache Watch via db's PathId.
    ///
    void Handle(TEvFetchDatabaseResponse::TPtr& ev) {
        const TString& databaseId = ev->Get()->DatabaseId;

        if (ev->Get()->Status == Ydb::StatusIds::SUCCESS) {
            YDB_LOG_DEBUG("[WorkloadService] Successfully fetched database info,",
                {"logPrefix", LogPrefix()},
                {"databaseId", databaseId},
                {"serverless", ev->Get()->Serverless},
                {"pathId", ev->Get()->PathId});

            // Subscribe to scheme cache watch for this PathId
            auto databaseState = GetOrCreateDatabaseState(databaseId);

            if (!databaseState->WatchKey) {
                databaseState->WatchKey = ++FreeWatchKey;
                auto subscribeEvent = new TEvTxProxySchemeCache::TEvWatchPathId(
                    ev->Get()->PathId, databaseState->WatchKey);

                Send(MakeSchemeCacheID(), subscribeEvent);

                YDB_LOG_DEBUG("[WorkloadService] Subscribed to scheme cache watch",
                    {"logPrefix", LogPrefix()},
                    {"database", databaseId},
                    {"pathId", ev->Get()->PathId},
                    {"watchKey", databaseState->WatchKey});
            }
        } else {
            YDB_LOG_DEBUG("[WorkloadService] Failed to fetch database info,",
                {"logPrefix", LogPrefix()},
                {"databaseId", databaseId},
                {"status", ev->Get()->Status},
                {"issues", ev->Get()->Issues.ToOneLineString()});
        }

        GetOrCreateDatabaseState(databaseId)->UpdateDatabaseInfo(ev);
    }

    ///
    /// Handles database deletion to prevent stale PathId mismatch on recreation.
    ///
    void Handle(TEvTxProxySchemeCache::TEvWatchNotifyDeleted::TPtr& ev) {
        const TString& databasePath = ev->Get()->Path;
        YDB_LOG_WARN("[WorkloadService] Database Cleaning all related states",
            {"logPrefix", LogPrefix()},
            {"deleted", databasePath});

        // Find all database states for this path
        std::vector<TString> databaseIdsToRemove;
        for (const auto& [databaseId, databaseState] : DatabaseToState) {
            if (DatabaseIdToDatabase(databaseId) == databasePath) {
                databaseIdsToRemove.push_back(databaseId);
            }
        }

        // Cleaning all pools and database state for each matching database ID
        for (const TString& databaseId : databaseIdsToRemove) {
            YDB_LOG_INFO("[WorkloadService] Removing database state",
                {"logPrefix", LogPrefix()},
                {"for", databaseId});
            CleanupDatabasePools(databaseId);
            DatabaseToState.erase(databaseId);
        }
    }

    ///
    /// Stops all active pool handlers and clears database state.
    ///
    void CleanupDatabasePools(const TString& databaseId) {
        std::vector<TString> poolsToRemove;
        const TString prefix = CanonizePath(databaseId) + "/";

        for (const auto& [poolKey, poolState] : PoolIdToState) {
            if (poolKey.StartsWith(prefix)) {
                YDB_LOG_INFO("[WorkloadService] Cleaning pool handler",
                    {"logPrefix", LogPrefix()},
                    {"for", poolKey});
                Send(poolState.PoolHandler, new TEvPrivate::TEvStopPoolHandler(true));

                if (poolState.NewPoolHandler) {
                    Send(*poolState.NewPoolHandler, new TEvPrivate::TEvStopPoolHandler(true));
                }

                for (const auto& previousHandler : poolState.PreviousPoolHandlers) {
                    Send(previousHandler, new TEvPrivate::TEvStopPoolHandler(true));
                }

                poolsToRemove.push_back(poolKey);
            }
        }

        for (const auto& poolKey : poolsToRemove) {
            Counters.ActivePools->Dec();
            PoolIdToState.erase(poolKey);
        }
    }

    void Handle(TEvPrivate::TEvFetchPoolResponse::TPtr& ev) {
        const TString& databaseId = ev->Get()->DatabaseId;
        const TString& poolId = ev->Get()->PoolId;

        TActorId poolHandler;
        if (ev->Get()->Status == Ydb::StatusIds::SUCCESS) {
            YDB_LOG_DEBUG("[WorkloadService] Successfully fetched",
                {"logPrefix", LogPrefix()},
                {"pool", poolId},
                {"databaseId", databaseId});
            poolHandler = GetOrCreatePoolState(databaseId, poolId, ev->Get()->PoolConfig)->PoolHandler;
        } else {
            YDB_LOG_WARN("[WorkloadService] Failed to fetch",
                {"logPrefix", LogPrefix()},
                {"pool", poolId},
                {"databaseId", databaseId},
                {"status", ev->Get()->Status},
                {"issues", ev->Get()->Issues.ToOneLineString()});
        }

        GetOrCreateDatabaseState(databaseId)->UpdatePoolInfo(ev, poolHandler);
    }

    void Handle(TEvPrivate::TEvResolvePoolResponse::TPtr& ev) {
        const auto& event = ev->Get()->Event;
        const TString& databaseId = event->Get()->DatabaseId;
        auto databaseState = GetOrCreateDatabaseState(databaseId);
        if (ev->Get()->DefaultPoolCreated) {
            databaseState->HasDefaultPool = true;
        }

        const TString& poolId = event->Get()->PoolId;
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            databaseState->RemovePendingSession(event->Get()->SessionId, [this](TEvCleanupRequest::TPtr event) {
                ReplyCleanupError(event->Sender, Ydb::StatusIds::NOT_FOUND, TStringBuilder() << "Pool " << event->Get()->PoolId << " not found");
            });
            ReplyContinueError(event->Sender, ev->Get()->Status, ev->Get()->Issues);
            return;
        }

        YDB_LOG_DEBUG("[WorkloadService] Successfully fetched pool",
            {"logPrefix", LogPrefix()},
            {"poolId", poolId},
            {"databaseId", databaseId},
            {"sessionId", event->Get()->SessionId});

        auto poolState = GetOrCreatePoolState(databaseId, poolId, ev->Get()->PoolConfig);
        poolState->PendingRequests.emplace(std::move(ev));
        poolState->StartPlaceRequest();
    }

    void Handle(TEvPrivate::TEvPlaceRequestIntoPoolResponse::TPtr& ev) {
        const TString& databaseId = ev->Get()->DatabaseId;
        const TString& poolId = ev->Get()->PoolId;
        const TString& sessionId = ev->Get()->SessionId;
        YDB_LOG_TRACE("[WorkloadService] Request placed into pool,",
            {"logPrefix", LogPrefix()},
            {"databaseId", databaseId},
            {"poolId", poolId},
            {"sessionId", sessionId});

        auto poolState = GetPoolState(databaseId, poolId);
        GetOrCreateDatabaseState(databaseId)->RemovePendingSession(sessionId, [this, poolState](TEvCleanupRequest::TPtr event) {
            if (poolState) {
                poolState->DoCleanupRequest(std::move(event));
            } else {
                ReplyCleanupError(event->Sender, Ydb::StatusIds::NOT_FOUND, TStringBuilder() << "Pool " << event->Get()->PoolId << " not found");
            }
        });

        if (poolState) {
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
        const TString& databaseId = event.GetDatabase();
        const TString& poolId = event.GetPoolId();
        YDB_LOG_TRACE("[WorkloadService] Got remote refresh request,",
            {"logPrefix", LogPrefix()},
            {"databaseId", databaseId},
            {"poolId", poolId},
            {"nodeId", ev->Sender.NodeId()});

        if (auto poolState = GetPoolState(databaseId, poolId)) {
            Send(ev->Forward(poolState->PoolHandler));
        }
    }

    void Handle(TEvPrivate::TEvCpuQuotaRequest::TPtr& ev) {
        const TActorId& poolHandler = ev->Sender;
        const double maxClusterLoad = ev->Get()->MaxClusterLoad;
        YDB_LOG_TRACE("[WorkloadService] Requested cpu quota from handler",
            {"logPrefix", LogPrefix()},
            {"poolHandler", poolHandler},
            {"maxClusterLoad", maxClusterLoad});

        CpuQuotaManager->RequestCpuQuota(poolHandler, maxClusterLoad, ev->Cookie);
        ScheduleCpuLoadRequest();
    }

    void Handle(TEvPrivate::TEvFinishRequestInPool::TPtr& ev) {
        const TString& databaseId = ev->Get()->DatabaseId;
        const TString& poolId = ev->Get()->PoolId;
        YDB_LOG_TRACE("[WorkloadService] Request finished in pool,",
            {"logPrefix", LogPrefix()},
            {"databaseId", databaseId},
            {"poolId", poolId},
            {"duration", ev->Get()->Duration},
            {"cpuConsumed", ev->Get()->CpuConsumed},
            {"adjustCpuQuota", ev->Get()->AdjustCpuQuota});

        if (auto poolState = GetPoolState(databaseId, poolId)) {
            poolState->OnRequestFinished();
        }
        if (ev->Get()->AdjustCpuQuota) {
            CpuQuotaManager->AdjustCpuQuota(ev->Get()->Duration, ev->Get()->CpuConsumed.SecondsFloat());
            ScheduleCpuLoadRequest();
        }
    }

    void Handle(TEvPrivate::TEvPrepareTablesRequest::TPtr& ev) {
        const TString& databaseId = ev->Get()->DatabaseId;
        const TString& poolId = ev->Get()->PoolId;
        YDB_LOG_TRACE("[WorkloadService] Got create teables request,",
            {"logPrefix", LogPrefix()},
            {"databaseId", databaseId},
            {"poolId", poolId});

        auto poolState = GetPoolState(databaseId, poolId);
        if (!poolState) {
            return;
        }

        if (TablesCreationStatus == ETablesCreationStatus::Finished) {
            Send(poolState->PoolHandler, new TEvPrivate::TEvTablesCreationFinished(true, {}));
        } else {
            poolState->WaitingInitialization = true;
            PendingHandlers.emplace(GetPoolKey(databaseId, poolId));
            PrepareWorkloadServiceTables();
        }
    }

    void Handle(TEvPrivate::TEvCleanupTablesFinished::TPtr& ev) {
        if (ev->Get()->Success) {
            YDB_LOG_DEBUG("[WorkloadService] Cleanup completed, tables",
                {"logPrefix", LogPrefix()},
                {"exists", ev->Get()->TablesExists});
        } else {
            YDB_LOG_CRIT("[WorkloadService] Failed to cleanup tables,",
                {"logPrefix", LogPrefix()},
                {"issues", ev->Get()->Issues.ToOneLineString()});
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
            YDB_LOG_DEBUG("[WorkloadService] Succefully created tables, send response to handlers",
                {"logPrefix", LogPrefix()});
            OnTabelsCreated(true);
            return;
        }

        YDB_LOG_ERROR("[WorkloadService] Failed to create tables,",
            {"logPrefix", LogPrefix()},
            {"issues", ev->Get()->Issues.ToOneLineString()});
        NYql::TIssues issues = GroupIssues(ev->Get()->Issues, "Failed to create workload service tables");
        OnTabelsCreated(false, issues);
    }

    void Handle(TEvPrivate::TEvCpuLoadResponse::TPtr& ev) {
        const bool success = ev->Get()->Status == Ydb::StatusIds::SUCCESS;
        if (!success) {
            YDB_LOG_ERROR("[WorkloadService] Failed to fetch cpu load",
                {"logPrefix", LogPrefix()},
                {"status", ev->Get()->Status},
                {"issues", ev->Get()->Issues.ToOneLineString()});
        } else {
            YDB_LOG_TRACE("[WorkloadService] Succesfully fetched cpu %, cpu",
                {"logPrefix", LogPrefix()},
                {"load", 100.0 * ev->Get()->InstantLoad},
                {"number", ev->Get()->CpuNumber});
        }

        CpuQuotaManager->CpuLoadRequestRunning = false;
        CpuQuotaManager->UpdateCpuLoad(ev->Get()->InstantLoad, ev->Get()->CpuNumber, success);
        ScheduleCpuLoadRequest();
    }

    void Handle(TEvPrivate::TEvResignPoolHandler::TPtr& ev) {
        const TString& databaseId = ev->Get()->DatabaseId;
        const TString& poolId = ev->Get()->PoolId;
        YDB_LOG_TRACE("[WorkloadService] Got resign request,",
            {"logPrefix", LogPrefix()},
            {"databaseId", databaseId},
            {"poolId", poolId});

        if (auto poolState = GetPoolState(databaseId, poolId)) {
            if (poolState->NewPoolHandler) {
                Send(*poolState->NewPoolHandler, new TEvPrivate::TEvStopPoolHandler(false));
            }
            poolState->NewPoolHandler = ev->Get()->NewHandler;
            poolState->UpdateHandler();
        }
    }

    void Handle(TEvPrivate::TEvStopPoolHandlerResponse::TPtr& ev) {
        const TString& databaseId = ev->Get()->DatabaseId;
        const TString& poolId = ev->Get()->PoolId;
        YDB_LOG_TRACE("[WorkloadService] Got stop pool handler response,",
            {"logPrefix", LogPrefix()},
            {"databaseId", databaseId},
            {"poolId", poolId});

        Counters.ActivePools->Dec();
        if (auto poolState = GetPoolState(databaseId, poolId)) {
            poolState->PreviousPoolHandlers.erase(ev->Sender);
        }
    }

private:
    void InitializeWorkloadService() {
        if (ServiceInitialized) {
            return;
        }
        ServiceInitialized = true;

        YDB_LOG_INFO("[WorkloadService] Started workload service initialization",
            {"logPrefix", LogPrefix()});
        Register(CreateCleanupTablesActor());
        RunNodeInfoRequest();
    }

    void PrepareWorkloadServiceTables() {
        if (PendingHandlers.empty()) {
            return;
        }

        if (TablesCreationStatus == ETablesCreationStatus::NotStarted) {
            YDB_LOG_INFO("[WorkloadService] Started workload service tables creation",
                {"logPrefix", LogPrefix()});
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
            if (!poolState.InFlightRequests && TInstant::Now() - poolState.LastUpdateTime > IDLE_DURATION && poolState.PendingRequests.empty()) {
                CpuQuotaManager->CleanupHandler(poolState.PoolHandler);
                Send(poolState.PoolHandler, new TEvPrivate::TEvStopPoolHandler(true));
                poolsToDelete.emplace_back(poolKey);
            }
        }
        for (const auto& poolKey : poolsToDelete) {
            PoolIdToState.erase(poolKey);
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
        Register(CreateTenantNodeEnumerationLookup(SelfId(), AppData()->TenantName));
    }

private:
    void ReplyContinueError(const TActorId& replyActorId, Ydb::StatusIds::StatusCode status, const TString& message) const {
        ReplyContinueError(replyActorId, status, {NYql::TIssue(message)});
    }

    void ReplyContinueError(const TActorId& replyActorId, Ydb::StatusIds::StatusCode status, NYql::TIssues issues) const {
        if (status == Ydb::StatusIds::UNSUPPORTED) {
            YDB_LOG_TRACE("[WorkloadService] Reply unsupported",
                {"logPrefix", LogPrefix()},
                {"replyActorId", replyActorId},
                {"issuesOneLine", issues.ToOneLineString()});
        } else {
            YDB_LOG_WARN("[WorkloadService] Reply continue error",
                {"logPrefix", LogPrefix()},
                {"status", status},
                {"replyActorId", replyActorId},
                {"issuesOneLine", issues.ToOneLineString()});
        }
        Send(replyActorId, new TEvContinueRequest(status, {}, {}, std::move(issues)));
    }

    void ReplyCleanupError(const TActorId& replyActorId, Ydb::StatusIds::StatusCode status, const TString& message) const {
        YDB_LOG_WARN("[WorkloadService] Reply cleanup error",
            {"logPrefix", LogPrefix()},
            {"status", status},
            {"replyActorId", replyActorId},
            {"message", message});
        Send(replyActorId, new TEvCleanupResponse(status, {NYql::TIssue(message)}));
    }

    TDatabaseState* GetOrCreateDatabaseState(TString databaseId) {
        auto databaseIt = DatabaseToState.find(databaseId);

        if (databaseIt != DatabaseToState.end()) {
            return &databaseIt->second;
        }

        YDB_LOG_INFO("[WorkloadService] Creating new database state for id",
            {"logPrefix", LogPrefix()},
            {"databaseId", databaseId});
        return &DatabaseToState.insert({databaseId, TDatabaseState{.SelfId = SelfId(), .EnabledResourcePoolsOnServerless = EnabledResourcePoolsOnServerless, .WorkloadManagerConfig = WorkloadManagerConfig}}).first->second;
    }

    TPoolState* GetOrCreatePoolState(const TString& databaseId, const TString& poolId, const NResourcePool::TPoolSettings& poolConfig) {
        const auto& poolKey = GetPoolKey(databaseId, poolId);
        if (auto poolState = GetPoolState(poolKey)) {
            return poolState;
        }

        YDB_LOG_INFO("[WorkloadService] Creating new handler for pool",
            {"logPrefix", LogPrefix()},
            {"poolKey", poolKey});

        const auto poolHandler = Register(CreatePoolHandlerActor(databaseId, poolId, poolConfig, EnableResourcePoolsCounters ? Counters.Counters : MakeIntrusive<NMonitoring::TDynamicCounters>()));
        const auto poolState = &PoolIdToState.insert({poolKey, TPoolState{.PoolHandler = poolHandler}}).first->second;

        Counters.ActivePools->Inc();
        ScheduleIdleCheck();

        return poolState;
    }

    TPoolState* GetPoolState(const TString& databaseId, const TString& poolId) {
        return GetPoolState(GetPoolKey(databaseId, poolId));
    }

    TPoolState* GetPoolState(const TString& key) {
        auto poolIt = PoolIdToState.find(key);
        if (poolIt != PoolIdToState.end()) {
            return &poolIt->second;
        }
        return nullptr;
    }

    static TString GetPoolKey(const TString& databaseId, const TString& poolId) {
        return CanonizePath(TStringBuilder() << databaseId << "/" << poolId);
    }

    TString LogPrefix() const {
        return "[Service] ";
    }

private:
    TCounters Counters;

    bool EnabledResourcePools = false;
    bool EnabledResourcePoolsOnServerless = false;
    bool EnableResourcePoolsCounters = false;
    bool ServiceInitialized = false;
    bool IdleChecksStarted = false;
    NKikimrConfig::TWorkloadManagerConfig WorkloadManagerConfig;
    ETablesCreationStatus TablesCreationStatus = ETablesCreationStatus::Cleanup;
    std::unordered_set<TString> PendingHandlers;  // DatabaseID/PoolID
    ui32 FreeWatchKey = 0;

    std::unordered_map<TString, TDatabaseState> DatabaseToState;  // DatabaseID to state
    std::unordered_map<TString, TPoolState> PoolIdToState;  // DatabaseID/PoolID to state
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
