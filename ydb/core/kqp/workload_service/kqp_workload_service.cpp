#include "kqp_workload_service.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/path.h>

#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>

#include <ydb/core/kqp/workload_service/actors/actors.h>
#include <ydb/core/kqp/workload_service/common/events.h>
#include <ydb/core/kqp/workload_service/common/helpers.h>
#include <ydb/core/kqp/workload_service/tables/table_queries.h>

#include <ydb/core/protos/console_config.pb.h>


namespace NKikimr::NKqp {

namespace NWorkload {

namespace {

using namespace NActors;

constexpr TDuration IDLE_DURATION = TDuration::Seconds(15);


class TKqpWorkloadService : public TActorBootstrapped<TKqpWorkloadService> {
    enum class ETablesCreationStatus {
        Cleanup,
        NotStarted,
        Pending,
        Finished,
    };

    struct TPoolState {
        TActorId PoolHandler;
        TActorContext ActorContext;

        std::queue<TEvPrivate::TEvResolvePoolResponse::TPtr> PendingRequests = {};
        bool WaitingInitialization = false;
        bool PlaceRequestRunning = false;
        std::optional<TActorId> NewPoolHandler = std::nullopt;

        ui64 InFlightRequests = 0;
        TInstant LastUpdateTime = TInstant::Now();

        void UpdateHandler() {
            if (PlaceRequestRunning || WaitingInitialization || !NewPoolHandler) {
                return;
            }

            ActorContext.Send(PoolHandler, new TEvPrivate::TEvStopPoolHandler());
            PoolHandler = *NewPoolHandler;
            NewPoolHandler = std::nullopt;
            InFlightRequests = 0;
        }

        void StartPlaceRequest() {
            if (PlaceRequestRunning || PendingRequests.empty()) {
                return;
            }

            PlaceRequestRunning = true;
            InFlightRequests++;
            ActorContext.Send(PendingRequests.front()->Forward(PoolHandler));
            PendingRequests.pop();
        }

        void OnRequestFinished() {
            Y_ENSURE(InFlightRequests);
            InFlightRequests--;
            LastUpdateTime = TInstant::Now();
        }
    };

public:
    explicit TKqpWorkloadService(NMonitoring::TDynamicCounterPtr counters)
        : Counters(counters)
    {
        RegisterCounters();
    }

    void Bootstrap() {
        Become(&TKqpWorkloadService::MainState);

        // Subscribe for FeatureFlags
        Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()), new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({
            (ui32)NKikimrConsole::TConfigItem::FeatureFlagsItem
        }), IEventHandle::FlagTrackDelivery);

        EnabledResourcePools = AppData()->FeatureFlags.GetEnableResourcePools();
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
        if (EnabledResourcePools) {
            LOG_I("Resource pools was enanbled");
            InitializeWorkloadService();
        } else {
            LOG_I("Resource pools was disabled");
        }

        auto responseEvent = std::make_unique<NConsole::TEvConsole::TEvConfigNotificationResponse>(event);
        Send(ev->Sender, responseEvent.release(), IEventHandle::FlagTrackDelivery, ev->Cookie);
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) const {
        switch (ev->Get()->SourceType) {
            case NConsole::TEvConfigsDispatcher::EvSetConfigSubscriptionRequest:
                LOG_C("Failed to deliver subscription request to config dispatcher");
                break;

            case NConsole::TEvConsole::EvConfigNotificationResponse:
                LOG_E("Failed to deliver config notification response");
                break;

            default:
                LOG_E("Undelivered event with unexpected source type: " << ev->Get()->SourceType);
                break;
        }
    }

    void Handle(TEvPlaceRequestIntoPool::TPtr& ev) {
        const TActorId& workerActorId = ev->Sender;
        if (!EnabledResourcePools) {
            ReplyContinueError(workerActorId, Ydb::StatusIds::UNSUPPORTED, "Workload manager is disabled. Please contact your system administrator to enable it");
            return;
        }

        // Add AllAuthenticatedUsers group SID into user token
        ev->Get()->UserToken = GetUserToken(ev->Get()->UserToken);

        LOG_D("Recieved new request from " << workerActorId << ", Database: " << ev->Get()->Database << ", PoolId: " << ev->Get()->PoolId << ", SessionId: " << ev->Get()->SessionId);
        bool hasDefaultPool = DatabasesWithDefaultPool.contains(CanonizePath(ev->Get()->Database));
        Register(CreatePoolResolverActor(std::move(ev), hasDefaultPool));
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

    void HandleWakeup() {
        IdleChecksStarted = false;

        std::vector<TString> poolsToDelete;
        poolsToDelete.reserve(PoolIdToState.size());
        for (const auto& [poolKey, poolState] : PoolIdToState) {
            if (!poolState.InFlightRequests && TInstant::Now() - poolState.LastUpdateTime > IDLE_DURATION) {
                Send(poolState.PoolHandler, new TEvPrivate::TEvStopPoolHandler());
                poolsToDelete.emplace_back(poolKey);
            }
        }
        for (const auto& poolKey : poolsToDelete) {
            PoolIdToState.erase(poolKey);
            ActivePools->Dec();
        }

        if (!PoolIdToState.empty()) {
            StartIdleChecks();
        }
    }

    STRICT_STFUNC(MainState,
        sFunc(TEvents::TEvPoison, HandlePoison);
        sFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, HandleSetConfigSubscriptionResponse);
        hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, Handle);
        hFunc(TEvents::TEvUndelivered, Handle);

        hFunc(TEvPlaceRequestIntoPool, Handle);
        hFunc(TEvCleanupRequest, Handle);
        sFunc(TEvents::TEvWakeup, HandleWakeup);

        hFunc(TEvPrivate::TEvResolvePoolResponse, Handle);
        hFunc(TEvPrivate::TEvPlaceRequestIntoPoolResponse, Handle);
        hFunc(TEvPrivate::TEvRefreshPoolState, Handle);
        hFunc(TEvPrivate::TEvFinishRequestInPool, Handle);
        hFunc(TEvPrivate::TEvPrepareTablesRequest, Handle);
        hFunc(TEvPrivate::TEvCleanupTablesFinished, Handle);
        hFunc(TEvPrivate::TEvTablesCreationFinished, Handle);
        hFunc(TEvPrivate::TEvResignPoolHandler, Handle);
    )

private:
    void Handle(TEvPrivate::TEvResolvePoolResponse::TPtr& ev) {
        const auto& event = ev->Get()->Event;
        const TString& database = event->Get()->Database;
        if (ev->Get()->DefaultPoolCreated) {
            DatabasesWithDefaultPool.insert(CanonizePath(database));
        }

        const TString& poolId = event->Get()->PoolId;
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            ReplyContinueError(event->Sender, ev->Get()->Status, ev->Get()->Issues);
            return;
        }

        LOG_D("Successfully fetched pool " << poolId << ", Database: " << database << ", SessionId: " << event->Get()->SessionId);

        auto poolState = GetPoolState(database, poolId);
        if (!poolState) {
            TString poolKey = GetPoolKey(database, poolId);
            LOG_I("Creating new handler for pool " << poolKey);

            auto poolHandler = Register(CreatePoolHandlerActor(database, poolId, ev->Get()->PoolConfig, Counters));
            poolState = &PoolIdToState.insert({poolKey, TPoolState{.PoolHandler = poolHandler, .ActorContext = ActorContext()}}).first->second;

            ActivePools->Inc();
            StartIdleChecks();
        }

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

    void Handle(TEvPrivate::TEvRefreshPoolState::TPtr& ev) {
        const auto& event = ev->Get()->Record;
        const TString& database = event.GetDatabase();
        const TString& poolId = event.GetPoolId();
        LOG_T("Got remote refresh request, Database: " << database << ", PoolId: " << poolId << ", NodeId: " << ev->Sender.NodeId());

        if (auto poolState = GetPoolState(database, poolId)) {
            Send(ev->Forward(poolState->PoolHandler));
        }
    }

    void Handle(TEvPrivate::TEvFinishRequestInPool::TPtr& ev) {
        const TString& database = ev->Get()->Database;
        const TString& poolId = ev->Get()->PoolId;
        LOG_T("Request finished in pool, Database: " << database << ", PoolId: " << poolId);

        if (auto poolState = GetPoolState(database, poolId)) {
            poolState->OnRequestFinished();
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

    void StartIdleChecks() {
        if (IdleChecksStarted) {
            return;
        }
        IdleChecksStarted = true;

        Schedule(IDLE_DURATION, new TEvents::TEvWakeup());
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

    static TIntrusivePtr<NACLib::TUserToken> GetUserToken(TIntrusiveConstPtr<NACLib::TUserToken> userToken) {
        auto token = MakeIntrusive<NACLib::TUserToken>(userToken ? userToken->GetUserSID() : NACLib::TSID(), TVector<NACLib::TSID>{});

        bool hasAllAuthenticatedUsersSID = false;
        const auto& allAuthenticatedUsersSID = AppData()->AllAuthenticatedUsers;
        if (userToken) {
            for (const auto& groupSID : userToken->GetGroupSIDs()) {
                token->AddGroupSID(groupSID);
                hasAllAuthenticatedUsersSID = hasAllAuthenticatedUsersSID || groupSID == allAuthenticatedUsersSID;
            }
        }

        if (!hasAllAuthenticatedUsersSID) {
            token->AddGroupSID(allAuthenticatedUsersSID);
        }

        return token;
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

    void RegisterCounters() {
        ActivePools = Counters->GetCounter("ActivePools", false);
    }

private:
    NMonitoring::TDynamicCounterPtr Counters;

    bool EnabledResourcePools = false;
    bool ServiceInitialized = false;
    bool IdleChecksStarted = false;
    ETablesCreationStatus TablesCreationStatus = ETablesCreationStatus::Cleanup;
    std::unordered_set<TString> PendingHandlers;

    std::unordered_set<TString> DatabasesWithDefaultPool;
    std::unordered_map<TString, TPoolState> PoolIdToState;

    NMonitoring::TDynamicCounters::TCounterPtr ActivePools;
};

}  // anonymous namespace

}  // namespace NWorkload

IActor* CreateKqpWorkloadService(NMonitoring::TDynamicCounterPtr counters) {
    return new NWorkload::TKqpWorkloadService(counters);
}

}  // namespace NKikimr::NKqp
