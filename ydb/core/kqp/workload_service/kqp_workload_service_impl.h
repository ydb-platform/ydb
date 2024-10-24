#pragma once

#include <queue>

#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/workload_service/actors/actors.h>
#include <ydb/core/kqp/workload_service/common/cpu_quota_manager.h>
#include <ydb/core/kqp/workload_service/common/events.h>
#include <ydb/core/kqp/workload_service/common/helpers.h>


namespace NKikimr::NKqp::NWorkload {

constexpr TDuration IDLE_DURATION = TDuration::Seconds(60);


struct TDatabaseState {
    NActors::TActorContext ActorContext;
    bool& EnabledResourcePoolsOnServerless;

    std::vector<TEvPlaceRequestIntoPool::TPtr> PendingRequersts = {};
    std::unordered_set<TString> PendingSessionIds = {};
    std::unordered_map<TString, std::vector<TEvCleanupRequest::TPtr>> PendingCancelRequests = {};  // Session ID to requests
    std::unordered_map<TString, std::unordered_set<TActorId>> PendingSubscriptions = {};  // Pool ID to subscribers
    bool HasDefaultPool = false;
    bool Serverless = false;
    bool DatabaseUnsupported = false;

    TInstant LastUpdateTime = TInstant::Zero();

    void DoSubscribeRequest(TEvSubscribeOnPoolChanges::TPtr ev) {
        const TString& poolId = ev->Get()->PoolId;
        auto& subscribers = PendingSubscriptions[poolId];
        if (subscribers.empty()) {
            ActorContext.Register(CreatePoolFetcherActor(ActorContext.SelfID, ev->Get()->DatabaseId, poolId, nullptr));
        }

        subscribers.emplace(ev->Sender);
    }

    void DoPlaceRequest(TEvPlaceRequestIntoPool::TPtr ev) {
        TString databaseId = ev->Get()->DatabaseId;
        PendingSessionIds.emplace(ev->Get()->SessionId);
        PendingRequersts.emplace_back(std::move(ev));

        if (!EnabledResourcePoolsOnServerless && (TInstant::Now() - LastUpdateTime) > IDLE_DURATION) {
            ActorContext.Register(CreateDatabaseFetcherActor(ActorContext.SelfID, DatabaseIdToDatabase(databaseId)));
        } else if (!DatabaseUnsupported) {
            StartPendingRequests();
        } else {
            ReplyContinueError(Ydb::StatusIds::UNSUPPORTED, {NYql::TIssue(TStringBuilder() << "Unsupported database: " << databaseId)});
        }
    }

    void UpdatePoolInfo(const TEvPrivate::TEvFetchPoolResponse::TPtr& ev, NActors::TActorId poolHandler) {
        const TString& poolId = ev->Get()->PoolId;
        auto& subscribers = PendingSubscriptions[poolId];
        if (subscribers.empty()) {
            return;
        }

        if (ev->Get()->Status == Ydb::StatusIds::SUCCESS && poolHandler) {
            ActorContext.Send(poolHandler, new TEvPrivate::TEvUpdatePoolSubscription(ev->Get()->PathId, subscribers));
        } else {
            const TString& databaseId = ev->Get()->DatabaseId;
            for (const auto& subscriber : subscribers) {
                ActorContext.Send(subscriber, new TEvUpdatePoolInfo(databaseId, poolId, std::nullopt, std::nullopt));
            }
        }
        subscribers.clear();
    }

    void UpdateDatabaseInfo(const TEvFetchDatabaseResponse::TPtr& ev) {
        DatabaseUnsupported = ev->Get()->Status == Ydb::StatusIds::UNSUPPORTED;
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            ReplyContinueError(ev->Get()->Status, GroupIssues(ev->Get()->Issues, "Failed to fetch database info"));
            return;
        }

        if (Serverless != ev->Get()->Serverless) {
            ActorContext.Send(MakeKqpProxyID(ActorContext.SelfID.NodeId()), new TEvKqp::TEvUpdateDatabaseInfo(ev->Get()->Database, ev->Get()->DatabaseId, ev->Get()->Serverless));
        }

        LastUpdateTime = TInstant::Now();
        Serverless = ev->Get()->Serverless;
        StartPendingRequests();
    }

    void RemovePendingSession(const TString& sessionId, std::function<void(TEvCleanupRequest::TPtr)> callback) {
        for (auto& event : PendingCancelRequests[sessionId]) {
            callback(std::move(event));
        }
        PendingCancelRequests.erase(sessionId);
        PendingSessionIds.erase(sessionId);
    }

private:
    void StartPendingRequests() {
        if (!EnabledResourcePoolsOnServerless && Serverless) {
            ReplyContinueError(Ydb::StatusIds::UNSUPPORTED, {NYql::TIssue("Resource pools are disabled for serverless domains. Please contact your system administrator to enable it")});
            return;
        }

        for (auto& ev : PendingRequersts) {
            ActorContext.Register(CreatePoolResolverActor(std::move(ev), HasDefaultPool));
        }
        PendingRequersts.clear();
    }

    void ReplyContinueError(Ydb::StatusIds::StatusCode status, NYql::TIssues issues) {
        for (const auto& ev : PendingRequersts) {
            RemovePendingSession(ev->Get()->SessionId, [this](TEvCleanupRequest::TPtr event) {
                ActorContext.Send(event->Sender, new TEvCleanupResponse(Ydb::StatusIds::NOT_FOUND, {NYql::TIssue(TStringBuilder() << "Pool " << event->Get()->PoolId << " not found")}));
            });
            ActorContext.Send(ev->Sender, new TEvContinueRequest(status, {}, {}, issues));
        }
        PendingRequersts.clear();
    }
};

struct TPoolState {
    NActors::TActorId PoolHandler;
    NActors::TActorContext ActorContext;

    std::queue<TEvPrivate::TEvResolvePoolResponse::TPtr> PendingRequests = {};
    bool WaitingInitialization = false;
    bool PlaceRequestRunning = false;
    std::optional<TActorId> NewPoolHandler = std::nullopt;
    std::unordered_set<TActorId> PreviousPoolHandlers = {};

    ui64 InFlightRequests = 0;
    TInstant LastUpdateTime = TInstant::Now();

    void UpdateHandler() {
        if (PlaceRequestRunning || WaitingInitialization || !NewPoolHandler) {
            return;
        }

        ActorContext.Send(PoolHandler, new TEvPrivate::TEvStopPoolHandler(false));
        PreviousPoolHandlers.insert(PoolHandler);
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

    void DoCleanupRequest(TEvCleanupRequest::TPtr event) {
        for (const auto& poolHandler : PreviousPoolHandlers) {
            ActorContext.Send(poolHandler, new TEvCleanupRequest(
                event->Get()->DatabaseId, event->Get()->SessionId,
                event->Get()->PoolId, event->Get()->Duration, event->Get()->CpuConsumed
            ));
        }
        ActorContext.Send(event->Forward(PoolHandler));
    }
};

struct TCpuQuotaManagerState {
    TCpuQuotaManager CpuQuotaManager;
    NActors::TActorContext ActorContext;
    bool CpuLoadRequestRunning = false;
    TInstant CpuLoadRequestTime = TInstant::Zero();

    TCpuQuotaManagerState(NActors::TActorContext actorContext, NMonitoring::TDynamicCounterPtr subComponent)
        : CpuQuotaManager(TDuration::Seconds(1), TDuration::Seconds(10), IDLE_DURATION, 0.1, true, 0, subComponent)
        , ActorContext(actorContext)
    {}

    void RequestCpuQuota(TActorId poolHandler, double maxClusterLoad, ui64 coockie) {
        auto response = CpuQuotaManager.RequestCpuQuota(0.0, maxClusterLoad);

        bool quotaAccepted = response.Status == NYdb::EStatus::SUCCESS;
        ActorContext.Send(poolHandler, new TEvPrivate::TEvCpuQuotaResponse(quotaAccepted, maxClusterLoad, std::move(response.Issues)), 0, coockie);

        // Schedule notification
        if (!quotaAccepted) {
            if (auto it = HandlersLimits.find(poolHandler); it != HandlersLimits.end()) {
                PendingHandlers[it->second].erase(poolHandler);
            }
            HandlersLimits[poolHandler] = maxClusterLoad;
            PendingHandlers[maxClusterLoad].insert(poolHandler);
        }
    }

    void UpdateCpuLoad(double instantLoad, ui64 cpuNumber, bool success) {
        CpuQuotaManager.UpdateCpuLoad(instantLoad, cpuNumber, success);
        CheckPendingQueue();
    }

    void AdjustCpuQuota(TDuration duration, double cpuSecondsConsumed) {
        CpuQuotaManager.AdjustCpuQuota(0.0, duration, cpuSecondsConsumed);
        CheckPendingQueue();
    }

    std::optional<TDuration> GetCpuLoadRequestDelay() {
        if (CpuLoadRequestRunning) {
            return std::nullopt;
        }

        auto requestTime = CpuQuotaManager.GetMonitoringRequestTime();
        if (!CpuLoadRequestTime || CpuLoadRequestTime > requestTime) {
            CpuLoadRequestTime = requestTime;
            return CpuLoadRequestTime - TInstant::Now();
        }
        return std::nullopt;
    }

    void CleanupHandler(TActorId poolHandler) {
        if (auto it = HandlersLimits.find(poolHandler); it != HandlersLimits.end()) {
            PendingHandlers[it->second].erase(poolHandler);
            HandlersLimits.erase(it);
        }
    }

private:
    void CheckPendingQueue() {
        while (!PendingHandlers.empty()) {
            const auto& [maxClusterLoad, poolHandlers] = *PendingHandlers.begin();
            if (!CpuQuotaManager.HasCpuQuota(maxClusterLoad)) {
                break;
            }

            for (const TActorId& poolHandler : poolHandlers) {
                ActorContext.Send(poolHandler, new TEvPrivate::TEvRefreshPoolState());
                HandlersLimits.erase(poolHandler);
            }
            PendingHandlers.erase(PendingHandlers.begin());
        }
    }

private:
    std::map<double, std::unordered_set<TActorId>> PendingHandlers;
    std::unordered_map<TActorId, double> HandlersLimits;
};

}  // namespace NKikimr::NKqp::NWorkload
