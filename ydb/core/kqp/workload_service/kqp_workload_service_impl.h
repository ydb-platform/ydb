#pragma once

#include <queue>

#include <ydb/core/kqp/workload_service/common/cpu_quota_manager.h>
#include <ydb/core/kqp/workload_service/common/events.h>


namespace NKikimr::NKqp::NWorkload {

constexpr TDuration IDLE_DURATION = TDuration::Seconds(60);

struct TPoolState {
    NActors::TActorId PoolHandler;
    NActors::TActorContext ActorContext;

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
        ActorContext.Send(poolHandler, new TEvPrivate::TEvCpuQuotaResponse(quotaAccepted), 0, coockie);

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
