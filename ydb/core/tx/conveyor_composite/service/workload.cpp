#include "workload.h"

#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/runtime/scheduler/tree/dynamic.h>
#include <ydb/library/actors/core/actor.h>

#include <yt/yt/core/utilex/random.h>

namespace NKikimr::NConveyorComposite {

    TConveyorWorkUnit::TConveyorWorkUnit(
        TWorkloadContext context, const NKqp::NScheduler::NHdrf::NDynamic::TQueryPtr& query)
        : Context(std::move(context))
        , SchedulableTask(std::make_shared<NKqp::NScheduler::TSchedulableTask>(query))
    {
    }

    TConveyorWorkUnit::~TConveyorWorkUnit() {
        AFL_VERIFY(!Running);
        if (Throttled) {
            LeaveThrottle(TMonotonic::Now());
        }
    }

    bool TConveyorWorkUnit::TryStart() {
        AFL_VERIFY(!Running);

        if (!SchedulableTask->Query->GetSnapshot() || !SchedulableTask->TryIncreaseUsage()) {
            const auto now = TMonotonic::Now();
            if (Throttled) {
                SchedulableTask->IncreaseBurstThrottle(now - ThrottleStart);
            } else {
                Throttled = true;
                SchedulableTask->IncreaseThrottle();
            }
            ThrottleStart = now;
            return false;
        }

        if (Throttled) {
            LeaveThrottle(TMonotonic::Now());
        }
        Running = true;
        return true;
    }

    void TConveyorWorkUnit::Finish(TDuration actual) {
        AFL_VERIFY(Running);

        SchedulableTask->DecreaseUsage(actual, NKqp::NScheduler::TSchedulableTask::CPU_DEFAULT);
        Running = false;
    }

    TDuration TConveyorWorkUnit::CalculateDelay() const {
        const auto query = SchedulableTask->Query;
        const auto randomDelay = TDuration::MicroSeconds(
            RandomNumber<ui64>() % query->DelayParams->MaxRandomDelay.MicroSeconds());
        return Min(query->DelayParams->MaxDelay, query->DelayParams->MinDelay + randomDelay);
    }

    void TConveyorWorkUnit::LeaveThrottle(TMonotonic now) {
        AFL_VERIFY(Throttled);
        SchedulableTask->IncreaseBurstThrottle(now - ThrottleStart);
        SchedulableTask->DecreaseThrottle();
        Throttled = false;
    }

    TWorkloadScheduler::TWorkloadScheduler(NKqp::NScheduler::TComputeSchedulerPtr scheduler)
        : Scheduler(std::move(scheduler))
    {
    }

    void TWorkloadScheduler::RegisterProcess(const TWorkloadContext& context) {
        if (!context.IsDefined()) {
            return;
        }

        auto [it, inserted] = Queries.try_emplace(context.QueryId);
        auto& entry = it->second;
        if (inserted) {
            entry.Context = context;
        } else {
            AFL_VERIFY(entry.Context == context);
        }
        ++entry.ProcessRefCount;
        EnsureQueryRegistration(entry);
    }

    void TWorkloadScheduler::UnregisterProcess(const TWorkloadContext& context) {
        if (!context.IsDefined()) {
            return;
        }

        auto it = Queries.find(context.QueryId);
        AFL_VERIFY(it != Queries.end());
        AFL_VERIFY(it->second.Context == context);
        AFL_VERIFY(it->second.ProcessRefCount > 0);
        if (--it->second.ProcessRefCount != 0) {
            return;
        }
        it->second.PendingWorkUnit.reset();
        if (it->second.RegistrationPending) {
            return;
        }
        if (it->second.Query && it->second.OwnsRegistration) {
            RemoveQuery(context.QueryId);
        }
        Queries.erase(it);
    }

    void TWorkloadScheduler::OnQueryResponse(ui64 queryId, NKqp::NScheduler::NHdrf::NDynamic::TQueryPtr query) {
        auto it = Queries.find(queryId);
        if (it == Queries.end()) {
            return;
        }

        it->second.RegistrationPending = false;
        it->second.Query = std::move(query);
        if (it->second.ProcessRefCount == 0) {
            if (it->second.Query && it->second.OwnsRegistration) {
                RemoveQuery(queryId);
            }
            Queries.erase(it);
        }
    }

    bool TWorkloadScheduler::TryAddToBatch(const TWorkloadContext& context, TConveyorWorkUnits& workUnits) {
        if (!context.IsDefined()) {
            return true;
        }
        if (!IsEnabled()) {
            if (auto it = Queries.find(context.QueryId); it != Queries.end()) {
                it->second.PendingWorkUnit.reset();
            }
            return true;
        }
        if (auto it = workUnits.find(context.QueryId); it != workUnits.end()) {
            AFL_VERIFY(it->second->GetContext() == context);
            return true;
        }

        auto it = Queries.find(context.QueryId);
        AFL_VERIFY(it != Queries.end());
        AFL_VERIFY(it->second.Context == context);
        EnsureQueryRegistration(it->second);
        if (!it->second.Query) {
            RegisterRetry(TDuration::MilliSeconds(1));
            return false;
        }

        if (!it->second.PendingWorkUnit) {
            it->second.PendingWorkUnit = std::make_unique<TConveyorWorkUnit>(context, it->second.Query);
        }
        if (!it->second.PendingWorkUnit->TryStart()) {
            RegisterRetry(it->second.PendingWorkUnit->CalculateDelay());
            return false;
        }

        AFL_VERIFY(workUnits.emplace(context.QueryId, std::move(it->second.PendingWorkUnit)).second);
        return true;
    }

    std::optional<TMonotonic> TWorkloadScheduler::ExtractNextWakeup() {
        return std::exchange(NextWakeup, std::nullopt);
    }

    void TWorkloadScheduler::EnsureQueryRegistration(TQueryEntry& entry) {
        if (!IsEnabled() || entry.Query || entry.RegistrationPending) {
            return;
        }
        if (entry.Query = Scheduler->GetQuery(
                entry.Context.DatabaseId, entry.Context.PoolId, entry.Context.QueryId)) {
            return;
        }
        AFL_VERIFY(NActors::TlsActivationContext);

        const auto schedulerServiceId = NKqp::MakeKqpSchedulerServiceId(
            NActors::TActorContext::AsActorContext().SelfID.NodeId());
        NActors::TActorContext::AsActorContext().Send(
            schedulerServiceId, new NKqp::NScheduler::TEvAddDatabase(entry.Context.DatabaseId));
        NActors::TActorContext::AsActorContext().Send(
            schedulerServiceId, new NKqp::NScheduler::TEvAddPool(entry.Context.DatabaseId, entry.Context.PoolId));
        auto addQuery = std::make_unique<NKqp::NScheduler::TEvAddQuery>();
        addQuery->DatabaseId = entry.Context.DatabaseId;
        addQuery->PoolId = entry.Context.PoolId;
        addQuery->QueryId = entry.Context.QueryId;
        NActors::TActorContext::AsActorContext().Send(schedulerServiceId, addQuery.release());
        entry.RegistrationPending = true;
        entry.OwnsRegistration = true;
    }

    void TWorkloadScheduler::RemoveQuery(ui64 queryId) {
        if (!NActors::TlsActivationContext) {
            return;
        }
        auto removeQuery = std::make_unique<NKqp::NScheduler::TEvRemoveQuery>();
        removeQuery->QueryId = queryId;
        NActors::TActorContext::AsActorContext().Send(
            NKqp::MakeKqpSchedulerServiceId(NActors::TActorContext::AsActorContext().SelfID.NodeId()), removeQuery.release());
    }

    void TWorkloadScheduler::RegisterRetry(TDuration delay) {
        const auto deadline = TMonotonic::Now() + Max(delay, TDuration::MilliSeconds(1));
        if (!NextWakeup || deadline < *NextWakeup) {
            NextWakeup = deadline;
        }
    }

} // namespace NKikimr::NConveyorComposite
