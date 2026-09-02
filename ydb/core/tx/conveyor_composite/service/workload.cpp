#include "workload.h"

#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/runtime/scheduler/tree/dynamic.h>
#include <ydb/library/actors/core/actor.h>

#include <yt/yt/core/utilex/random.h>

namespace NKikimr::NConveyorComposite {

    void TCpuQuotaBucket::Update(const NKqp::NScheduler::TCpuQuotaSettings& settings, TMonotonic now) {
        Bucket.Fill(now);
        if (settings.IsUnlimited()) {
            Bucket.SetUnlimited();
            return;
        }

        AFL_VERIFY(std::isfinite(settings.RefillRateUsPerSecond));
        AFL_VERIFY(std::isfinite(settings.BurstCapacityUs));
        AFL_VERIFY(settings.RefillRateUsPerSecond >= 0);
        AFL_VERIFY(settings.BurstCapacityUs >= 0);
        Bucket.SetRate(settings.RefillRateUsPerSecond);
        Bucket.SetCapacity(settings.BurstCapacityUs);
    }

    std::optional<TDuration> TCpuQuotaBucket::CalculateDelay(TMonotonic now) {
        Bucket.Fill(now);
        if (Bucket.IsUnlimited() || Bucket.Available() > 0) {
            return TDuration::Zero();
        }
        if (Bucket.GetRate() <= 0 || Bucket.GetCapacity() <= 0) {
            return std::nullopt;
        }

        return Bucket.NextAvailableDelay() + TDuration::MicroSeconds(1);
    }

    void TCpuQuotaBucket::Account(TDuration actual, TMonotonic now) {
        AFL_VERIFY(actual >= TDuration::Zero());
        Bucket.Fill(now);
        if (!Bucket.IsUnlimited()) {
            Bucket.Take(actual.MicroSeconds());
        }
    }

    void TWorkloadPoolState::UpdateQuota(const NKqp::NScheduler::TCpuQuotaSettings& settings, TMonotonic now) {
        Quota.Update(settings, now);
    }

    std::optional<TDuration> TWorkloadPoolState::CalculateQuotaDelay(TMonotonic now) {
        return Quota.CalculateDelay(now);
    }

    void TWorkloadPoolState::Account(TDuration actual, TMonotonic now) {
        Quota.Account(actual, now);
    }

    TWorkloadQueryState::TWorkloadQueryState(TWorkloadContext context,
                                             NKqp::NScheduler::NHdrf::NDynamic::TQueryPtr query, TWorkloadPoolStatePtr pool)
        : Context(std::move(context))
        , Query(std::move(query))
        , Pool(std::move(pool))
    {
        AFL_VERIFY(Context.IsDefined());
        AFL_VERIFY(Query);
        AFL_VERIFY(Pool);
    }

    void TWorkloadQueryState::UpdateQuota(
        const NKqp::NScheduler::TWorkloadCpuQuotaSettings& settings, TMonotonic now) {
        Quota.Update(settings.Query, now);
        Pool->UpdateQuota(settings.Pool, now);
    }

    std::optional<TDuration> TWorkloadQueryState::CalculateQuotaDelay(TMonotonic now) {
        const auto queryDelay = Quota.CalculateDelay(now);
        const auto poolDelay = Pool->CalculateQuotaDelay(now);
        if (!queryDelay || !poolDelay) {
            return std::nullopt;
        }
        return Max(*queryDelay, *poolDelay);
    }

    void TWorkloadQueryState::Account(TDuration actual, TMonotonic now) {
        Quota.Account(actual, now);
        Pool->Account(actual, now);
    }

    TConveyorWorkUnit::TConveyorWorkUnit(TWorkloadQueryStatePtr queryState)
        : QueryState(std::move(queryState))
        , SchedulableTask(std::make_shared<NKqp::NScheduler::TSchedulableTask>(QueryState->GetQuery()))
    {
        AFL_VERIFY(QueryState);
    }

    TConveyorWorkUnit::~TConveyorWorkUnit() {
        AFL_VERIFY(!Running);
        if (Throttled) {
            LeaveThrottle(TMonotonic::Now());
        }
    }

    bool TConveyorWorkUnit::TryStart() {
        AFL_VERIFY(!Running);

        const auto now = TMonotonic::Now();
        const auto quotaDelay = QueryState->CalculateQuotaDelay(now);
        const bool quotaAvailable = quotaDelay && *quotaDelay == TDuration::Zero();
        if (!quotaAvailable || !SchedulableTask->Query->GetSnapshot() || !SchedulableTask->TryIncreaseUsage()) {
            RetryDelay = quotaAvailable ? CalculateHdrfDelay() : quotaDelay.value_or(CalculateHdrfDelay());
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
            LeaveThrottle(now);
        }
        RetryDelay = TDuration::Zero();
        Running = true;
        return true;
    }

    void TConveyorWorkUnit::Finish(TDuration actual) {
        AFL_VERIFY(Running);

        const auto now = TMonotonic::Now();
        QueryState->Account(actual, now);
        SchedulableTask->DecreaseUsage(actual, NKqp::NScheduler::TSchedulableTask::CPU_DEFAULT);
        Running = false;
    }

    TDuration TConveyorWorkUnit::CalculateDelay() const {
        AFL_VERIFY(!Running);
        return RetryDelay;
    }

    TDuration TConveyorWorkUnit::CalculateHdrfDelay() const {
        const auto query = SchedulableTask->Query;
        const auto maxRandomDelayUs = Max<ui64>(query->DelayParams->MaxRandomDelay.MicroSeconds(), 1);
        const auto randomDelay = TDuration::MicroSeconds(
            RandomNumber<ui64>() % maxRandomDelayUs);
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
        if (it->second.QueryState && it->second.OwnsRegistration) {
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
        if (query) {
            it->second.QueryState = MakeQueryState(it->second.Context, std::move(query));
        }
        if (it->second.ProcessRefCount == 0) {
            if (it->second.QueryState && it->second.OwnsRegistration) {
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
        if (!it->second.QueryState) {
            RegisterRetry(TDuration::MilliSeconds(1));
            return false;
        }
        const auto quotaSettings = Scheduler->GetCpuQuotaSettings(
            context.DatabaseId, context.PoolId, context.QueryId);
        if (!quotaSettings) {
            RegisterRetry(TDuration::MilliSeconds(1));
            return false;
        }
        it->second.QueryState->UpdateQuota(*quotaSettings, TMonotonic::Now());

        if (!it->second.PendingWorkUnit) {
            it->second.PendingWorkUnit = std::make_unique<TConveyorWorkUnit>(it->second.QueryState);
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
        if (!IsEnabled() || entry.QueryState || entry.RegistrationPending) {
            return;
        }
        if (auto query = Scheduler->GetQuery(
                entry.Context.DatabaseId, entry.Context.PoolId, entry.Context.QueryId)) {
            entry.QueryState = MakeQueryState(entry.Context, std::move(query));
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

    TWorkloadQueryStatePtr TWorkloadScheduler::MakeQueryState(
        const TWorkloadContext& context, NKqp::NScheduler::NHdrf::NDynamic::TQueryPtr query) {
        const auto poolKey = std::make_pair(context.DatabaseId, context.PoolId);
        auto [poolIt, _] = Pools.try_emplace(poolKey, std::make_shared<TWorkloadPoolState>());
        return std::make_shared<TWorkloadQueryState>(context, std::move(query), poolIt->second);
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
