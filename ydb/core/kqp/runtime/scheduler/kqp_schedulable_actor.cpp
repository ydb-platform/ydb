#include "kqp_schedulable_actor.h"

#include "kqp_compute_pool.h"

namespace NKikimr::NKqp::NScheduler {

namespace {

    constexpr TDuration AverageBatch = TDuration::MicroSeconds(100);
    constexpr double MinCapacity = 1e-9;

    constexpr TDuration ToDuration(double t) {
        return TDuration::MicroSeconds(t);
    }

} // namespace

TSchedulerEntity::TSchedulerEntity(TPool* pool)
    : Pool(pool)
    , LastExecutionTime(AverageBatch)
{
    ++Pool->EntitiesCount;
}

TSchedulerEntity::~TSchedulerEntity() {
    --Pool->EntitiesCount;
}

void TSchedulerEntity::TrackTime(TDuration time, TMonotonic) {
    Pool->TrackedMicroSeconds.fetch_add(time.MicroSeconds());
}

void TSchedulerEntity::UpdateLastExecutionTime(TDuration time) {
    Wakeups = 0;
    if (IsThrottled) {
        // TODO: how is it possible to have execution time while being throttled?
        // resume-throttle is for proper update of |DelayedSumBatches|
        MarkResumed();
        LastExecutionTime = time;
        MarkThrottled();
    } else {
        LastExecutionTime = time;
    }
}

TMaybe<TDuration> TSchedulerEntity::Delay(TMonotonic now, TPool* pool) {
    auto current = pool->MutableStats.Current();
    auto limit = current.get()->Limit(now);
    auto tracked = pool->TrackedMicroSeconds.load();
    if (limit > tracked) {
        return {};
    } else {
        if (current.get()->Capacity < MinCapacity) {
            return MaxDelay;
        }
        return Min(MaxDelay, ToDuration((tracked - limit +
                    Max<i64>(0, pool->DelayedSumBatches.load()) + LastExecutionTime.MicroSeconds() +
                    ActivationPenalty.MicroSeconds() * (pool->DelayedCount.load() + 1) +
                    current.get()->MaxLimitDeviation) / current.get()->Capacity));
    }
}

TMaybe<TDuration> TSchedulerEntity::Delay(TMonotonic now) {
    TMaybe<TDuration> result;
    auto poolResult = Delay(now, Pool);
    if (!result) {
        result = poolResult;
    } else if (poolResult && *result < *poolResult) {
        result = poolResult;
    }
    return result;
}

void TSchedulerEntity::MarkThrottled() {
    IsThrottled = true;
    Pool->DelayedSumBatches.fetch_add(LastExecutionTime.MicroSeconds());
    Pool->DelayedCount.fetch_add(1);
}

void TSchedulerEntity::MarkResumed() {
    IsThrottled = false;
    Pool->DelayedSumBatches.fetch_sub(LastExecutionTime.MicroSeconds());
    Pool->DelayedCount.fetch_sub(1);
}

void TSchedulerEntity::MarkResumed(TMonotonic now) {
    MarkResumed();
    if (auto lastNow = Pool->MutableStats.Current().get()->LastNowRecalc; now > lastNow) {
        Pool->ThrottledMicroSeconds.fetch_add((now - lastNow).MicroSeconds());
    }
}

} // namespace NKikimr::NKqp::NScheduler
