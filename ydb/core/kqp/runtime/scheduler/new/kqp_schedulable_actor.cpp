#include "kqp_schedulable_actor.h"

#include "tree/dynamic.h"

namespace NKikimr::NKqp::NScheduler {

using namespace NHdrf::NDynamic;

// class TSchedulableTask

TSchedulableTask::TSchedulableTask(const TQueryPtr& query)
    : Query(query)
{
    Y_ENSURE(query);
    ++Query->Demand;
}

TSchedulableTask::~TSchedulableTask() {
    --Query->Demand;
}

void TSchedulableTask::IncreaseUsage(const TDuration& burstThrottle) {
    Y_ENSURE(Query);
    for (TTreeElementBase* parent = Query.get(); parent; parent = parent->Parent) {
        ++parent->Usage;
        parent->BurstThrottle += burstThrottle.MicroSeconds();
    }
}

void TSchedulableTask::DecreaseUsage(const TDuration& burstUsage) {
    for (TTreeElementBase* parent = Query.get(); parent; parent = parent->Parent) {
        --parent->Usage;
        parent->BurstUsage += burstUsage.MicroSeconds();
    }
}

// class TSchedulableActorHelper

TSchedulableActorHelper::TSchedulableActorHelper(TOptions&& options)
    : SchedulableTask(std::move(options.SchedulableTask))
{
}

// static
TMonotonic TSchedulableActorHelper::Now() {
    return TMonotonic::Now();
}

bool TSchedulableActorHelper::IsSchedulable() const {
    return !!SchedulableTask;
}

void TSchedulableActorHelper::StartExecution(const TDuration& burstThrottle) {
    Y_ASSERT(SchedulableTask);

    Timer.Reset();
    SchedulableTask->IncreaseUsage(burstThrottle);
}

void TSchedulableActorHelper::StopExecution() {
    Y_ASSERT(SchedulableTask);

    SchedulableTask->DecreaseUsage(TDuration::MicroSeconds(Timer.Passed() * 1'000'000));
}

std::optional<TDuration> TSchedulableActorHelper::CalculateDelay(TMonotonic now) const {
    // const auto usage = SchedulableTask->Query->BurstUsage.load();
    // const auto limit = SchedulableTask->Query->FairShare * (now - LastNowRecalc + SMOOTH_PERIOD) + TrackedBefore;

    // if (current_limit > usage) {
    //     return {};
    // }

    // if (current->FairShare < MinCapacity) {
    //     return MaxDelay;
    // }

    // return Min(
    //     MaxDelay,
    //     (usage + current->TrackedBefore +
    //      Max<i64>(0, pool->DelayedSumBatches) +
    //      BatchTime + ActivationPenalty * (pool->DelayedCount + 1)
    //     ) / current->FairShare - (now - current->LastNowRecalc)
    // );
    Y_UNUSED(now);
    return {};
}

void TSchedulableActorHelper::AccountThrottledTime(TMonotonic) {
    // TODO: no throttled time for now
}

} // namespace NKikimr::NKqp::NScheduler
