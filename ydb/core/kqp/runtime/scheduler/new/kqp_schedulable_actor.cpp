#include "kqp_schedulable_actor.h"

#include "tree/dynamic.h"

namespace NKikimr::NKqp::NScheduler {

static constexpr TDuration AverageBatchTime = TDuration::MicroSeconds(100);

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
    Y_ENSURE(Query);
    for (TTreeElementBase* parent = Query.get(); parent; parent = parent->Parent) {
        --parent->Usage;
        parent->BurstUsage += burstUsage.MicroSeconds();
    }
}

// class TSchedulableActorHelper

TSchedulableActorHelper::TSchedulableActorHelper(TOptions&& options)
    : SchedulableTask(std::move(options.SchedulableTask))
    , BatchTime(AverageBatchTime)
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

    TDuration timePassed = TDuration::MicroSeconds(Timer.Passed() * 1'000'000);
    SchedulableTask->Query->DelayedSumBatches += (timePassed - BatchTime).MicroSeconds();
    BatchTime = timePassed;
    SchedulableTask->DecreaseUsage(timePassed);
}

void TSchedulableActorHelper::Throttle() {
    Y_ASSERT(!Throttled);

    Throttled = true;
    SchedulableTask->Query->DelayedSumBatches += BatchTime.MicroSeconds();
    SchedulableTask->Query->DelayedCount++;
    StartThrottle = Now();
}

bool TSchedulableActorHelper::IsThrottled() const {
    return Throttled;
}

TDuration TSchedulableActorHelper::Resume() {
    Y_ASSERT(Throttled);

    Throttled = false;
    SchedulableTask->Query->DelayedSumBatches -= BatchTime.MicroSeconds();
    SchedulableTask->Query->DelayedCount--;
    return Now() - StartThrottle;
}

std::optional<TDuration> TSchedulableActorHelper::CalculateDelay(TMonotonic now) const {
    const auto SmoothPeriod = TDuration::MilliSeconds(100);
    const auto MaxDelay = TDuration::Seconds(10);
    const auto ActivationPenalty = TDuration::MicroSeconds(10);

    const auto query = SchedulableTask->Query;
    const auto snapshot = query->GetSnapshot();
    const auto usage = query->BurstUsage.load();
    const auto share = snapshot->FairShare;
    const auto limit = share * (now - snapshot->Timestamp + SmoothPeriod).MicroSeconds() + snapshot->AdjustedUsage;

    if (limit > usage) {
        return {};
    }

    if (share < 1e-9) {
        return MaxDelay;
    }

    return Min(
        MaxDelay,
        TDuration::MicroSeconds(
            (usage - snapshot->AdjustedUsage + Max<i64>(0, query->DelayedSumBatches) +
             (BatchTime + ActivationPenalty * (query->DelayedCount + 1)).MicroSeconds()
            ) / share
        ) - (now - snapshot->Timestamp)
    );
}

void TSchedulableActorHelper::AccountThrottledTime(TMonotonic) {
    // TODO: no throttled time for now
}

} // namespace NKikimr::NKqp::NScheduler
