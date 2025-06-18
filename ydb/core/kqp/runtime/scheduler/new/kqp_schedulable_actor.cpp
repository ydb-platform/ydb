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

bool TSchedulableTask::TryIncreaseUsage(const TDuration& burstThrottle) {
    const auto snapshot = Query->GetSnapshot();
    ui64 newUsage = Query->Usage.load();
    bool increased = false;

    while (!increased && newUsage < snapshot->Parent->Demand) {
        increased = Query->Usage.compare_exchange_weak(newUsage, newUsage + 1);
    }

    if (!increased) {
        return false;
    }

    Query->BurstThrottle += burstThrottle.MicroSeconds();
    for (TTreeElementBase* parent = Query->Parent; parent; parent = parent->Parent) {
        ++parent->Usage;
        parent->BurstThrottle += burstThrottle.MicroSeconds();
    }

    return true;
}

void TSchedulableTask::IncreaseUsage(const TDuration& burstThrottle) {
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

void TSchedulableTask::IncreaseThrottle() {
    for (TTreeElementBase* parent = Query.get(); parent; parent = parent->Parent) {
        ++parent->Throttle;
    }
}

void TSchedulableTask::DecreaseThrottle() {
    for (TTreeElementBase* parent = Query.get(); parent; parent = parent->Parent) {
        --parent->Throttle;
    }
}

// class TSchedulableActorHelper

TSchedulableActorHelper::TSchedulableActorHelper(TOptions&& options)
    : SchedulableTask(std::move(options.SchedulableTask))
    , Schedulable(options.IsSchedulable)
    , PoolId(options.PoolId)
    , BatchTime(AverageBatchTime)
{
    Y_ENSURE(SchedulableTask);
}

// static
TMonotonic TSchedulableActorHelper::Now() {
    return TMonotonic::Now();
}

bool TSchedulableActorHelper::IsSchedulable() const {
    return Schedulable;
}

bool TSchedulableActorHelper::StartExecution(const TDuration& burstThrottle) {
    Y_ASSERT(!Executed);
    Y_ASSERT(!Throttled);

    Y_DEFER {
        Timer.Reset();
    };

    if (IsSchedulable() && SchedulableTask->Query->GetSnapshot()) {
        // TODO: check heuristics if we should execute this task.
        return Executed = SchedulableTask->TryIncreaseUsage(burstThrottle);
    }

    SchedulableTask->IncreaseUsage(burstThrottle);
    return Executed = true;
}

bool TSchedulableActorHelper::IsExecuted() const {
    return Executed;
}

void TSchedulableActorHelper::StopExecution() {
    Y_ASSERT(Executed);
    Y_ASSERT(!Throttled);

    TDuration timePassed = TDuration::MicroSeconds(Timer.Passed() * 1'000'000);
    SchedulableTask->Query->DelayedSumBatches += (timePassed - BatchTime).MicroSeconds();
    BatchTime = timePassed;
    SchedulableTask->DecreaseUsage(timePassed);
    Executed = false;
}

void TSchedulableActorHelper::Throttle(TMonotonic now) {
    Y_ASSERT(!Executed);
    Y_ASSERT(!Throttled);

    Throttled = true;
    SchedulableTask->Query->DelayedSumBatches += BatchTime.MicroSeconds();
    SchedulableTask->Query->DelayedCount++;
    StartThrottle = now;
}

bool TSchedulableActorHelper::IsThrottled() const {
    return Throttled;
}

TDuration TSchedulableActorHelper::Resume(TMonotonic now) {
    Y_ASSERT(!Executed);
    Y_ASSERT(Throttled);

    Throttled = false;
    SchedulableTask->Query->DelayedSumBatches -= BatchTime.MicroSeconds();
    SchedulableTask->Query->DelayedCount--;
    return now - StartThrottle;
}

TDuration TSchedulableActorHelper::CalculateDelay(TMonotonic now) const {
    const auto MaxDelay = TDuration::Seconds(10);
    const auto ActivationPenalty = TDuration::MicroSeconds(10);

    const auto query = SchedulableTask->Query;
    const auto snapshot = query->GetSnapshot();

    const auto usage = query->BurstUsage.load();
    const auto share = snapshot->FairShare;

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

} // namespace NKikimr::NKqp::NScheduler
