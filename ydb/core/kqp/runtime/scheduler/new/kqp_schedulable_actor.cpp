#include "kqp_schedulable_actor.h"

#include "tree/dynamic.h"

namespace NKikimr::NKqp::NScheduler {

static constexpr TDuration AverageExecutionTime = TDuration::MicroSeconds(100);

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
    , LastExecutionTime(AverageExecutionTime)
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
        SchedulableTask->Query->CurrentTasksTime += LastExecutionTime.MicroSeconds();
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
    SchedulableTask->Query->CurrentTasksTime -= LastExecutionTime.MicroSeconds();
    LastExecutionTime = timePassed;
    SchedulableTask->DecreaseUsage(timePassed);
    Executed = false;
}

void TSchedulableActorHelper::Throttle(TMonotonic now) {
    Y_ASSERT(!Executed);
    Y_ASSERT(!Throttled);

    Throttled = true;
    SchedulableTask->Query->WaitingTasksTime += LastExecutionTime.MicroSeconds();
    SchedulableTask->IncreaseThrottle();
    StartThrottle = now;
    ++ExecuteAttempts;
}

bool TSchedulableActorHelper::IsThrottled() const {
    return Throttled;
}

TDuration TSchedulableActorHelper::Resume(TMonotonic now) {
    Y_ASSERT(!Executed);
    Y_ASSERT(Throttled);

    Throttled = false;
    SchedulableTask->Query->WaitingTasksTime -= LastExecutionTime.MicroSeconds();
    SchedulableTask->DecreaseThrottle();
    ExecuteAttempts = 0;

    return now - StartThrottle;
}

TDuration TSchedulableActorHelper::CalculateDelay(TMonotonic) const {
    const auto MaxDelay = TDuration::Seconds(5);
    const auto MinDelay = TDuration::MicroSeconds(10);
    const auto AttemptBonus = TDuration::MicroSeconds(5);

    const auto query = SchedulableTask->Query;
    const auto snapshot = query->GetSnapshot();

    const auto share = snapshot->FairShare;

    if (share < 1e-9) {
        return MaxDelay;
    }

    i64 delay =
        + (query->CurrentTasksTime / share)               // current tasks to complete
        + (query->WaitingTasksTime / share)               // waiting tasks to complete
        // TODO: (currentUsage - averageUsage) * penalty  // penalty for usage since last snapshot
        - (ExecuteAttempts * AttemptBonus.MicroSeconds()) // bonus for number of attempts
        + (RandomNumber<ui64>() % 100)                    // random delay
    ;

    return Min(MaxDelay, Max(MinDelay, TDuration::MicroSeconds(Max<i64>(0, delay))));
}

} // namespace NKikimr::NKqp::NScheduler
