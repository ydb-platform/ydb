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

// TODO: referring to the pool's fair-share and usage - query's fair-share is ignored.
bool TSchedulableTask::TryIncreaseUsage() {
    const auto snapshot = Query->GetSnapshot();
    auto pool = Query->Parent;
    ui64 newUsage = pool->Usage.load();
    bool increased = false;

    while (!increased && newUsage < snapshot->Parent->FairShare) {
        increased = pool->Usage.compare_exchange_weak(newUsage, newUsage + 1);
    }

    if (!increased) {
        return false;
    }

    ++Query->Usage;
    for (TTreeElementBase* parent = pool->Parent; parent; parent = parent->Parent) {
        ++parent->Usage;
    }

    return true;
}

// TODO: referring to the pool's fair-share and usage - query's fair-share is ignored.
void TSchedulableTask::IncreaseUsage() {
    for (TTreeElementBase* parent = Query.get(); parent; parent = parent->Parent) {
        ++parent->Usage;
    }
}

// TODO: referring to the pool's fair-share and usage - query's fair-share is ignored.
void TSchedulableTask::DecreaseUsage(const TDuration& burstUsage) {
    for (TTreeElementBase* parent = Query.get(); parent; parent = parent->Parent) {
        --parent->Usage;
        parent->BurstUsage += burstUsage.MicroSeconds();
    }
}

void TSchedulableTask::IncreaseExtraUsage() {
    for (TTreeElementBase* parent = Query.get(); parent; parent = parent->Parent) {
        ++parent->UsageExtra;
    }
}

void TSchedulableTask::DecreaseExtraUsage(const TDuration& burstUsageExtra) {
    for (TTreeElementBase* parent = Query.get(); parent; parent = parent->Parent) {
        --parent->UsageExtra;
        parent->BurstUsageExtra += burstUsageExtra.MicroSeconds();
    }
}

void TSchedulableTask::IncreaseBurstThrottle(const TDuration& burstThrottle) {
    for (TTreeElementBase* parent = Query.get(); parent; parent = parent->Parent) {
        parent->BurstThrottle += burstThrottle.MicroSeconds();
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

bool TSchedulableActorHelper::StartExecution(TMonotonic now) {
    Y_ASSERT(!Executed);

    Y_DEFER {
        if (Throttled) {
            SchedulableTask->IncreaseBurstThrottle(now - StartThrottle);
        }

        Timer.Reset();
        if (Executed) {
            if (Throttled) {
                Resume();
            }

            SchedulableTask->Query->CurrentTasksTime += LastExecutionTime.MicroSeconds();
        } else {
            if (!Throttled) {
                Throttled = true;
                SchedulableTask->Query->WaitingTasksTime += LastExecutionTime.MicroSeconds();
                SchedulableTask->IncreaseThrottle();
            }
            StartThrottle = now;
            ++ExecuteAttempts;
        }
    };

    if (IsSchedulable() && SchedulableTask->Query->GetSnapshot()) {
        // TODO: check heuristics if we should execute this task.
        Executed = SchedulableTask->TryIncreaseUsage();
        return Executed;
    }

    SchedulableTask->IncreaseUsage();
    Executed = true;
    return Executed;
}

void TSchedulableActorHelper::StopExecution() {
    if (Executed) {
        Y_ASSERT(!Throttled);

        TDuration timePassed = TDuration::MicroSeconds(Timer.Passed() * 1'000'000);
        SchedulableTask->Query->CurrentTasksTime -= LastExecutionTime.MicroSeconds();
        LastExecutionTime = timePassed;
        SchedulableTask->DecreaseUsage(timePassed);
        Executed = false;
    } else if (Throttled) {
        Resume();
    }
}

TDuration TSchedulableActorHelper::CalculateDelay(TMonotonic) const {
    const auto MaxDelay = TDuration::Seconds(3);
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

    auto delayDuration = Min(MaxDelay, Max(MinDelay, TDuration::MicroSeconds(Max<i64>(0, delay))));
    if (query->Delay) {
        query->Delay->Collect(delayDuration.MicroSeconds());
    }
    return delayDuration;
}

void TSchedulableActorHelper::Resume() {
    Y_ASSERT(Throttled);

    Throttled = false;
    SchedulableTask->Query->WaitingTasksTime -= LastExecutionTime.MicroSeconds();
    SchedulableTask->DecreaseThrottle();
    ExecuteAttempts = 0;
}

} // namespace NKikimr::NKqp::NScheduler
