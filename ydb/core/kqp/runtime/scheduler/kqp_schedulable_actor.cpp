#include "kqp_schedulable_actor.h"

#include "kqp_schedulable_task.h"

#include <ydb/core/kqp/runtime/scheduler/tree/dynamic.h>

#include <util/generic/scope.h>

namespace NKikimr::NKqp::NScheduler {

static constexpr TDuration AverageExecutionTime = TDuration::MicroSeconds(100); // TODO: make configurable from outside

using namespace NHdrf::NDynamic;

TSchedulableActorBase::TSchedulableActorBase(const TOptions& options)
    : IsSchedulable(options.IsSchedulable)
    , LastExecutionTime(AverageExecutionTime)
{
    if (options.Query) {
        SchedulableTask = std::make_shared<TSchedulableTask>(options.Query);
    }

    Y_ENSURE(!IsSchedulable || IsAccountable());
}

void TSchedulableActorBase::RegisterForResume(const NActors::TActorId& actorId) {
    Y_ASSERT(SchedulableTask);

    if (IsSchedulable) {
        SchedulableTask->RegisterForResume(actorId);
    }
}

bool TSchedulableActorBase::StartExecution(TMonotonic now) {
    Y_ASSERT(SchedulableTask);
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

    if (IsSchedulable && SchedulableTask->Query->GetSnapshot()) {
        // TODO: check heuristics if we should execute this task.
        Executed = SchedulableTask->TryIncreaseUsage();
        return Executed;
    }

    SchedulableTask->IncreaseUsage();
    Executed = true;
    return Executed;
}

void TSchedulableActorBase::StopExecution(bool& forcedResume) {
    Y_ASSERT(SchedulableTask);

    if (Executed) {
        Y_ASSERT(!Throttled);

        TDuration timePassed = TDuration::MicroSeconds(Timer.Passed() * 1'000'000);
        SchedulableTask->Query->CurrentTasksTime -= LastExecutionTime.MicroSeconds();
        LastExecutionTime = timePassed;
        SchedulableTask->DecreaseUsage(timePassed, forcedResume);
        forcedResume = false;
        Executed = false;

        SchedulableTask->Query->ResumeTasks(SchedulableTask->GetSpareUsage());
        // TODO: resume tasks for all queries from parent leaf pool
    } else if (Throttled) {
        Resume();
    }
}

TDuration TSchedulableActorBase::CalculateDelay(TMonotonic) const {
    Y_ASSERT(SchedulableTask);

    const auto query = SchedulableTask->Query;
    const auto snapshot = query->GetSnapshot();

    const auto share = snapshot->FairShare;

    if (share < 1e-9) {
        return query->DelayParams->MaxDelay;
    }

    i64 delay =
        + (query->CurrentTasksTime / share)                                          // current tasks to complete
        + (query->WaitingTasksTime / share)                                          // waiting tasks to complete
        // TODO: (currentUsage - averageUsage) * penalty                             // penalty for usage since last snapshot
        - (ExecuteAttempts * query->DelayParams->AttemptBonus.MicroSeconds())        // bonus for number of attempts
        + (RandomNumber<ui64>() % query->DelayParams->MaxRandomDelay.MicroSeconds()) // random delay
    ;

    auto delayDuration = Min(query->DelayParams->MaxDelay, Max(query->DelayParams->MinDelay, TDuration::MicroSeconds(Max<i64>(0, delay))));
    if (query->Delay) {
        query->Delay->Collect(delayDuration.MicroSeconds());
    }
    return delayDuration;
}

void TSchedulableActorBase::Resume() {
    Y_ASSERT(Throttled);

    Throttled = false;
    SchedulableTask->Query->WaitingTasksTime -= LastExecutionTime.MicroSeconds();
    SchedulableTask->DecreaseThrottle();
    ExecuteAttempts = 0;
}

} // namespace NKikimr::NKqp::NScheduler
