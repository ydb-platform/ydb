#include "kqp_schedulable_base.h"

#include "kqp_schedulable_task.h"

#include <ydb/core/kqp/runtime/scheduler/tree/dynamic.h>

#include <util/generic/scope.h>

namespace NKikimr::NKqp::NScheduler {

static constexpr TDuration AverageExecutionTime = TDuration::MicroSeconds(100); // TODO: make configurable from outside

using namespace NHdrf::NDynamic;

namespace {
    NYql::NDq::TWorkScope ExtractScope(const NHdrf::NDynamic::TQueryPtr& query) {
        NYql::NDq::TWorkScope scope;
        if (!query) {
            return scope;
        }
        if (auto* pool = query->GetParent()) {
            scope.Name = std::get<NHdrf::TPoolId>(pool->GetId());
            if (auto* database = pool->GetParent()) {
                scope.Namespace = std::get<NHdrf::TDatabaseId>(database->GetId());
            }
        }
        return scope;
    }
} // namespace

TSchedulableBase::TSchedulableBase(const TOptions& options)
    : Scope(ExtractScope(options.Query))
    , IsSchedulable(options.IsSchedulable)
    , LastExecutionTime(AverageExecutionTime)
{
    if (options.Query) {
        SchedulableTask = std::make_shared<TSchedulableTask>(options.Query);
    }

    Y_ENSURE(!IsSchedulable || IsAccountable());
}

void TSchedulableBase::RegisterForResume(const NActors::TActorId& actorId) {
    Y_ASSERT(SchedulableTask);

    if (IsSchedulable) {
        SchedulableTask->RegisterForResume(actorId);
    }
}

std::optional<TDuration> TSchedulableBase::TryStartExecution(TMonotonic now) {
    if (StartExecution(now)) {
        return std::nullopt;
    }
    return CalculateDelay(now);
}

bool TSchedulableBase::StartExecution(TMonotonic now) {
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

    if (IsSchedulable) {
        // TODO: check heuristics if we should execute this task.
        Executed = SchedulableTask->TryIncreaseUsage();
        return Executed;
    }

    SchedulableTask->IncreaseUsage();
    Executed = true;
    return Executed;
}

void TSchedulableBase::StopExecution(bool& forcedResume) {
    Y_ASSERT(SchedulableTask);

    if (Executed) {
        Y_ASSERT(!Throttled);

        TDuration timePassed = TDuration::MicroSeconds(Timer.Passed() * 1'000'000);
        SchedulableTask->Query->CurrentTasksTime -= LastExecutionTime.MicroSeconds();
        LastExecutionTime = timePassed;
        SchedulableTask->DecreaseUsage(timePassed, forcedResume ? TSchedulableTask::CPU_RESUMED : TSchedulableTask::CPU_DEFAULT);
        forcedResume = false;
        Executed = false;

        if (auto spareUsage = SchedulableTask->GetSpareUsage()) {
            SchedulableTask->Query->ResumeTasks(spareUsage);
        }
        // TODO: resume tasks for all queries from parent leaf pool
    } else if (Throttled) {
        Resume();
    }
}

TDuration TSchedulableBase::CalculateDelay(TMonotonic) const {
    Y_ASSERT(SchedulableTask);

    const auto query = SchedulableTask->Query;
    const auto snapshot = query->GetSnapshot();
    const auto share = snapshot ? snapshot->FairShare : 1; // TODO: check if each query is allowed minimum fair-share?

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

void TSchedulableBase::Resume() {
    Y_ASSERT(Throttled);

    Throttled = false;
    SchedulableTask->Query->WaitingTasksTime -= LastExecutionTime.MicroSeconds();
    SchedulableTask->DecreaseThrottle();
    ExecuteAttempts = 0;
}

} // namespace NKikimr::NKqp::NScheduler
