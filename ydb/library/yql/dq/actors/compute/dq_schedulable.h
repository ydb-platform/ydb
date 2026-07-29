#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <library/cpp/time_provider/monotonic.h>

#include <util/datetime/base.h>

#include <memory>

namespace NYql::NDq {

///
/// Per work-unit gate + attribution. 
///
struct IDqSchedulableWork {
    virtual ~IDqSchedulableWork() = default;

    // Gate before a unit of work. Returns false when quota is exhausted.
    virtual bool StartExecution(TMonotonic now) = 0;

    // Called after the unit finishes. `forcedResume` is passed through to
    // signal whether the actor was woken up by resume rather than natural
    // completion.
    virtual void StopExecution(bool& forcedResume) = 0;

    // Estimated delay until quota is likely available again. Use after a
    // failed StartExecution, e.g. WaitForEvent(now + CalculateDelay(now)).
    virtual TDuration CalculateDelay(TMonotonic now) const = 0;

    // Subscribe on wake-up when quota frees up. The actor will receive
    // TEvWakeup from the scheduler.
    virtual void RegisterForResume(const NActors::TActorId& actorId) = 0;

    virtual void RecordUsage(TDuration elapsed) = 0;
};

///
/// Factory carried through TSourceArguments. Implementation lives in kqp
/// scheduler; sources create per-actor/coroutine schedulable-work wrappers
/// from it without pulling in kqp types.
///
struct IDqSchedulerContext {
    virtual ~IDqSchedulerContext() = default;

    virtual std::shared_ptr<IDqSchedulableWork> CreateSchedulableWork() = 0;
};

using IDqSchedulerContextPtr = std::shared_ptr<IDqSchedulerContext>;

} // namespace NYql::NDq
