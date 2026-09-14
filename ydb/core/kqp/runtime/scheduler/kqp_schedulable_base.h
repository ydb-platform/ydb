#pragma once

#include "fwd.h"

#include <library/cpp/time_provider/monotonic.h>

#include <util/generic/yexception.h>
#include <util/system/hp_timer.h>

#include <optional>

namespace NActors {
    struct TActorId;
}

namespace NKikimr::NKqp::NScheduler {

class TSchedulableBase {
public:
    struct TOptions {
        NHdrf::NDynamic::TQueryPtr Query;
        bool IsSchedulable;
    };

    explicit TSchedulableBase(const TOptions& options);
    ~TSchedulableBase();

    // TODO: hand out an RAII guard instead of a bare pair,
    //       so that skipping the release becomes impossible to express.
    std::optional<TDuration> TryStartExecution(TMonotonic now);
    void StopExecution();
    void NotifyResumed(bool byScheduler);
    void RegisterForResume(const NActors::TActorId& actorId);

    const NHdrf::TFullPoolId& GetFullPoolId() const {
        Y_ENSURE(IsAccountable());
        return FullPoolId;
    }

    bool IsAccountable() const {
        return !!SchedulableTask;
    }

    bool IsThrottled() const {
        return Throttled;
    }

    bool IsExecuting() const {
        return Executed;
    }

private:
    bool StartExecution(TMonotonic now);
    TDuration CalculateDelay(TMonotonic now) const;
    void Resume();

    TSchedulableTaskPtr SchedulableTask;
    const NHdrf::TFullPoolId FullPoolId;
    const bool IsSchedulable;

    THPTimer Timer;
    bool Executed = false;
    bool Throttled = false;
    bool ForcedResume = false;
    TMonotonic StartThrottle;

    TDuration LastExecutionTime;
    ui64 ExecuteAttempts = 0;
};

using TSchedulableOptions = TSchedulableBase::TOptions;

} // namespace NKikimr::NKqp::NScheduler
