#pragma once

#include "fwd.h"

#include <library/cpp/time_provider/monotonic.h>

#include <util/system/hp_timer.h>

namespace NActors {
    struct TActorId;
}

namespace NKikimr::NKqp::NScheduler {

class TSchedulableActorBase {
public:
    struct TOptions {
        NHdrf::NDynamic::TQueryPtr Query;
        bool IsSchedulable;
    };

protected:
    explicit TSchedulableActorBase(const TOptions& options);

    static inline TMonotonic Now() {
        return TMonotonic::Now();
    }

    void RegisterForResume(const NActors::TActorId& actorId);

    inline bool IsAccountable() const {
        return !!SchedulableTask;
    }
    inline bool IsThrottled() const {
        return Throttled;
    }

    bool StartExecution(TMonotonic now);
    void StopExecution(bool& forcedResume);

    TDuration CalculateDelay(TMonotonic now) const;

private:
    void Resume();

    TSchedulableTaskPtr SchedulableTask;
    const bool IsSchedulable;

    THPTimer Timer;
    bool Executed = false;
    bool Throttled = false;
    TMonotonic StartThrottle;

    TDuration LastExecutionTime;
    ui64 ExecuteAttempts = 0;
};

using TSchedulableActorOptions = TSchedulableActorBase::TOptions;

} // namespace NKikimr::NKqp::NScheduler
