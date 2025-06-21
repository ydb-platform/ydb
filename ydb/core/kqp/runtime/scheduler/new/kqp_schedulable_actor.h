#pragma once

#include "fwd.h"

#include <library/cpp/time_provider/monotonic.h>

#include <util/datetime/base.h>
#include <util/system/hp_timer.h>

namespace NKikimr::NKqp::NScheduler {

// The proxy-object between any schedulable actor and the scheduler itself
struct TSchedulableTask {
    explicit TSchedulableTask(const NHdrf::NDynamic::TQueryPtr& query);
    ~TSchedulableTask();

    bool TryIncreaseUsage(const TDuration& burstThrottle);
    void IncreaseUsage(const TDuration& burstThrottle);
    void DecreaseUsage(const TDuration& burstUsage);

    void IncreaseThrottle();
    void DecreaseThrottle();

    NHdrf::NDynamic::TQueryPtr Query;
};

class TSchedulableActorHelper {
public:
    struct TOptions {
        TSchedulableTaskPtr SchedulableTask;
        bool IsSchedulable;
    };

protected:
    explicit TSchedulableActorHelper(TOptions&& options);

    static TMonotonic Now();
    bool IsSchedulable() const;

    bool StartExecution(const TDuration& burstThrottle);
    bool IsExecuted() const;
    void StopExecution();

    void Throttle(TMonotonic now);
    bool IsThrottled() const;
    TDuration Resume(TMonotonic now);

    TDuration CalculateDelay(TMonotonic now) const;

private:
    TSchedulableTaskPtr SchedulableTask;
    const bool Schedulable;
    THPTimer Timer;
    bool Executed = false;
    bool Throttled = false;
    TMonotonic StartThrottle;

    TDuration LastExecutionTime;
    ui64 ExecuteAttempts = 0;
};

} // namespace NKikimr::NKqp::NScheduler
