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

    bool TryIncreaseUsage();
    void IncreaseUsage();
    void DecreaseUsage(const TDuration& burstUsage);

    // Account extra usage which doesn't affect scheduling
    void IncreaseExtraUsage();
    void DecreaseExtraUsage(const TDuration& burstUsage);

    void IncreaseBurstThrottle(const TDuration& burstThrottle);
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

    bool StartExecution(TMonotonic now);
    void StopExecution();

    TDuration CalculateDelay(TMonotonic now) const;

private:
    void Resume();

    TSchedulableTaskPtr SchedulableTask;
    const bool Schedulable;
    THPTimer Timer;
    bool Executed = false;
    bool Throttled = false;
    TMonotonic StartThrottle;

    TDuration LastExecutionTime;
    ui64 ExecuteAttempts = 0;
};

using TSchedulableActorOptions = TSchedulableActorHelper::TOptions;

} // namespace NKikimr::NKqp::NScheduler
