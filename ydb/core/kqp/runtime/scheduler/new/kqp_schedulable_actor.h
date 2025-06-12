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

    void IncreaseUsage(const TDuration& burstThrottle);
    void DecreaseUsage(const TDuration& burstUsage);

    NHdrf::NDynamic::TQueryPtr Query;
};

class TSchedulableActorHelper {
public:
    struct TOptions {
        TSchedulableTaskPtr SchedulableTask;
    };

protected:
    explicit TSchedulableActorHelper(TOptions&& options);

    static TMonotonic Now();
    bool IsSchedulable() const;

    void StartExecution(const TDuration& burstThrottle);
    void StopExecution();

    void Throttle();
    bool IsThrottled() const;
    TDuration Resume();

    std::optional<TDuration> CalculateDelay(TMonotonic now) const;

    void AccountThrottledTime(TMonotonic now);

private:
    TSchedulableTaskPtr SchedulableTask;
    THPTimer Timer;
    bool Throttled = false;
    TMonotonic StartThrottle;
    TDuration BatchTime;
};

} // namespace NKikimr::NKqp::NScheduler
