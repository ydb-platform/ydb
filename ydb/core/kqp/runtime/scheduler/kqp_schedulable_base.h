#pragma once

#include "fwd.h"

#include <ydb/library/yql/dq/actors/compute/dq_schedulable.h>

#include <library/cpp/time_provider/monotonic.h>

#include <util/system/hp_timer.h>

namespace NActors {
    struct TActorId;
}

namespace NKikimr::NKqp::NScheduler {

class TSchedulableBase : public NYql::NDq::IDqSchedulableWork {
public:
    struct TOptions {
        NHdrf::NDynamic::TQueryPtr Query;
        bool IsSchedulable;
    };

    explicit TSchedulableBase(const TOptions& options);

protected:
    // Public via IDqSchedulableWork vtable, protected for direct-inheritance users
    // (CA base), matching the pre-merge visibility.
    bool StartExecution(TMonotonic now) override;
    void StopExecution(bool& forcedResume) override;
    TDuration CalculateDelay(TMonotonic now) const override;
    void RegisterForResume(const NActors::TActorId& actorId) override;
    NYql::NDq::TPoolKey GetPoolKey() const override { return Key; }

    static inline TMonotonic Now() {
        return TMonotonic::Now();
    }

    inline bool IsAccountable() const {
        return !!SchedulableTask;
    }
    inline bool IsThrottled() const {
        return Throttled;
    }

private:
    void Resume();

    TSchedulableTaskPtr SchedulableTask;
    const NYql::NDq::TPoolKey Key;
    const bool IsSchedulable;

    THPTimer Timer;
    bool Executed = false;
    bool Throttled = false;
    TMonotonic StartThrottle;

    TDuration LastExecutionTime;
    ui64 ExecuteAttempts = 0;
};

using TSchedulableOptions = TSchedulableBase::TOptions;

} // namespace NKikimr::NKqp::NScheduler
