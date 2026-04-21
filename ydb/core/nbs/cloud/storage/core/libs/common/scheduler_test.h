#pragma once

#include "public.h"

#include "library/cpp/threading/future/core/future.h"
#include "scheduler.h"

#include <util/generic/vector.h>
#include <util/system/mutex.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

class TTestScheduler final: public IScheduler
{
    struct TScheduledCallback
    {
        TCallback Callback;
        TInstant Deadline;
    };

private:
    TMutex CallbacksLock;
    TVector<TScheduledCallback> Callbacks;
    std::optional<NThreading::TPromise<void>> GotNewCallback;
    TInstant Now;

public:
    explicit TTestScheduler(TInstant now = TInstant::Max())
        : Now(now)
    {}

    void Start() override
    {}

    void Stop() override
    {}

    void Schedule(
        ITaskQueue* taskQueue,
        TInstant deadline,
        TCallback callback) override;

    void RunAllScheduledTasks();

    void RunAllScheduledTasksUntilNow();

    NThreading::TFuture<void> WaitForTaskSchedule();

    void AdvanceTime(TDuration duration);

private:
    void RunAllScheduledTasksUntilDeadline(TInstant deadline);
};

}   // namespace NYdb::NBS
