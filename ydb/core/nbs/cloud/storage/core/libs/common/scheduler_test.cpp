#include "scheduler_test.h"

#include <algorithm>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

void TTestScheduler::Schedule(
    ITaskQueue* taskQueue,
    TInstant deadline,
    TCallback callback)
{
    Y_UNUSED(taskQueue);

    with_lock (CallbacksLock) {
        Callbacks.push_back({callback, deadline});
        if (GotNewCallback) {
            GotNewCallback->SetValue();
        }
        GotNewCallback = std::nullopt;
    }
}

void TTestScheduler::RunAllScheduledTasks()
{
    RunAllScheduledTasksUntilDeadline(TInstant::Max());
}

void TTestScheduler::RunAllScheduledTasksUntilNow()
{
    RunAllScheduledTasksUntilDeadline(Now);
}

void TTestScheduler::RunAllScheduledTasksUntilDeadline(TInstant deadline)
{
    TVector<TScheduledCallback> callbacks;
    with_lock (CallbacksLock) {
        auto removed = std::ranges::remove_if(
            Callbacks,
            [&](auto& callbackInfo)
            {
                if (callbackInfo.Deadline <= deadline) {
                    callbacks.emplace_back(std::move(callbackInfo));
                    return true;
                }
                return false;
            });
        Callbacks.erase(removed.begin(), removed.end());
    }

    for (auto& [callback, _]: callbacks) {
        callback();
    }
}

NThreading::TFuture<void> TTestScheduler::WaitForTaskSchedule()
{
    with_lock (CallbacksLock) {
        if (!GotNewCallback) {
            GotNewCallback = NThreading::NewPromise();
        }
        return GotNewCallback->GetFuture();
    }
}

void TTestScheduler::AdvanceTime(TDuration duration)
{
    TGuard guard(CallbacksLock);
    Now += duration;
}

}   // namespace NYdb::NBS
