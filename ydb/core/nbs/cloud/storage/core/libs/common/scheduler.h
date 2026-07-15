#pragma once

#include "public.h"

#include "startable.h"

#include <util/datetime/base.h>

#include <functional>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

using TCallback = std::function<void()>;

////////////////////////////////////////////////////////////////////////////////

struct IScheduler: public IStartable
{
    virtual ~IScheduler() = default;

    virtual void
    Schedule(ITaskQueue* taskQueue, TInstant deadline, TCallback callback) = 0;

    template <typename T>
    void Schedule(TInstant deadLine, T callback)
    {
        Schedule(nullptr, deadLine, std::forward<T>(callback));
    }
};

////////////////////////////////////////////////////////////////////////////////

ISchedulerPtr CreateScheduler();
ISchedulerPtr CreateScheduler(ITimerPtr timer);
ISchedulerPtr CreateSchedulerStub();

ISchedulerPtr CreateBackgroundScheduler(
    ISchedulerPtr scheduler,
    ITaskQueuePtr taskQueue);

}   // namespace NYdb::NBS
