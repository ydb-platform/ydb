#pragma once

#include "fwd.h"

#include <library/cpp/messagebus/scheduler/scheduler.h>

#include <util/datetime/base.h>

namespace NRainCheck {
    class TSleepService {
    private:
        THolder< ::NBus::NPrivate::TScheduler> SchedulerHolder;
        ::NBus::NPrivate::TScheduler* const Scheduler;

    public:
        TSleepService(::NBus::NPrivate::TScheduler*);
        TSleepService();
        ~TSleepService();

        // Wake up a task after given duration.
        void Sleep(TSubtaskCompletion* r, TDuration);
    };

}
