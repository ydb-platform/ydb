#include "rain_check.h"

#include <util/system/yassert.h>

using namespace NRainCheck;
using namespace NRainCheck::NPrivate;
using namespace NBus;
using namespace NBus::NPrivate;

TSleepService::TSleepService(::NBus::NPrivate::TScheduler* scheduler)
    : Scheduler(scheduler)
{
}

NRainCheck::TSleepService::TSleepService()
    : SchedulerHolder(new TScheduler)
    , Scheduler(SchedulerHolder.Get())
{
}

NRainCheck::TSleepService::~TSleepService() {
    if (!!SchedulerHolder) {
        Scheduler->Stop();
    }
}

namespace {
    struct TSleepServiceScheduleItem: public IScheduleItem {
        ISubtaskListener* const Parent;

        TSleepServiceScheduleItem(ISubtaskListener* parent, TInstant time)
            : IScheduleItem(time)
            , Parent(parent)
        {
        }

        void Do() override {
            Parent->SetDone();
        }
    };
}

void TSleepService::Sleep(TSubtaskCompletion* r, TDuration duration) {
    TTaskRunnerBase* current = TTaskRunnerBase::CurrentTask();
    r->SetRunning(current);
    Scheduler->Schedule(new TSleepServiceScheduleItem(r, TInstant::Now() + duration));
}
