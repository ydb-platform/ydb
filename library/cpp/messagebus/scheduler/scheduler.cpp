#include "scheduler.h"

#include <util/datetime/base.h>
#include <util/generic/algorithm.h>
#include <util/generic/yexception.h>

//#include "dummy_debugger.h"

using namespace NBus;
using namespace NBus::NPrivate;

class TScheduleDeadlineCompare {
public:
    bool operator()(const IScheduleItemAutoPtr& i1, const IScheduleItemAutoPtr& i2) const noexcept {
        return i1->GetScheduleTime() > i2->GetScheduleTime();
    }
};

TScheduler::TScheduler()
    : StopThread(false)
    , Thread([&] { this->SchedulerThread(); })
{
}

TScheduler::~TScheduler() {
    Y_ABORT_UNLESS(StopThread, "state check");
}

size_t TScheduler::Size() const {
    TGuard<TLock> guard(Lock);
    return Items.size() + (!!NextItem ? 1 : 0);
}

void TScheduler::Stop() {
    {
        TGuard<TLock> guard(Lock);
        Y_ABORT_UNLESS(!StopThread, "Scheduler already stopped");
        StopThread = true;
        CondVar.Signal();
    }
    Thread.Get();

    if (!!NextItem) {
        NextItem.Destroy();
    }

    for (auto& item : Items) {
        item.Destroy();
    }
}

void TScheduler::Schedule(TAutoPtr<IScheduleItem> i) {
    TGuard<TLock> lock(Lock);
    if (StopThread)
        return;

    if (!!NextItem) {
        if (i->GetScheduleTime() < NextItem->GetScheduleTime()) {
            DoSwap(i, NextItem);
        }
    }

    Items.push_back(i);
    PushHeap(Items.begin(), Items.end(), TScheduleDeadlineCompare());

    FillNextItem();

    CondVar.Signal();
}

void TScheduler::FillNextItem() {
    if (!NextItem && !Items.empty()) {
        PopHeap(Items.begin(), Items.end(), TScheduleDeadlineCompare());
        NextItem = Items.back();
        Items.erase(Items.end() - 1);
    }
}

void TScheduler::SchedulerThread() {
    for (;;) {
        IScheduleItemAutoPtr current;

        {
            TGuard<TLock> guard(Lock);

            if (StopThread) {
                break;
            }

            if (!!NextItem) {
                CondVar.WaitD(Lock, NextItem->GetScheduleTime());
            } else {
                CondVar.WaitI(Lock);
            }

            if (StopThread) {
                break;
            }

            // signal comes if either scheduler is to be stopped of there's work to do
            Y_ABORT_UNLESS(!!NextItem, "state check");

            if (TInstant::Now() < NextItem->GetScheduleTime()) {
                // NextItem is updated since WaitD
                continue;
            }

            current = NextItem.Release();
        }

        current->Do();
        current.Destroy();

        {
            TGuard<TLock> guard(Lock);
            FillNextItem();
        }
    }
}
