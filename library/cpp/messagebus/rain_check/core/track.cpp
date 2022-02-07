#include "track.h"

using namespace NRainCheck;
using namespace NRainCheck::NPrivate;

void TTaskTrackerReceipt::SetDone() {
    TaskTracker->GetQueue<TTaskTrackerReceipt*>()->EnqueueAndSchedule(this);
}

TString TTaskTrackerReceipt::GetStatusSingleLine() {
    return Task->GetStatusSingleLine();
}

TTaskTracker::TTaskTracker(NActor::TExecutor* executor)
    : NActor::TActor<TTaskTracker>(executor)
{
}

TTaskTracker::~TTaskTracker() {
    Y_ASSERT(Tasks.Empty());
}

void TTaskTracker::Shutdown() {
    ShutdownFlag.Set(true);
    Schedule();
    ShutdownEvent.WaitI();
}

void TTaskTracker::ProcessItem(NActor::TDefaultTag, NActor::TDefaultTag, ITaskFactory* taskFactory) {
    THolder<ITaskFactory> holder(taskFactory);

    THolder<TTaskTrackerReceipt> receipt(new TTaskTrackerReceipt(this));
    receipt->Task = taskFactory->NewTask(receipt.Get());

    Tasks.PushBack(receipt.Release());
}

void TTaskTracker::ProcessItem(NActor::TDefaultTag, NActor::TDefaultTag, TTaskTrackerReceipt* receipt) {
    Y_ASSERT(!receipt->Empty());
    receipt->Unlink();
    delete receipt;
}

void TTaskTracker::ProcessItem(NActor::TDefaultTag, NActor::TDefaultTag, TAsyncResult<TTaskTrackerStatus>* status) {
    TTaskTrackerStatus s;
    s.Size = Tasks.Size();
    status->SetResult(s);
}

void TTaskTracker::Act(NActor::TDefaultTag) {
    GetQueue<TAsyncResult<TTaskTrackerStatus>*>()->DequeueAll();
    GetQueue<ITaskFactory*>()->DequeueAll();
    GetQueue<TTaskTrackerReceipt*>()->DequeueAll();

    if (ShutdownFlag.Get()) {
        if (Tasks.Empty()) {
            ShutdownEvent.Signal();
        }
    }
}

ui32 TTaskTracker::Size() {
    TAsyncResult<TTaskTrackerStatus> r;
    GetQueue<TAsyncResult<TTaskTrackerStatus>*>()->EnqueueAndSchedule(&r);
    return r.GetResult().Size;
}
