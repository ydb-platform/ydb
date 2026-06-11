#include "scheduler_thread.h"

#include "helpers.h"
#include "private.h"
#include "invoker_queue.h"

namespace NYT::NConcurrency {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TSchedulerThread::TSchedulerThread(
    TIntrusivePtr<NThreading::TEventCount> callbackEventCount,
    std::string threadGroupName,
    std::string threadName,
    NThreading::TThreadOptions options)
    : TFiberSchedulerThread(
        std::move(threadGroupName),
        std::move(threadName),
        std::move(options))
    , CallbackEventCount_(std::move(callbackEventCount))
{ }

TSchedulerThread::~TSchedulerThread()
{
    Stop();
}

void TSchedulerThread::OnStart()
{ }

void TSchedulerThread::Stop(bool graceful)
{
    GracefulStop_.store(graceful);
    TThread::Stop();
}

void TSchedulerThread::Stop()
{
    TFiberSchedulerThread::Stop();
    TThread::Stop();
}

void TSchedulerThread::StartEpilogue()
{
    TFiberSchedulerThread::StartEpilogue();
    OnStart();
}

void TSchedulerThread::StopPrologue()
{
    TFiberSchedulerThread::StopPrologue();
    CallbackEventCount_->NotifyAll();
}

void TSchedulerThread::StopEpilogue()
{
    TFiberSchedulerThread::StopEpilogue();
}

TClosure TSchedulerThread::OnExecute()
{
    EndExecute();

    while (true) {
        auto cookie = CallbackEventCount_->PrepareWait();

        // Stop flag must be checked after PrepareWait because thread is notified when stop flag is set.
        // Otherwise, we can miss notification.
        bool stopping = IsStopping();
        if (stopping && !GracefulStop_.load()) {
            CallbackEventCount_->CancelWait();
            return TClosure();
        }

        auto callback = BeginExecute();

        if (callback || stopping) {
            CallbackEventCount_->CancelWait();
            return callback;
        }

        constexpr auto WaitTimeout = TDuration::Seconds(1);
        if (!CallbackEventCount_->Wait(cookie, WaitTimeout)) {
            ReclaimHazardPointersPeriodically(GetCpuInstant(), /*force*/ false);
        }

        EndExecute();
    }
}

TClosure TSchedulerThread::BeginExecuteImpl(bool dequeued, TEnqueuedAction* action)
{
    if (!dequeued) {
        return {};
    }
    ReclaimHazardPointersPeriodically(action->StartedAt, /*force*/ false);
    return std::move(action->Callback);
}

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NConcurrency
