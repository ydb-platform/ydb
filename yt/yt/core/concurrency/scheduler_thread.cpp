#include "scheduler_thread.h"
#include "private.h"

namespace NYT::NConcurrency {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TSchedulerThreadBase::~TSchedulerThreadBase()
{
    Stop();
}

TSchedulerThreadBase::TSchedulerThreadBase(
    TIntrusivePtr<NThreading::TEventCount> callbackEventCount,
    const TString& threadGroupName,
    const TString& threadName,
    NThreading::EThreadPriority threadPriority,
    int shutdownPriority)
    : TFiberSchedulerThread(
        threadGroupName,
        threadName,
        threadPriority,
        shutdownPriority)
    , CallbackEventCount_(std::move(callbackEventCount))
{ }

void TSchedulerThreadBase::OnStart()
{ }

void TSchedulerThreadBase::OnStop()
{ }

void TSchedulerThreadBase::Stop(bool graceful)
{
    GracefulStop_.store(graceful);
    TThread::Stop();
}

void TSchedulerThreadBase::Stop()
{
    TThread::Stop();
}

void TSchedulerThreadBase::StartEpilogue()
{
    OnStart();
}

void TSchedulerThreadBase::StopPrologue()
{
    CallbackEventCount_->NotifyAll();
}

void TSchedulerThreadBase::StopEpilogue()
{
    OnStop();
}

////////////////////////////////////////////////////////////////////////////////

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

        CallbackEventCount_->Wait(cookie);
        EndExecute();
    }
}

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NConcurrency
