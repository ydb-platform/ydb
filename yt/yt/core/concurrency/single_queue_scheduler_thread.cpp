#include "single_queue_scheduler_thread.h"
#include "invoker_queue.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <class TQueueImpl>
TSingleQueueSchedulerThread<TQueueImpl>::TSingleQueueSchedulerThread(
    TInvokerQueuePtr<TQueueImpl> queue,
    TIntrusivePtr<NThreading::TEventCount> callbackEventCount,
    const TString& threadGroupName,
    const TString& threadName,
    NThreading::EThreadPriority threadPriority,
    int shutdownPriority)
    : TSchedulerThread(
        std::move(callbackEventCount),
        threadGroupName,
        threadName,
        threadPriority,
        shutdownPriority)
    , Queue_(std::move(queue))
    , Token_(Queue_->MakeConsumerToken())
{ }

template <class TQueueImpl>
TClosure TSingleQueueSchedulerThread<TQueueImpl>::BeginExecute()
{
    return Queue_->BeginExecute(&CurrentAction_, &Token_);
}

template <class TQueueImpl>
void TSingleQueueSchedulerThread<TQueueImpl>::EndExecute()
{
    Queue_->EndExecute(&CurrentAction_);
}

template <class TQueueImpl>
void TSingleQueueSchedulerThread<TQueueImpl>::OnStart()
{
    Queue_->SetThreadId(GetThreadId());
}

////////////////////////////////////////////////////////////////////////////////

template class TSingleQueueSchedulerThread<TMpmcQueueImpl>;
template class TSingleQueueSchedulerThread<TMpscQueueImpl>;

////////////////////////////////////////////////////////////////////////////////

template <class TQueueImpl>
TSuspendableSingleQueueSchedulerThread<TQueueImpl>::TSuspendableSingleQueueSchedulerThread(
    TInvokerQueuePtr<TQueueImpl> queue,
    TIntrusivePtr<NThreading::TEventCount> callbackEventCount,
    const TString& threadGroupName,
    const TString& threadName)
    : TSchedulerThread(
        std::move(callbackEventCount),
        threadGroupName,
        threadName)
    , Queue_(std::move(queue))
    , Token_(Queue_->MakeConsumerToken())
{ }

template <class TQueueImpl>
TFuture<void> TSuspendableSingleQueueSchedulerThread<TQueueImpl>::Suspend(bool immediately)
{
    auto guard = Guard(Lock_);

    if (!Suspending_.exchange(true)) {
        SuspendImmediately_ = immediately;
        SuspendedPromise_ = NewPromise<void>();
        ResumeEvent_ = New<NThreading::TEvent>();
    } else if (immediately) {
        SuspendImmediately_ = true;
    }

    return SuspendedPromise_.ToFuture();
}

template <class TQueueImpl>
void TSuspendableSingleQueueSchedulerThread<TQueueImpl>::Resume()
{
    YT_VERIFY(Suspending_);
    YT_VERIFY(SuspendedPromise_.IsSet());

    auto guard = Guard(Lock_);

    Suspending_ = false;
    SuspendImmediately_ = false;

    ResumeEvent_->NotifyAll();
}

template <class TQueueImpl>
void TSuspendableSingleQueueSchedulerThread<TQueueImpl>::Shutdown(bool graceful)
{
    auto guard = Guard(Lock_);

    if (Suspending_) {
        Suspending_ = false;
        SuspendImmediately_ = false;

        ResumeEvent_->NotifyAll();
    }

    Stop(graceful);
}

template <class TQueueImpl>
TClosure TSuspendableSingleQueueSchedulerThread<TQueueImpl>::BeginExecute()
{
    if (Suspending_ && (SuspendImmediately_ || Queue_->IsEmpty())) {
        TIntrusivePtr<NThreading::TEvent> resumeEvent;
        {
            auto guard = Guard(Lock_);

            SuspendedPromise_.Set();
            resumeEvent = ResumeEvent_;
        }

        resumeEvent->Wait();
    }

    return Queue_->BeginExecute(&CurrentAction_, &Token_);
}

template <class TQueueImpl>
void TSuspendableSingleQueueSchedulerThread<TQueueImpl>::EndExecute()
{
    Queue_->EndExecute(&CurrentAction_);
}

template <class TQueueImpl>
void TSuspendableSingleQueueSchedulerThread<TQueueImpl>::OnStart()
{
    Queue_->SetThreadId(GetThreadId());
}

template <class TQueueImpl>
void TSuspendableSingleQueueSchedulerThread<TQueueImpl>::OnStop()
{
    Queue_->DrainConsumer();
}

////////////////////////////////////////////////////////////////////////////////

template class TSuspendableSingleQueueSchedulerThread<TMpscQueueImpl>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
