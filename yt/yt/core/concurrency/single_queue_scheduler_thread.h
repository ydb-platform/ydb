#pragma once

#include "private.h"
#include "scheduler_thread.h"
#include "invoker_queue.h"

#include <yt/yt/core/actions/future.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <class TQueueImpl>
class TSingleQueueSchedulerThread
    : public TSchedulerThread
{
public:
    TSingleQueueSchedulerThread(
        TInvokerQueuePtr<TQueueImpl> queue,
        TIntrusivePtr<NThreading::TEventCount> callbackEventCount,
        TString threadGroupName,
        TString threadName,
        NThreading::TThreadOptions options = {});

protected:
    const TInvokerQueuePtr<TQueueImpl> Queue_;
    typename TQueueImpl::TConsumerToken Token_;

    TEnqueuedAction CurrentAction_;

    TClosure BeginExecute() override;
    void EndExecute() override;

    void OnStart() override;
};

////////////////////////////////////////////////////////////////////////////////

template <class TQueueImpl>
class TSuspendableSingleQueueSchedulerThread
    : public TSchedulerThread
{
public:
    TSuspendableSingleQueueSchedulerThread(
        TInvokerQueuePtr<TQueueImpl> queue,
        TIntrusivePtr<NThreading::TEventCount> callbackEventCount,
        TString threadGroupName,
        TString threadName,
        NThreading::TThreadOptions options);

    TFuture<void> Suspend(bool immediately);

    void Resume();

    void Shutdown(bool graceful);

protected:
    const TInvokerQueuePtr<TQueueImpl> Queue_;
    typename TQueueImpl::TConsumerToken Token_;

    TEnqueuedAction CurrentAction_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);

    std::atomic<bool> Suspending_ = false;

    std::atomic<bool> SuspendImmediately_ = false;
    TPromise<void> SuspendedPromise_ = NewPromise<void>();
    TIntrusivePtr<NThreading::TEvent> ResumeEvent_;

    TClosure BeginExecute() override;
    void EndExecute() override;

    void OnStart() override;
    void OnStop() override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
