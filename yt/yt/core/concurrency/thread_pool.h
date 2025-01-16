#pragma once

#include "public.h"

#include <yt/yt/core/actions/callback.h>

#include <yt/yt/core/threading/public.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

struct IThreadPool
    : public virtual TRefCounted
{
    //! Terminates all the threads.
    virtual void Shutdown() = 0;

    //! Returns the current thread count.
    /*!
     *  This can differ from value set by #SetThreadCount
     *  because it clamped between 1 and the maximum thread count.
     */
    virtual int GetThreadCount() = 0;

    //! Updates thread count at runtime.
    virtual void SetThreadCount(int threadCount) = 0;

    //! Updates polling period at runtime.
    virtual void SetPollingPeriod(TDuration pollingPeriod) = 0;

    //! Returns the invoker for enqueuing callbacks into the thread pool.
    virtual const IInvokerPtr& GetInvoker() = 0;
};

DEFINE_REFCOUNTED_TYPE(IThreadPool)

////////////////////////////////////////////////////////////////////////////////

struct TThreadPoolOptions
{
    NThreading::EThreadPriority ThreadPriority = NThreading::EThreadPriority::Normal;
    TDuration PollingPeriod = TDuration::MilliSeconds(10);
    std::function<void()> ThreadInitializer;
};

IThreadPoolPtr CreateThreadPool(
    int threadCount,
    const TString& threadNamePrefix,
    const TThreadPoolOptions& options = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
