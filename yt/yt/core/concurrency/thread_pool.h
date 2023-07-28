#pragma once

#include "public.h"

#include <yt/yt/core/actions/callback.h>

#include <yt/yt/core/threading/public.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

struct IThreadPool
    : public virtual TRefCounted
{
    virtual void Shutdown() = 0;

    //! Returns current thread count, it can differ from value set by Configure()
    //! because it clamped between 1 and maximum thread count.
    virtual int GetThreadCount() = 0;
    virtual void Configure(int threadCount) = 0;

    virtual const IInvokerPtr& GetInvoker() = 0;
};

DEFINE_REFCOUNTED_TYPE(IThreadPool)

IThreadPoolPtr CreateThreadPool(
    int threadCount,
    const TString& threadNamePrefix,
    NThreading::EThreadPriority threadPriority = NThreading::EThreadPriority::Normal,
    TDuration pollingPeriod = TDuration::MilliSeconds(10));

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
