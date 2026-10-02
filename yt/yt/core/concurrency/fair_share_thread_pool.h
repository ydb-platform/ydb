#pragma once

#include "public.h"

#include <yt/yt/core/actions/public.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

struct IFairShareThreadPool
    : public virtual TRefCounted
{
    virtual IInvokerPtr GetInvoker(const TFairShareThreadPoolTag& tag) = 0;

    virtual void SetThreadCount(int threadCount) = 0;

    virtual void Shutdown() = 0;
};

DEFINE_REFCOUNTED_TYPE(IFairShareThreadPool)

////////////////////////////////////////////////////////////////////////////////

IFairShareThreadPoolPtr CreateFairShareThreadPool(
    int threadCount,
    const std::string& threadNamePrefix);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
