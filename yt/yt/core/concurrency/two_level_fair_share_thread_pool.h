#pragma once

#include "public.h"

#include <yt/yt/core/actions/public.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

struct IPoolWeightProvider
    : public virtual TRefCounted
{
    virtual double GetWeight(const TString& poolName) = 0;
};

DEFINE_REFCOUNTED_TYPE(IPoolWeightProvider)

////////////////////////////////////////////////////////////////////////////////

struct ITwoLevelFairShareThreadPool
    : public virtual TRefCounted
{
    virtual int GetThreadCount() = 0;
    virtual void Configure(int threadCount) = 0;
    virtual void Configure(TDuration pollingPeriod) = 0;

    virtual IInvokerPtr GetInvoker(
        const TString& poolName,
        const TFairShareThreadPoolTag& tag) = 0;

    virtual void Shutdown() = 0;

    using TWaitTimeObserver = std::function<void(TDuration)>;
    virtual void RegisterWaitTimeObserver(TWaitTimeObserver waitTimeObserver) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITwoLevelFairShareThreadPool)

ITwoLevelFairShareThreadPoolPtr CreateTwoLevelFairShareThreadPool(
    int threadCount,
    const TString& threadNamePrefix,
    IPoolWeightProviderPtr poolWeightProvider = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
