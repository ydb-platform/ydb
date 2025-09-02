#pragma once

#include "public.h"

#include <yt/yt/core/actions/signal.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

struct IPoolWeightProvider
    : public virtual TRefCounted
{
    virtual double GetWeight(const std::string& poolName) = 0;
};

DEFINE_REFCOUNTED_TYPE(IPoolWeightProvider)

////////////////////////////////////////////////////////////////////////////////

struct ITwoLevelFairShareThreadPool
    : public virtual TRefCounted
{
    virtual int GetThreadCount() = 0;
    virtual void SetThreadCount(int threadCount) = 0;
    virtual void SetPollingPeriod(TDuration pollingPeriod) = 0;

    virtual IInvokerPtr GetInvoker(
        const std::string& poolName,
        const TFairShareThreadPoolTag& tag) = 0;

    virtual void Shutdown() = 0;

    //! Invoked to inform of the current wait time for invocations via this invoker.
    //! These invocations, however, are not guaranteed.
    using TWaitTimeObserver = TCallback<void(TDuration waitTime)>;

    DECLARE_INTERFACE_SIGNAL(TWaitTimeObserver::TSignature, WaitTimeObserved);
};

DEFINE_REFCOUNTED_TYPE(ITwoLevelFairShareThreadPool)

////////////////////////////////////////////////////////////////////////////////

struct TNewTwoLevelFairShareThreadPoolOptions
{
    IPoolWeightProviderPtr PoolWeightProvider = nullptr;
    bool VerboseLogging = false;
    TDuration PollingPeriod = TDuration::MilliSeconds(10);
    TDuration PoolRetentionTime = TDuration::Seconds(30);
};

////////////////////////////////////////////////////////////////////////////////

ITwoLevelFairShareThreadPoolPtr CreateTwoLevelFairShareThreadPool(
    int threadCount,
    const std::string& threadNamePrefix,
    const TNewTwoLevelFairShareThreadPoolOptions& options = {});


} // namespace NYT::NConcurrency
