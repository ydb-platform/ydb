#pragma once

#include "public.h"
#include "private.h"

#include <yt/yt/core/actions/invoker_pool.h>
#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/misc/adjusted_exponential_moving_average.h>
#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/profiling/public.h>

#include <functional>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Represents several buckets of callbacks with one CPU time counter per each bucket.
struct IFairShareCallbackQueue
    : public virtual TRefCounted
{
    //! Enqueues #callback to bucket with index #bucketIndex.
    virtual void Enqueue(TClosure callback, int bucketIndex) = 0;

    //! Dequeues callback from the most starving bucket (due to CPU time counters) and
    //! stores it at #resultCallback, also stores index of the bucket at #resultBucketIndex.
    //! Returns |true| if and only if there is at least one callback in the queue.
    virtual bool TryDequeue(TClosure* resultCallback, int* resultBucketIndex) = 0;

    //! Add #cpuTime to the CPU time counter for bucket with index #bucketIndex.
    virtual void AccountCpuTime(int bucketIndex, NProfiling::TCpuDuration cpuTime) = 0;
};

DEFINE_REFCOUNTED_TYPE(IFairShareCallbackQueue)

////////////////////////////////////////////////////////////////////////////////

//! Creates multithreaded implementation of the IFairShareCallbackQueue with #bucketCount buckets.
IFairShareCallbackQueuePtr CreateFairShareCallbackQueue(int bucketCount);

using TFairShareCallbackQueueFactory = std::function<IFairShareCallbackQueuePtr(int)>;

////////////////////////////////////////////////////////////////////////////////

//! Creates an invoker pool that holds number of invokers and shares CPU time between them fairly.
//! Pay attention: CPU time accounting between consecutive context switches is not supported yet,
//! so use with care in case of multiple workers in the underlying invoker.
//! Factory #callbackQueueFactory is used by the invoker pool for creation of the storage for callbacks.
//! Ability to specify #callbackQueueFactory is provided for testing purposes.
TDiagnosableInvokerPoolPtr CreateFairShareInvokerPool(
    IInvokerPtr underlyingInvoker,
    int invokerCount,
    TFairShareCallbackQueueFactory callbackQueueFactory = CreateFairShareCallbackQueue,
    TDuration actionTimeRelevancyHalflife = TAdjustedExponentialMovingAverage::DefaultHalflife);

////////////////////////////////////////////////////////////////////////////////

//! Creates invoker pool from above with invokerCount = bucketNames.size()
//! And adds profiling on top of it.

TDiagnosableInvokerPoolPtr CreateProfiledFairShareInvokerPool(
    IInvokerPtr underlyingInvoker,
    TFairShareCallbackQueueFactory callbackQueueFactory = CreateFairShareCallbackQueue,
    TDuration actionTimeRelevancyHalflife = TAdjustedExponentialMovingAverage::DefaultHalflife,
    const TString& poolName = "fair_share_invoker_pool",
    std::vector<TString> bucketNames = {},
    NProfiling::IRegistryImplPtr registry = nullptr);

////////////////////////////////////////////////////////////////////////////////

//! Same as above but bucket names are derived from EInvoker domain values.

template <class EInvoker>
    requires TEnumTraits<EInvoker>::IsEnum
TDiagnosableInvokerPoolPtr CreateEnumIndexedProfiledFairShareInvokerPool(
    IInvokerPtr underlyingInvoker,
    TFairShareCallbackQueueFactory callbackQueueFactory = CreateFairShareCallbackQueue,
    TDuration actionTimeRelevancyHalflife = TAdjustedExponentialMovingAverage::DefaultHalflife,
    const TString& poolName = "fair_share_invoker_pool",
    NProfiling::IRegistryImplPtr registry = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

#define FAIR_SHARE_INVOKER_POOL_INL_H_
#include "fair_share_invoker_pool-inl.h"
#undef FAIR_SHARE_INVOKER_POOL_INL_H_
