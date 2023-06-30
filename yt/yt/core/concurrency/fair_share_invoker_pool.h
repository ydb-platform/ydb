#pragma once

#include "public.h"
#include "private.h"

#include <yt/yt/core/actions/invoker_pool.h>
#include <yt/yt/core/actions/public.h>

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
IDiagnosableInvokerPoolPtr CreateFairShareInvokerPool(
    IInvokerPtr underlyingInvoker,
    int invokerCount,
    TFairShareCallbackQueueFactory callbackQueueFactory = CreateFairShareCallbackQueue);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
