#pragma once

#include "public.h"

#include <yt/yt/core/actions/callback.h>

#include <yt/yt/core/misc/range.h>

#include <yt/yt/core/profiling/public.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

struct IFairShareActionQueue
    : public TRefCounted
{
    virtual const IInvokerPtr& GetInvoker(int index) = 0;

    virtual void Reconfigure(const THashMap<TString, double>& newBucketWeights) = 0;
};

DEFINE_REFCOUNTED_TYPE(IFairShareActionQueue)

////////////////////////////////////////////////////////////////////////////////

IFairShareActionQueuePtr CreateFairShareActionQueue(
    const TString& threadName,
    const std::vector<TString>& queueNames,
    const THashMap<TString, std::vector<TString>>& bucketToQueues = {},
    NProfiling::IRegistryImplPtr registry = {});

////////////////////////////////////////////////////////////////////////////////

template <typename EQueue>
struct IEnumIndexedFairShareActionQueue
    : public TRefCounted
{
    virtual const IInvokerPtr& GetInvoker(EQueue queue) = 0;

    virtual void Reconfigure(const THashMap<TString, double>& newBucketWeights) = 0;
};

////////////////////////////////////////////////////////////////////////////////

template <typename EQueue, typename EBucket = EQueue>
IEnumIndexedFairShareActionQueuePtr<EQueue> CreateEnumIndexedFairShareActionQueue(
    const TString& threadName,
    const THashMap<EBucket, std::vector<EQueue>>& bucketToQueues = {},
    NProfiling::IRegistryImplPtr registry = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

#define FAIR_SHARE_ACTION_QUEUE_INL_H_
#include "fair_share_action_queue-inl.h"
#undef FAIR_SHARE_ACTION_QUEUE_INL_H_
