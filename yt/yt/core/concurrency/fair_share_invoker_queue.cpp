#include "fair_share_invoker_queue.h"

#include "invoker_queue.h"

#include <library/cpp/yt/threading/event_count.h>

#include <cmath>

namespace NYT::NConcurrency {

using namespace NProfiling;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TFairShareInvokerQueue::TFairShareInvokerQueue(
    TIntrusivePtr<NThreading::TEventCount> callbackEventCount,
    const std::vector<TBucketDescription>& bucketDescriptions,
    NProfiling::IRegistryImplPtr registry)
    : Weights_(bucketDescriptions.size(), 1.0)
{
    Buckets_.reserve(bucketDescriptions.size());
    for (const auto& bucketDescription : bucketDescriptions) {
        auto& bucket = Buckets_.emplace_back();
        bucket.Queue = New<TMpscInvokerQueue>(
            callbackEventCount,
            bucketDescription.QueueTagSets,
            bucketDescription.QueueProfilerTags,
            registry);
        int profilingTagCount = bucketDescription.QueueTagSets.size();
        bucket.Invokers.reserve(profilingTagCount);
        for (int index = 0; index < profilingTagCount; ++index) {
            bucket.Invokers.push_back(bucket.Queue->GetProfilingTagSettingInvoker(index));
        }
    }
}

TFairShareInvokerQueue::~TFairShareInvokerQueue() = default;

void TFairShareInvokerQueue::SetThreadId(NThreading::TThreadId threadId)
{
    for (auto& bucket : Buckets_) {
        bucket.Queue->SetThreadId(threadId);
    }
}

const IInvokerPtr& TFairShareInvokerQueue::GetInvoker(int bucketIndex, int queueIndex) const
{
    YT_ASSERT(0 <= bucketIndex && bucketIndex < static_cast<int>(Buckets_.size()));
    const auto& bucket = Buckets_[bucketIndex];

    YT_ASSERT(0 <= queueIndex && queueIndex < static_cast<int>(bucket.Invokers.size()));
    return bucket.Invokers[queueIndex];
}

void TFairShareInvokerQueue::Shutdown()
{
    for (auto& bucket : Buckets_) {
        bucket.Queue->Shutdown();
    }
}

void TFairShareInvokerQueue::DrainProducer()
{
    for (auto& bucket : Buckets_) {
        bucket.Queue->DrainProducer();
    }
}

void TFairShareInvokerQueue::DrainConsumer()
{
    for (auto& bucket : Buckets_) {
        bucket.Queue->DrainConsumer();
    }
}

bool TFairShareInvokerQueue::IsRunning() const
{
    for (const auto& bucket : Buckets_) {
        if (!bucket.Queue->IsRunning()) {
            return false;
        }
    }
    return true;
}

bool TFairShareInvokerQueue::BeginExecute(TEnqueuedAction* action)
{
    YT_VERIFY(!CurrentBucket_);

    // Reconfigure queue if required.
    if (NeedToReconfigure_) {
        auto guard = Guard(WeightsLock_);
        YT_ASSERT(Weights_.size() == Buckets_.size());
        for (int bucketIndex = 0; bucketIndex < std::ssize(Weights_); ++bucketIndex) {
            Buckets_[bucketIndex].InversedWeight = std::ceil(UnitWeight * Weights_[bucketIndex]);
        }
        NeedToReconfigure_ = false;
    }

    // Check if any callback is ready at all.
    CurrentBucket_ = GetStarvingBucket();
    if (!CurrentBucket_) {
        return false;
    }

    // Reduce excesses (with truncation).
    auto delta = CurrentBucket_->ExcessTime;
    for (auto& bucket : Buckets_) {
        bucket.ExcessTime = std::max<NProfiling::TCpuDuration>(bucket.ExcessTime - delta, 0);
    }

    // Pump the starving queue.
    return CurrentBucket_->Queue->BeginExecute(action);
}

void TFairShareInvokerQueue::EndExecute(TEnqueuedAction* action)
{
    if (!CurrentBucket_) {
        return;
    }

    CurrentBucket_->Queue->EndExecute(action);
    CurrentBucket_->ExcessTime += (action->FinishedAt - action->StartedAt);
    CurrentBucket_ = nullptr;
}

void TFairShareInvokerQueue::Reconfigure(std::vector<double> weights)
{
    auto guard = Guard(WeightsLock_);
    Weights_ = std::move(weights);

    NeedToReconfigure_ = true;
}

TFairShareInvokerQueue::TBucket* TFairShareInvokerQueue::GetStarvingBucket()
{
    // Compute min excess over non-empty queues.
    auto minExcessTime = std::numeric_limits<NProfiling::TCpuDuration>::max();
    TBucket* minBucket = nullptr;
    for (auto& bucket : Buckets_) {
        const auto& queue = bucket.Queue;
        YT_ASSERT(queue);
        if (!queue->IsEmpty()) {
            auto weightedExcessTime = bucket.ExcessTime * bucket.InversedWeight;
            if (weightedExcessTime < minExcessTime) {
                minExcessTime = weightedExcessTime;
                minBucket = &bucket;
            }
        }
    }
    return minBucket;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

