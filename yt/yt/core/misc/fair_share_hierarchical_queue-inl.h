#pragma once
#ifndef FAIR_SHARE_HIERARCHICAL_QUEUE_INL_H_
#error "Direct inclusion of this file is not allowed, include fair_share_hierarchical_queue.h"
// For the sake of sane code completion.
#include "fair_share_hierarchical_queue.h"
#endif

#include "config.h"

#include <yt/yt/core/misc/collection_helpers.h>

#include <util/digest/multi.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <typename TTag>
TFairShareHierarchyLevel<TTag>::TFairShareHierarchyLevel(TTag tag, double weight)
    : Tag_(std::move(tag))
    , Weight_(weight)
{
    YT_VERIFY(weight > 0.0);
}

template <typename TTag>
const TTag& TFairShareHierarchyLevel<TTag>::GetTag() const
{
    return Tag_;
}

template <typename TTag>
double TFairShareHierarchyLevel<TTag>::GetWeight() const
{
    return Weight_;
}

////////////////////////////////////////////////////////////////////////////////

template <typename TTag>
TFairShareHierarchicalSchedulerLog<TTag>::TFairShareHierarchicalSchedulerLog(
    TGuid requestId,
    i64 size,
    bool isSlot,
    std::vector<TFairShareHierarchyLevel<TTag>> levels,
    TInstant createdAt)
    : RequestId_(requestId)
    , Size_(size)
    , Slot_(isSlot)
    , CreatedAt_(createdAt)
    , Levels_(std::move(levels))
{ }

////////////////////////////////////////////////////////////////////////////////

template <typename TTag>
TFairShareHierarchicalSlotQueueSlot<TTag>::TFairShareHierarchicalSlotQueueSlot(
    i64 size,
    std::vector<IFairShareHierarchicalSlotQueueResourcePtr> resources,
    std::vector<TFairShareHierarchyLevel<TTag>> levels)
    : SlotId_(TGuid::Create())
    , Size_(size)
    , Levels_(std::move(levels))
    , EnqueueTime_(TInstant::Now())
    , Resources_(std::move(resources))
{
    YT_ASSERT(Size_ >= 0);
}

template <typename TTag>
const std::vector<TFairShareHierarchyLevel<TTag>>& TFairShareHierarchicalSlotQueueSlot<TTag>::GetLevels() const
{
    return Levels_;
}

template <typename TTag>
TGuid TFairShareHierarchicalSlotQueueSlot<TTag>::GetSlotId() const
{
    return SlotId_;
}

template <typename TTag>
const std::vector<IFairShareHierarchicalSlotQueueResourcePtr>& TFairShareHierarchicalSlotQueueSlot<TTag>::GetResources() const
{
    return Resources_;
}

template <typename TTag>
void TFairShareHierarchicalSlotQueueSlot<TTag>::Cancel(TError error)
{
    Cancelled_.Fire(std::move(error));
}

template <typename TTag>
i64 TFairShareHierarchicalSlotQueueSlot<TTag>::GetSize() const
{
    return Size_;
}

template <typename TTag>
TInstant TFairShareHierarchicalSlotQueueSlot<TTag>::GetEnqueueTime() const
{
    return EnqueueTime_;
}

template <typename TTag>
TError TFairShareHierarchicalSlotQueueSlot<TTag>::AcquireResources()
{
    TError allResourcesAcquired;

    // Try to acquire all resources.
    for (auto& resource : Resources_) {
        auto error = resource->AcquireResource();
        if (!error.IsOK()) {
            allResourcesAcquired = std::move(error);
            break;
        }
    }

    // If not all resources were acquired, release any that were acquired.
    if (!allResourcesAcquired.IsOK()) {
        for (auto& resource : Resources_) {
            if (resource->IsAcquired()) {
                resource->ReleaseResource();
            }
        }
    }

    return allResourcesAcquired;
}

template <typename TTag>
bool TFairShareHierarchicalSlotQueueSlot<TTag>::NeedExceedsLimit() const
{
    // Check if any resource's need exceeds its total capacity.
    for (auto& resource : Resources_) {
        if (resource->GetNeedResourcesCount() > resource->GetTotalResourcesCount()) {
            return true;
        }
    }

    return false;
}

template <typename TTag>
void TFairShareHierarchicalSlotQueueSlot<TTag>::ReleaseResources()
{
    // Release all acquired resources.
    for (auto& resource : Resources_) {
        if (resource->IsAcquired()) {
            resource->ReleaseResource();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename TTag>
bool CompareByEnqueueTime(const TFairShareHierarchicalSlotQueueSlotPtr<TTag>& lhs, const TFairShareHierarchicalSlotQueueSlotPtr<TTag>& rhs)
{
    auto lhsTime = lhs->GetEnqueueTime();
    auto rhsTime = rhs->GetEnqueueTime();

    // If the times are equal, compare by request ID.
    if (lhsTime == rhsTime) {
        return lhs->GetSlotId() < rhs->GetSlotId();
    } else if (lhsTime < rhsTime) {
        // If lhs was enqueued earlier, it has higher priority.
        return false;
    } else {
        // If rhs was enqueued earlier, it has higher priority.
        return true;
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename TTag>
TFairShareHierarchicalScheduler<TTag>::TFairShareHierarchicalScheduler(
    TFairShareHierarchicalSchedulerDynamicConfigPtr config,
    NProfiling::TProfiler profiler)
    : Config_(std::move(config))
    , Profiler_(profiler.WithHot())
    , RootBucket_(New<TFairShareHierarchicalSlotQueueBucketNode>())
{
    profiler.AddFuncGauge("/slot_window_size", MakeStrong(this), [this] {
        return RootBucket_->SlotWindowSize.load();
    });
    profiler.AddFuncGauge("/request_window_size", MakeStrong(this), [this] {
        return RootBucket_->RequestWindowSize.load();
    });
    profiler.AddFuncGauge("/slot_window_log_count", MakeStrong(this), [this] {
        return RootBucket_->SlotWindowLogCount.load();
    });
    profiler.AddFuncGauge("/request_window_log_count", MakeStrong(this), [this] {
        return RootBucket_->RequestWindowLogCount.load();
    });
    profiler.AddFuncGauge("/bucket_count", MakeStrong(this), [this] {
        return BucketCount_.load();
    });
    profiler.AddFuncGauge("/window_size", MakeStrong(this), [this] {
        auto config = Config_.Acquire();
        return config->WindowSize.Seconds();
    });

    EnqueueLogWallTimer_ = profiler.Timer("/time/enqueue_wall_time");
    DequeueLogWallTimer_ = profiler.Timer("/time/dequeue_wall_time");
    TrimLogWallTimer_ = profiler.Timer("/time/trim_wall_time");
}

template <typename TTag>
NYTree::IYPathServicePtr TFairShareHierarchicalScheduler<TTag>::GetOrchidService()
{
    return NYTree::IYPathService::FromProducer(BIND(&TFairShareHierarchicalScheduler<TTag>::BuildOrchid, MakeWeak(this)));
}

template <typename TTag>
void TFairShareHierarchicalScheduler<TTag>::TFairShareHierarchicalSlotQueueBucketNode::BuildOrchid(NYTree::TFluentAny fluent)
{
    THashMap<TTag, TFairShareHierarchicalSlotQueueBucketNodePtr> localCopy;
    i64 slotWindowLogCount = 0;
    i64 requestWindowLogCount = 0;
    {
        auto guard = ReaderGuard(BucketLock);
        localCopy = Buckets;
    }

    slotWindowLogCount = SlotWindowLogCount;
    requestWindowLogCount = RequestWindowLogCount;

    fluent
        .BeginMap()
            .Item("slot_fair_share").Value(SlotFairShare)
            .Item("request_fair_share").Value(RequestFairShare)
            .Item("slot_total_fair_share").Value(SlotTotalFairShare)
            .Item("request_total_fair_share").Value(RequestTotalFairShare)
            .Item("slot_weight").Value(slotWindowLogCount == 0 ? 0 : SummarySlotWeight / slotWindowLogCount)
            .Item("request_weight").Value(requestWindowLogCount == 0 ? 0 : SummaryRequestWeight / requestWindowLogCount)
            .Item("slot_window_size").Value(SlotWindowSize)
            .Item("request_window_size").Value(RequestWindowSize)
            .Item("slot_window_log_count").Value(slotWindowLogCount)
            .Item("request_window_log_count").Value(requestWindowLogCount)
            .Item("buckets").DoMapFor(localCopy, [] (auto fluent, const auto& entry) {
                const auto& [tag, bucket] = entry;
                fluent.Item(tag).Do([&] (auto fluent) {
                    bucket->BuildOrchid(fluent);
                });
            })
        .EndMap();
}

template <typename TTag>
void TFairShareHierarchicalScheduler<TTag>::BuildOrchid(NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("root").Do([&] (auto fluent) {
                auto bucket = RootBucket_;
                bucket->BuildOrchid(fluent);
            })
        .EndMap();
}

template <typename TTag>
TFairShareHierarchicalScheduler<TTag>::TFairShareHierarchicalSlotQueueBucketNodePtr TFairShareHierarchicalScheduler<TTag>::GetBucket(
    const std::vector<TTag>& tags) const
{
    auto it = tags.begin();
    auto bucket = RootBucket_;

    while (it != tags.end()) {
        auto& tag = *it;
        auto guard = ReaderGuard(bucket->BucketLock);
        auto bucketIt = bucket->Buckets.find(tag);

        if (bucketIt == bucket->Buckets.end()) {
            return nullptr;
        }

        bucket = bucketIt->second;
        ++it;
    }

    return bucket;
}

template <typename TTag>
TFairShareHierarchicalScheduler<TTag>::TFairShareHierarchicalSlotQueueBucketNodePtr
TFairShareHierarchicalScheduler<TTag>::GetOrCreateChild(
    const TFairShareHierarchicalScheduler<TTag>::TFairShareHierarchicalSlotQueueBucketNodePtr& parent,
    const TTag& tag)
{
    YT_ASSERT_SPINLOCK_AFFINITY(parent->BucketLock);

    auto bucketIt = parent->Buckets.find(tag);

    if (bucketIt == parent->Buckets.end()) {
        // If not, create a new child node.
        auto newBucket = New<TFairShareHierarchicalSlotQueueBucketNode>();
        EmplaceOrCrash(parent->Buckets, tag, newBucket);
        BucketCount_.fetch_add(1);
        return newBucket;
    } else {
        // If it exists, return the existing child node.
        return bucketIt->second;
    }
}

template <typename TTag>
bool TFairShareHierarchicalScheduler<TTag>::CompareSlots(
    const TFairShareHierarchicalSlotQueueSlotPtr<TTag>& lhs,
    const TFairShareHierarchicalSlotQueueSlotPtr<TTag>& rhs,
    bool isSlot) const
{
    NProfiling::TEventTimerGuard timer(CompareWallTimer_);

    auto lhsBucket = RootBucket_;
    auto rhsBucket = RootBucket_;

    auto& lhsLevels = lhs->GetLevels();
    auto& rhsLevels = rhs->GetLevels();

    auto lhsLevelIt = lhs->GetLevels().begin();
    auto rhsLevelIt = rhs->GetLevels().begin();

    TFairShareHierarchicalSlotQueueBucketNodePtr newLhsBucket = nullptr;
    TFairShareHierarchicalSlotQueueBucketNodePtr newRhsBucket = nullptr;

    i64 lhsStream = 0;
    i64 rhsStream = 0;

    // At each step, for the tags of two elements of the same level,
    // we perform a consumption comparison.
    while (true) {
        if (lhsLevelIt == lhsLevels.end() || rhsLevelIt == rhsLevels.end()) {
            // If the tags are over, then we have reached the end,
            // in which case we can compare the elements only in the order of insertion.
            return CompareByEnqueueTime(lhs, rhs);
        } else {
            auto& lhsLevel = *lhsLevelIt;
            auto& rhsLevel = *rhsLevelIt;

            {
                auto lhsGuard = ReaderGuard(lhsBucket->BucketLock);
                auto lhsBucketChildIt = lhsBucket->Buckets.find(lhsLevel.GetTag());
                newLhsBucket = lhsBucketChildIt == lhsBucket->Buckets.end()
                    ? nullptr
                    : lhsBucketChildIt->second;
                lhsStream = newLhsBucket == nullptr
                    ? 0
                    : (isSlot ? newLhsBucket->SlotWindowSize.load() : newLhsBucket->RequestWindowSize.load());
            }

            {
                auto rhsGuard = ReaderGuard(lhsBucket->BucketLock);
                auto rhsBucketChildIt = rhsBucket->Buckets.find(rhsLevel.GetTag());
                newRhsBucket = rhsBucketChildIt == rhsBucket->Buckets.end()
                    ? nullptr
                    : rhsBucketChildIt->second;
                rhsStream = newRhsBucket == nullptr
                    ? 0
                    : (isSlot ? newRhsBucket->SlotWindowSize.load() : newRhsBucket->RequestWindowSize.load());
            }

            if (lhsStream == 0 && rhsStream == 0) {
                // If both streams are zero, compare by enqueue time.
                return CompareByEnqueueTime(lhs, rhs);
            }

            // Calculate the weighted consumed resources for comparison.
            auto left = rhsStream * lhsLevel.GetWeight();
            auto right = lhsStream * rhsLevel.GetWeight();

            if (left < right) {
                return true;
            } else if (right < left) {
                return false;
            } else {
                lhsBucket = newLhsBucket;
                rhsBucket = newRhsBucket;

                // If the weighted resources are equal, move to the next level.
                ++lhsLevelIt;
                ++rhsLevelIt;
            }
        }
    }
}

template <typename TTag>
void TFairShareHierarchicalScheduler<TTag>::UpdateCounters(
    bool isEnqueue,
    bool isSlot,
    i64 size,
    double weight,
    const TFairShareHierarchicalSlotQueueBucketNodePtr& currentParent,
    const TFairShareHierarchicalSlotQueueBucketNodePtr& root,
    const TFairShareHierarchicalSlotQueueBucketNodePtr& current) const
{
    if (isSlot) {
        current->SlotWindowLogCount.fetch_add(isEnqueue ? 1 : -1);
        current->SlotWindowSize.fetch_add(isEnqueue ? size : -size);
        current->SummarySlotWeight.fetch_add(isEnqueue ? weight : -weight);
    } else {
        current->RequestWindowLogCount.fetch_add(isEnqueue ? 1 : -1);
        current->RequestWindowSize.fetch_add(isEnqueue ? size : -size);
        current->SummaryRequestWeight.fetch_add(isEnqueue ? weight : -weight);
    }

    if (!currentParent) {
        return;
    }

    if (isSlot) {
        auto currentWindowStream = current->SlotWindowSize.load();
        auto parentWindowStream = currentParent->SlotWindowSize.load();
        auto rootWindowStream = root->SlotWindowSize.load();

        if (parentWindowStream > 0 &&
            rootWindowStream > 0)
        {
            current->SlotFairShare.store(1.0 * currentWindowStream / parentWindowStream);
            current->SlotTotalFairShare.store(1.0 * currentWindowStream / rootWindowStream);
        }
    } else {
        auto currentWindowStream = current->RequestWindowSize.load();
        auto parentWindowStream = currentParent->RequestWindowSize.load();
        auto rootWindowStream = root->RequestWindowSize.load();

        if (parentWindowStream > 0 &&
            rootWindowStream > 0)
        {
            current->RequestFairShare.store(1.0 * currentWindowStream / parentWindowStream);
            current->RequestTotalFairShare.store(1.0 * currentWindowStream / rootWindowStream);
        }
    }
}

template <typename TTag>
void TFairShareHierarchicalScheduler<TTag>::UpdateBuckets(
    const std::vector<TFairShareHierarchicalSlotQueueBucketNodePtr>& touchedParents,
    const std::vector<TFairShareHierarchicalSlotQueueBucketNodePtr>& touchedBuckets,
    const std::vector<TTag>& touchedTags,
    const std::vector<double>& weights,
    const TFairShareHierarchicalSlotQueueBucketNodePtr& root,
    i64 size,
    bool isEnqueue,
    bool isSlot)
{
    UpdateCounters(
        isEnqueue,
        isSlot,
        size,
        1.0,
        nullptr,
        root,
        RootBucket_);

    for (int index = touchedBuckets.size() - 1; index >= 0; --index) {
        auto& bucket = touchedBuckets[index];
        auto& parent = touchedParents[index];
        auto& tag = touchedTags[index];
        auto weight = weights[index];

        UpdateCounters(
            isEnqueue,
            isSlot,
            size,
            weight,
            parent,
            root,
            bucket);

        // If there are no items left in the bucket and no one holds a link to it,
        // then you need to delete this bucket.
        auto guard = WriterGuard(parent->BucketLock);
        bucket->RefCount.fetch_sub(1);
        if (bucket->RefCount.load() == 0 &&
            bucket->SlotWindowLogCount == 0 &&
            bucket->RequestWindowLogCount == 0) {
            EraseOrCrash(parent->Buckets, tag);
            BucketCount_.fetch_sub(1);
        }
    }
}

template <typename TTag>
void TFairShareHierarchicalScheduler<TTag>::EnqueueLog(const TFairShareHierarchicalSchedulerLogPtr<TTag>& log)
{
    NProfiling::TEventTimerGuard timer(EnqueueLogWallTimer_);

    auto& levels = log->Levels();
    auto it = levels.begin();
    auto bucket = RootBucket_;
    TFairShareHierarchicalSlotQueueBucketNodePtr parent = nullptr;
    TFairShareHierarchicalSlotQueueBucketNodePtr nextBucket = nullptr;

    std::vector<TFairShareHierarchicalSlotQueueBucketNodePtr> touchedParents;
    std::vector<TFairShareHierarchicalSlotQueueBucketNodePtr> touchedBuckets;
    std::vector<TTag> touchedTags;
    std::vector<double> weights;

    while (it != levels.end()) {
        auto& level = *it;

        {
            // We go down the tree and create buckets if there aren't any yet.
            auto guard = WriterGuard(bucket->BucketLock);
            nextBucket = GetOrCreateChild(bucket, level.GetTag());
            nextBucket->RefCount.fetch_add(1);
        }

        touchedBuckets.push_back(nextBucket);
        touchedParents.push_back(bucket);
        touchedTags.push_back(level.GetTag());
        weights.push_back(level.GetWeight());

        parent = bucket;
        bucket = nextBucket;
        ++it;
    }

    UpdateBuckets(
        touchedParents,
        touchedBuckets,
        touchedTags,
        weights,
        RootBucket_,
        log->GetSize(),
        /*isEnqueue*/ true,
        log->IsSlot());

    {
        // Add the log to the history.
        auto guard = WriterGuard(log->IsSlot() ? SlotHistoryLock_ : RequestHistoryLock_);
        EmplaceOrCrash(
            log->IsSlot() ? SlotHistory_ : RequestHistory_,
            TFairShareLogKey{
                .RequestId = log->GetRequestId(),
                .CreatedAt = log->GetCreatedAt(),
            },
            std::move(log));
    }
}

template <typename TTag>
void TFairShareHierarchicalScheduler<TTag>::DoDequeue(const TFairShareHierarchicalSchedulerLogPtr<TTag>& log)
{
    auto& levels = log->Levels();
    auto it = levels.begin();
    auto bucket = RootBucket_;
    TFairShareHierarchicalSlotQueueBucketNodePtr parent = nullptr;
    TFairShareHierarchicalSlotQueueBucketNodePtr nextBucket = nullptr;

    std::vector<TFairShareHierarchicalSlotQueueBucketNodePtr> touchedParents;
    std::vector<TFairShareHierarchicalSlotQueueBucketNodePtr> touchedBuckets;
    std::vector<TTag> touchedTags;
    std::vector<double> weights;

    while (it != levels.end()) {
        auto& level = *it;

        {
            // Going down the tree, it is guaranteed that a bucket always
            // exists for the element being deleted from the log.
            auto guard = WriterGuard(bucket->BucketLock);
            nextBucket = GetOrCrash(bucket->Buckets, level.GetTag());
            nextBucket->RefCount.fetch_add(1);
        }

        touchedBuckets.push_back(nextBucket);
        touchedParents.push_back(bucket);
        touchedTags.push_back(level.GetTag());
        weights.push_back(level.GetWeight());

        // Move to the child bucket for the current level.
        parent = bucket;
        bucket = nextBucket;
        ++it;
    }

    UpdateBuckets(
        touchedParents,
        touchedBuckets,
        touchedTags,
        weights,
        RootBucket_,
        log->GetSize(),
        /*isEnqueue*/ false,
        log->IsSlot());
}

template <typename TTag>
void TFairShareHierarchicalScheduler<TTag>::DequeueLog(const TFairShareLogKey& key)
{
    NProfiling::TEventTimerGuard timer(DequeueLogWallTimer_);

    TFairShareHierarchicalSchedulerLogPtr<TTag> log = nullptr;
    {
        auto guard = WriterGuard(SlotHistoryLock_);
        auto it = SlotHistory_.find(key);

        if (it == SlotHistory_.end()) {
            // In this case, the log may have already been deleted if a trim has occurred.
            return;
        }

        log = it->second;
        EraseOrCrash(SlotHistory_, key);

        // This method of explicit deletion from history can only be used for slot logs.
        YT_VERIFY(log->IsSlot());
    }

    DoDequeue(log);
}

template <typename TTag>
void TFairShareHierarchicalScheduler<TTag>::TrimLog()
{
    NProfiling::TEventTimerGuard timer(TrimLogWallTimer_);

    auto now = TInstant::Now();
    auto windowSize = Config_.Acquire()->WindowSize;

    auto trim = [&] (auto& history, auto& lock) {
        // Iterate through the items and remove those that have expired.
        while (true) {
            TFairShareHierarchicalSchedulerLogPtr<TTag> log = nullptr;
            bool needDelete = false;
            {
                auto guard = WriterGuard(lock);

                if (history.empty()) {
                    return;
                }

                auto it = history.end();
                --it;
                log = it->second;

                needDelete = (log->GetCreatedAt() + windowSize) < now;

                if (!needDelete) {
                    return;
                }

                EraseOrCrash(
                    history,
                    TFairShareLogKey{
                        .RequestId = log->GetRequestId(),
                        .CreatedAt = log->GetCreatedAt(),
                    });
            }

            DoDequeue(log);
        }
    };

    trim(SlotHistory_, SlotHistoryLock_);
    trim(RequestHistory_, RequestHistoryLock_);
}

template <typename TTag>
void TFairShareHierarchicalScheduler<TTag>::Reconfigure(const TFairShareHierarchicalSchedulerDynamicConfigPtr& config)
{
    Config_.Store(config);
}

////////////////////////////////////////////////////////////////////////////////

template <typename TTag>
TFairShareHierarchicalSlotQueue<TTag>::TFairShareHierarchicalSlotQueue(
    TFairShareHierarchicalSchedulerPtr<TTag> hierarchicalScheduler,
    NProfiling::TProfiler profiler)
    : HierarchicalScheduler_(std::move(hierarchicalScheduler))
    , Profiler_(profiler.WithHot())
{
    EnqueueSlotSizeCounter_ = profiler.Counter("/enqueue_slot_size");
    AccountSlotSizeCounter_ = profiler.Counter("/account_slot_size");
    PreemptSlotSizeCounter_ = profiler.Counter("/preempt_slot_size");

    EnqueueSlotCountCounter_ = profiler.Counter("/enqueue_slot_count");
    AccountSlotCountCounter_ = profiler.Counter("/account_slot_count");
    PreemptSlotCountCounter_ = profiler.Counter("/preempt_count");

    profiler.AddFuncGauge("/slot_size", MakeStrong(this), [this] {
        return SlotSize_.load();
    });

    profiler.AddFuncGauge("/slot_count", MakeStrong(this), [this] {
        return SlotCount_.load();
    });

    ExecTimer_ = profiler.Timer("/time/wait");

    EnqueueSlotWallTimer_ = profiler.Timer("/time/enqueue_slot_wall_time");
    DequeueSlotWallTimer_ = profiler.Timer("/time/dequeue_slot_wall_time");
    PeekSlotWallTimer_ = profiler.Timer("/time/peek_slot_wall_time");
}

// Checks if the queue is empty.
template <typename TTag>
bool TFairShareHierarchicalSlotQueue<TTag>::IsEmpty() const
{
    auto guard = Guard(QueueLock_);
    return Queue_.empty();
}

template <typename TTag>
TErrorOr<TFairShareHierarchicalSlotQueueSlotPtr<TTag>> TFairShareHierarchicalSlotQueue<TTag>::EnqueueSlot(
    i64 size,
    std::vector<IFairShareHierarchicalSlotQueueResourcePtr> resources,
    std::vector<TFairShareHierarchyLevel<TTag>> levels)
{
    NProfiling::TEventTimerGuard timer(EnqueueSlotWallTimer_);

    // Create a new queue slot.
    auto slot = New<TFairShareHierarchicalSlotQueueSlot<TTag>>(
        size,
        std::move(resources),
        std::move(levels));

    // Check if the slot exceeds resource limits.
    if (slot->NeedExceedsLimit()) {
        return TError("Slot exceeds limit");
    }

    const auto preempt = [&] (i64 size, TError error) {
        PreemptSlotSizeCounter_.Increment(size);
        PreemptSlotCountCounter_.Increment();
        return std::move(error);
    };

    while (true) {
        // Attempt to acquire resources for the slot.
        auto result = slot->AcquireResources();

        if (result.IsOK()) {
            // If resources are acquired, enqueue the slot.
            HierarchicalScheduler_->EnqueueLog(New<TFairShareHierarchicalSchedulerLog<TTag>>(
                slot->GetSlotId(),
                slot->GetSize(),
                /*isSlot*/ true,
                slot->GetLevels(),
                slot->GetEnqueueTime()));

            EnqueueSlotSizeCounter_.Increment(slot->GetSize());
            EnqueueSlotCountCounter_.Increment();

            SlotSize_.fetch_add(slot->GetSize());
            SlotCount_.fetch_add(1);

            auto guard = Guard(QueueLock_);
            EmplaceOrCrash(
                Queue_,
                TFairShareLogKey{
                    .RequestId = slot->GetSlotId(),
                    .CreatedAt = slot->GetEnqueueTime(),
                },
                slot);
            return slot;
        } else {
            // Trim expired slots from the queue.
            HierarchicalScheduler_->TrimLog();
            auto guard = Guard(QueueLock_);

            if (Queue_.empty()) {
                guard.Release();
                // If the queue is empty and resources cannot be acquired, return an error.
                return preempt(slot->GetSize(), std::move(result));
            }

            // Find the slot with the lowest priority.
            auto minSlot = slot;
            for (const auto& [_, currentSlot] : Queue_) {
                if (HierarchicalScheduler_->CompareSlots(currentSlot, minSlot, /*isSlot*/ true)) {
                    minSlot = currentSlot;
                }
            }

            // If the new slot has higher priority, preempt the lowest priority slot.
            if (minSlot->GetSlotId() != slot->GetSlotId()) {
                EraseOrCrash(
                    Queue_,
                    TFairShareLogKey{
                        .RequestId = minSlot->GetSlotId(),
                        .CreatedAt = minSlot->GetEnqueueTime(),
                    });
                guard.Release();

                SlotCount_ -= 1;
                SlotSize_ -= minSlot->GetSize();

                HierarchicalScheduler_->DequeueLog(
                    TFairShareLogKey{
                        .RequestId = minSlot->GetSlotId(),
                        .CreatedAt = minSlot->GetEnqueueTime(),
                    });
                minSlot->ReleaseResources();

                // Cancel the preempted slot.
                minSlot->Cancel(preempt(minSlot->GetSize(), std::move(result)));
            } else {
                guard.Release();
                // If the new slot does not have higher priority, return an error.
                return preempt(slot->GetSize(), std::move(result));
            }
        }
    }
}

template <typename TTag>
void TFairShareHierarchicalSlotQueue<TTag>::DequeueSlot(const TFairShareHierarchicalSlotQueueSlotPtr<TTag>& slot)
{
    NProfiling::TEventTimerGuard timer(DequeueSlotWallTimer_);

    {
        auto guard = Guard(QueueLock_);

        auto key = TFairShareLogKey{
            .RequestId = slot->GetSlotId(),
            .CreatedAt = slot->GetEnqueueTime(),
        };
        auto it = Queue_.find(key);

        if (it == Queue_.end()) {
            return;
        }

        SlotCount_ -= 1;
        SlotSize_ -= slot->GetSize();
        EraseOrCrash(
            Queue_,
            key);
    }

    ExecTimer_.Record(TInstant::Now() - slot->GetEnqueueTime());
}

template <typename TTag>
TFairShareHierarchicalSlotQueueSlotPtr<TTag> TFairShareHierarchicalSlotQueue<TTag>::PeekSlot(const THashSet<TFairShareSlotId>& slotFilter)
{
    NProfiling::TEventTimerGuard timer(PeekSlotWallTimer_);

    HierarchicalScheduler_->TrimLog();

    auto guard = Guard(QueueLock_);

    if (Queue_.empty()) {
        return nullptr;
    }

    TFairShareHierarchicalSlotQueueSlotPtr<TTag> maxSlot = nullptr;
    for (const auto& [_, slot] : Queue_) {
        if (!slotFilter.contains(slot->GetSlotId())) {
            continue;
        }

        if (!maxSlot) {
            maxSlot = slot;
            continue;
        }

        if (HierarchicalScheduler_->CompareSlots(maxSlot, slot, /*isSlot*/ false)) {
            maxSlot = slot;
        }
    }

    return maxSlot;
}

template <typename TTag>
void TFairShareHierarchicalSlotQueue<TTag>::AccountSlot(
    TFairShareHierarchicalSlotQueueSlotPtr<TTag> slot,
    i64 requestSize)
{
    TFairShareHierarchicalSchedulerLogPtr<TTag> itemForDequeue = New<TFairShareHierarchicalSchedulerLog<TTag>>(
        TGuid::Create(),
        requestSize,
        /*isSlot*/ false,
        slot->GetLevels(),
        TInstant::Now());

    AccountSlotSizeCounter_.Increment(requestSize);
    AccountSlotCountCounter_.Increment();

    HierarchicalScheduler_->EnqueueLog(std::move(itemForDequeue));
}

////////////////////////////////////////////////////////////////////////////////

template <typename TTag>
TFairShareHierarchicalSchedulerPtr<TTag> CreateFairShareHierarchicalScheduler(
    TFairShareHierarchicalSchedulerDynamicConfigPtr config,
    NProfiling::TProfiler profiler)
{
    return New<TFairShareHierarchicalScheduler<TTag>>(std::move(config), std::move(profiler));
}

template <typename TTag>
TFairShareHierarchicalSlotQueuePtr<TTag> CreateFairShareHierarchicalSlotQueue(
    TFairShareHierarchicalSchedulerPtr<TTag> hierarchicalScheduler,
    NProfiling::TProfiler profiler)
{
    return New<TFairShareHierarchicalSlotQueue<TTag>>(std::move(hierarchicalScheduler), std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
