#pragma once

#include "mpsc_sharded_queue.h"

#include <yt/yt/core/threading/thread.h>

#include <yt/yt/core/profiling/public.h>

#include <yt/yt/core/misc/heap.h>
#include <yt/yt/core/misc/ring_queue.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NMpscFSQueue {

////////////////////////////////////////////////////////////////////////////////

template <typename TPoolId, typename TItem, typename TFairShareTag>
struct TEnqueuedTaskGeneric
{
    TItem Item;

    TPoolId PoolId = {};
    double PoolWeight = 1;
    TFairShareTag FairShareTag = {};
};

template <typename TPoolId, typename TItem, typename TFairShareTag>
struct THeapItemGeneric;

template <typename TPoolId, typename TItem, typename TFairShareTag>
struct TExecutionPoolGeneric;

template <typename TPoolId, typename TItem, typename TFairShareTag>
struct TBucketGeneric final
{
    using TExecutorPoolPtr = TIntrusivePtr<TExecutionPoolGeneric<TPoolId, TItem, TFairShareTag>>;
    using TEnqueuedTask = TEnqueuedTaskGeneric<TPoolId, TItem, TFairShareTag>;
    using THeapItem = THeapItemGeneric<TPoolId, TItem, TFairShareTag>;

    const TExecutorPoolPtr Pool;
    const TFairShareTag Tag;

    TRingQueue<TEnqueuedTask> Queue;
    THeapItem* HeapIterator = nullptr;
    TCpuDuration ExcessTime = 0;
    int RunningTaskCount = 0;

    TBucketGeneric(TExecutorPoolPtr pool, TFairShareTag tag);
};

////////////////////////////////////////////////////////////////////////////////

template <typename TPoolId, typename TItem, typename TFairShareTag>
struct THeapItemGeneric
{
    using TBucketPtr = TIntrusivePtr<TBucketGeneric<TPoolId, TItem, TFairShareTag>>;

    TBucketPtr Bucket;

    THeapItemGeneric(const THeapItemGeneric&) = delete;
    THeapItemGeneric& operator=(const THeapItemGeneric&) = delete;

    explicit THeapItemGeneric(TBucketPtr bucket);

    THeapItemGeneric(THeapItemGeneric&& other) noexcept;

    THeapItemGeneric& operator=(THeapItemGeneric&& other) noexcept;

    void AdjustBackReference(THeapItemGeneric* iterator);

    ~THeapItemGeneric();

    bool operator<(const THeapItemGeneric& rhs) const;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TPoolId, typename TItem, typename TFairShareTag>
struct TExecutionPoolGeneric final
{
    using TBucket = TBucketGeneric<TPoolId, TItem, TFairShareTag>;
    using TBucketPtr = TIntrusivePtr<TBucket>;

    using TEnqueuedTask = TEnqueuedTaskGeneric<TPoolId, TItem, TFairShareTag>;
    using THeapItem = THeapItemGeneric<TPoolId, TItem, TFairShareTag>;

    struct TExecutingTask
    {
        TEnqueuedTask Task;
        TBucketPtr Bucket;
        TCpuDuration LastAccounted = 0;
    };

    const TPoolId PoolId;
    double Weight = 1.0;

    TCpuDuration ExcessTime = 0;
    int RunningTaskCount = 0;

    std::vector<THeapItem> Heap;
    THashMap<TFairShareTag, TBucketPtr> TagToBucket;


    explicit TExecutionPoolGeneric(TPoolId poolId);

    std::optional<TExecutingTask> TryDequeue();

    TBucketPtr GetOrAddBucket(const TEnqueuedTask& scheduled, const TIntrusivePtr<TExecutionPoolGeneric>& pool);

    bool operator<(const TExecutionPoolGeneric& pool) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMpscFSQueue

template <typename TPoolId, typename TItem, typename TFairShareTag>
class TMpscFairShareQueue
{
public:
    using TEnqueuedTask = NMpscFSQueue::TEnqueuedTaskGeneric<TPoolId, TItem, TFairShareTag>;
    using TExecutionPool = NMpscFSQueue::TExecutionPoolGeneric<TPoolId, TItem, TFairShareTag>;
    using TExecutingTask = typename TExecutionPool::TExecutingTask;

    using TBucket = NMpscFSQueue::TBucketGeneric<TPoolId, TItem, TFairShareTag>;
    using TBucketPtr = TIntrusivePtr<TBucket>;
    using TExecutionPoolPtr = TIntrusivePtr<TExecutionPool>;

    ~TMpscFairShareQueue();

    void Enqueue(TEnqueuedTask task);

    void EnqueueMany(std::vector<TEnqueuedTask>&& tasks);

    void PrepareDequeue();

    TItem TryDequeue();

    void MarkFinished(const TItem& item, TCpuDuration finishedTime);

    int GetPoolCount();

    void Cleanup();

    int GetShardSize();

private:
    using TCookie = uintptr_t;

    TMpscShardedQueue<TEnqueuedTask> Prequeue_;

    THashMap<TPoolId, int> PoolIdToPoolIndex_;
    std::vector<TExecutionPoolPtr> Pools_;

    THashMap<TCookie, TExecutingTask> Executing_;
    int DequeCounter_ = 0;


    void MoveToFairQueue(std::vector<TEnqueuedTask>& tasks);
    int GetEmptyPoolIndex();
    const TExecutionPoolPtr& GetOrAddPool(const TEnqueuedTask& task);

    TExecutionPoolPtr GetStarvingPool();

    void UpdateExcessTime(TExecutingTask& executing, TCpuDuration now);

    void AccountCurrentlyExecutingBuckets();
    void TruncatePoolExcessTime();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define MPSC_FAIR_SHARE_QUEUE_INL_H_
#include "mpsc_fair_share_queue-inl.h"
#undef MPSC_FAIR_SHARE_QUEUE_INL_H_
