#ifndef MPSC_FAIR_SHARE_QUEUE_INL_H_
#error "Direct inclusion of this file is not allowed, include mpsc_fair_share_queue.h"
// For the sake of sane code completion.
#include "mpsc_fair_share_queue.h"
#endif

#include <type_traits>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NMpscFSQueue {

////////////////////////////////////////////////////////////////////////////////

using TCookie = uintptr_t;

// For yandex smart pointer types
template <typename T>
    requires requires(T t) { t.Get(); }
TCookie ToCookie(const T& value)
{
    return reinterpret_cast<TCookie>(value.Get());
}

// For std smart pointers
template <typename T>
    requires requires(T t) { t.get(); }
TCookie ToCookie(const T& value)
{
    return reinterpret_cast<TCookie>(value.get());
}

// For types that are directly convertible to uintptr_t, like integral and pointer
template <typename T>
    requires std::is_convertible_v<T, TCookie>
TCookie ToCookie(T value)
{
    return value;
}

////////////////////////////////////////////////////////////////////////////////

template <typename TPoolId, typename TItem, typename TFairShareTag>
TBucketGeneric<TPoolId, TItem, TFairShareTag>::TBucketGeneric(TExecutorPoolPtr pool, TFairShareTag tag)
    : Pool(std::move(pool))
    , Tag(std::move(tag))
{ }

////////////////////////////////////////////////////////////////////////////////

template <typename TPoolId, typename TItem, typename TFairShareTag>
THeapItemGeneric<TPoolId, TItem, TFairShareTag>::THeapItemGeneric(TBucketPtr bucket)
    : Bucket(std::move(bucket))
{
    AdjustBackReference(this);
}

template <typename TPoolId, typename TItem, typename TFairShareTag>
THeapItemGeneric<TPoolId, TItem, TFairShareTag>::THeapItemGeneric(THeapItemGeneric&& other) noexcept
    : Bucket(std::move(other.Bucket))
{
    AdjustBackReference(this);
}

template <typename TPoolId, typename TItem, typename TFairShareTag>
auto THeapItemGeneric<TPoolId, TItem, TFairShareTag>::operator=(THeapItemGeneric&& other) noexcept -> THeapItemGeneric&
{
    Bucket = std::move(other.Bucket);
    AdjustBackReference(this);

    return *this;
}

template <typename TPoolId, typename TItem, typename TFairShareTag>
void THeapItemGeneric<TPoolId, TItem, TFairShareTag>::AdjustBackReference(THeapItemGeneric* iterator)
{
    if (Bucket) {
        Bucket->HeapIterator = iterator;
    }
}

template <typename TPoolId, typename TItem, typename TFairShareTag>
THeapItemGeneric<TPoolId, TItem, TFairShareTag>::~THeapItemGeneric()
{
    if (Bucket) {
        Bucket->HeapIterator = nullptr;
    }
}

template <typename TPoolId, typename TItem, typename TFairShareTag>
bool THeapItemGeneric<TPoolId, TItem, TFairShareTag>::operator<(const THeapItemGeneric& rhs) const
{
    return std::tie(Bucket->ExcessTime, Bucket->RunningTaskCount) <
        std::tie(rhs.Bucket->ExcessTime, rhs.Bucket->RunningTaskCount);
}

////////////////////////////////////////////////////////////////////////////////

template <typename TPoolId, typename TItem, typename TFairShareTag>
TExecutionPoolGeneric<TPoolId, TItem, TFairShareTag>::TExecutionPoolGeneric(TPoolId poolId)
    : PoolId(std::move(poolId))
{ }

template <typename TPoolId, typename TItem, typename TFairShareTag>
auto TExecutionPoolGeneric<TPoolId, TItem, TFairShareTag>::TryDequeue() -> std::optional<TExecutingTask>
{
    auto now = GetCpuInstant();
    std::optional<TExecutingTask> result;
    while (!Heap.empty() && !result) {
        const auto& bucket = Heap.front().Bucket;

        if (!bucket->Queue.empty()) {
            result = TExecutingTask{
                std::move(bucket->Queue.front()),
                bucket,
                now,
            };

            ++bucket->RunningTaskCount;
            ++RunningTaskCount;

            bucket->Queue.pop();
        }

        if (bucket->Queue.empty()) {
            ExtractHeap(Heap.begin(), Heap.end());
            Heap.pop_back();
        }
    }

    return result;
}

template <typename TPoolId, typename TItem, typename TFairShareTag>
auto TExecutionPoolGeneric<TPoolId, TItem, TFairShareTag>::GetOrAddBucket(
    const TEnqueuedTask& scheduled,
    const TIntrusivePtr<TExecutionPoolGeneric>& pool) -> TBucketPtr
{
    TBucketPtr bucket;

    auto it = TagToBucket.find(scheduled.FairShareTag);
    if (it != TagToBucket.end()) {
        bucket = it->second;
        YT_VERIFY(bucket);
    } else {
        bucket = New<TBucket>(pool, scheduled.FairShareTag);
        if (!Heap.empty()) {
            bucket->ExcessTime = Heap.front().Bucket->ExcessTime;
        }
    }

    if (!bucket->HeapIterator) {
        TagToBucket[scheduled.FairShareTag] = bucket;
        Heap.emplace_back(bucket);
        AdjustHeapBack(Heap.begin(), Heap.end());
    }
    YT_VERIFY(bucket->HeapIterator);

    return bucket;
}

template <typename TPoolId, typename TItem, typename TFairShareTag>
bool TExecutionPoolGeneric<TPoolId, TItem, TFairShareTag>::operator<(const TExecutionPoolGeneric& pool) const
{
    return std::tie(ExcessTime, RunningTaskCount) < std::tie(pool.ExcessTime, pool.RunningTaskCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMpscFSQueue

template <typename TPoolId, typename TItem, typename TFairShareTag>
TMpscFairShareQueue<TPoolId, TItem, TFairShareTag>::~TMpscFairShareQueue()
{
    Cleanup();
}

template <typename TPoolId, typename TItem, typename TFairShareTag>
void TMpscFairShareQueue<TPoolId, TItem, TFairShareTag>::Enqueue(TEnqueuedTask task)
{
    Prequeue_.Enqueue(std::move(task));
}

template <typename TPoolId, typename TItem, typename TFairShareTag>
void TMpscFairShareQueue<TPoolId, TItem, TFairShareTag>::EnqueueMany(std::vector<TEnqueuedTask>&& tasks)
{
    Prequeue_.EnqueueMany(std::move(tasks));
}

template <typename TPoolId, typename TItem, typename TFairShareTag>
void TMpscFairShareQueue<TPoolId, TItem, TFairShareTag>::PrepareDequeue()
{
    AccountCurrentlyExecutingBuckets();
    TruncatePoolExcessTime();
    Prequeue_.ConsumeAll([&] (auto& batch) {
        MoveToFairQueue(batch);
    });
}
template <typename TPoolId, typename TItem, typename TFairShareTag>
void TMpscFairShareQueue<TPoolId, TItem, TFairShareTag>::TruncatePoolExcessTime()
{
    auto minPool = GetStarvingPool();
    if (!minPool) {
        return;
    }

    for (const auto& pool : Pools_) {
        if (pool) {
            pool->ExcessTime = std::max<TCpuDuration>(0, pool->ExcessTime - minPool->ExcessTime);
        }
    }
}

template <typename TPoolId, typename TItem, typename TFairShareTag>
TItem TMpscFairShareQueue<TPoolId, TItem, TFairShareTag>::TryDequeue()
{
    auto pool = GetStarvingPool();
    if (!pool) {
        return {};
    }

    if (auto runningTask = pool->TryDequeue(); runningTask) {
        auto result = std::move(runningTask->Task.Item);
        auto cookie = NMpscFSQueue::ToCookie(result);
        Executing_[cookie] = std::move(*runningTask);
        return result;
    }
    return {};
}

template <typename TPoolId, typename TItem, typename TFairShareTag>
void TMpscFairShareQueue<TPoolId, TItem, TFairShareTag>::MarkFinished(const TItem& item, TCpuDuration finishedTime)
{
    auto cookie = NMpscFSQueue::ToCookie(item);
    auto it = Executing_.find(cookie);
    YT_VERIFY(it != Executing_.end());
    const auto& bucket = it->second.Bucket;

    YT_VERIFY(--bucket->RunningTaskCount >= 0);
    YT_VERIFY(--bucket->Pool->RunningTaskCount >= 0);

    UpdateExcessTime(it->second, finishedTime);

    Executing_.erase(it);

    static constexpr int CleanupPeriod = 50'000;

    if (++DequeCounter_ == CleanupPeriod) {
        Cleanup();
        DequeCounter_ = 0;
    }
}

template <typename TPoolId, typename TItem, typename TFairShareTag>
int TMpscFairShareQueue<TPoolId, TItem, TFairShareTag>::GetPoolCount()
{
    int count = 0;
    for (const auto& pool : Pools_) {
        if (pool) {
            ++count;
        }
    }

    return count;
}

template <typename TPoolId, typename TItem, typename TFairShareTag>
void TMpscFairShareQueue<TPoolId, TItem, TFairShareTag>::Cleanup()
{
    for (auto& pool : Pools_) {
        if (!pool) {
            continue;
        }

        std::vector<TFairShareTag> erasedTags;

        for (const auto& [tag, bucket] : pool->TagToBucket) {
            if (bucket->RunningTaskCount == 0 && bucket->Queue.empty()) {
                YT_VERIFY(!bucket->HeapIterator);
                erasedTags.push_back(tag);
            }
        }

        for (const auto& tag : erasedTags) {
            pool->TagToBucket.erase(tag);
        }

        if (pool->TagToBucket.empty()) {
            YT_VERIFY(pool->RunningTaskCount == 0);
            PoolIdToPoolIndex_.erase(pool->PoolId);
            pool.Reset();
        }
    }
}

template <typename TPoolId, typename TItem, typename TFairShareTag>
int TMpscFairShareQueue<TPoolId, TItem, TFairShareTag>::GetShardSize()
{
    return Prequeue_.GetShardSize();
}

template <typename TPoolId, typename TItem, typename TFairShareTag>
void TMpscFairShareQueue<TPoolId, TItem, TFairShareTag>::MoveToFairQueue(std::vector<TEnqueuedTask>& tasks)
{
    for (auto& task : tasks) {
        const auto& pool = GetOrAddPool(task);
        auto bucket = pool->GetOrAddBucket(task, pool);
        bucket->Queue.push(std::move(task));
    }
}

template <typename TPoolId, typename TItem, typename TFairShareTag>
int TMpscFairShareQueue<TPoolId, TItem, TFairShareTag>::GetEmptyPoolIndex()
{
    for (int index = 0; index <  std::ssize(Pools_); ++index) {
        if (!Pools_[index]) {
            return index;
        }
    }

    Pools_.emplace_back();
    return std::ssize(Pools_) - 1;
}

template <typename TPoolId, typename TItem, typename TFairShareTag>
auto TMpscFairShareQueue<TPoolId, TItem, TFairShareTag>::GetOrAddPool(const TEnqueuedTask& task) -> const TExecutionPoolPtr&
{
    auto it = PoolIdToPoolIndex_.find(task.PoolId);
    if (it == PoolIdToPoolIndex_.end()) {
        auto index = GetEmptyPoolIndex();
        Pools_[index] = New<TExecutionPool>(task.PoolId);
        it = PoolIdToPoolIndex_.emplace(task.PoolId, index).first;
    }

    const auto& pool = Pools_[it->second];
    pool->Weight = task.PoolWeight;
    return pool;
}

template <typename TPoolId, typename TItem, typename TFairShareTag>
auto TMpscFairShareQueue<TPoolId, TItem, TFairShareTag>::GetStarvingPool() -> TExecutionPoolPtr
{
    TExecutionPoolPtr result;

    for (const auto& pool : Pools_) {
        if (!pool || pool->Heap.empty()) {
            continue;
        }
        if (!result || *pool < *result) {
            result = pool;
        }
    }

    return result;
}

template <typename TPoolId, typename TItem, typename TFairShareTag>
void TMpscFairShareQueue<TPoolId, TItem, TFairShareTag>::UpdateExcessTime(TExecutingTask& executing, TCpuDuration now)
{
    auto delta = now - executing.LastAccounted;

    executing.Bucket->ExcessTime += delta;
    const auto& pool = executing.Bucket->Pool;
    pool->ExcessTime += delta / pool->Weight;

    executing.LastAccounted = now;

    auto heapIterator = executing.Bucket->HeapIterator;
    if (!heapIterator) {
        return;
    }

    auto heapIndex = heapIterator - pool->Heap.data();
    YT_VERIFY(heapIndex < std::ssize(pool->Heap));
    SiftDown(pool->Heap.begin(), pool->Heap.end(), pool->Heap.begin() + heapIndex, std::less<>());
}

template <typename TPoolId, typename TItem, typename TFairShareTag>
void TMpscFairShareQueue<TPoolId, TItem, TFairShareTag>::AccountCurrentlyExecutingBuckets()
{
    auto now = GetCpuInstant();

    for (auto& [_, executing] : Executing_) {
        UpdateExcessTime(executing, now);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
