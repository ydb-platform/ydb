#pragma once

#include <yt/yt/core/profiling/tscp.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <vector>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <typename TItem>
class TSimpleMpscSpinLockQueue
{
public:
    void Enqueue(TItem item);
    void EnqueueMany(std::vector<TItem>&& items);
    std::vector<TItem>& DequeueAll();

    int GetSize();

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, QueueLock_);
    std::vector<TItem> Queue_;

    std::vector<TItem> Dequeued_;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TItem>
class TMpscShardedQueue
{
public:
    void Enqueue(TItem item);
    void EnqueueMany(std::vector<TItem>&& items);

    // TConsumer is a functor with a signature: void (vector<TItem>& batch)
    template <typename TConsumer>
    i64 ConsumeAll(TConsumer consumer);

    int GetShardSize();

private:
    struct alignas(2 * CacheLineSize) TShard
    {
        TSimpleMpscSpinLockQueue<TItem> Queue;
    };

    std::array<TShard, NProfiling::TTscp::MaxProcessorId> Shards_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define MPSC_SHARDED_QUEUE_INL_H_
#include "mpsc_sharded_queue-inl.h"
#undef MPSC_SHARDED_QUEUE_INL_H_
