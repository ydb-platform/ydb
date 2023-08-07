#ifndef MPSC_SHARDED_QUEUE_INL_H_
#error "Direct inclusion of this file is not allowed, include mpsc_sharded_queue.h"
// For the sake of sane code completion.
#include "mpsc_sharded_queue.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <typename TItem>
void TSimpleMpscSpinLockQueue<TItem>::Enqueue(TItem item)
{
    auto guard = Guard(QueueLock_);
    Queue_.push_back(std::move(item));
}

template <typename TItem>
void TSimpleMpscSpinLockQueue<TItem>::EnqueueMany(std::vector<TItem>&& items)
{
    auto guard = Guard(QueueLock_);
    Queue_.insert(
        Queue_.end(),
        std::make_move_iterator(items.begin()),
        std::make_move_iterator(items.end()));
}

template <typename TItem>
std::vector<TItem>& TSimpleMpscSpinLockQueue<TItem>::DequeueAll()
{
    Dequeued_.clear();

    {
        auto guard = Guard(QueueLock_);
        Queue_.swap(Dequeued_);
    }

    return Dequeued_;
}

template <typename TItem>
int TSimpleMpscSpinLockQueue<TItem>::GetSize()
{
    auto guard = Guard(QueueLock_);
    return std::ssize(Queue_);
}

////////////////////////////////////////////////////////////////////////////////

template <typename TItem>
void TMpscShardedQueue<TItem>::Enqueue(TItem item)
{
    auto tscp = NProfiling::TTscp::Get();
    auto& shardQueue = Shards_[tscp.ProcessorId].Queue;
    shardQueue.Enqueue(std::move(item));
}

template <typename TItem>
void TMpscShardedQueue<TItem>::EnqueueMany(std::vector<TItem>&& items)
{
    auto tscp = NProfiling::TTscp::Get();
    auto& shardQueue = Shards_[tscp.ProcessorId].Queue;
    shardQueue.EnqueueMany(std::move(items));
}

template <typename TItem>
template <typename TConsumer>
i64 TMpscShardedQueue<TItem>::ConsumeAll(TConsumer consumer)
{
    i64 consumedCount = 0;

    for (auto& shard : Shards_) {
        auto& batch = shard.Queue.DequeueAll();

        consumedCount += std::ssize(batch);
        consumer(batch);
    }

    return consumedCount;
}

template <typename TItem>
int TMpscShardedQueue<TItem>::GetShardSize()
{
    auto tscp = NProfiling::TTscp::Get();
    auto& shardQueue = Shards_[tscp.ProcessorId].Queue;
    return shardQueue.GetSize();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
