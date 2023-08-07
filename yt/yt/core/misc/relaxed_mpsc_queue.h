#pragma once

#include <yt/yt/core/misc/public.h>

#include <library/cpp/yt/memory/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////
//
// A non-linearizable multiple-producer single-consumer queue.
//
// Based on http://www.1024cores.net/home/lock-free-algorithms/queues/non-intrusive-mpsc-node-based-queue
//
////////////////////////////////////////////////////////////////////////////////

struct TRelaxedMpscQueueHook
{
    std::atomic<TRelaxedMpscQueueHook*> Next = nullptr;
};

class TRelaxedMpscQueueBase
{
protected:
    TRelaxedMpscQueueBase();
    ~TRelaxedMpscQueueBase();

    //! Pushes the (detached) node to the queue.
    //! Node ownership is transferred to the queue.
    void EnqueueImpl(TRelaxedMpscQueueHook* node) noexcept;

    //! Pops and detaches a node from the queue. Node ownership is transferred to the caller.
    //! When null is returned then the queue is either empty or blocked.
    //! This method does not distinguish between these two cases.
    TRelaxedMpscQueueHook* TryDequeueImpl() noexcept;

private:
    alignas(CacheLineSize) TRelaxedMpscQueueHook Stub_;

    //! Producer-side.
    alignas(CacheLineSize) std::atomic<TRelaxedMpscQueueHook*> Head_;

    //! Consumer-side.
    alignas(CacheLineSize) TRelaxedMpscQueueHook* Tail_;
};

template <class T, TRelaxedMpscQueueHook T::*Hook>
class TRelaxedIntrusiveMpscQueue
    : public TRelaxedMpscQueueBase
{
public:
    TRelaxedIntrusiveMpscQueue() = default;
    TRelaxedIntrusiveMpscQueue(const TRelaxedIntrusiveMpscQueue&) = delete;
    TRelaxedIntrusiveMpscQueue(TRelaxedIntrusiveMpscQueue&&) = delete;

    ~TRelaxedIntrusiveMpscQueue();

    void Enqueue(std::unique_ptr<T> node);
    std::unique_ptr<T> TryDequeue();

private:
    static TRelaxedMpscQueueHook* HookFromNode(T* node) noexcept;
    static T* NodeFromHook(TRelaxedMpscQueueHook* hook) noexcept;
};

template <class T>
class TRelaxedMpscQueue
{
public:
    TRelaxedMpscQueue() = default;
    TRelaxedMpscQueue(const TRelaxedMpscQueue&) = delete;
    TRelaxedMpscQueue(TRelaxedMpscQueue&&) = delete;

    void Enqueue(T&& value);
    bool TryDequeue(T* value);

private:
    struct TNode
    {
        explicit TNode(T&& value);

        T Value;
        TRelaxedMpscQueueHook Hook;
    };

    TRelaxedIntrusiveMpscQueue<TNode, &TNode::Hook> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define RELAXED_MPSC_QUEUE_INL_H_
#include "relaxed_mpsc_queue-inl.h"
#undef RELAXED_MPSC_QUEUE_INL_H_

