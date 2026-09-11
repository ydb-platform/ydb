#pragma once

#include "cpu.h"
#include <util/system/spinlock.h>

#include <atomic>
#include <concepts>

template <class T, class TTag = void>
class TIntrusiveFunnelQueue;

// Hook for an allocation-free FIFO MPSC queue. An item may inherit from hooks
// with different tags to participate in several queues independently.
template <class T, class TTag = void>
class TIntrusiveFunnelQueueItem
{
    friend class TIntrusiveFunnelQueue<T, TTag>;

public:
    TIntrusiveFunnelQueueItem() noexcept = default;

    TIntrusiveFunnelQueueItem(const TIntrusiveFunnelQueueItem&) = delete;
    TIntrusiveFunnelQueueItem& operator=(const TIntrusiveFunnelQueueItem&) = delete;
    TIntrusiveFunnelQueueItem(TIntrusiveFunnelQueueItem&&) = delete;
    TIntrusiveFunnelQueueItem& operator=(TIntrusiveFunnelQueueItem&&) = delete;

private:
    std::atomic<T*> Next_ = nullptr;
};

// Multiple-producer single-consumer intrusive FIFO queue.
//
// Algorithm
// ---------
//
// Front points to the next item to consume and Back points to the last item
// reserved by a producer:
//
//     Front -> A -> B -> C <- Back
//
// A producer first exchanges Back with its item. The exchange totally orders
// producers and is the producer-side linearization point. The producer then
// publishes the item either through previous->Next, or through Front when the
// queue was empty. Consequently, there is a short intermediate state in which
// Back already points to a new item but the preceding item does not point to it
// yet:
//
//     Front -> A -> null       B <- Back
//
// The consumer distinguishes the last item by changing Back from Front to null.
// If that compare-exchange fails, a successor has already been reserved and the
// consumer waits until its producer publishes Front->Next. This is why the queue
// preserves FIFO order without locks, but is not lock-free as a whole: a producer
// suspended between reserving Back and publishing Next temporarily blocks the
// consumer.
//
// When removal of the last item races with a new push, Front is cleared with a
// compare-exchange. It either succeeds before the producer publishes the new
// front, or fails because that publication has already happened. In both cases
// the empty-to-nonempty transition is not lost. Push returns true for this
// transition so that a caller can wake a sleeping consumer.
//
// Memory ordering
// ---------------
//
// A producer initializes the item before the acq_rel exchange of Back, and then
// publishes it with a release store to Front or Next. The consumer uses acquire
// loads before reading the item. The acquire part of the Back exchange also lets
// a producer safely access the preceding item reserved by another producer.
//
// Ownership and lifetime
// ----------------------
//
// The queue performs no allocation and does not own its items. An item must stay
// alive from the beginning of Push until it is returned by Pop, and it must not
// be pushed twice concurrently. Pop detaches the returned hook, allowing the item
// to be destroyed or pushed again immediately.
//
// Pop calls must be serialized with each other. Destruction of the queue must be
// externally serialized with all Push and Pop calls.
template <class T, class TTag>
class TIntrusiveFunnelQueue
{
    using TItem = TIntrusiveFunnelQueueItem<T, TTag>;

public:
    TIntrusiveFunnelQueue() noexcept
    {
        static_assert(std::derived_from<T, TItem>);
    }

    TIntrusiveFunnelQueue(const TIntrusiveFunnelQueue&) = delete;
    TIntrusiveFunnelQueue& operator=(const TIntrusiveFunnelQueue&) = delete;
    TIntrusiveFunnelQueue(TIntrusiveFunnelQueue&&) = delete;
    TIntrusiveFunnelQueue& operator=(TIntrusiveFunnelQueue&&) = delete;

    // Returns true if the queue was empty before this push. This may be used to
    // wake the consumer on the empty-to-nonempty transition.
    bool Push(T* item) noexcept
    {
        item->Next_.store(nullptr, std::memory_order_relaxed);

        T* const previous = Back_.exchange(item, std::memory_order_acq_rel);
        if (previous) {
            previous->Next_.store(item, std::memory_order_release);
        } else {
            Front_.store(item, std::memory_order_release);
        }
        return !previous;
    }

    // Returns a detached item or nullptr. Must only be called by one consumer.
    T* Pop() noexcept
    {
        T* const front = Front_.load(std::memory_order_acquire);
        if (!front) {
            return nullptr;
        }

        T* expected = front;
        if (Back_.compare_exchange_strong(
            expected,
            nullptr,
            std::memory_order_acq_rel,
            std::memory_order_acquire))
        {
            expected = front;
            Front_.compare_exchange_strong(
                expected,
                nullptr,
                std::memory_order_acq_rel,
                std::memory_order_acquire);
        } else {
            T* next = nullptr;
            while (!(next = front->Next_.load(std::memory_order_acquire))) {
                SpinLockPause();
            }
            Front_.store(next, std::memory_order_release);
        }

        front->Next_.store(nullptr, std::memory_order_relaxed);
        return front;
    }

    bool IsEmpty() const noexcept
    {
        return Front_.load(std::memory_order_acquire) == nullptr;
    }

private:
    alignas(PLATFORM_CACHE_LINE) std::atomic<T*> Front_ = nullptr;
    alignas(PLATFORM_CACHE_LINE) std::atomic<T*> Back_ = nullptr;
};
