#pragma once

#include <util/generic/bitops.h>

#include <atomic>
#include <vector>

namespace NKikimr {

// Single-producer / single-consumer circular queue
template <typename T>
class TSpscCircularQueue {
public:
    TSpscCircularQueue()
        : Capacity(0)
        , CapacityMask(0)
    {
    }

    // Not safe to call concurrently with TryPush/TryPop; external synchronization required.
    // All items in the queue are lost.
    void Resize(size_t capacity) {
        Capacity = capacity ? FastClp2(capacity) : 0;
        CapacityMask = Capacity ? (Capacity - 1) : 0;
        Queue.resize(Capacity);
        ProducerState.Head.store(0, std::memory_order_release);
        ConsumerState.Tail.store(0, std::memory_order_release);
        ProducerState.CachedTail = 0;
        ConsumerState.CachedHead = 0;
    }

    bool TryPush(T&& item) {
        const auto head = ProducerState.Head.load(std::memory_order_relaxed);
        if (head - ProducerState.CachedTail == Capacity) {
            ProducerState.CachedTail = ConsumerState.Tail.load(std::memory_order_acquire);
            if (head - ProducerState.CachedTail == Capacity) {
                return false;
            }
        }

        Queue[head & CapacityMask] = std::move(item);
        ProducerState.Head.store(head + 1, std::memory_order_release);
        return true;
    }

    bool TryPop(T& item) {
        if (Capacity == 0) {
            return false;
        }

        const auto tail = ConsumerState.Tail.load(std::memory_order_relaxed);
        if (tail == ConsumerState.CachedHead) {
            ConsumerState.CachedHead = ProducerState.Head.load(std::memory_order_acquire);
            if (tail == ConsumerState.CachedHead) {
                return false;
            }
        }

        item = std::move(Queue[tail & CapacityMask]);
        ConsumerState.Tail.store(tail + 1, std::memory_order_release);
        return true;
    }

    size_t Size() const {
        const auto head = ProducerState.Head.load(std::memory_order_acquire);
        const auto tail = ConsumerState.Tail.load(std::memory_order_acquire);
        return head - tail;
    }

    bool Empty() const {
        return Size() == 0;
    }

    bool IsFull() const {
        if (Capacity == 0) {
            return false;
        }

        return Size() == Capacity;
    }

private:
    struct alignas(64) TProducerState
    {
        std::atomic<size_t> Head = 0;
        size_t CachedTail = 0;
    };

    struct alignas(64) TConsumerState
    {
        std::atomic<size_t> Tail = 0;
        size_t CachedHead = 0;
    };

    std::vector<T> Queue;
    size_t Capacity = 0;
    size_t CapacityMask = 0;
    TProducerState ProducerState;
    TConsumerState ConsumerState;
};

} // namespace NKikimr
