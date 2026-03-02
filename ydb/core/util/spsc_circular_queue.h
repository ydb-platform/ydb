#pragma once

#include <util/generic/bitops.h>

#include <atomic>
#include <vector>

namespace NKikimr {

template <typename T>
class TSpscCircularQueue {
public:
    TSpscCircularQueue()
        : FirstEmpty(0)
        , FirstUsed(0)
        , Capacity(0)
        , Size_(0)
    {
    }

    void Resize(size_t capacity) {
        Capacity = capacity ? FastClp2(capacity) : 0;
        Queue.resize(Capacity);
    }

    bool TryPush(T&& item) {
        const auto currentSize = Size_.load(std::memory_order_acquire);
        if (currentSize == Capacity) {
            return false;
        }

        Queue[FirstEmpty] = std::move(item);
        FirstEmpty = (FirstEmpty + 1) & (Capacity - 1);
        Size_.fetch_add(1, std::memory_order_release);
        return true;
    }

    bool TryPop(T& item) {
        const auto currentSize = Size_.load(std::memory_order_acquire);
        if (currentSize == 0) {
            return false;
        }

        item = std::move(Queue[FirstUsed]);
        FirstUsed = (FirstUsed + 1) & (Capacity - 1);
        Size_.fetch_sub(1, std::memory_order_release);
        return true;
    }

    size_t Size() const {
        return Size_.load(std::memory_order_acquire);
    }

    bool Empty() const {
        return Size_.load(std::memory_order_acquire) == 0;
    }

    bool IsFull() const {
        return Size_.load(std::memory_order_acquire) == Capacity;
    }

private:
    std::vector<T> Queue;
    size_t FirstEmpty;
    size_t FirstUsed;
    size_t Capacity = 0;
    std::atomic<size_t> Size_;
};

} // namespace NKikimr
