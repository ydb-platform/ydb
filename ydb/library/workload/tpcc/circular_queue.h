#pragma once

#include <vector>

namespace NYdb::NTPCC {

// TODO: it's unclear what is better to use here:
// * on one hand this requires lock (spinlock is just fine), but does no memory allocations
// * on another hand MPSC doesn't need lock, but allocates memory
template <typename T>
class TCircularQueue {
public:
    TCircularQueue()
        : FirstEmpty(0)
        , FirstUsed(0)
        , Size(0)
    {
    }

    void Resize(size_t capacity) {
        Queue.resize(capacity);
    }

    bool TryPush(T&& item) {
        if (Size == Queue.size()) {
            return false;
        }
        Queue[FirstEmpty] = std::move(item);
        FirstEmpty = (FirstEmpty + 1) % Queue.size();
        ++Size;
        return true;
    }

    bool TryPop(T& item) {
        if (Size == 0) {
            return false;
        }
        item = std::move(Queue[FirstUsed]);
        FirstUsed = (FirstUsed + 1) % Queue.size();
        --Size;
        return true;
    }

    size_t GetSize() const {
        return Size;
    }

    bool Empty() const {
        return Size == 0;
    }

    bool IsFull() const {
        return Size == Queue.size();
    }

private:
    std::vector<T> Queue;
    size_t FirstEmpty;
    size_t FirstUsed;
    size_t Size;
};

} // namespace NYdb::NTPCC
