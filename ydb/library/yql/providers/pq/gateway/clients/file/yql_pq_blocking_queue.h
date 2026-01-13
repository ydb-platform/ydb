#pragma once

#include <util/generic/deque.h>
#include <util/system/condvar.h>

namespace NYql {

template <typename TEvent>
class TBlockingEQueue {
public:
    explicit TBlockingEQueue(size_t maxSize)
        : MaxSize(maxSize)
    {}

    void Push(TEvent&& e, size_t size = 0) {
        with_lock(Mutex) {
            CanPush.WaitI(Mutex, [this]() { return CanPushPredicate(); });
            Events.emplace_back(std::move(e), size);
            Size += size;
        }
        CanPop.BroadCast();
    }

    void BlockUntilEvent() {
        with_lock(Mutex) {
            CanPop.WaitI(Mutex, [this]() { return CanPopPredicate(); });
        }
    }

    std::optional<TEvent> Pop(bool block) {
        with_lock(Mutex) {
            if (block) {
                CanPop.WaitI(Mutex, [this]() { return CanPopPredicate(); });
            } else {
                if (!CanPopPredicate()) {
                    return {};
                }
            }

            if (Events.empty()) {
                return {};
            }

            auto [front, size] = std::move(Events.front());
            Events.pop_front();
            Size -= size;
            if (Size < MaxSize) {
                CanPush.BroadCast();
            }

            return std::move(front); // cast to std::optional<>
        }
    }

    void Stop() {
        with_lock(Mutex) {
            Stopped = true;
            CanPop.BroadCast();
            CanPush.BroadCast();
        }
    }

    bool IsStopped() {
        with_lock(Mutex) {
            return Stopped;
        }
    }

private:
    bool CanPopPredicate() const {
        return !Events.empty() || Stopped;
    }

    bool CanPushPredicate() const {
        return Size < MaxSize || Stopped;
    }

    const size_t MaxSize;
    size_t Size = 0;
    TDeque<std::pair<TEvent, size_t>> Events;
    bool Stopped = false;
    TMutex Mutex;
    TCondVar CanPop;
    TCondVar CanPush;
};

} // namespace NYql
