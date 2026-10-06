#pragma once

#include <util/generic/deque.h>
#include <util/system/condvar.h>

#include <optional>
#include <utility>

namespace NYql {

// A bounded, thread-safe event queue used to bridge the YTsaurus pull API and
// the MessageStream event interface. Producers block once the accumulated
// byte size reaches MaxSize, providing backpressure toward the poller thread.
template <typename TEvent>
class TBlockingEQueue {
public:
    explicit TBlockingEQueue(size_t maxSize)
        : MaxSize(maxSize)
    {}

    void Push(TEvent&& e, size_t size = 0) {
        with_lock(Mutex) {
            // Wait until there is room. The predicate also returns true when the
            // queue is stopped so that a blocked Push is unblocked by Stop()
            // (which broadcasts CanPush) instead of waiting forever.
            CanPush.WaitI(Mutex, [this]() { return CanAcceptPush(); });
            if (Stopped) {
                return;
            }
            Events.emplace_back(std::move(e), size);
            Size += size;
        }
        CanPop.BroadCast();
    }

    // Control events must not wait for the data consumer to release capacity.
    void PushControl(TEvent&& event) {
        with_lock(Mutex) {
            if (Stopped) {
                return;
            }
            Events.emplace_back(std::move(event), 0);
        }
        CanPop.BroadCast();
    }

    // Non-blocking push: returns true if the event was enqueued, false if
    // the queue is full or already stopped. Useful for close events that
    // must not block on backpressure.
    bool TryPush(TEvent&& e, size_t size = 0) {
        with_lock(Mutex) {
            if (Stopped || !CanPushPredicate()) {
                return false;
            }
            Events.emplace_back(std::move(e), size);
            Size += size;
        }
        CanPop.BroadCast();
        return true;
    }

    bool CanPushPredicate() {
        return !Stopped && Size < MaxSize;
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

            return std::move(front);
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

    // Predicate for the blocking Push(): true when there is room, or when the
    // queue is stopped (so a blocked Push is released by Stop() rather than
    // waiting forever). This is intentionally different from the public
    // CanPushPredicate(), which reports whether a new push is allowed at all.
    bool CanAcceptPush() const {
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
