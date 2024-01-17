#pragma once
#include <ydb/library/actors/core/events.h>

#include <concepts>
#include <queue>

namespace NKikimr {

/*
 * T is expected to provide the following methods:
 *
 * ui32 GetEventPriority(IEventHandle*);
 * void PushProcessIncomingEvent();
 * void PocessEvent(std::unique_ptr<IEventHandle>);
 *
 * This is not implemented as a concept, because the intended usage is storing a
 * TEventPriorityQueue<TSomeActor> inside TSomeActor, and that would require
 * checking the concept against incomplete type TSomeActor, and such checks
 * are not allowed to use any members
 */
template<typename T>
class TEventPriorityQueue {
private:
    class TInternalQueue {
    private:
        std::map<ui32, std::queue<std::unique_ptr<NActors::IEventHandle>>> Queue_;

    public:
        bool Empty() const {
            return Queue_.empty();
        }

        void Push(ui32 priority, std::unique_ptr<NActors::IEventHandle> ev) {
            Queue_[priority].push(std::move(ev));
        }

        std::unique_ptr<NActors::IEventHandle> Pop() {
            Y_ABORT_UNLESS(!Empty());
            auto it = Queue_.begin();
            auto& miniQueue = it->second;
            Y_ABORT_UNLESS(!miniQueue.empty());
            auto ev = std::move(miniQueue.front());
            miniQueue.pop();
            if (miniQueue.empty()) {
                Queue_.erase(it);
            }
            return ev;
        }
    };

    TInternalQueue EventQueue_;
    T& Actor_;

public:
    explicit TEventPriorityQueue(T& actor) : Actor_(actor) {}

    void EnqueueIncomingEvent(STATEFN_SIG) {
        const auto priority = Actor_.GetEventPriority(ev.Get());
        if (EventQueue_.Empty()) {
            Actor_.PushProcessIncomingEvent();
        }
        EventQueue_.Push(priority, std::unique_ptr<NActors::IEventHandle>(ev.Release()));
    }

    void ProcessIncomingEvent() {
        auto ev = EventQueue_.Pop();
        Actor_.ProcessEvent(std::move(ev));
        if (!EventQueue_.Empty()) {
            Actor_.PushProcessIncomingEvent();
        }
    }
};
} // NKikimr
