#pragma once
#include "async.h"
#include <ydb/library/actors/core/events.h>

namespace NActors {

    /**
     * Queue for low-priority async scheduling
     */
    class TAsyncLowPriorityQueue
        : private TActorRunnableItem::TImpl<TAsyncLowPriorityQueue>
    {
        friend TActorRunnableItem::TImpl<TAsyncLowPriorityQueue>;

        class [[nodiscard]] TAsyncLowPriorityQueueAwaiter;

    public:
        TAsyncLowPriorityQueue() = default;

        TAsyncLowPriorityQueue(TAsyncLowPriorityQueue&&) = delete;
        TAsyncLowPriorityQueue(const TAsyncLowPriorityQueue&) = delete;
        TAsyncLowPriorityQueue& operator=(TAsyncLowPriorityQueue&&) = delete;
        TAsyncLowPriorityQueue& operator=(const TAsyncLowPriorityQueue&) = delete;

        ~TAsyncLowPriorityQueue() {
            if (Event) {
                Event->Item = nullptr;
                Event = nullptr;
            }
            Y_ABORT_UNLESS(!Awaiters && !NextAwaiters,
                "Unexpected TAsyncLowPriorityQueue destruction before all awaiting coroutines");
        }

        /**
         * Will wait for the next mailbox cycle when awaited
         */
        TAsyncLowPriorityQueueAwaiter Next() {
            return TAsyncLowPriorityQueueAwaiter{ this };
        }

    private:
        class [[nodiscard]] TAsyncLowPriorityQueueAwaiter
            : public TIntrusiveListItem<TAsyncLowPriorityQueueAwaiter>
        {
            friend TAsyncLowPriorityQueue;

        public:
            static constexpr bool IsActorAwareAwaiter = true;

            TAsyncLowPriorityQueueAwaiter(TAsyncLowPriorityQueue* self)
                : Self(self)
            {}

            TAsyncLowPriorityQueueAwaiter(TAsyncLowPriorityQueueAwaiter&&) = delete;
            TAsyncLowPriorityQueueAwaiter(const TAsyncLowPriorityQueueAwaiter&) = delete;
            TAsyncLowPriorityQueueAwaiter& operator=(TAsyncLowPriorityQueueAwaiter&&) = delete;
            TAsyncLowPriorityQueueAwaiter& operator=(const TAsyncLowPriorityQueueAwaiter&) = delete;

            TAsyncLowPriorityQueueAwaiter& CoAwaitByValue() && noexcept {
                return *this;
            }

            bool await_ready() noexcept {
                return false;
            }

            template<class TPromise>
            void await_suspend(std::coroutine_handle<TPromise> h) {
                Actor = &h.promise().GetActor();
                Continuation = h;
                Self->Enqueue(this);
            }

            std::coroutine_handle<> await_cancel(std::coroutine_handle<> h) {
                Self->Cancel(this);
                return h;
            }

            void await_resume() noexcept {}

        private:
            void Resume() noexcept {
                Continuation.resume();
            }

        private:
            TAsyncLowPriorityQueue* const Self;
            IActor* Actor;
            std::coroutine_handle<> Continuation;
        };

        void Enqueue(TAsyncLowPriorityQueueAwaiter* awaiter) {
            if (Event) {
                // Current event was sent before this call, we need to wait for the next event
                Y_DEBUG_ABORT_UNLESS(Awaiters, "Unexpected event pending without awaiters");
                Y_DEBUG_ABORT_UNLESS(Awaiters.Front()->Actor == awaiter->Actor,
                    "A single TLowPriorityQueue cannot be used by multiple actors");
                NextAwaiters.PushBack(awaiter);
            } else {
                Y_DEBUG_ABORT_UNLESS(!Awaiters && !NextAwaiters, "Unexpected awaiters without an event pending");
                Awaiters.PushBack(awaiter);
                Start(awaiter);
            }
        }

        void Cancel(TAsyncLowPriorityQueueAwaiter* awaiter) {
            awaiter->Unlink();

            // Current pending event may be reused by the next awaiter in the queue
            if (Awaiters) {
                Y_ABORT_UNLESS(Event, "Unexpected awaiters waiting without an event pending");
                return;
            }

            // Current inflight event cannot be reused
            if (Event) {
                Event->Item = nullptr;
                Event = nullptr;
                Reschedule();
            }
        }

        void Start(TAsyncLowPriorityQueueAwaiter* awaiter) {
            auto selfId = awaiter->Actor->SelfId();
            bool ok = selfId.Send(selfId, (Event = new TEvents::TEvResumeRunnable(this)));
            Y_ABORT_UNLESS(ok, "Unexpected failure to send an event to SelfId");
        }

        void Reschedule() {
            Awaiters.Append(NextAwaiters);
            if (Awaiters) {
                Start(Awaiters.Front());
            }
        }

        void DoRun(IActor* actor) {
            // Note: DoRun is called when either event is delivered, in which
            // case actor != nullptr, or the event is destroyed/undelivered
            // before being disarmed, in which case actor == nullptr.
            Event = nullptr;
            if (actor) {
                // We must never be called without an active awaiter
                Y_ABORT_UNLESS(Awaiters, "Unexpected event delivery without an active awaiter");
                auto* current = Awaiters.PopFront();
                Reschedule();
                current->Resume();
            }
        }

    private:
        TEvents::TEvResumeRunnable* Event = nullptr;
        TIntrusiveList<TAsyncLowPriorityQueueAwaiter> Awaiters;
        TIntrusiveList<TAsyncLowPriorityQueueAwaiter> NextAwaiters;
    };

} // namespace NActors
