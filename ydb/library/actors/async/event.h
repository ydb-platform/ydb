#pragma once
#include "async.h"

namespace NActors {

    class TAsyncEvent;

    namespace NDetail {

        class TAsyncEventAwaiter;

        class TAsyncEventAwaiterQueue {
        public:
            void Add(TAsyncEventAwaiter*);
            void Remove(TAsyncEventAwaiter*);
            TAsyncEventAwaiter* PopFront();

            explicit operator bool() const noexcept {
                return bool(Queue_);
            }

            size_t GetSize() const noexcept {
                return Count_;
            }

        private:
            TIntrusiveList<TAsyncEventAwaiter> Queue_;
            size_t Count_ = 0;
        };

        class [[nodiscard]] TAsyncEventAwaiter
            : public TIntrusiveListItem<TAsyncEventAwaiter>
            , private TActorRunnableItem::TImpl<TAsyncEventAwaiter>
        {
            friend TActorRunnableItem::TImpl<TAsyncEventAwaiter>;

        public:
            static constexpr bool IsActorAwareAwaiter = true;

            TAsyncEventAwaiter(TAsyncEventAwaiterQueue& queue) noexcept
                : Queue(queue)
            {}

            ~TAsyncEventAwaiter() {
                if (Waiting()) {
                    Queue.Remove(this);
                }
            }

            TAsyncEventAwaiter(TAsyncEventAwaiter&&) = delete;
            TAsyncEventAwaiter(const TAsyncEventAwaiter&) = delete;
            TAsyncEventAwaiter& operator=(TAsyncEventAwaiter&&) = delete;
            TAsyncEventAwaiter& operator=(const TAsyncEventAwaiter&) = delete;

            TAsyncEventAwaiter& CoAwaitByValue() && noexcept {
                return *this;
            }

            bool await_ready() noexcept {
                return false;
            }

            void await_suspend(std::coroutine_handle<> continuation) noexcept {
                Queue.Add(this);
                Continuation = continuation;
            }

            bool await_cancel(std::coroutine_handle<>) noexcept {
                if (Waiting()) {
                    Queue.Remove(this);
                    return true;
                }
                // Already scheduled to resume
                return false;
            }

            bool await_resume() noexcept {
                return Resumed;
            }

            bool Waiting() const {
                return !TIntrusiveListItem<TAsyncEventAwaiter>::Empty();
            }

            void Resume() {
                Resumed = true;
                TActorRunnableQueue::Schedule(this);
            }

            void Detach() {
                Resumed = false;
                TActorRunnableQueue::Schedule(this);
            }

        private:
            void DoRun(IActor*) noexcept {
                Continuation.resume();
            }

        private:
            TAsyncEventAwaiterQueue& Queue;
            std::coroutine_handle<> Continuation;
            bool Resumed;
        };

        inline void TAsyncEventAwaiterQueue::Add(TAsyncEventAwaiter* awaiter) {
            Queue_.PushBack(awaiter);
            Count_++;
        }

        inline void TAsyncEventAwaiterQueue::Remove(TAsyncEventAwaiter* awaiter) {
            Queue_.Remove(awaiter);
            Count_--;
        }

        inline TAsyncEventAwaiter* TAsyncEventAwaiterQueue::PopFront() {
            Y_ASSERT(Queue_ && Count_ > 0);
            TAsyncEventAwaiter* awaiter = Queue_.PopFront();
            Count_--;
            return awaiter;
        }

        template<class TCallback>
        class [[nodiscard]] TAsyncEventAwaiterWithCallback
            : public TAsyncEventAwaiter
        {
        public:
            TAsyncEventAwaiterWithCallback(TAsyncEventAwaiterQueue& queue, TCallback&& callback) noexcept
                : TAsyncEventAwaiter(queue)
                , Callback(std::forward<TCallback>(callback))
            {}

            TAsyncEventAwaiterWithCallback& CoAwaitByValue() && noexcept {
                return *this;
            }

            void await_suspend(std::coroutine_handle<> continuation) {
                TAsyncEventAwaiter::await_suspend(continuation);
                std::forward<TCallback>(Callback)();
            }

        private:
            TCallback&& Callback;
        };

    } // namespace NDetail

    class TAsyncEvent {
    public:
        TAsyncEvent() = default;

        TAsyncEvent(TAsyncEvent&&) = delete;
        TAsyncEvent(const TAsyncEvent&) = delete;
        TAsyncEvent& operator=(TAsyncEvent&&) = delete;
        TAsyncEvent& operator=(const TAsyncEvent&) = delete;

        ~TAsyncEvent() {
            while (Queue) {
                auto* awaiter = Queue.PopFront();
                awaiter->Detach();
            }
        }

        /**
         * Resumes when the event is notified or destroyed
         */
        NDetail::TAsyncEventAwaiter Wait() noexcept {
            return NDetail::TAsyncEventAwaiter(Queue);
        }

        /**
         * Resumes when the event is notified or destroyed
         *
         * The callback is called immediately after registering an awaiter,
         * which allows e.g. updating the number of awaiters in counters.
         */
        template<class TCallback>
        NDetail::TAsyncEventAwaiterWithCallback<TCallback> Wait(TCallback&& callback) noexcept {
            return NDetail::TAsyncEventAwaiterWithCallback<TCallback>(Queue, std::forward<TCallback>(callback));
        }

        bool HasAwaiters() const noexcept {
            return bool(Queue);
        }

        size_t AwaitersCount() const noexcept {
            return Queue.GetSize();
        }

        bool NotifyOne() noexcept {
            if (Queue) {
                auto* awaiter = Queue.PopFront();
                awaiter->Resume();
                return true;
            }
            return false;
        }

        bool NotifyAll() noexcept {
            bool result = false;
            while (Queue) {
                auto* awaiter = Queue.PopFront();
                awaiter->Resume();
                result = true;
            }
            return result;
        }

    private:
        NDetail::TAsyncEventAwaiterQueue Queue;
    };

} // namespace NActors
