#pragma once
#include "async.h"

namespace NActors {

    class TAsyncEvent;

    namespace NDetail {

        class [[nodiscard]] TAsyncEventAwaiter
            : public TIntrusiveListItem<TAsyncEventAwaiter>
            , private TActorRunnableItem::TImpl<TAsyncEventAwaiter>
        {
            friend TActorRunnableItem::TImpl<TAsyncEventAwaiter>;

        public:
            static constexpr bool IsActorAwareAwaiter = true;

            TAsyncEventAwaiter(TIntrusiveList<TAsyncEventAwaiter>& list) noexcept
                : List(list)
            {}

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
                List.PushBack(this);
                Continuation = continuation;
            }

            bool await_cancel(std::coroutine_handle<>) noexcept {
                if (!TIntrusiveListItem<TAsyncEventAwaiter>::Empty()) {
                    TIntrusiveListItem<TAsyncEventAwaiter>::Unlink();
                    return true;
                }
                // Already scheduled to resume
                return false;
            }

            bool await_resume() noexcept {
                return Resumed;
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
            TIntrusiveList<TAsyncEventAwaiter>& List;
            std::coroutine_handle<> Continuation;
            bool Resumed;
        };

    } // namespace NDetail

    class TAsyncEvent {
    public:
        TAsyncEvent() = default;
        TAsyncEvent(TAsyncEvent&&) = default;

        TAsyncEvent& operator=(TAsyncEvent&& rhs) noexcept {
            if (this != &rhs) [[likely]] {
                while (!Awaiters.Empty()) {
                    auto* awaiter = Awaiters.PopFront();
                    awaiter->Detach();
                }
                Awaiters = std::move(rhs.Awaiters);
            }
            return *this;
        }

        ~TAsyncEvent() {
            while (!Awaiters.Empty()) {
                auto* awaiter = Awaiters.PopFront();
                awaiter->Detach();
            }
        }

        NDetail::TAsyncEventAwaiter Wait() noexcept {
            return NDetail::TAsyncEventAwaiter(Awaiters);
        }

        bool HasAwaiters() const noexcept {
            return !Awaiters.Empty();
        }

        bool NotifyOne() noexcept {
            if (!Awaiters.Empty()) {
                auto* awaiter = Awaiters.PopFront();
                awaiter->Resume();
                return true;
            }
            return false;
        }

        bool NotifyAll() noexcept {
            bool result = false;
            while (!Awaiters.Empty()) {
                auto* awaiter = Awaiters.PopFront();
                awaiter->Resume();
                result = true;
            }
            return result;
        }

    private:
        TIntrusiveList<NDetail::TAsyncEventAwaiter> Awaiters;
    };

} // namespace NActors
