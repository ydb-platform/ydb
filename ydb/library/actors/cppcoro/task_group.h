#pragma once
#include "task_result.h"
#include <util/generic/ptr.h>
#include <util/system/compiler.h>
#include <util/system/yassert.h>
#include <coroutine>
#include <atomic>
#include <memory>

namespace NActors {

    namespace NDetail {

        template<class T>
        struct TTaskGroupResult final : public TTaskResult<T> {
            TTaskGroupResult* Next;
        };

        template<class T>
        struct TTaskGroupSink final
            : public TAtomicRefCount<TTaskGroupSink<T>>
        {
            std::atomic<void*> LastReady{ nullptr };
            TTaskGroupResult<T>* ReadyQueue = nullptr;
            std::coroutine_handle<> Continuation;

            static constexpr uintptr_t MarkerAwaiting = 1;
            static constexpr uintptr_t MarkerDetached = 2;

            ~TTaskGroupSink() noexcept {
                if (!IsDetached()) {
                    Detach();
                }
            }

            std::coroutine_handle<> Push(std::unique_ptr<TTaskGroupResult<T>>&& result) noexcept {
                void* currentValue = LastReady.load(std::memory_order_acquire);
                for (;;) {
                    if (currentValue == (void*)MarkerAwaiting) {
                        if (Y_UNLIKELY(!LastReady.compare_exchange_weak(currentValue, nullptr, std::memory_order_acquire))) {
                            continue;
                        }
                        // We consume the awaiter
                        Y_DEBUG_ABORT_UNLESS(ReadyQueue == nullptr, "TaskGroup is awaiting with non-empty ready queue");
                        result->Next = ReadyQueue;
                        ReadyQueue = result.release();
                        return std::exchange(Continuation, {});
                    }
                    if (currentValue == (void*)MarkerDetached) {
                        // Task group is detached, discard the result
                        return std::noop_coroutine();
                    }
                    TTaskGroupResult<T>* current = reinterpret_cast<TTaskGroupResult<T>*>(currentValue);
                    result->Next = current;
                    void* nextValue = result.get();
                    if (Y_LIKELY(LastReady.compare_exchange_weak(currentValue, nextValue, std::memory_order_acq_rel))) {
                        // Result successfully added
                        result.release();
                        return std::noop_coroutine();
                    }
                }
            }

            bool Ready() const noexcept {
                return ReadyQueue != nullptr || LastReady.load(std::memory_order_acquire) != nullptr;
            }

            Y_NO_INLINE std::coroutine_handle<> Suspend(std::coroutine_handle<> h) noexcept {
                Y_DEBUG_ABORT_UNLESS(ReadyQueue == nullptr, "Caller suspending with non-empty ready queue");
                Continuation = h;
                void* currentValue = LastReady.load(std::memory_order_acquire);
                for (;;) {
                    if (currentValue == nullptr) {
                        if (Y_UNLIKELY(!LastReady.compare_exchange_weak(currentValue, (void*)MarkerAwaiting, std::memory_order_release))) {
                            continue;
                        }
                        // Continuation may wake up on another thread
                        return std::noop_coroutine();
                    }
                    Y_ABORT_UNLESS(currentValue != (void*)MarkerAwaiting, "TaskGroup is suspending with an awaiting marker");
                    Y_ABORT_UNLESS(currentValue != (void*)MarkerDetached, "TaskGroup is suspending with a detached marker");
                    // Race: ready queue is not actually empty
                    Continuation = {};
                    return h;
                }
            }

            std::unique_ptr<TTaskGroupResult<T>> Resume() noexcept {
                std::unique_ptr<TTaskGroupResult<T>> result;
                if (ReadyQueue == nullptr) {
                    void* headValue = LastReady.exchange(nullptr, std::memory_order_acq_rel);
                    Y_ABORT_UNLESS(headValue != (void*)MarkerAwaiting, "TaskGroup is resuming with an awaiting marker");
                    Y_ABORT_UNLESS(headValue != (void*)MarkerDetached, "TaskGroup is resuming with a detached marker");
                    Y_ABORT_UNLESS(headValue, "TaskGroup is resuming with an empty queue");
                    TTaskGroupResult<T>* head = reinterpret_cast<TTaskGroupResult<T>*>(headValue);
                    while (head) {
                        auto* next = std::exchange(head->Next, nullptr);
                        head->Next = ReadyQueue;
                        ReadyQueue = head;
                        head = next;
                    }
                }
                Y_ABORT_UNLESS(ReadyQueue != nullptr);
                result.reset(ReadyQueue);
                ReadyQueue = std::exchange(result->Next, nullptr);
                return result;
            }

            static void Dispose(TTaskGroupResult<T>* head) noexcept {
                while (head) {
                    auto* next = std::exchange(head->Next, nullptr);
                    std::unique_ptr<TTaskGroupResult<T>> ptr(head);
                    head = next;
                }
            }

            bool IsDetached() const noexcept {
                void* headValue = LastReady.load(std::memory_order_acquire);
                return headValue == (void*)MarkerDetached;
            }

            void Detach() noexcept {
                // After this exchange all new results will be discarded
                void* headValue = LastReady.exchange((void*)MarkerDetached, std::memory_order_acq_rel);
                Y_ABORT_UNLESS(headValue != (void*)MarkerAwaiting, "TaskGroup is detaching with an awaiting marker");
                Y_ABORT_UNLESS(headValue != (void*)MarkerDetached, "TaskGroup is detaching with a detached marker");
                if (headValue) {
                    Dispose(reinterpret_cast<TTaskGroupResult<T>*>(headValue));
                }
                if (ReadyQueue) {
                    Dispose(std::exchange(ReadyQueue, nullptr));
                }
            }
        };

        template<class T>
        class TTaskGroupResultHandler {
        public:
            void unhandled_exception() noexcept {
                Result->SetException(std::current_exception());
            }

            template<class TResult>
            void return_value(TResult&& result) {
                Result->SetValue(std::forward<TResult>(result));
            }

        protected:
            std::unique_ptr<TTaskGroupResult<T>> Result = std::make_unique<TTaskGroupResult<T>>();
        };

        template<>
        class TTaskGroupResultHandler<void> {
        public:
            void unhandled_exception() noexcept {
                Result->SetException(std::current_exception());
            }

            void return_void() noexcept {
                Result->SetValue();
            }

        protected:
            std::unique_ptr<TTaskGroupResult<void>> Result = std::make_unique<TTaskGroupResult<void>>();
        };

        template<class T>
        class TTaskGroupPromise final : public TTaskGroupResultHandler<T> {
        public:
            using THandle = std::coroutine_handle<TTaskGroupPromise<T>>;

            THandle get_return_object() noexcept {
                return THandle::from_promise(*this);
            }

            static auto initial_suspend() noexcept { return std::suspend_always{}; }

            struct TFinalSuspend {
                static bool await_ready() noexcept { return false; }
                static void await_resume() noexcept { Y_ABORT("unexpected coroutine resume"); }

                Y_NO_INLINE
                static std::coroutine_handle<> await_suspend(std::coroutine_handle<TTaskGroupPromise<T>> h) noexcept {
                    auto& promise = h.promise();
                    auto sink = std::move(promise.Sink);
                    auto next = sink->Push(std::move(promise.Result));
                    h.destroy();
                    return next;
                }
            };

            static auto final_suspend() noexcept { return TFinalSuspend{}; }

            void SetSink(const TIntrusivePtr<TTaskGroupSink<T>>& sink) {
                Sink = sink;
            }

        private:
            TIntrusivePtr<TTaskGroupSink<T>> Sink;
        };

        template<class T>
        class TTaskGroupTask final {
        public:
            using THandle = std::coroutine_handle<TTaskGroupPromise<T>>;
            using promise_type = TTaskGroupPromise<T>;
            using value_type = T;

        public:
            TTaskGroupTask(THandle handle)
                : Handle(handle)
            {}

            void Start(const TIntrusivePtr<TTaskGroupSink<T>>& sink) {
                Handle.promise().SetSink(sink);
                Handle.resume();
            }

        private:
            THandle Handle;
        };

        template<class T, class TAwaitable>
        TTaskGroupTask<T> CreateTaskGroupTask(TAwaitable awaitable) {
            co_return co_await std::move(awaitable);
        }

    } // namespace NDetail

    /**
     * A task group allows starting multiple subtasks of the same result type
     * and awaiting them in a structured way. When task group is destroyed
     * all subtasks are detached in a thread-safe way.
     */
    template<class T>
    class TTaskGroup {
    public:
        TTaskGroup() = default;

        TTaskGroup(const TTaskGroup&) = delete;
        TTaskGroup(TTaskGroup&&) = delete;
        TTaskGroup& operator=(const TTaskGroup&) = delete;
        TTaskGroup& operator=(TTaskGroup&&) = delete;

        ~TTaskGroup() {
            Sink_->Detach();
        }

        /**
         * Add task to the group that will await the result of awaitable
         */
        template<class TAwaitable>
        void AddTask(TAwaitable&& awaitable) {
            auto task = NDetail::CreateTaskGroupTask<T>(std::forward<TAwaitable>(awaitable));
            task.Start(Sink_);
            ++TaskCount_;
        }

        /**
         * Returns the number of tasks left unawaited
         */
        size_t TaskCount() const {
            return TaskCount_;
        }

        class TAwaiter {
        public:
            explicit TAwaiter(TTaskGroup& taskGroup) noexcept
                : TaskGroup_(taskGroup)
            {}

            bool await_ready() const noexcept {
                Y_ABORT_UNLESS(TaskGroup_.TaskCount_ > 0, "Not enough tasks to await");
                --TaskGroup_.TaskCount_;
                return TaskGroup_.Sink_->Ready();
            }

            std::coroutine_handle<> await_suspend(std::coroutine_handle<> h) noexcept {
                return TaskGroup_.Sink_->Suspend(h);
            }

            T await_resume() {
                return std::move(*TaskGroup_.Sink_->Resume()).Value();
            }

        private:
            TTaskGroup& TaskGroup_;
        };

        /**
         * Await result of the next task in the task group
         */
        TAwaiter operator co_await() noexcept {
            return TAwaiter(*this);
        }

    private:
        TIntrusivePtr<NDetail::TTaskGroupSink<T>> Sink_ = MakeIntrusive<NDetail::TTaskGroupSink<T>>();
        size_t TaskCount_ = 0;
    };

} // namespace NActors
