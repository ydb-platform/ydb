#pragma once
#include "async.h"
#include "decorator.h"

namespace NActors {

    namespace NDetail {

        class TAsyncCancellationSourceTask : public TIntrusiveListItem<TAsyncCancellationSourceTask> {
        protected:
            ~TAsyncCancellationSourceTask() = default;

        public:
            virtual void OnCancel() = 0;
        };

        class TAsyncCancellationSourceState final : public TSimpleRefCount<TAsyncCancellationSourceState> {
            friend TAsyncCancellationSourceTask;

        public:
            TAsyncCancellationSourceState() = default;

            void AddTask(TAsyncCancellationSourceTask* task) noexcept {
                Tasks.PushBack(task);
            }

            void Cancel() {
                Cancelled = true;
                while (!Tasks.Empty()) {
                    TAsyncCancellationSourceTask* task = Tasks.PopFront();
                    task->OnCancel();
                }
            }

            bool IsCancelled() const noexcept {
                return Cancelled;
            }

        private:
            TIntrusiveList<TAsyncCancellationSourceTask> Tasks;
            bool Cancelled = false;
        };

        template<class T>
        class TWithCancellationSourceAwaiter
            : private TActorRunnableItem::TImpl<TWithCancellationSourceAwaiter<T>>
            , private TAsyncDecoratorAwaiter<T, TWithCancellationSourceAwaiter<T>>
            , private TAsyncCancellationSourceTask
        {
            friend TActorRunnableItem::TImpl<TWithCancellationSourceAwaiter<T>>;
            friend TAsyncDecoratorAwaiter<T, TWithCancellationSourceAwaiter<T>>;
            using TBase = TAsyncDecoratorAwaiter<T, TWithCancellationSourceAwaiter<T>>;

        public:
            template<class TCallback>
            TWithCancellationSourceAwaiter(TAsyncCancellationSourceState* state, TCallback&& callback)
                : TBase(std::forward<TCallback>(callback))
            {
                if (state) {
                    Cancelled = state->IsCancelled();
                    if (!Cancelled) {
                        state->AddTask(this);
                    }
                }
            }

            TWithCancellationSourceAwaiter& CoAwaitByValue() && noexcept {
                return *this;
            }

        private:
            void OnCancel() override {
                Y_ABORT_UNLESS(!Cancelled, "Unexpected OnCancel() when already cancelled");
                Cancelled = true;
                if (Started) {
                    // Note: we must not resume this work recursively
                    if (auto next = TBase::Cancel()) {
                        Scheduled = next;
                        TActorRunnableQueue::Schedule(this);
                    }
                }
            }

            std::coroutine_handle<> Bypass() noexcept {
                if (this->GetCancellation()) {
                    if (!Cancelled) {
                        // Don't want unexpected OnCancel() calls in the future
                        TAsyncCancellationSourceTask::Unlink();
                    }
                    // Bypass everything when already cancelled
                    return this->GetAsyncBody();
                }
                return nullptr;
            }

            std::coroutine_handle<> StartCancelled() noexcept {
                // We should have bypassed everything
                Y_ABORT("should never be called");
            }

            std::coroutine_handle<> Start() noexcept {
                if (Cancelled) {
                    if (auto next = TBase::Cancel()) [[unlikely]] {
                        Y_ABORT("unexpected cancellation work before async body started");
                    }
                }
                Started = true;
                return TBase::Start();
            }

            std::coroutine_handle<> Cancel() {
                if (!Cancelled) {
                    // Don't want unexpected OnCancel() calls in the future
                    TAsyncCancellationSourceTask::Unlink();
                    // We don't need to intercept unwind now
                    return TBase::CancelBypass();
                }
                return nullptr;
            }

            std::coroutine_handle<> OnResume(std::coroutine_handle<> h) {
                if (!Cancelled) {
                    // Don't want unexpected OnCancel() calls in the future
                    TAsyncCancellationSourceTask::Unlink();
                }
                if (Scheduled) {
                    // We resume scheduled work now and postpone resume until later
                    return std::exchange(Scheduled, h);
                }
                return h;
            }

            std::coroutine_handle<> OnUnwind(std::coroutine_handle<> h) {
                if (!Cancelled) {
                    // Don't want unexpected OnCancel() calls in the future
                    TAsyncCancellationSourceTask::Unlink();
                }
                if (!h) {
                    // Caller was not cancelled yet, resume with exception instead
                    h = this->GetContinuation();
                    ThrowException = true;
                }
                if (Scheduled) {
                    // We resume scheduled work now and postpone unwind until later
                    return std::exchange(Scheduled, h);
                }
                return h;
            }

            decltype(auto) Return() {
                if (ThrowException) {
                    throw TAsyncCancellation() << "operation cancelled";
                }

                return TBase::Return();
            }

            void DoRun(IActor*) noexcept {
                Y_ABORT_UNLESS(Scheduled, "Scheduled without a scheduled item");
                Scheduled.resume();
            }

        private:
            std::coroutine_handle<> Scheduled;
            bool Started = false;
            bool Cancelled = false;
            bool ThrowException = false;
        };

    } // namespace NDetail

    class TAsyncCancellationSource {
    public:
        TAsyncCancellationSource()
            : State(new NDetail::TAsyncCancellationSourceState)
        {}

        TAsyncCancellationSource(std::nullptr_t)
            : State(nullptr)
        {}

        explicit operator bool() const {
            return bool(State);
        }

        void Cancel() {
            if (State) {
                State->Cancel();
            }
        }

        bool IsCancelled() const {
            return State ? State->IsCancelled() : false;
        }

        template<IsAsyncCoroutineCallable TCallback>
        auto WithCancellation(TCallback&& callback) {
            using TCallbackResult = decltype(std::forward<TCallback>(callback)());
            using T = typename TCallbackResult::result_type;
            return NDetail::TWithCancellationSourceAwaiter<T>(State.Get(), std::forward<TCallback>(callback));
        }

    private:
        TIntrusivePtr<NDetail::TAsyncCancellationSourceState> State;
    };

    namespace NDetail {

        template<class T, class TOnCancel>
        class TWhenCancelledAwaiter
            : public TAsyncDecoratorAwaiter<T, TWhenCancelledAwaiter<T, TOnCancel>>
        {
            friend TAsyncDecoratorAwaiter<T, TWhenCancelledAwaiter<T, TOnCancel>>;
            using TBase = TAsyncDecoratorAwaiter<T, TWhenCancelledAwaiter<T, TOnCancel>>;

        public:
            template<class TCallback>
            TWhenCancelledAwaiter(TCallback&& callback, TOnCancel&& onCancel)
                : TBase(std::forward<TCallback>(callback))
                , OnCancel(std::forward<TOnCancel>(onCancel))
            {}

            TWhenCancelledAwaiter& CoAwaitByValue() && noexcept {
                return *this;
            }

        private:
            struct TOnCancelResumeCallback : public TAwaitCancelSource {
                TWhenCancelledAwaiter* Self = nullptr;

                IActor& GetActor() {
                    return Self->GetActor();
                }

                TAwaitCancelSource& GetAwaitCancelSource() {
                    return *this;
                }

                std::coroutine_handle<> operator()() {
                    return Self->OnCancelFinished();
                }
            };

        private:
            std::coroutine_handle<> Bypass() noexcept {
                if (this->Cancellation()) {
                    // Don't even try starting async body
                    return this->Cancellation();
                }
                return nullptr;
            }

            std::coroutine_handle<> StartCancelled() noexcept {
                Y_ABORT("should never be called");
            }

            std::coroutine_handle<> Cancel() {
                if (Finished) [[unlikely]] {
                    return nullptr;
                }
                try {
                    OnCancelResumeProxy = MakeCallbackCoroutine<TOnCancelResumeCallback>();
                    OnCancelCoroutine.UnsafeMoveFrom(std::move(OnCancel)());
                } catch (...) {
                    OnCancelException = std::current_exception();
                    if (OnCancelCoroutine) {
                        OnCancelCoroutine.Destroy();
                    }
                    if (OnCancelResumeProxy) {
                        OnCancelResumeProxy.destroy();
                    }
                    return TBase::Cancel();
                }
                OnCancelCoroutine.GetHandle().promise().SetContinuation(OnCancelResumeProxy.GetHandle());
                return OnCancelCoroutine.GetHandle();
            }

            std::coroutine_handle<> OnResume(std::coroutine_handle<> h) {
                Finished = h;
                if (OnCancelCoroutine) {
                    // We need to cancel currently running OnCancel
                    if (auto next = OnCancelResumeProxy->Cancel(OnCancelResumeProxy.GetHandle())) {
                        return next;
                    }
                    // And also wait until OnCancelFinished()
                    return std::noop_coroutine();
                }
                return h;
            }

            std::coroutine_handle<> OnCancelFinished() {
                Y_ABORT_UNLESS(OnCancelCoroutine);
                TAsyncResult<void>& onCancelResult = OnCancelCoroutine.GetHandle().promise();
                // Note: we don't care when OnCancel success or unwinds
                if (onCancelResult && onCancelResult.HasException()) {
                    OnCancelException = onCancelResult.ExtractException();
                    try {
                        std::rethrow_exception(OnCancelException);
                    } catch (const TAsyncCancellation&) {
                        // TAsyncCancellation is not an error
                        OnCancelException = nullptr;
                    } catch (...) {}
                }
                OnCancelCoroutine.Destroy();
                OnCancelResumeProxy.destroy();
                // Async body might have finished concurrently
                if (Finished) {
                    if (OnCancelException) {
                        // Resume and rethrow OnCancel exception instead
                        return this->GetContinuation();
                    }
                    return Finished;
                }
                // Cancel async body now (it will either resume or unwind)
                return TBase::Cancel();
            }

            std::coroutine_handle<> OnUnwind(std::coroutine_handle<> h) {
                Finished = h;
                Y_ABORT_UNLESS(!OnCancelCoroutine);
                if (OnCancelException) {
                    // Resume and rethrow OnCancel exception instead
                    return this->GetContinuation();
                }
                return h;
            }

            decltype(auto) Return() {
                if (OnCancelException) {
                    std::rethrow_exception(std::move(OnCancelException));
                }

                return TBase::Return();
            }

        private:
            TOnCancel&& OnCancel;
            TCallbackCoroutine<TOnCancelResumeCallback> OnCancelResumeProxy;
            async<void> OnCancelCoroutine = async<void>::UnsafeEmpty();
            std::exception_ptr OnCancelException;
            std::coroutine_handle<> Finished;
        };

    } // namespace NDetail

    /**
     * Starts the wrapped coroutine, but calls and awaits onCancel when caller requests cancellation.
     *
     * Cancellation to the wrapped coroutine is not propagated until onCancel finishes.
     */
    template<IsAsyncCoroutineCallable TCallback, IsSpecificAsyncCoroutineCallable<void> TOnCancel>
    inline auto WhenCancelled(TCallback&& callback, TOnCancel&& onCancel) {
        using TCallbackResult = decltype(std::forward<TCallback>(callback()));
        using T = typename TCallbackResult::result_type;
        return NDetail::TWhenCancelledAwaiter<T, TOnCancel>(std::forward<TCallback>(callback), std::forward<TOnCancel>(onCancel));
    }

} // namespace NActors
