#pragma once
#include "async.h"
#include "decorator.h"

namespace NActors {

    /**
     * Returns a new TAsyncCancellationScope with currently running and
     * non-cancelled handler attached as a sink when awaited. May only be
     * used directly inside an async actor event handler.
     */
    inline auto TAsyncCancellationScope::WithCurrentHandler() {
        return NDetail::TActorAsyncHandlerPromise::TCreateAttachedCancellationScopeOp{};
    }

    namespace NDetail {

        template<class T, bool Shielded>
        class [[nodiscard]] TWrapCancellationScopeAwaiter
            : private TAsyncCancellable
            , private TActorRunnableItem::TImpl<TWrapCancellationScopeAwaiter<T, Shielded>>
            , public TAsyncDecoratorAwaiter<T, TWrapCancellationScopeAwaiter<T, Shielded>>
        {
            friend TActorRunnableItem::TImpl<TWrapCancellationScopeAwaiter<T, Shielded>>;
            friend TAsyncDecoratorAwaiter<T, TWrapCancellationScopeAwaiter<T, Shielded>>;
            using TBase = TAsyncDecoratorAwaiter<T, TWrapCancellationScopeAwaiter<T, Shielded>>;

        public:
            template<class TCallback, class... TArgs>
            TWrapCancellationScopeAwaiter(TAsyncCancellationScope& scope, TCallback&& callback, TArgs&&... args)
                : TBase(std::forward<TCallback>(callback), std::forward<TArgs>(args)...)
            {
                Cancelled = scope.IsCancelled();
                if (!Cancelled) {
                    scope.AddSink(*this);
                }
            }

            TWrapCancellationScopeAwaiter& CoAwaitByValue() && noexcept {
                return *this;
            }

        private:
            void Cancel() noexcept override {
                Y_ABORT_UNLESS(!Cancelled, "Unexpected Cancel() when already cancelled");
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
                if constexpr (!Shielded) {
                    if (this->GetCancellation()) {
                        if (!Cancelled) {
                            // Don't want unexpected Cancel() calls in the future
                            TAsyncCancellable::Unlink();
                        }
                        // Bypass everything when already cancelled by the caller
                        return this->GetAsyncBody();
                    }
                }
                return nullptr;
            }

            std::coroutine_handle<> StartCancelled() noexcept {
                if constexpr (!Shielded) {
                    // We should have bypassed everything
                    Y_ABORT("should never be called");
                } else {
                    // We shield wrapped coroutine from caller's cancellation
                    return Start();
                }
            }

            std::coroutine_handle<> Start() noexcept {
                Started = true;
                if (Cancelled) {
                    if (auto next = TBase::Cancel()) [[unlikely]] {
                        Y_ABORT("unexpected cancellation work before async body started");
                    }
                }
                return TBase::Start();
            }

            std::coroutine_handle<> OnCancel() noexcept {
                if constexpr (!Shielded) {
                    if (!Cancelled) {
                        Cancelled = true;
                        // Don't want unexpected Cancel() calls in the future
                        TAsyncCancellable::Unlink();
                        // We don't need to intercept unwind from now on
                        return TBase::CancelWithBypass();
                    }
                }
                return nullptr;
            }

            std::coroutine_handle<> OnResume(std::coroutine_handle<> h) {
                if (!Cancelled) {
                    // Don't want unexpected Cancel() calls in the future
                    TAsyncCancellable::Unlink();
                }
                if (Scheduled) {
                    // We resume scheduled work now and postpone resume until later
                    return std::exchange(Scheduled, h);
                }
                return h;
            }

            std::coroutine_handle<> OnUnwind(std::coroutine_handle<> h) {
                if (!Cancelled) {
                    // Don't want unexpected Cancel() calls in the future
                    TAsyncCancellable::Unlink();
                }
                if constexpr (!Shielded) {
                    if (!h) {
                        // Caller was not cancelled yet, resume with exception instead
                        h = this->GetContinuation();
                    }
                } else {
                    // We shield wrapped coroutine from caller's cancellation
                    h = this->GetContinuation();
                }
                if (Scheduled) {
                    // We resume scheduled work now and postpone unwind until later
                    return std::exchange(Scheduled, h);
                }
                return h;
            }

            bool Return() requires (std::is_void_v<T>) {
                if (this->DidUnwind()) {
                    return false;
                }

                TBase::Return();
                return true;
            }

            std::optional<T> Return() requires (!std::is_void_v<T>) {
                if (this->DidUnwind()) {
                    return std::nullopt;
                }

                struct TImplicitConverter {
                    TBase& base;

                    operator T() const {
                        return base.Return();
                    }
                };

                return TImplicitConverter{ *this };
            }

            void DoRun(IActor*) noexcept {
                Y_ABORT_UNLESS(Scheduled, "Scheduled without a coroutine to resume");
                std::exchange(Scheduled, nullptr).resume();
            }

        private:
            std::coroutine_handle<> Scheduled;
            bool Cancelled = false;
            bool Started = false;
        };

    } // namespace NDetail

    /**
     * Runs the wrapped coroutine attached to this cancellation scope when
     * awaited. This allows cancelling this coroutine independently from
     * caller cancellation.
     */
    template<class T>
    inline auto TAsyncCancellationScope::Wrap(async<T> wrapped) {
        return NDetail::TWrapCancellationScopeAwaiter<T, false>(*this, [&wrapped]{ return wrapped.UnsafeMove(); });
    }

    /**
     * Runs the wrapped coroutine attached to this cancellation scope when
     * awaited. This allows cancelling this coroutine independently from
     * caller cancellation.
     */
    template<class TCallback, class... TArgs>
    inline auto TAsyncCancellationScope::Wrap(TCallback&& callback, TArgs&&... args)
        requires IsAsyncCoroutineCallable<TCallback, TArgs...>
    {
        using TCallbackResult = std::invoke_result_t<TCallback, TArgs...>;
        using T = typename TCallbackResult::result_type;
        return NDetail::TWrapCancellationScopeAwaiter<T, false>(*this, std::forward<TCallback>(callback), std::forward<TArgs>(args)...);
    }

    /**
     * Runs the wrapped coroutine attached to this cancellation scope when
     * awaited. This allows cancelling this coroutine independently from
     * caller cancellation. The call is shilded from caller's cancallation.
     */
    template<class T>
    inline auto TAsyncCancellationScope::WrapShielded(async<T> wrapped) {
        return NDetail::TWrapCancellationScopeAwaiter<T, true>(*this, [&wrapped]{ return wrapped.UnsafeMove(); });
    }

    /**
     * Runs the wrapped coroutine attached to this cancellation scope when
     * awaited. This allows cancelling this coroutine independently from
     * caller cancellation. The call is shilded from caller's cancallation.
     */
    template<class TCallback, class... TArgs>
    inline auto TAsyncCancellationScope::WrapShielded(TCallback&& callback, TArgs&&... args)
        requires IsAsyncCoroutineCallable<TCallback, TArgs...>
    {
        using TCallbackResult = std::invoke_result_t<TCallback, TArgs...>;
        using T = typename TCallbackResult::result_type;
        return NDetail::TWrapCancellationScopeAwaiter<T, true>(*this, std::forward<TCallback>(callback), std::forward<TArgs>(args)...);
    }

    namespace NDetail {

        template<class T>
        concept IsOnCancelCallbackResultType = (
            std::is_same_v<T, void> ||
            std::is_same_v<T, bool> ||
            std::is_same_v<T, async<void>> ||
            std::is_same_v<T, async<bool>>);

        template<class TCallback>
        using TOnCancelCallbackResultType = decltype(std::declval<TCallback&&>()());

        template<class TCallback>
        concept IsOnCancelCallback = IsOnCancelCallbackResultType<TOnCancelCallbackResultType<TCallback>>;

        // The default implementation is for types void and bool
        template<class TOnCancelCallback, class TDerived, class T = TOnCancelCallbackResultType<TOnCancelCallback>>
        class TInterceptCancellationBase {
        protected:
            TInterceptCancellationBase(TOnCancelCallback&& onCancelCallback)
                : OnCancelCallback(std::forward<TOnCancelCallback>(onCancelCallback))
            {}

            bool InterceptCancel() noexcept {
                try {
                    if constexpr (std::is_same_v<T, void>) {
                        std::forward<TOnCancelCallback>(OnCancelCallback)();
                        return true;
                    } else {
                        static_assert(std::is_same_v<T, bool>, "OnCancelCallback must return either void or bool");
                        return std::forward<TOnCancelCallback>(OnCancelCallback)();
                    }
                } catch (...) {
                    Exception = std::current_exception();
                    return true;
                }
            }

            std::coroutine_handle<> InterceptResume(std::coroutine_handle<> h) noexcept {
                return h;
            }

            std::coroutine_handle<> InterceptUnwind(std::coroutine_handle<> h) noexcept {
                if (Exception) {
                    return static_cast<TDerived&>(*this).GetErrorContinuation();
                }
                return h;
            }

            void InterceptReturn() {
                if (Exception) {
                    std::rethrow_exception(Exception);
                }
            }

        private:
            TOnCancelCallback&& OnCancelCallback;
            std::exception_ptr Exception;
        };

        // This specialization is for types async<void> and async<bool>
        template<class TOnCancelCallback, class TDerived, class T>
        class TInterceptCancellationBase<TOnCancelCallback, TDerived, async<T>> {
        private:
            class TResumeCallback : private TAwaitCancelSource {
                friend TInterceptCancellationBase;
                friend TCallbackCoroutine<TResumeCallback>;

            public:
                IActor& GetActor() {
                    return static_cast<TDerived&>(*Self).GetActor();
                }

                TAwaitCancelSource& GetAwaitCancelSource() {
                    return *this;
                }

            private:
                std::coroutine_handle<> operator()() {
                    return Self->OnCancelFinished();
                }

            private:
                TInterceptCancellationBase* Self = nullptr;
            };

        protected:
            TInterceptCancellationBase(TOnCancelCallback&& onCancelCallback)
                : OnCancelCallback(std::forward<TOnCancelCallback>(onCancelCallback))
            {}

            std::coroutine_handle<> InterceptCancel() noexcept {
                try {
                    ResumeProxy = MakeCallbackCoroutine<TResumeCallback>();
                    ResumeProxy->Self = this;
                    Coroutine.UnsafeMoveFrom(std::move(OnCancelCallback)());
                } catch (...) {
                    Exception = std::current_exception();
                    if (Coroutine) {
                        Coroutine.Destroy();
                    }
                    if (ResumeProxy) {
                        ResumeProxy.destroy();
                    }
                    return nullptr;
                }
                Coroutine.GetHandle().promise().SetContinuation(ResumeProxy.GetHandle());
                return Coroutine.GetHandle();
            }

            std::coroutine_handle<> InterceptResume(std::coroutine_handle<> h) noexcept {
                if (Coroutine) {
                    Continuation = h;
                    // We need to cancel running OnCancel and wait until it finishes
                    if (auto next = ResumeProxy->Cancel(ResumeProxy.GetHandle())) {
                        return next;
                    }
                    return std::noop_coroutine();
                }

                return h;
            }

            std::coroutine_handle<> InterceptUnwind(std::coroutine_handle<> h) noexcept {
                if (Coroutine) {
                    Continuation = h;
                    // We need to cancel running OnCancel and wait until it finishes
                    if (auto next = ResumeProxy->Cancel(ResumeProxy.GetHandle())) {
                        return next;
                    }
                    return std::noop_coroutine();
                }

                if (Exception) {
                    return static_cast<TDerived&>(*this).GetErrorContinuation();
                }

                return h;
            }

            void InterceptReturn() {
                if (Exception) {
                    std::rethrow_exception(Exception);
                }
            }

        private:
            std::coroutine_handle<> OnCancelFinished() noexcept {
                bool cancel = false;
                TAsyncResult<T>& result = Coroutine.GetHandle().promise();
                if (result.HasValue()) {
                    if constexpr (std::is_same_v<T, void>) {
                        cancel = true;
                    } else {
                        static_assert(std::is_same_v<T, bool>, "OnCancelCallback must return either async<void> or async<bool>");
                        cancel = result.ExtractValue();
                    }
                } else if (result.HasException()) {
                    Exception = result.ExtractException();
                    cancel = true;
                }
                // We need to free memory and call destructors in case of OnCancel unwind
                Coroutine.Destroy();
                ResumeProxy.destroy();
                // Intercepted body may have finished already
                if (Continuation) {
                    if (Exception) {
                        return static_cast<TDerived&>(*this).GetErrorContinuation();
                    }
                    return Continuation;
                }
                // Otherwise we might need to cancel currently running async body
                if (auto next = static_cast<TDerived&>(*this).OnInterceptCancelResult(cancel)) {
                    return next;
                }
                // We must return a valid coroutine handle
                return std::noop_coroutine();
            }

        private:
            TOnCancelCallback&& OnCancelCallback;
            TCallbackCoroutine<TResumeCallback> ResumeProxy;
            async<T> Coroutine = async<T>::UnsafeEmpty();
            std::coroutine_handle<> Continuation;
            std::exception_ptr Exception;
        };

        template<class T, class TOnCancelCallback>
        class TInterceptCancellationAwaiter
            : public TAsyncDecoratorAwaiter<T, TInterceptCancellationAwaiter<T, TOnCancelCallback>>
            , private TInterceptCancellationBase<TOnCancelCallback, TInterceptCancellationAwaiter<T, TOnCancelCallback>>
        {
            friend TAsyncDecoratorAwaiter<T, TInterceptCancellationAwaiter<T, TOnCancelCallback>>;
            friend TInterceptCancellationBase<TOnCancelCallback, TInterceptCancellationAwaiter<T, TOnCancelCallback>>;
            using TDecorator = TAsyncDecoratorAwaiter<T, TInterceptCancellationAwaiter<T, TOnCancelCallback>>;
            using TCancelInterceptor = TInterceptCancellationBase<TOnCancelCallback, TInterceptCancellationAwaiter<T, TOnCancelCallback>>;

        public:
            template<class TCallback>
            TInterceptCancellationAwaiter(TCallback&& callback, TOnCancelCallback&& onCancelCallback)
                : TDecorator(std::forward<TCallback>(callback))
                , TCancelInterceptor(std::forward<TOnCancelCallback>(onCancelCallback))
            {}

            TInterceptCancellationAwaiter& CoAwaitByValue() && noexcept {
                return *this;
            }

        private:
            std::coroutine_handle<> Bypass() noexcept {
                if (auto cancellation = TDecorator::GetCancellation()) {
                    // Don't even try starting async body
                    return cancellation;
                }
                return nullptr;
            }

            std::coroutine_handle<> StartCancelled() noexcept {
                Y_ABORT("should never be called");
            }

            std::coroutine_handle<> OnCancel() noexcept {
                if (IgnoreOnCancel) [[unlikely]] {
                    return nullptr;
                }

                IgnoreOnCancel = true;
                return OnInterceptCancelResult(TCancelInterceptor::InterceptCancel());
            }

            std::coroutine_handle<> OnInterceptCancelResult(bool cancel) noexcept {
                if (cancel) {
                    return TDecorator::Cancel();
                }
                return nullptr;
            }

            std::coroutine_handle<> OnInterceptCancelResult(std::coroutine_handle<> start) noexcept {
                if (start) {
                    return start;
                }
                return TDecorator::Cancel();
            }

            std::coroutine_handle<> OnResume(std::coroutine_handle<> h) noexcept {
                IgnoreOnCancel = true;
                return TCancelInterceptor::InterceptResume(h);
            }

            std::coroutine_handle<> OnUnwind(std::coroutine_handle<> h) noexcept {
                IgnoreOnCancel = true;
                return TCancelInterceptor::InterceptUnwind(h);
            }

            std::coroutine_handle<> GetErrorContinuation() noexcept {
                return TDecorator::GetContinuation();
            }

            decltype(auto) Return() {
                TCancelInterceptor::InterceptReturn();
                return TDecorator::Return();
            }

        private:
            bool IgnoreOnCancel = false;
        };

    } // namespace NDetail

    /**
     * Runs the wrapped coroutine when awaited, but calls (and optionally awaits) onCancelCallback when caller requests cancellation
     *
     * Cancellation to the wrapped coroutine is not propagated until onCancel returns void, returns true or throws an exception.
     *
     * When the wrapped coroutine returns early onCancelCallback is cancelled and awaited before returning to caller.
     */
    template<class T, NDetail::IsOnCancelCallback TOnCancelCallback>
    inline auto InterceptCancellation(async<T> wrapped, TOnCancelCallback&& onCancelCallback) {
        return NDetail::TInterceptCancellationAwaiter<T, TOnCancelCallback>(
            [&wrapped]{ return wrapped.UnsafeMove(); },
            std::forward<TOnCancelCallback>(onCancelCallback));
    }

    /**
     * Runs the wrapped coroutine when awaited, but calls (and optionally awaits) onCancelCallback when caller requests cancellation
     *
     * Cancellation to the wrapped coroutine is not propagated until onCancel returns void, returns true or throws an exception.
     *
     * When the wrapped coroutine returns early onCancelCallback is cancelled and awaited before returning to caller.
     */
    template<IsAsyncCoroutineCallable TCallback, NDetail::IsOnCancelCallback TOnCancelCallback>
    inline auto InterceptCancellation(TCallback&& callback, TOnCancelCallback&& onCancelCallback) {
        using TCallbackResult = std::invoke_result_t<TCallback>;
        using T = typename TCallbackResult::result_type;
        return NDetail::TInterceptCancellationAwaiter<T, TOnCancelCallback>(
            std::forward<TCallback>(callback),
            std::forward<TOnCancelCallback>(onCancelCallback));
    }

} // namespace NActors
