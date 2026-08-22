#pragma once
#include "async.h"
#include "callback_coroutine.h"

namespace NActors::NDetail {

    template<class R, class TDerived>
    class TAsyncDecoratorAwaiter {
        friend TAwaitCancelSource;

    public:
        static constexpr bool IsActorAwareAwaiter = true;

        template<class TCallback, class... TArgs>
        TAsyncDecoratorAwaiter(TCallback&& callback, TArgs&&... args)
            : Coroutine(std::invoke(std::forward<TCallback>(callback), std::forward<TArgs>(args)...))
        {}

    private:
        /**
         * Base class for resume interception coroutine
         */
        class TResumeCallback : private TAwaitCancelSource {
            friend TAsyncDecoratorAwaiter;
            friend TCallbackCoroutine<TResumeCallback>;

        public:
            IActor& GetActor() noexcept {
                return *Self->Actor;
            }

            TAwaitCancelSource& GetAwaitCancelSource() noexcept {
                return *this;
            }

        private:
            std::coroutine_handle<> operator()() noexcept {
                return Self->OnResumeCallback();
            }

        private:
            TAsyncDecoratorAwaiter* Self = nullptr;
        };

        /**
         * Base class for unwind interception coroutine
         */
        class TUnwindCallback {
            friend TAsyncDecoratorAwaiter;
            friend TCallbackCoroutine<TUnwindCallback>;

        public:
            TUnwindCallback() = default;

        private:
            std::coroutine_handle<> operator()() noexcept {
                return Self->OnUnwindCallback();
            }

        private:
            TAsyncDecoratorAwaiter* Self = nullptr;
        };

    public:
        bool await_ready() noexcept {
            return false;
        }

        template<class TPromise>
        std::coroutine_handle<> await_suspend(std::coroutine_handle<TPromise> c) {
            TPromise& caller = c.promise();
            IActor& actor = caller.GetActor();
            TAwaitCancelSource& source = caller.GetAwaitCancelSource();

            Actor = &actor;
            Continuation = c;
            Cancellation = source.GetCancellation();

            if (auto next = static_cast<TDerived&>(*this).Bypass()) {
                Coroutine.GetHandle().promise().SetContinuation(c);
                return next;
            }

            ResumeProxy = MakeCallbackCoroutine<TResumeCallback>();
            ResumeProxy->Self = this;

            Coroutine.GetHandle().promise().SetContinuation(ResumeProxy.GetHandle());

            if (Cancellation) {
                return static_cast<TDerived&>(*this).StartCancelled();
            }

            // Subscribe to caller's cancellation attempts
            Cleanup = source.SetAwaiter(*this);

            // We start normally, unsubscribing on exceptions
            TCallCleanup callCleanup{ this };
            auto next = static_cast<TDerived&>(*this).Start();
            callCleanup.Cancel();

            return next;
        }

        decltype(auto) await_resume() {
            Cleanup();
            return static_cast<TDerived&>(*this).Return();
        }

    public:
        /**
         * Allows bypassing decorator
         *
         * When Bypass() returns a valid coroutine handle it is resumed without
         * decoration. Return path or cancellation will not be intercepted.
         *
         * Default returns nullptr (no bypass).
         */
        [[nodiscard]] std::coroutine_handle<> Bypass() noexcept {
            return nullptr;
        }

        /**
         * Allows modifying startup behavior when caller is already cancelled.
         *
         * Must return a valid coroutine handle which will be resumed.
         *
         * Default propagates cancellation and calls Start().
         */
        [[nodiscard]] std::coroutine_handle<> StartCancelled() {
            Y_DEBUG_ABORT_UNLESS(ResumeProxy, "cannot start bypassed coroutines");
            if (!ResumeProxy->GetCancellation()) [[likely]] {
                ResumeProxy->SetCancellation(CreateUnwindProxy());
            }
            return static_cast<TDerived&>(*this).Start();
        }

        /**
         * Allows modifying startup behavior
         *
         * Must return a valid coroutine handle which will be resumed.
         *
         * Examples:
         *
         * - GetAsyncBody(): async body starts normally
         * - GetContinuation(): async body does not start, return to caller
         * - GetCancellation(): async body does not start, unwind caller
         * - Any other coroutine handle allows for async setup before
         *   eventually resuming any handle above or calling Start().
         *
         * Default returns GetAsyncBody().
         */
        [[nodiscard]] std::coroutine_handle<> Start() noexcept {
            Y_DEBUG_ABORT_UNLESS(ResumeProxy, "cannot start bypassed coroutines");
            return GetAsyncBody();
        }

        /**
         * Allows modifying behavior when caller requests cancellation
         *
         * Default calls Cancel().
         */
        [[nodiscard]] std::coroutine_handle<> OnCancel() noexcept {
            return Cancel();
        }

        /**
         * Allows modifying the normal return behavior
         *
         * By default returns h (which is equal to Continuation) to return back
         * to caller, but may return any resumable coroutine handle. Decorator is
         * responsible to eventually resume either Continuation or Cancellation.
         */
        [[nodiscard]] std::coroutine_handle<> OnResume(std::coroutine_handle<> h) noexcept {
            return h;
        }

        /**
         * Allows modifying the unwind return behavior
         *
         * By default returns h (which is equal to Cancellation) to unwind the
         * caller, but may return any resumable coroutine handle. Decorator is
         * responsible to eventually resume either Continuation or Cancellation.
         *
         * Note: h may be nullptr after a call to Cancel() in case the caller is
         * not also cancelled and is still waiting for the result. Decorator is
         * responsible to eventually resume Continuation in that case.
         */
        [[nodiscard]] std::coroutine_handle<> OnUnwind(std::coroutine_handle<> h) noexcept {
            return h;
        }

        /**
         * Allows modifying the result when returning to caller
         *
         * Default returns the async body result.
         */
        R Return() {
            return Coroutine.GetHandle().promise().ExtractValue();
        }

    protected:
        /**
         * Returns task actor
         */
        IActor& GetActor() const {
            return *Actor;
        }

        /**
         * Returns the async body handle for returning from Bypass() or Start().
         */
        std::coroutine_handle<> GetAsyncBody() const noexcept {
            return Coroutine.GetHandle();
        }

        /**
         * Returns the normal return coroutine handle.
         */
        std::coroutine_handle<> GetContinuation() const noexcept {
            return Continuation;
        }

        /**
         * Returns the cancelled return coroutine handle for stack unwinding.
         */
        std::coroutine_handle<> GetCancellation() const noexcept {
            return Cancellation;
        }

        /**
         * Requests async body cancellation
         *
         * When the returned handle is not nullptr it must be resumed either
         * with symmetric transfer or in the background. Usually this starts
         * some cancellation related activity, which may eventually result in
         * OnUnwind being called, but not necessarily.
         *
         * Important: when scheduling the returned handle to execute in the
         * background it is important to remember async body may return normally
         * before such a handle is resumed, which may cause destructors to free
         * memory when you let it continue. Care must be taken to properly wait
         * until background unwind is processed before returning to caller.
         */
        [[nodiscard]] std::coroutine_handle<> Cancel() noexcept {
            Y_DEBUG_ABORT_UNLESS(ResumeProxy, "cannot cancel bypassed coroutines");
            if (!ResumeProxy->GetCancellation()) [[likely]] {
                return ResumeProxy->Cancel(CreateUnwindProxy());
            } else {
                return nullptr;
            }
        }

        /**
         * Requests async body cancellation without interception
         *
         * This is a performance optimization, valid when decorator always
         * returns cancellation handle on unwind without delays anyway.
         *
         * OnUnwind will not be called after this method succeeds.
         */
        [[nodiscard]] std::coroutine_handle<> CancelWithBypass() noexcept {
            Y_ASSERT(Cancellation);
            Y_DEBUG_ABORT_UNLESS(ResumeProxy, "cannot cancel bypassed coroutines");
            if (!ResumeProxy->GetCancellation()) {
                return ResumeProxy->Cancel(Cancellation);
            } else {
                return nullptr;
            }
        }

        /**
         * Returns true after async body unwinds
         */
        bool DidUnwind() const noexcept {
            return !Coroutine;
        }

    private:
        std::coroutine_handle<> CreateUnwindProxy() {
            Y_DEBUG_ABORT_UNLESS(!UnwindProxy, "unexpected unwind proxy already allocated");
            UnwindProxy = MakeCallbackCoroutine<TUnwindCallback>();
            UnwindProxy->Self = this;
            return UnwindProxy;
        }

    private:
        std::coroutine_handle<> await_cancel(std::coroutine_handle<> h) noexcept {
            Cancellation = h;
            return static_cast<TDerived&>(*this).OnCancel();
        }

        std::coroutine_handle<> OnResumeCallback() noexcept {
            return static_cast<TDerived&>(*this).OnResume(Continuation);
        }

        std::coroutine_handle<> OnUnwindCallback() noexcept {
            Coroutine.Destroy();
            return static_cast<TDerived&>(*this).OnUnwind(Cancellation);
        }

    private:
        struct TCallCleanup {
            TAsyncDecoratorAwaiter* Self;

            inline ~TCallCleanup() noexcept {
                if (Self) [[unlikely]] {
                    Self->Cleanup();
                }
            }

            inline void Cancel() noexcept {
                Self = nullptr;
            }
        };

    private:
        IActor* Actor = nullptr;
        std::coroutine_handle<> Continuation;
        std::coroutine_handle<> Cancellation;
        TAwaitCancelCleanup Cleanup;
        TCallbackCoroutine<TResumeCallback> ResumeProxy;
        TCallbackCoroutine<TUnwindCallback> UnwindProxy;
        // Must be below proxies, so it is destroyed first
        async<R> Coroutine;
    };

} // namespace NActors::NDetail
