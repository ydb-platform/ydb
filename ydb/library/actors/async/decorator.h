#pragma once
#include "async.h"
#include "symmetric_proxy.h"

namespace NActors {

    template<class T, class TDerived>
    class TAsyncDecoratorAwaiter
        : public NDetail::TActorAwareAwaiter
        , private NDetail::TAwaitCancelSource
        , private NDetail::TSymmetricResumeCallback<TAsyncDecoratorAwaiter<T, TDerived>>
        , private NDetail::TSymmetricCancelCallback<TAsyncDecoratorAwaiter<T, TDerived>, /*Lazy*/ true>
    {
        friend NDetail::TAwaitCancelSource;
        friend NDetail::TSymmetricResumeCallback<TAsyncDecoratorAwaiter<T, TDerived>>;
        friend NDetail::TSymmetricCancelCallback<TAsyncDecoratorAwaiter<T, TDerived>, /*Lazy*/ true>;

    public:
        template<class TCallback>
        TAsyncDecoratorAwaiter(TCallback&& callback)
            : Coroutine{ std::forward<TCallback>(callback)() }
        {}

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

            if (auto h = static_cast<TDerived&>(*this).Bypass()) {
                Coroutine.GetHandle().promise().SetContinuation(c, actor, source);
                return h;
            }

            // We want to hook early in case anything below includes async steps
            Coroutine.GetHandle().promise().SetContinuation(this->GetResumeHandle(), actor, *this);

            if (Cancellation) {
                // Starting in a cancelled state, call decorator's Cancel to
                // customize cancellation. It might want to return immediately,
                // start some asybc task, or ignore caller's cancellation.
                if (auto h = static_cast<TDerived&>(*this).Cancel()) {
                    return h;
                }
            } else {
                // Subscribe to caller's cancellation attempts
                Cleanup = source.SetAwaiter(*this);
            }

            // Note: assume Start may throw exceptions
            TCallCleanup callCleanup{ this };
            auto h = static_cast<TDerived&>(*this).Start();
            callCleanup.Cancel();

            return h;
        }

        decltype(auto) await_resume() {
            Cleanup();
            static_cast<TDerived&>(*this).OnReturn();
            return Coroutine.GetHandle().promise().ExtractValue();
        }

    public:
        /**
         * Allows bypassing decorator
         *
         * When Bypass returns a valid coroutine handle it will be resumed with
         * this decorator bypassed. Return path or cancellation will not be
         * intercepted.
         *
         * Examples:
         *
         * - GetAsyncBody(): async body starts in bypassed state
         * - GetContinuation(): async body does not start, return to caller
         * - GetCancellation(): async body does not start, unwind caller
         * - Any other coroutine handle allows for async setup before
         *   eventually resuming any handle above.
         *
         * Default returns nullptr (no bypass).
         */
        [[nodiscard]] std::coroutine_handle<> Bypass() noexcept {
            return nullptr;
        }

        /**
         * Allows modifying behavior when caller requests cancellation
         *
         * By default passes cancellation to async body.
         *
         * When calling this method manually and the returned handle is not
         * nullptr it must be resumed either with symmetric transfer or in
         * background. Usually this starts some cancellation work, which may
         * eventually result in OnCancel being called, but not necessarily.
         *
         * Important: when scheduling this handle to execute in background it
         * is important to remember async body may return normally before this
         * handle is resumed, which may cause destructors to free memory where
         * decoractor stored this handle. Care must be taken to properly wait
         * until background resume is processed before returning to caller.
         *
         * Note: this method may be called between Bypass and Start when caller
         * is cancelled already. Start will only be called when this method
         * returns nullptr in that case.
         */
        [[nodiscard]] std::coroutine_handle<> Cancel() {
            if (!TAwaitCancelSource::GetCancellation()) {
                return TAwaitCancelSource::Cancel(this->GetCancelHandle());
            } else {
                return nullptr;
            }
        }

        /**
         * Allows modifying startup behavior.
         *
         * Must return a valid coroutine handle which will be resumed.
         *
         * Examples:
         *
         * - GetAsyncBody(): async body starts normally
         * - GetContinuation(): async body does not start, return to caller
         * - GetCancellation(): async body does not start, unwind caller
         * - Any other coroutine handle allows for async setup before
         *   eventually resuming any handle above.
         *
         * Default returns GetAsyncBody().
         */
        [[nodiscard]] std::coroutine_handle<> Start() noexcept {
            return GetAsyncBody();
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
         * Allows modifying behavior just before returning to caller
         */
        void OnReturn() noexcept {
            // nothing
        }

    protected:
        /**
         * Returns task actor
         */
        IActor& GetActor() const {
            return *Actor;
        }

        /**
         * Returns the async callback body for use in OnStart.
         */
        std::coroutine_handle<> GetAsyncBody() const {
            return Coroutine.GetHandle();
        }

        /**
         * Returns the normal return coroutine handle.
         */
        std::coroutine_handle<> GetContinuation() const {
            return Continuation;
        }

        /**
         * Returns the cancelled return coroutine handle for stack unwinding.
         */
        std::coroutine_handle<> GetCancellation() const {
            return Cancellation;
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
            if (!TAwaitCancelSource::GetCancellation()) {
                return TAwaitCancelSource::Cancel(Cancellation);
            } else {
                return nullptr;
            }
        }

    private:
        std::coroutine_handle<> AwaitCancel(std::coroutine_handle<> h) noexcept {
            Cancellation = h;
            return static_cast<TDerived&>(*this).Cancel();
        }

        std::coroutine_handle<> OnResumeHandle() noexcept {
            return static_cast<TDerived&>(*this).OnResume(Continuation);
        }

        std::coroutine_handle<> OnCancelHandle() noexcept {
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
        async<T> Coroutine;
        IActor* Actor = nullptr;
        std::coroutine_handle<> Continuation;
        std::coroutine_handle<> Cancellation;
        NDetail::TAwaitCancelCleanup Cleanup;
    };

} // namespace NActors
