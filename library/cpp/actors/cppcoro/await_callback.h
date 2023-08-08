#include <coroutine>
#include <exception>
#include <concepts>

namespace NActors {

    namespace NDetail {

        template<class TAwaitable>
        decltype(auto) GetAwaiter(TAwaitable&& awaitable) {
            if constexpr (requires { ((TAwaitable&&) awaitable).operator co_await(); }) {
                return ((TAwaitable&&) awaitable).operator co_await();
            } else if constexpr (requires { operator co_await((TAwaitable&&) awaitable); }) {
                return operator co_await((TAwaitable&&) awaitable);
            } else {
                return ((TAwaitable&&) awaitable);
            }
        }

        template<class TAwaitable>
        using TAwaitResult = decltype(GetAwaiter(std::declval<TAwaitable>()).await_resume());

        template<class TCallback, class TResult>
        class TCallbackResult {
        public:
            TCallbackResult(TCallback& callback)
                : Callback(callback)
            {}

            template<class TRealResult>
            void return_value(TRealResult&& result) noexcept {
                Callback(std::forward<TRealResult>(result));
            }

        private:
            TCallback& Callback;
        };

        template<class TCallback>
        class TCallbackResult<TCallback, void> {
        public:
            TCallbackResult(TCallback& callback)
                : Callback(callback)
            {}

            void return_void() noexcept {
                Callback();
            }

        private:
            TCallback& Callback;
        };

        template<class TAwaitable, class TCallback>
        class TAwaitThenCallbackPromise
            : public TCallbackResult<TCallback, TAwaitResult<TAwaitable>>
        {
        public:
            using THandle = std::coroutine_handle<TAwaitThenCallbackPromise<TAwaitable, TCallback>>;

            TAwaitThenCallbackPromise(TAwaitable&, TCallback& callback)
                : TCallbackResult<TCallback, TAwaitResult<TAwaitable>>(callback)
            {}

            THandle get_return_object() noexcept {
                return THandle::from_promise(*this);
            }

            static auto initial_suspend() noexcept { return std::suspend_never{}; }
            static auto final_suspend() noexcept { return std::suspend_never{}; }

            void unhandled_exception() noexcept {
                std::terminate();
            }
        };

        template<class TAwaitable, class TCallback>
        class TAwaitThenCallback {
        public:
            using promise_type = TAwaitThenCallbackPromise<TAwaitable, TCallback>;

            using THandle = typename promise_type::THandle;

            TAwaitThenCallback(THandle) noexcept {}
        };

    } // namespace NDetail

    /**
     * Awaits the awaitable and calls callback with the result.
     *
     * Note: program terminates if awaitable or callback throw an exception.
     */
    template<class TAwaitable, class TCallback>
    NDetail::TAwaitThenCallback<TAwaitable, TCallback> AwaitThenCallback(TAwaitable awaitable, TCallback) {
        // Note: underlying promise takes callback argument address and calls it when we return
        co_return co_await std::move(awaitable);
    }

} // namespace NActors
