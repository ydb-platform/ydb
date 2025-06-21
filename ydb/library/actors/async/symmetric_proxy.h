#pragma once
#include <coroutine>
#include <exception>

namespace NActors {

    namespace NDetail {

        struct TSetupSymmetricTransferCallback {
            // Type erased resume callback for symmetric transfer
            struct TBridge {
                std::coroutine_handle<> (*ResumeFn)(TBridge*) noexcept;
            };

            // Empty marker type is elided from arguments
            struct TResume {};

            // A promise type that calls ResumeFn on every co_yield
            struct promise_type {
                TBridge* Bridge;

                TSetupSymmetricTransferCallback get_return_object() noexcept {
                    return { std::coroutine_handle<promise_type>::from_promise(*this) };
                }

                static std::suspend_always initial_suspend() noexcept { return {}; }
                static std::suspend_never final_suspend() noexcept { return {}; }
                static void unhandled_exception() noexcept { std::terminate(); }
                static void return_void() noexcept {}

                struct TResumeAwaiter {
                    static bool await_ready() noexcept { return false; }
                    static void await_resume() noexcept {}
                    static std::coroutine_handle<> await_suspend(std::coroutine_handle<promise_type> self) {
                        TBridge* bridge = self.promise().Bridge;
                        return bridge->ResumeFn(bridge);
                    }
                };

                static TResumeAwaiter yield_value(TResume) { return {}; }
            };

            std::coroutine_handle<promise_type> Handle;
        };

        /**
         * Implementation of a coroutine handle which calls ResumeFn on every resume
         */
        inline TSetupSymmetricTransferCallback SetupSymmetricTransferCallback() {
            for (;;) {
                co_yield TSetupSymmetricTransferCallback::TResume{};
            }
        }

        /**
         * Returns a coroutine handle which calls bridge->ResumeFn on every resume
         */
        inline std::coroutine_handle<> CreateSymmetricTransferCallback(TSetupSymmetricTransferCallback::TBridge* bridge) {
            auto setup = SetupSymmetricTransferCallback();
            setup.Handle.promise().Bridge = bridge;
            return setup.Handle;
        }

        /**
         * Base class for types which need resume interception with symmetric transfer
         *
         * TDerived must implement OnResume method returning std::coroutine_handle<>
         */
        template<class TDerived>
        class TSymmetricTransferCallback : private TSetupSymmetricTransferCallback::TBridge {
        public:
            std::coroutine_handle<> ToCoroutineHandle() const noexcept {
                return Handle;
            }

        protected:
            TSymmetricTransferCallback()
                : Handle(CreateSymmetricTransferCallback(this))
            {
                ResumeFn = +[](TSetupSymmetricTransferCallback::TBridge* base) noexcept -> std::coroutine_handle<> {
                    return static_cast<TDerived&>(static_cast<TSymmetricTransferCallback&>(*base)).OnResume();
                };
            }

            ~TSymmetricTransferCallback() noexcept {
                Handle.destroy();
            }

            TSymmetricTransferCallback(const TSymmetricTransferCallback&) = delete;
            TSymmetricTransferCallback& operator=(const TSymmetricTransferCallback&) = delete;

        private:
            std::coroutine_handle<> Handle;
        };

    } // namespace NDetail

} // namespace NActors
