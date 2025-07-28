#pragma once
#include <concepts>
#include <coroutine>

namespace NActors::NDetail {

    /**
     * Special value yielded on resume internally by callback coroutines
     */
    struct TCallbackCoroutineResume {};

    /**
     * A coroutine class which subclasses TCallback and calls its operator() on every resume
     */
    template<class TCallback>
    class TCallbackCoroutine {
    public:
        struct promise_type : public TCallback {
            TCallbackCoroutine<TCallback> get_return_object() noexcept {
                return TCallbackCoroutine<TCallback>(std::coroutine_handle<promise_type>::from_promise(*this));
            }

            static constexpr std::suspend_always initial_suspend() noexcept { return {}; }
            static constexpr std::suspend_always final_suspend() noexcept { return {}; }
            static void unhandled_exception() { throw; }
            static constexpr void return_void() noexcept {}

            struct TResumeAwaiter {
                static constexpr bool await_ready() noexcept { return false; }

                static void await_suspend(std::coroutine_handle<promise_type> self)
                    noexcept(noexcept(self.promise()()))
                    // Note: not a named concept for friends to work correctly
                    requires requires { { self.promise()() } -> std::same_as<void>; }
                {
                    self.promise()();
                }

                static std::coroutine_handle<> await_suspend(std::coroutine_handle<promise_type> self)
                    noexcept(noexcept(self.promise()()))
                    // Note: not a named concept for friends to work correctly
                    requires requires { { self.promise()() } -> std::convertible_to<std::coroutine_handle<>>; }
                {
                    return self.promise()();
                }

                static void await_resume() noexcept {}
            };

            static TResumeAwaiter yield_value(TCallbackCoroutineResume) {
                return TResumeAwaiter{};
            }
        };

    public:
        constexpr TCallbackCoroutine() noexcept = default;
        constexpr TCallbackCoroutine(std::nullptr_t) noexcept {}

        TCallbackCoroutine(TCallbackCoroutine&& rhs) noexcept
            : Handle(rhs.Handle)
        {
            rhs.Handle = nullptr;
        }

        TCallbackCoroutine& operator=(std::nullptr_t) noexcept {
            if (Handle) {
                Handle.destroy();
                Handle = nullptr;
            }
            return *this;
        }

        TCallbackCoroutine& operator=(TCallbackCoroutine&& rhs) noexcept {
            if (this != &rhs) [[likely]] {
                if (Handle) {
                    Handle.destroy();
                }
                Handle = rhs.Handle;
                rhs.Handle = nullptr;
            }
            return *this;
        }

        ~TCallbackCoroutine() noexcept {
            if (Handle) {
                Handle.destroy();
            }
        }

        constexpr std::coroutine_handle<promise_type> GetHandle() const noexcept {
            return Handle;
        }

        constexpr std::coroutine_handle<promise_type> Release() noexcept {
            auto h = Handle;
            Handle = nullptr;
            return h;
        }

        constexpr explicit operator bool() const noexcept {
            return bool(Handle);
        }

        constexpr operator std::coroutine_handle<>() const noexcept {
            return Handle;
        }

        constexpr operator std::coroutine_handle<promise_type>() const noexcept {
            return Handle;
        }

        TCallback& operator*() const noexcept {
            return Handle.promise();
        }

        TCallback* operator->() const noexcept {
            return &Handle.promise();
        }

        bool done() const {
            return Handle.done();
        }

        void resume() const {
            Handle.resume();
        }

        void operator()() const {
            Handle();
        }

        void destroy() {
            if (Handle) {
                Handle.destroy();
                Handle = nullptr;
            }
        }

        static std::coroutine_handle<> FromCallback(TCallback& callback) {
            return std::coroutine_handle<promise_type>::from_promise(static_cast<promise_type&>(callback));
        }

    private:
        constexpr explicit TCallbackCoroutine(std::coroutine_handle<promise_type> handle) noexcept
            : Handle(handle)
        {}

    private:
        std::coroutine_handle<promise_type> Handle;
    };

    /**
     * Allocates a coroutine which subclasses TCallback and calls its operator() on every resume
     */
    template<class TCallback>
    inline TCallbackCoroutine<TCallback> MakeCallbackCoroutine() {
        for (;;) {
            co_yield TCallbackCoroutineResume{};
        }
    }

} // namespace NActors::NDetail
