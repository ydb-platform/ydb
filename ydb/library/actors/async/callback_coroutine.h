#pragma once
#include <concepts>
#include <coroutine>

namespace NActors {

    namespace NDetail {

        template<class TCallback>
        concept IsCallbackCoroutineResumeVoid = requires(TCallback& callback) {
            { callback() } noexcept -> std::same_as<void>;
        };

        template<class TCallback>
        concept IsCallbackCoroutineResumeHandle = requires(TCallback& callback) {
            { callback() } noexcept -> std::convertible_to<std::coroutine_handle<>>;
        };

        struct TCallbackCoroutineResume {};

    } // namespace NDetail

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

            static std::suspend_always initial_suspend() noexcept { return {}; }
            static std::suspend_never final_suspend() noexcept { return {}; }
            static void unhandled_exception() noexcept {}
            static void return_void() noexcept {}

            struct TResumeAwaiter {
                static bool await_ready() noexcept { return false; }

                static void await_suspend(std::coroutine_handle<promise_type> self) noexcept
                    requires (NDetail::IsCallbackCoroutineResumeVoid<TCallback>)
                {
                    self.promise()();
                }

                static std::coroutine_handle<> await_suspend(std::coroutine_handle<promise_type> self) noexcept
                    requires (NDetail::IsCallbackCoroutineResumeHandle<TCallback>)
                {
                    return self.promise()();
                }

                static void await_resume() noexcept {}
            };

            static TResumeAwaiter yield_value(NDetail::TCallbackCoroutineResume) {
                return TResumeAwaiter{};
            }
        };

    public:
        constexpr TCallbackCoroutine() noexcept = default;
        constexpr TCallbackCoroutine(std::nullptr_t) noexcept {}

        constexpr explicit TCallbackCoroutine(std::coroutine_handle<promise_type> handle) noexcept
            : Handle(handle)
        {}

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

        constexpr explicit operator bool() const noexcept {
            return bool(Handle);
        }

        constexpr operator std::coroutine_handle<>() const noexcept {
            return Handle;
        }

        constexpr std::coroutine_handle<promise_type> Release() noexcept {
            auto h = Handle;
            Handle = nullptr;
            return h;
        }

        TCallback& operator*() const noexcept {
            return Handle.promise();
        }

        TCallback* operator->() const noexcept {
            return &Handle.promise();
        }

        static std::coroutine_handle<> FromCallback(TCallback& callback) noexcept {
            return std::coroutine_handle<promise_type>::from_promise(static_cast<promise_type&>(callback));
        }

    private:
        std::coroutine_handle<promise_type> Handle;
    };

    /**
     * Allocates a coroutine which subclasses TCallback and calls its operator() on every resume
     */
    template<class TCallback>
    inline TCallbackCoroutine<TCallback> MakeCallbackCoroutine() {
        for (;;) {
            co_yield NDetail::TCallbackCoroutineResume{};
        }
    }

} // namespace NActors
