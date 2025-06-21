#pragma once
#include <coroutine>
#include <exception>
#include <optional>
#include <type_traits>
#include <utility>

namespace NActors {

    namespace NDetail {

        template<class TCallback>
        concept IsResumeCallbackVoid = requires(TCallback& callback) {
            { callback() } noexcept -> std::same_as<void>;
        };

        template<class TCallback>
        concept IsResumeCallbackHandle = requires(TCallback& callback) {
            { callback() } noexcept -> std::convertible_to<std::coroutine_handle<>>;
        };

        template<class TCallback>
        class TCallbackCoroutinePromise;

        template<class TCallback>
        class TCallbackCoroutineTemporary {
        public:
            using promise_type = TCallbackCoroutinePromise<TCallback>;

            TCallbackCoroutineTemporary(std::coroutine_handle<promise_type> handle)
                : Handle(handle)
            {}

            TCallbackCoroutineTemporary(const TCallbackCoroutineTemporary&) = delete;
            TCallbackCoroutineTemporary& operator=(const TCallbackCoroutineTemporary&) = delete;

            ~TCallbackCoroutineTemporary() {
                if (Handle) {
                    Handle.destroy();
                }
            }

            std::coroutine_handle<promise_type> Release() {
                return std::exchange(Handle, nullptr);
            }

        public:
            std::coroutine_handle<promise_type> Handle;
        };

        struct TCallbackCoroutineResume {};

        template<class TCallback>
        class TCallbackCoroutinePromise {
        public:
            template<class... TArgs>
            void Init(TArgs&&... args) {
                Callback.emplace(std::forward<TArgs>(args)...);
            }

            TCallbackCoroutineTemporary<TCallback> get_return_object() noexcept {
                return TCallbackCoroutineTemporary<TCallback>(
                    std::coroutine_handle<TCallbackCoroutinePromise>::from_promise(*this));
            }

            static auto initial_suspend() noexcept { return std::suspend_always{}; }
            static auto final_suspend() noexcept { return std::suspend_never{}; }

            static void unhandled_exception() noexcept { std::terminate(); }
            static void return_void() noexcept {}

            struct TResume {
                static bool await_ready() noexcept { return false; }

                static void await_suspend(std::coroutine_handle<TCallbackCoroutinePromise> self) noexcept
                    requires (IsResumeCallbackVoid<TCallback>)
                {
                    self.promise().GetCallback()();
                }

                static std::coroutine_handle<> await_suspend(std::coroutine_handle<TCallbackCoroutinePromise> self) noexcept
                    requires (IsResumeCallbackHandle<TCallback>)
                {
                    return self.promise().GetCallback()();
                }

                static void await_resume() noexcept {}
            };

            static TResume yield_value(TCallbackCoroutineResume) {
                return TResume{};
            }

            TCallback& GetCallback() noexcept {
                return *Callback;
            }

        private:
            std::optional<TCallback> Callback;
        };

        template<class TCallback>
        TCallbackCoroutineTemporary<TCallback> AllocateCallbackCoroutine() {
            for (;;) {
                co_yield TCallbackCoroutineResume{};
            }
        }

    } // namespace NDetail

    template<class TCallback>
    class [[nodiscard]] TCallbackCoroutine {
        using THandle = std::coroutine_handle<NDetail::TCallbackCoroutinePromise<TCallback>>;

    public:
        TCallbackCoroutine() = default;

        ~TCallbackCoroutine() {
            if (Handle_) {
                Handle_.destroy();
            }
        }

        TCallbackCoroutine(TCallbackCoroutine&& rhs) noexcept
            : Handle_(rhs.Handle_)
        {
            rhs.Handle_ = {};
        }

        TCallbackCoroutine& operator=(TCallbackCoroutine&& rhs) noexcept {
            if (this != &rhs) {
                if (Handle_) {
                    Handle_.destroy();
                }
                Handle_ = rhs.Handle_;
                rhs.Handle_ = {};
            }
            return *this;
        }

        explicit operator bool() const noexcept {
            return bool(Handle_);
        }

        operator std::coroutine_handle<>() const noexcept {
            return Handle_;
        }

        TCallback& operator*() {
            return Handle_.promise().GetCallback();
        }

        const TCallback& operator*() const {
            return Handle_.promise().GetCallback();
        }

        TCallback* operator->() {
            return &Handle_.promise().GetCallback();
        }

        const TCallback* operator->() const {
            return &Handle_.promise().GetCallback();
        }

        template<class T, class... TArgs>
        friend TCallbackCoroutine<T> MakeCallbackCoroutine(TArgs&&... args);

    private:
        explicit TCallbackCoroutine(THandle handle)
            : Handle_(handle)
        {}

    private:
        THandle Handle_;
    };

    template<class TCallback, class... TArgs>
    TCallbackCoroutine<TCallback> MakeCallbackCoroutine(TArgs&&... args) {
        auto tmp = NDetail::AllocateCallbackCoroutine<TCallback>();
        tmp.Handle.promise().Init(std::forward<TArgs>(args)...);
        return TCallbackCoroutine<TCallback>(tmp.Release());
    }

} // namespace NActors
