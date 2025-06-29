#pragma once
#include "callback_coroutine.h"

namespace NActors::NDetail {

    template<class TDerived>
    concept HasOnCoroutineHandleResume = requires(TDerived& derived) {
        derived.OnCoroutineHandleResume();
    };

    template<class TDerived>
    concept HasOnCoroutineHandleDestroy = requires(TDerived& derived) {
        derived.OnCoroutineHandleDestroy();
    };

    /**
     * Base class for types which need coroutine handle with resume/destroy
     * interception and with support for symmetric transfer.
     *
     * TDerived must implement `std::coroutine_handle<> OnCoroutineHandleResume()`.
     */
    template<class TDerived, bool Lazy = false>
    class TSymmetricTransferCallback {
    public:
        std::coroutine_handle<> GetCoroutineHandle() noexcept(!Lazy) {
            if constexpr (Lazy) {
                if (!Coroutine) {
                    Coroutine = Create();
                }
            }
            return Coroutine;
        }

    protected:
        TSymmetricTransferCallback()
            : Coroutine{ CreateInitial() }
        {}

        ~TSymmetricTransferCallback() noexcept {
            if (Coroutine) {
                Coroutine->Self = nullptr;
            }
        }

        TSymmetricTransferCallback(TSymmetricTransferCallback&&) = delete;
        TSymmetricTransferCallback(const TSymmetricTransferCallback&) = delete;
        TSymmetricTransferCallback& operator=(TSymmetricTransferCallback&&) = delete;
        TSymmetricTransferCallback& operator=(const TSymmetricTransferCallback&) = delete;

    private:
        struct TBridge {
            TSymmetricTransferCallback* Self = nullptr;

            ~TBridge() noexcept {
                if (Self) {
                    Self->Coroutine.Release();
                    if constexpr (HasOnCoroutineHandleDestroy<TDerived>) {
                        return static_cast<TDerived&>(*Self).OnCoroutineHandleDestroy();
                    }
                }
            }

            std::coroutine_handle<> operator()() noexcept {
                return static_cast<TDerived&>(*Self).OnCoroutineHandleResume();
            }
        };

        inline TCallbackCoroutine<TBridge> Create() {
            auto c = MakeCallbackCoroutine<TBridge>();
            c->Self = this;
            return c;
        }

        inline TCallbackCoroutine<TBridge> CreateInitial() {
            if constexpr (!Lazy) {
                return Create();
            } else {
                return nullptr;
            }
        }

    private:
        TCallbackCoroutine<TBridge> Coroutine;
    };

    /**
     * Provides a common resume interception coroutine handle.
     *
     * TDerived must implement `std::coroutine_handle<> OnResumeHandle()`.
     */
    template<class TDerived, bool Lazy = false>
    class TSymmetricResumeCallback
        : private TSymmetricTransferCallback<TSymmetricResumeCallback<TDerived, Lazy>, Lazy>
    {
        friend TSymmetricTransferCallback<TSymmetricResumeCallback<TDerived, Lazy>, Lazy>;

    protected:
        TSymmetricResumeCallback() = default;

        std::coroutine_handle<> GetResumeHandle() noexcept(!Lazy) {
            return this->GetCoroutineHandle();
        }

    private:
        std::coroutine_handle<> OnCoroutineHandleResume() noexcept {
            return static_cast<TDerived&>(*this).OnResumeHandle();
        }
    };

    /**
     * Provides a common cancel interception coroutine handle.
     *
     * TDerived must implement `std::coroutine_handle<> OnCancelHandle()`.
     */
    template<class TDerived, bool Lazy = false>
    class TSymmetricCancelCallback
        : private TSymmetricTransferCallback<TSymmetricCancelCallback<TDerived, Lazy>, Lazy>
    {
        friend TSymmetricTransferCallback<TSymmetricCancelCallback<TDerived, Lazy>, Lazy>;

    protected:
        TSymmetricCancelCallback() = default;

        std::coroutine_handle<> GetCancelHandle() noexcept(!Lazy) {
            return this->GetCoroutineHandle();
        }

    private:
        std::coroutine_handle<> OnCoroutineHandleResume() noexcept {
            return static_cast<TDerived&>(*this).OnCancelHandle();
        }
    };

} // namespace NActors::NDetail
