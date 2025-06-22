#pragma once
#include "callback_coroutine.h"

namespace NActors::NDetail {

    template<class TDerived>
    concept HasOnResume = requires(TDerived& derived) {
        derived.OnResume();
    };

    template<class TDerived>
    concept HasOnDestroy = requires(TDerived& derived) {
        derived.OnDestroy();
    };

    /**
     * Base class for types which need resume interception with symmetric transfer
     *
     * TDerived must implement OnResume method returning std::coroutine_handle<>
     */
    template<class TDerived>
    class TSymmetricTransferCallback {
    public:
        std::coroutine_handle<> ToCoroutineHandle() const noexcept {
            return Coroutine;
        }

    protected:
        TSymmetricTransferCallback()
            : Coroutine{ MakeCallbackCoroutine<TBridge>() }
        {
            Coroutine->Self = this;
        }

        ~TSymmetricTransferCallback() noexcept {
            Coroutine->Self = nullptr;
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
                    if constexpr (HasOnDestroy<TDerived>) {
                        return static_cast<TDerived&>(*Self).OnDestroy();
                    }
                }
            }

            std::coroutine_handle<> operator()() noexcept {
                return static_cast<TDerived&>(*Self).OnResume();
            }
        };

    private:
        TCallbackCoroutine<TBridge> Coroutine;
    };

} // namespace NActors::NDetail
