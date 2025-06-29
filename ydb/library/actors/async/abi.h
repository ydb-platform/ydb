#pragma once
#include <coroutine>

namespace NActors::NDetail {

    /**
     * Common ABI for coroutines is two function pointers for resume and destroy
     */
    struct TCustomCoroutineAbi {
        void (*ResumeFn)(void*);
        void (*DestroyFn)(void*);
    };

    /**
     * Provides std::coroutine_handle<> which translates its resume and destroy to TDerived method calls
     */
    template<class TDerived>
    class TCustomCoroutineCallbacks : private TCustomCoroutineAbi {
    private:
        void* ToAddress() {
            TCustomCoroutineAbi* abi = this;
            return abi;
        }

        static TCustomCoroutineCallbacks& FromAddress(void* ptr) {
            return static_cast<TCustomCoroutineCallbacks&>(
                *reinterpret_cast<TCustomCoroutineAbi*>(ptr));
        }

        static TDerived& DerivedFromPtr(void* ptr) {
            return static_cast<TDerived&>(FromAddress(ptr));
        }

    protected:
        TCustomCoroutineCallbacks() {
            this->ResumeFn = +[](void* ptr) {
                DerivedFromPtr(ptr).OnResume();
            };
            this->DestroyFn = +[](void* ptr) {
                DerivedFromPtr(ptr).OnDestroy();
            };
        }

        TCustomCoroutineCallbacks(const TCustomCoroutineCallbacks&) = delete;
        TCustomCoroutineCallbacks& operator=(const TCustomCoroutineCallbacks&) = delete;

    public:
        std::coroutine_handle<> ToCoroutineHandle() {
            return std::coroutine_handle<>::from_address(ToAddress());
        }
    };

} // namespace NActors::NDetail
