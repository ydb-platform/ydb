#pragma once

#include <util/thread/singleton.h>

namespace NTSAN {
    template <typename T>
    inline void RelaxedStore(volatile T* a, T x) {
        static_assert(std::is_integral<T>::value || std::is_pointer<T>::value, "expect std::is_integral<T>::value || std::is_pointer<T>::value");
#ifdef _win_
        *a = x;
#else
        __atomic_store_n(a, x, __ATOMIC_RELAXED);
#endif
    }

    template <typename T>
    inline T RelaxedLoad(volatile T* a) {
#ifdef _win_
        return *a;
#else
        return __atomic_load_n(a, __ATOMIC_RELAXED);
#endif
    }

}

void SetCurrentThreadName(const char* name);

namespace NThreadExtra {
    namespace NPrivate {
        template <typename TValue, typename TTag>
        struct TValueHolder {
            TValue Value;
        };
    }
}

template <typename TValue, typename TTag>
static inline TValue* FastTlsSingletonWithTag() {
    return &FastTlsSingleton< ::NThreadExtra::NPrivate::TValueHolder<TValue, TTag>>()->Value;
}
