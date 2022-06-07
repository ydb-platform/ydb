#pragma once

#include <util/system/defaults.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/spinlock.h>

#include <library/cpp/sse/sse.h> // The header chooses appropriate SSE support

static_assert(sizeof(TAtomic) == 8, "expect sizeof(TAtomic) == 8");

// we need explicit 32 bit operations to keep cache-line friendly packs
// so have to define some atomics additionaly to arcadia one
#ifdef _win_
#pragma intrinsic(_InterlockedCompareExchange)
#pragma intrinsic(_InterlockedExchangeAdd)
#pragma intrinsic(_InterlockedIncrement)
#pragma intrinsic(_InterlockedDecrement)
#endif

inline bool AtomicUi32Cas(volatile ui32* a, ui32 exchange, ui32 compare) {
#ifdef _win_
    return _InterlockedCompareExchange((volatile long*)a, exchange, compare) == (long)compare;
#else
    ui32 expected = compare;
    return __atomic_compare_exchange_n(a, &expected, exchange, false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
#endif
}

inline ui32 AtomicUi32Add(volatile ui32* a, ui32 add) {
#ifdef _win_
    return _InterlockedExchangeAdd((volatile long*)a, add) + add;
#else
    return __atomic_add_fetch(a, add, __ATOMIC_SEQ_CST);
#endif
}

inline ui32 AtomicUi32Sub(volatile ui32* a, ui32 sub) {
#ifdef _win_
    return _InterlockedExchangeAdd((volatile long*)a, -(long)sub) - sub;
#else
    return __atomic_sub_fetch(a, sub, __ATOMIC_SEQ_CST);
#endif
}

inline ui32 AtomicUi32Increment(volatile ui32* a) {
#ifdef _win_
    return _InterlockedIncrement((volatile long*)a);
#else
    return __atomic_add_fetch(a, 1, __ATOMIC_SEQ_CST);
#endif
}

inline ui32 AtomicUi32Decrement(volatile ui32* a) {
#ifdef _win_
    return _InterlockedDecrement((volatile long*)a);
#else
    return __atomic_sub_fetch(a, 1, __ATOMIC_SEQ_CST);
#endif
}

template <typename T>
inline void AtomicStore(volatile T* a, T x) {
    static_assert(std::is_integral<T>::value || std::is_pointer<T>::value, "expect std::is_integral<T>::value || std::is_pointer<T>::value");
#ifdef _win_
    *a = x;
#else
    __atomic_store_n(a, x, __ATOMIC_RELEASE);
#endif
}

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
inline T AtomicLoad(volatile T* a) {
#ifdef _win_
    return *a;
#else
    return __atomic_load_n(a, __ATOMIC_ACQUIRE);
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
