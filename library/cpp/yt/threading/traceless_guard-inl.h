#pragma once

#ifndef TRACELESS_GUARD_INL_H_
#error "Direct inclusion of this file is not allowed, include traceless_guard.h"
#endif
#undef TRACELESS_GUARD_INL_H_

#include "rw_spin_lock.h"
#include "spin_lock_count.h"

#include <util/system/guard.h>

#include <concepts>

namespace NYT::NThreading::NPrivate {

////////////////////////////////////////////////////////////////////////////////

#ifdef NDEBUG

template <class T, class TOps = TCommonLockOps<T>>
using TTracelessGuard = TGuard<T, TOps>;

template <class T, class TOps = TCommonLockOps<T>>
using TTracelessInverseGuard = TInverseGuard<T, TOps>;

template <class T, class TOps = TTryLockOps<T>>
using TTracelessTryGuard = TTryGuard<T, TOps>;

template <class T>
using TTracelessReaderGuard = TReaderGuard<T>;

template <class T>
using TTracelessWriterGuard = TWriterGuard<T>;

#else

template <class TOps>
struct TTracelessOps
    : public TOps
{
    template <class T>
    static void Acquire(T* t) noexcept
    {
        TOps::Acquire(t);
        NDetail::RecordSpinLockReleased();
    }

    template <class T>
    static void Release(T* t) noexcept
    {
        NDetail::RecordSpinLockAcquired();
        TOps::Release(t);
    }

    template <class T>
        requires requires (T* t) { { TOps::TryAcquire(t) } -> std::same_as<bool>; }
    static bool TryAcquire(T* t) noexcept
    {
        bool isAcquired = TOps::TryAcquire(t);

        if (isAcquired) {
            NDetail::RecordSpinLockReleased();
        }

        return isAcquired;
    }
};

template <CTrackedSpinLock T, class TOps = TCommonLockOps<T>>
using TTracelessGuard = TGuard<T, TTracelessOps<TOps>>;

template <CTrackedSpinLock T, class TOps = TCommonLockOps<T>>
using TTracelessInverseGuard = TInverseGuard<T, TTracelessOps<TOps>>;

template <CTrackedSpinLock T, class TOps = TTryLockOps<T>>
using TTracelessTryGuard = TTryGuard<T, TTracelessOps<TOps>>;

template <CTrackedSpinLock T>
using TTracelessReaderGuard = TGuard<T, TTracelessOps<TReaderSpinlockTraits<T>>>;

template <CTrackedSpinLock T>
using TTracelessWriterGuard = TGuard<T, TTracelessOps<TWriterSpinlockTraits<T>>>;

#endif

////////////////////////////////////////////////////////////////////////////////

template <CTrackedSpinLock T>
TTracelessGuard<T> TracelessGuard(const T& mutex)
{
    return {&mutex};
}

template <CTrackedSpinLock T>
TTracelessTryGuard<T> TracelessTryGuard(const T& mutex)
{
    return {&mutex};
}

template <CTrackedSpinLock T>
TTracelessReaderGuard<T> TracelessReaderGuard(const T& mutex)
{
    return {&mutex};
}

template <CTrackedSpinLock T>
TTracelessWriterGuard<T> TracelessWriterGuard(const T& mutex)
{
    return {&mutex};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NThreading::NPrivate
