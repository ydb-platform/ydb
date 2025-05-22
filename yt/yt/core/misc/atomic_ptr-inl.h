#ifndef ATOMIC_PTR_INL_H_
#error "Direct inclusion of this file is not allowed, include atomic_ptr.h"
// For the sake of sane code completion.
#include "atomic_ptr.h"
#endif
#undef ATOMIC_PTR_INL_H_

#include "private.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class T>
TIntrusivePtr<T> TryMakeStrongFromHazard(const THazardPtr<T>& ptr)
{
    if (!ptr) {
        return nullptr;
    }

    if (!GetRefCounter(ptr.Get())->TryRef()) {
        constexpr auto& Logger = LockFreeLogger;
        YT_LOG_TRACE("Failed to acquire intrusive ptr from hazard ptr (Ptr: %v)",
            ptr.Get());
        return nullptr;
    }

    return TIntrusivePtr<T>(ptr.Get(), /*addReference*/ false);
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class T, bool EnableAcquireHazard>
TAtomicPtr<T, EnableAcquireHazard>::TAtomicPtr(std::nullptr_t)
{ }

template <class T, bool EnableAcquireHazard>
TAtomicPtr<T, EnableAcquireHazard>::TAtomicPtr(TIntrusivePtr<T> other)
    : Ptr_(other.Release())
{ }

template <class T, bool EnableAcquireHazard>
TAtomicPtr<T, EnableAcquireHazard>::TAtomicPtr(TAtomicPtr&& other)
    : Ptr_(other.Ptr_)
{
    other.Ptr_ = nullptr;
}

template <class T, bool EnableAcquireHazard>
TAtomicPtr<T, EnableAcquireHazard>::TAtomicPtr(T* ptr)
    : Ptr_(ptr)
{ }

template <class T, bool EnableAcquireHazard>
TAtomicPtr<T, EnableAcquireHazard>::~TAtomicPtr()
{
    Drop(Ptr_.load());
}

template <class T, bool EnableAcquireHazard>
void TAtomicPtr<T, EnableAcquireHazard>::Drop(T* ptr)
{
    if (ptr) {
        if constexpr (EnableAcquireHazard) {
            RetireHazardPointer(ptr, [] (T* ptr) {
                Unref(ptr);
            });
        } else {
            Unref(ptr);
        }
    }
}

template <class T, bool EnableAcquireHazard>
TAtomicPtr<T, EnableAcquireHazard>& TAtomicPtr<T, EnableAcquireHazard>::operator=(TIntrusivePtr<T> other)
{
    Exchange(std::move(other));
    return *this;
}

template <class T, bool EnableAcquireHazard>
TAtomicPtr<T, EnableAcquireHazard>& TAtomicPtr<T, EnableAcquireHazard>::operator=(std::nullptr_t)
{
    Exchange(TIntrusivePtr<T>());
    return *this;
}

template <class T, bool EnableAcquireHazard>
void TAtomicPtr<T, EnableAcquireHazard>::Reset()
{
    Exchange(TIntrusivePtr<T>());
}

template <class T, bool EnableAcquireHazard>
THazardPtr<T> TAtomicPtr<T, EnableAcquireHazard>::AcquireHazard() const
{
    static_assert(EnableAcquireHazard, "EnableAcquireHazard must be true");
    return DoAcquireHazard();
}

template <class T, bool EnableAcquireHazard>
THazardPtr<T> TAtomicPtr<T, EnableAcquireHazard>::DoAcquireHazard() const
{
    return THazardPtr<T>::Acquire([&] {
        return Ptr_.load(std::memory_order::acquire);
    });
}

template <class T, bool EnableAcquireHazard>
TIntrusivePtr<T> TAtomicPtr<T, EnableAcquireHazard>::AcquireWeak() const
{
    return NYT::NDetail::TryMakeStrongFromHazard(DoAcquireHazard());
}

template <class T, bool EnableAcquireHazard>
TIntrusivePtr<T> TAtomicPtr<T, EnableAcquireHazard>::Acquire() const
{
    while (auto hazardPtr = DoAcquireHazard()) {
        if (auto ptr = NYT::NDetail::TryMakeStrongFromHazard(hazardPtr)) {
            return ptr;
        }
    }
    return nullptr;
}

template <class T, bool EnableAcquireHazard>
TAtomicPtr<T, EnableAcquireHazard> TAtomicPtr<T, EnableAcquireHazard>::Exchange(TIntrusivePtr<T> other)
{
    auto* oldPtr = Ptr_.exchange(other.Release());
    return TAtomicPtr<T, EnableAcquireHazard>(oldPtr);
}

template <class T, bool EnableAcquireHazard>
void TAtomicPtr<T, EnableAcquireHazard>::Store(TIntrusivePtr<T> other)
{
    Exchange(std::move(other));
}

template <class T, bool EnableAcquireHazard>
TAtomicPtr<T, EnableAcquireHazard> TAtomicPtr<T, EnableAcquireHazard>::SwapIfCompare(THazardPtr<T>& compare, TIntrusivePtr<T> target)
{
    auto* comparePtr = compare.Get();
    auto* targetPtr = target.Get();
    if (Ptr_.compare_exchange_strong(comparePtr, targetPtr)) {
        target.Release();
        return TAtomicPtr<T, EnableAcquireHazard>(comparePtr);
    } else {
        compare.Reset();
        compare = THazardPtr<T>::Acquire([&] {
            return Ptr_.load(std::memory_order::acquire);
        }, comparePtr);
    }
    return {};
}

template <class T, bool EnableAcquireHazard>
bool TAtomicPtr<T, EnableAcquireHazard>::SwapIfCompare(T* comparePtr, TIntrusivePtr<T> target)
{
    constexpr auto& Logger = LockFreeLogger;

    auto* targetPtr = target.Get();
    auto* savedPtr = comparePtr;
    if (!Ptr_.compare_exchange_strong(comparePtr, targetPtr)) {
        YT_LOG_TRACE("CAS failed (Current: %v, Compare: %v, Target: %v)",
            comparePtr,
            savedPtr,
            targetPtr);
        return false;
    }

    YT_LOG_TRACE("CAS succeeded (Compare: %v, Target: %v)",
        comparePtr,
        targetPtr);
    target.Release();
    Drop(comparePtr);
    return true;
}

template <class T, bool EnableAcquireHazard>
bool TAtomicPtr<T, EnableAcquireHazard>::SwapIfCompare(const TIntrusivePtr<T>& compare, TIntrusivePtr<T> target)
{
    return SwapIfCompare(compare.Get(), std::move(target));
}

template <class T, bool EnableAcquireHazard>
bool TAtomicPtr<T, EnableAcquireHazard>::SwapIfCompare(const TIntrusivePtr<T>& compare, TIntrusivePtr<T>* target)
{
    auto* ptr = compare.Get();
    if (Ptr_.compare_exchange_strong(ptr, target->Ptr_)) {
        target->Ptr_ = ptr;
        return true;
    }
    return false;
}

template <class T, bool EnableAcquireHazard>
TAtomicPtr<T, EnableAcquireHazard>::operator bool() const
{
    return Ptr_.load() != nullptr;
}

////////////////////////////////////////////////////////////////////////////////

template <class T, bool EnableAcquireHazard>
bool operator==(const TAtomicPtr<T, EnableAcquireHazard>& lhs, const T* rhs)
{
    return lhs.Ptr_.load() == rhs;
}

template <class T, bool EnableAcquireHazard>
bool operator==(const T* lhs, const TAtomicPtr<T, EnableAcquireHazard>& rhs)
{
    return lhs == rhs.Ptr_.load();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
