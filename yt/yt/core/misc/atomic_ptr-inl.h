#ifndef ATOMIC_PTR_INL_H_
#error "Direct inclusion of this file is not allowed, include atomic_ptr.h"
// For the sake of sane code completion.
#include "atomic_ptr.h"
#endif
#undef ATOMIC_PTR_INL_H_

#include "private.h"

#include <library/cpp/yt/memory/ref_counted.h>

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
TTaggedPtr<T> TAtomicPtr<T, EnableAcquireHazard>::MakeTagged(T* ptr)
{
    if (!ptr) {
        return {};
    }
    // Compute the vbase offset *now*, while the caller still owns a strong
    // reference. The lookup is safe; the resulting offset can later be applied
    // to a (possibly retired) |ptr| value without rereading the object.
    if constexpr (std::derived_from<T, TRefCountedBase>) {
        auto offset = reinterpret_cast<uintptr_t>(static_cast<TRefCountedBase*>(ptr)) -
            reinterpret_cast<uintptr_t>(ptr);
        YT_ASSERT(offset < (1ULL << PackedPtrTagBits));
        return TTaggedPtr<T>{ptr, static_cast<ui16>(offset)};
    } else {
        return TTaggedPtr<T>{ptr, 0};
    }
}

template <class T, bool EnableAcquireHazard>
TTaggedPtr<T> TAtomicPtr<T, EnableAcquireHazard>::UnpackTagged(TPackedPtr packed)
{
    return TTaggedPtr<T>::Unpack(packed);
}

template <class T, bool EnableAcquireHazard>
TTaggedPtr<T> TAtomicPtr<T, EnableAcquireHazard>::LoadTagged() const
{
    return UnpackTagged(Ptr_.load(std::memory_order::acquire));
}

template <class T, bool EnableAcquireHazard>
TAtomicPtr<T, EnableAcquireHazard>::TAtomicPtr(std::nullptr_t)
{ }

template <class T, bool EnableAcquireHazard>
TAtomicPtr<T, EnableAcquireHazard>::TAtomicPtr(TIntrusivePtr<T> other)
    : Ptr_(MakeTagged(other.Release()).Pack())
{ }

template <class T, bool EnableAcquireHazard>
TAtomicPtr<T, EnableAcquireHazard>::TAtomicPtr(TAtomicPtr&& other) noexcept
    : Ptr_(other.Ptr_.load())
{
    other.Ptr_ = TPackedPtr{};
}

template <class T, bool EnableAcquireHazard>
TAtomicPtr<T, EnableAcquireHazard>::TAtomicPtr(TTaggedPtr<T> taggedPtr)
    : Ptr_(taggedPtr.Pack())
{ }

template <class T, bool EnableAcquireHazard>
TAtomicPtr<T, EnableAcquireHazard>::~TAtomicPtr()
{
    Drop(LoadTagged());
}

template <class T, bool EnableAcquireHazard>
void TAtomicPtr<T, EnableAcquireHazard>::Drop(TTaggedPtr<T> taggedPtr)
{
    auto* ptr = taggedPtr.Ptr;
    if (!ptr) {
        return;
    }
    if constexpr (EnableAcquireHazard) {
        // protectedPtr must match the value the reader publishes into the
        // hazard slot — that is the canonical TRefCountedBase* address,
        // computed without dereferencing |*ptr|. reclaimPtr is the typed
        // pointer the deleter expects.
        void* protectedPtr = reinterpret_cast<char*>(ptr) + taggedPtr.Tag;
        RetireHazardPointer(
            protectedPtr,
            ptr,
            [] (void* reclaimPtr) { Unref(static_cast<T*>(reclaimPtr)); });
    } else {
        Unref(ptr);
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
        return LoadTagged();
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
    auto newPacked = MakeTagged(other.Release()).Pack();
    auto oldPacked = Ptr_.exchange(newPacked);
    return TAtomicPtr<T, EnableAcquireHazard>(UnpackTagged(oldPacked));
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
    auto comparePacked = MakeTagged(comparePtr).Pack();
    auto newPacked = MakeTagged(target.Get()).Pack();
    if (Ptr_.compare_exchange_strong(comparePacked, newPacked)) {
        target.Release();
        return TAtomicPtr<T, EnableAcquireHazard>(MakeTagged(comparePtr));
    } else {
        compare.Reset();
        compare = THazardPtr<T>::Acquire(
            [&] { return LoadTagged(); },
            UnpackTagged(comparePacked));
    }
    return {};
}

template <class T, bool EnableAcquireHazard>
bool TAtomicPtr<T, EnableAcquireHazard>::SwapIfCompare(T* comparePtr, TIntrusivePtr<T> target)
{
    constexpr auto& Logger = LockFreeLogger;

    auto* targetPtr = target.Get();
    auto* savedPtr = comparePtr;
    auto comparePacked = MakeTagged(comparePtr).Pack();
    auto newPacked = MakeTagged(targetPtr).Pack();
    if (!Ptr_.compare_exchange_strong(comparePacked, newPacked)) {
        YT_LOG_TRACE("CAS failed (Current: %v, Compare: %v, Target: %v)",
            UnpackTagged(comparePacked).Ptr,
            savedPtr,
            targetPtr);
        return false;
    }

    YT_LOG_TRACE("CAS succeeded (Compare: %v, Target: %v)",
        comparePtr,
        targetPtr);
    target.Release();
    Drop(MakeTagged(comparePtr));
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
    auto comparePacked = MakeTagged(compare.Get()).Pack();
    auto newPacked = MakeTagged(target->Ptr_).Pack();
    if (Ptr_.compare_exchange_strong(comparePacked, newPacked)) {
        target->Ptr_ = UnpackTagged(comparePacked).Ptr;
        return true;
    }
    return false;
}

template <class T, bool EnableAcquireHazard>
TAtomicPtr<T, EnableAcquireHazard>::operator bool() const
{
    return LoadTagged().Ptr != nullptr;
}

////////////////////////////////////////////////////////////////////////////////

template <class T, bool EnableAcquireHazard>
bool operator==(const TAtomicPtr<T, EnableAcquireHazard>& lhs, const T* rhs)
{
    return lhs.LoadTagged().Ptr == rhs;
}

template <class T, bool EnableAcquireHazard>
bool operator==(const T* lhs, const TAtomicPtr<T, EnableAcquireHazard>& rhs)
{
    return lhs == rhs.LoadTagged().Ptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
