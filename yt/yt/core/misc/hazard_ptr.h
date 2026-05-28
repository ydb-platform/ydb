#pragma once

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <library/cpp/yt/memory/tagged_ptr.h>

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TRefCountedBase;

////////////////////////////////////////////////////////////////////////////////

void ReclaimHazardPointers(bool flush = true);

using THazardPtrReclaimer = void(*)(void* reclaimPtr);
void RetireHazardPointer(
    void* protectedPtr,
    void* reclaimPtr,
    THazardPtrReclaimer reclaimer);

//! NB: #reclaimer must be stateless.
template <class T, class TReclaimer>
void RetireHazardPointer(T* ptr, TReclaimer reclaimer);

////////////////////////////////////////////////////////////////////////////////

//! Relcaims hazard pointers on destruction.
struct THazardPtrReclaimGuard
{
    ~THazardPtrReclaimGuard();
};

////////////////////////////////////////////////////////////////////////////////

//! Relcaims hazard pointers on destruction and also on context switch.
struct THazardPtrReclaimOnContextSwitchGuard
    : public THazardPtrReclaimGuard
    , public NConcurrency::TContextSwitchGuard
{
    THazardPtrReclaimOnContextSwitchGuard();
};

////////////////////////////////////////////////////////////////////////////////

//! Protects an object from destruction (or deallocation) before CAS.
//! Destruction or deallocation depends on delete callback in ScheduleObjectDeletion.
template <class T>
class THazardPtr
{
public:
    static_assert(T::EnableHazard, "T::EnableHazard must be true.");

    THazardPtr() = default;
    THazardPtr(const THazardPtr&) = delete;
    THazardPtr(THazardPtr&& other) noexcept;

    THazardPtr& operator=(const THazardPtr&) = delete;
    THazardPtr& operator=(THazardPtr&& other) noexcept;

    //! Reject types that virtually inherit TRefCountedBase.
    //! Such types must go through TAtomicPtr<T>.
    template <class TPtrLoader>
    static THazardPtr Acquire(TPtrLoader&& ptrLoader, T* ptr)
        requires (!std::derived_from<T, TRefCountedBase> ||
            requires(TRefCountedBase* b) { static_cast<T*>(b); });
    template <class TPtrLoader>
    static THazardPtr Acquire(TPtrLoader&& ptrLoader);

    void Reset() noexcept;

    ~THazardPtr();

    T* Get() const;

    // Operators * and -> are allowed to use only when hazard ptr protects from object
    // destruction (ref count decrementation). Not memory deallocation.
    T& operator*() const;
    T* operator->() const;

    explicit operator bool() const;

private:
    // TAtomicPtr stores a precomputed vbase offset alongside the pointer and
    // uses this overload to publish the canonical base address into the
    // hazard slot without dereferencing |*ptr| — needed when T inherits
    // TRefCountedBase virtually.
    template <class T_, bool EnableAcquireHazard_>
    friend class TAtomicPtr;

    template <class TPtrLoader>
    static THazardPtr Acquire(TPtrLoader&& ptrLoader, TTaggedPtr<T> taggedPtr);

    THazardPtr(T* ptr, std::atomic<void*>* hazardPtr, void* protectedPtr);

    T* Ptr_ = nullptr;
    std::atomic<void*>* HazardPtr_ = nullptr;
#ifndef NDEBUG
    // The value last published into HazardPtr_'s slot. Saved here so Reset's
    // YT_VERIFY can confirm no one else has overwritten the slot — we cannot
    // reconstruct it from Ptr_ alone when virtual inheritance makes the
    // canonical base address differ from the typed pointer.
    void* ProtectedPtr_ = nullptr;
#endif
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
bool operator==(const THazardPtr<T>& lhs, const T* rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define HAZARD_PTR_INL_H_
#include "hazard_ptr-inl.h"
#undef HAZARD_PTR_INL_H_
