#pragma once

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <library/cpp/yt/logging/logger.h>

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void ReclaimHazardPointers(bool flush = true);

using THazardPtrReclaimer = void(*)(TPackedPtr packedPtr);
void RetireHazardPointer(TPackedPtr packedPtr, THazardPtrReclaimer reclaimer);

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
    THazardPtr(THazardPtr&& other);

    THazardPtr& operator=(const THazardPtr&) = delete;
    THazardPtr& operator=(THazardPtr&& other);

    template <class TPtrLoader>
    static THazardPtr Acquire(TPtrLoader&& ptrLoader, T* ptr);
    template <class TPtrLoader>
    static THazardPtr Acquire(TPtrLoader&& ptrLoader);

    void Reset();

    ~THazardPtr();

    T* Get() const;

    // Operators * and -> are allowed to use only when hazard ptr protects from object
    // destruction (ref count decrementation). Not memory deallocation.
    T& operator*() const;
    T* operator->() const;

    explicit operator bool() const;

private:
    THazardPtr(T* ptr, std::atomic<void*>* hazardPtr);

    T* Ptr_ = nullptr;
    std::atomic<void*>* HazardPtr_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
bool operator==(const THazardPtr<T>& lhs, const T* rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define HAZARD_PTR_INL_H_
#include "hazard_ptr-inl.h"
#undef HAZARD_PTR_INL_H_
