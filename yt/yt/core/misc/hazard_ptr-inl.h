#ifndef HAZARD_PTR_INL_H_
#error "Direct inclusion of this file is not allowed, include hazard_ptr.h"
// For the sake of sane code completion.
#include "hazard_ptr.h"
#endif
#undef HAZARD_PTR_INL_H_

#include <library/cpp/yt/misc/tls.h>

#include <array>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TRefCountedBase;

namespace NDetail {

constexpr int MaxHazardPointersPerThread = 2;
using THazardPointerSet = std::array<std::atomic<void*>, MaxHazardPointersPerThread>;

YT_DECLARE_THREAD_LOCAL(THazardPointerSet, HazardPointers);

struct THazardThreadState;
YT_DECLARE_THREAD_LOCAL(THazardThreadState*, HazardThreadState);

void InitHazardThreadState();

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class T, class TReclaimer>
void RetireHazardPointer(T* ptr, TReclaimer /*reclaimer*/)
{
    RetireHazardPointer(
        ptr,
        ptr,
        [] (void* reclaimPtr) { TReclaimer()(static_cast<T*>(reclaimPtr)); });
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
THazardPtr<T>::THazardPtr(THazardPtr&& other) noexcept
    : Ptr_(other.Ptr_)
    , HazardPtr_(other.HazardPtr_)
#ifndef NDEBUG
    , ProtectedPtr_(other.ProtectedPtr_)
#endif
{
    other.Ptr_ = nullptr;
    other.HazardPtr_ = nullptr;
#ifndef NDEBUG
    other.ProtectedPtr_ = nullptr;
#endif
}

template <class T>
THazardPtr<T>& THazardPtr<T>::operator=(THazardPtr&& other) noexcept
{
    if (this != &other) {
        Reset();
        Ptr_ = other.Ptr_;
        HazardPtr_ = other.HazardPtr_;
#ifndef NDEBUG
        ProtectedPtr_ = other.ProtectedPtr_;
#endif
        other.Ptr_ = nullptr;
        other.HazardPtr_ = nullptr;
#ifndef NDEBUG
        other.ProtectedPtr_ = nullptr;
#endif
    }
    return *this;
}

namespace NDetail {

inline std::atomic<void*>* FindEmptyHazardSlot()
{
    auto& hazardPointers = NYT::NDetail::HazardPointers();
    for (auto it = hazardPointers.begin(); it != hazardPointers.end(); ++it) {
        auto& ptr = *it;
        if (!ptr.load(std::memory_order::relaxed)) {
            return &ptr;
        }
    }
    // Too many hazard pointers are being used in a single thread concurrently.
    // Try increasing MaxHazardPointersPerThread.
    YT_ABORT();
}

} // namespace NDetail

template <class T>
template <class TPtrLoader>
THazardPtr<T> THazardPtr<T>::Acquire(TPtrLoader&& ptrLoader, T* ptr)
    requires (!std::derived_from<T, TRefCountedBase> ||
        requires(TRefCountedBase* b) { static_cast<T*>(b); })
{
    return Acquire(
        [&] { return TTaggedPtr<T>{ptrLoader(), 0}; },
        TTaggedPtr<T>{ptr, 0});
}

template <class T>
template <class TPtrLoader>
THazardPtr<T> THazardPtr<T>::Acquire(TPtrLoader&& ptrLoader, TTaggedPtr<T> taggedPtr)
{
    if (!taggedPtr.Ptr) {
        return {};
    }

    auto* hazardPtr = NYT::NDetail::FindEmptyHazardSlot();

    if (!NYT::NDetail::HazardThreadState()) [[unlikely]] {
        NYT::NDetail::InitHazardThreadState();
    }

    // The canonical hazard-slot value is the typed pointer adjusted by the
    // cached vbase offset; never read from |*taggedPtr.Ptr|.
    auto computeBasePtr = [] (TTaggedPtr<T> p) -> void* {
        return reinterpret_cast<char*>(p.Ptr) + p.Tag;
    };

    void* protectedPtr;
    void* checkPtr;
    do {
        protectedPtr = computeBasePtr(taggedPtr);
        hazardPtr->store(protectedPtr, std::memory_order::release);
        std::atomic_thread_fence(std::memory_order::seq_cst);
        checkPtr = protectedPtr;
        taggedPtr = ptrLoader();
    } while (computeBasePtr(taggedPtr) != checkPtr);

    return THazardPtr(taggedPtr.Ptr, hazardPtr, protectedPtr);
}

template <class T>
template <class TPtrLoader>
THazardPtr<T> THazardPtr<T>::Acquire(TPtrLoader&& ptrLoader)
{
    return Acquire(std::forward<TPtrLoader>(ptrLoader), ptrLoader());
}

template <class T>
void THazardPtr<T>::Reset() noexcept
{
    if (Ptr_) {
#ifdef NDEBUG
        HazardPtr_->store(nullptr, std::memory_order::release);
#else
        YT_VERIFY(HazardPtr_->exchange(nullptr) == ProtectedPtr_);
        ProtectedPtr_ = nullptr;
#endif
        Ptr_ = nullptr;
        HazardPtr_ = nullptr;
    }
}

template <class T>
THazardPtr<T>::~THazardPtr()
{
    Reset();
}

template <class T>
T* THazardPtr<T>::Get() const
{
    return Ptr_;
}

template <class T>
T& THazardPtr<T>::operator*() const
{
    YT_ASSERT(Ptr_);
    return *Ptr_;
}

template <class T>
T* THazardPtr<T>::operator->() const
{
    YT_ASSERT(Ptr_);
    return Ptr_;
}

template <class T>
THazardPtr<T>::operator bool() const
{
    return Ptr_ != nullptr;
}

template <class T>
THazardPtr<T>::THazardPtr(T* ptr, std::atomic<void*>* hazardPtr, [[maybe_unused]] void* protectedPtr)
    : Ptr_(ptr)
    , HazardPtr_(hazardPtr)
#ifndef NDEBUG
    , ProtectedPtr_(protectedPtr)
#endif
{ }

////////////////////////////////////////////////////////////////////////////////

template <class T>
bool operator==(const THazardPtr<T>& lhs, const T* rhs)
{
    return lhs.Get() == rhs;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
