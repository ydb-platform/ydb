#pragma once

#include <util/system/yassert.h>
#include <util/generic/ptr.h>

#include <memory>

namespace NKikimr {

template <typename T>
class TTrackingAllocator : public std::allocator<T> {
public:
    typedef size_t size_type;
    typedef T* pointer;
    typedef const T* const_pointer;

    template<typename U>
    struct rebind {
        typedef TTrackingAllocator<U> other;
    };

    struct propagate_on_container_copy_assignment : public std::true_type {};
    struct propagate_on_container_move_assignment : public std::true_type {};
    struct propagate_on_container_swap : public std::true_type {};

    template<typename U> friend class TTrackingAllocator;

    TTrackingAllocator() noexcept
        : std::allocator<T>()
        , Allocated(new size_t(0))
    {
    }

    TTrackingAllocator(const TSimpleSharedPtr<size_t>& allocated) noexcept
        : std::allocator<T>()
        , Allocated(allocated)
    {
    }

    TTrackingAllocator(const TTrackingAllocator& a) noexcept
        : std::allocator<T>(a)
        , Allocated(a.Allocated)
    {
    }

    TTrackingAllocator(TTrackingAllocator&& a) noexcept
        : std::allocator<T>(std::move(static_cast<std::allocator<T>&&>(a)))
        , Allocated(a.Allocated)
    {
        a.Allocated = nullptr;
    }

    template <class U>
    TTrackingAllocator(const TTrackingAllocator<U>& a) noexcept
        : std::allocator<T>(a)
        , Allocated(a.Allocated)
    {
    }

    TTrackingAllocator& operator=(const TTrackingAllocator& a) noexcept
    {
        Allocated = a.Allocated;
        std::allocator<T>::operator=(a);
        return *this;
    }

    TTrackingAllocator& operator=(TTrackingAllocator&& a) noexcept
    {
        Allocated = a.Allocated;
        a.Allocated = nullptr;
        std::allocator<T>::operator=(std::move(static_cast<std::allocator<T>&&>(a)));
        return *this;
    }

    ~TTrackingAllocator() {
    }

    pointer allocate(size_type n, const void* hint = nullptr) { 
        *Allocated += n * sizeof(T);
        return std::allocator<T>::allocate(n, hint);
    }

    void deallocate(pointer p, size_type n) {
        Y_ASSERT(*Allocated >= n * sizeof(T));
        *Allocated -= n * sizeof(T);
        std::allocator<T>::deallocate(p, n);
    }

    size_t GetAllocated() const {
        return *Allocated;
    }

    TSimpleSharedPtr<size_t> GetAllocatedPtr() const {
        return Allocated;
    }

private:
    TSimpleSharedPtr<size_t> Allocated;
};

} // NKikimr
