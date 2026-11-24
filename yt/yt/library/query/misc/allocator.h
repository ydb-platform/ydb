#pragma once

#include <library/cpp/yt/logging/logger.h>

#include <library/cpp/yt/memory/chunked_memory_pool.h>

#include <type_traits>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TAllocatorOverChunkProvider
{
public:
    // std::allocator_traits
    using value_type = T;
    using pointer = T*;
    using const_pointer = const T*;
    using reference = T&;
    using const_reference = const T&;
    using size_type = size_t;
    using difference_type = ptrdiff_t;
    using propagate_on_container_copy_assignment = std::true_type;
    using propagate_on_container_move_assignment = std::true_type;
    using propagate_on_container_swap = std::false_type;
    using is_always_equal = std::true_type;

    TAllocatorOverChunkProvider(
        IMemoryChunkProviderPtr memoryChunkProvider,
        TRefCountedTypeCookie tagCookie);

    TAllocatorOverChunkProvider(const TAllocatorOverChunkProvider& other) = default;

    TAllocatorOverChunkProvider<T>& operator=(const TAllocatorOverChunkProvider& other) = default;

    size_type max_size() const;

    T* allocate(size_t n);

    void deallocate(T* p, size_t n) noexcept;

    template <class U>
    bool operator==(const TAllocatorOverChunkProvider<U>& /*other*/) const noexcept;

    template <class U>
    bool operator!=(const TAllocatorOverChunkProvider<U>& /*other*/) const noexcept;

    template <class U>
    TAllocatorOverChunkProvider(const TAllocatorOverChunkProvider<U>& other);

private:
    template <class>
    friend class TAllocatorOverChunkProvider;

    IMemoryChunkProviderPtr Provider_;
    TRefCountedTypeCookie Cookie_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

#define ALLOCATOR_INL_H_
#include "allocator-inl.h"
#undef ALLOCATOR_INL_H_
