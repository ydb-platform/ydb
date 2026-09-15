#pragma once

#include <util/memory/alloc.h>

template <typename Type>
struct TStdIAllocator {
    using value_type = Type;
    using pointer = Type*;
    using const_pointer = const Type*;
    using reference = Type&;
    using const_reference = const Type&;
    using size_type = size_t;
    using difference_type = ptrdiff_t;

    template <typename U> // NOLINTNEXTLINE(google-explicit-constructor)
    TStdIAllocator(const TStdIAllocator<U>& other) noexcept
        : Allocator(other.Allocator)
    {
    }

    template <typename U>
    struct rebind { // NOLINT(readability-identifier-naming)
        using other = TStdIAllocator<U>;
    };

    template <typename U>
    bool operator==(const TStdIAllocator<U>& other) const {
        return Allocator == other.Allocator;
    }

    template <typename U>
    bool operator!=(const TStdIAllocator<U>& other) const {
        return Allocator != other.Allocator;
    }

    pointer allocate(size_type n, const void* = nullptr) // NOLINT(readability-identifier-naming)
    {
        return (pointer)Allocator->Allocate(n * sizeof(value_type)).Data;
    }

    void deallocate(const_pointer p, size_type n) noexcept // NOLINT(readability-identifier-naming)
    {
        return Allocator->Release(IAllocator::TBlock{.Data = (void*)p, .Len = n * sizeof(value_type)});
    }

    // NOLINTNEXTLINE(google-explicit-constructor)
    TStdIAllocator(IAllocator* allocator)
        : Allocator(allocator)
    {
    }

    IAllocator* Allocator;
};
