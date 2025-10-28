#pragma once

#include <util/memory/alloc.h>

template <typename Type>
struct TStdIAllocator {
    typedef Type value_type;
    typedef Type* pointer;
    typedef const Type* const_pointer;
    typedef Type& reference;
    typedef const Type& const_reference;
    typedef size_t size_type;
    typedef ptrdiff_t difference_type;

    template <typename U>
    TStdIAllocator(const TStdIAllocator<U>& other) noexcept
        : Allocator(other.Allocator)
    {
    }

    template <typename U>
    struct rebind { // NOLINT(readability-identifier-naming)
        typedef TStdIAllocator<U> other;
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

    TStdIAllocator(IAllocator* allocator)
        : Allocator(allocator)
    {
    }

    IAllocator* Allocator;
};
