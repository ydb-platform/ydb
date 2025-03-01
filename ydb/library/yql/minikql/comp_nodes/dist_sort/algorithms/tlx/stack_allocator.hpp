/*******************************************************************************
 * tlx/stack_allocator.hpp
 *
 * An allocator derived from short_alloc by Howard Hinnant, which first takes
 * memory from a stack allocated reserved area and then from malloc().
 *
 * from http://howardhinnant.github.io/stack_alloc.html by Howard Hinnant and
 * http://codereview.stackexchange.com/questions/31528/a-working-stack-allocator
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2015-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_STACK_ALLOCATOR_HEADER
#define TLX_STACK_ALLOCATOR_HEADER

#include <cassert>
#include <cstddef>
#include <cstdlib>

#include <tlx/allocator_base.hpp>

namespace tlx {

/*!
 * Storage area allocated on the stack and usable by a StackAllocator.
 */
template <size_t Size>
class StackArena
{
    static constexpr size_t alignment = 16;

    //! union to enforce alignment of buffer area
    union AlignmentHelper {
        int i;
        long l;
        long long ll;
        long double ld;
        double d;
        void* p;
        void (* pf)();
        AlignmentHelper* ps;
    };

    union {
        //! stack memory area used for allocations.
        char buf_[Size];
        //! enforce alignment
        AlignmentHelper dummy_for_alignment_;
    };

    //! pointer into free bytes in buf_
    char* ptr_;

    //! debug method to check whether ptr_ is still in buf_.
    bool pointer_in_buffer(char* p) noexcept
    { return buf_ <= p && p <= buf_ + Size; }

public:
    //! default constructor: free pointer at the beginning.
    StackArena() noexcept : ptr_(buf_) { }

    //! destructor clears ptr_ for debugging.
    ~StackArena() { ptr_ = nullptr; }

    StackArena(const StackArena&) = delete;
    StackArena& operator = (const StackArena&) = delete;

    char * allocate(size_t n) {
        assert(pointer_in_buffer(ptr_) &&
               "StackAllocator has outlived StackArena");

        // try to allocate from stack memory area
        if (buf_ + Size >= ptr_ + n) {
            char* r = ptr_;
            ptr_ += n;
            if (n % alignment != 0)
                ptr_ += alignment - n % alignment;
            return r;
        }
        // otherwise fallback to malloc()
        return static_cast<char*>(malloc(n));
    }

    void deallocate(char* p, size_t n) noexcept {
        assert(pointer_in_buffer(ptr_) &&
               "StackAllocator has outlived StackArena");

        if (pointer_in_buffer(p)) {
            // free memory area (only works for a stack-ordered
            // allocations/deallocations).
            if (p + n == ptr_)
                ptr_ = p;
        }
        else {
            free(p);
        }
    }

    //! size of memory area
    static constexpr size_t size() noexcept { return Size; }

    //! return number of bytes used in StackArena
    size_t used() const noexcept { return static_cast<size_t>(ptr_ - buf_); }

    //! reset memory area
    void reset() noexcept { ptr_ = buf_; }
};

template <typename Type, size_t Size>
class StackAllocator : public AllocatorBase<Type>
{
public:
    using value_type = Type;
    using pointer = Type*;
    using const_pointer = const Type*;
    using reference = Type&;
    using const_reference = const Type&;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;

    //! C++11 type flag
    using is_always_equal = std::false_type;

    //! required rebind.
    template <typename Other>
    struct rebind { using other = StackAllocator<Other, Size>; };

    //! default constructor to invalid arena
    StackAllocator() noexcept : arena_(nullptr) { }

    //! constructor with explicit arena reference
    explicit StackAllocator(StackArena<Size>& arena) noexcept
        : arena_(&arena) { }

    //! constructor from another allocator with same arena size
    template <typename Other>
    StackAllocator(const StackAllocator<Other, Size>& other) noexcept
        : arena_(other.arena_) { }

    //! copy-constructor: default
    StackAllocator(const StackAllocator&) noexcept = default;

#if !defined(_MSC_VER)
    //! copy-assignment: default
    StackAllocator& operator = (const StackAllocator&) noexcept = default;

    //! move-constructor: default
    StackAllocator(StackAllocator&&) noexcept = default;

    //! move-assignment: default
    StackAllocator& operator = (StackAllocator&&) noexcept = default;
#endif

    //! allocate method: get memory from arena
    pointer allocate(size_t n) {
        return reinterpret_cast<Type*>(arena_->allocate(n * sizeof(Type)));
    }

    //! deallocate method: release from arena
    void deallocate(pointer p, size_t n) noexcept {
        arena_->deallocate(reinterpret_cast<char*>(p), n * sizeof(Type));
    }

    template <typename Other, size_t OtherSize>
    bool operator == (
        const StackAllocator<Other, OtherSize>& other) const noexcept {
        return Size == OtherSize && arena_ == other.arena_;
    }

    template <typename Other, size_t OtherSize>
    bool operator != (
        const StackAllocator<Other, OtherSize>& other) const noexcept {
        return !operator == (other);
    }

    template <typename Other, size_t OtherSize>
    friend class StackAllocator;

private:
    StackArena<Size>* arena_;
};

} // namespace tlx

#endif // !TLX_STACK_ALLOCATOR_HEADER

/******************************************************************************/
