/*******************************************************************************
 * tlx/allocator_base.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_ALLOCATOR_BASE_HEADER
#define TLX_ALLOCATOR_BASE_HEADER

#include <cstddef>
#include <memory>
#include <type_traits>

namespace tlx {

template <typename Type>
class AllocatorBase
{
    static constexpr bool debug = true;

public:
    using value_type = Type;
    using pointer = Type*;
    using const_pointer = const Type*;
    using reference = Type&;
    using const_reference = const Type&;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;

    //! C++11 type flag
    using is_always_equal = std::true_type;
    //! C++11 type flag
    using propagate_on_container_move_assignment = std::true_type;

    //! Returns the address of x.
    pointer address(reference x) const noexcept {
        return std::addressof(x);
    }

    //! Returns the address of x.
    const_pointer address(const_reference x) const noexcept {
        return std::addressof(x);
    }

    //! Maximum size possible to allocate
    size_type max_size() const noexcept {
        return size_t(-1) / sizeof(Type);
    }

#if __cplusplus >= 201103L
    //! Constructs an element object on the location pointed by p.
    template <typename SubType, typename... Args>
    void construct(SubType* p, Args&& ... args)
    noexcept(std::is_nothrow_constructible<SubType, Args...>::value) {
        ::new (static_cast<void*>(p))SubType(std::forward<Args>(args) ...); // NOLINT
    }

    //! Destroys in-place the object pointed by p.
    template <typename SubType>
    void destroy(SubType* p) const noexcept(std::is_nothrow_destructible<SubType>::value) {
        p->~SubType();
    }
#else
    //! Constructs an element object on the location pointed by p.
    void construct(pointer p, const_reference value) {
        ::new (static_cast<void*>(p))Type(value); // NOLINT
    }

#if defined(_MSC_VER)
// disable false-positive warning C4100: 'p': unreferenced formal parameter
#pragma warning(push)
#pragma warning(disable:4100)
#endif
    //! Destroys in-place the object pointed by p.
    void destroy(pointer p) const noexcept {
        p->~Type();
    }
#if defined(_MSC_VER)
#pragma warning(push)
#endif
#endif
};

} // namespace tlx

#endif // !TLX_ALLOCATOR_BASE_HEADER

/******************************************************************************/
