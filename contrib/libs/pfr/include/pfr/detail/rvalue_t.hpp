// Copyright (c) 2016-2025 Antony Polukhin
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef PFR_DETAIL_RVALUE_T_HPP
#define PFR_DETAIL_RVALUE_T_HPP
#pragma once

#if !defined(PFR_INTERFACE_UNIT)
#include <type_traits>
#include <utility>      // std::enable_if_t
#endif

// This header provides aliases rvalue_t and lvalue_t.
//
// Usage: template <class T> void foo(rvalue<T> rvalue);
//
// Those are useful for
//  * better type safety - you can validate at compile time that only rvalue reference is passed into the function
//  * documentation and readability - rvalue_t<T> is much better than T&&+SFINAE

namespace pfr { namespace detail {

/// Binds to rvalues only, no copying allowed.
template <class T
#ifdef PFR_DETAIL_STRICT_RVALUE_TESTING
    , class = std::enable_if_t<std::is_rvalue_reference<T&&>::value>
#endif
>
using rvalue_t = T&&;

/// Binds to mutable lvalues only

}} // namespace pfr::detail

#endif // PFR_DETAIL_RVALUE_T_HPP
