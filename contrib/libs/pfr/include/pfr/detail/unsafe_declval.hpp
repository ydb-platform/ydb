// Copyright (c) 2019-2025 Antony Polukhin.
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef PFR_DETAIL_UNSAFE_DECLVAL_HPP
#define PFR_DETAIL_UNSAFE_DECLVAL_HPP
#pragma once

#include <pfr/detail/config.hpp>

#if !defined(PFR_INTERFACE_UNIT)
#include <type_traits>
#endif

namespace pfr { namespace detail {

// This function serves as a link-time assert. If linker requires it, then
// `unsafe_declval()` is used at runtime.
void report_if_you_see_link_error_with_this_function() noexcept;

// For returning non default constructible types. Do NOT use at runtime!
//
// GCCs std::declval may not be used in potentionally evaluated contexts,
// so we reinvent it.
template <class T>
constexpr T unsafe_declval() noexcept {
    report_if_you_see_link_error_with_this_function();

    typename std::remove_reference<T>::type* ptr = nullptr;
    ptr += 42; // suppresses 'null pointer dereference' warnings
    return static_cast<T>(*ptr);
}

}} // namespace pfr::detail


#endif // PFR_DETAIL_UNSAFE_DECLVAL_HPP

