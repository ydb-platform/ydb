// Copyright (c) 2016-2023 Antony Polukhin
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef PFR_DETAIL_CAST_TO_LAYOUT_COMPATIBLE_HPP
#define PFR_DETAIL_CAST_TO_LAYOUT_COMPATIBLE_HPP
#pragma once

#include <pfr/detail/config.hpp>

#include <type_traits>
#include <utility>      // metaprogramming stuff
#include <pfr/detail/rvalue_t.hpp>

namespace pfr { namespace detail {

template <class T, class U>
constexpr void static_assert_layout_compatible() noexcept {
    static_assert(
        std::alignment_of<T>::value == std::alignment_of<U>::value,
        "====================> Boost.PFR: Alignment check failed, probably your structure has user-defined alignment for the whole structure or for some of the fields."
    );
    static_assert(sizeof(T) == sizeof(U), "====================> Boost.PFR: Size check failed, probably your structure has bitfields or user-defined alignment.");
}

/// @cond
#ifdef __GNUC__
#define MAY_ALIAS __attribute__((__may_alias__))
#else
#define MAY_ALIAS
#endif
/// @endcond

template <class To, class From>
MAY_ALIAS const To& cast_to_layout_compatible(const From& val) noexcept {
    MAY_ALIAS const To* const t = reinterpret_cast<const To*>( std::addressof(val) );
    detail::static_assert_layout_compatible<To, From>();
    return *t;
}

template <class To, class From>
MAY_ALIAS const volatile To& cast_to_layout_compatible(const volatile From& val) noexcept {
    MAY_ALIAS const volatile To* const t = reinterpret_cast<const volatile To*>( std::addressof(val) );
    detail::static_assert_layout_compatible<To, From>();
    return *t;
}


template <class To, class From>
MAY_ALIAS volatile To& cast_to_layout_compatible(volatile From& val) noexcept {
    MAY_ALIAS volatile To* const t = reinterpret_cast<volatile To*>( std::addressof(val) );
    detail::static_assert_layout_compatible<To, From>();
    return *t;
}


template <class To, class From>
MAY_ALIAS To& cast_to_layout_compatible(From& val) noexcept {
    MAY_ALIAS To* const t = reinterpret_cast<To*>( std::addressof(val) );
    detail::static_assert_layout_compatible<To, From>();
    return *t;
}

#ifdef PFR_DETAIL_STRICT_RVALUE_TESTING
template <class To, class From>
To&& cast_to_layout_compatible(rvalue_t<From> val) noexcept = delete;
#endif

#undef MAY_ALIAS


}} // namespace pfr::detail

#endif // PFR_DETAIL_CAST_TO_LAYOUT_COMPATIBLE_HPP
