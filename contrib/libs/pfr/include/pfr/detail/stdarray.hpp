// Copyright (c) 2023 Denis Mikhailov
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef PFR_DETAIL_STDARRAY_HPP
#define PFR_DETAIL_STDARRAY_HPP
#pragma once

#include <pfr/detail/config.hpp>

#include <utility> // metaprogramming stuff
#include <array>
#include <type_traits> // for std::common_type_t
#include <cstddef>

#include <pfr/detail/sequence_tuple.hpp>

namespace pfr { namespace detail {

template <class... Types>
constexpr auto make_stdarray(const Types&... t) noexcept {
    return std::array<std::common_type_t<Types...>, sizeof...(Types)>{t...};
}

template <class T, std::size_t... I>
constexpr auto make_stdarray_from_tietuple(const T& t, std::index_sequence<I...>, int) noexcept {
    return detail::make_stdarray(
        pfr::detail::sequence_tuple::get<I>(t)...
    );
}

template <class T>
constexpr auto make_stdarray_from_tietuple(const T&, std::index_sequence<>, long) noexcept {
    return std::array<std::nullptr_t, 0>{};
}

}} // namespace pfr::detail

#endif // PFR_DETAIL_STDARRAY_HPP

