// Copyright (c) 2018 Adam Butcher, Antony Polukhin
// Copyright (c) 2019-2023 Antony Polukhin
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef PFR_DETAIL_TIE_FROM_STRUCTURE_TUPLE_HPP
#define PFR_DETAIL_TIE_FROM_STRUCTURE_TUPLE_HPP
#pragma once

#include <pfr/detail/config.hpp>

#include <pfr/detail/core.hpp>

#include <pfr/detail/stdtuple.hpp>
#include <pfr/tuple_size.hpp>
#include <pfr/detail/make_integer_sequence.hpp>

#include <tuple>

namespace pfr { namespace detail {

/// \brief A `std::tuple` capable of de-structuring assignment used to support
/// a tie of multiple lvalue references to fields of an aggregate T.
///
/// \sa pfr::tie_from_structure
template <typename... Elements>
struct tie_from_structure_tuple : std::tuple<Elements&...> {
    using base = std::tuple<Elements&...>;
    using base::base;

    template <typename T>
    constexpr tie_from_structure_tuple& operator= (T const& t) {
        base::operator=(
            detail::make_stdtiedtuple_from_tietuple(
                detail::tie_as_tuple(t),
                detail::make_index_sequence<tuple_size_v<T>>()));
        return *this;
    }
};

}} // namespace pfr::detail

#endif // PFR_DETAIL_TIE_FROM_STRUCTURE_TUPLE_HPP
