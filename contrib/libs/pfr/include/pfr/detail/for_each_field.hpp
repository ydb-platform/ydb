// Copyright (c) 2016-2025 Antony Polukhin
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef PFR_DETAIL_FOR_EACH_FIELD_HPP
#define PFR_DETAIL_FOR_EACH_FIELD_HPP
#pragma once

#include <pfr/detail/config.hpp>

#include <pfr/detail/core.hpp>
#include <pfr/detail/fields_count.hpp>
#include <pfr/detail/for_each_field_impl.hpp>
#include <pfr/detail/make_integer_sequence.hpp>

#if !defined(PFR_INTERFACE_UNIT)
#include <type_traits>      // metaprogramming stuff
#endif

namespace pfr { namespace detail {

template <class T, class F>
constexpr void for_each_field(T&& value, F&& func) {
    constexpr std::size_t fields_count_val = pfr::detail::fields_count<std::remove_reference_t<T>>();

    ::pfr::detail::for_each_field_dispatcher(
        value,
        [f = std::forward<F>(func)](auto&& t) mutable {
            // MSVC related workaround. Its lambdas do not capture constexprs.
            constexpr std::size_t fields_count_val_in_lambda
                = pfr::detail::fields_count<std::remove_reference_t<T>>();

            ::pfr::detail::for_each_field_impl(
                t,
                std::forward<F>(f),
                detail::make_index_sequence<fields_count_val_in_lambda>{},
                std::is_rvalue_reference<T&&>{}
            );
        },
        detail::make_index_sequence<fields_count_val>{}
    );
}

}} // namespace pfr::detail


#endif // PFR_DETAIL_FOR_EACH_FIELD_HPP
