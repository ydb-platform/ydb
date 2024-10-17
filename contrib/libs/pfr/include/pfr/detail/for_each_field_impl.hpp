// Copyright (c) 2016-2023 Antony Polukhin
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef PFR_DETAIL_FOR_EACH_FIELD_IMPL_HPP
#define PFR_DETAIL_FOR_EACH_FIELD_IMPL_HPP
#pragma once

#include <pfr/detail/config.hpp>

#include <utility>      // metaprogramming stuff

#include <pfr/detail/sequence_tuple.hpp>
#include <pfr/detail/rvalue_t.hpp>

namespace pfr { namespace detail {

template <std::size_t Index>
using size_t_ = std::integral_constant<std::size_t, Index >;

template <class T, class F, class I, class = decltype(std::declval<F>()(std::declval<T>(), I{}))>
constexpr void for_each_field_impl_apply(T&& v, F&& f, I i, long) {
    std::forward<F>(f)(std::forward<T>(v), i);
}

template <class T, class F, class I>
constexpr void for_each_field_impl_apply(T&& v, F&& f, I /*i*/, int) {
    std::forward<F>(f)(std::forward<T>(v));
}

#if !defined(__cpp_fold_expressions) || __cpp_fold_expressions < 201603
template <class T, class F, std::size_t... I>
constexpr void for_each_field_impl(T& t, F&& f, std::index_sequence<I...>, std::false_type /*move_values*/) {
     const int v[] = {0, (
         detail::for_each_field_impl_apply(sequence_tuple::get<I>(t), std::forward<F>(f), size_t_<I>{}, 1L),
         0
     )...};
     (void)v;
}


template <class T, class F, std::size_t... I>
constexpr void for_each_field_impl(T& t, F&& f, std::index_sequence<I...>, std::true_type /*move_values*/) {
     const int v[] = {0, (
         detail::for_each_field_impl_apply(sequence_tuple::get<I>(std::move(t)), std::forward<F>(f), size_t_<I>{}, 1L),
         0
     )...};
     (void)v;
}
#else
template <class T, class F, std::size_t... I>
constexpr void for_each_field_impl(T& t, F&& f, std::index_sequence<I...>, std::false_type /*move_values*/) {
     (detail::for_each_field_impl_apply(sequence_tuple::get<I>(t), std::forward<F>(f), size_t_<I>{}, 1L), ...);
}

template <class T, class F, std::size_t... I>
constexpr void for_each_field_impl(T& t, F&& f, std::index_sequence<I...>, std::true_type /*move_values*/) {
     (detail::for_each_field_impl_apply(sequence_tuple::get<I>(std::move(t)), std::forward<F>(f), size_t_<I>{}, 1L), ...);
}
#endif

}} // namespace pfr::detail


#endif // PFR_DETAIL_FOR_EACH_FIELD_IMPL_HPP
