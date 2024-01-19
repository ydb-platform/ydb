// Copyright (c) 2016-2023 Antony Polukhin
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef PFR_DETAIL_SEQUENCE_TUPLE_HPP
#define PFR_DETAIL_SEQUENCE_TUPLE_HPP
#pragma once

#include <pfr/detail/config.hpp>
#include <pfr/detail/make_integer_sequence.hpp>

#include <utility>      // metaprogramming stuff
#include <cstddef>      // std::size_t

///////////////////// Tuple that holds its values in the supplied order
namespace pfr { namespace detail { namespace sequence_tuple {

template <std::size_t N, class T>
struct base_from_member {
    T value;
};

template <class I, class ...Tail>
struct tuple_base;



template <std::size_t... I, class ...Tail>
struct tuple_base< std::index_sequence<I...>, Tail... >
    : base_from_member<I , Tail>...
{
    static constexpr std::size_t size_v = sizeof...(I);

    // We do not use `noexcept` in the following functions, because if user forget to put one then clang will issue an error:
    // "error: exception specification of explicitly defaulted default constructor does not match the calculated one".
    constexpr tuple_base() = default;
    constexpr tuple_base(tuple_base&&) = default;
    constexpr tuple_base(const tuple_base&) = default;

    constexpr tuple_base(Tail... v) noexcept
        : base_from_member<I, Tail>{ v }...
    {}
};

template <>
struct tuple_base<std::index_sequence<> > {
    static constexpr std::size_t size_v = 0;
};

template <std::size_t N, class T>
constexpr T& get_impl(base_from_member<N, T>& t) noexcept {
    // NOLINTNEXTLINE(clang-analyzer-core.uninitialized.UndefReturn)
    return t.value;
}

template <std::size_t N, class T>
constexpr const T& get_impl(const base_from_member<N, T>& t) noexcept {
    // NOLINTNEXTLINE(clang-analyzer-core.uninitialized.UndefReturn)
    return t.value;
}

template <std::size_t N, class T>
constexpr volatile T& get_impl(volatile base_from_member<N, T>& t) noexcept {
    // NOLINTNEXTLINE(clang-analyzer-core.uninitialized.UndefReturn)
    return t.value;
}

template <std::size_t N, class T>
constexpr const volatile T& get_impl(const volatile base_from_member<N, T>& t) noexcept {
    // NOLINTNEXTLINE(clang-analyzer-core.uninitialized.UndefReturn)
    return t.value;
}

template <std::size_t N, class T>
constexpr T&& get_impl(base_from_member<N, T>&& t) noexcept {
    // NOLINTNEXTLINE(clang-analyzer-core.uninitialized.UndefReturn)
    return std::forward<T>(t.value);
}


template <class T, std::size_t N>
constexpr T& get_by_type_impl(base_from_member<N, T>& t) noexcept {
    // NOLINTNEXTLINE(clang-analyzer-core.uninitialized.UndefReturn)
    return t.value;
}

template <class T, std::size_t N>
constexpr const T& get_by_type_impl(const base_from_member<N, T>& t) noexcept {
    // NOLINTNEXTLINE(clang-analyzer-core.uninitialized.UndefReturn)
    return t.value;
}

template <class T, std::size_t N>
constexpr volatile T& get_by_type_impl(volatile base_from_member<N, T>& t) noexcept {
    // NOLINTNEXTLINE(clang-analyzer-core.uninitialized.UndefReturn)
    return t.value;
}

template <class T, std::size_t N>
constexpr const volatile T& get_by_type_impl(const volatile base_from_member<N, T>& t) noexcept {
    // NOLINTNEXTLINE(clang-analyzer-core.uninitialized.UndefReturn)
    return t.value;
}

template <class T, std::size_t N>
constexpr T&& get_by_type_impl(base_from_member<N, T>&& t) noexcept {
    // NOLINTNEXTLINE(clang-analyzer-core.uninitialized.UndefReturn)
    return std::forward<T>(t.value);
}

template <class T, std::size_t N>
constexpr const T&& get_by_type_impl(const base_from_member<N, T>&& t) noexcept {
    // NOLINTNEXTLINE(clang-analyzer-core.uninitialized.UndefReturn)
    return std::forward<T>(t.value);
}




template <class ...Values>
struct tuple: tuple_base<
    detail::index_sequence_for<Values...>,
    Values...>
{
    using tuple_base<
        detail::index_sequence_for<Values...>,
        Values...
    >::tuple_base;

    constexpr static std::size_t size() noexcept { return sizeof...(Values); }
    constexpr static bool empty() noexcept { return size() == 0; }
};


template <std::size_t N, class ...T>
constexpr decltype(auto) get(tuple<T...>& t) noexcept {
    static_assert(N < tuple<T...>::size_v, "====================> Boost.PFR: Tuple index out of bounds");
    return sequence_tuple::get_impl<N>(t);
}

template <std::size_t N, class ...T>
constexpr decltype(auto) get(const tuple<T...>& t) noexcept {
    static_assert(N < tuple<T...>::size_v, "====================> Boost.PFR: Tuple index out of bounds");
    return sequence_tuple::get_impl<N>(t);
}

template <std::size_t N, class ...T>
constexpr decltype(auto) get(const volatile tuple<T...>& t) noexcept {
    static_assert(N < tuple<T...>::size_v, "====================> Boost.PFR: Tuple index out of bounds");
    return sequence_tuple::get_impl<N>(t);
}

template <std::size_t N, class ...T>
constexpr decltype(auto) get(volatile tuple<T...>& t) noexcept {
    static_assert(N < tuple<T...>::size_v, "====================> Boost.PFR: Tuple index out of bounds");
    return sequence_tuple::get_impl<N>(t);
}

template <std::size_t N, class ...T>
constexpr decltype(auto) get(tuple<T...>&& t) noexcept {
    static_assert(N < tuple<T...>::size_v, "====================> Boost.PFR: Tuple index out of bounds");
    return sequence_tuple::get_impl<N>(std::move(t));
}

template <std::size_t I, class T>
using tuple_element = std::remove_reference< decltype(
        ::pfr::detail::sequence_tuple::get<I>( std::declval<T>() )
    ) >;

template <class... Args>
constexpr auto make_sequence_tuple(Args... args) noexcept {
    return ::pfr::detail::sequence_tuple::tuple<Args...>{ args... };
}

}}} // namespace pfr::detail::sequence_tuple

#endif // PFR_CORE_HPP
