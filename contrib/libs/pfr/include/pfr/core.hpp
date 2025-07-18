// Copyright (c) 2016-2025 Antony Polukhin
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef PFR_CORE_HPP
#define PFR_CORE_HPP
#pragma once

#include <pfr/detail/config.hpp>

#if !defined(PFR_USE_MODULES) || defined(PFR_INTERFACE_UNIT)

#include <pfr/detail/core.hpp>

#include <pfr/detail/sequence_tuple.hpp>
#include <pfr/detail/stdtuple.hpp>
#include <pfr/detail/for_each_field.hpp>
#include <pfr/detail/make_integer_sequence.hpp>
#include <pfr/detail/tie_from_structure_tuple.hpp>

#include <pfr/tuple_size.hpp>

#if !defined(PFR_INTERFACE_UNIT)
#include <type_traits>
#include <utility>      // metaprogramming stuff
#endif

/// \file pfr/core.hpp
/// Contains all the basic tuple-like interfaces \forcedlink{get}, \forcedlink{tuple_size}, \forcedlink{tuple_element_t}, and others.
///
/// \b Synopsis:

namespace pfr {

PFR_BEGIN_MODULE_EXPORT

/// \brief Returns reference or const reference to a field with index `I` in \aggregate `val`.
/// Overload taking the type `U` returns reference or const reference to a field
/// with provided type `U` in \aggregate `val` if there's only one field of such type in `val`.
///
/// \b Example:
/// \code
///     struct my_struct { int i, short s; };
///     my_struct s {10, 11};
///
///     assert(pfr::get<0>(s) == 10);
///     pfr::get<1>(s) = 0;
///
///     assert(pfr::get<int>(s) == 10);
///     pfr::get<short>(s) = 11;
/// \endcode
template <std::size_t I, class T>
constexpr decltype(auto) get(const T& val) noexcept {
    return detail::sequence_tuple::get<I>( detail::tie_as_tuple(val) );
}

/// \overload get
template <std::size_t I, class T>
constexpr decltype(auto) get(T& val
#if !PFR_USE_CPP17
    , std::enable_if_t<std::is_assignable<T, T>::value>* = nullptr
#endif
) noexcept {
    return detail::sequence_tuple::get<I>( detail::tie_as_tuple(val) );
}

#if !PFR_USE_CPP17
/// \overload get
template <std::size_t I, class T>
constexpr auto get(T&, std::enable_if_t<!std::is_assignable<T, T>::value>* = nullptr) noexcept {
    static_assert(sizeof(T) && false, "====================> Boost.PFR: Calling pfr::get on non const non assignable type is allowed only in C++17");
    return 0;
}
#endif


/// \overload get
template <std::size_t I, class T>
constexpr auto get(T&& val, std::enable_if_t< std::is_rvalue_reference<T&&>::value>* = nullptr) noexcept {
    return std::move(detail::sequence_tuple::get<I>( detail::tie_as_tuple(val) ));
}


/// \overload get
template <class U, class T>
constexpr const U& get(const T& val) noexcept {
    return detail::sequence_tuple::get_by_type_impl<const U&>( detail::tie_as_tuple(val) );
}


/// \overload get
template <class U, class T>
constexpr U& get(T& val
#if !PFR_USE_CPP17
    , std::enable_if_t<std::is_assignable<T, T>::value>* = nullptr
#endif
) noexcept {
    return detail::sequence_tuple::get_by_type_impl<U&>( detail::tie_as_tuple(val) );
}

#if !PFR_USE_CPP17
/// \overload get
template <class U, class T>
constexpr U& get(T&, std::enable_if_t<!std::is_assignable<T, T>::value>* = nullptr) noexcept {
    static_assert(sizeof(T) && false, "====================> Boost.PFR: Calling pfr::get on non const non assignable type is allowed only in C++17");
    return 0;
}
#endif


/// \overload get
template <class U, class T>
constexpr U&& get(T&& val, std::enable_if_t< std::is_rvalue_reference<T&&>::value>* = nullptr) noexcept {
    return std::move(detail::sequence_tuple::get_by_type_impl<U&>( detail::tie_as_tuple(val) ));
}


/// \brief `tuple_element` has a member typedef `type` that returns the type of a field with index I in \aggregate T.
///
/// \b Example:
/// \code
///     std::vector< pfr::tuple_element<0, my_structure>::type > v;
/// \endcode
template <std::size_t I, class T>
using tuple_element = detail::sequence_tuple::tuple_element<I, decltype( ::pfr::detail::tie_as_tuple(std::declval<T&>()) ) >;


/// \brief Type of a field with index `I` in \aggregate `T`.
///
/// \b Example:
/// \code
///     std::vector< pfr::tuple_element_t<0, my_structure> > v;
/// \endcode
template <std::size_t I, class T>
using tuple_element_t = typename tuple_element<I, T>::type;


/// \brief Creates a `std::tuple` from fields of an \aggregate `val`.
///
/// \b Example:
/// \code
///     struct my_struct { int i, short s; };
///     my_struct s {10, 11};
///     std::tuple<int, short> t = pfr::structure_to_tuple(s);
///     assert(get<0>(t) == 10);
/// \endcode
template <class T>
constexpr auto structure_to_tuple(const T& val) {
    return detail::make_stdtuple_from_tietuple(
        detail::tie_as_tuple(val),
        detail::make_index_sequence< tuple_size_v<T> >()
    );
}


/// \brief std::tie` like function that ties fields of a structure.
///
/// \returns a `std::tuple` with lvalue and const lvalue references to fields of an \aggregate `val`.
///
/// \b Example:
/// \code
///     void foo(const int&, const short&);
///     struct my_struct { int i, short s; };
///
///     const my_struct const_s{1, 2};
///     std::apply(foo, pfr::structure_tie(const_s));
///
///     my_struct s;
///     pfr::structure_tie(s) = std::tuple<int, short>{10, 11};
///     assert(s.s == 11);
/// \endcode
template <class T>
constexpr auto structure_tie(const T& val) noexcept {
    return detail::make_conststdtiedtuple_from_tietuple(
        detail::tie_as_tuple(const_cast<T&>(val)),
        detail::make_index_sequence< tuple_size_v<T> >()
    );
}


/// \overload structure_tie
template <class T>
constexpr auto structure_tie(T& val
#if !PFR_USE_CPP17
    , std::enable_if_t<std::is_assignable<T, T>::value>* = nullptr
#endif
) noexcept {
    return detail::make_stdtiedtuple_from_tietuple(
        detail::tie_as_tuple(val),
        detail::make_index_sequence< tuple_size_v<T> >()
    );
}

#if !PFR_USE_CPP17
/// \overload structure_tie
template <class T>
constexpr auto structure_tie(T&, std::enable_if_t<!std::is_assignable<T, T>::value>* = nullptr) noexcept {
    static_assert(sizeof(T) && false, "====================> Boost.PFR: Calling pfr::structure_tie on non const non assignable type is allowed only in C++17");
    return 0;
}
#endif


/// \overload structure_tie
template <class T>
constexpr auto structure_tie(T&&, std::enable_if_t< std::is_rvalue_reference<T&&>::value>* = nullptr) noexcept {
    static_assert(sizeof(T) && false, "====================> Boost.PFR: Calling pfr::structure_tie on rvalue references is forbidden");
    return 0;
}

/// Calls `func` for each field of a `value`.
///
/// \param func must have one of the following signatures:
///     * any_return_type func(U&& field)                // field of value is perfect forwarded to function
///     * any_return_type func(U&& field, std::size_t i)
///     * any_return_type func(U&& value, I i)           // Here I is an `std::integral_constant<size_t, field_index>`
///
/// \param value To each field of this variable will be the `func` applied.
///
/// \b Example:
/// \code
///     struct my_struct { int i, short s; };
///     int sum = 0;
///     pfr::for_each_field(my_struct{20, 22}, [&sum](const auto& field) { sum += field; });
///     assert(sum == 42);
/// \endcode
template <class T, class F>
constexpr void for_each_field(T&& value, F&& func) {
    return ::pfr::detail::for_each_field(std::forward<T>(value), std::forward<F>(func));
}

/// \brief std::tie-like function that allows assigning to tied values from aggregates.
///
/// \returns an object with lvalue references to `args...`; on assignment of an \aggregate value to that
/// object each field of an aggregate is assigned to the corresponding `args...` reference.
///
/// \b Example:
/// \code
///     auto f() {
///       struct { struct { int x, y } p; short s; } res { { 4, 5 }, 6 };
///       return res;
///     }
///     auto [p, s] = f();
///     pfr::tie_from_structure(p, s) = f();
/// \endcode
template <typename... Elements>
constexpr detail::tie_from_structure_tuple<Elements...> tie_from_structure(Elements&... args) noexcept {
    return detail::tie_from_structure_tuple<Elements...>(args...);
}

PFR_END_MODULE_EXPORT

} // namespace pfr

#endif  // #if !defined(PFR_USE_MODULES) || defined(PFR_INTERFACE_UNIT)

#endif // PFR_CORE_HPP
