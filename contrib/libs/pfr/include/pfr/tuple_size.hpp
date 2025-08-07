// Copyright (c) 2016-2025 Antony Polukhin
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)


#ifndef PFR_TUPLE_SIZE_HPP
#define PFR_TUPLE_SIZE_HPP
#pragma once

#include <pfr/detail/config.hpp>

#if !defined(PFR_USE_MODULES) || defined(PFR_INTERFACE_UNIT)

#include <pfr/detail/sequence_tuple.hpp>
#include <pfr/detail/fields_count.hpp>

#if !defined(PFR_INTERFACE_UNIT)
#include <type_traits>
#include <utility>      // metaprogramming stuff
#endif

/// \file pfr/tuple_size.hpp
/// Contains tuple-like interfaces to get fields count \forcedlink{tuple_size}, \forcedlink{tuple_size_v}.
///
/// \b Synopsis:
namespace pfr {

PFR_BEGIN_MODULE_EXPORT

/// Has a static const member variable `value` that contains fields count in a T.
/// Works for any T that satisfies \aggregate.
///
/// \b Example:
/// \code
///     std::array<int, pfr::tuple_size<my_structure>::value > a;
/// \endcode
template <class T>
using tuple_size = detail::size_t_< pfr::detail::fields_count<T>() >;


/// `tuple_size_v` is a template variable that contains fields count in a T and
/// works for any T that satisfies \aggregate.
///
/// \b Example:
/// \code
///     std::array<int, pfr::tuple_size_v<my_structure> > a;
/// \endcode
template <class T>
constexpr std::size_t tuple_size_v = tuple_size<T>::value;

PFR_END_MODULE_EXPORT

} // namespace pfr

#endif  // #if defined(PFR_USE_MODULES) && !defined(PFR_INTERFACE_UNIT)

#endif // PFR_TUPLE_SIZE_HPP
