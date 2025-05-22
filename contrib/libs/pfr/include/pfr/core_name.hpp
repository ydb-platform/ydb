// Copyright (c) 2023 Bela Schaum, X-Ryl669, Denis Mikhailov.
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)


// Initial implementation by Bela Schaum, https://github.com/schaumb
// The way to make it union and UB free by X-Ryl669, https://github.com/X-Ryl669
//

#ifndef PFR_CORE_NAME_HPP
#define PFR_CORE_NAME_HPP
#pragma once

#include <pfr/detail/config.hpp>

#include <pfr/detail/core_name.hpp>

#include <pfr/detail/sequence_tuple.hpp>
#include <pfr/detail/stdarray.hpp>
#include <pfr/detail/make_integer_sequence.hpp>

#include <cstddef> // for std::size_t

#include <pfr/tuple_size.hpp>

/// \file pfr/core_name.hpp
/// Contains functions \forcedlink{get_name} and \forcedlink{names_as_array} to know which names each field of any \aggregate has.
///
/// \fnrefl for details.
///
/// \b Synopsis:

namespace pfr {

/// \brief Returns name of a field with index `I` in \aggregate `T`.
///
/// \b Example:
/// \code
///     struct my_struct { int i, short s; };
///
///     assert(pfr::get_name<0, my_struct>() == "i");
///     assert(pfr::get_name<1, my_struct>() == "s");
/// \endcode
template <std::size_t I, class T>
constexpr
#ifdef PFR_DOXYGEN_INVOKED
std::string_view
#else
auto
#endif
get_name() noexcept {
    return detail::get_name<T, I>();
}

// FIXME: implement this
// template<class U, class T>
// constexpr auto get_name() noexcept {
//     return detail::sequence_tuple::get_by_type_impl<U>( detail::tie_as_names_tuple<T>() );
// }

/// \brief Creates a `std::array` from names of fields of an \aggregate `T`.
///
/// \b Example:
/// \code
///     struct my_struct { int i, short s; };
///     std::array<std::string_view, 2> a = pfr::names_as_array<my_struct>();
///     assert(a[0] == "i");
/// \endcode
template <class T>
constexpr
#ifdef PFR_DOXYGEN_INVOKED
std::array<std::string_view, pfr::tuple_size_v<T>>
#else
auto
#endif
names_as_array() noexcept {
    return detail::make_stdarray_from_tietuple(
        detail::tie_as_names_tuple<T>(),
        detail::make_index_sequence< tuple_size_v<T> >(),
        1L
    );
}

} // namespace pfr

#endif // PFR_CORE_NAME_HPP

