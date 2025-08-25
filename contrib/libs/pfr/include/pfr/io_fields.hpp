// Copyright (c) 2016-2025 Antony Polukhin
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)


#ifndef PFR_IO_FIELDS_HPP
#define PFR_IO_FIELDS_HPP
#pragma once

#include <pfr/detail/config.hpp>

#if !defined(PFR_USE_MODULES) || defined(PFR_INTERFACE_UNIT)

#include <pfr/detail/core.hpp>
#include <pfr/detail/sequence_tuple.hpp>
#include <pfr/detail/io.hpp>
#include <pfr/detail/make_integer_sequence.hpp>
#include <pfr/tuple_size.hpp>

#if !defined(PFR_INTERFACE_UNIT)
#include <type_traits>
#include <utility>      // metaprogramming stuff
#endif

/// \file pfr/io_fields.hpp
/// Contains IO manipulator \forcedlink{io_fields} to read/write any \aggregate field-by-field.
///
/// \b Example:
/// \code
///     struct my_struct {
///         int i;
///         short s;
///     };
///
///     std::ostream& operator<<(std::ostream& os, const my_struct& x) {
///         return os << pfr::io_fields(x);  // Equivalent to: os << "{ " << x.i << " ," <<  x.s << " }"
///     }
///
///     std::istream& operator>>(std::istream& is, my_struct& x) {
///         return is >> pfr::io_fields(x);  // Equivalent to: is >> "{ " >> x.i >> " ," >>  x.s >> " }"
///     }
/// \endcode
///
/// \podops for other ways to define operators and more details.
///
/// \b Synopsis:

namespace pfr {

namespace detail {

template <class T>
struct io_fields_impl {
    T value;
};

PFR_BEGIN_MODULE_EXPORT

template <class Char, class Traits, class T>
std::basic_ostream<Char, Traits>& operator<<(std::basic_ostream<Char, Traits>& out, io_fields_impl<const T&>&& x) {
    const T& value = x.value;
    constexpr std::size_t fields_count_val = pfr::detail::fields_count<T>();
    out << '{';
#if PFR_USE_CPP17 || PFR_USE_LOOPHOLE
    detail::print_impl<0, fields_count_val>::print(out, detail::tie_as_tuple(value));
#else
    ::pfr::detail::for_each_field_dispatcher(
        value,
        [&out](const auto& val) {
            // We can not reuse `fields_count_val` in lambda because compilers had issues with
            // passing constexpr variables into lambdas. Computing is again is the most portable solution.
            constexpr std::size_t fields_count_val_lambda = pfr::detail::fields_count<T>();
            detail::print_impl<0, fields_count_val_lambda>::print(out, val);
        },
        detail::make_index_sequence<fields_count_val>{}
    );
#endif
    return out << '}';
}


template <class Char, class Traits, class T>
std::basic_ostream<Char, Traits>& operator<<(std::basic_ostream<Char, Traits>& out, io_fields_impl<T>&& x) {
    return out << io_fields_impl<const std::remove_reference_t<T>&>{x.value};
}

template <class Char, class Traits, class T>
std::basic_istream<Char, Traits>& operator>>(std::basic_istream<Char, Traits>& in, io_fields_impl<T&>&& x) {
    T& value = x.value;
    constexpr std::size_t fields_count_val = pfr::detail::fields_count<T>();

    const auto prev_exceptions = in.exceptions();
    in.exceptions( typename std::basic_istream<Char, Traits>::iostate(0) );
    const auto prev_flags = in.flags( typename std::basic_istream<Char, Traits>::fmtflags(0) );

    char parenthis = {};
    in >> parenthis;
    if (parenthis != '{') in.setstate(std::basic_istream<Char, Traits>::failbit);

#if PFR_USE_CPP17 || PFR_USE_LOOPHOLE
    detail::read_impl<0, fields_count_val>::read(in, detail::tie_as_tuple(value));
#else
    ::pfr::detail::for_each_field_dispatcher(
        value,
        [&in](const auto& val) {
            // We can not reuse `fields_count_val` in lambda because compilers had issues with
            // passing constexpr variables into lambdas. Computing is again is the most portable solution.
            constexpr std::size_t fields_count_val_lambda = pfr::detail::fields_count<T>();
            detail::read_impl<0, fields_count_val_lambda>::read(in, val);
        },
        detail::make_index_sequence<fields_count_val>{}
    );
#endif

    in >> parenthis;
    if (parenthis != '}') in.setstate(std::basic_istream<Char, Traits>::failbit);

    in.flags(prev_flags);
    in.exceptions(prev_exceptions);

    return in;
}

template <class Char, class Traits, class T>
std::basic_istream<Char, Traits>& operator>>(std::basic_istream<Char, Traits>& in, io_fields_impl<const T&>&& ) {
    static_assert(sizeof(T) && false, "====================> Boost.PFR: Attempt to use istream operator on a pfr::io_fields wrapped type T with const qualifier.");
    return in;
}

template <class Char, class Traits, class T>
std::basic_istream<Char, Traits>& operator>>(std::basic_istream<Char, Traits>& in, io_fields_impl<T>&& ) {
    static_assert(sizeof(T) && false, "====================> Boost.PFR: Attempt to use istream operator on a pfr::io_fields wrapped temporary of type T.");
    return in;
}

PFR_END_MODULE_EXPORT

} // namespace detail

PFR_BEGIN_MODULE_EXPORT

/// IO manipulator to read/write \aggregate `value` field-by-field.
///
/// \b Example:
/// \code
///     struct my_struct {
///         int i;
///         short s;
///     };
///
///     std::ostream& operator<<(std::ostream& os, const my_struct& x) {
///         return os << pfr::io_fields(x);  // Equivalent to: os << "{ " << x.i << " ," <<  x.s << " }"
///     }
///
///     std::istream& operator>>(std::istream& is, my_struct& x) {
///         return is >> pfr::io_fields(x);  // Equivalent to: is >> "{ " >> x.i >> " ," >>  x.s >> " }"
///     }
/// \endcode
///
/// Input and output streaming operators for `pfr::io_fields` are symmetric, meaning that you get the original value by streaming it and
/// reading back if each fields streaming operator is symmetric.
///
/// \customio
template <class T>
auto io_fields(T&& value) noexcept {
    return detail::io_fields_impl<T>{std::forward<T>(value)};
}

PFR_END_MODULE_EXPORT

} // namespace pfr

#endif  // #if !defined(PFR_USE_MODULES) || defined(PFR_INTERFACE_UNIT)

#endif // PFR_IO_FIELDS_HPP
