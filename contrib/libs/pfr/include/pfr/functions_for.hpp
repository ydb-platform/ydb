// Copyright (c) 2016-2025 Antony Polukhin
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef PFR_FUNCTIONS_FOR_HPP
#define PFR_FUNCTIONS_FOR_HPP
#pragma once

#include <pfr/detail/config.hpp>

#if !defined(PFR_USE_MODULES) || defined(PFR_INTERFACE_UNIT)
#include <pfr/ops_fields.hpp>
#include <pfr/io_fields.hpp>
#endif

/// \file pfr/functions_for.hpp
/// Contains PFR_FUNCTIONS_FOR macro that defined comparison and stream operators for T along with hash_value function.
/// \b Example:
/// \code
///     #include <pfr/functions_for.hpp>
///
///     namespace my_namespace {
///         struct my_struct {      // No operators defined for that structure
///             int i; short s; char data[7]; bool bl; int a,b,c,d,e,f;
///         };
///         PFR_FUNCTIONS_FOR(my_struct)
///     }
/// \endcode
///
/// \podops for other ways to define operators and more details.
///
/// \b Synopsis:

/// \def PFR_FUNCTIONS_FOR(T)
/// Defines comparison and stream operators for T along with hash_value function.
///
/// \b Example:
/// \code
///     #include <pfr/functions_for.hpp>
///     struct comparable_struct {      // No operators defined for that structure
///         int i; short s; char data[7]; bool bl; int a,b,c,d,e,f;
///     };
///     PFR_FUNCTIONS_FOR(comparable_struct)
///     // ...
///
///     comparable_struct s1 {0, 1, "Hello", false, 6,7,8,9,10,11};
///     comparable_struct s2 {0, 1, "Hello", false, 6,7,8,9,10,11111};
///     assert(s1 < s2);
///     std::cout << s1 << std::endl; // Outputs: {0, 1, H, e, l, l, o, , , 0, 6, 7, 8, 9, 10, 11}
/// \endcode
///
/// \podops for other ways to define operators and more details.
///
/// \b Defines \b following \b for \b T:
/// \code
/// bool operator==(const T& lhs, const T& rhs);
/// bool operator!=(const T& lhs, const T& rhs);
/// bool operator< (const T& lhs, const T& rhs);
/// bool operator> (const T& lhs, const T& rhs);
/// bool operator<=(const T& lhs, const T& rhs);
/// bool operator>=(const T& lhs, const T& rhs);
///
/// template <class Char, class Traits>
/// std::basic_ostream<Char, Traits>& operator<<(std::basic_ostream<Char, Traits>& out, const T& value);
///
/// template <class Char, class Traits>
/// std::basic_istream<Char, Traits>& operator>>(std::basic_istream<Char, Traits>& in, T& value);
///
/// // helper function for Boost unordered containers and boost::hash<>.
/// std::size_t hash_value(const T& value);
/// \endcode

#define PFR_FUNCTIONS_FOR(T)                                                                                                          \
    PFR_MAYBE_UNUSED inline bool operator==(const T& lhs, const T& rhs) { return ::pfr::eq_fields(lhs, rhs); }                 \
    PFR_MAYBE_UNUSED inline bool operator!=(const T& lhs, const T& rhs) { return ::pfr::ne_fields(lhs, rhs); }                 \
    PFR_MAYBE_UNUSED inline bool operator< (const T& lhs, const T& rhs) { return ::pfr::lt_fields(lhs, rhs); }                 \
    PFR_MAYBE_UNUSED inline bool operator> (const T& lhs, const T& rhs) { return ::pfr::gt_fields(lhs, rhs); }                 \
    PFR_MAYBE_UNUSED inline bool operator<=(const T& lhs, const T& rhs) { return ::pfr::le_fields(lhs, rhs); }                 \
    PFR_MAYBE_UNUSED inline bool operator>=(const T& lhs, const T& rhs) { return ::pfr::ge_fields(lhs, rhs); }                 \
    template <class Char, class Traits>                                                                                                     \
    PFR_MAYBE_UNUSED inline ::std::basic_ostream<Char, Traits>& operator<<(::std::basic_ostream<Char, Traits>& out, const T& value) { \
        return out << ::pfr::io_fields(value);                                                                                       \
    }                                                                                                                                       \
    template <class Char, class Traits>                                                                                                     \
    PFR_MAYBE_UNUSED inline ::std::basic_istream<Char, Traits>& operator>>(::std::basic_istream<Char, Traits>& in, T& value) {        \
        return in >> ::pfr::io_fields(value);                                                                                        \
    }                                                                                                                                       \
    PFR_MAYBE_UNUSED inline std::size_t hash_value(const T& v) {                                                                      \
        return ::pfr::hash_fields(v);                                                                                                \
    }                                                                                                                                       \
/**/

#endif // PFR_FUNCTIONS_FOR_HPP

