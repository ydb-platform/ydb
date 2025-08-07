// Copyright (c) 2016-2025 Antony Polukhin
// Copyright (c) 2022 Denis Mikhailov
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef PFR_CONFIG_HPP
#define PFR_CONFIG_HPP
#pragma once

#if !defined(PFR_USE_MODULES) && (__cplusplus >= 201402L || (defined(_MSC_VER) && defined(_MSVC_LANG) && _MSC_VER > 1900))
#include <type_traits> // to get non standard platform macro definitions (__GLIBCXX__ for example)
#elif defined(PFR_USE_MODULES)
#include <version>
#endif

/// \file pfr/config.hpp
/// Contains all the macros that describe Boost.PFR configuration, like PFR_ENABLED
///
/// \note This header file doesn't require C++14 Standard and supports all C++ compilers, even pre C++14 compilers (C++11, C++03...).

// Reminder:
//  * MSVC++ 14.2 _MSC_VER == 1927 <- Loophole is known to work (Visual Studio ????)
//  * MSVC++ 14.1 _MSC_VER == 1916 <- Loophole is known to NOT work (Visual Studio 2017)
//  * MSVC++ 14.0 _MSC_VER == 1900 (Visual Studio 2015)
//  * MSVC++ 12.0 _MSC_VER == 1800 (Visual Studio 2013)

#ifdef PFR_NOT_SUPPORTED
#   error Please, do not set PFR_NOT_SUPPORTED value manually, use '-DPFR_ENABLED=0' instead of it
#endif

#if defined(_MSC_VER)
#   if !defined(_MSVC_LANG) || _MSC_VER <= 1900
#       define PFR_NOT_SUPPORTED 1
#   endif
#elif __cplusplus < 201402L
#   define PFR_NOT_SUPPORTED 1
#endif

#ifndef PFR_USE_LOOPHOLE
#   if defined(_MSC_VER)
#       if _MSC_VER >= 1927
#           define PFR_USE_LOOPHOLE 1
#       else
#           define PFR_USE_LOOPHOLE 0
#       endif
#   elif defined(__clang_major__) && __clang_major__ >= 8
#       define PFR_USE_LOOPHOLE 0
#   else
#       define PFR_USE_LOOPHOLE 1
#   endif
#endif

#ifndef PFR_USE_CPP17
#   ifdef __cpp_structured_bindings
#       define PFR_USE_CPP17 1
#   elif defined(_MSVC_LANG)
#       if _MSVC_LANG >= 201703L
#           define PFR_USE_CPP17 1
#       else
#           define PFR_USE_CPP17 0
#       endif
#   else
#       define PFR_USE_CPP17 0
#   endif
#endif

#if (!PFR_USE_CPP17 && !PFR_USE_LOOPHOLE)
#   if (defined(_MSC_VER) && _MSC_VER < 1916) ///< in Visual Studio 2017 v15.9 PFR library with classic engine normally works
#      define PFR_NOT_SUPPORTED 1
#   endif
#endif

#ifndef PFR_USE_STD_MAKE_INTEGRAL_SEQUENCE
#   if defined(PFR_USE_MODULES)
#       define PFR_USE_STD_MAKE_INTEGRAL_SEQUENCE 1
// Assume that libstdc++ since GCC-7.3 does not have linear instantiation depth in std::make_integral_sequence
#   elif defined( __GLIBCXX__) && __GLIBCXX__ >= 20180101
#       define PFR_USE_STD_MAKE_INTEGRAL_SEQUENCE 1
#   elif defined(_MSC_VER)
#       define PFR_USE_STD_MAKE_INTEGRAL_SEQUENCE 1
//# elif other known working lib
#   else
#       define PFR_USE_STD_MAKE_INTEGRAL_SEQUENCE 0
#   endif
#endif

#ifndef PFR_HAS_GUARANTEED_COPY_ELISION
#   if  defined(__cpp_guaranteed_copy_elision) && (!defined(_MSC_VER) || _MSC_VER > 1928)
#       define PFR_HAS_GUARANTEED_COPY_ELISION 1
#   else
#       define PFR_HAS_GUARANTEED_COPY_ELISION 0
#   endif
#endif

#ifndef PFR_ENABLE_IMPLICIT_REFLECTION
#   if  defined(__cpp_lib_is_aggregate)
#       define PFR_ENABLE_IMPLICIT_REFLECTION 1
#   else
// There is no way to detect potential ability to be reflectable without std::is_aggregare
#       define PFR_ENABLE_IMPLICIT_REFLECTION 0
#   endif
#endif

#ifndef PFR_CORE_NAME_ENABLED
#   if  (__cplusplus >= 202002L) || (defined(_MSVC_LANG) && (_MSVC_LANG >= 202002L))
#       if (defined(__cpp_nontype_template_args) && __cpp_nontype_template_args >= 201911) \
         || (defined(__clang_major__) && __clang_major__ >= 12)
#           define PFR_CORE_NAME_ENABLED 1
#       else
#           define PFR_CORE_NAME_ENABLED 0
#       endif
#   else
#       define PFR_CORE_NAME_ENABLED 0
#   endif
#endif


#ifndef PFR_CORE_NAME_PARSING
#   if defined(_MSC_VER) && !defined(__clang__)
#       define PFR_CORE_NAME_PARSING (sizeof("auto __cdecl pfr::detail::name_of_field_impl<") - 1, sizeof(">(void) noexcept") - 1, backward("->"))
#   elif defined(__clang__)
#       define PFR_CORE_NAME_PARSING (sizeof("auto pfr::detail::name_of_field_impl() [MsvcWorkaround = ") - 1, sizeof("}]") - 1, backward("."))
#   elif defined(__GNUC__)
#       define PFR_CORE_NAME_PARSING (sizeof("consteval auto pfr::detail::name_of_field_impl() [with MsvcWorkaround = ") - 1, sizeof(")]") - 1, backward("::"))
#   else
// Default parser for other platforms... Just skip nothing!
#       define PFR_CORE_NAME_PARSING (0, 0, "")
#   endif
#endif

#if defined(__has_cpp_attribute)
#   if __has_cpp_attribute(maybe_unused)
#       define PFR_MAYBE_UNUSED [[maybe_unused]]
#   endif
#endif

#ifndef PFR_MAYBE_UNUSED
#   define PFR_MAYBE_UNUSED
#endif

#ifndef PFR_ENABLED
#   ifdef PFR_NOT_SUPPORTED
#       define PFR_ENABLED 0
#   else
#       define PFR_ENABLED 1
#   endif
#endif

#undef PFR_NOT_SUPPORTED

#ifdef PFR_INTERFACE_UNIT
#   define PFR_BEGIN_MODULE_EXPORT export {
#   define PFR_END_MODULE_EXPORT }
#else
#   define PFR_BEGIN_MODULE_EXPORT
#   define PFR_END_MODULE_EXPORT
#endif

#if defined(PFR_USE_MODULES) && !defined(PFR_INTERFACE_UNIT)
import pfr;
#endif

#endif // PFR_CONFIG_HPP
