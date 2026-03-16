// Copyright 2024 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FXCRT_TEMPLATE_UTIL_H_
#define CORE_FXCRT_TEMPLATE_UTIL_H_

#include <stddef.h>
#include <iosfwd>
#include <iterator>
#include <type_traits>
#include <utility>

#include "build/build_config.h"

namespace pdfium {

template <class T> struct is_non_const_reference : std::false_type {};
template <class T> struct is_non_const_reference<T&> : std::true_type {};
template <class T> struct is_non_const_reference<const T&> : std::false_type {};

namespace internal {

// Uses expression SFINAE to detect whether using operator<< would work.
template <typename T, typename = void>
struct SupportsOstreamOperator : std::false_type {};
template <typename T>
struct SupportsOstreamOperator<T,
                               decltype(void(std::declval<std::ostream&>()
                                             << std::declval<T>()))>
    : std::true_type {};

// Used to detech whether the given type is an iterator.  This is normally used
// with std::enable_if to provide disambiguation for functions that take
// templatzed iterators as input.
template <typename T, typename = void>
struct is_iterator : std::false_type {};

template <typename T>
struct is_iterator<
    T,
    std::void_t<typename std::iterator_traits<T>::iterator_category>>
    : std::true_type {};

}  // namespace internal

}  // namespace pdfium

#endif  // CORE_FXCRT_TEMPLATE_UTIL_H_
