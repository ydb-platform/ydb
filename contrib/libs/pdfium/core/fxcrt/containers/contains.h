// Copyright 2024 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FXCRT_CONTAINERS_CONTAINS_H_
#define CORE_FXCRT_CONTAINERS_CONTAINS_H_

#include <algorithm>
#include <iterator>
#include <type_traits>

#include "core/fxcrt/template_util.h"

namespace pdfium {

namespace internal {

// Small helper to detect whether a given type has a nested `key_type` typedef.
// Used below to catch misuses of the API for associative containers.
template <typename T, typename SFINAE = void>
struct HasKeyType : std::false_type {};

template <typename T>
struct HasKeyType<T, std::void_t<typename T::key_type>> : std::true_type {};

// Utility type traits used for specializing pdfium::Contains() below.
template <typename Container, typename Element, typename = void>
struct HasFindWithNpos : std::false_type {};

template <typename Container, typename Element>
struct HasFindWithNpos<
    Container,
    Element,
    std::void_t<decltype(std::declval<const Container&>().find(
                             std::declval<const Element&>()) !=
                         Container::npos)>> : std::true_type {};

template <typename Container, typename Element, typename = void>
struct HasFindWithEnd : std::false_type {};

template <typename Container, typename Element>
struct HasFindWithEnd<
    Container,
    Element,
    std::void_t<decltype(std::declval<const Container&>().find(
                             std::declval<const Element&>()) !=
                         std::declval<const Container&>().end())>>
    : std::true_type {};

template <typename Container, typename Element, typename = void>
struct HasContains : std::false_type {};

template <typename Container, typename Element>
struct HasContains<
    Container,
    Element,
    std::void_t<decltype(std::declval<const Container&>().contains(
        std::declval<const Element&>()))>> : std::true_type {};

}  // namespace internal

// General purpose implementation to check if |container| contains |value|.
template <typename Container,
          typename Value,
          std::enable_if_t<
              !internal::HasFindWithNpos<Container, Value>::value &&
              !internal::HasFindWithEnd<Container, Value>::value &&
              !internal::HasContains<Container, Value>::value>* = nullptr>
bool Contains(const Container& container, const Value& value) {
  static_assert(
      !internal::HasKeyType<Container>::value,
      "Error: About to perform linear search on an associative container. "
      "Either use a more generic comparator (e.g. std::less<>) or, if a linear "
      "search is desired, provide an explicit projection parameter.");
  using std::begin;
  using std::end;
  return std::find(begin(container), end(container), value) != end(container);
}

// Specialized Contains() implementation for when |container| has a find()
// member function and a static npos member, but no contains() member function.
template <typename Container,
          typename Value,
          std::enable_if_t<internal::HasFindWithNpos<Container, Value>::value &&
                           !internal::HasContains<Container, Value>::value>* =
              nullptr>
bool Contains(const Container& container, const Value& value) {
  return container.find(value) != Container::npos;
}

// Specialized Contains() implementation for when |container| has a find()
// and end() member function, but no contains() member function.
template <typename Container,
          typename Value,
          std::enable_if_t<internal::HasFindWithEnd<Container, Value>::value &&
                           !internal::HasContains<Container, Value>::value>* =
              nullptr>
bool Contains(const Container& container, const Value& value) {
  return container.find(value) != container.end();
}

// Specialized Contains() implementation for when |container| has a contains()
// member function.
template <
    typename Container,
    typename Value,
    std::enable_if_t<internal::HasContains<Container, Value>::value>* = nullptr>
bool Contains(const Container& container, const Value& value) {
  return container.contains(value);
}

}  // namespace pdfium

#endif  // CORE_FXCRT_CONTAINERS_CONTAINS_H_
