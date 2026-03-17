// Copyright 2015 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FXCRT_STL_UTIL_H_
#define CORE_FXCRT_STL_UTIL_H_

#include <algorithm>
#include <array>
#include <iterator>
#include <memory>

#include "core/fxcrt/check_op.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/numerics/safe_conversions.h"

namespace fxcrt {

// Means of generating a key for searching STL collections of std::unique_ptr
// that avoids the side effect of deleting the pointer.
template <class T>
class FakeUniquePtr : public std::unique_ptr<T> {
 public:
  using std::unique_ptr<T>::unique_ptr;
  ~FakeUniquePtr() { std::unique_ptr<T>::release(); }
};

// Type-deducing wrapper for FakeUniquePtr<T>.
template <class T>
FakeUniquePtr<T> MakeFakeUniquePtr(T* arg) {
  return FakeUniquePtr<T>(arg);
}

// Convenience routine for "int-fected" code, so that the stl collection
// size_t size() method return values will be checked.
template <typename ResultType, typename Collection>
ResultType CollectionSize(const Collection& collection) {
  return pdfium::checked_cast<ResultType>(collection.size());
}

// Convenience routine for "int-fected" code, to handle signed indicies. The
// compiler can deduce the type, making this more convenient than the above.
template <typename IndexType, typename Collection>
bool IndexInBounds(const Collection& collection, IndexType index) {
  return index >= 0 && index < CollectionSize<IndexType>(collection);
}

// Equivalent of C++20 std::ranges::fill().
template <typename T, typename V>
void Fill(T&& container, const V& value) {
  std::fill(std::begin(container), std::end(container), value);
}

// Non-flawed version of C++20 std::ranges::copy(), which takes an output
// range as the second parameter and CHECKS() if it not sufficiently sized.
template <typename T, typename U>
void Copy(const T& source_container, U&& dest_container) {
  static_assert(sizeof(source_container[0]) == sizeof(dest_container[0]));
  CHECK_GE(std::size(dest_container), std::size(source_container));
  std::copy(std::begin(source_container), std::end(source_container),
            std::begin(dest_container));
}

// ToArray<>() implementation as taken from chromium /base. Replace with
// std::to_array<>() when C++20 becomes available.
//
// Helper inspired by C++20's std::to_array to convert a C-style array to a
// std::array. As opposed to the C++20 version this implementation does not
// provide an overload for rvalues and does not strip cv qualifers from the
// returned std::array::value_type. The returned value_type needs to be
// specified explicitly, allowing the construction of std::arrays with const
// elements.
//
// Reference: https://en.cppreference.com/w/cpp/container/array/to_array
template <typename U, typename T, size_t N, size_t... I>
constexpr std::array<U, N> ToArrayImpl(const T (&data)[N],
                                       std::index_sequence<I...>) {
  // SAFETY: compiler-deduced size `N`.
  return UNSAFE_BUFFERS({{static_cast<U>(data[I])...}});
}

template <typename U, size_t N>
constexpr std::array<U, N> ToArray(const U (&data)[N]) {
  return ToArrayImpl<U>(data, std::make_index_sequence<N>());
}

}  // namespace fxcrt

#endif  // CORE_FXCRT_STL_UTIL_H_
