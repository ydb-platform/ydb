// Copyright 2021 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FXCRT_SPAN_UTIL_H_
#define CORE_FXCRT_SPAN_UTIL_H_

#include <stdint.h>

#include <optional>
#include <type_traits>

#include "core/fxcrt/check_op.h"
#include "core/fxcrt/fx_memcpy_wrappers.h"
#include "core/fxcrt/span.h"

namespace fxcrt {

// Bounds-checked byte-for-byte copies from spans into spans. Returns a
// span describing the remaining portion of the destination span.
template <typename T1,
          typename T2,
          size_t N1,
          size_t N2,
          typename P1,
          typename P2,
          typename = std::enable_if_t<sizeof(T1) == sizeof(T2) &&
                                      std::is_trivially_copyable_v<T1> &&
                                      std::is_trivially_copyable_v<T2>>>
inline pdfium::span<T1> spancpy(pdfium::span<T1, N1, P1> dst,
                                pdfium::span<T2, N2, P2> src) {
  CHECK_GE(dst.size(), src.size());
  // SAFETY: SFINAE ensures `sizeof(T1)` equals `sizeof(T2)`, so comparing
  // `size()` for equality ensures `size_bytes()` are equal, and `size_bytes()`
  // accurately describes `data()`.
  UNSAFE_BUFFERS(FXSYS_memcpy(dst.data(), src.data(), src.size_bytes()));
  return dst.subspan(src.size());
}

// Bounds-checked byte-for-byte moves from spans into spans. Returns a
// span describing the remaining portion of the destination span.
template <typename T1,
          typename T2,
          size_t N1,
          size_t N2,
          typename P1,
          typename P2,
          typename = std::enable_if_t<sizeof(T1) == sizeof(T2) &&
                                      std::is_trivially_copyable_v<T1> &&
                                      std::is_trivially_copyable_v<T2>>>
inline pdfium::span<T1> spanmove(pdfium::span<T1, N1, P1> dst,
                                 pdfium::span<T2, N2, P2> src) {
  CHECK_GE(dst.size(), src.size());
  // SAFETY: SFINAE ensures `sizeof(T1)` equals `sizeof(T2)`, so comparing
  // `size()` for equality ensures `size_bytes()` are equal, and `size_bytes()`
  // accurately describes `data()`.
  UNSAFE_BUFFERS(FXSYS_memmove(dst.data(), src.data(), src.size_bytes()));
  return dst.subspan(src.size());
}

// Bounds-checked byte-for-byte copies from spans into spans. Performs the
// copy if there is room, and returns true. Otherwise does not copy anything
// and returns false.
template <typename T1,
          typename T2,
          size_t N1,
          size_t N2,
          typename P1,
          typename P2,
          typename = std::enable_if_t<sizeof(T1) == sizeof(T2) &&
                                      std::is_trivially_copyable_v<T1> &&
                                      std::is_trivially_copyable_v<T2>>>
inline bool try_spancpy(pdfium::span<T1, N1, P1> dst,
                        pdfium::span<T2, N2, P2> src) {
  if (dst.size() < src.size()) {
    return false;
  }
  // SAFETY: SFINAE ensures `sizeof(T1)` equals `sizeof(T2)`, the test above
  // ensures `src.size()` <= `dst.size()` which implies `src.size_bytes()`
  // <= `dst.size_bytes()`, and `dst.size_bytes()` describes `dst.data()`.
  UNSAFE_BUFFERS(FXSYS_memcpy(dst.data(), src.data(), src.size_bytes()));
  return true;
}

// Bounds-checked byte-for-byte moves from spans into spans. Peforms the
// move if there is room, and returns true. Otherwise does not move anything
// and returns false.
template <typename T1,
          typename T2,
          size_t N1,
          size_t N2,
          typename P1,
          typename P2,
          typename = std::enable_if_t<sizeof(T1) == sizeof(T2) &&
                                      std::is_trivially_copyable_v<T1> &&
                                      std::is_trivially_copyable_v<T2>>>
inline bool try_spanmove(pdfium::span<T1, N1, P1> dst,
                         pdfium::span<T2, N2, P2> src) {
  if (dst.size() < src.size()) {
    return false;
  }
  // SAFETY: SFINAE ensures `sizeof(T1)` equals `sizeof(T2)`, the test above
  // ensures `src.size()` <= `dst.size()` which implies `src.size_bytes()`
  // <= `dst.size_bytes()`, and `dst.size_bytes()` describes `dst.data()`.
  UNSAFE_BUFFERS(FXSYS_memmove(dst.data(), src.data(), src.size_bytes()));
  return true;
}

// Bounds-checked byte-for-byte equality of same-sized spans. This is
// helpful because span does not (yet) have an operator==().
template <typename T1,
          typename T2,
          size_t N1,
          size_t N2,
          typename P1,
          typename P2,
          typename = std::enable_if_t<sizeof(T1) == sizeof(T2) &&
                                      std::is_trivially_copyable_v<T1> &&
                                      std::is_trivially_copyable_v<T2>>>
bool span_equals(pdfium::span<T1, N1, P1> s1, pdfium::span<T2, N2, P2> s2) {
  // SAFETY: For both `s1` and `s2`, there are `size_bytes()` valid bytes at
  // the corresponding `data()`, and the sizes are the same.
  return s1.size_bytes() == s2.size_bytes() &&
         UNSAFE_BUFFERS(FXSYS_memcmp(s1.data(), s2.data(), s1.size_bytes())) ==
             0;
}

// Returns the first position where `needle` occurs in `haystack`.
template <typename T,
          typename U,
          typename = std::enable_if_t<sizeof(T) == sizeof(U) &&
                                      std::is_trivially_copyable_v<T> &&
                                      std::is_trivially_copyable_v<U>>>
std::optional<size_t> spanpos(pdfium::span<T> haystack,
                              pdfium::span<U> needle) {
  if (needle.empty() || needle.size() > haystack.size()) {
    return std::nullopt;
  }
  // After this `end_pos`, not enough characters remain in `haystack` for
  // a full match to occur.
  size_t end_pos = haystack.size() - needle.size();
  for (size_t haystack_pos = 0; haystack_pos <= end_pos; ++haystack_pos) {
    auto candidate = haystack.subspan(haystack_pos, needle.size());
    if (fxcrt::span_equals(candidate, needle)) {
      return haystack_pos;
    }
  }
  return std::nullopt;
}

template <typename T,
          typename U,
          typename = typename std::enable_if_t<std::is_const_v<T> ||
                                               !std::is_const_v<U>>>
inline pdfium::span<T> reinterpret_span(pdfium::span<U> s) noexcept {
  CHECK(alignof(T) == alignof(U) ||
        reinterpret_cast<uintptr_t>(s.data()) % alignof(T) == 0u);
  // SAFETY: relies on correct conversion of size_bytes() result.
  return UNSAFE_BUFFERS(pdfium::make_span(reinterpret_cast<T*>(s.data()),
                                          s.size_bytes() / sizeof(T)));
}

}  // namespace fxcrt

#endif  // CORE_FXCRT_SPAN_UTIL_H_
