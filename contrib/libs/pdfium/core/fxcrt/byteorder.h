// Copyright 2019 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FXCRT_BYTEORDER_H_
#define CORE_FXCRT_BYTEORDER_H_

#include <stdint.h>

#include "build/build_config.h"
#include "core/fxcrt/span.h"

#if defined(COMPILER_MSVC)
#include <stdlib.h>
#endif

namespace fxcrt {

namespace internal {

#if defined(COMPILER_MSVC) && !defined(__clang__)
// TODO(thestig): See
// https://developercommunity.visualstudio.com/t/Mark-some-built-in-functions-as-constexp/362558
// https://developercommunity.visualstudio.com/t/constexpr-byte-swapping-optimization/983963
#define FXCRT_BYTESWAPS_CONSTEXPR
#else
#define FXCRT_BYTESWAPS_CONSTEXPR constexpr
#endif

// Returns a value with all bytes in |x| swapped, i.e. reverses the endianness.
// TODO(thestig): Once C++23 is available, replace with std::byteswap.
inline FXCRT_BYTESWAPS_CONSTEXPR uint16_t ByteSwap(uint16_t x) {
#if defined(COMPILER_MSVC) && !defined(__clang__)
  return _byteswap_ushort(x);
#else
  return __builtin_bswap16(x);
#endif
}

inline FXCRT_BYTESWAPS_CONSTEXPR uint32_t ByteSwap(uint32_t x) {
#if defined(COMPILER_MSVC) && !defined(__clang__)
  return _byteswap_ulong(x);
#else
  return __builtin_bswap32(x);
#endif
}

#undef FXCRT_BYTESWAPS_CONSTEXPR

}  // namespace internal

// NOTE: Prefer From*() methods when data is known to be aligned.

// Converts the bytes in |x| from host order (endianness) to little endian, and
// returns the result.
inline uint16_t FromLE16(uint16_t x) {
#if defined(ARCH_CPU_LITTLE_ENDIAN)
  return x;
#else
  return internal::ByteSwap(x);
#endif
}

inline uint32_t FromLE32(uint32_t x) {
#if defined(ARCH_CPU_LITTLE_ENDIAN)
  return x;
#else
  return internal::ByteSwap(x);
#endif
}

// Converts the bytes in |x| from host order (endianness) to big endian, and
// returns the result.
inline uint16_t FromBE16(uint16_t x) {
#if defined(ARCH_CPU_LITTLE_ENDIAN)
  return internal::ByteSwap(x);
#else
  return x;
#endif
}

inline uint32_t FromBE32(uint32_t x) {
#if defined(ARCH_CPU_LITTLE_ENDIAN)
  return internal::ByteSwap(x);
#else
  return x;
#endif
}

// Transfer to/from spans irrespective of alignments.
inline uint16_t GetUInt16MSBFirst(pdfium::span<const uint8_t, 2> span) {
  return (static_cast<uint32_t>(span[0]) << 8) | static_cast<uint32_t>(span[1]);
}

inline uint32_t GetUInt32MSBFirst(pdfium::span<const uint8_t, 4> span) {
  return (static_cast<uint32_t>(span[0]) << 24) |
         (static_cast<uint32_t>(span[1]) << 16) |
         (static_cast<uint32_t>(span[2]) << 8) | static_cast<uint32_t>(span[3]);
}

inline uint16_t GetUInt16LSBFirst(pdfium::span<const uint8_t, 2> span) {
  return (static_cast<uint32_t>(span[1]) << 8) | static_cast<uint32_t>(span[0]);
}

inline uint32_t GetUInt32LSBFirst(pdfium::span<const uint8_t, 4> span) {
  return (static_cast<uint32_t>(span[3]) << 24) |
         (static_cast<uint32_t>(span[2]) << 16) |
         (static_cast<uint32_t>(span[1]) << 8) | static_cast<uint32_t>(span[0]);
}

inline void PutUInt16MSBFirst(uint16_t value, pdfium::span<uint8_t, 2> span) {
  span[0] = value >> 8;
  span[1] = value & 0xff;
}

inline void PutUInt32MSBFirst(uint32_t value, pdfium::span<uint8_t, 4> span) {
  span[0] = value >> 24;
  span[1] = (value >> 16) & 0xff;
  span[2] = (value >> 8) & 0xff;
  span[3] = value & 0xff;
}

inline void PutUInt16LSBFirst(uint16_t value, pdfium::span<uint8_t, 2> span) {
  span[1] = value >> 8;
  span[0] = value & 0xff;
}

inline void PutUInt32LSBFirst(uint32_t value, pdfium::span<uint8_t, 4> span) {
  span[3] = value >> 24;
  span[2] = (value >> 16) & 0xff;
  span[1] = (value >> 8) & 0xff;
  span[0] = value & 0xff;
}

}  // namespace fxcrt

#endif  // CORE_FXCRT_BYTEORDER_H_
