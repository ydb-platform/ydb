// Copyright 2017 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef HIGHWAYHASH_HH_BUFFER_H_
#define HIGHWAYHASH_HH_BUFFER_H_

// Helper functions used by hh_avx2 and hh_sse41.

// WARNING: this is a "restricted" header because it is included from
// translation units compiled with different flags. This header and its
// dependencies must not define any function unless it is static inline and/or
// within namespace HH_TARGET_NAME. See arch_specific.h for details.

#include "highwayhash/vector128.h"

// For auto-dependency generation, we need to include all headers but not their
// contents (otherwise compilation fails because -msse4.1 is not specified).
#ifndef HH_DISABLE_TARGET_SPECIFIC

namespace highwayhash {
// To prevent ODR violations when including this from multiple translation
// units (TU) that are compiled with different flags, the contents must reside
// in a namespace whose name is unique to the TU. NOTE: this behavior is
// incompatible with precompiled modules and requires textual inclusion instead.
namespace HH_TARGET_NAME {

template <uint32_t kSizeOffset>
struct IntMask {};  // primary template

template <>
struct IntMask<0> {
  // Returns 32-bit lanes : ~0U if that lane can be loaded given "size" bytes.
  // Typical case: size = 0..16, nothing deducted.
  HH_INLINE V4x32U operator()(const V4x32U& size) const {
    // Lane n is valid if size >= (n + 1) * 4; subtract one because we only have
    // greater-than comparisons and don't want a negated mask.
    return V4x32U(_mm_cmpgt_epi32(size, V4x32U(15, 11, 7, 3)));
  }
};

template <>
struct IntMask<16> {
  // "size" is 16..31; this is for loading the upper half of a packet, so
  // effectively deduct 16 from size by changing the comparands.
  HH_INLINE V4x32U operator()(const V4x32U& size) const {
    return V4x32U(_mm_cmpgt_epi32(size, V4x32U(31, 27, 23, 19)));
  }
};

// Inserts "bytes4" into "prev" at the lowest i such that mask[i] = 0.
// Assumes prev[j] == 0 if mask[j] = 0.
HH_INLINE V4x32U Insert4AboveMask(const uint32_t bytes4, const V4x32U& mask,
                                  const V4x32U& prev) {
  // There is no 128-bit shift by a variable count. Using shuffle_epi8 with a
  // control mask requires a table lookup. We know the shift count is a
  // multiple of 4 bytes, so we can broadcastd_epi32 and clear all lanes except
  // those where mask != 0. This works because any upper output lanes need not
  // be zero.
  return prev | AndNot(mask, V4x32U(bytes4));
}

// Shifts "suffix" left by "prefix_len" = 0..15 bytes, clears upper bytes of
// "prefix", and returns the merged/concatenated bytes.
HH_INLINE V4x32U Concatenate(const V4x32U& prefix, const size_t prefix_len,
                             const V4x32U& suffix) {
  static const uint64_t table[V16x8U::N][V2x64U::N] = {
      {0x0706050403020100ull, 0x0F0E0D0C0B0A0908ull},
      {0x06050403020100FFull, 0x0E0D0C0B0A090807ull},
      {0x050403020100FFFFull, 0x0D0C0B0A09080706ull},
      {0x0403020100FFFFFFull, 0x0C0B0A0908070605ull},
      {0x03020100FFFFFFFFull, 0x0B0A090807060504ull},
      {0x020100FFFFFFFFFFull, 0x0A09080706050403ull},
      {0x0100FFFFFFFFFFFFull, 0x0908070605040302ull},
      {0x00FFFFFFFFFFFFFFull, 0x0807060504030201ull},
      {0xFFFFFFFFFFFFFFFFull, 0x0706050403020100ull},
      {0xFFFFFFFFFFFFFFFFull, 0x06050403020100FFull},
      {0xFFFFFFFFFFFFFFFFull, 0x050403020100FFFFull},
      {0xFFFFFFFFFFFFFFFFull, 0x0403020100FFFFFFull},
      {0xFFFFFFFFFFFFFFFFull, 0x03020100FFFFFFFFull},
      {0xFFFFFFFFFFFFFFFFull, 0x020100FFFFFFFFFFull},
      {0xFFFFFFFFFFFFFFFFull, 0x0100FFFFFFFFFFFFull},
      {0xFFFFFFFFFFFFFFFFull, 0x00FFFFFFFFFFFFFFull}};
  const V2x64U control = Load<V2x64U>(&table[prefix_len][0]);
  const V2x64U shifted_suffix(_mm_shuffle_epi8(suffix, control));
  return V4x32U(_mm_blendv_epi8(shifted_suffix, prefix, control));
}

}  // namespace HH_TARGET_NAME
}  // namespace highwayhash

#endif  // HH_DISABLE_TARGET_SPECIFIC
#endif  // HIGHWAYHASH_HH_BUFFER_H_
