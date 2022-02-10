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

#ifndef HIGHWAYHASH_HH_LOAD3_H_
#define HIGHWAYHASH_HH_LOAD3_H_

// WARNING: this is a "restricted" header because it is included from
// translation units compiled with different flags. This header and its
// dependencies must not define any function unless it is static inline and/or
// within namespace HH_TARGET_NAME. See arch_specific.h for details.

#include <stddef.h>
#include <stdint.h>

#include "highwayhash/arch_specific.h"
#include "highwayhash/compiler_specific.h"
#include "highwayhash/endianess.h"

namespace highwayhash {
// To prevent ODR violations when including this from multiple translation
// units (TU) that are compiled with different flags, the contents must reside
// in a namespace whose name is unique to the TU. NOTE: this behavior is
// incompatible with precompiled modules and requires textual inclusion instead.
namespace HH_TARGET_NAME {

// Loads 0 to 3 bytes from a given location using one of several policies.
// These are potentially faster than 8-bit loads, but require certain additional
// promises by the caller: that 'out of bounds' memory accesses are allowed,
// and/or that the bytes may be permuted or duplicated.
class Load3 {
 public:
  // In increasing order of complexity:
  struct AllowReadBeforeAndReturn {};
  struct AllowReadBefore {};
  struct AllowUnordered {};
  struct AllowNone {};

  // Up to 4 preceding bytes may be read and returned along with the 0..3
  // valid bytes. The valid bytes are in little-endian order, except that the
  // preceding bytes occupy the least-significant bytes.
  HH_INLINE uint32_t operator()(AllowReadBeforeAndReturn, const char* from,
                                const size_t size_mod4) {
    // It's safe to read before "from", so we can load 32 bits, which is faster
    // than individual byte loads. We assume little-endian byte order, so
    // big-endian platforms will need to swap. Type punning can generate
    // incorrect code if compiled with strict aliasing; the only safe
    // alternatives are memcpy and reading through char*. We must avoid memcpy
    // because string.h must not be included per the warning above. On GCC and
    // Clang, we can use a builtin instead.
    uint32_t last4;
    Copy(from + size_mod4 - 4, 4, reinterpret_cast<char*>(&last4));
    return host_from_le32(last4);
  }

  // As above, but preceding bytes are removed and upper byte(s) are zero.
  HH_INLINE uint64_t operator()(AllowReadBefore, const char* from,
                                const size_t size_mod4) {
    // Shift 0..3 valid bytes into LSB as if loaded in little-endian order.
    // 64-bit type enables 32-bit shift when size_mod4 == 0.
    uint64_t last3 = operator()(AllowReadBeforeAndReturn(), from, size_mod4);
    last3 >>= 32 - (size_mod4 * 8);
    return last3;
  }

  // The bytes need not be loaded in little-endian order. This particular order
  // (and the duplication of some bytes depending on "size_mod4") was chosen for
  // computational convenience and can no longer be changed because it is part
  // of the HighwayHash length padding definition.
  HH_INLINE uint64_t operator()(AllowUnordered, const char* from,
                                const size_t size_mod4) {
    uint64_t last3 = 0;
    // Not allowed to read any bytes; early-out is faster than reading from a
    // constant array of zeros.
    if (size_mod4 == 0) {
      return last3;
    }

    // These indices are chosen as an easy-to-compute sequence containing the
    // same elements as [0, size), but repeated and/or reordered. This enables
    // unconditional loads, which outperform conditional 8 or 16+8 bit loads.
    const uint64_t idx0 = 0;
    const uint64_t idx1 = size_mod4 >> 1;
    const uint64_t idx2 = size_mod4 - 1;
    // Store into least significant bytes (avoids one shift).
    last3 = static_cast<uint64_t>(from[idx0]);
    last3 += static_cast<uint64_t>(from[idx1]) << 8;
    last3 += static_cast<uint64_t>(from[idx2]) << 16;
    return last3;
  }

  // Must read exactly [0, size) bytes in little-endian order.
  HH_INLINE uint64_t operator()(AllowNone, const char* from,
                                const size_t size_mod4) {
    // We need to load in little-endian order without accessing anything outside
    // [from, from + size_mod4). Unrolling is faster than looping backwards.
    uint64_t last3 = 0;
    if (size_mod4 >= 1) {
      last3 += U64FromChar(from[0]);
    }
    if (size_mod4 >= 2) {
      last3 += U64FromChar(from[1]) << 8;
    }
    if (size_mod4 == 3) {
      last3 += U64FromChar(from[2]) << 16;
    }
    return last3;
  }

 private:
  static HH_INLINE uint32_t U32FromChar(const char c) {
    return static_cast<uint32_t>(static_cast<unsigned char>(c));
  }

  static HH_INLINE uint64_t U64FromChar(const char c) {
    return static_cast<uint64_t>(static_cast<unsigned char>(c));
  }

  static HH_INLINE void Copy(const char* HH_RESTRICT from, const size_t size,
                             char* HH_RESTRICT to) {
#if HH_MSC_VERSION
    for (size_t i = 0; i < size; ++i) {
      to[i] = from[i];
    }
#else
    __builtin_memcpy(to, from, size);
#endif
  }
};

}  // namespace HH_TARGET_NAME
}  // namespace highwayhash

#endif  // HIGHWAYHASH_LOAD3_H_
