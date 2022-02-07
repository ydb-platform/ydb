// Copyright 2015-2017 Google Inc. All Rights Reserved.
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

#ifndef HIGHWAYHASH_HH_PORTABLE_H_
#define HIGHWAYHASH_HH_PORTABLE_H_

// WARNING: this is a "restricted" header because it is included from
// translation units compiled with different flags. This header and its
// dependencies must not define any function unless it is static inline and/or
// within namespace HH_TARGET_NAME. See arch_specific.h for details.

#include "highwayhash/arch_specific.h"
#include "highwayhash/compiler_specific.h"
#include "highwayhash/endianess.h"
#include "highwayhash/hh_types.h"
#include "highwayhash/load3.h"

namespace highwayhash {
// See vector128.h for why this namespace is necessary; we match it here for
// consistency. As a result, this header requires textual inclusion.
namespace HH_TARGET_NAME {

class HHStatePortable {
 public:
  static const int kNumLanes = 4;
  using Lanes = uint64_t[kNumLanes];

  explicit HH_INLINE HHStatePortable(const HHKey keys) { Reset(keys); }

  HH_INLINE void Reset(const HHKey keys) {
    static const Lanes init0 = {0xdbe6d5d5fe4cce2full, 0xa4093822299f31d0ull,
                                0x13198a2e03707344ull, 0x243f6a8885a308d3ull};
    static const Lanes init1 = {0x3bd39e10cb0ef593ull, 0xc0acf169b5f18a8cull,
                                0xbe5466cf34e90c6cull, 0x452821e638d01377ull};
    Lanes rotated_keys;
    Rotate64By32(keys, &rotated_keys);
    Copy(init0, &mul0);
    Copy(init1, &mul1);
    Xor(init0, keys, &v0);
    Xor(init1, rotated_keys, &v1);
  }

  HH_INLINE void Update(const HHPacket& packet) {
    Lanes packet_lanes;
    CopyPartial(&packet[0], sizeof(HHPacket),
                reinterpret_cast<char*>(&packet_lanes));
    for (int lane = 0; lane < kNumLanes; ++lane) {
      packet_lanes[lane] = host_from_le64(packet_lanes[lane]);
    }
    Update(packet_lanes);
  }

  HH_INLINE void UpdateRemainder(const char* bytes, const size_t size_mod32) {
    // 'Length padding' differentiates zero-valued inputs that have the same
    // size/32. mod32 is sufficient because each Update behaves as if a
    // counter were injected, because the state is large and mixed thoroughly.
    const uint64_t mod32_pair = (size_mod32 << 32) + size_mod32;
    for (int lane = 0; lane < kNumLanes; ++lane) {
      v0[lane] += mod32_pair;
    }
    Rotate32By(reinterpret_cast<uint32_t*>(&v1), size_mod32);

    const size_t size_mod4 = size_mod32 & 3;
    const char* remainder = bytes + (size_mod32 & ~3);

    HHPacket packet HH_ALIGNAS(32) = {0};
    CopyPartial(bytes, remainder - bytes, &packet[0]);

    if (size_mod32 & 16) {  // 16..31 bytes left
      // Read the last 0..3 bytes and previous 1..4 into the upper bits.
      // Insert into the upper four bytes of packet, which are zero.
      uint32_t last4 =
          Load3()(Load3::AllowReadBeforeAndReturn(), remainder, size_mod4);
      CopyPartial(reinterpret_cast<const char*>(&last4), 4, &packet[28]);
    } else {  // size_mod32 < 16
      uint64_t last4 = Load3()(Load3::AllowUnordered(), remainder, size_mod4);

      // Rather than insert at packet + 28, it is faster to initialize
      // the otherwise empty packet + 16 with up to 64 bits of padding.
      CopyPartial(reinterpret_cast<const char*>(&last4), sizeof(last4),
                  &packet[16]);
    }
    Update(packet);
  }

  HH_INLINE void Finalize(HHResult64* HH_RESTRICT result) {
    PermuteAndUpdate();
    PermuteAndUpdate();
    PermuteAndUpdate();
    PermuteAndUpdate();

    *result = v0[0] + v1[0] + mul0[0] + mul1[0];
  }

  HH_INLINE void Finalize(HHResult128* HH_RESTRICT result) {
    PermuteAndUpdate();
    PermuteAndUpdate();
    PermuteAndUpdate();
    PermuteAndUpdate();

    (*result)[0] = v0[0] + mul0[0] + v1[2] + mul1[2];
    (*result)[1] = v0[1] + mul0[1] + v1[3] + mul1[3];
  }

  HH_INLINE void Finalize(HHResult256* HH_RESTRICT result) {
    PermuteAndUpdate();
    PermuteAndUpdate();
    PermuteAndUpdate();
    PermuteAndUpdate();

    ModularReduction(v1[1] + mul1[1], v1[0] + mul1[0], v0[1] + mul0[1],
                     v0[0] + mul0[0], &(*result)[1], &(*result)[0]);
    ModularReduction(v1[3] + mul1[3], v1[2] + mul1[2], v0[3] + mul0[3],
                     v0[2] + mul0[2], &(*result)[3], &(*result)[2]);
  }

  static HH_INLINE void ZeroInitialize(char* HH_RESTRICT buffer) {
    for (size_t i = 0; i < sizeof(HHPacket); ++i) {
      buffer[i] = 0;
    }
  }

  static HH_INLINE void CopyPartial(const char* HH_RESTRICT from,
                                    const size_t size_mod32,
                                    char* HH_RESTRICT buffer) {
    for (size_t i = 0; i < size_mod32; ++i) {
      buffer[i] = from[i];
    }
  }

  static HH_INLINE void AppendPartial(const char* HH_RESTRICT from,
                                      const size_t size_mod32,
                                      char* HH_RESTRICT buffer,
                                      const size_t buffer_valid) {
    for (size_t i = 0; i < size_mod32; ++i) {
      buffer[buffer_valid + i] = from[i];
    }
  }

  HH_INLINE void AppendAndUpdate(const char* HH_RESTRICT from,
                                 const size_t size_mod32,
                                 const char* HH_RESTRICT buffer,
                                 const size_t buffer_valid) {
    HHPacket tmp HH_ALIGNAS(32);
    for (size_t i = 0; i < buffer_valid; ++i) {
      tmp[i] = buffer[i];
    }
    for (size_t i = 0; i < size_mod32; ++i) {
      tmp[buffer_valid + i] = from[i];
    }
    Update(tmp);
  }

 private:
  static HH_INLINE void Copy(const Lanes& source, Lanes* HH_RESTRICT dest) {
    for (int lane = 0; lane < kNumLanes; ++lane) {
      (*dest)[lane] = source[lane];
    }
  }

  static HH_INLINE void Add(const Lanes& source, Lanes* HH_RESTRICT dest) {
    for (int lane = 0; lane < kNumLanes; ++lane) {
      (*dest)[lane] += source[lane];
    }
  }

  template <typename LanesOrPointer>
  static HH_INLINE void Xor(const Lanes& op1, const LanesOrPointer& op2,
                            Lanes* HH_RESTRICT dest) {
    for (int lane = 0; lane < kNumLanes; ++lane) {
      (*dest)[lane] = op1[lane] ^ op2[lane];
    }
  }

// Clears all bits except one byte at the given offset.
#define MASK(v, bytes) ((v) & (0xFFull << ((bytes)*8)))

  // 16-byte permutation; shifting is about 10% faster than byte loads.
  // Adds zipper-merge result to add*.
  static HH_INLINE void ZipperMergeAndAdd(const uint64_t v1, const uint64_t v0,
                                          uint64_t* HH_RESTRICT add1,
                                          uint64_t* HH_RESTRICT add0) {
    *add0 += ((MASK(v0, 3) + MASK(v1, 4)) >> 24) +
             ((MASK(v0, 5) + MASK(v1, 6)) >> 16) + MASK(v0, 2) +
             (MASK(v0, 1) << 32) + (MASK(v1, 7) >> 8) + (v0 << 56);

    *add1 += ((MASK(v1, 3) + MASK(v0, 4)) >> 24) + MASK(v1, 2) +
             (MASK(v1, 5) >> 16) + (MASK(v1, 1) << 24) + (MASK(v0, 6) >> 8) +
             (MASK(v1, 0) << 48) + MASK(v0, 7);
  }

#undef MASK

  // For inputs that are already in native byte order (e.g. PermuteAndAdd)
  HH_INLINE void Update(const Lanes& packet_lanes) {
    Add(packet_lanes, &v1);
    Add(mul0, &v1);

    // (Loop is faster than unrolling)
    for (int lane = 0; lane < kNumLanes; ++lane) {
      const uint32_t v1_32 = static_cast<uint32_t>(v1[lane]);
      mul0[lane] ^= v1_32 * (v0[lane] >> 32);
      v0[lane] += mul1[lane];
      const uint32_t v0_32 = static_cast<uint32_t>(v0[lane]);
      mul1[lane] ^= v0_32 * (v1[lane] >> 32);
    }

    ZipperMergeAndAdd(v1[1], v1[0], &v0[1], &v0[0]);
    ZipperMergeAndAdd(v1[3], v1[2], &v0[3], &v0[2]);

    ZipperMergeAndAdd(v0[1], v0[0], &v1[1], &v1[0]);
    ZipperMergeAndAdd(v0[3], v0[2], &v1[3], &v1[2]);
  }

  static HH_INLINE uint64_t Rotate64By32(const uint64_t x) {
    return (x >> 32) | (x << 32);
  }

  template <typename LanesOrPointer>
  static HH_INLINE void Rotate64By32(const LanesOrPointer& v,
                                     Lanes* HH_RESTRICT rotated) {
    for (int i = 0; i < kNumLanes; ++i) {
      (*rotated)[i] = Rotate64By32(v[i]);
    }
  }

  static HH_INLINE void Rotate32By(uint32_t* halves, const uint64_t count) {
    for (int i = 0; i < 2 * kNumLanes; ++i) {
      const uint32_t x = halves[i];
      halves[i] = (x << count) | (x >> (32 - count));
    }
  }

  static HH_INLINE void Permute(const Lanes& v, Lanes* HH_RESTRICT permuted) {
    (*permuted)[0] = Rotate64By32(v[2]);
    (*permuted)[1] = Rotate64By32(v[3]);
    (*permuted)[2] = Rotate64By32(v[0]);
    (*permuted)[3] = Rotate64By32(v[1]);
  }

  HH_INLINE void PermuteAndUpdate() {
    Lanes permuted;
    Permute(v0, &permuted);
    Update(permuted);
  }

  // Computes a << kBits for 128-bit a = (a1, a0).
  // Bit shifts are only possible on independent 64-bit lanes. We therefore
  // insert the upper bits of a0 that were lost into a1. This is slightly
  // shorter than Lemire's (a << 1) | (((a >> 8) << 1) << 8) approach.
  template <int kBits>
  static HH_INLINE void Shift128Left(uint64_t* HH_RESTRICT a1,
                                     uint64_t* HH_RESTRICT a0) {
    const uint64_t shifted1 = (*a1) << kBits;
    const uint64_t top_bits = (*a0) >> (64 - kBits);
    *a0 <<= kBits;
    *a1 = shifted1 | top_bits;
  }

  // Modular reduction by the irreducible polynomial (x^128 + x^2 + x).
  // Input: a 256-bit number a3210.
  static HH_INLINE void ModularReduction(const uint64_t a3_unmasked,
                                         const uint64_t a2, const uint64_t a1,
                                         const uint64_t a0,
                                         uint64_t* HH_RESTRICT m1,
                                         uint64_t* HH_RESTRICT m0) {
    // The upper two bits must be clear, otherwise a3 << 2 would lose bits,
    // in which case we're no longer computing a reduction.
    const uint64_t a3 = a3_unmasked & 0x3FFFFFFFFFFFFFFFull;
    // See Lemire, https://arxiv.org/pdf/1503.03465v8.pdf.
    uint64_t a3_shl1 = a3;
    uint64_t a2_shl1 = a2;
    uint64_t a3_shl2 = a3;
    uint64_t a2_shl2 = a2;
    Shift128Left<1>(&a3_shl1, &a2_shl1);
    Shift128Left<2>(&a3_shl2, &a2_shl2);
    *m1 = a1 ^ a3_shl1 ^ a3_shl2;
    *m0 = a0 ^ a2_shl1 ^ a2_shl2;
  }

  Lanes v0;
  Lanes v1;
  Lanes mul0;
  Lanes mul1;
};

}  // namespace HH_TARGET_NAME
}  // namespace highwayhash

#endif  // HIGHWAYHASH_HH_PORTABLE_H_
