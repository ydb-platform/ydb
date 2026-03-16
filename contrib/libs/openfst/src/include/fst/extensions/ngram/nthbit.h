// Copyright 2005-2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the 'License');
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an 'AS IS' BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// See www.openfst.org for extensive documentation on this weighted
// finite-state transducer library.

#ifndef FST_EXTENSIONS_NGRAM_NTHBIT_H_
#define FST_EXTENSIONS_NGRAM_NTHBIT_H_

#include <cstdint>

#if defined(__aarch64__)
#include <arm_neon.h>
#endif

#include <fst/log.h>

#if defined(__BMI2__)  // Intel Bit Manipulation Instruction Set 2
// PDEP requires BMI2; this is present starting with Haswell.

#include <immintrin.h>

namespace fst {
// Returns the position (0-63) of the r-th 1 bit in v.
// 0 <= r < CountOnes(v) <= 64. Therefore, v must not be 0.
inline uint32_t nth_bit(uint64_t v, uint32_t r) {
  DCHECK_NE(v, 0);
  DCHECK_LE(0, r);
  DCHECK_LT(r, __builtin_popcountll(v));

  // PDEP example from https://stackoverflow.com/a/27453505
  // __builtin_ctzll is UB for 0, but the conditions above ensure that can't
  // happen.
  return __builtin_ctzll(_pdep_u64(uint64_t{1} << r, v));
}
}  // namespace fst

#elif SIZE_MAX == UINT32_MAX
// Detect 32-bit architectures via size_t.

namespace fst {
// Returns the position (0-63) of the r-th 1 bit in v.
// 0 <= r < CountOnes(v) <= 64. Therefore, v must not be 0.
uint32_t nth_bit(uint64_t v, uint32_t r);
}  // namespace fst

#elif SIZE_MAX == UINT64_MAX
// Default 64-bit version, used by ARM64 and Intel < Haswell.

namespace fst {
namespace internal {
extern const uint64_t kPrefixSumOverflow[64];
extern const uint8_t kSelectInByte[2048];
}  // namespace internal

// Returns the position (0-63) of the r-th 1 bit in v.
// 0 <= r < CountOnes(v) <= 64. Therefore, v must not be 0.
//
// This version is based on the paper "Broadword Implementation of
// Rank/Select Queries" by Sebastiano Vigna, p. 5, Algorithm 2, with
// improvements from "Optimized Succinct Data Structures for Massive Data"
// by Gog & Petri, 2014.
inline uint32_t nth_bit(const uint64_t v, const uint32_t r) {
  constexpr uint64_t kOnesStep8 = 0x0101010101010101;
  constexpr uint64_t kMSBsStep8 = 0x80 * kOnesStep8;

  DCHECK_NE(v, 0);
  DCHECK_LE(0, r);
  DCHECK_LT(r, __builtin_popcountll(v));

#if defined(__aarch64__)
  // Use the ARM64 CNT instruction to compute a byte-wise popcount.
  const uint64_t s =
      reinterpret_cast<uint64_t>(vcnt_u8(reinterpret_cast<uint8x8_t>(v)));
#else
  constexpr uint64_t kOnesStep4 = 0x1111111111111111;
  uint64_t s = v;
  s = s - ((s >> 1) & (0x5 * kOnesStep4));
  s = (s & (0x3 * kOnesStep4)) + ((s >> 2) & (0x3 * kOnesStep4));
  s = (s + (s >> 4)) & (0xF * kOnesStep8);
#endif
  // s now contains the byte-wise popcounts of v.

  // byte_sums contains partial sums of the byte-wise popcounts.
  // That is, byte i contains the popcounts of bytes <= i.
  uint64_t byte_sums = s * kOnesStep8;

  // kPrefixSumOverflow[r] == (0x7F - r) * kOnesStep8, so the high bit is
  // still set if byte_sums - r > 0, or byte_sums > r. The first one set
  // is in the byte with the sum larger than r (since r is 0-based),
  // so this is the byte we need.
  const uint64_t b = (byte_sums + internal::kPrefixSumOverflow[r]) & kMSBsStep8;
  // The first bit set is the high bit in the byte, so
  // num_trailing_zeros == 8 * byte_nr + 7 and the byte number is the
  // number of trailing zeros divided by 8.
  const int byte_nr = __builtin_ctzll(b) >> 3;
  const int shift = byte_nr << 3;
  // The top byte contains the whole-word popcount; we never need that.
  byte_sums <<= 8;
  // Paper uses reinterpret_cast<uint8_t *>; use shift/mask instead.
  const int rank_in_byte = r - (byte_sums >> shift) & 0xFF;
  return shift +
         internal::kSelectInByte[(rank_in_byte << 8) + ((v >> shift) & 0xFF)];
}
}  // namespace fst

#else

#error Unrecognized architecture size

#endif

#endif  // FST_EXTENSIONS_NGRAM_NTHBIT_H_
