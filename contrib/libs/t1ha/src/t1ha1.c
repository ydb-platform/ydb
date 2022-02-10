/*
 *  Copyright (c) 2016-2020 Positive Technologies, https://www.ptsecurity.com,
 *  Fast Positive Hash.
 *
 *  Portions Copyright (c) 2010-2020 Leonid Yuriev <leo@yuriev.ru>,
 *  The 1Hippeus project (t1h).
 *
 *  This software is provided 'as-is', without any express or implied
 *  warranty. In no event will the authors be held liable for any damages
 *  arising from the use of this software.
 *
 *  Permission is granted to anyone to use this software for any purpose,
 *  including commercial applications, and to alter it and redistribute it
 *  freely, subject to the following restrictions:
 *
 *  1. The origin of this software must not be misrepresented; you must not
 *     claim that you wrote the original software. If you use this software
 *     in a product, an acknowledgement in the product documentation would be
 *     appreciated but is not required.
 *  2. Altered source versions must be plainly marked as such, and must not be
 *     misrepresented as being the original software.
 *  3. This notice may not be removed or altered from any source distribution.
 */

/*
 * t1ha = { Fast Positive Hash, aka "Позитивный Хэш" }
 * by [Positive Technologies](https://www.ptsecurity.ru)
 *
 * Briefly, it is a 64-bit Hash Function:
 *  1. Created for 64-bit little-endian platforms, in predominantly for x86_64,
 *     but portable and without penalties it can run on any 64-bit CPU.
 *  2. In most cases up to 15% faster than City64, xxHash, mum-hash, metro-hash
 *     and all others portable hash-functions (which do not use specific
 *     hardware tricks).
 *  3. Not suitable for cryptography.
 *
 * The Future will (be) Positive. Всё будет хорошо.
 *
 * ACKNOWLEDGEMENT:
 * The t1ha was originally developed by Leonid Yuriev (Леонид Юрьев)
 * for The 1Hippeus project - zerocopy messaging in the spirit of Sparta!
 */

#ifndef T1HA1_DISABLED
#include "t1ha_bits.h"
#include "t1ha_selfcheck.h"

/* xor-mul-xor mixer */
static __inline uint64_t mix64(uint64_t v, uint64_t p) {
  v *= p;
  return v ^ rot64(v, 41);
}

static __inline uint64_t final_weak_avalanche(uint64_t a, uint64_t b) {
  /* LY: for performance reason on a some not high-end CPUs
   * I replaced the second mux64() operation by mix64().
   * Unfortunately this approach fails the "strict avalanche criteria",
   * see test results at https://github.com/demerphq/smhasher. */
  return mux64(rot64(a + b, 17), prime_4) + mix64(a ^ b, prime_0);
}

/* TODO: C++ template in the next version */
#define T1HA1_BODY(ENDIANNES, ALIGNESS)                                        \
  const uint64_t *v = (const uint64_t *)data;                                  \
  if (unlikely(len > 32)) {                                                    \
    uint64_t c = rot64(len, 17) + seed;                                        \
    uint64_t d = len ^ rot64(seed, 17);                                        \
    const uint64_t *detent =                                                   \
        (const uint64_t *)((const uint8_t *)data + len - 31);                  \
    do {                                                                       \
      const uint64_t w0 = fetch64_##ENDIANNES##_##ALIGNESS(v + 0);             \
      const uint64_t w1 = fetch64_##ENDIANNES##_##ALIGNESS(v + 1);             \
      const uint64_t w2 = fetch64_##ENDIANNES##_##ALIGNESS(v + 2);             \
      const uint64_t w3 = fetch64_##ENDIANNES##_##ALIGNESS(v + 3);             \
      v += 4;                                                                  \
      prefetch(v);                                                             \
                                                                               \
      const uint64_t d02 = w0 ^ rot64(w2 + d, 17);                             \
      const uint64_t c13 = w1 ^ rot64(w3 + c, 17);                             \
      d -= b ^ rot64(w1, 31);                                                  \
      c += a ^ rot64(w0, 41);                                                  \
      b ^= prime_0 * (c13 + w2);                                               \
      a ^= prime_1 * (d02 + w3);                                               \
    } while (likely(v < detent));                                              \
                                                                               \
    a ^= prime_6 * (rot64(c, 17) + d);                                         \
    b ^= prime_5 * (c + rot64(d, 17));                                         \
    len &= 31;                                                                 \
  }                                                                            \
                                                                               \
  switch (len) {                                                               \
  default:                                                                     \
    b += mux64(fetch64_##ENDIANNES##_##ALIGNESS(v++), prime_4);                \
  /* fall through */                                                           \
  case 24:                                                                     \
  case 23:                                                                     \
  case 22:                                                                     \
  case 21:                                                                     \
  case 20:                                                                     \
  case 19:                                                                     \
  case 18:                                                                     \
  case 17:                                                                     \
    a += mux64(fetch64_##ENDIANNES##_##ALIGNESS(v++), prime_3);                \
  /* fall through */                                                           \
  case 16:                                                                     \
  case 15:                                                                     \
  case 14:                                                                     \
  case 13:                                                                     \
  case 12:                                                                     \
  case 11:                                                                     \
  case 10:                                                                     \
  case 9:                                                                      \
    b += mux64(fetch64_##ENDIANNES##_##ALIGNESS(v++), prime_2);                \
  /* fall through */                                                           \
  case 8:                                                                      \
  case 7:                                                                      \
  case 6:                                                                      \
  case 5:                                                                      \
  case 4:                                                                      \
  case 3:                                                                      \
  case 2:                                                                      \
  case 1:                                                                      \
    a += mux64(tail64_##ENDIANNES##_##ALIGNESS(v, len), prime_1);              \
  /* fall through */                                                           \
  case 0:                                                                      \
    return final_weak_avalanche(a, b);                                         \
  }

uint64_t t1ha1_le(const void *data, size_t len, uint64_t seed) {
  uint64_t a = seed;
  uint64_t b = len;

#if T1HA_SYS_UNALIGNED_ACCESS == T1HA_UNALIGNED_ACCESS__EFFICIENT
  T1HA1_BODY(le, unaligned);
#else
  const bool misaligned = (((uintptr_t)data) & (ALIGNMENT_64 - 1)) != 0;
  if (misaligned) {
    T1HA1_BODY(le, unaligned);
  } else {
    T1HA1_BODY(le, aligned);
  }
#endif
}

uint64_t t1ha1_be(const void *data, size_t len, uint64_t seed) {
  uint64_t a = seed;
  uint64_t b = len;

#if T1HA_SYS_UNALIGNED_ACCESS == T1HA_UNALIGNED_ACCESS__EFFICIENT
  T1HA1_BODY(be, unaligned);
#else
  const bool misaligned = (((uintptr_t)data) & (ALIGNMENT_64 - 1)) != 0;
  if (misaligned) {
    T1HA1_BODY(be, unaligned);
  } else {
    T1HA1_BODY(be, aligned);
  }
#endif
}

#endif /* T1HA1_DISABLED */
