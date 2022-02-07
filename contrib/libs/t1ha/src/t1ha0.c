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

#ifndef T1HA0_DISABLED
#include "t1ha_bits.h"
#include "t1ha_selfcheck.h"

static __maybe_unused __always_inline uint32_t tail32_le_aligned(const void *v,
                                                                 size_t tail) {
  const uint8_t *const p = (const uint8_t *)v;
#if T1HA_USE_FAST_ONESHOT_READ && !defined(__SANITIZE_ADDRESS__)
  /* We can perform a 'oneshot' read, which is little bit faster. */
  const unsigned shift = ((4 - tail) & 3) << 3;
  return fetch32_le_aligned(p) & ((~UINT32_C(0)) >> shift);
#else
  uint32_t r = 0;
  switch (tail & 3) {
  default:
    unreachable();
/* fall through */
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  /* For most CPUs this code is better when not needed
   * copying for alignment or byte reordering. */
  case 0:
    return fetch32_le_aligned(p);
  case 3:
    r = (uint32_t)p[2] << 16;
  /* fall through */
  case 2:
    return r + fetch16_le_aligned(p);
  case 1:
    return p[0];
#else
  case 0:
    r += p[3];
    r <<= 8;
  /* fall through */
  case 3:
    r += p[2];
    r <<= 8;
  /* fall through */
  case 2:
    r += p[1];
    r <<= 8;
  /* fall through */
  case 1:
    return r + p[0];
#endif
  }
#endif /* T1HA_USE_FAST_ONESHOT_READ */
}

static __maybe_unused __always_inline uint32_t
tail32_le_unaligned(const void *v, size_t tail) {
  const uint8_t *p = (const uint8_t *)v;
#ifdef can_read_underside
  /* On some systems (e.g. x86) we can perform a 'oneshot' read, which
   * is little bit faster. Thanks Marcin Żukowski <marcin.zukowski@gmail.com>
   * for the reminder. */
  const unsigned offset = (4 - tail) & 3;
  const unsigned shift = offset << 3;
  if (likely(can_read_underside(p, 4))) {
    p -= offset;
    return fetch32_le_unaligned(p) >> shift;
  }
  return fetch32_le_unaligned(p) & ((~UINT32_C(0)) >> shift);
#else
  uint32_t r = 0;
  switch (tail & 3) {
  default:
    unreachable();
/* fall through */
#if T1HA_SYS_UNALIGNED_ACCESS == T1HA_UNALIGNED_ACCESS__EFFICIENT &&           \
    __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  /* For most CPUs this code is better when not needed
   * copying for alignment or byte reordering. */
  case 0:
    return fetch32_le_unaligned(p);
  case 3:
    r = (uint32_t)p[2] << 16;
  /* fall through */
  case 2:
    return r + fetch16_le_unaligned(p);
  case 1:
    return p[0];
#else
  /* For most CPUs this code is better than a
   * copying for alignment and/or byte reordering. */
  case 0:
    r += p[3];
    r <<= 8;
  /* fall through */
  case 3:
    r += p[2];
    r <<= 8;
  /* fall through */
  case 2:
    r += p[1];
    r <<= 8;
  /* fall through */
  case 1:
    return r + p[0];
#endif
  }
#endif /* can_read_underside */
}

static __maybe_unused __always_inline uint32_t tail32_be_aligned(const void *v,
                                                                 size_t tail) {
  const uint8_t *const p = (const uint8_t *)v;
#if T1HA_USE_FAST_ONESHOT_READ && !defined(__SANITIZE_ADDRESS__)
  /* We can perform a 'oneshot' read, which is little bit faster. */
  const unsigned shift = ((4 - tail) & 3) << 3;
  return fetch32_be_aligned(p) >> shift;
#else
  switch (tail & 3) {
  default:
    unreachable();
/* fall through */
#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
  /* For most CPUs this code is better when not needed
   * copying for alignment or byte reordering. */
  case 1:
    return p[0];
  case 2:
    return fetch16_be_aligned(p);
  case 3:
    return fetch16_be_aligned(p) << 8 | p[2];
  case 0:
    return fetch32_be_aligned(p);
#else
  case 1:
    return p[0];
  case 2:
    return p[1] | (uint32_t)p[0] << 8;
  case 3:
    return p[2] | (uint32_t)p[1] << 8 | (uint32_t)p[0] << 16;
  case 0:
    return p[3] | (uint32_t)p[2] << 8 | (uint32_t)p[1] << 16 |
           (uint32_t)p[0] << 24;
#endif
  }
#endif /* T1HA_USE_FAST_ONESHOT_READ */
}

static __maybe_unused __always_inline uint32_t
tail32_be_unaligned(const void *v, size_t tail) {
  const uint8_t *p = (const uint8_t *)v;
#ifdef can_read_underside
  /* On some systems we can perform a 'oneshot' read, which is little bit
   * faster. Thanks Marcin Żukowski <marcin.zukowski@gmail.com> for the
   * reminder. */
  const unsigned offset = (4 - tail) & 3;
  const unsigned shift = offset << 3;
  if (likely(can_read_underside(p, 4))) {
    p -= offset;
    return fetch32_be_unaligned(p) & ((~UINT32_C(0)) >> shift);
  }
  return fetch32_be_unaligned(p) >> shift;
#else
  switch (tail & 3) {
  default:
    unreachable();
/* fall through */
#if T1HA_SYS_UNALIGNED_ACCESS == T1HA_UNALIGNED_ACCESS__EFFICIENT &&           \
    __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
  /* For most CPUs this code is better when not needed
   * copying for alignment or byte reordering. */
  case 1:
    return p[0];
  case 2:
    return fetch16_be_unaligned(p);
  case 3:
    return fetch16_be_unaligned(p) << 8 | p[2];
  case 0:
    return fetch32_be_unaligned(p);
#else
  /* For most CPUs this code is better than a
   * copying for alignment and/or byte reordering. */
  case 1:
    return p[0];
  case 2:
    return p[1] | (uint32_t)p[0] << 8;
  case 3:
    return p[2] | (uint32_t)p[1] << 8 | (uint32_t)p[0] << 16;
  case 0:
    return p[3] | (uint32_t)p[2] << 8 | (uint32_t)p[1] << 16 |
           (uint32_t)p[0] << 24;
#endif
  }
#endif /* can_read_underside */
}

/***************************************************************************/

#ifndef rot32
static __maybe_unused __always_inline uint32_t rot32(uint32_t v, unsigned s) {
  return (v >> s) | (v << (32 - s));
}
#endif /* rot32 */

static __always_inline void mixup32(uint32_t *a, uint32_t *b, uint32_t v,
                                    uint32_t prime) {
  uint64_t l = mul_32x32_64(*b + v, prime);
  *a ^= (uint32_t)l;
  *b += (uint32_t)(l >> 32);
}

static __always_inline uint64_t final32(uint32_t a, uint32_t b) {
  uint64_t l = (b ^ rot32(a, 13)) | (uint64_t)a << 32;
  l *= prime_0;
  l ^= l >> 41;
  l *= prime_4;
  l ^= l >> 47;
  l *= prime_6;
  return l;
}

/* 32-bit 'magic' primes */
static const uint32_t prime32_0 = UINT32_C(0x92D78269);
static const uint32_t prime32_1 = UINT32_C(0xCA9B4735);
static const uint32_t prime32_2 = UINT32_C(0xA4ABA1C3);
static const uint32_t prime32_3 = UINT32_C(0xF6499843);
static const uint32_t prime32_4 = UINT32_C(0x86F0FD61);
static const uint32_t prime32_5 = UINT32_C(0xCA2DA6FB);
static const uint32_t prime32_6 = UINT32_C(0xC4BB3575);

/* TODO: C++ template in the next version */
#define T1HA0_BODY(ENDIANNES, ALIGNESS)                                        \
  const uint32_t *v = (const uint32_t *)data;                                  \
  if (unlikely(len > 16)) {                                                    \
    uint32_t c = ~a;                                                           \
    uint32_t d = rot32(b, 5);                                                  \
    const uint32_t *detent =                                                   \
        (const uint32_t *)((const uint8_t *)data + len - 15);                  \
    do {                                                                       \
      const uint32_t w0 = fetch32_##ENDIANNES##_##ALIGNESS(v + 0);             \
      const uint32_t w1 = fetch32_##ENDIANNES##_##ALIGNESS(v + 1);             \
      const uint32_t w2 = fetch32_##ENDIANNES##_##ALIGNESS(v + 2);             \
      const uint32_t w3 = fetch32_##ENDIANNES##_##ALIGNESS(v + 3);             \
      v += 4;                                                                  \
      prefetch(v);                                                             \
                                                                               \
      const uint32_t d13 = w1 + rot32(w3 + d, 17);                             \
      const uint32_t c02 = w0 ^ rot32(w2 + c, 11);                             \
      d ^= rot32(a + w0, 3);                                                   \
      c ^= rot32(b + w1, 7);                                                   \
      b = prime32_1 * (c02 + w3);                                              \
      a = prime32_0 * (d13 ^ w2);                                              \
    } while (likely(v < detent));                                              \
                                                                               \
    c += a;                                                                    \
    d += b;                                                                    \
    a ^= prime32_6 * (rot32(c, 16) + d);                                       \
    b ^= prime32_5 * (c + rot32(d, 16));                                       \
                                                                               \
    len &= 15;                                                                 \
  }                                                                            \
                                                                               \
  switch (len) {                                                               \
  default:                                                                     \
    mixup32(&a, &b, fetch32_##ENDIANNES##_##ALIGNESS(v++), prime32_4);         \
  /* fall through */                                                           \
  case 12:                                                                     \
  case 11:                                                                     \
  case 10:                                                                     \
  case 9:                                                                      \
    mixup32(&b, &a, fetch32_##ENDIANNES##_##ALIGNESS(v++), prime32_3);         \
  /* fall through */                                                           \
  case 8:                                                                      \
  case 7:                                                                      \
  case 6:                                                                      \
  case 5:                                                                      \
    mixup32(&a, &b, fetch32_##ENDIANNES##_##ALIGNESS(v++), prime32_2);         \
  /* fall through */                                                           \
  case 4:                                                                      \
  case 3:                                                                      \
  case 2:                                                                      \
  case 1:                                                                      \
    mixup32(&b, &a, tail32_##ENDIANNES##_##ALIGNESS(v, len), prime32_1);       \
  /* fall through */                                                           \
  case 0:                                                                      \
    return final32(a, b);                                                      \
  }

uint64_t t1ha0_32le(const void *data, size_t len, uint64_t seed) {
  uint32_t a = rot32((uint32_t)len, 17) + (uint32_t)seed;
  uint32_t b = (uint32_t)len ^ (uint32_t)(seed >> 32);

#if T1HA_SYS_UNALIGNED_ACCESS == T1HA_UNALIGNED_ACCESS__EFFICIENT
  T1HA0_BODY(le, unaligned);
#else
  const bool misaligned = (((uintptr_t)data) & (ALIGNMENT_32 - 1)) != 0;
  if (misaligned) {
    T1HA0_BODY(le, unaligned);
  } else {
    T1HA0_BODY(le, aligned);
  }
#endif
}

uint64_t t1ha0_32be(const void *data, size_t len, uint64_t seed) {
  uint32_t a = rot32((uint32_t)len, 17) + (uint32_t)seed;
  uint32_t b = (uint32_t)len ^ (uint32_t)(seed >> 32);

#if T1HA_SYS_UNALIGNED_ACCESS == T1HA_UNALIGNED_ACCESS__EFFICIENT
  T1HA0_BODY(be, unaligned);
#else
  const bool misaligned = (((uintptr_t)data) & (ALIGNMENT_32 - 1)) != 0;
  if (misaligned) {
    T1HA0_BODY(be, unaligned);
  } else {
    T1HA0_BODY(be, aligned);
  }
#endif
}

/***************************************************************************/

#if T1HA0_AESNI_AVAILABLE && defined(__ia32__)
__cold uint64_t t1ha_ia32cpu_features(void) {
  uint32_t features = 0;
  uint32_t extended = 0;
#if defined(__GNUC__) || defined(__clang__)
  uint32_t eax, ebx, ecx, edx;
  const unsigned cpuid_max = __get_cpuid_max(0, NULL);
  if (cpuid_max >= 1) {
    __cpuid_count(1, 0, eax, ebx, features, edx);
    if (cpuid_max >= 7)
      __cpuid_count(7, 0, eax, extended, ecx, edx);
  }
#elif defined(_MSC_VER)
  int info[4];
  __cpuid(info, 0);
  const unsigned cpuid_max = info[0];
  if (cpuid_max >= 1) {
    __cpuidex(info, 1, 0);
    features = info[2];
    if (cpuid_max >= 7) {
      __cpuidex(info, 7, 0);
      extended = info[1];
    }
  }
#endif
  return features | (uint64_t)extended << 32;
}
#endif /* T1HA0_AESNI_AVAILABLE && __ia32__ */

#if T1HA0_RUNTIME_SELECT

__cold t1ha0_function_t t1ha0_resolve(void) {

#if T1HA0_AESNI_AVAILABLE && defined(__ia32__)
  uint64_t features = t1ha_ia32cpu_features();
  if (t1ha_ia32_AESNI_avail(features)) {
    if (t1ha_ia32_AVX_avail(features))
      return t1ha_ia32_AVX2_avail(features) ? t1ha0_ia32aes_avx2
                                            : t1ha0_ia32aes_avx;
    return t1ha0_ia32aes_noavx;
  }
#endif /* T1HA0_AESNI_AVAILABLE && __ia32__ */

#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
#if (UINTPTR_MAX > 0xffffFFFFul || ULONG_MAX > 0xffffFFFFul) &&                \
    (!defined(T1HA1_DISABLED) || !defined(T1HA2_DISABLED))
#ifndef T1HA1_DISABLED
  return t1ha1_be;
#else
  return t1ha2_atonce;
#endif /* T1HA1_DISABLED */
#else
  return t1ha0_32be;
#endif
#else /* __BYTE_ORDER__ != __ORDER_BIG_ENDIAN__ */
#if (UINTPTR_MAX > 0xffffFFFFul || ULONG_MAX > 0xffffFFFFul) &&                \
    (!defined(T1HA1_DISABLED) || !defined(T1HA2_DISABLED))
#ifndef T1HA1_DISABLED
  return t1ha1_le;
#else
  return t1ha2_atonce;
#endif /* T1HA1_DISABLED */
#else
  return t1ha0_32le;
#endif
#endif /* __BYTE_ORDER__ */
}

#if T1HA_USE_INDIRECT_FUNCTIONS
/* Use IFUNC (GNU ELF indirect functions) to choice implementation at runtime.
 * For more info please see
 * https://en.wikipedia.org/wiki/Executable_and_Linkable_Format
 * and https://sourceware.org/glibc/wiki/GNU_IFUNC */
#if __has_attribute(__ifunc__)
uint64_t t1ha0(const void *data, size_t len, uint64_t seed)
    __attribute__((__ifunc__("t1ha0_resolve")));
#else
__asm("\t.globl\tt1ha0\n\t.type\tt1ha0, "
      "%gnu_indirect_function\n\t.set\tt1ha0,t1ha0_resolve");
#endif /* __has_attribute(__ifunc__) */

#elif __GNUC_PREREQ(4, 0) || __has_attribute(__constructor__)

uint64_t (*t1ha0_funcptr)(const void *, size_t, uint64_t);

static __cold void __attribute__((__constructor__)) t1ha0_init(void) {
  t1ha0_funcptr = t1ha0_resolve();
}

#else /* T1HA_USE_INDIRECT_FUNCTIONS */

static __cold uint64_t t1ha0_proxy(const void *data, size_t len,
                                   uint64_t seed) {
  t1ha0_funcptr = t1ha0_resolve();
  return t1ha0_funcptr(data, len, seed);
}

uint64_t (*t1ha0_funcptr)(const void *, size_t, uint64_t) = t1ha0_proxy;

#endif /* !T1HA_USE_INDIRECT_FUNCTIONS */
#endif /* T1HA0_RUNTIME_SELECT */

#endif /* T1HA0_DISABLED */
