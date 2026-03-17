/*********************************************************************
  Blosc - Blocked Shuffling and Compression Library

  Copyright (c) 2021  The Blosc Developers <blosc@blosc.org>
  https://blosc.org
  License: BSD 3-Clause (see LICENSE.txt)

  See LICENSE.txt for details about copyright and rights to use.
**********************************************************************/

/*********************************************************************
  The code in this file is heavily based on FastLZ, a lightning-fast
  lossless compression library.  See LICENSES/FASTLZ.txt for details.
**********************************************************************/


#include <stdio.h>
#include <stdbool.h>
#include "blosclz.h"
#include "fastcopy.h"
#include "blosc-common.h"


/*
 * Give hints to the compiler for branch prediction optimization.
 */
#if defined(__GNUC__) && (__GNUC__ > 2)
#define BLOSCLZ_LIKELY(c)    (__builtin_expect((c), 1))
#define BLOSCLZ_UNLIKELY(c)  (__builtin_expect((c), 0))
#else
#define BLOSCLZ_LIKELY(c)    (c)
#define BLOSCLZ_UNLIKELY(c)  (c)
#endif

/*
 * Use inlined functions for supported systems.
 */
#if defined(_MSC_VER) && !defined(__cplusplus)   /* Visual Studio */
#define inline __inline  /* Visual C is not C99, but supports some kind of inline */
#endif

#define MAX_COPY 32U
#define MAX_DISTANCE 8191
#define MAX_FARDISTANCE (65535 + MAX_DISTANCE - 1)

#ifdef BLOSC_STRICT_ALIGN
  #define BLOSCLZ_READU16(p) ((p)[0] | (p)[1]<<8)
  #define BLOSCLZ_READU32(p) ((p)[0] | (p)[1]<<8 | (p)[2]<<16 | (p)[3]<<24)
#else
  #define BLOSCLZ_READU16(p) *((const uint16_t*)(p))
  #define BLOSCLZ_READU32(p) *((const uint32_t*)(p))
#endif

#define HASH_LOG (14U)
#define HASH_LOG2 (12U)

// This is used in LZ4 and seems to work pretty well here too
#define HASH_FUNCTION(v, s, h) {      \
  (v) = ((s) * 2654435761U) >> (32U - (h)); \
}


#if defined(__AVX2__)
static uint8_t *get_run_32(uint8_t *ip, const uint8_t *ip_bound, const uint8_t *ref) {
    uint8_t x = ip[-1];

    while (ip < (ip_bound - (sizeof(__m256i)))) {
        __m256i value, value2, cmp;
        /* Broadcast the value for every byte in a 256-bit register */
        memset(&value, x, sizeof(__m256i));
        value2 = _mm256_loadu_si256((__m256i *)ref);
        cmp = _mm256_cmpeq_epi64(value, value2);
        if ((unsigned)_mm256_movemask_epi8(cmp) != 0xFFFFFFFF) {
            /* Return the byte that starts to differ */
            while (*ref++ == x) ip++;
            return ip;
        }
        else {
            ip += sizeof(__m256i);
            ref += sizeof(__m256i);
        }
    }
    /* Look into the remainder */
    while ((ip < ip_bound) && (*ref++ == x)) ip++;
    return ip;
}
#endif

#if defined(__SSE2__)
uint8_t *get_run_16(uint8_t *ip, const uint8_t *ip_bound, const uint8_t *ref) {
  uint8_t x = ip[-1];

  while (ip < (ip_bound - sizeof(__m128i))) {
    __m128i value, value2, cmp;
    /* Broadcast the value for every byte in a 128-bit register */
    memset(&value, x, sizeof(__m128i));
    value2 = _mm_loadu_si128((__m128i *)ref);
    cmp = _mm_cmpeq_epi32(value, value2);
    if (_mm_movemask_epi8(cmp) != 0xFFFF) {
      /* Return the byte that starts to differ */
      while (*ref++ == x) ip++;
      return ip;
    }
    else {
      ip += sizeof(__m128i);
      ref += sizeof(__m128i);
    }
  }
  /* Look into the remainder */
  while ((ip < ip_bound) && (*ref++ == x)) ip++;
  return ip;
}

#endif


static uint8_t *get_run(uint8_t *ip, const uint8_t *ip_bound, const uint8_t *ref) {
  uint8_t x = ip[-1];
  int64_t value, value2;
  /* Broadcast the value for every byte in a 64-bit register */
  memset(&value, x, 8);
  /* safe because the outer check against ip limit */
  while (ip < (ip_bound - sizeof(int64_t))) {
#if defined(BLOSC_STRICT_ALIGN)
    memcpy(&value2, ref, 8);
#else
    value2 = ((int64_t*)ref)[0];
#endif
    if (value != value2) {
      /* Return the byte that starts to differ */
      while (*ref++ == x) ip++;
      return ip;
    }
    else {
      ip += 8;
      ref += 8;
    }
  }
  /* Look into the remainder */
  while ((ip < ip_bound) && (*ref++ == x)) ip++;
  return ip;
}


/* Return the byte that starts to differ */
uint8_t *get_match(uint8_t *ip, const uint8_t *ip_bound, const uint8_t *ref) {
#if !defined(BLOSC_STRICT_ALIGN)
  while (ip < (ip_bound - sizeof(int64_t))) {
    if (*(int64_t*)ref != *(int64_t*)ip) {
      /* Return the byte that starts to differ */
      while (*ref++ == *ip++) {}
      return ip;
    }
    else {
      ip += sizeof(int64_t);
      ref += sizeof(int64_t);
    }
  }
#endif
  /* Look into the remainder */
  while ((ip < ip_bound) && (*ref++ == *ip++)) {}
  return ip;
}


#if defined(__SSE2__)
static uint8_t *get_match_16(uint8_t *ip, const uint8_t *ip_bound, const uint8_t *ref) {
  __m128i value, value2, cmp;

  while (ip < (ip_bound - sizeof(__m128i))) {
    value = _mm_loadu_si128((__m128i *) ip);
    value2 = _mm_loadu_si128((__m128i *) ref);
    cmp = _mm_cmpeq_epi32(value, value2);
    if (_mm_movemask_epi8(cmp) != 0xFFFF) {
      /* Return the byte that starts to differ */
      while (*ref++ == *ip++) {}
      return ip;
    }
    else {
      ip += sizeof(__m128i);
      ref += sizeof(__m128i);
    }
  }
  /* Look into the remainder */
  while ((ip < ip_bound) && (*ref++ == *ip++)) {}
  return ip;
}
#endif


#if defined(__AVX2__)
static uint8_t *get_match_32(uint8_t *ip, const uint8_t *ip_bound, const uint8_t *ref) {

  while (ip < (ip_bound - sizeof(__m256i))) {
    __m256i value, value2, cmp;
    value = _mm256_loadu_si256((__m256i *) ip);
    value2 = _mm256_loadu_si256((__m256i *)ref);
    cmp = _mm256_cmpeq_epi64(value, value2);
    if ((unsigned)_mm256_movemask_epi8(cmp) != 0xFFFFFFFF) {
      /* Return the byte that starts to differ */
      while (*ref++ == *ip++) {}
      return ip;
    }
    else {
      ip += sizeof(__m256i);
      ref += sizeof(__m256i);
    }
  }
  /* Look into the remainder */
  while ((ip < ip_bound) && (*ref++ == *ip++)) {}
  return ip;
}
#endif


static uint8_t* get_run_or_match(uint8_t* ip, uint8_t* ip_bound, const uint8_t* ref, bool run) {
  if (BLOSCLZ_UNLIKELY(run)) {
#if defined(__AVX2__)
    // Extensive experiments on AMD Ryzen3 say that regular get_run is faster
    // ip = get_run_32(ip, ip_bound, ref);
    ip = get_run(ip, ip_bound, ref);
#elif defined(__SSE2__)
    // Extensive experiments on AMD Ryzen3 say that regular get_run is faster
    // ip = get_run_16(ip, ip_bound, ref);
    ip = get_run(ip, ip_bound, ref);
#else
    ip = get_run(ip, ip_bound, ref);
#endif
  }
  else {
#if defined(__AVX2__)
    // Extensive experiments on AMD Ryzen3 say that regular get_match_16 is faster
    // ip = get_match_32(ip, ip_bound, ref);
    ip = get_match_16(ip, ip_bound, ref);
#elif defined(__SSE2__)
    ip = get_match_16(ip, ip_bound, ref);
#else
    ip = get_match(ip, ip_bound, ref);
#endif
  }

  return ip;
}


#define LITERAL(ip, op, op_limit, anchor, copy) {       \
  if (BLOSCLZ_UNLIKELY((op) + 2 > (op_limit)))          \
    goto out;                                           \
  *(op)++ = *(anchor)++;                                \
  (ip) = (anchor);                                      \
  (copy)++;                                             \
  if (BLOSCLZ_UNLIKELY((copy) == MAX_COPY)) {           \
    (copy) = 0;                                         \
    *(op)++ = MAX_COPY-1;                               \
  }                                                     \
}

#define LITERAL2(ip, anchor, copy) {                    \
  oc++; (anchor)++;                                     \
  (ip) = (anchor);                                      \
  (copy)++;                                             \
  if (BLOSCLZ_UNLIKELY((copy) == MAX_COPY)) {           \
    (copy) = 0;                                         \
    oc++;                                               \
  }                                                     \
}

#define MATCH_SHORT(op, op_limit, len, distance) {        \
  if (BLOSCLZ_UNLIKELY((op) + 2 > (op_limit)))            \
    goto out;                                             \
  *(op)++ = (uint8_t)(((len) << 5U) + ((distance) >> 8U));\
  *(op)++ = (uint8_t)(((distance) & 255U));               \
}

#define MATCH_LONG(op, op_limit, len, distance) {       \
  if (BLOSCLZ_UNLIKELY((op) + 1 > (op_limit)))          \
    goto out;                                           \
  *(op)++ = (uint8_t)((7U << 5U) + ((distance) >> 8U)); \
  for ((len) -= 7; (len) >= 255; (len) -= 255) {        \
    if (BLOSCLZ_UNLIKELY((op) + 1 > (op_limit)))        \
      goto out;                                         \
    *(op)++ = 255;                                      \
  }                                                     \
  if (BLOSCLZ_UNLIKELY((op) + 2 > (op_limit)))          \
    goto out;                                           \
  *(op)++ = (uint8_t)(len);                             \
  *(op)++ = (uint8_t)(((distance) & 255U));             \
}

#define MATCH_SHORT_FAR(op, op_limit, len, distance) {      \
  if (BLOSCLZ_UNLIKELY((op) + 4 > (op_limit)))              \
    goto out;                                               \
  *(op)++ = (uint8_t)(((len) << 5U) + 31);                  \
  *(op)++ = 255;                                            \
  *(op)++ = (uint8_t)((distance) >> 8U);                    \
  *(op)++ = (uint8_t)((distance) & 255U);                   \
}

#define MATCH_LONG_FAR(op, op_limit, len, distance) {       \
  if (BLOSCLZ_UNLIKELY((op) + 1 > (op_limit)))              \
    goto out;                                               \
  *(op)++ = (7U << 5U) + 31;                                \
  for ((len) -= 7; (len) >= 255; (len) -= 255) {            \
    if (BLOSCLZ_UNLIKELY((op) + 1 > (op_limit)))            \
      goto out;                                             \
    *(op)++ = 255;                                          \
  }                                                         \
  if (BLOSCLZ_UNLIKELY((op) + 4 > (op_limit)))              \
    goto out;                                               \
  *(op)++ = (uint8_t)(len);                                 \
  *(op)++ = 255;                                            \
  *(op)++ = (uint8_t)((distance) >> 8U);                    \
  *(op)++ = (uint8_t)((distance) & 255U);                   \
}


// Get a guess for the compressed size of a buffer
static double get_cratio(uint8_t* ibase, int maxlen, int minlen, int ipshift) {
  uint8_t* ip = ibase;
  int32_t oc = 0;
  const uint16_t hashlen = (1U << (uint8_t)HASH_LOG2);
  uint16_t htab[1U << (uint8_t)HASH_LOG2];
  uint32_t hval;
  uint32_t seq;
  uint8_t copy;
  // Make a tradeoff between testing too much and too little
  uint16_t limit = (maxlen > hashlen) ? hashlen : maxlen;
  uint8_t* ip_bound = ibase + limit - 1;
  uint8_t* ip_limit = ibase + limit - 12;

  // Initialize the hash table to distances of 0
  memset(htab, 0, hashlen * sizeof(uint16_t));

  /* we start with literal copy */
  copy = 4;
  oc += 5;

  /* main loop */
  while (BLOSCLZ_LIKELY(ip < ip_limit)) {
    const uint8_t* ref;
    unsigned distance;
    uint8_t* anchor = ip;    /* comparison starting-point */

    /* find potential match */
    seq = BLOSCLZ_READU32(ip);
    HASH_FUNCTION(hval, seq, HASH_LOG2)
    ref = ibase + htab[hval];

    /* calculate distance to the match */
    distance = (unsigned int)(anchor - ref);

    /* update hash table */
    htab[hval] = (uint16_t) (anchor - ibase);

    if (distance == 0 || (distance >= MAX_FARDISTANCE)) {
      LITERAL2(ip, anchor, copy)
      continue;
    }

    /* is this a match? check the first 4 bytes */
    if (BLOSCLZ_READU32(ref) == BLOSCLZ_READU32(ip)) {
      ref += 4;
    }
    else {
      /* no luck, copy as a literal */
      LITERAL2(ip, anchor, copy)
      continue;
    }

    /* last matched byte */
    ip = anchor + 4;

    /* distance is biased */
    distance--;

    /* get runs or matches; zero distance means a run */
    ip = get_run_or_match(ip, ip_bound, ref, !distance);

    ip -= ipshift;
    int len = (int)(ip - anchor);
    if (len < minlen) {
      LITERAL2(ip, anchor, copy)
      continue;
    }

    /* if we haven't copied anything, adjust the output counter */
    if (!copy)
      oc--;
    /* reset literal counter */
    copy = 0;

    /* encode the match */
    if (distance < MAX_DISTANCE) {
      if (len >= 7) {
        oc += ((len - 7) / 255) + 1;
      }
      oc += 2;
    }
    else {
      /* far away, but not yet in the another galaxy... */
      if (len >= 7) {
        oc += ((len - 7) / 255) + 1;
      }
      oc += 4;
    }

    /* update the hash at match boundary */
    seq = BLOSCLZ_READU32(ip);
    HASH_FUNCTION(hval, seq, HASH_LOG2)
    htab[hval] = (uint16_t)(ip++ - ibase);
    ip++;
    /* assuming literal copy */
    oc++;
  }

  double ic = (double)(ip - ibase);
  return ic / (double)oc;
}


int blosclz_compress(const int clevel, const void* input, int length,
                     void* output, int maxout, const int split_block) {
  uint8_t* ibase = (uint8_t*)input;

  // Experiments say that checking 1/4 of the buffer is enough to figure out approx cratio
  int maxlen = length / 4;
  // Start probing somewhere inside the buffer
  int shift = length - maxlen;
  // Actual entropy probing!
  double cratio = get_cratio(ibase + shift, maxlen, 3, 3);
  // discard probes with small compression ratios (too expensive)
  double cratio_[10] = {0, 2, 1.5, 1.2, 1.2, 1.2, 1.2, 1.15, 1.1, 1.0};
  if (cratio < cratio_[clevel]) {
      goto out;
  }

  /* When we go back in a match (shift), we obtain quite different compression properties.
   * It looks like 4 is more useful in combination with bitshuffle and small typesizes
   * Fallback to 4 because it provides more consistent results for large cratios.
   *
   * In this block we also check cratios for the beginning of the buffers and
   * eventually discard those that are small (take too long to decompress).
   * This process is called _entropy probing_.
   */
  unsigned ipshift = 4;
  // Compute optimal shift and minimum lengths for encoding
  // Use 4 by default, except for low entropy data, where we should do a best effort
  unsigned minlen = 4;
  // BloscLZ works better with splits mostly, so when data is not split, do a best effort
  // Why using cratio < 4 is based in experiments with low and high entropy
  if (!split_block || cratio < 4) {
    ipshift = 3;
    minlen = 3;
  }
  else {
    minlen = 4;
  }

  uint8_t hashlog_[10] = {0, HASH_LOG - 2, HASH_LOG - 1, HASH_LOG, HASH_LOG,
                          HASH_LOG, HASH_LOG, HASH_LOG, HASH_LOG, HASH_LOG};
  uint8_t hashlog = hashlog_[clevel];

  uint8_t* ip = ibase;
  uint8_t* ip_bound = ibase + length - 1;
  uint8_t* ip_limit = ibase + length - 12;
  uint8_t* op = (uint8_t*)output;
  const uint8_t* op_limit = op + maxout;
  uint32_t seq;
  uint8_t copy;
  uint32_t hval;

  /* input and output buffer cannot be less than 16 and 66 bytes or we can get into trouble */
  if (length < 16 || maxout < 66) {
    return 0;
  }

  // Initialize the hash table
  uint32_t htab[1U << (uint8_t)HASH_LOG];
  memset(htab, 0, (1U << hashlog) * sizeof(uint32_t));

  /* we start with literal copy */
  copy = 4;
  *op++ = MAX_COPY - 1;
  *op++ = *ip++;
  *op++ = *ip++;
  *op++ = *ip++;
  *op++ = *ip++;

  /* main loop */
  while (BLOSCLZ_LIKELY(ip < ip_limit)) {
    const uint8_t* ref;
    unsigned distance;
    uint8_t* anchor = ip;    /* comparison starting-point */

    /* find potential match */
    seq = BLOSCLZ_READU32(ip);
    HASH_FUNCTION(hval, seq, hashlog)
    ref = ibase + htab[hval];

    /* calculate distance to the match */
    distance = (unsigned int)(anchor - ref);

    /* update hash table */
    htab[hval] = (uint32_t) (anchor - ibase);

    if (distance == 0 || (distance >= MAX_FARDISTANCE)) {
      LITERAL(ip, op, op_limit, anchor, copy)
      continue;
    }

    /* is this a match? check the first 4 bytes */
    if (BLOSCLZ_UNLIKELY(BLOSCLZ_READU32(ref) == BLOSCLZ_READU32(ip))) {
      ref += 4;
    } else {
      /* no luck, copy as a literal */
      LITERAL(ip, op, op_limit, anchor, copy)
      continue;
    }

    /* last matched byte */
    ip = anchor + 4;

    /* distance is biased */
    distance--;

    /* get runs or matches; zero distance means a run */
    ip = get_run_or_match(ip, ip_bound, ref, !distance);

    /* length is biased, '1' means a match of 3 bytes */
    ip -= ipshift;

    unsigned len = (int)(ip - anchor);

    // Encoding short lengths is expensive during decompression
    if (len < minlen || (len <= 5 && distance >= MAX_DISTANCE)) {
      LITERAL(ip, op, op_limit, anchor, copy)
      continue;
    }

    /* if we have copied something, adjust the copy count */
    if (copy)
      /* copy is biased, '0' means 1 byte copy */
      *(op - copy - 1) = (uint8_t)(copy - 1);
    else
      /* back, to overwrite the copy count */
      op--;
    /* reset literal counter */
    copy = 0;

    /* encode the match */
    if (distance < MAX_DISTANCE) {
      if (len < 7) {
        MATCH_SHORT(op, op_limit, len, distance)
      } else {
        MATCH_LONG(op, op_limit, len, distance)
      }
    } else {
      /* far away, but not yet in the another galaxy... */
      distance -= MAX_DISTANCE;
      if (len < 7) {
        MATCH_SHORT_FAR(op, op_limit, len, distance)
      } else {
        MATCH_LONG_FAR(op, op_limit, len, distance)
      }
    }

    /* update the hash at match boundary */
    seq = BLOSCLZ_READU32(ip);
    HASH_FUNCTION(hval, seq, hashlog)
    htab[hval] = (uint32_t) (ip++ - ibase);
    if (clevel == 9) {
      // In some situations, including a second hash proves to be useful,
      // but not in others.  Activating here in max clevel only.
      seq >>= 8U;
      HASH_FUNCTION(hval, seq, hashlog)
      htab[hval] = (uint32_t) (ip++ - ibase);
    }
    else {
      ip++;
    }

    if (BLOSCLZ_UNLIKELY(op + 1 > op_limit))
      goto out;

    /* assuming literal copy */
    *op++ = MAX_COPY - 1;
  }

  /* left-over as literal copy */
  while (BLOSCLZ_UNLIKELY(ip <= ip_bound)) {
    if (BLOSCLZ_UNLIKELY(op + 2 > op_limit)) goto out;
    *op++ = *ip++;
    copy++;
    if (BLOSCLZ_UNLIKELY(copy == MAX_COPY)) {
      copy = 0;
      *op++ = MAX_COPY - 1;
    }
  }

  /* if we have copied something, adjust the copy length */
  if (copy)
    *(op - copy - 1) = (uint8_t)(copy - 1);
  else
    op--;

  /* marker for blosclz */
  *(uint8_t*)output |= (1U << 5U);

  return (int)(op - (uint8_t*)output);

  out:
  return 0;
}

// See https://habr.com/en/company/yandex/blog/457612/
#if defined(__AVX2__)

#if defined(_MSC_VER)
#define ALIGNED_(x) __declspec(align(x))
#else
#if defined(__GNUC__)
#define ALIGNED_(x) __attribute__ ((aligned(x)))
#endif
#endif
#define ALIGNED_TYPE_(t, x) t ALIGNED_(x)

static unsigned char* copy_match_16(unsigned char *op, const unsigned char *match, int32_t len)
{
  size_t offset = op - match;
  while (len >= 16) {

    static const ALIGNED_TYPE_(uint8_t, 16) masks[] =
      {
                0,  1,  2,  1,  4,  1,  4,  2,  8,  7,  6,  5,  4,  3,  2,  1, // offset = 0, not used as mask, but for shift
                0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, // offset = 1
                0,  1,  0,  1,  0,  1,  0,  1,  0,  1,  0,  1,  0,  1,  0,  1,
                0,  1,  2,  0,  1,  2,  0,  1,  2,  0,  1,  2,  0,  1,  2,  0,
                0,  1,  2,  3,  0,  1,  2,  3,  0,  1,  2,  3,  0,  1,  2,  3,
                0,  1,  2,  3,  4,  0,  1,  2,  3,  4,  0,  1,  2,  3,  4,  0,
                0,  1,  2,  3,  4,  5,  0,  1,  2,  3,  4,  5,  0,  1,  2,  3,
                0,  1,  2,  3,  4,  5,  6,  0,  1,  2,  3,  4,  5,  6,  0,  1,
                0,  1,  2,  3,  4,  5,  6,  7,  0,  1,  2,  3,  4,  5,  6,  7,
                0,  1,  2,  3,  4,  5,  6,  7,  8,  0,  1,  2,  3,  4,  5,  6,
                0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  0,  1,  2,  3,  4,  5,
                0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10,  0,  1,  2,  3,  4,
                0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11,  0,  1,  2,  3,
                0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12,  0,  1,  2,
                0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13,  0,  1,
                0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14,  0,
                0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14,  15, // offset = 16
      };

    _mm_storeu_si128((__m128i *)(op),
                     _mm_shuffle_epi8(_mm_loadu_si128((const __m128i *)(match)),
                                      _mm_load_si128((const __m128i *)(masks) + offset)));

    match += masks[offset];

    op += 16;
    len -= 16;
  }
  // Deal with remainders
  for (; len > 0; len--) {
    *op++ = *match++;
  }
  return op;
}
#endif

// LZ4 wildCopy which can reach excellent copy bandwidth (even if insecure)
static inline void wild_copy(uint8_t *out, const uint8_t* from, uint8_t* end) {
  uint8_t* d = out;
  const uint8_t* s = from;
  uint8_t* const e = end;

  do { memcpy(d,s,8); d+=8; s+=8; } while (d<e);
}

int blosclz_decompress(const void* input, int length, void* output, int maxout) {
  const uint8_t* ip = (const uint8_t*)input;
  const uint8_t* ip_limit = ip + length;
  uint8_t* op = (uint8_t*)output;
  uint32_t ctrl;
  uint8_t* op_limit = op + maxout;
  if (BLOSCLZ_UNLIKELY(length == 0)) {
    return 0;
  }
  ctrl = (*ip++) & 31U;

  while (1) {
    if (ctrl >= 32) {
      // match
      int32_t len = (int32_t)(ctrl >> 5U) - 1 ;
      int32_t ofs = (int32_t)(ctrl & 31U) << 8U;
      uint8_t code;
      const uint8_t* ref = op - ofs;

      if (len == 7 - 1) {
        do {
          if (BLOSCLZ_UNLIKELY(ip + 1 >= ip_limit)) {
            return 0;
          }
          code = *ip++;
          len += code;
        } while (code == 255);
      }
      else {
        if (BLOSCLZ_UNLIKELY(ip + 1 >= ip_limit)) {
          return 0;
        }
      }
      code = *ip++;
      len += 3;
      ref -= code;

      /* match from 16-bit distance */
      if (BLOSCLZ_UNLIKELY(code == 255)) {
        if (ofs == (31U << 8U)) {
          if (ip + 1 >= ip_limit) {
            return 0;
          }
          ofs = (*ip++) << 8U;
          ofs += *ip++;
          ref = op - ofs - MAX_DISTANCE;
        }
      }

      if (BLOSCLZ_UNLIKELY(op + len > op_limit)) {
        return 0;
      }

      if (BLOSCLZ_UNLIKELY(ref - 1 < (uint8_t*)output)) {
        return 0;
      }

      if (BLOSCLZ_UNLIKELY(ip >= ip_limit)) break;
      ctrl = *ip++;

      ref--;
      if (ref == op - 1) {
        /* optimized copy for a run */
        memset(op, *ref, len);
        op += len;
      }
      else if ((op - ref >= 8) && (op_limit - op >= len + 8)) {
        // copy with an overlap not larger than 8
        wild_copy(op, ref, op + len);
        op += len;
      }
      else {
        // general copy with any overlap
#if defined(__AVX2__)
        if (op - ref <= 16) {
          // This is not faster on a combination of compilers (clang, gcc, icc) or machines, but
          // it is not slower either.  Let's activate here for experimentation.
          op = copy_match_16(op, ref, len);
        }
        else {
#endif
          op = copy_match(op, ref, (unsigned) len);
#if defined(__AVX2__)
        }
#endif
      }
    }
    else {
      // literal
      ctrl++;
      if (BLOSCLZ_UNLIKELY(op + ctrl > op_limit)) {
        return 0;
      }
      if (BLOSCLZ_UNLIKELY(ip + ctrl > ip_limit)) {
        return 0;
      }

      memcpy(op, ip, ctrl); op += ctrl; ip += ctrl;
      // On GCC-6, fastcopy this is still faster than plain memcpy
      // However, using recent CLANG/LLVM 9.0, there is almost no difference
      // in performance.
      // And starting on CLANG/LLVM 10 and GCC 9, memcpy is generally faster.
      // op = fastcopy(op, ip, (unsigned) ctrl); ip += ctrl;

      if (BLOSCLZ_UNLIKELY(ip >= ip_limit)) break;
      ctrl = *ip++;
    }
  }

  return (int)(op - (uint8_t*)output);
}
