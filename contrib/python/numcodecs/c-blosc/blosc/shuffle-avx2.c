/*********************************************************************
  Blosc - Blocked Shuffling and Compression Library

  Author: Francesc Alted <francesc@blosc.org>

  See LICENSE.txt for details about copyright and rights to use.
**********************************************************************/

#include "shuffle-generic.h"
#include "shuffle-avx2.h"

/* Define dummy functions if AVX2 is not available for the compilation target and compiler. */
#if !defined(__AVX2__)
#include <stdlib.h>

void
blosc_internal_shuffle_avx2(const size_t bytesoftype, const size_t blocksize,
                            const uint8_t* const _src, uint8_t* const _dest) {
  abort();
}

void
blosc_internal_unshuffle_avx2(const size_t bytesoftype, const size_t blocksize,
                              const uint8_t* const _src, uint8_t* const _dest) {
  abort();
}

#else /* defined(__AVX2__) */

#include <immintrin.h>


/* The next is useful for debugging purposes */
#if 0
#include <stdio.h>
#include <string.h>

static void printymm(__m256i ymm0)
{
  uint8_t buf[32];

  ((__m256i *)buf)[0] = ymm0;
  printf("%x,%x,%x,%x,%x,%x,%x,%x,%x,%x,%x,%x,%x,%x,%x,%x\n",
          buf[0], buf[1], buf[2], buf[3],
          buf[4], buf[5], buf[6], buf[7],
          buf[8], buf[9], buf[10], buf[11],
          buf[12], buf[13], buf[14], buf[15],
          buf[16], buf[17], buf[18], buf[19],
          buf[20], buf[21], buf[22], buf[23],
          buf[24], buf[25], buf[26], buf[27],
          buf[28], buf[29], buf[30], buf[31]);
}
#endif

/* GCC doesn't include the split load/store intrinsics
   needed for the tiled shuffle, so define them here. */
#if defined(__GNUC__) && !defined(__clang__) && !defined(__ICC)
static inline __m256i
__attribute__((__always_inline__))
_mm256_loadu2_m128i(const __m128i* const hiaddr, const __m128i* const loaddr)
{
  return _mm256_inserti128_si256(
    _mm256_castsi128_si256(_mm_loadu_si128(loaddr)), _mm_loadu_si128(hiaddr), 1);
}

static inline void
__attribute__((__always_inline__))
_mm256_storeu2_m128i(__m128i* const hiaddr, __m128i* const loaddr, const __m256i a)
{
  _mm_storeu_si128(loaddr, _mm256_castsi256_si128(a));
  _mm_storeu_si128(hiaddr, _mm256_extracti128_si256(a, 1));
}
#endif  /* defined(__GNUC__) */

/* Routine optimized for shuffling a buffer for a type size of 2 bytes. */
static void
shuffle2_avx2(uint8_t* const dest, const uint8_t* const src,
  const size_t vectorizable_elements, const size_t total_elements)
{
  static const size_t bytesoftype = 2;
  size_t j;
  int k;
  __m256i ymm0[2], ymm1[2];

  /* Create the shuffle mask.
     NOTE: The XMM/YMM 'set' intrinsics require the arguments to be ordered from
     most to least significant (i.e., their order is reversed when compared to
     loading the mask from an array). */
  const __m256i shmask = _mm256_set_epi8(
    0x0f, 0x0d, 0x0b, 0x09, 0x07, 0x05, 0x03, 0x01,
    0x0e, 0x0c, 0x0a, 0x08, 0x06, 0x04, 0x02, 0x00,
    0x0f, 0x0d, 0x0b, 0x09, 0x07, 0x05, 0x03, 0x01,
    0x0e, 0x0c, 0x0a, 0x08, 0x06, 0x04, 0x02, 0x00);

  for (j = 0; j < vectorizable_elements; j += sizeof(__m256i)) {
    /* Fetch 32 elements (64 bytes) then transpose bytes, words and double words. */
    for (k = 0; k < 2; k++) {
      ymm0[k] = _mm256_loadu_si256((__m256i*)(src + (j * bytesoftype) + (k * sizeof(__m256i))));
      ymm1[k] = _mm256_shuffle_epi8(ymm0[k], shmask);
    }

    ymm0[0] = _mm256_permute4x64_epi64(ymm1[0], 0xd8);
    ymm0[1] = _mm256_permute4x64_epi64(ymm1[1], 0x8d);

    ymm1[0] = _mm256_blend_epi32(ymm0[0], ymm0[1], 0xf0);
    ymm0[1] = _mm256_blend_epi32(ymm0[0], ymm0[1], 0x0f);
    ymm1[1] = _mm256_permute4x64_epi64(ymm0[1], 0x4e);

    /* Store the result vectors */
    uint8_t* const dest_for_jth_element = dest + j;
    for (k = 0; k < 2; k++) {
      _mm256_storeu_si256((__m256i*)(dest_for_jth_element + (k * total_elements)), ymm1[k]);
    }
  }
}

/* Routine optimized for shuffling a buffer for a type size of 4 bytes. */
static void
shuffle4_avx2(uint8_t* const dest, const uint8_t* const src,
  const size_t vectorizable_elements, const size_t total_elements)
{
  static const size_t bytesoftype = 4;
  size_t i;
  int j;
  __m256i ymm0[4], ymm1[4];

  /* Create the shuffle mask.
     NOTE: The XMM/YMM 'set' intrinsics require the arguments to be ordered from
     most to least significant (i.e., their order is reversed when compared to
     loading the mask from an array). */
  const __m256i mask = _mm256_set_epi32(
    0x07, 0x03, 0x06, 0x02, 0x05, 0x01, 0x04, 0x00);

  for (i = 0; i < vectorizable_elements; i += sizeof(__m256i)) {
    /* Fetch 32 elements (128 bytes) then transpose bytes and words. */
    for (j = 0; j < 4; j++) {
      ymm0[j] = _mm256_loadu_si256((__m256i*)(src + (i * bytesoftype) + (j * sizeof(__m256i))));
      ymm1[j] = _mm256_shuffle_epi32(ymm0[j], 0xd8);
      ymm0[j] = _mm256_shuffle_epi32(ymm0[j], 0x8d);
      ymm0[j] = _mm256_unpacklo_epi8(ymm1[j], ymm0[j]);
      ymm1[j] = _mm256_shuffle_epi32(ymm0[j], 0x04e);
      ymm0[j] = _mm256_unpacklo_epi16(ymm0[j], ymm1[j]);
    }
    /* Transpose double words */
    for (j = 0; j < 2; j++) {
      ymm1[j*2] = _mm256_unpacklo_epi32(ymm0[j*2], ymm0[j*2+1]);
      ymm1[j*2+1] = _mm256_unpackhi_epi32(ymm0[j*2], ymm0[j*2+1]);
    }
    /* Transpose quad words */
    for (j = 0; j < 2; j++) {
      ymm0[j*2] = _mm256_unpacklo_epi64(ymm1[j], ymm1[j+2]);
      ymm0[j*2+1] = _mm256_unpackhi_epi64(ymm1[j], ymm1[j+2]);
    }
    for (j = 0; j < 4; j++) {
      ymm0[j] = _mm256_permutevar8x32_epi32(ymm0[j], mask);
    }
    /* Store the result vectors */
    uint8_t* const dest_for_ith_element = dest + i;
    for (j = 0; j < 4; j++) {
      _mm256_storeu_si256((__m256i*)(dest_for_ith_element + (j * total_elements)), ymm0[j]);
    }
  }
}

/* Routine optimized for shuffling a buffer for a type size of 8 bytes. */
static void
shuffle8_avx2(uint8_t* const dest, const uint8_t* const src,
  const size_t vectorizable_elements, const size_t total_elements)
{
  static const size_t bytesoftype = 8;
  size_t j;
  int k, l;
  __m256i ymm0[8], ymm1[8];

  for (j = 0; j < vectorizable_elements; j += sizeof(__m256i)) {
    /* Fetch 32 elements (256 bytes) then transpose bytes. */
    for (k = 0; k < 8; k++) {
      ymm0[k] = _mm256_loadu_si256((__m256i*)(src + (j * bytesoftype) + (k * sizeof(__m256i))));
      ymm1[k] = _mm256_shuffle_epi32(ymm0[k], 0x4e);
      ymm1[k] = _mm256_unpacklo_epi8(ymm0[k], ymm1[k]);
    }
    /* Transpose words */
    for (k = 0, l = 0; k < 4; k++, l +=2) {
      ymm0[k*2] = _mm256_unpacklo_epi16(ymm1[l], ymm1[l+1]);
      ymm0[k*2+1] = _mm256_unpackhi_epi16(ymm1[l], ymm1[l+1]);
    }
    /* Transpose double words */
    for (k = 0, l = 0; k < 4; k++, l++) {
      if (k == 2) l += 2;
      ymm1[k*2] = _mm256_unpacklo_epi32(ymm0[l], ymm0[l+2]);
      ymm1[k*2+1] = _mm256_unpackhi_epi32(ymm0[l], ymm0[l+2]);
    }
    /* Transpose quad words */
    for (k = 0; k < 4; k++) {
      ymm0[k*2] = _mm256_unpacklo_epi64(ymm1[k], ymm1[k+4]);
      ymm0[k*2+1] = _mm256_unpackhi_epi64(ymm1[k], ymm1[k+4]);
    }
    for(k = 0; k < 8; k++) {
      ymm1[k] = _mm256_permute4x64_epi64(ymm0[k], 0x72);
      ymm0[k] = _mm256_permute4x64_epi64(ymm0[k], 0xD8);
      ymm0[k] = _mm256_unpacklo_epi16(ymm0[k], ymm1[k]);
    }
    /* Store the result vectors */
    uint8_t* const dest_for_jth_element = dest + j;
    for (k = 0; k < 8; k++) {
      _mm256_storeu_si256((__m256i*)(dest_for_jth_element + (k * total_elements)), ymm0[k]);
    }
  }
}

/* Routine optimized for shuffling a buffer for a type size of 16 bytes. */
static void
shuffle16_avx2(uint8_t* const dest, const uint8_t* const src,
  const size_t vectorizable_elements, const size_t total_elements)
{
  static const size_t bytesoftype = 16;
  size_t j;
  int k, l;
  __m256i ymm0[16], ymm1[16];

  /* Create the shuffle mask.
     NOTE: The XMM/YMM 'set' intrinsics require the arguments to be ordered from
     most to least significant (i.e., their order is reversed when compared to
     loading the mask from an array). */
  const __m256i shmask = _mm256_set_epi8(
    0x0f, 0x07, 0x0e, 0x06, 0x0d, 0x05, 0x0c, 0x04,
    0x0b, 0x03, 0x0a, 0x02, 0x09, 0x01, 0x08, 0x00,
    0x0f, 0x07, 0x0e, 0x06, 0x0d, 0x05, 0x0c, 0x04,
    0x0b, 0x03, 0x0a, 0x02, 0x09, 0x01, 0x08, 0x00);

  for (j = 0; j < vectorizable_elements; j += sizeof(__m256i)) {
    /* Fetch 32 elements (512 bytes) into 16 YMM registers. */
    for (k = 0; k < 16; k++) {
      ymm0[k] = _mm256_loadu_si256((__m256i*)(src + (j * bytesoftype) + (k * sizeof(__m256i))));
    }
    /* Transpose bytes */
    for (k = 0, l = 0; k < 8; k++, l +=2) {
      ymm1[k*2] = _mm256_unpacklo_epi8(ymm0[l], ymm0[l+1]);
      ymm1[k*2+1] = _mm256_unpackhi_epi8(ymm0[l], ymm0[l+1]);
    }
    /* Transpose words */
    for (k = 0, l = -2; k < 8; k++, l++) {
      if ((k%2) == 0) l += 2;
      ymm0[k*2] = _mm256_unpacklo_epi16(ymm1[l], ymm1[l+2]);
      ymm0[k*2+1] = _mm256_unpackhi_epi16(ymm1[l], ymm1[l+2]);
    }
    /* Transpose double words */
    for (k = 0, l = -4; k < 8; k++, l++) {
      if ((k%4) == 0) l += 4;
      ymm1[k*2] = _mm256_unpacklo_epi32(ymm0[l], ymm0[l+4]);
      ymm1[k*2+1] = _mm256_unpackhi_epi32(ymm0[l], ymm0[l+4]);
    }
    /* Transpose quad words */
    for (k = 0; k < 8; k++) {
      ymm0[k*2] = _mm256_unpacklo_epi64(ymm1[k], ymm1[k+8]);
      ymm0[k*2+1] = _mm256_unpackhi_epi64(ymm1[k], ymm1[k+8]);
    }
    for (k = 0; k < 16; k++) {
      ymm0[k] = _mm256_permute4x64_epi64(ymm0[k], 0xd8);
      ymm0[k] = _mm256_shuffle_epi8(ymm0[k], shmask);
    }
    /* Store the result vectors */
    uint8_t* const dest_for_jth_element = dest + j;
    for (k = 0; k < 16; k++) {
      _mm256_storeu_si256((__m256i*)(dest_for_jth_element + (k * total_elements)), ymm0[k]);
    }
  }
}

/* Routine optimized for shuffling a buffer for a type size larger than 16 bytes. */
static void
shuffle16_tiled_avx2(uint8_t* const dest, const uint8_t* const src,
  const size_t vectorizable_elements, const size_t total_elements, const size_t bytesoftype)
{
  size_t j;
  int k, l;
  __m256i ymm0[16], ymm1[16];

  const lldiv_t vecs_per_el = lldiv(bytesoftype, sizeof(__m128i));

  /* Create the shuffle mask.
     NOTE: The XMM/YMM 'set' intrinsics require the arguments to be ordered from
     most to least significant (i.e., their order is reversed when compared to
     loading the mask from an array). */
  const __m256i shmask = _mm256_set_epi8(
    0x0f, 0x07, 0x0e, 0x06, 0x0d, 0x05, 0x0c, 0x04,
    0x0b, 0x03, 0x0a, 0x02, 0x09, 0x01, 0x08, 0x00,
    0x0f, 0x07, 0x0e, 0x06, 0x0d, 0x05, 0x0c, 0x04,
    0x0b, 0x03, 0x0a, 0x02, 0x09, 0x01, 0x08, 0x00);

  for (j = 0; j < vectorizable_elements; j += sizeof(__m256i)) {
    /* Advance the offset into the type by the vector size (in bytes), unless this is
    the initial iteration and the type size is not a multiple of the vector size.
    In that case, only advance by the number of bytes necessary so that the number
    of remaining bytes in the type will be a multiple of the vector size. */
    size_t offset_into_type;
    for (offset_into_type = 0; offset_into_type < bytesoftype;
      offset_into_type += (offset_into_type == 0 && vecs_per_el.rem > 0 ? vecs_per_el.rem : sizeof(__m128i))) {

      /* Fetch elements in groups of 512 bytes */
      const uint8_t* const src_with_offset = src + offset_into_type;
      for (k = 0; k < 16; k++) {
        ymm0[k] = _mm256_loadu2_m128i(
          (__m128i*)(src_with_offset + (j + (2 * k) + 1) * bytesoftype),
          (__m128i*)(src_with_offset + (j + (2 * k)) * bytesoftype));
      }
      /* Transpose bytes */
      for (k = 0, l = 0; k < 8; k++, l +=2) {
        ymm1[k*2] = _mm256_unpacklo_epi8(ymm0[l], ymm0[l+1]);
        ymm1[k*2+1] = _mm256_unpackhi_epi8(ymm0[l], ymm0[l+1]);
      }
      /* Transpose words */
      for (k = 0, l = -2; k < 8; k++, l++) {
        if ((k%2) == 0) l += 2;
        ymm0[k*2] = _mm256_unpacklo_epi16(ymm1[l], ymm1[l+2]);
        ymm0[k*2+1] = _mm256_unpackhi_epi16(ymm1[l], ymm1[l+2]);
      }
      /* Transpose double words */
      for (k = 0, l = -4; k < 8; k++, l++) {
        if ((k%4) == 0) l += 4;
        ymm1[k*2] = _mm256_unpacklo_epi32(ymm0[l], ymm0[l+4]);
        ymm1[k*2+1] = _mm256_unpackhi_epi32(ymm0[l], ymm0[l+4]);
      }
      /* Transpose quad words */
      for (k = 0; k < 8; k++) {
        ymm0[k*2] = _mm256_unpacklo_epi64(ymm1[k], ymm1[k+8]);
        ymm0[k*2+1] = _mm256_unpackhi_epi64(ymm1[k], ymm1[k+8]);
      }
      for (k = 0; k < 16; k++) {
        ymm0[k] = _mm256_permute4x64_epi64(ymm0[k], 0xd8);
        ymm0[k] = _mm256_shuffle_epi8(ymm0[k], shmask);
      }
      /* Store the result vectors */
      uint8_t* const dest_for_jth_element = dest + j;
      for (k = 0; k < 16; k++) {
        _mm256_storeu_si256((__m256i*)(dest_for_jth_element + (total_elements * (offset_into_type + k))), ymm0[k]);
      }
    }
  }
}

/* Routine optimized for unshuffling a buffer for a type size of 2 bytes. */
static void
unshuffle2_avx2(uint8_t* const dest, const uint8_t* const src,
  const size_t vectorizable_elements, const size_t total_elements)
{
  static const size_t bytesoftype = 2;
  size_t i;
  int j;
  __m256i ymm0[2], ymm1[2];

  for (i = 0; i < vectorizable_elements; i += sizeof(__m256i)) {
    /* Load 32 elements (64 bytes) into 2 YMM registers. */
    const uint8_t* const src_for_ith_element = src + i;
    for (j = 0; j < 2; j++) {
      ymm0[j] = _mm256_loadu_si256((__m256i*)(src_for_ith_element + (j * total_elements)));
    }
    /* Shuffle bytes */
    for (j = 0; j < 2; j++) {
      ymm0[j] = _mm256_permute4x64_epi64(ymm0[j], 0xd8);
    }
    /* Compute the low 64 bytes */
    ymm1[0] = _mm256_unpacklo_epi8(ymm0[0], ymm0[1]);
    /* Compute the hi 64 bytes */
    ymm1[1] = _mm256_unpackhi_epi8(ymm0[0], ymm0[1]);
    /* Store the result vectors in proper order */
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (0 * sizeof(__m256i))), ymm1[0]);
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (1 * sizeof(__m256i))), ymm1[1]);
  }
}

/* Routine optimized for unshuffling a buffer for a type size of 4 bytes. */
static void
unshuffle4_avx2(uint8_t* const dest, const uint8_t* const src,
  const size_t vectorizable_elements, const size_t total_elements)
{
  static const size_t bytesoftype = 4;
  size_t i;
  int j;
  __m256i ymm0[4], ymm1[4];

  for (i = 0; i < vectorizable_elements; i += sizeof(__m256i)) {
    /* Load 32 elements (128 bytes) into 4 YMM registers. */
    const uint8_t* const src_for_ith_element = src + i;
    for (j = 0; j < 4; j++) {
      ymm0[j] = _mm256_loadu_si256((__m256i*)(src_for_ith_element + (j * total_elements)));
    }
    /* Shuffle bytes */
    for (j = 0; j < 2; j++) {
      /* Compute the low 64 bytes */
      ymm1[j] = _mm256_unpacklo_epi8(ymm0[j*2], ymm0[j*2+1]);
      /* Compute the hi 64 bytes */
      ymm1[2+j] = _mm256_unpackhi_epi8(ymm0[j*2], ymm0[j*2+1]);
    }
    /* Shuffle 2-byte words */
    for (j = 0; j < 2; j++) {
      /* Compute the low 64 bytes */
      ymm0[j] = _mm256_unpacklo_epi16(ymm1[j*2], ymm1[j*2+1]);
      /* Compute the hi 64 bytes */
      ymm0[2+j] = _mm256_unpackhi_epi16(ymm1[j*2], ymm1[j*2+1]);
    }
    ymm1[0] = _mm256_permute2x128_si256(ymm0[0], ymm0[2], 0x20);
    ymm1[1] = _mm256_permute2x128_si256(ymm0[1], ymm0[3], 0x20);
    ymm1[2] = _mm256_permute2x128_si256(ymm0[0], ymm0[2], 0x31);
    ymm1[3] = _mm256_permute2x128_si256(ymm0[1], ymm0[3], 0x31);

    /* Store the result vectors in proper order */
    for (j = 0; j < 4; j++) {
      _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (j * sizeof(__m256i))), ymm1[j]);
    }
  }
}

/* Routine optimized for unshuffling a buffer for a type size of 8 bytes. */
static void
unshuffle8_avx2(uint8_t* const dest, const uint8_t* const src,
  const size_t vectorizable_elements, const size_t total_elements)
{
  static const size_t bytesoftype = 8;
  size_t i;
  int j;
  __m256i ymm0[8], ymm1[8];

  for (i = 0; i < vectorizable_elements; i += sizeof(__m256i)) {
    /* Fetch 32 elements (256 bytes) into 8 YMM registers. */
    const uint8_t* const src_for_ith_element = src + i;
    for (j = 0; j < 8; j++) {
      ymm0[j] = _mm256_loadu_si256((__m256i*)(src_for_ith_element + (j * total_elements)));
    }
    /* Shuffle bytes */
    for (j = 0; j < 4; j++) {
      /* Compute the low 32 bytes */
      ymm1[j] = _mm256_unpacklo_epi8(ymm0[j*2], ymm0[j*2+1]);
      /* Compute the hi 32 bytes */
      ymm1[4+j] = _mm256_unpackhi_epi8(ymm0[j*2], ymm0[j*2+1]);
    }
    /* Shuffle words */
    for (j = 0; j < 4; j++) {
      /* Compute the low 32 bytes */
      ymm0[j] = _mm256_unpacklo_epi16(ymm1[j*2], ymm1[j*2+1]);
      /* Compute the hi 32 bytes */
      ymm0[4+j] = _mm256_unpackhi_epi16(ymm1[j*2], ymm1[j*2+1]);
    }
    for (j = 0; j < 8; j++) {
      ymm0[j] = _mm256_permute4x64_epi64(ymm0[j], 0xd8);
    }

    /* Shuffle 4-byte dwords */
    for (j = 0; j < 4; j++) {
      /* Compute the low 32 bytes */
      ymm1[j] = _mm256_unpacklo_epi32(ymm0[j*2], ymm0[j*2+1]);
      /* Compute the hi 32 bytes */
      ymm1[4+j] = _mm256_unpackhi_epi32(ymm0[j*2], ymm0[j*2+1]);
    }

    /* Store the result vectors in proper order */
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (0 * sizeof(__m256i))), ymm1[0]);
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (1 * sizeof(__m256i))), ymm1[2]);
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (2 * sizeof(__m256i))), ymm1[1]);
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (3 * sizeof(__m256i))), ymm1[3]);
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (4 * sizeof(__m256i))), ymm1[4]);
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (5 * sizeof(__m256i))), ymm1[6]);
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (6 * sizeof(__m256i))), ymm1[5]);
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (7 * sizeof(__m256i))), ymm1[7]);
  }
}

/* Routine optimized for unshuffling a buffer for a type size of 16 bytes. */
static void
unshuffle16_avx2(uint8_t* const dest, const uint8_t* const src,
  const size_t vectorizable_elements, const size_t total_elements)
{
  static const size_t bytesoftype = 16;
  size_t i;
  int j;
  __m256i ymm0[16], ymm1[16];

  for (i = 0; i < vectorizable_elements; i += sizeof(__m256i)) {
    /* Fetch 32 elements (512 bytes) into 16 YMM registers. */
    const uint8_t* const src_for_ith_element = src + i;
    for (j = 0; j < 16; j++) {
      ymm0[j] = _mm256_loadu_si256((__m256i*)(src_for_ith_element + (j * total_elements)));
    }

    /* Shuffle bytes */
    for (j = 0; j < 8; j++) {
      /* Compute the low 32 bytes */
      ymm1[j] = _mm256_unpacklo_epi8(ymm0[j*2], ymm0[j*2+1]);
      /* Compute the hi 32 bytes */
      ymm1[8+j] = _mm256_unpackhi_epi8(ymm0[j*2], ymm0[j*2+1]);
    }
    /* Shuffle 2-byte words */
    for (j = 0; j < 8; j++) {
      /* Compute the low 32 bytes */
      ymm0[j] = _mm256_unpacklo_epi16(ymm1[j*2], ymm1[j*2+1]);
      /* Compute the hi 32 bytes */
      ymm0[8+j] = _mm256_unpackhi_epi16(ymm1[j*2], ymm1[j*2+1]);
    }
    /* Shuffle 4-byte dwords */
    for (j = 0; j < 8; j++) {
      /* Compute the low 32 bytes */
      ymm1[j] = _mm256_unpacklo_epi32(ymm0[j*2], ymm0[j*2+1]);
      /* Compute the hi 32 bytes */
      ymm1[8+j] = _mm256_unpackhi_epi32(ymm0[j*2], ymm0[j*2+1]);
    }

    /* Shuffle 8-byte qwords */
    for (j = 0; j < 8; j++) {
      /* Compute the low 32 bytes */
      ymm0[j] = _mm256_unpacklo_epi64(ymm1[j*2], ymm1[j*2+1]);
      /* Compute the hi 32 bytes */
      ymm0[8+j] = _mm256_unpackhi_epi64(ymm1[j*2], ymm1[j*2+1]);
    }

    for (j = 0; j < 8; j++) {
      ymm1[j] = _mm256_permute2x128_si256(ymm0[j], ymm0[j+8], 0x20);
      ymm1[j+8] = _mm256_permute2x128_si256(ymm0[j], ymm0[j+8], 0x31);
    }

    /* Store the result vectors in proper order */
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (0 * sizeof(__m256i))), ymm1[0]);
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (1 * sizeof(__m256i))), ymm1[4]);
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (2 * sizeof(__m256i))), ymm1[2]);
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (3 * sizeof(__m256i))), ymm1[6]);
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (4 * sizeof(__m256i))), ymm1[1]);
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (5 * sizeof(__m256i))), ymm1[5]);
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (6 * sizeof(__m256i))), ymm1[3]);
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (7 * sizeof(__m256i))), ymm1[7]);
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (8 * sizeof(__m256i))), ymm1[8]);
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (9 * sizeof(__m256i))), ymm1[12]);
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (10 * sizeof(__m256i))), ymm1[10]);
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (11 * sizeof(__m256i))), ymm1[14]);
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (12 * sizeof(__m256i))), ymm1[9]);
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (13 * sizeof(__m256i))), ymm1[13]);
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (14 * sizeof(__m256i))), ymm1[11]);
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (15 * sizeof(__m256i))), ymm1[15]);
  }
}

/* Routine optimized for unshuffling a buffer for a type size larger than 16 bytes. */
static void
unshuffle16_tiled_avx2(uint8_t* const dest, const uint8_t* const src,
  const size_t vectorizable_elements, const size_t total_elements, const size_t bytesoftype)
{
  size_t i;
  int j;
  __m256i ymm0[16], ymm1[16];

  const lldiv_t vecs_per_el = lldiv(bytesoftype, sizeof(__m128i));

  /* The unshuffle loops are inverted (compared to shuffle_tiled16_avx2)
     to optimize cache utilization. */
  size_t offset_into_type;
  for (offset_into_type = 0; offset_into_type < bytesoftype;
    offset_into_type += (offset_into_type == 0 && vecs_per_el.rem > 0 ? vecs_per_el.rem : sizeof(__m128i))) {
    for (i = 0; i < vectorizable_elements; i += sizeof(__m256i)) {
      /* Load the first 16 bytes of 32 adjacent elements (512 bytes) into 16 YMM registers */
      const uint8_t* const src_for_ith_element = src + i;
      for (j = 0; j < 16; j++) {
        ymm0[j] = _mm256_loadu_si256((__m256i*)(src_for_ith_element + (total_elements * (offset_into_type + j))));
      }

      /* Shuffle bytes */
      for (j = 0; j < 8; j++) {
        /* Compute the low 32 bytes */
        ymm1[j] = _mm256_unpacklo_epi8(ymm0[j*2], ymm0[j*2+1]);
        /* Compute the hi 32 bytes */
        ymm1[8+j] = _mm256_unpackhi_epi8(ymm0[j*2], ymm0[j*2+1]);
      }
      /* Shuffle 2-byte words */
      for (j = 0; j < 8; j++) {
        /* Compute the low 32 bytes */
        ymm0[j] = _mm256_unpacklo_epi16(ymm1[j*2], ymm1[j*2+1]);
        /* Compute the hi 32 bytes */
        ymm0[8+j] = _mm256_unpackhi_epi16(ymm1[j*2], ymm1[j*2+1]);
      }
      /* Shuffle 4-byte dwords */
      for (j = 0; j < 8; j++) {
        /* Compute the low 32 bytes */
        ymm1[j] = _mm256_unpacklo_epi32(ymm0[j*2], ymm0[j*2+1]);
        /* Compute the hi 32 bytes */
        ymm1[8+j] = _mm256_unpackhi_epi32(ymm0[j*2], ymm0[j*2+1]);
      }

      /* Shuffle 8-byte qwords */
      for (j = 0; j < 8; j++) {
        /* Compute the low 32 bytes */
        ymm0[j] = _mm256_unpacklo_epi64(ymm1[j*2], ymm1[j*2+1]);
        /* Compute the hi 32 bytes */
        ymm0[8+j] = _mm256_unpackhi_epi64(ymm1[j*2], ymm1[j*2+1]);
      }

      for (j = 0; j < 8; j++) {
        ymm1[j] = _mm256_permute2x128_si256(ymm0[j], ymm0[j+8], 0x20);
        ymm1[j+8] = _mm256_permute2x128_si256(ymm0[j], ymm0[j+8], 0x31);
      }

      /* Store the result vectors in proper order */
      const uint8_t* const dest_with_offset = dest + offset_into_type;
      _mm256_storeu2_m128i(
        (__m128i*)(dest_with_offset + (i + 0x01) * bytesoftype),
        (__m128i*)(dest_with_offset + (i + 0x00) * bytesoftype), ymm1[0]);
      _mm256_storeu2_m128i(
        (__m128i*)(dest_with_offset + (i + 0x03) * bytesoftype),
        (__m128i*)(dest_with_offset + (i + 0x02) * bytesoftype), ymm1[4]);
      _mm256_storeu2_m128i(
        (__m128i*)(dest_with_offset + (i + 0x05) * bytesoftype),
        (__m128i*)(dest_with_offset + (i + 0x04) * bytesoftype), ymm1[2]);
      _mm256_storeu2_m128i(
        (__m128i*)(dest_with_offset + (i + 0x07) * bytesoftype),
        (__m128i*)(dest_with_offset + (i + 0x06) * bytesoftype), ymm1[6]);
      _mm256_storeu2_m128i(
        (__m128i*)(dest_with_offset + (i + 0x09) * bytesoftype),
        (__m128i*)(dest_with_offset + (i + 0x08) * bytesoftype), ymm1[1]);
      _mm256_storeu2_m128i(
        (__m128i*)(dest_with_offset + (i + 0x0b) * bytesoftype),
        (__m128i*)(dest_with_offset + (i + 0x0a) * bytesoftype), ymm1[5]);
      _mm256_storeu2_m128i(
        (__m128i*)(dest_with_offset + (i + 0x0d) * bytesoftype),
        (__m128i*)(dest_with_offset + (i + 0x0c) * bytesoftype), ymm1[3]);
      _mm256_storeu2_m128i(
        (__m128i*)(dest_with_offset + (i + 0x0f) * bytesoftype),
        (__m128i*)(dest_with_offset + (i + 0x0e) * bytesoftype), ymm1[7]);
      _mm256_storeu2_m128i(
        (__m128i*)(dest_with_offset + (i + 0x11) * bytesoftype),
        (__m128i*)(dest_with_offset + (i + 0x10) * bytesoftype), ymm1[8]);
      _mm256_storeu2_m128i(
        (__m128i*)(dest_with_offset + (i + 0x13) * bytesoftype),
        (__m128i*)(dest_with_offset + (i + 0x12) * bytesoftype), ymm1[12]);
      _mm256_storeu2_m128i(
        (__m128i*)(dest_with_offset + (i + 0x15) * bytesoftype),
        (__m128i*)(dest_with_offset + (i + 0x14) * bytesoftype), ymm1[10]);
      _mm256_storeu2_m128i(
        (__m128i*)(dest_with_offset + (i + 0x17) * bytesoftype),
        (__m128i*)(dest_with_offset + (i + 0x16) * bytesoftype), ymm1[14]);
      _mm256_storeu2_m128i(
        (__m128i*)(dest_with_offset + (i + 0x19) * bytesoftype),
        (__m128i*)(dest_with_offset + (i + 0x18) * bytesoftype), ymm1[9]);
      _mm256_storeu2_m128i(
        (__m128i*)(dest_with_offset + (i + 0x1b) * bytesoftype),
        (__m128i*)(dest_with_offset + (i + 0x1a) * bytesoftype), ymm1[13]);
      _mm256_storeu2_m128i(
        (__m128i*)(dest_with_offset + (i + 0x1d) * bytesoftype),
        (__m128i*)(dest_with_offset + (i + 0x1c) * bytesoftype), ymm1[11]);
      _mm256_storeu2_m128i(
        (__m128i*)(dest_with_offset + (i + 0x1f) * bytesoftype),
        (__m128i*)(dest_with_offset + (i + 0x1e) * bytesoftype), ymm1[15]);
    }
  }
}

/* Shuffle a block.  This can never fail. */
void
blosc_internal_shuffle_avx2(const size_t bytesoftype, const size_t blocksize,
                            const uint8_t* const _src, uint8_t* const _dest) {
  const size_t vectorized_chunk_size = bytesoftype * sizeof(__m256i);

  /* If the block size is too small to be vectorized,
     use the generic implementation. */
  if (blocksize < vectorized_chunk_size) {
    blosc_internal_shuffle_generic(bytesoftype, blocksize, _src, _dest);
    return;
  }

  /* If the blocksize is not a multiple of both the typesize and
     the vector size, round the blocksize down to the next value
     which is a multiple of both. The vectorized shuffle can be
     used for that portion of the data, and the naive implementation
     can be used for the remaining portion. */
  const size_t vectorizable_bytes = blocksize - (blocksize % vectorized_chunk_size);

  const size_t vectorizable_elements = vectorizable_bytes / bytesoftype;
  const size_t total_elements = blocksize / bytesoftype;

  /* Optimized shuffle implementations */
  switch (bytesoftype)
  {
  case 2:
    shuffle2_avx2(_dest, _src, vectorizable_elements, total_elements);
    break;
  case 4:
    shuffle4_avx2(_dest, _src, vectorizable_elements, total_elements);
    break;
  case 8:
    shuffle8_avx2(_dest, _src, vectorizable_elements, total_elements);
    break;
  case 16:
    shuffle16_avx2(_dest, _src, vectorizable_elements, total_elements);
    break;
  default:
    /* For types larger than 16 bytes, use the AVX2 tiled shuffle. */
    if (bytesoftype > sizeof(__m128i)) {
      shuffle16_tiled_avx2(_dest, _src, vectorizable_elements, total_elements, bytesoftype);
    }
    else {
      /* Non-optimized shuffle */
      blosc_internal_shuffle_generic(bytesoftype, blocksize, _src, _dest);
      /* The non-optimized function covers the whole buffer,
         so we're done processing here. */
      return;
    }
  }

  /* If the buffer had any bytes at the end which couldn't be handled
     by the vectorized implementations, use the non-optimized version
     to finish them up. */
  if (vectorizable_bytes < blocksize) {
    shuffle_generic_inline(bytesoftype, vectorizable_bytes, blocksize, _src, _dest);
  }
}

/* Unshuffle a block.  This can never fail. */
void
blosc_internal_unshuffle_avx2(const size_t bytesoftype, const size_t blocksize,
                              const uint8_t* const _src, uint8_t* const _dest) {
  const size_t vectorized_chunk_size = bytesoftype * sizeof(__m256i);

  /* If the block size is too small to be vectorized,
     use the generic implementation. */
  if (blocksize < vectorized_chunk_size) {
    blosc_internal_unshuffle_generic(bytesoftype, blocksize, _src, _dest);
    return;
  }

  /* If the blocksize is not a multiple of both the typesize and
     the vector size, round the blocksize down to the next value
     which is a multiple of both. The vectorized unshuffle can be
     used for that portion of the data, and the naive implementation
     can be used for the remaining portion. */
  const size_t vectorizable_bytes = blocksize - (blocksize % vectorized_chunk_size);

  const size_t vectorizable_elements = vectorizable_bytes / bytesoftype;
  const size_t total_elements = blocksize / bytesoftype;

  /* Optimized unshuffle implementations */
  switch (bytesoftype)
  {
  case 2:
    unshuffle2_avx2(_dest, _src, vectorizable_elements, total_elements);
    break;
  case 4:
    unshuffle4_avx2(_dest, _src, vectorizable_elements, total_elements);
    break;
  case 8:
    unshuffle8_avx2(_dest, _src, vectorizable_elements, total_elements);
    break;
  case 16:
    unshuffle16_avx2(_dest, _src, vectorizable_elements, total_elements);
    break;
  default:
    /* For types larger than 16 bytes, use the AVX2 tiled unshuffle. */
    if (bytesoftype > sizeof(__m128i)) {
      unshuffle16_tiled_avx2(_dest, _src, vectorizable_elements, total_elements, bytesoftype);
    }
    else {
      /* Non-optimized unshuffle */
      blosc_internal_unshuffle_generic(bytesoftype, blocksize, _src, _dest);
      /* The non-optimized function covers the whole buffer,
         so we're done processing here. */
      return;
    }
  }

  /* If the buffer had any bytes at the end which couldn't be handled
     by the vectorized implementations, use the non-optimized version
     to finish them up. */
  if (vectorizable_bytes < blocksize) {
    unshuffle_generic_inline(bytesoftype, vectorizable_bytes, blocksize, _src, _dest);
  }
}

#endif /* !defined(__AVX2__) */
