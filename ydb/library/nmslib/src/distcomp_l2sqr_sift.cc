/**
 * Non-metric Space Library
 *
 * Main developers: Bilegsaikhan Naidan, Leonid Boytsov, Yury Malkov, Ben Frederickson, David Novak
 *
 * For the complete list of contributors and further details see:
 * https://github.com/nmslib/nmslib
 *
 * Copyright (c) 2013-2018
 *
 * This code is released under the
 * Apache License Version 2.0 http://www.apache.org/licenses/.
 *
 */
#include "distcomp.h"
#include "logging.h"
#include "utils.h"
#include "pow.h"
#include "portable_intrinsics.h"

#include <cstdlib>
#include <limits>
#include <algorithm>
#include <cmath>

namespace similarity {

using namespace std;

DistTypeSIFT l2SqrSIFTNaive(const uint8_t* pVect1,
                            const uint8_t* pVect2) {
  DistTypeSIFT res = 0;
  for (uint_fast32_t i = 0; i < SIFT_DIM; ++i) {
    DistTypeSIFT d = DistTypeSIFT(pVect1[i]) - DistTypeSIFT(pVect2[i]);
    res += d*d;
  }

  return res;
}

DistTypeSIFT l2SqrSIFTPrecomp(const uint8_t* pVect1,
                              const uint8_t* pVect2) {
  DistTypeSIFT sumProd = 0;
  for (uint_fast32_t i = 0; i < SIFT_DIM; ++i) {
    sumProd +=  DistTypeSIFT(pVect1[i]) * DistTypeSIFT(pVect2[i]);
  }

  return *reinterpret_cast<const DistTypeSIFT*>(pVect1 + SIFT_DIM) +
         *reinterpret_cast<const DistTypeSIFT*>(pVect2 + SIFT_DIM) - 2 * sumProd;
}

DistTypeSIFT l2SqrSIFTPrecompSSE2(const uint8_t* pVect1,
                                  const uint8_t* pVect2) {
#ifndef PORTABLE_SSE2
  #pragma message WARN("l2SqrSIFTPrecompSSE4: SSE2 is not available")
  return l2SqrSIFTPrecomp(pVect1, pVect2);
#else
  const unsigned dim = SIFT_DIM;

  DistTypeSIFT sumProd = 0;

  size_t sse_offset = (dim / 16) * 16;

  const __m128i* pStart1 = reinterpret_cast<const __m128i*>(pVect1);
  const __m128i* pStart2 = reinterpret_cast<const __m128i*>(pVect2);
  const __m128i* pEnd2   = reinterpret_cast<const __m128i*>(pVect1 + sse_offset);

  __m128i zero, x1, y1;
  zero = _mm_xor_si128(zero,zero);
  __m128i sum = zero;

  PORTABLE_ALIGN32 int32_t unpack[4];


  while (pStart1 < pEnd2) {
    const __m128i x = _mm_loadu_si128(pStart1++);
    const __m128i y = _mm_loadu_si128(pStart2++);
    x1 = _mm_unpackhi_epi8(x,zero);
    y1 = _mm_unpackhi_epi8(y,zero);
    sum = _mm_add_epi32(sum, _mm_madd_epi16(x1, y1));
    x1 = _mm_unpacklo_epi8(x,zero);
    y1 = _mm_unpacklo_epi8(y,zero);
    sum = _mm_add_epi32(sum, _mm_madd_epi16(x1, y1));
  }
  _mm_store_si128((__m128i *)unpack, sum);
  sumProd += unpack[0] + unpack[1] + unpack[2] + unpack[3];

  if (dim & 16) {
    for (uint_fast32_t i = sse_offset; i < dim; ++i) {
      sumProd += DistTypeSIFT(pVect1[i])*DistTypeSIFT(pVect2[i]);
    }
  }

  return
      *reinterpret_cast<const DistTypeSIFT*>(pVect1 + dim) +
      *reinterpret_cast<const DistTypeSIFT*>(pVect2 + dim) - 2*sumProd;
#endif
}

DistTypeSIFT l2SqrSIFTPrecompAVX(const uint8_t* pVect1,
                                 const uint8_t* pVect2) {
#ifndef PORTABLE_AVX2
#pragma message WARN("l2SqrSIFTPrecompAVX: AVX2 is not available")
  #ifndef PORTABLE_SSE4
  #pragma message WARN("l2SqrSIFTPrecompAVX: SSE4 is not available")
    return l2SqrSIFTPrecomp(pVect1, pVect2);
  #else
    return l2SqrSIFTPrecompSSE2(pVect1, pVect2);
  #endif
#else
  const unsigned dim = SIFT_DIM;

  DistTypeSIFT sumProd = 0;

  size_t sse_offset = (dim / 32) * 32;

  const __m256i* pStart1 = reinterpret_cast<const __m256i*>(pVect1);
  const __m256i* pStart2 = reinterpret_cast<const __m256i*>(pVect2);
  const __m256i* pEnd1   = reinterpret_cast<const __m256i*>(pVect1 + sse_offset);

  __m256i zero, x1, y1;
  zero = _mm256_xor_si256(zero,zero);
  __m256i sum = zero;

  int32_t PORTABLE_ALIGN32 unpack[8];

  while (pStart1 < pEnd1) {
    const __m256i x = _mm256_loadu_si256(pStart1++);
    const __m256i y = _mm256_loadu_si256(pStart2++);
    x1 = _mm256_unpackhi_epi8(x,zero);
    y1 = _mm256_unpackhi_epi8(y,zero);
    sum = _mm256_add_epi32(sum, _mm256_madd_epi16(x1, y1));
    x1 = _mm256_unpacklo_epi8(x,zero);
    y1 = _mm256_unpacklo_epi8(y,zero);
    sum = _mm256_add_epi32(sum, _mm256_madd_epi16(x1, y1));
  }
  _mm256_store_si256((__m256i *)unpack, sum);
  sumProd += unpack[0] + unpack[1] + unpack[2] + unpack[3] +
         unpack[4] + unpack[5] + unpack[6] + unpack[7];

  if (dim & 32) {
    for (uint_fast32_t i = sse_offset; i < dim; ++i) {
      sumProd += DistTypeSIFT(pVect1[i]) * DistTypeSIFT(pVect2[i]);
    }
  }

  return
      *reinterpret_cast<const DistTypeSIFT*>(pVect1+dim) +
      *reinterpret_cast<const DistTypeSIFT*>(pVect2+dim) - 2*sumProd;
#endif
}

}
