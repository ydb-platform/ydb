/**
 * Non-metric Space Library
 *
 * Main developers: Bilegsaikhan Naidan, Leonid Boytsov, Yury Malkov, Ben Frederickson, David Novak
 *
 * For the complete list of contributors and further details see:
 * https://github.com/searchivarius/NonMetricSpaceLib
 *
 * Copyright (c) 2013-2018
 *
 * This code is released under the
 * Apache License Version 2.0 http://www.apache.org/licenses/.
 *
 */
/*
*
* A Hierarchical Navigable Small World (HNSW) approach.
*
* The main publication is (available on arxiv: http://arxiv.org/abs/1603.09320):
* "Efficient and robust approximate nearest neighbor search using Hierarchical Navigable Small World graphs"
* Yu. A. Malkov, D. A. Yashunin
* This code was contributed by Yu. A. Malkov. It also was used in tests from the paper.
*
*
*/
#pragma once


#include "portable_simd.h"
#include "portable_intrinsics.h"
#include "portable_prefetch.h"
#include "distcomp.h"

namespace similarity {

// Define a temporary array for the functions below. The AVX uses 256-bit registers, which
// is 8 floats
#define TMP_RES_ARRAY(varName)  float PORTABLE_ALIGN32 (varName)[8];

#if defined(PORTABLE_AVX)

inline float L2Sqr16Ext(const float *pVect1, const float *pVect2, size_t &qty, float * __restrict TmpRes) {
  #pragma message INFO("L2Sqr16Ext: using AVX version")
  size_t qty16 = qty >> 4;

  const float *pEnd1 = pVect1 + (qty16 << 4);

  __m256 diff_32_8, v1_32_8, v2_32_8;
  __m256 sum_32_8 = _mm256_set1_ps(0);

  while (pVect1 < pEnd1) {
    PREFETCH((char*)(pVect2 + 16), _MM_HINT_T0);
    v1_32_8 = _mm256_loadu_ps(pVect1);
    pVect1 += 8;
    v2_32_8 = _mm256_loadu_ps(pVect2);
    pVect2 += 8;
    diff_32_8 = _mm256_sub_ps(v1_32_8, v2_32_8);
    sum_32_8 = _mm256_add_ps(sum_32_8, _mm256_mul_ps(diff_32_8, diff_32_8));

    v1_32_8 = _mm256_loadu_ps(pVect1);
    pVect1 += 8;
    v2_32_8 = _mm256_loadu_ps(pVect2);
    pVect2 += 8;
    diff_32_8 = _mm256_sub_ps(v1_32_8, v2_32_8);
    sum_32_8 = _mm256_add_ps(sum_32_8, _mm256_mul_ps(diff_32_8, diff_32_8));
  }

  _mm256_store_ps(TmpRes, sum_32_8);
  return TmpRes[0] + TmpRes[1] + TmpRes[2] + TmpRes[3] + TmpRes[4] + TmpRes[5] + TmpRes[6] + TmpRes[7];
}

inline float L2SqrExt(const float *pVect1, const float *pVect2, size_t &qty, float *__restrict TmpRes) {
  #pragma message INFO("L2SqrExt: using AVX version")
  size_t qty4 = qty >> 2;
  size_t qty16 = qty >> 4;

  const float *pEnd1 = pVect1 + (qty16 << 4);
  const float *pEnd2 = pVect1 + (qty4 << 2);
  const float *pEnd3 = pVect1 + qty;

  __m256 diff_32_8, v1_32_8, v2_32_8;
  __m256 sum_32_8 = _mm256_set1_ps(0);

  while (pVect1 < pEnd1) {
    PREFETCH((char*)(pVect2 + 16), _MM_HINT_T0);
    v1_32_8 = _mm256_loadu_ps(pVect1);
    pVect1 += 8;
    v2_32_8 = _mm256_loadu_ps(pVect2);
    pVect2 += 8;
    diff_32_8 = _mm256_sub_ps(v1_32_8, v2_32_8);
    sum_32_8 = _mm256_add_ps(sum_32_8, _mm256_mul_ps(diff_32_8, diff_32_8));

    v1_32_8 = _mm256_loadu_ps(pVect1);
    pVect1 += 8;
    v2_32_8 = _mm256_loadu_ps(pVect2);
    pVect2 += 8;
    diff_32_8 = _mm256_sub_ps(v1_32_8, v2_32_8);
    sum_32_8 = _mm256_add_ps(sum_32_8, _mm256_mul_ps(diff_32_8, diff_32_8));
  }

  __m128 diff_32_4, v1_32_4, v2_32_4;
  __m128 sum_32_4 = _mm_add_ps(_mm256_extractf128_ps(sum_32_8, 0), _mm256_extractf128_ps(sum_32_8, 1));

  while (pVect1 < pEnd2) {
    v1_32_4 = _mm_loadu_ps(pVect1);
    pVect1 += 4;
    v2_32_4 = _mm_loadu_ps(pVect2);
    pVect2 += 4;
    diff_32_4 = _mm_sub_ps(v1_32_4, v2_32_4);
    sum_32_4 = _mm_add_ps(sum_32_4, _mm_mul_ps(diff_32_4, diff_32_4));
  }

  _mm_store_ps(TmpRes, sum_32_4);
  float sum_32_1 = TmpRes[0] + TmpRes[1] + TmpRes[2] + TmpRes[3];
  float diff_32_1;
  while (pVect1 < pEnd3) {
    diff_32_1 = *pVect1++ - *pVect2++;
    sum_32_1 += diff_32_1 * diff_32_1;
  }

  return sum_32_1;
}

inline float ScalarProduct(const float *__restrict pVect1, const float *__restrict pVect2, size_t qty,
                  float *__restrict TmpRes) {
  #pragma message INFO("ScalarProduct: using AVX version")

  size_t qty16 = qty / 16;
  size_t qty4 = qty / 4;

  const float *pEnd1 = pVect1 + 16 * qty16;
  const float *pEnd2 = pVect1 + 4 * qty4;
  const float *pEnd3 = pVect1 + qty;

  __m256 v1_32_8, v2_32_8;
  __m256 sum_32_8 = _mm256_set1_ps(0);

  while (pVect1 < pEnd1) {
    PREFETCH((char*)(pVect2 + 16), _MM_HINT_T0);

    v1_32_8 = _mm256_loadu_ps(pVect1);
    pVect1 += 8;
    v2_32_8 = _mm256_loadu_ps(pVect2);
    pVect2 += 8;
    sum_32_8 = _mm256_add_ps(sum_32_8, _mm256_mul_ps(v1_32_8, v2_32_8));

    v1_32_8 = _mm256_loadu_ps(pVect1);
    pVect1 += 8;
    v2_32_8 = _mm256_loadu_ps(pVect2);
    pVect2 += 8;
    sum_32_8 = _mm256_add_ps(sum_32_8, _mm256_mul_ps(v1_32_8, v2_32_8));
  }

  __m128 v1_32_4, v2_32_4;
  __m128 sum_32_4 = _mm_add_ps(_mm256_extractf128_ps(sum_32_8, 0), _mm256_extractf128_ps(sum_32_8, 1));

  while (pVect1 < pEnd2) {
    v1_32_4 = _mm_loadu_ps(pVect1);
    pVect1 += 4;
    v2_32_4 = _mm_loadu_ps(pVect2);
    pVect2 += 4;
    sum_32_4 = _mm_add_ps(sum_32_4, _mm_mul_ps(v1_32_4, v2_32_4));
  }

  _mm_store_ps(TmpRes, sum_32_4);
  float sum_32_1 = TmpRes[0] + TmpRes[1] + TmpRes[2] + TmpRes[3];
  while (pVect1 < pEnd3) {
    sum_32_1 += (*pVect1) * (*pVect2);
    ++pVect1;
    ++pVect2;
  }
  return sum_32_1;
}

#elif defined(PORTABLE_SSE2)

inline float L2Sqr16Ext(const float *pVect1, const float *pVect2, size_t &qty, float *__restrict TmpRes) {
  #pragma message INFO("L2Sqr16Ext: using SSE2 version")
  size_t qty16 = qty >> 4;

  const float *pEnd1 = pVect1 + (qty16 << 4);

  __m128 diff, v1_32_4, v2_32_4;
  __m128 sum_32_4 = _mm_set1_ps(0);
  while (pVect1 < pEnd1) {
    PREFETCH((char *) (pVect2 + 16), _MM_HINT_T0);
    v1_32_4 = _mm_loadu_ps(pVect1);
    pVect1 += 4;
    v2_32_4 = _mm_loadu_ps(pVect2);
    pVect2 += 4;
    diff = _mm_sub_ps(v1_32_4, v2_32_4);
    sum_32_4 = _mm_add_ps(sum_32_4, _mm_mul_ps(diff, diff));

    v1_32_4 = _mm_loadu_ps(pVect1);
    pVect1 += 4;
    v2_32_4 = _mm_loadu_ps(pVect2);
    pVect2 += 4;
    diff = _mm_sub_ps(v1_32_4, v2_32_4);
    sum_32_4 = _mm_add_ps(sum_32_4, _mm_mul_ps(diff, diff));

    v1_32_4 = _mm_loadu_ps(pVect1);
    pVect1 += 4;
    v2_32_4 = _mm_loadu_ps(pVect2);
    pVect2 += 4;
    diff = _mm_sub_ps(v1_32_4, v2_32_4);
    sum_32_4 = _mm_add_ps(sum_32_4, _mm_mul_ps(diff, diff));

    v1_32_4 = _mm_loadu_ps(pVect1);
    pVect1 += 4;
    v2_32_4 = _mm_loadu_ps(pVect2);
    pVect2 += 4;
    diff = _mm_sub_ps(v1_32_4, v2_32_4);
    sum_32_4 = _mm_add_ps(sum_32_4, _mm_mul_ps(diff, diff));
  }

  _mm_store_ps(TmpRes, sum_32_4);

  return TmpRes[0] + TmpRes[1] + TmpRes[2] + TmpRes[3];
}

inline float L2SqrExt(const float *pVect1, const float *pVect2, size_t &qty, float *__restrict TmpRes) {
  #pragma message INFO("L2SqrExt: using SSE2 version")
  size_t qty4 = qty >> 2;
  size_t qty16 = qty >> 4;

  const float *pEnd1 = pVect1 + (qty16 << 4);
  const float *pEnd2 = pVect1 + (qty4 << 2);
  const float *pEnd3 = pVect1 + qty;

  __m128 diff, v1_32_4, v2_32_4;
  __m128 sum_32_4 = _mm_set1_ps(0);

  while (pVect1 < pEnd1) {
    PREFETCH((char *) (pVect2 + 16), _MM_HINT_T0);
    v1_32_4 = _mm_loadu_ps(pVect1);
    pVect1 += 4;
    v2_32_4 = _mm_loadu_ps(pVect2);
    pVect2 += 4;
    diff = _mm_sub_ps(v1_32_4, v2_32_4);
    sum_32_4 = _mm_add_ps(sum_32_4, _mm_mul_ps(diff, diff));

    v1_32_4 = _mm_loadu_ps(pVect1);
    pVect1 += 4;
    v2_32_4 = _mm_loadu_ps(pVect2);
    pVect2 += 4;
    diff = _mm_sub_ps(v1_32_4, v2_32_4);
    sum_32_4 = _mm_add_ps(sum_32_4, _mm_mul_ps(diff, diff));

    v1_32_4 = _mm_loadu_ps(pVect1);
    pVect1 += 4;
    v2_32_4 = _mm_loadu_ps(pVect2);
    pVect2 += 4;
    diff = _mm_sub_ps(v1_32_4, v2_32_4);
    sum_32_4 = _mm_add_ps(sum_32_4, _mm_mul_ps(diff, diff));

    v1_32_4 = _mm_loadu_ps(pVect1);
    pVect1 += 4;
    v2_32_4 = _mm_loadu_ps(pVect2);
    pVect2 += 4;
    diff = _mm_sub_ps(v1_32_4, v2_32_4);
    sum_32_4 = _mm_add_ps(sum_32_4, _mm_mul_ps(diff, diff));
  }

  while (pVect1 < pEnd2) {
    v1_32_4 = _mm_loadu_ps(pVect1);
    pVect1 += 4;
    v2_32_4 = _mm_loadu_ps(pVect2);
    pVect2 += 4;
    diff = _mm_sub_ps(v1_32_4, v2_32_4);
    sum_32_4 = _mm_add_ps(sum_32_4, _mm_mul_ps(diff, diff));
  }

  _mm_store_ps(TmpRes, sum_32_4);
  float sum_32_1 = TmpRes[0] + TmpRes[1] + TmpRes[2] + TmpRes[3];
  float diff_32_1;
  while (pVect1 < pEnd3) {
      diff_32_1 = *pVect1++ - *pVect2++;
      sum_32_1 += diff_32_1 * diff_32_1;
  }

  return sum_32_1;
}

inline float ScalarProduct(const float *__restrict pVect1, const float *__restrict pVect2, size_t qty,
                  float *__restrict TmpRes) {
  #pragma message INFO("ScalarProduct: using SSE2 version")
  size_t qty16 = qty / 16;
  size_t qty4 = qty / 4;

  const float *pEnd1 = pVect1 + 16 * qty16;
  const float *pEnd2 = pVect1 + 4 * qty4;
  const float *pEnd3 = pVect1 + qty;

  __m128 v1_32_4, v2_32_4;
  __m128 sum_32_4 = _mm_set1_ps(0);

  while (pVect1 < pEnd1) {
    v1_32_4 = _mm_loadu_ps(pVect1);
    pVect1 += 4;
    v2_32_4 = _mm_loadu_ps(pVect2);
    pVect2 += 4;
    sum_32_4 = _mm_add_ps(sum_32_4, _mm_mul_ps(v1_32_4, v2_32_4));

    v1_32_4 = _mm_loadu_ps(pVect1);
    pVect1 += 4;
    v2_32_4 = _mm_loadu_ps(pVect2);
    pVect2 += 4;
    sum_32_4 = _mm_add_ps(sum_32_4, _mm_mul_ps(v1_32_4, v2_32_4));

    v1_32_4 = _mm_loadu_ps(pVect1);
    pVect1 += 4;
    v2_32_4 = _mm_loadu_ps(pVect2);
    pVect2 += 4;
    sum_32_4 = _mm_add_ps(sum_32_4, _mm_mul_ps(v1_32_4, v2_32_4));

    v1_32_4 = _mm_loadu_ps(pVect1);
    pVect1 += 4;
    v2_32_4 = _mm_loadu_ps(pVect2);
    pVect2 += 4;
    sum_32_4 = _mm_add_ps(sum_32_4, _mm_mul_ps(v1_32_4, v2_32_4));
  }

  while (pVect1 < pEnd2) {
    v1_32_4 = _mm_loadu_ps(pVect1);
    pVect1 += 4;
    v2_32_4 = _mm_loadu_ps(pVect2);
    pVect2 += 4;
    sum_32_4 = _mm_add_ps(sum_32_4, _mm_mul_ps(v1_32_4, v2_32_4));
  }

  _mm_store_ps(TmpRes, sum_32_4);
  float sum_32_1 = TmpRes[0] + TmpRes[1] + TmpRes[2] + TmpRes[3];
  while (pVect1 < pEnd3) {
    sum_32_1 += (*pVect1) * (*pVect2);
    ++pVect1;
    ++pVect2;
  }
  return sum_32_1;
}

#elif defined(PORTABLE_NEON)

inline float L2Sqr16Ext(const float *pVect1, const float *pVect2, size_t &qty, float * __restrict TmpRes) {
  #pragma message INFO("L2Sqr16Ext: Neon enabled")
  size_t qty16 = qty >> 4;
  const float *pEnd1 = pVect1 + (qty16 << 4);

  float32x4_t diff_32_4, v1_32_4, v2_32_4;
  float32x4_t sum_32_4 = vdupq_n_f32(0);

  while (pVect1 < pEnd1) {
    PREFETCH((char*)(pVect2 + 16), _MM_HINT_T0);
    v1_32_4 = vld1q_f32(pVect1);
    pVect1 += 4;
    v2_32_4 = vld1q_f32(pVect2);
    pVect2 += 4;
    diff_32_4 = vsubq_f32(v1_32_4, v2_32_4);
    sum_32_4 = vfmaq_f32(sum_32_4, diff_32_4, diff_32_4);

    v1_32_4 = vld1q_f32(pVect1);
    pVect1 += 4;
    v2_32_4 = vld1q_f32(pVect2);
    pVect2 += 4;
    diff_32_4 = vsubq_f32(v1_32_4, v2_32_4);
    sum_32_4 = vfmaq_f32(sum_32_4, diff_32_4, diff_32_4);

    v1_32_4 = vld1q_f32(pVect1);
    pVect1 += 4;
    v2_32_4 = vld1q_f32(pVect2);
    pVect2 += 4;
    diff_32_4 = vsubq_f32(v1_32_4, v2_32_4);
    sum_32_4 = vfmaq_f32(sum_32_4, diff_32_4, diff_32_4);

    v1_32_4 = vld1q_f32(pVect1);
    pVect1 += 4;
    v2_32_4 = vld1q_f32(pVect2);
    pVect2 += 4;
    diff_32_4 = vsubq_f32(v1_32_4, v2_32_4);
    sum_32_4 = vfmaq_f32(sum_32_4, diff_32_4, diff_32_4);
  }
  float32x4_t sum_32_2 = vpaddq_f32(sum_32_4, sum_32_4);
  return vdups_laneq_f32(sum_32_2, 0) + vdups_laneq_f32(sum_32_2, 1);
}

inline float L2SqrExt(const float *pVect1, const float *pVect2, size_t &qty, float *__restrict TmpRes) {
  #pragma message INFO("L2SqrExt: Neon enabled")
  size_t qty4 = qty >> 2;
  size_t qty16 = qty >> 4;

  const float *pEnd1 = pVect1 + (qty16 << 4);
  const float *pEnd2 = pVect1 + (qty4 << 2);
  const float *pEnd3 = pVect1 + qty;

  float32x4_t diff_32_4, v1_32_4, v2_32_4;
  float32x4_t sum_32_4 = vdupq_n_f32(0);

  while (pVect1 < pEnd1) {
    PREFETCH((char*)(pVect2 + 16), _MM_HINT_T0);
    v1_32_4 = vld1q_f32(pVect1);
    pVect1 += 4;
    v2_32_4 = vld1q_f32(pVect2);
    pVect2 += 4;
    diff_32_4 = vsubq_f32(v1_32_4, v2_32_4);
    sum_32_4 = vfmaq_f32(sum_32_4, diff_32_4, diff_32_4);

    v1_32_4 = vld1q_f32(pVect1);
    pVect1 += 4;
    v2_32_4 = vld1q_f32(pVect2);
    pVect2 += 4;
    diff_32_4 = vsubq_f32(v1_32_4, v2_32_4);
    sum_32_4 = vfmaq_f32(sum_32_4, diff_32_4, diff_32_4);

    v1_32_4 = vld1q_f32(pVect1);
    pVect1 += 4;
    v2_32_4 = vld1q_f32(pVect2);
    pVect2 += 4;
    diff_32_4 = vsubq_f32(v1_32_4, v2_32_4);
    sum_32_4 = vfmaq_f32(sum_32_4, diff_32_4, diff_32_4);

    v1_32_4 = vld1q_f32(pVect1);
    pVect1 += 4;
    v2_32_4 = vld1q_f32(pVect2);
    pVect2 += 4;
    diff_32_4 = vsubq_f32(v1_32_4, v2_32_4);
    sum_32_4 = vfmaq_f32(sum_32_4, diff_32_4, diff_32_4);
  }

  while (pVect1 < pEnd2) {
    v1_32_4 = vld1q_f32(pVect1);
    pVect1 += 4;
    v2_32_4 = vld1q_f32(pVect2);
    pVect2 += 4;
    diff_32_4 = vsubq_f32(v1_32_4, v2_32_4);
    sum_32_4 = vfmaq_f32(sum_32_4, diff_32_4, diff_32_4);
  }

  float32x4_t sum_32_2 = vpaddq_f32(sum_32_4, sum_32_4);
  float sum_32_1 = vdups_laneq_f32(sum_32_2, 0) + vdups_laneq_f32(sum_32_2, 1);
  float diff_32_1;
  while (pVect1 < pEnd3) {
    diff_32_1 = *pVect1++ - *pVect2++;
    sum_32_1 += diff_32_1 * diff_32_1;
  }

  return sum_32_1;
}

inline float ScalarProduct(const float *__restrict pVect1, const float *__restrict pVect2, size_t qty,
                  float *__restrict TmpRes) {
  #pragma message INFO("ScalarProduct: Neon enabled")
  size_t qty16 = qty / 16;
  size_t qty4 = qty / 4;

  const float *pEnd1 = pVect1 + 16 * qty16;
  const float *pEnd2 = pVect1 + 4 * qty4;
  const float *pEnd3 = pVect1 + qty;

  float32x4_t v1_32_4, v2_32_4;
  float32x4_t sum_32_4 = vdupq_n_f32(0);

  while (pVect1 < pEnd1) {
    PREFETCH((char*)(pVect2 + 16), _MM_HINT_T0);
    v1_32_4 = vld1q_f32(pVect1);
    pVect1 += 4;
    v2_32_4 = vld1q_f32(pVect2);
    pVect2 += 4;
    sum_32_4 = vfmaq_f32(sum_32_4, v1_32_4, v2_32_4);

    v1_32_4 = vld1q_f32(pVect1);
    pVect1 += 4;
    v2_32_4 = vld1q_f32(pVect2);
    pVect2 += 4;
    sum_32_4 = vfmaq_f32(sum_32_4, v1_32_4, v2_32_4);

    v1_32_4 = vld1q_f32(pVect1);
    pVect1 += 4;
    v2_32_4 = vld1q_f32(pVect2);
    pVect2 += 4;
    sum_32_4 = vfmaq_f32(sum_32_4, v1_32_4, v2_32_4);

    v1_32_4 = vld1q_f32(pVect1);
    pVect1 += 4;
    v2_32_4 = vld1q_f32(pVect2);
    pVect2 += 4;
    sum_32_4 = vfmaq_f32(sum_32_4, v1_32_4, v2_32_4);
  }

  while (pVect1 < pEnd2) {
    v1_32_4 = vld1q_f32(pVect1);
    pVect1 += 4;
    v2_32_4 = vld1q_f32(pVect2);
    pVect2 += 4;
    sum_32_4 = vfmaq_f32(sum_32_4, v1_32_4, v2_32_4);
  }

  float32x4_t sum_32_2 = vpaddq_f32(sum_32_4, sum_32_4);
  float sum_32_1 = vdups_laneq_f32(sum_32_2, 0) + vdups_laneq_f32(sum_32_2, 1);

  while (pVect1 < pEnd3) {
    sum_32_1 += (*pVect1) * (*pVect2);
    ++pVect1;
    ++pVect2;
  }
  return sum_32_1;
}

#else

inline float L2SqrExt(const float *pVect1, const float *pVect2, size_t &qty, float *__restrict TmpRes) {
  #pragma message INFO("L2SqrExt: SIMD is not available")
  float v = L2NormStandard(pVect1, pVect2, qty);
  return v * v;
}

inline float L2Sqr16Ext(const float *pVect1, const float *pVect2, size_t &qty, float * __restrict TmpRes) {
  #pragma message INFO("L2Sqr16Ext: SIMD is not available")
  return L2SqrExt(pVect1, pVect2, qty, TmpRes);
}

inline float ScalarProduct(const float *__restrict pVect1, const float *__restrict pVect2, size_t qty,
                  float *__restrict TmpRes) {
  #pragma message INFO("ScalarProduct: SIMD is not available")
  return ScalarProduct(pVect1, pVect2, qty);
}

#endif
}
