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
#include <cmath>

#include "distcomp.h"
#include "portable_intrinsics.h"
#include "utils.h"

namespace similarity {

using namespace std;


int SpearmanFootrule(const PivotIdType* x, const PivotIdType* y, size_t qty) {
  int res = 0;
  int diff;

  for (size_t i = 0; i < qty; ++i) {
    diff = x[i] >= y[i] ? x[i] - y[i] : y[i] - x[i];
    res += diff;
  }

  return res;
}

int SpearmanFootruleSIMD(const int32_t* pVect1, const int32_t* pVect2, size_t qty) {
#ifndef PORTABLE_SSE4
#pragma message WARN("SpearmanFootruleSIMD: SSE4.2 is not available, defaulting to pure C++ implementation!")

    return SpearmanFootrule(pVect1, pVect2, qty);
#else
    size_t qty4  = qty/4;
    size_t qty16 = qty/16;

    const int32_t* pEnd1 = pVect1 + 16 * qty16;
    const int32_t* pEnd2 = pVect1 + 4  * qty4;
    const int32_t* pEnd3 = pVect1 + qty;

    __m128i  diff, v1, v2;
    __m128i  sum = _mm_set1_epi32(0);

    while (pVect1 < pEnd1) {
        v1   = _mm_loadu_si128(reinterpret_cast<const __m128i*>(pVect1)); pVect1 += 4;
        v2   = _mm_loadu_si128(reinterpret_cast<const __m128i*>(pVect2)); pVect2 += 4;
        diff = _mm_sub_epi32(v1, v2);
        sum  = _mm_add_epi32(sum, _mm_max_epi32(_mm_sub_epi32(_mm_setzero_si128(), diff), diff));

        v1   = _mm_loadu_si128(reinterpret_cast<const __m128i*>(pVect1)); pVect1 += 4;
        v2   = _mm_loadu_si128(reinterpret_cast<const __m128i*>(pVect2)); pVect2 += 4;
        diff = _mm_sub_epi32(v1, v2);
        sum  = _mm_add_epi32(sum, _mm_max_epi32(_mm_sub_epi32(_mm_setzero_si128(), diff), diff));

        v1   = _mm_loadu_si128(reinterpret_cast<const __m128i*>(pVect1)); pVect1 += 4;
        v2   = _mm_loadu_si128(reinterpret_cast<const __m128i*>(pVect2)); pVect2 += 4;
        diff = _mm_sub_epi32(v1, v2);
        sum  = _mm_add_epi32(sum, _mm_max_epi32(_mm_sub_epi32(_mm_setzero_si128(), diff), diff));

        v1   = _mm_loadu_si128(reinterpret_cast<const __m128i*>(pVect1)); pVect1 += 4;
        v2   = _mm_loadu_si128(reinterpret_cast<const __m128i*>(pVect2)); pVect2 += 4;
        diff = _mm_sub_epi32(v1, v2);
        sum  = _mm_add_epi32(sum, _mm_max_epi32(_mm_sub_epi32(_mm_setzero_si128(), diff), diff));
    }

    while (pVect1 < pEnd2) {
        v1   = _mm_loadu_si128(reinterpret_cast<const __m128i*>(pVect1)); pVect1 += 4;
        v2   = _mm_loadu_si128(reinterpret_cast<const __m128i*>(pVect2)); pVect2 += 4;
        diff = _mm_sub_epi32(v1, v2);
        sum  = _mm_add_epi32(sum, _mm_max_epi32(_mm_sub_epi32(_mm_setzero_si128(), diff), diff));

    }

    int32_t PORTABLE_ALIGN16 TmpRes[4];

    _mm_store_si128(reinterpret_cast<__m128i*>(TmpRes), sum);
    int res= TmpRes[0] + TmpRes[1] + TmpRes[2] + TmpRes[3];

    while (pVect1 < pEnd3) {
        res += std::abs(*pVect1++ - *pVect2++);
    }

    return res;
#endif
}


int SpearmanRho(const PivotIdType* x, const PivotIdType* y, size_t qty) {
  int res = 0;
  PivotIdType diff;
  size_t i = 0;

  for (; i < qty; ++i) {
    diff = y[i] - x[i];
    res += diff * diff;
  }
  return res;
}

int SpearmanRhoSIMD(const PivotIdType* pVect1, const PivotIdType* pVect2, size_t qty) {
#ifndef PORTABLE_SSE4
#pragma message WARN("SpearmanRhoSIMD: SSE4.2 is not available, defaulting to pure C++ implementation!")
    return SpearmanRho(pVect1, pVect2, qty);
#else
    size_t qty4  = qty/4;
    size_t qty16 = qty/16;

    const int32_t* pEnd1 = pVect1 + 16 * qty16;
    const int32_t* pEnd2 = pVect1 + 4  * qty4;
    const int32_t* pEnd3 = pVect1 + qty;

    __m128i  diff, v1, v2;
    __m128i  sum = _mm_set1_epi16(0);

    while (pVect1 < pEnd1) {
        v1   = _mm_loadu_si128(reinterpret_cast<const __m128i*>(pVect1)); pVect1 += 4;
        v2   = _mm_loadu_si128(reinterpret_cast<const __m128i*>(pVect2)); pVect2 += 4;
        diff = _mm_sub_epi32(v1, v2);
        sum  = _mm_add_epi32(sum, _mm_mullo_epi32(diff, diff));

        v1   = _mm_loadu_si128(reinterpret_cast<const __m128i*>(pVect1)); pVect1 += 4;
        v2   = _mm_loadu_si128(reinterpret_cast<const __m128i*>(pVect2)); pVect2 += 4;
        diff = _mm_sub_epi32(v1, v2);
        sum  = _mm_add_epi32(sum, _mm_mullo_epi32(diff, diff));

        v1   = _mm_loadu_si128(reinterpret_cast<const __m128i*>(pVect1)); pVect1 += 4;
        v2   = _mm_loadu_si128(reinterpret_cast<const __m128i*>(pVect2)); pVect2 += 4;
        diff = _mm_sub_epi32(v1, v2);
        sum  = _mm_add_epi32(sum, _mm_mullo_epi32(diff, diff));

        v1   = _mm_loadu_si128(reinterpret_cast<const __m128i*>(pVect1)); pVect1 += 4;
        v2   = _mm_loadu_si128(reinterpret_cast<const __m128i*>(pVect2)); pVect2 += 4;
        diff = _mm_sub_epi32(v1, v2);
        sum  = _mm_add_epi32(sum, _mm_mullo_epi32(diff, diff));
    }

    while (pVect1 < pEnd2) {
        v1   = _mm_loadu_si128(reinterpret_cast<const __m128i*>(pVect1)); pVect1 += 4;
        v2   = _mm_loadu_si128(reinterpret_cast<const __m128i*>(pVect2)); pVect2 += 4;
        diff = _mm_sub_epi32(v1, v2);
        sum  = _mm_add_epi32(sum, _mm_mullo_epi32(diff, diff));
    }

    int32_t PORTABLE_ALIGN16 TmpRes[8];

    _mm_store_si128(reinterpret_cast<__m128i*>(TmpRes), sum);
    int32_t res= TmpRes[0] + TmpRes[1] + TmpRes[2] + TmpRes[3];

    while (pVect1 < pEnd3) {
        int diff = abs(*pVect1++ - *pVect2++);
        res += diff * diff;
    }

    return res;
#endif
}


}
