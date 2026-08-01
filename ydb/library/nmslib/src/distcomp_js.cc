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
#include "string.h"
#include "utils.h"

#include "portable_intrinsics.h"

#include <cstdlib>
#include <cstdint>
#include <limits>
#include <algorithm>


namespace similarity {

using namespace std;


/*
 * Jensen-Shannon divergence.
 * 
 * The square root of JS is a metric: 
 *
 * Österreicher, Ferdinand, and Igor Vajda. 
 * "A new class of metric divergences on probability spaces and its applicability in statistics." 
 * Annals of the Institute of Statistical Mathematics 55.3 (2003): 639-653.
 *
 *
 * Endres, Dominik Maria, and Johannes E. Schindelin. 
 * "A new metric for probability distributions." 
 * Information Theory, IEEE Transactions on 49.7 (2003): 1858-1860.
 *
 */
template <class T> T JSStandard(const T *pVect1, const T *pVect2, size_t qty)
{
    T sum1 = 0, sum2 = 0;

    for (size_t i = 0; i < qty; i++) {
      T m = (pVect1[i] + pVect2[i])*T(0.5);
      T lg1 = pVect1[i] < numeric_limits<T>::min() ? T(0):log(pVect1[i]);
      T lg2 = pVect2[i] < numeric_limits<T>::min() ? T(0):log(pVect2[i]);
      sum1 += pVect1[i] * lg1 + pVect2[i] * lg2;
      if (m >= numeric_limits<T>::min()) {
        sum2 +=  m * log(m);
      }
    }

    // Due to computation/rounding errors, we may get a small-magnitude negative number
    return std::max(T(0.5)*sum1 - sum2, T(0));
}

template float JSStandard<float>(const float* pVect1, const float* pVect2, size_t qty);

template <class T>
T JSPrecomp(const T* pVect1, const T* pVect2, size_t qty) {
    T sum1 = 0, sum2 = 0;

    const T* pEnd2 = pVect1 + qty;

    const T* pVectLog1 = pVect1 + qty;
    const T* pVectLog2 = pVect2 + qty;

    while (pVect1 < pEnd2) {
        T m = T(0.5)*(*pVect1 + *pVect2);
        sum1 += (*pVect1) * (*pVectLog1) + (*pVect2)*(*pVectLog2);
        if (m >= numeric_limits<T>::min()) {
          sum2 += m * log(m);
        }
        pVect1++; pVect2++; pVectLog1++; pVectLog2++;
    }

    // Due to computation/rounding errors, we may get a small-magnitude negative number
    return std::max(T(0.5)*sum1 - sum2, T(0));
}

template float JSPrecomp<float>(const float* pVect1, const float* pVect2, size_t qty);

const unsigned LogQty = 65536;

template <class T>
inline unsigned lapprox(T f) {
  return static_cast<unsigned>(floor(LogQty*f));
};

template <class T>
class ApproxLogs {
public:
  ApproxLogs() {
    for (unsigned i = 0; i <= LogQty; ++i) {
      T v = i * T(1.0) / LogQty;
      unsigned k = lapprox(v);
      LogTable[k] = log(1 + v);
    }
  }
  T LogTable[LogQty + 2];
};

template <class T> T 
JSPrecompApproxLog(const T *pVect1, const T *pVect2, size_t qty) {
    static ApproxLogs<T> ApproxLogs; // Thread-safe in C++11 
    static T* ltbl = ApproxLogs.LogTable;
    static T clog2 = log(T(2)); // Thread-safe in C++11 
    T sum = 0;

    const T* pEnd2 = pVect1 + qty;

    const T* pVectLog1 = pVect1 + qty;
    const T* pVectLog2 = pVect2 + qty;

    while (pVect1 < pEnd2) {
      T v1 = *pVect1;
      T v2 = *pVect2;
      T lv1 = *pVectLog1;
      T lv2 = *pVectLog2;

      sum += v1 * lv1 + v2 * lv2;

      if (v1 > v2) {
        swap(v1, v2);
        swap(lv1, lv2);
      }
      if (v2 >= numeric_limits<T>::min()) {
        sum -= (v1 + v2)*(lv2 + ltbl[lapprox(v1/v2)] - clog2);
      }
      pVect1++; pVect2++; pVectLog1++; pVectLog2++;
    }

    return std::max(T(0.5)*sum, T(0));
}

template float JSPrecompApproxLog<float>(const float* pVect1, const float* pVect2, size_t qty);

template <>
float JSPrecompSIMDApproxLog(const float* pVect1, const float* pVect2, size_t qty)
{
#ifndef PORTABLE_SSE2
#pragma message WARN("JSPrecompSIMDApproxLog<float>: SSE2 is not available, defaulting to pure C++ implementation!")
    return JSPrecompApproxLog(pVect1, pVect2, qty);
#else
    size_t qty4  = qty/4;

    static ApproxLogs<float> ApproxLogs; // Thread-safe in C++11
    static float clog2 = log(2.0f); // Thread-safe in C++11 
    static __m128 clog2simd = _mm_set1_ps(clog2); // Thread-safe in C++11
    static float* ltbl = ApproxLogs.LogTable;
    __m128 cmult = _mm_set1_ps(LogQty);

    const float* pEnd2 = pVect1 + 4  * qty4;
    const float* pEnd3 = pVect1 + qty;

    const float* pVectLog1 = pVect1 + qty;
    const float* pVectLog2 = pVect2 + qty;

    __m128  v1, v2, vLog1, vLog2, maxv, minv, max_mod_logv, ltmp;
    __m128  sum     = _mm_set1_ps(0), s1, s2;
    __m128  minVal  = _mm_set1_ps(numeric_limits<float>::min());
    __m128i tmpi;

    while (pVect1 < pEnd2) {
        int PORTABLE_ALIGN16 TmpRes[4];


        v1      = _mm_loadu_ps(pVect1);     pVect1 += 4;
        vLog1   = _mm_loadu_ps(pVectLog1);  pVectLog1 += 4;
        s1      = _mm_mul_ps(v1, vLog1);
        v2      = _mm_loadu_ps(pVect2);     pVect2 += 4;
        vLog2   = _mm_loadu_ps(pVectLog2);  pVectLog2 += 4;
        s2      =  _mm_mul_ps(v2, vLog2);
        /* 
         * If want to prevent division by zero, if v1 == v2 == 0,
         * then, we don't have to compute the second factor in
         * (v1 + v2)*(lv2 + ltbl[lapprox(v1/v2)] - clog2);
         * correctly, it will be multiplied by zero anyway.
         */
        maxv    = _mm_max_ps(_mm_max_ps(v1, v2), minVal);
        // This is the log with the largest modulo (recall that logs are < 0 here)
        max_mod_logv = _mm_max_ps(vLog1, vLog2);
        minv    = _mm_min_ps(v1, v2);
        tmpi    = _mm_cvttps_epi32(_mm_mul_ps(cmult, _mm_div_ps(minv, maxv)));
        sum     = _mm_add_ps(sum, _mm_add_ps(s1,s2));

        _mm_store_si128(reinterpret_cast<__m128i*>(TmpRes), tmpi);

        ltmp = _mm_set_ps(ltbl[TmpRes[3]], ltbl[TmpRes[2]], ltbl[TmpRes[1]], ltbl[TmpRes[0]]);
        __m128 d = _mm_sub_ps(_mm_add_ps(max_mod_logv, ltmp), clog2simd);
        sum     = _mm_sub_ps(sum, _mm_mul_ps(_mm_add_ps(v1, v2), d));
    }

    float PORTABLE_ALIGN16 TmpRes[4];

    _mm_store_ps(TmpRes, sum);
    float res= TmpRes[0] + TmpRes[1] + TmpRes[2] + TmpRes[3];

    while (pVect1 < pEnd3) {
      float v1 = *pVect1;
      float v2 = *pVect2;
      float lv1 = *pVectLog1;
      float lv2 = *pVectLog2;

      res += v1 * lv1 + v2 * lv2;

      if (v1 > v2) {
        swap(v1, v2);
        swap(lv1, lv2);
      }
      if (v2 >= numeric_limits<float>::min()) {
        res -= (v1 + v2)*(lv2 + ltbl[lapprox(v1/v2)] - clog2);
      }
      pVect1++; pVect2++; pVectLog1++; pVectLog2++;
    }

    // Due to computation/rounding errors, we may get a small-magnitude negative number
    return 0.5*max(0.f, res);
#endif
}
    

template float JSPrecompSIMDApproxLog<float>(const float* pVect1, const float* pVect2, size_t qty);

}
