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
#include <limits>
#include <algorithm>


namespace similarity {

using namespace std;


/*
 *  Itakura-Saito distance
 */

template <class T> 
T ItakuraSaito(const T* pVect1, const T* pVect2, size_t qty) {
    T sum = 0;

    for (size_t i = 0; i < qty; i++) { 
      sum += pVect1[i]/pVect2[i]  - log(pVect1[i]/pVect2[i]) - 1;
    }

    return sum;
}

template float  ItakuraSaito<float>(const float* pVect1, const float* pVect2, size_t qty);

template <class T> 
T ItakuraSaitoPrecomp(const T* pVect1, const T* pVect2, size_t qty) {
    T sum = 0;

    size_t qty4 = qty/4;
    const T* pEnd1 = pVect1 + qty4 * 4;
    const T* pEnd2 = pVect1 + qty;

    const T* pVectLog1 = pVect1 + qty;
    const T* pVectLog2 = pVect2 + qty;

    while (pVect1 < pEnd1) {
        sum += (*pVect1)/(*pVect2) - ((*pVectLog1) - (*pVectLog2)); ++pVect1; ++pVect2; ++pVectLog1; ++pVectLog2;
        sum += (*pVect1)/(*pVect2) - ((*pVectLog1) - (*pVectLog2)); ++pVect1; ++pVect2; ++pVectLog1; ++pVectLog2;
        sum += (*pVect1)/(*pVect2) - ((*pVectLog1) - (*pVectLog2)); ++pVect1; ++pVect2; ++pVectLog1; ++pVectLog2;
        sum += (*pVect1)/(*pVect2) - ((*pVectLog1) - (*pVectLog2)); ++pVect1; ++pVect2; ++pVectLog1; ++pVectLog2;
    }

    while (pVect1 < pEnd2) {
        sum += (*pVect1)/(*pVect2) - ((*pVectLog1) - (*pVectLog2)); ++pVect1; ++pVect2; ++pVectLog1; ++pVectLog2;
    }

    return sum - qty;
}

template float  ItakuraSaitoPrecomp<float>(const float* pVect1, const float* pVect2, size_t qty);


/* 
 * On new architectures unaligned loads are almost as fast as aligned ones. 
 * Ensuring that both pVect1 and pVect2 are similarly aligned could be hard.
 */
template <>
float ItakuraSaitoPrecompSIMD(const float* pVect1, const float* pVect2, size_t qty)
{
#ifndef PORTABLE_SSE2
#pragma message WARN("ItakuraSaitoPrecompSIMD<float>: SSE2 is not available, defaulting to pure C++ implementation!")
    return ItakuraSaitoPrecomp(pVect1, pVect2, qty);
#else
    size_t qty4  = qty/4;
    size_t qty16 = qty/16;

    const float* pEnd1 = pVect1 + 16 * qty16;
    const float* pEnd2 = pVect1 + 4  * qty4;
    const float* pEnd3 = pVect1 + qty;

    const float* pVectLog1 = pVect1 + qty;
    const float* pVectLog2 = pVect2 + qty;

    __m128  v1, v2, vLog1, vLog2;
    __m128  sum = _mm_set1_ps(0); 

    while (pVect1 < pEnd1) {
        v1      = _mm_loadu_ps(pVect1);     pVect1 += 4;
        v2      = _mm_loadu_ps(pVect2);     pVect2 += 4;
        vLog1   = _mm_loadu_ps(pVectLog1);  pVectLog1 += 4;
        vLog2   = _mm_loadu_ps(pVectLog2);  pVectLog2 += 4;
        sum  = _mm_add_ps(sum, _mm_sub_ps(_mm_div_ps(v1, v2), _mm_sub_ps(vLog1, vLog2)));

        v1      = _mm_loadu_ps(pVect1);     pVect1 += 4;
        v2      = _mm_loadu_ps(pVect2);     pVect2 += 4;
        vLog1   = _mm_loadu_ps(pVectLog1);  pVectLog1 += 4;
        vLog2   = _mm_loadu_ps(pVectLog2);  pVectLog2 += 4;
        sum  = _mm_add_ps(sum, _mm_sub_ps(_mm_div_ps(v1, v2), _mm_sub_ps(vLog1, vLog2)));

        v1      = _mm_loadu_ps(pVect1);     pVect1 += 4;
        v2      = _mm_loadu_ps(pVect2);     pVect2 += 4;
        vLog1   = _mm_loadu_ps(pVectLog1);  pVectLog1 += 4;
        vLog2   = _mm_loadu_ps(pVectLog2);  pVectLog2 += 4;
        sum  = _mm_add_ps(sum, _mm_sub_ps(_mm_div_ps(v1, v2), _mm_sub_ps(vLog1, vLog2)));

        v1      = _mm_loadu_ps(pVect1);     pVect1 += 4;
        v2      = _mm_loadu_ps(pVect2);     pVect2 += 4;
        vLog1   = _mm_loadu_ps(pVectLog1);  pVectLog1 += 4;
        vLog2   = _mm_loadu_ps(pVectLog2);  pVectLog2 += 4;
        sum  = _mm_add_ps(sum, _mm_sub_ps(_mm_div_ps(v1, v2), _mm_sub_ps(vLog1, vLog2)));

    }

    while (pVect1 < pEnd2) {
        v1      = _mm_loadu_ps(pVect1);     pVect1 += 4;
        v2      = _mm_loadu_ps(pVect2);     pVect2 += 4;
        vLog1   = _mm_loadu_ps(pVectLog1);  pVectLog1 += 4;
        vLog2   = _mm_loadu_ps(pVectLog2);  pVectLog2 += 4;
        sum  = _mm_add_ps(sum, _mm_sub_ps(_mm_div_ps(v1, v2), _mm_sub_ps(vLog1, vLog2)));

    }

    float PORTABLE_ALIGN16 TmpRes[4];

    _mm_store_ps(TmpRes, sum);
    float res= TmpRes[0] + TmpRes[1] + TmpRes[2] + TmpRes[3];

    while (pVect1 < pEnd3) {
        res += (*pVect1)/(*pVect2) - ((*pVectLog1) - (*pVectLog2)); ++pVect1; ++pVect2; ++pVectLog1; ++pVectLog2;
    }

    return res - qty;
#endif
}

template float  ItakuraSaitoPrecompSIMD<float>(const float* pVect1, const float* pVect2, size_t qty);


/*
 * KL-divergence
 */

template <class T> T KLStandard(const T *pVect1, const T *pVect2, size_t qty) 
{ 
    T sum = 0;

    for (size_t i = 0; i < qty; i++) { 
      sum += pVect1[i] * log(pVect1[i]/pVect2[i]);
    }

    return sum;
}

template float KLStandard<float>(const float *pVect1, const float *pVect2, size_t qty); 

template <class T> T KLStandardLogDiff(const T *p1, const T *p2, size_t qty) 
{ 
    T sum = 0;

    for (size_t i = 0; i < qty; i++) { 
      sum += p1[i] * (log(p1[i])-log(p2[i]));
    }

    return sum;
}

template float KLStandardLogDiff<float>(const float *p1, const float *p2, size_t qty);


template <class T> 
T KLPrecomp(const T* pVect1, const T* pVect2, size_t qty) {
    T sum = 0;

    size_t qty4 = qty/4;
    const T* pEnd1 = pVect1 + qty4 * 4;
    const T* pEnd2 = pVect1 + qty;

    const T* pVectLog1 = pVect1 + qty;
    const T* pVectLog2 = pVect2 + qty;

    while (pVect1 < pEnd1) {
        sum += (*pVect1++) * ((*pVectLog1++) - (*pVectLog2++));
        sum += (*pVect1++) * ((*pVectLog1++) - (*pVectLog2++));
        sum += (*pVect1++) * ((*pVectLog1++) - (*pVectLog2++));
        sum += (*pVect1++) * ((*pVectLog1++) - (*pVectLog2++));
    }

    while (pVect1 < pEnd2) {
        sum += (*pVect1++) * ((*pVectLog1++) - (*pVectLog2++));
    }

    return sum;
}

template float KLPrecomp<float>(const float* pVect1, const float* pVect2, size_t qty);

/* 
 * On new architectures unaligned loads are almost as fast as aligned ones. 
 * Ensuring that both pVect1 and pVect2 are similarly aligned could be hard.
 */
template <>
float KLPrecompSIMD(const float* pVect1, const float* pVect2, size_t qty)
{
#ifndef PORTABLE_SSE2
#pragma message WARN("KLPrecompSIMD<float>: SSE2 is not available, defaulting to pure C++ implementation!")
    return KLPrecomp(pVect1, pVect2, qty);
#else
    size_t qty4  = qty/4;
    size_t qty16 = qty/16;

    const float* pEnd1 = pVect1 + 16 * qty16;
    const float* pEnd2 = pVect1 + 4  * qty4;
    const float* pEnd3 = pVect1 + qty;

    const float* pVectLog1 = pVect1 + qty;
    const float* pVectLog2 = pVect2 + qty;

    __m128  v1, vLog1, vLog2;
    __m128  sum = _mm_set1_ps(0); 

    while (pVect1 < pEnd1) {
        v1      = _mm_loadu_ps(pVect1);     pVect1 += 4;
        vLog1   = _mm_loadu_ps(pVectLog1);  pVectLog1 += 4;
        vLog2   = _mm_loadu_ps(pVectLog2);  pVectLog2 += 4;
        sum  = _mm_add_ps(sum, _mm_mul_ps(v1, _mm_sub_ps(vLog1, vLog2)));

        v1      = _mm_loadu_ps(pVect1);     pVect1 += 4;
        vLog1   = _mm_loadu_ps(pVectLog1);  pVectLog1 += 4;
        vLog2   = _mm_loadu_ps(pVectLog2);  pVectLog2 += 4;
        sum  = _mm_add_ps(sum, _mm_mul_ps(v1, _mm_sub_ps(vLog1, vLog2)));

        v1      = _mm_loadu_ps(pVect1);     pVect1 += 4;
        vLog1   = _mm_loadu_ps(pVectLog1);  pVectLog1 += 4;
        vLog2   = _mm_loadu_ps(pVectLog2);  pVectLog2 += 4;
        sum  = _mm_add_ps(sum, _mm_mul_ps(v1, _mm_sub_ps(vLog1, vLog2)));

        v1      = _mm_loadu_ps(pVect1);     pVect1 += 4;
        vLog1   = _mm_loadu_ps(pVectLog1);  pVectLog1 += 4;
        vLog2   = _mm_loadu_ps(pVectLog2);  pVectLog2 += 4;
        sum  = _mm_add_ps(sum, _mm_mul_ps(v1, _mm_sub_ps(vLog1, vLog2)));
    }

    while (pVect1 < pEnd2) {
        v1      = _mm_loadu_ps(pVect1);     pVect1 += 4;
        vLog1   = _mm_loadu_ps(pVectLog1);  pVectLog1 += 4;
        vLog2   = _mm_loadu_ps(pVectLog2);  pVectLog2 += 4;
        sum  = _mm_add_ps(sum, _mm_mul_ps(v1, _mm_sub_ps(vLog1, vLog2)));
    }

    float PORTABLE_ALIGN16 TmpRes[4];

    _mm_store_ps(TmpRes, sum);
    float res= TmpRes[0] + TmpRes[1] + TmpRes[2] + TmpRes[3];

    while (pVect1 < pEnd3) {
        res += (*pVect1++) * ((*pVectLog1++) - (*pVectLog2++));
    }

    return res;
#endif
}

template float KLPrecompSIMD<float>(const float* pVect1, const float* pVect2, size_t qty);

template <class T> T KLGeneralStandard(const T *pVect1, const T *pVect2, size_t qty) 
{ 
    T sum = 0;

    for (size_t i = 0; i < qty; i++) { 
      sum += pVect1[i] * log(pVect1[i]/pVect2[i]) + pVect2[i] - pVect1[i];
    }

    return sum;
}

/*
 * Generalized KL-divergence
 */

template float KLGeneralStandard<float>(const float *pVect1, const float *pVect2, size_t qty); 

template <class T> 
T KLGeneralPrecomp(const T* pVect1, const T* pVect2, size_t qty) {
    T sum = 0;

    size_t qty4 = qty/4;
    const T* pEnd1 = pVect1 + qty4 * 4;
    const T* pEnd2 = pVect1 + qty;

    const T* pVectLog1 = pVect1 + qty;
    const T* pVectLog2 = pVect2 + qty;

    while (pVect1 < pEnd1) {
        sum += (*pVect1) * (*pVectLog1 - *pVectLog2) + *pVect2 - *pVect1; ++pVect1; ++pVect2; ++pVectLog1; ++pVectLog2;
        sum += (*pVect1) * (*pVectLog1 - *pVectLog2) + *pVect2 - *pVect1; ++pVect1; ++pVect2; ++pVectLog1; ++pVectLog2;
        sum += (*pVect1) * (*pVectLog1 - *pVectLog2) + *pVect2 - *pVect1; ++pVect1; ++pVect2; ++pVectLog1; ++pVectLog2;
        sum += (*pVect1) * (*pVectLog1 - *pVectLog2) + *pVect2 - *pVect1; ++pVect1; ++pVect2; ++pVectLog1; ++pVectLog2;
    }

    while (pVect1 < pEnd2) {
        sum += (*pVect1) * (*pVectLog1 - *pVectLog2) + *pVect2 - *pVect1; ++pVect1; ++pVect2; ++pVectLog1; ++pVectLog2;
    }

    return sum;
}

template float KLGeneralPrecomp<float>(const float* pVect1, const float* pVect2, size_t qty);

/* 
 * On new architectures unaligned loads are almost as fast as aligned ones. 
 * Ensuring that both pVect1 and pVect2 are similarly aligned could be hard.
 */
template <>
float KLGeneralPrecompSIMD(const float* pVect1, const float* pVect2, size_t qty)
{
#ifndef PORTABLE_SSE2
#pragma message WARN("KLGeneralPrecompSIMD<float>: SSE2 is not available, defaulting to pure C++ implementation!")
    return KLGeneralPrecomp(pVect1, pVect2, qty);
#else
    size_t qty4  = qty/4;
    size_t qty16 = qty/16;

    const float* pEnd1 = pVect1 + 16 * qty16;
    const float* pEnd2 = pVect1 + 4  * qty4;
    const float* pEnd3 = pVect1 + qty;

    const float* pVectLog1 = pVect1 + qty;
    const float* pVectLog2 = pVect2 + qty;

    __m128  v1, v2, vLog1, vLog2;
    __m128  sum = _mm_set1_ps(0); 

    while (pVect1 < pEnd1) {
        v1      = _mm_loadu_ps(pVect1);     pVect1 += 4;
        v2      = _mm_loadu_ps(pVect2);     pVect2 += 4;
        vLog1   = _mm_loadu_ps(pVectLog1);  pVectLog1 += 4;
        vLog2   = _mm_loadu_ps(pVectLog2);  pVectLog2 += 4;
        sum  = _mm_add_ps(_mm_add_ps(sum, _mm_mul_ps(v1, _mm_sub_ps(vLog1, vLog2))), _mm_sub_ps(v2, v1));

        v1      = _mm_loadu_ps(pVect1);     pVect1 += 4;
        v2      = _mm_loadu_ps(pVect2);     pVect2 += 4;
        vLog1   = _mm_loadu_ps(pVectLog1);  pVectLog1 += 4;
        vLog2   = _mm_loadu_ps(pVectLog2);  pVectLog2 += 4;
        sum  = _mm_add_ps(_mm_add_ps(sum, _mm_mul_ps(v1, _mm_sub_ps(vLog1, vLog2))), _mm_sub_ps(v2, v1));

        v1      = _mm_loadu_ps(pVect1);     pVect1 += 4;
        v2      = _mm_loadu_ps(pVect2);     pVect2 += 4;
        vLog1   = _mm_loadu_ps(pVectLog1);  pVectLog1 += 4;
        vLog2   = _mm_loadu_ps(pVectLog2);  pVectLog2 += 4;
        sum  = _mm_add_ps(_mm_add_ps(sum, _mm_mul_ps(v1, _mm_sub_ps(vLog1, vLog2))), _mm_sub_ps(v2, v1));

        v1      = _mm_loadu_ps(pVect1);     pVect1 += 4;
        v2      = _mm_loadu_ps(pVect2);     pVect2 += 4;
        vLog1   = _mm_loadu_ps(pVectLog1);  pVectLog1 += 4;
        vLog2   = _mm_loadu_ps(pVectLog2);  pVectLog2 += 4;
        sum  = _mm_add_ps(_mm_add_ps(sum, _mm_mul_ps(v1, _mm_sub_ps(vLog1, vLog2))), _mm_sub_ps(v2, v1));

    }

    while (pVect1 < pEnd2) {
        v1      = _mm_loadu_ps(pVect1);     pVect1 += 4;
        v2      = _mm_loadu_ps(pVect2);     pVect2 += 4;
        vLog1   = _mm_loadu_ps(pVectLog1);  pVectLog1 += 4;
        vLog2   = _mm_loadu_ps(pVectLog2);  pVectLog2 += 4;
        sum  = _mm_add_ps(_mm_add_ps(sum, _mm_mul_ps(v1, _mm_sub_ps(vLog1, vLog2))), _mm_sub_ps(v2, v1));

    }

    float PORTABLE_ALIGN16 TmpRes[4];

    _mm_store_ps(TmpRes, sum);
    float res= TmpRes[0] + TmpRes[1] + TmpRes[2] + TmpRes[3];

    while (pVect1 < pEnd3) {
        res += (*pVect1) * ((*pVectLog1++) - (*pVectLog2++)) + (*pVect2) - (*pVect1);
        ++pVect1; ++pVect2;
    }

    return res;
#endif
}

template float KLGeneralPrecompSIMD<float>(const float* pVect1, const float* pVect2, size_t qty);

}
