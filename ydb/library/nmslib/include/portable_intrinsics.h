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
#pragma once

#include <portable_popcount.h>
#include <portable_align.h>

// On 64-bit platforms SSE2 is always present, but Windows doesn't set SSE2 flag
// http://stackoverflow.com/questions/1067630/sse2-option-in-visual-c-x64
// Moreover: let's be careful about Windows-ARM
#if (defined(__SSE2__) || defined(__AVX__) || defined(_MSC_VER)) && \
    !(defined(_M_ARM64) || defined(_M_ARM))
#define PORTABLE_SSE2
#endif

// For Windows+MSVC there's no __ARM_NEON so let's define it ourselves on ARM64
#if defined(_MSC_VER)
  #if defined(_M_ARM64) && !defined(__ARM_NEON)
    #define __ARM_NEON 1
  #endif
#endif

// Unfortunately on Win32/64, windows does not define SSE4
#if defined(__SSE4_2__) || defined(__AVX__)
#define PORTABLE_SSE4
#endif

#if defined(__AVX__)
#define PORTABLE_AVX
#endif

#if defined(__AVX2__)
#define PORTABLE_AVX2
#endif

#if defined(__ARM_NEON)
#define PORTABLE_NEON
#endif

#if defined(PORTABLE_SSE2)
#include <portable_simd.h>
#endif

#if defined(PORTABLE_SSE4) 
/*
 * Based on explanations/suggestions from here
 * http://stackoverflow.com/questions/5526658/intel-sse-why-does-mm-extract-ps-return-int-instead-of-float
 */

#define MM_EXTRACT_DOUBLE(v,i) _mm_cvtsd_f64(_mm_shuffle_pd(v, v, _MM_SHUFFLE2(0, i)))
#define MM_EXTRACT_FLOAT(v,i) _mm_cvtss_f32(_mm_shuffle_ps(v, v, _MM_SHUFFLE(0, 0, 0, i)))


 /*
 * However, if we need to extract many numbers to sum the up, it is more efficient no
 * not to use the above MM_EXTRACT_FLOAT (https://github.com/searchivarius/BlogCode/tree/master/2016/bench_sums)
 *
 */
#endif
