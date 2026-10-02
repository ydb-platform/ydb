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
#pragma once

#if defined(_MSC_VER) && (defined(_M_IX86) || defined(_M_X64))
#include <intrin.h>
#define PREFETCH(a,sel) _mm_prefetch(a, sel)
#elif defined(__x86_64__) || defined(__i386__)
#include <mmintrin.h>
#define PREFETCH(a,sel) _mm_prefetch(a, sel)
#elif defined(__GNUC__)
#define PREFETCH(a,sel) __builtin_prefetch(a, 0, 0)
#else
#define PREFETCH(a,sel) do {} while (0)
#endif
