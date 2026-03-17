/*********************************************************************
  Blosc - Blocked Shuffling and Compression Library

  Author: Francesc Alted <francesc@blosc.org>

  See LICENSE.txt for details about copyright and rights to use.
**********************************************************************/

#ifndef SHUFFLE_COMMON_H
#define SHUFFLE_COMMON_H

#include "blosc-export.h"
#include <string.h>

#ifdef __GNUC__
#define BLOSC_GCC_VERSION (__GNUC__ * 100 + __GNUC_MINOR__)
#endif  // __GNUC__

/* Import standard integer type definitions */
#if defined(_WIN32) && !defined(__MINGW32__)
  /* stdint.h only available in VS2010 (VC++ 16.0) and newer */
  #if defined(_MSC_VER) && _MSC_VER < 1600
    #include "win32/stdint-windows.h"
  #else
    #include <stdint.h>
  #endif
#else
  #include <stdint.h>
#endif  /* _WIN32 */


/* Define the __SSE2__ symbol if compiling with Visual C++ and
   targeting the minimum architecture level supporting SSE2.
   Other compilers define this as expected and emit warnings
   when it is re-defined. */
#if !defined(__SSE2__) && defined(_MSC_VER) && \
    (defined(_M_X64) || (defined(_M_IX86) && _M_IX86_FP >= 2))
  #define __SSE2__
#endif

/*
 * Detect if the architecture is fine with unaligned access.
 */
#if !defined(BLOSC_STRICT_ALIGN)
#define BLOSC_STRICT_ALIGN
#if defined(__i386__) || defined(__386) || defined (__amd64)  /* GNU C, Sun Studio */
#undef BLOSC_STRICT_ALIGN
#elif defined(__i486__) || defined(__i586__) || defined(__i686__)  /* GNU C */
#undef BLOSC_STRICT_ALIGN
#elif defined(_M_IX86) || defined(_M_X64)   /* Intel, MSVC */
#undef BLOSC_STRICT_ALIGN
#elif defined(__386)
#undef BLOSC_STRICT_ALIGN
#elif defined(_X86_) /* MinGW */
#undef BLOSC_STRICT_ALIGN
#elif defined(__I86__) /* Digital Mars */
#undef BLOSC_STRICT_ALIGN
/* Seems like unaligned access in ARM (at least ARMv6) is pretty
   expensive, so we are going to always enforce strict alignment in ARM.
   If anybody suggest that newer ARMs are better, we can revisit this. */
/* #elif defined(__ARM_FEATURE_UNALIGNED) */  /* ARM, GNU C */
/* #undef BLOSC_STRICT_ALIGN */
#elif defined(_ARCH_PPC) || defined(__PPC__)
/* Modern PowerPC systems (like POWER8) should support unaligned access
   quite efficiently. */
#undef BLOSC_STRICT_ALIGN
#endif
#endif

#if defined(__SSE2__)
  #include <emmintrin.h>
#endif
#if defined(__AVX2__)
  #include <immintrin.h>
#endif

#endif  /* SHUFFLE_COMMON_H */
