/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Steve Borho <steve@borho.org>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02111, USA.
 *
 * This program is also available under a commercial proprietary license.
 * For more information, contact us at license @ x265.com.
 *****************************************************************************/

#include "primitives.h"
#include "x265.h"

/* The #if logic here must match the file lists in CMakeLists.txt */
#if X265_ARCH_X86
#if defined(__INTEL_COMPILER)
#define HAVE_SSE3
#define HAVE_SSSE3
#define HAVE_SSE4
#define HAVE_AVX2
#elif defined(__GNUC__)
#define GCC_VERSION (__GNUC__ * 10000 + __GNUC_MINOR__ * 100 + __GNUC_PATCHLEVEL__)
#if __clang__ || GCC_VERSION >= 40300 /* gcc_version >= gcc-4.3.0 */
#define HAVE_SSE3
#define HAVE_SSSE3
#define HAVE_SSE4
#endif
#if __clang__ || GCC_VERSION >= 40700 /* gcc_version >= gcc-4.7.0 */
#define HAVE_AVX2
#endif
#elif defined(_MSC_VER)
#define HAVE_SSE3
#define HAVE_SSSE3
#define HAVE_SSE4
#if _MSC_VER >= 1700 // VC11
#define HAVE_AVX2
#endif
#endif // compiler checks
#endif // if X265_ARCH_X86

namespace X265_NS {
// private x265 namespace

void setupIntrinsicDCT_sse3(EncoderPrimitives&);
void setupIntrinsicDCT_ssse3(EncoderPrimitives&);
void setupIntrinsicDCT_sse41(EncoderPrimitives&);

/* Use primitives for the best available vector architecture */
void setupInstrinsicPrimitives(EncoderPrimitives &p, int cpuMask)
{
#ifdef HAVE_SSE3
    if (cpuMask & X265_CPU_SSE3)
    {
        setupIntrinsicDCT_sse3(p);
    }
#endif
#ifdef HAVE_SSSE3
    if (cpuMask & X265_CPU_SSSE3)
    {
        setupIntrinsicDCT_ssse3(p);
    }
#endif
#ifdef HAVE_SSE4
    if (cpuMask & X265_CPU_SSE4)
    {
        setupIntrinsicDCT_sse41(p);
    }
#endif
    (void)p;
    (void)cpuMask;
}
}
