/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Steve Borho <steve@borho.org>
 *          Praveen Kumar Tiwari <praveen@multicorewareinc.com>
 *          Min Chen <chenm003@163.com> <min.chen@multicorewareinc.com>
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

#include "common.h"
#include "primitives.h"
#include "x265.h"
#include "cpu.h"

#define FUNCDEF_TU(ret, name, cpu, ...) \
    ret PFX(name ## _4x4_ ## cpu(__VA_ARGS__)); \
    ret PFX(name ## _8x8_ ## cpu(__VA_ARGS__)); \
    ret PFX(name ## _16x16_ ## cpu(__VA_ARGS__)); \
    ret PFX(name ## _32x32_ ## cpu(__VA_ARGS__)); \
    ret PFX(name ## _64x64_ ## cpu(__VA_ARGS__))

#define FUNCDEF_TU_S(ret, name, cpu, ...) \
    ret PFX(name ## _4_ ## cpu(__VA_ARGS__)); \
    ret PFX(name ## _8_ ## cpu(__VA_ARGS__)); \
    ret PFX(name ## _16_ ## cpu(__VA_ARGS__)); \
    ret PFX(name ## _32_ ## cpu(__VA_ARGS__)); \
    ret PFX(name ## _64_ ## cpu(__VA_ARGS__))

#define FUNCDEF_TU_S2(ret, name, cpu, ...) \
    ret PFX(name ## 4_ ## cpu(__VA_ARGS__)); \
    ret PFX(name ## 8_ ## cpu(__VA_ARGS__)); \
    ret PFX(name ## 16_ ## cpu(__VA_ARGS__)); \
    ret PFX(name ## 32_ ## cpu(__VA_ARGS__)); \
    ret PFX(name ## 64_ ## cpu(__VA_ARGS__))

#define FUNCDEF_PU(ret, name, cpu, ...) \
    ret PFX(name ## _4x4_   ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _8x8_   ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _16x16_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _32x32_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _64x64_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _8x4_   ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _4x8_   ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _16x8_  ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _8x16_  ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _16x32_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _32x16_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _64x32_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _32x64_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _16x12_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _12x16_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _16x4_  ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _4x16_  ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _32x24_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _24x32_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _32x8_  ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _8x32_  ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _64x48_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _48x64_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _64x16_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _16x64_ ## cpu)(__VA_ARGS__)

#define FUNCDEF_CHROMA_PU(ret, name, cpu, ...) \
    FUNCDEF_PU(ret, name, cpu, __VA_ARGS__); \
    ret PFX(name ## _4x2_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _2x4_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _8x2_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _2x8_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _8x6_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _6x8_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _8x12_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _12x8_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _6x16_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _16x6_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _2x16_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _16x2_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _4x12_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _12x4_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _32x12_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _12x32_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _32x4_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _4x32_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _32x48_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _48x32_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _16x24_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _24x16_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _8x64_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _64x8_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _64x24_ ## cpu)(__VA_ARGS__); \
    ret PFX(name ## _24x64_ ## cpu)(__VA_ARGS__);

extern "C" {
#include "pixel.h"
#include "pixel-util.h"
#include "mc.h"
#include "ipfilter8.h"
#include "loopfilter.h"
#include "blockcopy8.h"
#include "intrapred.h"
#include "dct8.h"
#include "seaintegral.h"
}

#define ALL_LUMA_CU_TYPED(prim, fncdef, fname, cpu) \
    p.cu[BLOCK_8x8].prim   = fncdef PFX(fname ## _8x8_ ## cpu); \
    p.cu[BLOCK_16x16].prim = fncdef PFX(fname ## _16x16_ ## cpu); \
    p.cu[BLOCK_32x32].prim = fncdef PFX(fname ## _32x32_ ## cpu); \
    p.cu[BLOCK_64x64].prim = fncdef PFX(fname ## _64x64_ ## cpu)
#define ALL_LUMA_CU_TYPED_S(prim, fncdef, fname, cpu) \
    p.cu[BLOCK_8x8].prim   = fncdef PFX(fname ## 8_ ## cpu); \
    p.cu[BLOCK_16x16].prim = fncdef PFX(fname ## 16_ ## cpu); \
    p.cu[BLOCK_32x32].prim = fncdef PFX(fname ## 32_ ## cpu); \
    p.cu[BLOCK_64x64].prim = fncdef PFX(fname ## 64_ ## cpu)
#define ALL_LUMA_TU_TYPED(prim, fncdef, fname, cpu) \
    p.cu[BLOCK_4x4].prim   = fncdef PFX(fname ## _4x4_ ## cpu); \
    p.cu[BLOCK_8x8].prim   = fncdef PFX(fname ## _8x8_ ## cpu); \
    p.cu[BLOCK_16x16].prim = fncdef PFX(fname ## _16x16_ ## cpu); \
    p.cu[BLOCK_32x32].prim = fncdef PFX(fname ## _32x32_ ## cpu)
#define ALL_LUMA_TU_TYPED_S(prim, fncdef, fname, cpu) \
    p.cu[BLOCK_4x4].prim   = fncdef PFX(fname ## 4_ ## cpu); \
    p.cu[BLOCK_8x8].prim   = fncdef PFX(fname ## 8_ ## cpu); \
    p.cu[BLOCK_16x16].prim = fncdef PFX(fname ## 16_ ## cpu); \
    p.cu[BLOCK_32x32].prim = fncdef PFX(fname ## 32_ ## cpu)
#define ALL_LUMA_BLOCKS_TYPED(prim, fncdef, fname, cpu) \
    p.cu[BLOCK_4x4].prim   = fncdef PFX(fname ## _4x4_ ## cpu); \
    p.cu[BLOCK_8x8].prim   = fncdef PFX(fname ## _8x8_ ## cpu); \
    p.cu[BLOCK_16x16].prim = fncdef PFX(fname ## _16x16_ ## cpu); \
    p.cu[BLOCK_32x32].prim = fncdef PFX(fname ## _32x32_ ## cpu); \
    p.cu[BLOCK_64x64].prim = fncdef PFX(fname ## _64x64_ ## cpu);
#define ALL_LUMA_CU(prim, fname, cpu)      ALL_LUMA_CU_TYPED(prim, , fname, cpu)
#define ALL_LUMA_CU_S(prim, fname, cpu)    ALL_LUMA_CU_TYPED_S(prim, , fname, cpu)
#define ALL_LUMA_TU(prim, fname, cpu)      ALL_LUMA_TU_TYPED(prim, , fname, cpu)
#define ALL_LUMA_BLOCKS(prim, fname, cpu)  ALL_LUMA_BLOCKS_TYPED(prim, , fname, cpu)
#define ALL_LUMA_TU_S(prim, fname, cpu)    ALL_LUMA_TU_TYPED_S(prim, , fname, cpu)

#define ALL_LUMA_PU_TYPED(prim, fncdef, fname, cpu) \
    p.pu[LUMA_8x8].prim   = fncdef PFX(fname ## _8x8_ ## cpu); \
    p.pu[LUMA_16x16].prim = fncdef PFX(fname ## _16x16_ ## cpu); \
    p.pu[LUMA_32x32].prim = fncdef PFX(fname ## _32x32_ ## cpu); \
    p.pu[LUMA_64x64].prim = fncdef PFX(fname ## _64x64_ ## cpu); \
    p.pu[LUMA_8x4].prim   = fncdef PFX(fname ## _8x4_ ## cpu); \
    p.pu[LUMA_4x8].prim   = fncdef PFX(fname ## _4x8_ ## cpu); \
    p.pu[LUMA_16x8].prim  = fncdef PFX(fname ## _16x8_ ## cpu); \
    p.pu[LUMA_8x16].prim  = fncdef PFX(fname ## _8x16_ ## cpu); \
    p.pu[LUMA_16x32].prim = fncdef PFX(fname ## _16x32_ ## cpu); \
    p.pu[LUMA_32x16].prim = fncdef PFX(fname ## _32x16_ ## cpu); \
    p.pu[LUMA_64x32].prim = fncdef PFX(fname ## _64x32_ ## cpu); \
    p.pu[LUMA_32x64].prim = fncdef PFX(fname ## _32x64_ ## cpu); \
    p.pu[LUMA_16x12].prim = fncdef PFX(fname ## _16x12_ ## cpu); \
    p.pu[LUMA_12x16].prim = fncdef PFX(fname ## _12x16_ ## cpu); \
    p.pu[LUMA_16x4].prim  = fncdef PFX(fname ## _16x4_ ## cpu); \
    p.pu[LUMA_4x16].prim  = fncdef PFX(fname ## _4x16_ ## cpu); \
    p.pu[LUMA_32x24].prim = fncdef PFX(fname ## _32x24_ ## cpu); \
    p.pu[LUMA_24x32].prim = fncdef PFX(fname ## _24x32_ ## cpu); \
    p.pu[LUMA_32x8].prim  = fncdef PFX(fname ## _32x8_ ## cpu); \
    p.pu[LUMA_8x32].prim  = fncdef PFX(fname ## _8x32_ ## cpu); \
    p.pu[LUMA_64x48].prim = fncdef PFX(fname ## _64x48_ ## cpu); \
    p.pu[LUMA_48x64].prim = fncdef PFX(fname ## _48x64_ ## cpu); \
    p.pu[LUMA_64x16].prim = fncdef PFX(fname ## _64x16_ ## cpu); \
    p.pu[LUMA_16x64].prim = fncdef PFX(fname ## _16x64_ ## cpu)
#define ALL_LUMA_PU(prim, fname, cpu) ALL_LUMA_PU_TYPED(prim, , fname, cpu)

#define ALL_LUMA_PU_T(prim, fname) \
    p.pu[LUMA_8x8].prim   = fname<LUMA_8x8>; \
    p.pu[LUMA_16x16].prim = fname<LUMA_16x16>; \
    p.pu[LUMA_32x32].prim = fname<LUMA_32x32>; \
    p.pu[LUMA_64x64].prim = fname<LUMA_64x64>; \
    p.pu[LUMA_8x4].prim   = fname<LUMA_8x4>; \
    p.pu[LUMA_4x8].prim   = fname<LUMA_4x8>; \
    p.pu[LUMA_16x8].prim  = fname<LUMA_16x8>; \
    p.pu[LUMA_8x16].prim  = fname<LUMA_8x16>; \
    p.pu[LUMA_16x32].prim = fname<LUMA_16x32>; \
    p.pu[LUMA_32x16].prim = fname<LUMA_32x16>; \
    p.pu[LUMA_64x32].prim = fname<LUMA_64x32>; \
    p.pu[LUMA_32x64].prim = fname<LUMA_32x64>; \
    p.pu[LUMA_16x12].prim = fname<LUMA_16x12>; \
    p.pu[LUMA_12x16].prim = fname<LUMA_12x16>; \
    p.pu[LUMA_16x4].prim  = fname<LUMA_16x4>; \
    p.pu[LUMA_4x16].prim  = fname<LUMA_4x16>; \
    p.pu[LUMA_32x24].prim = fname<LUMA_32x24>; \
    p.pu[LUMA_24x32].prim = fname<LUMA_24x32>; \
    p.pu[LUMA_32x8].prim  = fname<LUMA_32x8>; \
    p.pu[LUMA_8x32].prim  = fname<LUMA_8x32>; \
    p.pu[LUMA_64x48].prim = fname<LUMA_64x48>; \
    p.pu[LUMA_48x64].prim = fname<LUMA_48x64>; \
    p.pu[LUMA_64x16].prim = fname<LUMA_64x16>; \
    p.pu[LUMA_16x64].prim = fname<LUMA_16x64>

#define ALL_CHROMA_420_CU_TYPED(prim, fncdef, fname, cpu) \
    p.chroma[X265_CSP_I420].cu[BLOCK_420_4x4].prim   = fncdef PFX(fname ## _4x4_ ## cpu); \
    p.chroma[X265_CSP_I420].cu[BLOCK_420_8x8].prim   = fncdef PFX(fname ## _8x8_ ## cpu); \
    p.chroma[X265_CSP_I420].cu[BLOCK_420_16x16].prim = fncdef PFX(fname ## _16x16_ ## cpu); \
    p.chroma[X265_CSP_I420].cu[BLOCK_420_32x32].prim = fncdef PFX(fname ## _32x32_ ## cpu)
#define ALL_CHROMA_420_CU_TYPED_S(prim, fncdef, fname, cpu) \
    p.chroma[X265_CSP_I420].cu[BLOCK_420_4x4].prim   = fncdef PFX(fname ## _4_ ## cpu); \
    p.chroma[X265_CSP_I420].cu[BLOCK_420_8x8].prim   = fncdef PFX(fname ## _8_ ## cpu); \
    p.chroma[X265_CSP_I420].cu[BLOCK_420_16x16].prim = fncdef PFX(fname ## _16_ ## cpu); \
    p.chroma[X265_CSP_I420].cu[BLOCK_420_32x32].prim = fncdef PFX(fname ## _32_ ## cpu)
#define ALL_CHROMA_420_CU(prim, fname, cpu) ALL_CHROMA_420_CU_TYPED(prim, , fname, cpu)
#define ALL_CHROMA_420_CU_S(prim, fname, cpu) ALL_CHROMA_420_CU_TYPED_S(prim, , fname, cpu)

#define ALL_CHROMA_420_PU_TYPED(prim, fncdef, fname, cpu) \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_4x4].prim   = fncdef PFX(fname ## _4x4_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_8x8].prim   = fncdef PFX(fname ## _8x8_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_16x16].prim = fncdef PFX(fname ## _16x16_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_32x32].prim = fncdef PFX(fname ## _32x32_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_4x2].prim   = fncdef PFX(fname ## _4x2_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_2x4].prim   = fncdef PFX(fname ## _2x4_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_8x4].prim   = fncdef PFX(fname ## _8x4_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_4x8].prim   = fncdef PFX(fname ## _4x8_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_16x8].prim  = fncdef PFX(fname ## _16x8_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_8x16].prim  = fncdef PFX(fname ## _8x16_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_32x16].prim = fncdef PFX(fname ## _32x16_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_16x32].prim = fncdef PFX(fname ## _16x32_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_8x6].prim   = fncdef PFX(fname ## _8x6_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_6x8].prim   = fncdef PFX(fname ## _6x8_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_8x2].prim   = fncdef PFX(fname ## _8x2_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_2x8].prim   = fncdef PFX(fname ## _2x8_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_16x12].prim = fncdef PFX(fname ## _16x12_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_12x16].prim = fncdef PFX(fname ## _12x16_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_16x4].prim  = fncdef PFX(fname ## _16x4_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_4x16].prim  = fncdef PFX(fname ## _4x16_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_32x24].prim = fncdef PFX(fname ## _32x24_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_24x32].prim = fncdef PFX(fname ## _24x32_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_32x8].prim  = fncdef PFX(fname ## _32x8_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_8x32].prim  = fncdef PFX(fname ## _8x32_ ## cpu)
#define ALL_CHROMA_420_PU(prim, fname, cpu) ALL_CHROMA_420_PU_TYPED(prim, , fname, cpu)

#define ALL_CHROMA_420_4x4_PU_TYPED(prim, fncdef, fname, cpu) \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_4x4].prim   = fncdef PFX(fname ## _4x4_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_8x8].prim   = fncdef PFX(fname ## _8x8_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_16x16].prim = fncdef PFX(fname ## _16x16_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_32x32].prim = fncdef PFX(fname ## _32x32_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_8x4].prim   = fncdef PFX(fname ## _8x4_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_4x8].prim   = fncdef PFX(fname ## _4x8_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_16x8].prim  = fncdef PFX(fname ## _16x8_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_8x16].prim  = fncdef PFX(fname ## _8x16_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_32x16].prim = fncdef PFX(fname ## _32x16_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_16x32].prim = fncdef PFX(fname ## _16x32_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_16x12].prim = fncdef PFX(fname ## _16x12_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_12x16].prim = fncdef PFX(fname ## _12x16_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_16x4].prim  = fncdef PFX(fname ## _16x4_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_4x16].prim  = fncdef PFX(fname ## _4x16_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_32x24].prim = fncdef PFX(fname ## _32x24_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_24x32].prim = fncdef PFX(fname ## _24x32_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_32x8].prim  = fncdef PFX(fname ## _32x8_ ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_8x32].prim  = fncdef PFX(fname ## _8x32_ ## cpu)
#define ALL_CHROMA_420_4x4_PU(prim, fname, cpu) ALL_CHROMA_420_4x4_PU_TYPED(prim, , fname, cpu)

#define ALL_CHROMA_422_CU_TYPED(prim, fncdef, fname, cpu) \
    p.chroma[X265_CSP_I422].cu[BLOCK_422_4x8].prim   = fncdef PFX(fname ## _4x8_ ## cpu); \
    p.chroma[X265_CSP_I422].cu[BLOCK_422_8x16].prim  = fncdef PFX(fname ## _8x16_ ## cpu); \
    p.chroma[X265_CSP_I422].cu[BLOCK_422_16x32].prim = fncdef PFX(fname ## _16x32_ ## cpu); \
    p.chroma[X265_CSP_I422].cu[BLOCK_422_32x64].prim = fncdef PFX(fname ## _32x64_ ## cpu)
#define ALL_CHROMA_422_CU(prim, fname, cpu) ALL_CHROMA_422_CU_TYPED(prim, , fname, cpu)

#define ALL_CHROMA_422_PU_TYPED(prim, fncdef, fname, cpu) \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_4x8].prim   = fncdef PFX(fname ## _4x8_ ## cpu); \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_8x16].prim  = fncdef PFX(fname ## _8x16_ ## cpu); \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_16x32].prim = fncdef PFX(fname ## _16x32_ ## cpu); \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_32x64].prim = fncdef PFX(fname ## _32x64_ ## cpu); \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_4x4].prim   = fncdef PFX(fname ## _4x4_ ## cpu); \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_2x8].prim   = fncdef PFX(fname ## _2x8_ ## cpu); \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_8x8].prim   = fncdef PFX(fname ## _8x8_ ## cpu); \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_4x16].prim  = fncdef PFX(fname ## _4x16_ ## cpu); \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_16x16].prim = fncdef PFX(fname ## _16x16_ ## cpu); \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_8x32].prim  = fncdef PFX(fname ## _8x32_ ## cpu); \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_32x32].prim = fncdef PFX(fname ## _32x32_ ## cpu); \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_16x64].prim = fncdef PFX(fname ## _16x64_ ## cpu); \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_8x12].prim  = fncdef PFX(fname ## _8x12_ ## cpu); \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_6x16].prim  = fncdef PFX(fname ## _6x16_ ## cpu); \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_8x4].prim   = fncdef PFX(fname ## _8x4_ ## cpu); \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_2x16].prim  = fncdef PFX(fname ## _2x16_ ## cpu); \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_16x24].prim = fncdef PFX(fname ## _16x24_ ## cpu); \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_12x32].prim = fncdef PFX(fname ## _12x32_ ## cpu); \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_16x8].prim  = fncdef PFX(fname ## _16x8_ ## cpu); \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_4x32].prim  = fncdef PFX(fname ## _4x32_ ## cpu); \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_32x48].prim = fncdef PFX(fname ## _32x48_ ## cpu); \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_24x64].prim = fncdef PFX(fname ## _24x64_ ## cpu); \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_32x16].prim = fncdef PFX(fname ## _32x16_ ## cpu); \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_8x64].prim  = fncdef PFX(fname ## _8x64_ ## cpu)
#define ALL_CHROMA_422_PU(prim, fname, cpu) ALL_CHROMA_422_PU_TYPED(prim, , fname, cpu)

#define ALL_CHROMA_444_PU_TYPED(prim, fncdef, fname, cpu) \
    p.chroma[X265_CSP_I444].pu[LUMA_4x4].prim   = fncdef PFX(fname ## _4x4_ ## cpu); \
    p.chroma[X265_CSP_I444].pu[LUMA_8x8].prim   = fncdef PFX(fname ## _8x8_ ## cpu); \
    p.chroma[X265_CSP_I444].pu[LUMA_16x16].prim = fncdef PFX(fname ## _16x16_ ## cpu); \
    p.chroma[X265_CSP_I444].pu[LUMA_32x32].prim = fncdef PFX(fname ## _32x32_ ## cpu); \
    p.chroma[X265_CSP_I444].pu[LUMA_64x64].prim = fncdef PFX(fname ## _64x64_ ## cpu); \
    p.chroma[X265_CSP_I444].pu[LUMA_8x4].prim   = fncdef PFX(fname ## _8x4_ ## cpu); \
    p.chroma[X265_CSP_I444].pu[LUMA_4x8].prim   = fncdef PFX(fname ## _4x8_ ## cpu); \
    p.chroma[X265_CSP_I444].pu[LUMA_16x8].prim  = fncdef PFX(fname ## _16x8_ ## cpu); \
    p.chroma[X265_CSP_I444].pu[LUMA_8x16].prim  = fncdef PFX(fname ## _8x16_ ## cpu); \
    p.chroma[X265_CSP_I444].pu[LUMA_16x32].prim = fncdef PFX(fname ## _16x32_ ## cpu); \
    p.chroma[X265_CSP_I444].pu[LUMA_32x16].prim = fncdef PFX(fname ## _32x16_ ## cpu); \
    p.chroma[X265_CSP_I444].pu[LUMA_64x32].prim = fncdef PFX(fname ## _64x32_ ## cpu); \
    p.chroma[X265_CSP_I444].pu[LUMA_32x64].prim = fncdef PFX(fname ## _32x64_ ## cpu); \
    p.chroma[X265_CSP_I444].pu[LUMA_16x12].prim = fncdef PFX(fname ## _16x12_ ## cpu); \
    p.chroma[X265_CSP_I444].pu[LUMA_12x16].prim = fncdef PFX(fname ## _12x16_ ## cpu); \
    p.chroma[X265_CSP_I444].pu[LUMA_16x4].prim  = fncdef PFX(fname ## _16x4_ ## cpu); \
    p.chroma[X265_CSP_I444].pu[LUMA_4x16].prim  = fncdef PFX(fname ## _4x16_ ## cpu); \
    p.chroma[X265_CSP_I444].pu[LUMA_32x24].prim = fncdef PFX(fname ## _32x24_ ## cpu); \
    p.chroma[X265_CSP_I444].pu[LUMA_24x32].prim = fncdef PFX(fname ## _24x32_ ## cpu); \
    p.chroma[X265_CSP_I444].pu[LUMA_32x8].prim  = fncdef PFX(fname ## _32x8_ ## cpu); \
    p.chroma[X265_CSP_I444].pu[LUMA_8x32].prim  = fncdef PFX(fname ## _8x32_ ## cpu); \
    p.chroma[X265_CSP_I444].pu[LUMA_64x48].prim = fncdef PFX(fname ## _64x48_ ## cpu); \
    p.chroma[X265_CSP_I444].pu[LUMA_48x64].prim = fncdef PFX(fname ## _48x64_ ## cpu); \
    p.chroma[X265_CSP_I444].pu[LUMA_64x16].prim = fncdef PFX(fname ## _64x16_ ## cpu); \
    p.chroma[X265_CSP_I444].pu[LUMA_16x64].prim = fncdef PFX(fname ## _16x64_ ## cpu)
#define ALL_CHROMA_444_PU(prim, fname, cpu) ALL_CHROMA_444_PU_TYPED(prim, , fname, cpu)

#define AVC_LUMA_PU(name, cpu) \
    p.pu[LUMA_16x16].name = PFX(pixel_ ## name ## _16x16_ ## cpu); \
    p.pu[LUMA_16x8].name  = PFX(pixel_ ## name ## _16x8_ ## cpu); \
    p.pu[LUMA_8x16].name  = PFX(pixel_ ## name ## _8x16_ ## cpu); \
    p.pu[LUMA_8x8].name   = PFX(pixel_ ## name ## _8x8_ ## cpu); \
    p.pu[LUMA_8x4].name   = PFX(pixel_ ## name ## _8x4_ ## cpu); \
    p.pu[LUMA_4x8].name   = PFX(pixel_ ## name ## _4x8_ ## cpu); \
    p.pu[LUMA_4x4].name   = PFX(pixel_ ## name ## _4x4_ ## cpu); \
    p.pu[LUMA_4x16].name  = PFX(pixel_ ## name ## _4x16_ ## cpu)

#define HEVC_SAD(cpu) \
    p.pu[LUMA_8x32].sad  = PFX(pixel_sad_8x32_ ## cpu); \
    p.pu[LUMA_16x4].sad  = PFX(pixel_sad_16x4_ ## cpu); \
    p.pu[LUMA_16x12].sad = PFX(pixel_sad_16x12_ ## cpu); \
    p.pu[LUMA_16x32].sad = PFX(pixel_sad_16x32_ ## cpu); \
    p.pu[LUMA_16x64].sad = PFX(pixel_sad_16x64_ ## cpu); \
    p.pu[LUMA_32x8].sad  = PFX(pixel_sad_32x8_ ## cpu); \
    p.pu[LUMA_32x16].sad = PFX(pixel_sad_32x16_ ## cpu); \
    p.pu[LUMA_32x24].sad = PFX(pixel_sad_32x24_ ## cpu); \
    p.pu[LUMA_32x32].sad = PFX(pixel_sad_32x32_ ## cpu); \
    p.pu[LUMA_32x64].sad = PFX(pixel_sad_32x64_ ## cpu); \
    p.pu[LUMA_64x16].sad = PFX(pixel_sad_64x16_ ## cpu); \
    p.pu[LUMA_64x32].sad = PFX(pixel_sad_64x32_ ## cpu); \
    p.pu[LUMA_64x48].sad = PFX(pixel_sad_64x48_ ## cpu); \
    p.pu[LUMA_64x64].sad = PFX(pixel_sad_64x64_ ## cpu); \
    p.pu[LUMA_48x64].sad = PFX(pixel_sad_48x64_ ## cpu); \
    p.pu[LUMA_24x32].sad = PFX(pixel_sad_24x32_ ## cpu); \
    p.pu[LUMA_12x16].sad = PFX(pixel_sad_12x16_ ## cpu)

#define HEVC_SAD_X3(cpu) \
    p.pu[LUMA_16x8].sad_x3  = PFX(pixel_sad_x3_16x8_ ## cpu); \
    p.pu[LUMA_16x12].sad_x3 = PFX(pixel_sad_x3_16x12_ ## cpu); \
    p.pu[LUMA_16x16].sad_x3 = PFX(pixel_sad_x3_16x16_ ## cpu); \
    p.pu[LUMA_16x32].sad_x3 = PFX(pixel_sad_x3_16x32_ ## cpu); \
    p.pu[LUMA_16x64].sad_x3 = PFX(pixel_sad_x3_16x64_ ## cpu); \
    p.pu[LUMA_32x8].sad_x3  = PFX(pixel_sad_x3_32x8_ ## cpu); \
    p.pu[LUMA_32x16].sad_x3 = PFX(pixel_sad_x3_32x16_ ## cpu); \
    p.pu[LUMA_32x24].sad_x3 = PFX(pixel_sad_x3_32x24_ ## cpu); \
    p.pu[LUMA_32x32].sad_x3 = PFX(pixel_sad_x3_32x32_ ## cpu); \
    p.pu[LUMA_32x64].sad_x3 = PFX(pixel_sad_x3_32x64_ ## cpu); \
    p.pu[LUMA_24x32].sad_x3 = PFX(pixel_sad_x3_24x32_ ## cpu); \
    p.pu[LUMA_48x64].sad_x3 = PFX(pixel_sad_x3_48x64_ ## cpu); \
    p.pu[LUMA_64x16].sad_x3 = PFX(pixel_sad_x3_64x16_ ## cpu); \
    p.pu[LUMA_64x32].sad_x3 = PFX(pixel_sad_x3_64x32_ ## cpu); \
    p.pu[LUMA_64x48].sad_x3 = PFX(pixel_sad_x3_64x48_ ## cpu); \
    p.pu[LUMA_64x64].sad_x3 = PFX(pixel_sad_x3_64x64_ ## cpu)

#define HEVC_SAD_X4(cpu) \
    p.pu[LUMA_16x8].sad_x4  = PFX(pixel_sad_x4_16x8_ ## cpu); \
    p.pu[LUMA_16x12].sad_x4 = PFX(pixel_sad_x4_16x12_ ## cpu); \
    p.pu[LUMA_16x16].sad_x4 = PFX(pixel_sad_x4_16x16_ ## cpu); \
    p.pu[LUMA_16x32].sad_x4 = PFX(pixel_sad_x4_16x32_ ## cpu); \
    p.pu[LUMA_16x64].sad_x4 = PFX(pixel_sad_x4_16x64_ ## cpu); \
    p.pu[LUMA_32x8].sad_x4  = PFX(pixel_sad_x4_32x8_ ## cpu); \
    p.pu[LUMA_32x16].sad_x4 = PFX(pixel_sad_x4_32x16_ ## cpu); \
    p.pu[LUMA_32x24].sad_x4 = PFX(pixel_sad_x4_32x24_ ## cpu); \
    p.pu[LUMA_32x32].sad_x4 = PFX(pixel_sad_x4_32x32_ ## cpu); \
    p.pu[LUMA_32x64].sad_x4 = PFX(pixel_sad_x4_32x64_ ## cpu); \
    p.pu[LUMA_24x32].sad_x4 = PFX(pixel_sad_x4_24x32_ ## cpu); \
    p.pu[LUMA_48x64].sad_x4 = PFX(pixel_sad_x4_48x64_ ## cpu); \
    p.pu[LUMA_64x16].sad_x4 = PFX(pixel_sad_x4_64x16_ ## cpu); \
    p.pu[LUMA_64x32].sad_x4 = PFX(pixel_sad_x4_64x32_ ## cpu); \
    p.pu[LUMA_64x48].sad_x4 = PFX(pixel_sad_x4_64x48_ ## cpu); \
    p.pu[LUMA_64x64].sad_x4 = PFX(pixel_sad_x4_64x64_ ## cpu)

#define ASSIGN_SSE_PP(cpu) \
    p.cu[BLOCK_8x8].sse_pp   = PFX(pixel_ssd_8x8_ ## cpu); \
    p.cu[BLOCK_16x16].sse_pp = PFX(pixel_ssd_16x16_ ## cpu); \
    p.cu[BLOCK_32x32].sse_pp = PFX(pixel_ssd_32x32_ ## cpu); \
    p.chroma[X265_CSP_I422].cu[BLOCK_422_8x16].sse_pp = PFX(pixel_ssd_8x16_ ## cpu); \
    p.chroma[X265_CSP_I422].cu[BLOCK_422_16x32].sse_pp = PFX(pixel_ssd_16x32_ ## cpu); \
    p.chroma[X265_CSP_I422].cu[BLOCK_422_32x64].sse_pp = PFX(pixel_ssd_32x64_ ## cpu);

#define ASSIGN_SSE_SS(cpu) ALL_LUMA_BLOCKS(sse_ss, pixel_ssd_ss, cpu)

#define ASSIGN_SA8D(cpu) \
    ALL_LUMA_CU(sa8d, pixel_sa8d, cpu); \
    p.chroma[X265_CSP_I422].cu[BLOCK_422_8x16].sa8d = PFX(pixel_sa8d_8x16_ ## cpu); \
    p.chroma[X265_CSP_I422].cu[BLOCK_422_16x32].sa8d = PFX(pixel_sa8d_16x32_ ## cpu); \
    p.chroma[X265_CSP_I422].cu[BLOCK_422_32x64].sa8d = PFX(pixel_sa8d_32x64_ ## cpu)

#define PIXEL_AVG(cpu) \
    p.pu[LUMA_64x64].pixelavg_pp = PFX(pixel_avg_64x64_ ## cpu); \
    p.pu[LUMA_64x48].pixelavg_pp = PFX(pixel_avg_64x48_ ## cpu); \
    p.pu[LUMA_64x32].pixelavg_pp = PFX(pixel_avg_64x32_ ## cpu); \
    p.pu[LUMA_64x16].pixelavg_pp = PFX(pixel_avg_64x16_ ## cpu); \
    p.pu[LUMA_48x64].pixelavg_pp = PFX(pixel_avg_48x64_ ## cpu); \
    p.pu[LUMA_32x64].pixelavg_pp = PFX(pixel_avg_32x64_ ## cpu); \
    p.pu[LUMA_32x32].pixelavg_pp = PFX(pixel_avg_32x32_ ## cpu); \
    p.pu[LUMA_32x24].pixelavg_pp = PFX(pixel_avg_32x24_ ## cpu); \
    p.pu[LUMA_32x16].pixelavg_pp = PFX(pixel_avg_32x16_ ## cpu); \
    p.pu[LUMA_32x8].pixelavg_pp  = PFX(pixel_avg_32x8_ ## cpu); \
    p.pu[LUMA_24x32].pixelavg_pp = PFX(pixel_avg_24x32_ ## cpu); \
    p.pu[LUMA_16x64].pixelavg_pp = PFX(pixel_avg_16x64_ ## cpu); \
    p.pu[LUMA_16x32].pixelavg_pp = PFX(pixel_avg_16x32_ ## cpu); \
    p.pu[LUMA_16x16].pixelavg_pp = PFX(pixel_avg_16x16_ ## cpu); \
    p.pu[LUMA_16x12].pixelavg_pp = PFX(pixel_avg_16x12_ ## cpu); \
    p.pu[LUMA_16x8].pixelavg_pp  = PFX(pixel_avg_16x8_ ## cpu); \
    p.pu[LUMA_16x4].pixelavg_pp  = PFX(pixel_avg_16x4_ ## cpu); \
    p.pu[LUMA_12x16].pixelavg_pp = PFX(pixel_avg_12x16_ ## cpu); \
    p.pu[LUMA_8x32].pixelavg_pp  = PFX(pixel_avg_8x32_ ## cpu); \
    p.pu[LUMA_8x16].pixelavg_pp  = PFX(pixel_avg_8x16_ ## cpu); \
    p.pu[LUMA_8x8].pixelavg_pp   = PFX(pixel_avg_8x8_ ## cpu); \
    p.pu[LUMA_8x4].pixelavg_pp   = PFX(pixel_avg_8x4_ ## cpu);

#define PIXEL_AVG_W4(cpu) \
    p.pu[LUMA_4x4].pixelavg_pp  = PFX(pixel_avg_4x4_ ## cpu); \
    p.pu[LUMA_4x8].pixelavg_pp  = PFX(pixel_avg_4x8_ ## cpu); \
    p.pu[LUMA_4x16].pixelavg_pp = PFX(pixel_avg_4x16_ ## cpu);

#define CHROMA_420_FILTERS(cpu) \
    ALL_CHROMA_420_PU(filter_hpp, interp_4tap_horiz_pp, cpu); \
    ALL_CHROMA_420_PU(filter_hps, interp_4tap_horiz_ps, cpu); \
    ALL_CHROMA_420_PU(filter_vpp, interp_4tap_vert_pp, cpu); \
    ALL_CHROMA_420_PU(filter_vps, interp_4tap_vert_ps, cpu);

#define CHROMA_422_FILTERS(cpu) \
    ALL_CHROMA_422_PU(filter_hpp, interp_4tap_horiz_pp, cpu); \
    ALL_CHROMA_422_PU(filter_hps, interp_4tap_horiz_ps, cpu); \
    ALL_CHROMA_422_PU(filter_vpp, interp_4tap_vert_pp, cpu); \
    ALL_CHROMA_422_PU(filter_vps, interp_4tap_vert_ps, cpu);

#define CHROMA_444_FILTERS(cpu) \
    ALL_CHROMA_444_PU(filter_hpp, interp_4tap_horiz_pp, cpu); \
    ALL_CHROMA_444_PU(filter_hps, interp_4tap_horiz_ps, cpu); \
    ALL_CHROMA_444_PU(filter_vpp, interp_4tap_vert_pp, cpu); \
    ALL_CHROMA_444_PU(filter_vps, interp_4tap_vert_ps, cpu);

#define SETUP_CHROMA_420_VSP_FUNC_DEF(W, H, cpu) \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_ ## W ## x ## H].filter_vsp = PFX(interp_4tap_vert_sp_ ## W ## x ## H ## cpu);

#define CHROMA_420_VSP_FILTERS_SSE4(cpu) \
    SETUP_CHROMA_420_VSP_FUNC_DEF(4, 4, cpu); \
    SETUP_CHROMA_420_VSP_FUNC_DEF(4, 2, cpu); \
    SETUP_CHROMA_420_VSP_FUNC_DEF(2, 4, cpu); \
    SETUP_CHROMA_420_VSP_FUNC_DEF(4, 8, cpu); \
    SETUP_CHROMA_420_VSP_FUNC_DEF(6, 8, cpu); \
    SETUP_CHROMA_420_VSP_FUNC_DEF(2, 8, cpu); \
    SETUP_CHROMA_420_VSP_FUNC_DEF(16, 16, cpu); \
    SETUP_CHROMA_420_VSP_FUNC_DEF(16, 8, cpu); \
    SETUP_CHROMA_420_VSP_FUNC_DEF(16, 12, cpu); \
    SETUP_CHROMA_420_VSP_FUNC_DEF(12, 16, cpu); \
    SETUP_CHROMA_420_VSP_FUNC_DEF(16, 4, cpu); \
    SETUP_CHROMA_420_VSP_FUNC_DEF(4, 16, cpu); \
    SETUP_CHROMA_420_VSP_FUNC_DEF(32, 32, cpu); \
    SETUP_CHROMA_420_VSP_FUNC_DEF(32, 16, cpu); \
    SETUP_CHROMA_420_VSP_FUNC_DEF(16, 32, cpu); \
    SETUP_CHROMA_420_VSP_FUNC_DEF(32, 24, cpu); \
    SETUP_CHROMA_420_VSP_FUNC_DEF(24, 32, cpu); \
    SETUP_CHROMA_420_VSP_FUNC_DEF(32, 8, cpu);

#define CHROMA_420_VSP_FILTERS(cpu) \
    SETUP_CHROMA_420_VSP_FUNC_DEF(8, 2, cpu); \
    SETUP_CHROMA_420_VSP_FUNC_DEF(8, 4, cpu); \
    SETUP_CHROMA_420_VSP_FUNC_DEF(8, 6, cpu); \
    SETUP_CHROMA_420_VSP_FUNC_DEF(8, 8, cpu); \
    SETUP_CHROMA_420_VSP_FUNC_DEF(8, 16, cpu); \
    SETUP_CHROMA_420_VSP_FUNC_DEF(8, 32, cpu);

#define SETUP_CHROMA_422_VSP_FUNC_DEF(W, H, cpu) \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_ ## W ## x ## H].filter_vsp = PFX(interp_4tap_vert_sp_ ## W ## x ## H ## cpu);

#define CHROMA_422_VSP_FILTERS_SSE4(cpu) \
    SETUP_CHROMA_422_VSP_FUNC_DEF(4, 8, cpu); \
    SETUP_CHROMA_422_VSP_FUNC_DEF(4, 4, cpu); \
    SETUP_CHROMA_422_VSP_FUNC_DEF(2, 8, cpu); \
    SETUP_CHROMA_422_VSP_FUNC_DEF(4, 16, cpu); \
    SETUP_CHROMA_422_VSP_FUNC_DEF(6, 16, cpu); \
    SETUP_CHROMA_422_VSP_FUNC_DEF(2, 16, cpu); \
    SETUP_CHROMA_422_VSP_FUNC_DEF(16, 32, cpu); \
    SETUP_CHROMA_422_VSP_FUNC_DEF(16, 16, cpu); \
    SETUP_CHROMA_422_VSP_FUNC_DEF(16, 24, cpu); \
    SETUP_CHROMA_422_VSP_FUNC_DEF(12, 32, cpu); \
    SETUP_CHROMA_422_VSP_FUNC_DEF(16, 8, cpu); \
    SETUP_CHROMA_422_VSP_FUNC_DEF(4, 32, cpu); \
    SETUP_CHROMA_422_VSP_FUNC_DEF(32, 64, cpu); \
    SETUP_CHROMA_422_VSP_FUNC_DEF(32, 32, cpu); \
    SETUP_CHROMA_422_VSP_FUNC_DEF(16, 64, cpu); \
    SETUP_CHROMA_422_VSP_FUNC_DEF(32, 48, cpu); \
    SETUP_CHROMA_422_VSP_FUNC_DEF(24, 64, cpu); \
    SETUP_CHROMA_422_VSP_FUNC_DEF(32, 16, cpu);

#define CHROMA_422_VSP_FILTERS(cpu) \
    SETUP_CHROMA_422_VSP_FUNC_DEF(8, 4, cpu); \
    SETUP_CHROMA_422_VSP_FUNC_DEF(8, 8, cpu); \
    SETUP_CHROMA_422_VSP_FUNC_DEF(8, 12, cpu); \
    SETUP_CHROMA_422_VSP_FUNC_DEF(8, 16, cpu); \
    SETUP_CHROMA_422_VSP_FUNC_DEF(8, 32, cpu); \
    SETUP_CHROMA_422_VSP_FUNC_DEF(8, 64, cpu);

#define SETUP_CHROMA_444_VSP_FUNC_DEF(W, H, cpu) \
    p.chroma[X265_CSP_I444].pu[LUMA_ ## W ## x ## H].filter_vsp = PFX(interp_4tap_vert_sp_ ## W ## x ## H ## cpu);

#define CHROMA_444_VSP_FILTERS_SSE4(cpu) \
    SETUP_CHROMA_444_VSP_FUNC_DEF(4, 4, cpu); \
    SETUP_CHROMA_444_VSP_FUNC_DEF(4, 8, cpu); \
    SETUP_CHROMA_444_VSP_FUNC_DEF(16, 16, cpu); \
    SETUP_CHROMA_444_VSP_FUNC_DEF(16, 8, cpu); \
    SETUP_CHROMA_444_VSP_FUNC_DEF(16, 12, cpu); \
    SETUP_CHROMA_444_VSP_FUNC_DEF(12, 16, cpu); \
    SETUP_CHROMA_444_VSP_FUNC_DEF(16, 4, cpu); \
    SETUP_CHROMA_444_VSP_FUNC_DEF(4, 16, cpu); \
    SETUP_CHROMA_444_VSP_FUNC_DEF(32, 32, cpu); \
    SETUP_CHROMA_444_VSP_FUNC_DEF(32, 16, cpu); \
    SETUP_CHROMA_444_VSP_FUNC_DEF(16, 32, cpu); \
    SETUP_CHROMA_444_VSP_FUNC_DEF(32, 24, cpu); \
    SETUP_CHROMA_444_VSP_FUNC_DEF(24, 32, cpu); \
    SETUP_CHROMA_444_VSP_FUNC_DEF(32, 8, cpu); \
    SETUP_CHROMA_444_VSP_FUNC_DEF(64, 64, cpu); \
    SETUP_CHROMA_444_VSP_FUNC_DEF(64, 32, cpu); \
    SETUP_CHROMA_444_VSP_FUNC_DEF(32, 64, cpu); \
    SETUP_CHROMA_444_VSP_FUNC_DEF(64, 48, cpu); \
    SETUP_CHROMA_444_VSP_FUNC_DEF(48, 64, cpu); \
    SETUP_CHROMA_444_VSP_FUNC_DEF(64, 16, cpu); \
    SETUP_CHROMA_444_VSP_FUNC_DEF(16, 64, cpu);

#define CHROMA_444_VSP_FILTERS(cpu) \
    SETUP_CHROMA_444_VSP_FUNC_DEF(8, 8, cpu); \
    SETUP_CHROMA_444_VSP_FUNC_DEF(8, 4, cpu); \
    SETUP_CHROMA_444_VSP_FUNC_DEF(8, 16, cpu); \
    SETUP_CHROMA_444_VSP_FUNC_DEF(8, 32, cpu);

#define SETUP_CHROMA_420_VSS_FUNC_DEF(W, H, cpu) \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_ ## W ## x ## H].filter_vss = PFX(interp_4tap_vert_ss_ ## W ## x ## H ## cpu);

#define CHROMA_420_VSS_FILTERS(cpu) \
    SETUP_CHROMA_420_VSS_FUNC_DEF(4, 4, cpu); \
    SETUP_CHROMA_420_VSS_FUNC_DEF(4, 2, cpu); \
    SETUP_CHROMA_420_VSS_FUNC_DEF(8, 8, cpu); \
    SETUP_CHROMA_420_VSS_FUNC_DEF(8, 4, cpu); \
    SETUP_CHROMA_420_VSS_FUNC_DEF(4, 8, cpu); \
    SETUP_CHROMA_420_VSS_FUNC_DEF(8, 6, cpu); \
    SETUP_CHROMA_420_VSS_FUNC_DEF(8, 2, cpu); \
    SETUP_CHROMA_420_VSS_FUNC_DEF(16, 16, cpu); \
    SETUP_CHROMA_420_VSS_FUNC_DEF(16, 8, cpu); \
    SETUP_CHROMA_420_VSS_FUNC_DEF(8, 16, cpu); \
    SETUP_CHROMA_420_VSS_FUNC_DEF(16, 12, cpu); \
    SETUP_CHROMA_420_VSS_FUNC_DEF(12, 16, cpu); \
    SETUP_CHROMA_420_VSS_FUNC_DEF(16, 4, cpu); \
    SETUP_CHROMA_420_VSS_FUNC_DEF(4, 16, cpu); \
    SETUP_CHROMA_420_VSS_FUNC_DEF(32, 32, cpu); \
    SETUP_CHROMA_420_VSS_FUNC_DEF(32, 16, cpu); \
    SETUP_CHROMA_420_VSS_FUNC_DEF(16, 32, cpu); \
    SETUP_CHROMA_420_VSS_FUNC_DEF(32, 24, cpu); \
    SETUP_CHROMA_420_VSS_FUNC_DEF(24, 32, cpu); \
    SETUP_CHROMA_420_VSS_FUNC_DEF(32, 8, cpu); \
    SETUP_CHROMA_420_VSS_FUNC_DEF(8, 32, cpu);

#define CHROMA_420_VSS_FILTERS_SSE4(cpu) \
    SETUP_CHROMA_420_VSS_FUNC_DEF(2, 4, cpu); \
    SETUP_CHROMA_420_VSS_FUNC_DEF(2, 8, cpu); \
    SETUP_CHROMA_420_VSS_FUNC_DEF(6, 8, cpu);

#define SETUP_CHROMA_422_VSS_FUNC_DEF(W, H, cpu) \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_ ## W ## x ## H].filter_vss = PFX(interp_4tap_vert_ss_ ## W ## x ## H ## cpu);

#define CHROMA_422_VSS_FILTERS(cpu) \
    SETUP_CHROMA_422_VSS_FUNC_DEF(4, 8, cpu); \
    SETUP_CHROMA_422_VSS_FUNC_DEF(4, 4, cpu); \
    SETUP_CHROMA_422_VSS_FUNC_DEF(8, 16, cpu); \
    SETUP_CHROMA_422_VSS_FUNC_DEF(8, 8, cpu); \
    SETUP_CHROMA_422_VSS_FUNC_DEF(4, 16, cpu); \
    SETUP_CHROMA_422_VSS_FUNC_DEF(8, 12, cpu); \
    SETUP_CHROMA_422_VSS_FUNC_DEF(8, 4, cpu); \
    SETUP_CHROMA_422_VSS_FUNC_DEF(16, 32, cpu); \
    SETUP_CHROMA_422_VSS_FUNC_DEF(16, 16, cpu); \
    SETUP_CHROMA_422_VSS_FUNC_DEF(8, 32, cpu); \
    SETUP_CHROMA_422_VSS_FUNC_DEF(16, 24, cpu); \
    SETUP_CHROMA_422_VSS_FUNC_DEF(12, 32, cpu); \
    SETUP_CHROMA_422_VSS_FUNC_DEF(16, 8, cpu); \
    SETUP_CHROMA_422_VSS_FUNC_DEF(4, 32, cpu); \
    SETUP_CHROMA_422_VSS_FUNC_DEF(32, 64, cpu); \
    SETUP_CHROMA_422_VSS_FUNC_DEF(32, 32, cpu); \
    SETUP_CHROMA_422_VSS_FUNC_DEF(16, 64, cpu); \
    SETUP_CHROMA_422_VSS_FUNC_DEF(32, 48, cpu); \
    SETUP_CHROMA_422_VSS_FUNC_DEF(24, 64, cpu); \
    SETUP_CHROMA_422_VSS_FUNC_DEF(32, 16, cpu); \
    SETUP_CHROMA_422_VSS_FUNC_DEF(8, 64, cpu);

#define CHROMA_422_VSS_FILTERS_SSE4(cpu) \
    SETUP_CHROMA_422_VSS_FUNC_DEF(2, 8, cpu); \
    SETUP_CHROMA_422_VSS_FUNC_DEF(2, 16, cpu); \
    SETUP_CHROMA_422_VSS_FUNC_DEF(6, 16, cpu);

#define CHROMA_444_VSS_FILTERS(cpu) ALL_CHROMA_444_PU(filter_vss, interp_4tap_vert_ss, cpu)

#define LUMA_FILTERS(cpu) \
    ALL_LUMA_PU(luma_hpp, interp_8tap_horiz_pp, cpu); p.pu[LUMA_4x4].luma_hpp = PFX(interp_8tap_horiz_pp_4x4_ ## cpu); \
    ALL_LUMA_PU(luma_hps, interp_8tap_horiz_ps, cpu); p.pu[LUMA_4x4].luma_hps = PFX(interp_8tap_horiz_ps_4x4_ ## cpu); \
    ALL_LUMA_PU(luma_vpp, interp_8tap_vert_pp, cpu); p.pu[LUMA_4x4].luma_vpp = PFX(interp_8tap_vert_pp_4x4_ ## cpu); \
    ALL_LUMA_PU(luma_vps, interp_8tap_vert_ps, cpu); p.pu[LUMA_4x4].luma_vps = PFX(interp_8tap_vert_ps_4x4_ ## cpu); \
    ALL_LUMA_PU(luma_vsp, interp_8tap_vert_sp, cpu); p.pu[LUMA_4x4].luma_vsp = PFX(interp_8tap_vert_sp_4x4_ ## cpu); \
    ALL_LUMA_PU_T(luma_hvpp, interp_8tap_hv_pp_cpu); p.pu[LUMA_4x4].luma_hvpp = interp_8tap_hv_pp_cpu<LUMA_4x4>;

#define LUMA_VSS_FILTERS(cpu) ALL_LUMA_PU(luma_vss, interp_8tap_vert_ss, cpu); p.pu[LUMA_4x4].luma_vss = PFX(interp_8tap_vert_ss_4x4_ ## cpu)

#define LUMA_CU_BLOCKCOPY(type, cpu) \
    p.cu[BLOCK_4x4].copy_ ## type = PFX(blockcopy_ ## type ## _4x4_ ## cpu); \
    ALL_LUMA_CU(copy_ ## type, blockcopy_ ## type, cpu);

#define CHROMA_420_CU_BLOCKCOPY(type, cpu) ALL_CHROMA_420_CU(copy_ ## type, blockcopy_ ## type, cpu)
#define CHROMA_422_CU_BLOCKCOPY(type, cpu) ALL_CHROMA_422_CU(copy_ ## type, blockcopy_ ## type, cpu)

#define LUMA_PU_BLOCKCOPY(type, cpu)       ALL_LUMA_PU(copy_ ## type, blockcopy_ ## type, cpu); p.pu[LUMA_4x4].copy_ ## type = PFX(blockcopy_ ## type ## _4x4_ ## cpu)
#define CHROMA_420_PU_BLOCKCOPY(type, cpu) ALL_CHROMA_420_PU(copy_ ## type, blockcopy_ ## type, cpu)
#define CHROMA_422_PU_BLOCKCOPY(type, cpu) ALL_CHROMA_422_PU(copy_ ## type, blockcopy_ ## type, cpu)

#define LUMA_PIXELSUB(cpu) \
    p.cu[BLOCK_4x4].sub_ps = PFX(pixel_sub_ps_4x4_ ## cpu); \
    p.cu[BLOCK_4x4].add_ps = PFX(pixel_add_ps_4x4_ ## cpu); \
    ALL_LUMA_CU(sub_ps, pixel_sub_ps, cpu); \
    ALL_LUMA_CU(add_ps, pixel_add_ps, cpu);

#define CHROMA_420_PIXELSUB_PS(cpu) \
    ALL_CHROMA_420_CU(sub_ps, pixel_sub_ps, cpu); \
    ALL_CHROMA_420_CU(add_ps, pixel_add_ps, cpu);

#define CHROMA_422_PIXELSUB_PS(cpu) \
    ALL_CHROMA_422_CU(sub_ps, pixel_sub_ps, cpu); \
    ALL_CHROMA_422_CU(add_ps, pixel_add_ps, cpu);

#define LUMA_VAR(cpu)          ALL_LUMA_CU(var, pixel_var, cpu)

#define LUMA_ADDAVG(cpu)       ALL_LUMA_PU(addAvg, addAvg, cpu); p.pu[LUMA_4x4].addAvg = PFX(addAvg_4x4_ ## cpu)
#define CHROMA_420_ADDAVG(cpu) ALL_CHROMA_420_PU(addAvg, addAvg, cpu);
#define CHROMA_422_ADDAVG(cpu) ALL_CHROMA_422_PU(addAvg, addAvg, cpu);

#define SETUP_INTRA_ANG_COMMON(mode, fno, cpu) \
    p.cu[BLOCK_4x4].intra_pred[mode] = PFX(intra_pred_ang4_ ## fno ## _ ## cpu); \
    p.cu[BLOCK_8x8].intra_pred[mode] = PFX(intra_pred_ang8_ ## fno ## _ ## cpu); \
    p.cu[BLOCK_16x16].intra_pred[mode] = PFX(intra_pred_ang16_ ## fno ## _ ## cpu); \
    p.cu[BLOCK_32x32].intra_pred[mode] = PFX(intra_pred_ang32_ ## fno ## _ ## cpu);

#define SETUP_INTRA_ANG4(mode, fno, cpu) \
    p.cu[BLOCK_4x4].intra_pred[mode] = PFX(intra_pred_ang4_ ## fno ## _ ## cpu);

#define SETUP_INTRA_ANG16_32(mode, fno, cpu) \
    p.cu[BLOCK_16x16].intra_pred[mode] = PFX(intra_pred_ang16_ ## fno ## _ ## cpu); \
    p.cu[BLOCK_32x32].intra_pred[mode] = PFX(intra_pred_ang32_ ## fno ## _ ## cpu);

#define SETUP_INTRA_ANG4_8(mode, fno, cpu) \
    p.cu[BLOCK_4x4].intra_pred[mode] = PFX(intra_pred_ang4_ ## fno ## _ ## cpu); \
    p.cu[BLOCK_8x8].intra_pred[mode] = PFX(intra_pred_ang8_ ## fno ## _ ## cpu);

#define INTRA_ANG_SSSE3(cpu) \
    SETUP_INTRA_ANG_COMMON(2, 2, cpu); \
    SETUP_INTRA_ANG_COMMON(34, 2, cpu);

#define INTRA_ANG_SSE4_COMMON(cpu) \
    SETUP_INTRA_ANG_COMMON(3,  3,  cpu); \
    SETUP_INTRA_ANG_COMMON(4,  4,  cpu); \
    SETUP_INTRA_ANG_COMMON(5,  5,  cpu); \
    SETUP_INTRA_ANG_COMMON(6,  6,  cpu); \
    SETUP_INTRA_ANG_COMMON(7,  7,  cpu); \
    SETUP_INTRA_ANG_COMMON(8,  8,  cpu); \
    SETUP_INTRA_ANG_COMMON(9,  9,  cpu); \
    SETUP_INTRA_ANG_COMMON(10, 10, cpu); \
    SETUP_INTRA_ANG_COMMON(11, 11, cpu); \
    SETUP_INTRA_ANG_COMMON(12, 12, cpu); \
    SETUP_INTRA_ANG_COMMON(13, 13, cpu); \
    SETUP_INTRA_ANG_COMMON(14, 14, cpu); \
    SETUP_INTRA_ANG_COMMON(15, 15, cpu); \
    SETUP_INTRA_ANG_COMMON(16, 16, cpu); \
    SETUP_INTRA_ANG_COMMON(17, 17, cpu); \
    SETUP_INTRA_ANG_COMMON(18, 18, cpu);

#define SETUP_INTRA_ANG_HIGH(mode, fno, cpu) \
    p.cu[BLOCK_8x8].intra_pred[mode] = PFX(intra_pred_ang8_ ## fno ## _ ## cpu); \
    p.cu[BLOCK_16x16].intra_pred[mode] = PFX(intra_pred_ang16_ ## fno ## _ ## cpu); \
    p.cu[BLOCK_32x32].intra_pred[mode] = PFX(intra_pred_ang32_ ## fno ## _ ## cpu);

#define INTRA_ANG_SSE4_HIGH(cpu) \
    SETUP_INTRA_ANG_HIGH(19, 19, cpu); \
    SETUP_INTRA_ANG_HIGH(20, 20, cpu); \
    SETUP_INTRA_ANG_HIGH(21, 21, cpu); \
    SETUP_INTRA_ANG_HIGH(22, 22, cpu); \
    SETUP_INTRA_ANG_HIGH(23, 23, cpu); \
    SETUP_INTRA_ANG_HIGH(24, 24, cpu); \
    SETUP_INTRA_ANG_HIGH(25, 25, cpu); \
    SETUP_INTRA_ANG_HIGH(26, 26, cpu); \
    SETUP_INTRA_ANG_HIGH(27, 27, cpu); \
    SETUP_INTRA_ANG_HIGH(28, 28, cpu); \
    SETUP_INTRA_ANG_HIGH(29, 29, cpu); \
    SETUP_INTRA_ANG_HIGH(30, 30, cpu); \
    SETUP_INTRA_ANG_HIGH(31, 31, cpu); \
    SETUP_INTRA_ANG_HIGH(32, 32, cpu); \
    SETUP_INTRA_ANG_HIGH(33, 33, cpu); \
    SETUP_INTRA_ANG4(19, 17, cpu); \
    SETUP_INTRA_ANG4(20, 16, cpu); \
    SETUP_INTRA_ANG4(21, 15, cpu); \
    SETUP_INTRA_ANG4(22, 14, cpu); \
    SETUP_INTRA_ANG4(23, 13, cpu); \
    SETUP_INTRA_ANG4(24, 12, cpu); \
    SETUP_INTRA_ANG4(25, 11, cpu); \
    SETUP_INTRA_ANG4(26, 26, cpu); \
    SETUP_INTRA_ANG4(27, 9, cpu); \
    SETUP_INTRA_ANG4(28, 8, cpu); \
    SETUP_INTRA_ANG4(29, 7, cpu); \
    SETUP_INTRA_ANG4(30, 6, cpu); \
    SETUP_INTRA_ANG4(31, 5, cpu); \
    SETUP_INTRA_ANG4(32, 4, cpu); \
    SETUP_INTRA_ANG4(33, 3, cpu);

#define INTRA_ANG_SSE4(cpu) \
    SETUP_INTRA_ANG4_8(19, 17, cpu); \
    SETUP_INTRA_ANG4_8(20, 16, cpu); \
    SETUP_INTRA_ANG4_8(21, 15, cpu); \
    SETUP_INTRA_ANG4_8(22, 14, cpu); \
    SETUP_INTRA_ANG4_8(23, 13, cpu); \
    SETUP_INTRA_ANG4_8(24, 12, cpu); \
    SETUP_INTRA_ANG4_8(25, 11, cpu); \
    SETUP_INTRA_ANG4_8(26, 26, cpu); \
    SETUP_INTRA_ANG4_8(27, 9, cpu); \
    SETUP_INTRA_ANG4_8(28, 8, cpu); \
    SETUP_INTRA_ANG4_8(29, 7, cpu); \
    SETUP_INTRA_ANG4_8(30, 6, cpu); \
    SETUP_INTRA_ANG4_8(31, 5, cpu); \
    SETUP_INTRA_ANG4_8(32, 4, cpu); \
    SETUP_INTRA_ANG4_8(33, 3, cpu); \
    SETUP_INTRA_ANG16_32(19, 19, cpu); \
    SETUP_INTRA_ANG16_32(20, 20, cpu); \
    SETUP_INTRA_ANG16_32(21, 21, cpu); \
    SETUP_INTRA_ANG16_32(22, 22, cpu); \
    SETUP_INTRA_ANG16_32(23, 23, cpu); \
    SETUP_INTRA_ANG16_32(24, 24, cpu); \
    SETUP_INTRA_ANG16_32(25, 25, cpu); \
    SETUP_INTRA_ANG16_32(26, 26, cpu); \
    SETUP_INTRA_ANG16_32(27, 27, cpu); \
    SETUP_INTRA_ANG16_32(28, 28, cpu); \
    SETUP_INTRA_ANG16_32(29, 29, cpu); \
    SETUP_INTRA_ANG16_32(30, 30, cpu); \
    SETUP_INTRA_ANG16_32(31, 31, cpu); \
    SETUP_INTRA_ANG16_32(32, 32, cpu); \
    SETUP_INTRA_ANG16_32(33, 33, cpu);

#define CHROMA_420_VERT_FILTERS(cpu) \
    ALL_CHROMA_420_4x4_PU(filter_vss, interp_4tap_vert_ss, cpu); \
    ALL_CHROMA_420_4x4_PU(filter_vpp, interp_4tap_vert_pp, cpu); \
    ALL_CHROMA_420_4x4_PU(filter_vps, interp_4tap_vert_ps, cpu); \
    ALL_CHROMA_420_4x4_PU(filter_vsp, interp_4tap_vert_sp, cpu)

#define SETUP_CHROMA_420_VERT_FUNC_DEF(W, H, cpu) \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_ ## W ## x ## H].filter_vss = PFX(interp_4tap_vert_ss_ ## W ## x ## H ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_ ## W ## x ## H].filter_vpp = PFX(interp_4tap_vert_pp_ ## W ## x ## H ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_ ## W ## x ## H].filter_vps = PFX(interp_4tap_vert_ps_ ## W ## x ## H ## cpu); \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_ ## W ## x ## H].filter_vsp = PFX(interp_4tap_vert_sp_ ## W ## x ## H ## cpu);

#define CHROMA_420_VERT_FILTERS_SSE4(cpu) \
    SETUP_CHROMA_420_VERT_FUNC_DEF(2, 4, cpu); \
    SETUP_CHROMA_420_VERT_FUNC_DEF(2, 8, cpu); \
    SETUP_CHROMA_420_VERT_FUNC_DEF(4, 2, cpu); \
    SETUP_CHROMA_420_VERT_FUNC_DEF(6, 8, cpu);

#define SETUP_CHROMA_422_VERT_FUNC_DEF(W, H, cpu) \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_ ## W ## x ## H].filter_vss = PFX(interp_4tap_vert_ss_ ## W ## x ## H ## cpu); \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_ ## W ## x ## H].filter_vpp = PFX(interp_4tap_vert_pp_ ## W ## x ## H ## cpu); \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_ ## W ## x ## H].filter_vps = PFX(interp_4tap_vert_ps_ ## W ## x ## H ## cpu); \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_ ## W ## x ## H].filter_vsp = PFX(interp_4tap_vert_sp_ ## W ## x ## H ## cpu);

#define CHROMA_422_VERT_FILTERS(cpu) \
    SETUP_CHROMA_422_VERT_FUNC_DEF(4, 8, cpu); \
    SETUP_CHROMA_422_VERT_FUNC_DEF(8, 16, cpu); \
    SETUP_CHROMA_422_VERT_FUNC_DEF(8, 8, cpu); \
    SETUP_CHROMA_422_VERT_FUNC_DEF(4, 16, cpu); \
    SETUP_CHROMA_422_VERT_FUNC_DEF(8, 12, cpu); \
    SETUP_CHROMA_422_VERT_FUNC_DEF(8, 4, cpu); \
    SETUP_CHROMA_422_VERT_FUNC_DEF(16, 32, cpu); \
    SETUP_CHROMA_422_VERT_FUNC_DEF(16, 16, cpu); \
    SETUP_CHROMA_422_VERT_FUNC_DEF(8, 32, cpu); \
    SETUP_CHROMA_422_VERT_FUNC_DEF(16, 24, cpu); \
    SETUP_CHROMA_422_VERT_FUNC_DEF(12, 32, cpu); \
    SETUP_CHROMA_422_VERT_FUNC_DEF(16, 8, cpu); \
    SETUP_CHROMA_422_VERT_FUNC_DEF(4, 32, cpu); \
    SETUP_CHROMA_422_VERT_FUNC_DEF(32, 64, cpu); \
    SETUP_CHROMA_422_VERT_FUNC_DEF(32, 32, cpu); \
    SETUP_CHROMA_422_VERT_FUNC_DEF(16, 64, cpu); \
    SETUP_CHROMA_422_VERT_FUNC_DEF(32, 48, cpu); \
    SETUP_CHROMA_422_VERT_FUNC_DEF(24, 64, cpu); \
    SETUP_CHROMA_422_VERT_FUNC_DEF(32, 16, cpu); \
    SETUP_CHROMA_422_VERT_FUNC_DEF(8, 64, cpu);

#define CHROMA_422_VERT_FILTERS_SSE4(cpu) \
    SETUP_CHROMA_422_VERT_FUNC_DEF(2, 8, cpu); \
    SETUP_CHROMA_422_VERT_FUNC_DEF(2, 16, cpu); \
    SETUP_CHROMA_422_VERT_FUNC_DEF(4, 4, cpu); \
    SETUP_CHROMA_422_VERT_FUNC_DEF(6, 16, cpu);

#define CHROMA_444_VERT_FILTERS(cpu) \
    ALL_CHROMA_444_PU(filter_vss, interp_4tap_vert_ss, cpu); \
    ALL_CHROMA_444_PU(filter_vpp, interp_4tap_vert_pp, cpu); \
    ALL_CHROMA_444_PU(filter_vps, interp_4tap_vert_ps, cpu); \
    ALL_CHROMA_444_PU(filter_vsp, interp_4tap_vert_sp, cpu)

#define CHROMA_420_HORIZ_FILTERS(cpu) \
    ALL_CHROMA_420_PU(filter_hpp, interp_4tap_horiz_pp, cpu); \
    ALL_CHROMA_420_PU(filter_hps, interp_4tap_horiz_ps, cpu);

#define SETUP_CHROMA_422_HORIZ_FUNC_DEF(W, H, cpu) \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_ ## W ## x ## H].filter_hpp = PFX(interp_4tap_horiz_pp_ ## W ## x ## H ## cpu); \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_ ## W ## x ## H].filter_hps = PFX(interp_4tap_horiz_ps_ ## W ## x ## H ## cpu);

#define CHROMA_422_HORIZ_FILTERS(cpu) \
    SETUP_CHROMA_422_HORIZ_FUNC_DEF(4, 8, cpu); \
    SETUP_CHROMA_422_HORIZ_FUNC_DEF(4, 4, cpu); \
    SETUP_CHROMA_422_HORIZ_FUNC_DEF(2, 8, cpu); \
    SETUP_CHROMA_422_HORIZ_FUNC_DEF(8, 16, cpu); \
    SETUP_CHROMA_422_HORIZ_FUNC_DEF(8, 8, cpu); \
    SETUP_CHROMA_422_HORIZ_FUNC_DEF(4, 16, cpu); \
    SETUP_CHROMA_422_HORIZ_FUNC_DEF(8, 12, cpu); \
    SETUP_CHROMA_422_HORIZ_FUNC_DEF(6, 16, cpu); \
    SETUP_CHROMA_422_HORIZ_FUNC_DEF(8, 4, cpu); \
    SETUP_CHROMA_422_HORIZ_FUNC_DEF(2, 16, cpu); \
    SETUP_CHROMA_422_HORIZ_FUNC_DEF(16, 32, cpu); \
    SETUP_CHROMA_422_HORIZ_FUNC_DEF(16, 16, cpu); \
    SETUP_CHROMA_422_HORIZ_FUNC_DEF(8, 32, cpu); \
    SETUP_CHROMA_422_HORIZ_FUNC_DEF(16, 24, cpu); \
    SETUP_CHROMA_422_HORIZ_FUNC_DEF(12, 32, cpu); \
    SETUP_CHROMA_422_HORIZ_FUNC_DEF(16, 8, cpu); \
    SETUP_CHROMA_422_HORIZ_FUNC_DEF(4, 32, cpu); \
    SETUP_CHROMA_422_HORIZ_FUNC_DEF(32, 64, cpu); \
    SETUP_CHROMA_422_HORIZ_FUNC_DEF(32, 32, cpu); \
    SETUP_CHROMA_422_HORIZ_FUNC_DEF(16, 64, cpu); \
    SETUP_CHROMA_422_HORIZ_FUNC_DEF(32, 48, cpu); \
    SETUP_CHROMA_422_HORIZ_FUNC_DEF(24, 64, cpu); \
    SETUP_CHROMA_422_HORIZ_FUNC_DEF(32, 16, cpu); \
    SETUP_CHROMA_422_HORIZ_FUNC_DEF(8, 64, cpu);

#define CHROMA_444_HORIZ_FILTERS(cpu) \
    ALL_CHROMA_444_PU(filter_hpp, interp_4tap_horiz_pp, cpu); \
    ALL_CHROMA_444_PU(filter_hps, interp_4tap_horiz_ps, cpu);

namespace X265_NS {
// private x265 namespace

template<int size>
void interp_8tap_hv_pp_cpu(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int idxX, int idxY)
{
    ALIGN_VAR_32(int16_t, immed[MAX_CU_SIZE * (MAX_CU_SIZE + NTAPS_LUMA - 1)]);
    const int halfFilterSize = NTAPS_LUMA >> 1;
    const int immedStride = MAX_CU_SIZE;

    primitives.pu[size].luma_hps(src, srcStride, immed, immedStride, idxX, 1);
    primitives.pu[size].luma_vsp(immed + (halfFilterSize - 1) * immedStride, immedStride, dst, dstStride, idxY);
}

#if HIGH_BIT_DEPTH

void setupAssemblyPrimitives(EncoderPrimitives &p, int cpuMask) // Main10
{
#if !defined(X86_64)
#error "Unsupported build configuration (32bit x86 and HIGH_BIT_DEPTH), you must configure ENABLE_ASSEMBLY=OFF"
#endif

#if X86_64
    p.scanPosLast = PFX(scanPosLast_x64);
#endif

    if (cpuMask & X265_CPU_SSE2)
    {
        /* We do not differentiate CPUs which support MMX and not SSE2. We only check
         * for SSE2 and then use both MMX and SSE2 functions */
        AVC_LUMA_PU(sad, mmx2);

        p.pu[LUMA_16x16].sad = PFX(pixel_sad_16x16_sse2);
        p.pu[LUMA_16x8].sad  = PFX(pixel_sad_16x8_sse2);
        p.pu[LUMA_8x16].sad  = PFX(pixel_sad_8x16_sse2);
        HEVC_SAD(sse2);

        p.pu[LUMA_4x4].sad_x3   = PFX(pixel_sad_x3_4x4_mmx2);
        p.pu[LUMA_4x8].sad_x3   = PFX(pixel_sad_x3_4x8_mmx2);
        p.pu[LUMA_4x16].sad_x3  = PFX(pixel_sad_x3_4x16_mmx2);
        p.pu[LUMA_8x4].sad_x3   = PFX(pixel_sad_x3_8x4_sse2);
        p.pu[LUMA_8x8].sad_x3   = PFX(pixel_sad_x3_8x8_sse2);
        p.pu[LUMA_8x16].sad_x3  = PFX(pixel_sad_x3_8x16_sse2);
        p.pu[LUMA_8x32].sad_x3  = PFX(pixel_sad_x3_8x32_sse2);
        p.pu[LUMA_16x4].sad_x3  = PFX(pixel_sad_x3_16x4_sse2);
        p.pu[LUMA_12x16].sad_x3 = PFX(pixel_sad_x3_12x16_mmx2);
        HEVC_SAD_X3(sse2);

        p.pu[LUMA_4x4].sad_x4   = PFX(pixel_sad_x4_4x4_mmx2);
        p.pu[LUMA_4x8].sad_x4   = PFX(pixel_sad_x4_4x8_mmx2);
        p.pu[LUMA_4x16].sad_x4  = PFX(pixel_sad_x4_4x16_mmx2);
        p.pu[LUMA_8x4].sad_x4   = PFX(pixel_sad_x4_8x4_sse2);
        p.pu[LUMA_8x8].sad_x4   = PFX(pixel_sad_x4_8x8_sse2);
        p.pu[LUMA_8x16].sad_x4  = PFX(pixel_sad_x4_8x16_sse2);
        p.pu[LUMA_8x32].sad_x4  = PFX(pixel_sad_x4_8x32_sse2);
        p.pu[LUMA_16x4].sad_x4  = PFX(pixel_sad_x4_16x4_sse2);
        p.pu[LUMA_12x16].sad_x4 = PFX(pixel_sad_x4_12x16_mmx2);
        HEVC_SAD_X4(sse2);

        p.pu[LUMA_4x4].satd = p.cu[BLOCK_4x4].sa8d = PFX(pixel_satd_4x4_mmx2);
        ALL_LUMA_PU(satd, pixel_satd, sse2);

#if X265_DEPTH <= 10
        ASSIGN_SA8D(sse2);
#endif /* X265_DEPTH <= 10 */
        LUMA_PIXELSUB(sse2);
        CHROMA_420_PIXELSUB_PS(sse2);
        CHROMA_422_PIXELSUB_PS(sse2);

        LUMA_CU_BLOCKCOPY(ss, sse2);
        CHROMA_420_CU_BLOCKCOPY(ss, sse2);
        CHROMA_422_CU_BLOCKCOPY(ss, sse2);

        p.pu[LUMA_4x4].copy_pp = (copy_pp_t)PFX(blockcopy_ss_4x4_sse2);
        ALL_LUMA_PU_TYPED(copy_pp, (copy_pp_t), blockcopy_ss, sse2);
        ALL_CHROMA_420_PU_TYPED(copy_pp, (copy_pp_t), blockcopy_ss, sse2);
        ALL_CHROMA_422_PU_TYPED(copy_pp, (copy_pp_t), blockcopy_ss, sse2);

        CHROMA_420_VERT_FILTERS(sse2);
        CHROMA_422_VERT_FILTERS(_sse2);
        CHROMA_444_VERT_FILTERS(sse2);

        ALL_LUMA_PU(luma_hpp, interp_8tap_horiz_pp, sse2);
        p.pu[LUMA_4x4].luma_hpp = PFX(interp_8tap_horiz_pp_4x4_sse2);
        ALL_LUMA_PU(luma_hps, interp_8tap_horiz_ps, sse2);
        p.pu[LUMA_4x4].luma_hps = PFX(interp_8tap_horiz_ps_4x4_sse2);
        ALL_LUMA_PU(luma_vpp, interp_8tap_vert_pp, sse2);
        ALL_LUMA_PU(luma_vps, interp_8tap_vert_ps, sse2);

        p.ssim_4x4x2_core = PFX(pixel_ssim_4x4x2_core_sse2);
        p.ssim_end_4 = PFX(pixel_ssim_end4_sse2);
        PIXEL_AVG(sse2);
        PIXEL_AVG_W4(mmx2);
        LUMA_VAR(sse2);


        ALL_LUMA_TU(blockfill_s, blockfill_s, sse2);
        ALL_LUMA_TU_S(cpy1Dto2D_shr, cpy1Dto2D_shr_, sse2);
        ALL_LUMA_TU_S(cpy1Dto2D_shl, cpy1Dto2D_shl_, sse2);
        ALL_LUMA_TU_S(cpy2Dto1D_shr, cpy2Dto1D_shr_, sse2);
        ALL_LUMA_TU_S(cpy2Dto1D_shl, cpy2Dto1D_shl_, sse2);
        ALL_LUMA_TU_S(ssd_s, pixel_ssd_s_, sse2);
        ALL_LUMA_TU_S(calcresidual, getResidual, sse2);
        ALL_LUMA_TU_S(transpose, transpose, sse2);

        p.cu[BLOCK_4x4].intra_pred[PLANAR_IDX] = PFX(intra_pred_planar4_sse2);
        p.cu[BLOCK_8x8].intra_pred[PLANAR_IDX] = PFX(intra_pred_planar8_sse2);
        p.cu[BLOCK_16x16].intra_pred[PLANAR_IDX] = PFX(intra_pred_planar16_sse2);
        p.cu[BLOCK_32x32].intra_pred[PLANAR_IDX] = PFX(intra_pred_planar32_sse2);
        ALL_LUMA_TU_S(intra_pred[DC_IDX], intra_pred_dc, sse2);

        p.cu[BLOCK_4x4].intra_pred[2] = PFX(intra_pred_ang4_2_sse2);
        p.cu[BLOCK_4x4].intra_pred[3] = PFX(intra_pred_ang4_3_sse2);
        p.cu[BLOCK_4x4].intra_pred[4] = PFX(intra_pred_ang4_4_sse2);
        p.cu[BLOCK_4x4].intra_pred[5] = PFX(intra_pred_ang4_5_sse2);
        p.cu[BLOCK_4x4].intra_pred[6] = PFX(intra_pred_ang4_6_sse2);
        p.cu[BLOCK_4x4].intra_pred[7] = PFX(intra_pred_ang4_7_sse2);
        p.cu[BLOCK_4x4].intra_pred[8] = PFX(intra_pred_ang4_8_sse2);
        p.cu[BLOCK_4x4].intra_pred[9] = PFX(intra_pred_ang4_9_sse2);
        p.cu[BLOCK_4x4].intra_pred[10] = PFX(intra_pred_ang4_10_sse2);
        p.cu[BLOCK_4x4].intra_pred[11] = PFX(intra_pred_ang4_11_sse2);
        p.cu[BLOCK_4x4].intra_pred[12] = PFX(intra_pred_ang4_12_sse2);
        p.cu[BLOCK_4x4].intra_pred[13] = PFX(intra_pred_ang4_13_sse2);
        p.cu[BLOCK_4x4].intra_pred[14] = PFX(intra_pred_ang4_14_sse2);
        p.cu[BLOCK_4x4].intra_pred[15] = PFX(intra_pred_ang4_15_sse2);
        p.cu[BLOCK_4x4].intra_pred[16] = PFX(intra_pred_ang4_16_sse2);
        p.cu[BLOCK_4x4].intra_pred[17] = PFX(intra_pred_ang4_17_sse2);
        p.cu[BLOCK_4x4].intra_pred[18] = PFX(intra_pred_ang4_18_sse2);
        p.cu[BLOCK_4x4].intra_pred[19] = PFX(intra_pred_ang4_19_sse2);
        p.cu[BLOCK_4x4].intra_pred[20] = PFX(intra_pred_ang4_20_sse2);
        p.cu[BLOCK_4x4].intra_pred[21] = PFX(intra_pred_ang4_21_sse2);
        p.cu[BLOCK_4x4].intra_pred[22] = PFX(intra_pred_ang4_22_sse2);
        p.cu[BLOCK_4x4].intra_pred[23] = PFX(intra_pred_ang4_23_sse2);
        p.cu[BLOCK_4x4].intra_pred[24] = PFX(intra_pred_ang4_24_sse2);
        p.cu[BLOCK_4x4].intra_pred[25] = PFX(intra_pred_ang4_25_sse2);
        p.cu[BLOCK_4x4].intra_pred[26] = PFX(intra_pred_ang4_26_sse2);
        p.cu[BLOCK_4x4].intra_pred[27] = PFX(intra_pred_ang4_27_sse2);
        p.cu[BLOCK_4x4].intra_pred[28] = PFX(intra_pred_ang4_28_sse2);
        p.cu[BLOCK_4x4].intra_pred[29] = PFX(intra_pred_ang4_29_sse2);
        p.cu[BLOCK_4x4].intra_pred[30] = PFX(intra_pred_ang4_30_sse2);
        p.cu[BLOCK_4x4].intra_pred[31] = PFX(intra_pred_ang4_31_sse2);
        p.cu[BLOCK_4x4].intra_pred[32] = PFX(intra_pred_ang4_32_sse2);
        p.cu[BLOCK_4x4].intra_pred[33] = PFX(intra_pred_ang4_33_sse2);

        p.chroma[X265_CSP_I422].cu[BLOCK_422_32x64].sse_pp = (pixel_sse_t)PFX(pixel_ssd_ss_32x64_sse2);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_4x8].sse_pp = (pixel_sse_t)PFX(pixel_ssd_ss_4x8_mmx2);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_8x16].sse_pp = (pixel_sse_t)PFX(pixel_ssd_ss_8x16_sse2);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_16x32].sse_pp = (pixel_sse_t)PFX(pixel_ssd_ss_16x32_sse2);
#if X265_DEPTH <= 10
        p.cu[BLOCK_4x4].sse_ss = PFX(pixel_ssd_ss_4x4_mmx2);
        ALL_LUMA_CU(sse_ss, pixel_ssd_ss, sse2);
#endif
        p.cu[BLOCK_4x4].dct = PFX(dct4_sse2);
        p.cu[BLOCK_8x8].dct = PFX(dct8_sse2);
        p.cu[BLOCK_4x4].idct = PFX(idct4_sse2);
        p.cu[BLOCK_8x8].idct = PFX(idct8_sse2);

        p.idst4x4 = PFX(idst4_sse2);
        p.dst4x4 = PFX(dst4_sse2);

        LUMA_VSS_FILTERS(sse2);

        p.frameInitLowres = PFX(frame_init_lowres_core_sse2);
        // TODO: the planecopy_sp is really planecopy_SC now, must be fix it 
        //p.planecopy_sp = PFX(downShift_16_sse2);
        p.planecopy_sp_shl = PFX(upShift_16_sse2);

        ALL_CHROMA_420_PU(p2s, filterPixelToShort, sse2);
        ALL_CHROMA_422_PU(p2s, filterPixelToShort, sse2);
        ALL_CHROMA_444_PU(p2s, filterPixelToShort, sse2);
        ALL_LUMA_PU(convert_p2s, filterPixelToShort, sse2);
        ALL_LUMA_TU(count_nonzero, count_nonzero, sse2);
        p.propagateCost = PFX(mbtree_propagate_cost_sse2);
    }
    if (cpuMask & X265_CPU_SSE3)
    {
        ALL_CHROMA_420_PU(filter_hpp, interp_4tap_horiz_pp, sse3);
        ALL_CHROMA_422_PU(filter_hpp, interp_4tap_horiz_pp, sse3);
        ALL_CHROMA_444_PU(filter_hpp, interp_4tap_horiz_pp, sse3);
        ALL_CHROMA_420_PU(filter_hps, interp_4tap_horiz_ps, sse3);
        ALL_CHROMA_422_PU(filter_hps, interp_4tap_horiz_ps, sse3);
        ALL_CHROMA_444_PU(filter_hps, interp_4tap_horiz_ps, sse3);
    }
    if (cpuMask & X265_CPU_SSSE3)
    {
        p.scale1D_128to64 = PFX(scale1D_128to64_ssse3);
        p.scale2D_64to32 = PFX(scale2D_64to32_ssse3);

        // p.pu[LUMA_4x4].satd = p.cu[BLOCK_4x4].sa8d = PFX(pixel_satd_4x4_ssse3); this one is broken
        ALL_LUMA_PU(satd, pixel_satd, ssse3);
#if X265_DEPTH <= 10
        ASSIGN_SA8D(ssse3);
#endif
        INTRA_ANG_SSSE3(ssse3);

        p.dst4x4 = PFX(dst4_ssse3);
        p.cu[BLOCK_8x8].idct = PFX(idct8_ssse3);

        p.frameInitLowres = PFX(frame_init_lowres_core_ssse3);

        ALL_LUMA_PU(convert_p2s, filterPixelToShort, ssse3);

        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x4].p2s = PFX(filterPixelToShort_4x4_ssse3);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x8].p2s = PFX(filterPixelToShort_4x8_ssse3);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x16].p2s = PFX(filterPixelToShort_4x16_ssse3);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x4].p2s = PFX(filterPixelToShort_8x4_ssse3);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x8].p2s = PFX(filterPixelToShort_8x8_ssse3);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x16].p2s = PFX(filterPixelToShort_8x16_ssse3);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x32].p2s = PFX(filterPixelToShort_8x32_ssse3);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x4].p2s = PFX(filterPixelToShort_16x4_ssse3);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x8].p2s = PFX(filterPixelToShort_16x8_ssse3);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x12].p2s = PFX(filterPixelToShort_16x12_ssse3);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x16].p2s = PFX(filterPixelToShort_16x16_ssse3);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x32].p2s = PFX(filterPixelToShort_16x32_ssse3);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x8].p2s = PFX(filterPixelToShort_32x8_ssse3);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x16].p2s = PFX(filterPixelToShort_32x16_ssse3);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x24].p2s = PFX(filterPixelToShort_32x24_ssse3);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x32].p2s = PFX(filterPixelToShort_32x32_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x4].p2s = PFX(filterPixelToShort_4x4_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x8].p2s = PFX(filterPixelToShort_4x8_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x16].p2s = PFX(filterPixelToShort_4x16_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x32].p2s = PFX(filterPixelToShort_4x32_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x4].p2s = PFX(filterPixelToShort_8x4_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x8].p2s = PFX(filterPixelToShort_8x8_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x12].p2s = PFX(filterPixelToShort_8x12_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x16].p2s = PFX(filterPixelToShort_8x16_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x32].p2s = PFX(filterPixelToShort_8x32_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x64].p2s = PFX(filterPixelToShort_8x64_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_12x32].p2s = PFX(filterPixelToShort_12x32_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x8].p2s = PFX(filterPixelToShort_16x8_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x16].p2s = PFX(filterPixelToShort_16x16_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x24].p2s = PFX(filterPixelToShort_16x24_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x32].p2s = PFX(filterPixelToShort_16x32_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x64].p2s = PFX(filterPixelToShort_16x64_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_24x64].p2s = PFX(filterPixelToShort_24x64_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x16].p2s = PFX(filterPixelToShort_32x16_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x32].p2s = PFX(filterPixelToShort_32x32_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x48].p2s = PFX(filterPixelToShort_32x48_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x64].p2s = PFX(filterPixelToShort_32x64_ssse3);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x2].p2s = PFX(filterPixelToShort_4x2_ssse3);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x2].p2s = PFX(filterPixelToShort_8x2_ssse3);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x6].p2s = PFX(filterPixelToShort_8x6_ssse3);
        p.findPosFirstLast = PFX(findPosFirstLast_ssse3);
        p.fix8Unpack = PFX(cutree_fix8_unpack_ssse3);
        p.fix8Pack = PFX(cutree_fix8_pack_ssse3);
    }
    if (cpuMask & X265_CPU_SSE4)
    {
        p.pelFilterLumaStrong[0] = PFX(pelFilterLumaStrong_V_sse4);
        p.pelFilterLumaStrong[1] = PFX(pelFilterLumaStrong_H_sse4);
        p.pelFilterChroma[0] = PFX(pelFilterChroma_V_sse4);
        p.pelFilterChroma[1] = PFX(pelFilterChroma_H_sse4);

        p.saoCuOrgE0 = PFX(saoCuOrgE0_sse4);
        p.saoCuOrgE1 = PFX(saoCuOrgE1_sse4);
        p.saoCuOrgE1_2Rows = PFX(saoCuOrgE1_2Rows_sse4);
        p.saoCuOrgE2[0] = PFX(saoCuOrgE2_sse4);
        p.saoCuOrgE2[1] = PFX(saoCuOrgE2_sse4);
        p.saoCuOrgE3[0] = PFX(saoCuOrgE3_sse4);
        p.saoCuOrgE3[1] = PFX(saoCuOrgE3_sse4);
        p.saoCuOrgB0 = PFX(saoCuOrgB0_sse4);
        p.sign = PFX(calSign_sse4);

        LUMA_ADDAVG(sse4);
        CHROMA_420_ADDAVG(sse4);
        CHROMA_422_ADDAVG(sse4);

        LUMA_FILTERS(sse4);
        CHROMA_420_HORIZ_FILTERS(sse4);
        CHROMA_420_VERT_FILTERS_SSE4(_sse4);
        CHROMA_422_HORIZ_FILTERS(_sse4);
        CHROMA_422_VERT_FILTERS_SSE4(_sse4);
        CHROMA_444_HORIZ_FILTERS(sse4);

        p.cu[BLOCK_8x8].dct = PFX(dct8_sse4);
        p.quant = PFX(quant_sse4);
        p.nquant = PFX(nquant_sse4);
        p.dequant_normal = PFX(dequant_normal_sse4);
        p.dequant_scaling = PFX(dequant_scaling_sse4);

        // p.pu[LUMA_4x4].satd = p.cu[BLOCK_4x4].sa8d = PFX(pixel_satd_4x4_sse4); fails tests
        ALL_LUMA_PU(satd, pixel_satd, sse4);
#if X265_DEPTH <= 10
        ASSIGN_SA8D(sse4);
#endif

        p.cu[BLOCK_4x4].intra_filter = PFX(intra_filter_4x4_sse4);
        p.cu[BLOCK_8x8].intra_filter = PFX(intra_filter_8x8_sse4);
        p.cu[BLOCK_16x16].intra_filter = PFX(intra_filter_16x16_sse4);
        p.cu[BLOCK_32x32].intra_filter = PFX(intra_filter_32x32_sse4);

        p.cu[BLOCK_4x4].intra_pred[PLANAR_IDX] = PFX(intra_pred_planar4_sse4);
        p.cu[BLOCK_8x8].intra_pred[PLANAR_IDX] = PFX(intra_pred_planar8_sse4);
        p.cu[BLOCK_16x16].intra_pred[PLANAR_IDX] = PFX(intra_pred_planar16_sse4);
        p.cu[BLOCK_32x32].intra_pred[PLANAR_IDX] = PFX(intra_pred_planar32_sse4);
        ALL_LUMA_TU_S(intra_pred[DC_IDX], intra_pred_dc, sse4);
        INTRA_ANG_SSE4_COMMON(sse4);
        INTRA_ANG_SSE4_HIGH(sse4);

        p.planecopy_cp = PFX(upShift_8_sse4);
        p.weight_pp = PFX(weight_pp_sse4);
        p.weight_sp = PFX(weight_sp_sse4);

        p.cu[BLOCK_4x4].psy_cost_pp = PFX(psyCost_pp_4x4_sse4);

        // TODO: check POPCNT flag!
        ALL_LUMA_TU_S(copy_cnt, copy_cnt_, sse4);
#if X265_DEPTH <= 10
        ALL_LUMA_CU(psy_cost_pp, psyCost_pp, sse4);
#endif

        p.chroma[X265_CSP_I420].pu[CHROMA_420_2x4].p2s = PFX(filterPixelToShort_2x4_sse4);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_2x8].p2s = PFX(filterPixelToShort_2x8_sse4);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_6x8].p2s = PFX(filterPixelToShort_6x8_sse4);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_2x8].p2s = PFX(filterPixelToShort_2x8_sse4);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_2x16].p2s = PFX(filterPixelToShort_2x16_sse4);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_6x16].p2s = PFX(filterPixelToShort_6x16_sse4);
        p.costCoeffRemain = PFX(costCoeffRemain_sse4);
#if X86_64
        p.saoCuStatsE0 = PFX(saoCuStatsE0_sse4);
        p.saoCuStatsE1 = PFX(saoCuStatsE1_sse4);
        p.saoCuStatsE2 = PFX(saoCuStatsE2_sse4);
        p.saoCuStatsE3 = PFX(saoCuStatsE3_sse4);
#endif
    }
    if (cpuMask & X265_CPU_AVX)
    {
        // p.pu[LUMA_4x4].satd = p.cu[BLOCK_4x4].sa8d = PFX(pixel_satd_4x4_avx); fails tests
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x24].satd = PFX(pixel_satd_16x24_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x48].satd = PFX(pixel_satd_32x48_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_24x64].satd = PFX(pixel_satd_24x64_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x64].satd = PFX(pixel_satd_8x64_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x12].satd = PFX(pixel_satd_8x12_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_12x32].satd = PFX(pixel_satd_12x32_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x32].satd = PFX(pixel_satd_4x32_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x8].satd = PFX(pixel_satd_4x8_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x16].satd = PFX(pixel_satd_8x16_avx);
        // p.chroma[X265_CSP_I422].pu[CHROMA_422_4x4].satd = PFX(pixel_satd_4x4_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x8].satd = PFX(pixel_satd_8x8_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x16].satd = PFX(pixel_satd_4x16_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x32].satd = PFX(pixel_satd_8x32_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x4].satd = PFX(pixel_satd_8x4_avx);

        ALL_LUMA_PU(satd, pixel_satd, avx);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x8].satd = PFX(pixel_satd_8x8_avx);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x4].satd = PFX(pixel_satd_8x4_avx);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x16].satd = PFX(pixel_satd_8x16_avx);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x32].satd = PFX(pixel_satd_8x32_avx);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_12x16].satd = PFX(pixel_satd_12x16_avx);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_24x32].satd = PFX(pixel_satd_24x32_avx);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x16].satd = PFX(pixel_satd_4x16_avx);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x8].satd = PFX(pixel_satd_4x8_avx);
#if X265_DEPTH <= 10
        ASSIGN_SA8D(avx);
#endif
        p.chroma[X265_CSP_I420].cu[BLOCK_420_8x8].sa8d = PFX(pixel_sa8d_8x8_avx);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_16x16].sa8d = PFX(pixel_sa8d_16x16_avx);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_32x32].sa8d = PFX(pixel_sa8d_32x32_avx);
        LUMA_VAR(avx);
        p.ssim_4x4x2_core = PFX(pixel_ssim_4x4x2_core_avx);
        p.ssim_end_4 = PFX(pixel_ssim_end4_avx);

        // copy_pp primitives
        // 16 x N
        p.pu[LUMA_64x64].copy_pp = (copy_pp_t)PFX(blockcopy_ss_64x64_avx);
        p.pu[LUMA_16x4].copy_pp = (copy_pp_t)PFX(blockcopy_ss_16x4_avx);
        p.pu[LUMA_16x8].copy_pp = (copy_pp_t)PFX(blockcopy_ss_16x8_avx);
        p.pu[LUMA_16x12].copy_pp = (copy_pp_t)PFX(blockcopy_ss_16x12_avx);
        p.pu[LUMA_16x16].copy_pp = (copy_pp_t)PFX(blockcopy_ss_16x16_avx);
        p.pu[LUMA_16x32].copy_pp = (copy_pp_t)PFX(blockcopy_ss_16x32_avx);
        p.pu[LUMA_16x64].copy_pp = (copy_pp_t)PFX(blockcopy_ss_16x64_avx);
        p.pu[LUMA_64x16].copy_pp = (copy_pp_t)PFX(blockcopy_ss_64x16_avx);
        p.pu[LUMA_64x32].copy_pp = (copy_pp_t)PFX(blockcopy_ss_64x32_avx);
        p.pu[LUMA_64x48].copy_pp = (copy_pp_t)PFX(blockcopy_ss_64x48_avx);
        p.pu[LUMA_64x64].copy_pp = (copy_pp_t)PFX(blockcopy_ss_64x64_avx);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x4].copy_pp = (copy_pp_t)PFX(blockcopy_ss_16x4_avx);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x8].copy_pp = (copy_pp_t)PFX(blockcopy_ss_16x8_avx);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x12].copy_pp = (copy_pp_t)PFX(blockcopy_ss_16x12_avx);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x16].copy_pp = (copy_pp_t)PFX(blockcopy_ss_16x16_avx);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x32].copy_pp = (copy_pp_t)PFX(blockcopy_ss_16x32_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x16].copy_pp = (copy_pp_t)PFX(blockcopy_ss_16x16_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x24].copy_pp = (copy_pp_t)PFX(blockcopy_ss_16x24_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x32].copy_pp = (copy_pp_t)PFX(blockcopy_ss_16x32_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x64].copy_pp = (copy_pp_t)PFX(blockcopy_ss_16x64_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x8].copy_pp = (copy_pp_t)PFX(blockcopy_ss_16x8_avx);

        // 24 X N
        p.pu[LUMA_24x32].copy_pp = (copy_pp_t)PFX(blockcopy_ss_24x32_avx);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_24x32].copy_pp = (copy_pp_t)PFX(blockcopy_ss_24x32_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_24x64].copy_pp = (copy_pp_t)PFX(blockcopy_ss_24x64_avx);

        // 32 x N
        p.pu[LUMA_32x8].copy_pp = (copy_pp_t)PFX(blockcopy_ss_32x8_avx);
        p.pu[LUMA_32x16].copy_pp = (copy_pp_t)PFX(blockcopy_ss_32x16_avx);
        p.pu[LUMA_32x24].copy_pp = (copy_pp_t)PFX(blockcopy_ss_32x24_avx);
        p.pu[LUMA_32x32].copy_pp = (copy_pp_t)PFX(blockcopy_ss_32x32_avx);
        p.pu[LUMA_32x64].copy_pp = (copy_pp_t)PFX(blockcopy_ss_32x64_avx);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x8].copy_pp = (copy_pp_t)PFX(blockcopy_ss_32x8_avx);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x16].copy_pp = (copy_pp_t)PFX(blockcopy_ss_32x16_avx);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x24].copy_pp = (copy_pp_t)PFX(blockcopy_ss_32x24_avx);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x32].copy_pp = (copy_pp_t)PFX(blockcopy_ss_32x32_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x16].copy_pp = (copy_pp_t)PFX(blockcopy_ss_32x16_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x32].copy_pp = (copy_pp_t)PFX(blockcopy_ss_32x32_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x48].copy_pp = (copy_pp_t)PFX(blockcopy_ss_32x48_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x64].copy_pp = (copy_pp_t)PFX(blockcopy_ss_32x64_avx);

        // 48 X 64
        p.pu[LUMA_48x64].copy_pp = (copy_pp_t)PFX(blockcopy_ss_48x64_avx);

        // copy_ss primitives
        // 16 X N
        p.cu[BLOCK_16x16].copy_ss = PFX(blockcopy_ss_16x16_avx);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_16x16].copy_ss = PFX(blockcopy_ss_16x16_avx);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_16x32].copy_ss = PFX(blockcopy_ss_16x32_avx);

        // 32 X N
        p.cu[BLOCK_32x32].copy_ss = PFX(blockcopy_ss_32x32_avx);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_32x32].copy_ss = PFX(blockcopy_ss_32x32_avx);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_32x64].copy_ss = PFX(blockcopy_ss_32x64_avx);

        // 64 X N
        p.cu[BLOCK_64x64].copy_ss = PFX(blockcopy_ss_64x64_avx);

        // copy_ps primitives
        // 16 X N
        p.cu[BLOCK_16x16].copy_ps = (copy_ps_t)PFX(blockcopy_ss_16x16_avx);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_16x16].copy_ps = (copy_ps_t)PFX(blockcopy_ss_16x16_avx);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_16x32].copy_ps = (copy_ps_t)PFX(blockcopy_ss_16x32_avx);

        // 32 X N
        p.cu[BLOCK_32x32].copy_ps = (copy_ps_t)PFX(blockcopy_ss_32x32_avx);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_32x32].copy_ps = (copy_ps_t)PFX(blockcopy_ss_32x32_avx);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_32x64].copy_ps = (copy_ps_t)PFX(blockcopy_ss_32x64_avx);

        // 64 X N
        p.cu[BLOCK_64x64].copy_ps = (copy_ps_t)PFX(blockcopy_ss_64x64_avx);

        // copy_sp primitives
        // 16 X N
        p.cu[BLOCK_16x16].copy_sp = (copy_sp_t)PFX(blockcopy_ss_16x16_avx);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_16x16].copy_sp = (copy_sp_t)PFX(blockcopy_ss_16x16_avx);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_16x32].copy_sp = (copy_sp_t)PFX(blockcopy_ss_16x32_avx);

        // 32 X N
        p.cu[BLOCK_32x32].copy_sp = (copy_sp_t)PFX(blockcopy_ss_32x32_avx);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_32x32].copy_sp = (copy_sp_t)PFX(blockcopy_ss_32x32_avx);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_32x64].copy_sp = (copy_sp_t)PFX(blockcopy_ss_32x64_avx);

        // 64 X N
        p.cu[BLOCK_64x64].copy_sp = (copy_sp_t)PFX(blockcopy_ss_64x64_avx);

        p.frameInitLowres = PFX(frame_init_lowres_core_avx);

        p.pu[LUMA_64x16].copy_pp = (copy_pp_t)PFX(blockcopy_ss_64x16_avx);
        p.pu[LUMA_64x32].copy_pp = (copy_pp_t)PFX(blockcopy_ss_64x32_avx);
        p.pu[LUMA_64x48].copy_pp = (copy_pp_t)PFX(blockcopy_ss_64x48_avx);
        p.pu[LUMA_64x64].copy_pp = (copy_pp_t)PFX(blockcopy_ss_64x64_avx);
        p.propagateCost = PFX(mbtree_propagate_cost_avx);
    }
    if (cpuMask & X265_CPU_XOP)
    {
        //p.pu[LUMA_4x4].satd = p.cu[BLOCK_4x4].sa8d = PFX(pixel_satd_4x4_xop); this one is broken
        ALL_LUMA_PU(satd, pixel_satd, xop);
#if X265_DEPTH <= 10
        ASSIGN_SA8D(xop);
#endif
        LUMA_VAR(xop);
        p.frameInitLowres = PFX(frame_init_lowres_core_xop);
    }
    if (cpuMask & X265_CPU_AVX2)
    {
#if X265_DEPTH == 12
        ASSIGN_SA8D(avx2);
#endif
        p.cu[BLOCK_4x4].intra_filter = PFX(intra_filter_4x4_avx2);

        // TODO: the planecopy_sp is really planecopy_SC now, must be fix it
        //p.planecopy_sp = PFX(downShift_16_avx2);
        p.planecopy_sp_shl = PFX(upShift_16_avx2);

        p.saoCuOrgE0 = PFX(saoCuOrgE0_avx2);
        p.saoCuOrgE1 = PFX(saoCuOrgE1_avx2);
        p.saoCuOrgE1_2Rows = PFX(saoCuOrgE1_2Rows_avx2);
        p.saoCuOrgE2[0] = PFX(saoCuOrgE2_avx2);
        p.saoCuOrgE2[1] = PFX(saoCuOrgE2_32_avx2);
        p.saoCuOrgE3[0] = PFX(saoCuOrgE3_avx2);
        p.saoCuOrgE3[1] = PFX(saoCuOrgE3_32_avx2);
        p.saoCuOrgB0 = PFX(saoCuOrgB0_avx2);

        p.cu[BLOCK_16x16].intra_pred[2]     = PFX(intra_pred_ang16_2_avx2);
        p.cu[BLOCK_16x16].intra_pred[3]     = PFX(intra_pred_ang16_3_avx2);
        p.cu[BLOCK_16x16].intra_pred[4]     = PFX(intra_pred_ang16_4_avx2);
        p.cu[BLOCK_16x16].intra_pred[5]     = PFX(intra_pred_ang16_5_avx2);
        p.cu[BLOCK_16x16].intra_pred[6]     = PFX(intra_pred_ang16_6_avx2);
        p.cu[BLOCK_16x16].intra_pred[7]     = PFX(intra_pred_ang16_7_avx2);
        p.cu[BLOCK_16x16].intra_pred[8]     = PFX(intra_pred_ang16_8_avx2);
        p.cu[BLOCK_16x16].intra_pred[9]     = PFX(intra_pred_ang16_9_avx2);
        p.cu[BLOCK_16x16].intra_pred[10]    = PFX(intra_pred_ang16_10_avx2);
        p.cu[BLOCK_16x16].intra_pred[11]    = PFX(intra_pred_ang16_11_avx2);
        p.cu[BLOCK_16x16].intra_pred[12]    = PFX(intra_pred_ang16_12_avx2);
        p.cu[BLOCK_16x16].intra_pred[13]    = PFX(intra_pred_ang16_13_avx2);
        p.cu[BLOCK_16x16].intra_pred[14]    = PFX(intra_pred_ang16_14_avx2);
        p.cu[BLOCK_16x16].intra_pred[15]    = PFX(intra_pred_ang16_15_avx2);
        p.cu[BLOCK_16x16].intra_pred[16]    = PFX(intra_pred_ang16_16_avx2);
        p.cu[BLOCK_16x16].intra_pred[17]    = PFX(intra_pred_ang16_17_avx2);
        p.cu[BLOCK_16x16].intra_pred[18]    = PFX(intra_pred_ang16_18_avx2);
        p.cu[BLOCK_16x16].intra_pred[19]    = PFX(intra_pred_ang16_19_avx2);
        p.cu[BLOCK_16x16].intra_pred[20]    = PFX(intra_pred_ang16_20_avx2);
        p.cu[BLOCK_16x16].intra_pred[21]    = PFX(intra_pred_ang16_21_avx2);
        p.cu[BLOCK_16x16].intra_pred[22]    = PFX(intra_pred_ang16_22_avx2);
        p.cu[BLOCK_16x16].intra_pred[23]    = PFX(intra_pred_ang16_23_avx2);
        p.cu[BLOCK_16x16].intra_pred[24]    = PFX(intra_pred_ang16_24_avx2);
        p.cu[BLOCK_16x16].intra_pred[25]    = PFX(intra_pred_ang16_25_avx2);
        p.cu[BLOCK_16x16].intra_pred[26]    = PFX(intra_pred_ang16_26_avx2);
        p.cu[BLOCK_16x16].intra_pred[27]    = PFX(intra_pred_ang16_27_avx2);
        p.cu[BLOCK_16x16].intra_pred[28]    = PFX(intra_pred_ang16_28_avx2);
        p.cu[BLOCK_16x16].intra_pred[29]    = PFX(intra_pred_ang16_29_avx2);
        p.cu[BLOCK_16x16].intra_pred[30]    = PFX(intra_pred_ang16_30_avx2);
        p.cu[BLOCK_16x16].intra_pred[31]    = PFX(intra_pred_ang16_31_avx2);
        p.cu[BLOCK_16x16].intra_pred[32]    = PFX(intra_pred_ang16_32_avx2);
        p.cu[BLOCK_16x16].intra_pred[33]    = PFX(intra_pred_ang16_33_avx2);
        p.cu[BLOCK_16x16].intra_pred[34]    = PFX(intra_pred_ang16_2_avx2);

        p.cu[BLOCK_32x32].intra_pred[2]     = PFX(intra_pred_ang32_2_avx2);
        p.cu[BLOCK_32x32].intra_pred[3]     = PFX(intra_pred_ang32_3_avx2);
        p.cu[BLOCK_32x32].intra_pred[4]     = PFX(intra_pred_ang32_4_avx2);
        p.cu[BLOCK_32x32].intra_pred[5]     = PFX(intra_pred_ang32_5_avx2);
        p.cu[BLOCK_32x32].intra_pred[6]     = PFX(intra_pred_ang32_6_avx2);
        p.cu[BLOCK_32x32].intra_pred[7]     = PFX(intra_pred_ang32_7_avx2);
        p.cu[BLOCK_32x32].intra_pred[8]     = PFX(intra_pred_ang32_8_avx2);
        p.cu[BLOCK_32x32].intra_pred[9]     = PFX(intra_pred_ang32_9_avx2);
        p.cu[BLOCK_32x32].intra_pred[10]    = PFX(intra_pred_ang32_10_avx2);
        p.cu[BLOCK_32x32].intra_pred[11]    = PFX(intra_pred_ang32_11_avx2);
        p.cu[BLOCK_32x32].intra_pred[12]    = PFX(intra_pred_ang32_12_avx2);
        p.cu[BLOCK_32x32].intra_pred[13]    = PFX(intra_pred_ang32_13_avx2);
        p.cu[BLOCK_32x32].intra_pred[14]    = PFX(intra_pred_ang32_14_avx2);
        p.cu[BLOCK_32x32].intra_pred[15]    = PFX(intra_pred_ang32_15_avx2);
        p.cu[BLOCK_32x32].intra_pred[16]    = PFX(intra_pred_ang32_16_avx2);
        p.cu[BLOCK_32x32].intra_pred[17]    = PFX(intra_pred_ang32_17_avx2);
        p.cu[BLOCK_32x32].intra_pred[18]    = PFX(intra_pred_ang32_18_avx2);
        p.cu[BLOCK_32x32].intra_pred[19]    = PFX(intra_pred_ang32_19_avx2);
        p.cu[BLOCK_32x32].intra_pred[20]    = PFX(intra_pred_ang32_20_avx2);
        p.cu[BLOCK_32x32].intra_pred[21]    = PFX(intra_pred_ang32_21_avx2);
        p.cu[BLOCK_32x32].intra_pred[22]    = PFX(intra_pred_ang32_22_avx2);
        p.cu[BLOCK_32x32].intra_pred[23]    = PFX(intra_pred_ang32_23_avx2);
        p.cu[BLOCK_32x32].intra_pred[24]    = PFX(intra_pred_ang32_24_avx2);
        p.cu[BLOCK_32x32].intra_pred[25]    = PFX(intra_pred_ang32_25_avx2);
        p.cu[BLOCK_32x32].intra_pred[26]    = PFX(intra_pred_ang32_26_avx2);
        p.cu[BLOCK_32x32].intra_pred[27]    = PFX(intra_pred_ang32_27_avx2);
        p.cu[BLOCK_32x32].intra_pred[28]    = PFX(intra_pred_ang32_28_avx2);
        p.cu[BLOCK_32x32].intra_pred[29]    = PFX(intra_pred_ang32_29_avx2);
        p.cu[BLOCK_32x32].intra_pred[30]    = PFX(intra_pred_ang32_30_avx2);
        p.cu[BLOCK_32x32].intra_pred[31]    = PFX(intra_pred_ang32_31_avx2);
        p.cu[BLOCK_32x32].intra_pred[32]    = PFX(intra_pred_ang32_32_avx2);
        p.cu[BLOCK_32x32].intra_pred[33]    = PFX(intra_pred_ang32_33_avx2);
        p.cu[BLOCK_32x32].intra_pred[34]    = PFX(intra_pred_ang32_2_avx2);

        p.pu[LUMA_12x16].pixelavg_pp = PFX(pixel_avg_12x16_avx2);
        p.pu[LUMA_16x4].pixelavg_pp = PFX(pixel_avg_16x4_avx2);
        p.pu[LUMA_16x8].pixelavg_pp = PFX(pixel_avg_16x8_avx2);
        p.pu[LUMA_16x12].pixelavg_pp = PFX(pixel_avg_16x12_avx2);
        p.pu[LUMA_16x16].pixelavg_pp = PFX(pixel_avg_16x16_avx2);
        p.pu[LUMA_16x32].pixelavg_pp = PFX(pixel_avg_16x32_avx2);
        p.pu[LUMA_16x64].pixelavg_pp = PFX(pixel_avg_16x64_avx2);
        p.pu[LUMA_24x32].pixelavg_pp = PFX(pixel_avg_24x32_avx2);
        p.pu[LUMA_32x8].pixelavg_pp = PFX(pixel_avg_32x8_avx2);
        p.pu[LUMA_32x16].pixelavg_pp = PFX(pixel_avg_32x16_avx2);
        p.pu[LUMA_32x24].pixelavg_pp = PFX(pixel_avg_32x24_avx2);
        p.pu[LUMA_32x32].pixelavg_pp = PFX(pixel_avg_32x32_avx2);
        p.pu[LUMA_32x64].pixelavg_pp = PFX(pixel_avg_32x64_avx2);
        p.pu[LUMA_64x16].pixelavg_pp = PFX(pixel_avg_64x16_avx2);
        p.pu[LUMA_64x32].pixelavg_pp = PFX(pixel_avg_64x32_avx2);
        p.pu[LUMA_64x48].pixelavg_pp = PFX(pixel_avg_64x48_avx2);
        p.pu[LUMA_64x64].pixelavg_pp = PFX(pixel_avg_64x64_avx2);
        p.pu[LUMA_48x64].pixelavg_pp = PFX(pixel_avg_48x64_avx2);

        p.pu[LUMA_8x4].addAvg   = PFX(addAvg_8x4_avx2);
        p.pu[LUMA_8x8].addAvg   = PFX(addAvg_8x8_avx2);
        p.pu[LUMA_8x16].addAvg  = PFX(addAvg_8x16_avx2);
        p.pu[LUMA_8x32].addAvg  = PFX(addAvg_8x32_avx2);
        p.pu[LUMA_12x16].addAvg = PFX(addAvg_12x16_avx2);
        p.pu[LUMA_16x4].addAvg  = PFX(addAvg_16x4_avx2);
        p.pu[LUMA_16x8].addAvg  = PFX(addAvg_16x8_avx2);
        p.pu[LUMA_16x12].addAvg = PFX(addAvg_16x12_avx2);
        p.pu[LUMA_16x16].addAvg = PFX(addAvg_16x16_avx2);
        p.pu[LUMA_16x32].addAvg = PFX(addAvg_16x32_avx2);
        p.pu[LUMA_16x64].addAvg = PFX(addAvg_16x64_avx2);
        p.pu[LUMA_24x32].addAvg = PFX(addAvg_24x32_avx2);
        p.pu[LUMA_32x8].addAvg  = PFX(addAvg_32x8_avx2);
        p.pu[LUMA_32x16].addAvg = PFX(addAvg_32x16_avx2);
        p.pu[LUMA_32x24].addAvg = PFX(addAvg_32x24_avx2);
        p.pu[LUMA_32x32].addAvg = PFX(addAvg_32x32_avx2);
        p.pu[LUMA_32x64].addAvg = PFX(addAvg_32x64_avx2);
        p.pu[LUMA_48x64].addAvg = PFX(addAvg_48x64_avx2);
        p.pu[LUMA_64x16].addAvg = PFX(addAvg_64x16_avx2);
        p.pu[LUMA_64x32].addAvg = PFX(addAvg_64x32_avx2);
        p.pu[LUMA_64x48].addAvg = PFX(addAvg_64x48_avx2);
        p.pu[LUMA_64x64].addAvg = PFX(addAvg_64x64_avx2);

        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x2].addAvg   = PFX(addAvg_8x2_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x4].addAvg   = PFX(addAvg_8x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x6].addAvg   = PFX(addAvg_8x6_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x8].addAvg   = PFX(addAvg_8x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x16].addAvg  = PFX(addAvg_8x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x32].addAvg  = PFX(addAvg_8x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_12x16].addAvg = PFX(addAvg_12x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x4].addAvg  = PFX(addAvg_16x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x8].addAvg  = PFX(addAvg_16x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x12].addAvg = PFX(addAvg_16x12_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x16].addAvg = PFX(addAvg_16x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x32].addAvg = PFX(addAvg_16x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x8].addAvg  = PFX(addAvg_32x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x16].addAvg = PFX(addAvg_32x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x24].addAvg = PFX(addAvg_32x24_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x32].addAvg = PFX(addAvg_32x32_avx2);

        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x16].addAvg = PFX(addAvg_8x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x32].addAvg = PFX(addAvg_16x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x64].addAvg = PFX(addAvg_32x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x8].addAvg = PFX(addAvg_8x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x16].addAvg = PFX(addAvg_16x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x32].addAvg = PFX(addAvg_8x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x32].addAvg = PFX(addAvg_32x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x64].addAvg = PFX(addAvg_16x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x12].addAvg = PFX(addAvg_8x12_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x4].addAvg = PFX(addAvg_8x4_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x24].addAvg = PFX(addAvg_16x24_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x8].addAvg = PFX(addAvg_16x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x64].addAvg = PFX(addAvg_8x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_24x64].addAvg = PFX(addAvg_24x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_12x32].addAvg = PFX(addAvg_12x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x16].addAvg = PFX(addAvg_32x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x48].addAvg = PFX(addAvg_32x48_avx2);

        p.cu[BLOCK_4x4].psy_cost_pp = PFX(psyCost_pp_4x4_avx2);
        p.cu[BLOCK_16x16].intra_pred[PLANAR_IDX] = PFX(intra_pred_planar16_avx2);
        p.cu[BLOCK_32x32].intra_pred[PLANAR_IDX] = PFX(intra_pred_planar32_avx2);

        p.cu[BLOCK_8x8].psy_cost_pp = PFX(psyCost_pp_8x8_avx2);
        p.cu[BLOCK_16x16].psy_cost_pp = PFX(psyCost_pp_16x16_avx2);
        p.cu[BLOCK_32x32].psy_cost_pp = PFX(psyCost_pp_32x32_avx2);
        p.cu[BLOCK_64x64].psy_cost_pp = PFX(psyCost_pp_64x64_avx2);

        p.cu[BLOCK_16x16].intra_pred[DC_IDX] = PFX(intra_pred_dc16_avx2);
        p.cu[BLOCK_32x32].intra_pred[DC_IDX] = PFX(intra_pred_dc32_avx2);

        p.pu[LUMA_48x64].satd = PFX(pixel_satd_48x64_avx2);

        p.pu[LUMA_64x16].satd = PFX(pixel_satd_64x16_avx2);
        p.pu[LUMA_64x32].satd = PFX(pixel_satd_64x32_avx2);
        p.pu[LUMA_64x48].satd = PFX(pixel_satd_64x48_avx2);
        p.pu[LUMA_64x64].satd = PFX(pixel_satd_64x64_avx2);

        p.pu[LUMA_32x8].satd = PFX(pixel_satd_32x8_avx2);
        p.pu[LUMA_32x16].satd = PFX(pixel_satd_32x16_avx2);
        p.pu[LUMA_32x24].satd = PFX(pixel_satd_32x24_avx2);
        p.pu[LUMA_32x32].satd = PFX(pixel_satd_32x32_avx2);
        p.pu[LUMA_32x64].satd = PFX(pixel_satd_32x64_avx2);

        p.pu[LUMA_16x4].satd = PFX(pixel_satd_16x4_avx2);
        p.pu[LUMA_16x8].satd = PFX(pixel_satd_16x8_avx2);
        p.pu[LUMA_16x12].satd = PFX(pixel_satd_16x12_avx2);
        p.pu[LUMA_16x16].satd = PFX(pixel_satd_16x16_avx2);
        p.pu[LUMA_16x32].satd = PFX(pixel_satd_16x32_avx2);
        p.pu[LUMA_16x64].satd = PFX(pixel_satd_16x64_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x16].satd = PFX(pixel_satd_16x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x8].satd = PFX(pixel_satd_16x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x32].satd = PFX(pixel_satd_16x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x12].satd = PFX(pixel_satd_16x12_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x4].satd = PFX(pixel_satd_16x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x32].satd = PFX(pixel_satd_32x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x16].satd = PFX(pixel_satd_32x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x24].satd = PFX(pixel_satd_32x24_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x8].satd = PFX(pixel_satd_32x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x32].satd = PFX(pixel_satd_16x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x64].satd = PFX(pixel_satd_32x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x16].satd = PFX(pixel_satd_16x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x32].satd = PFX(pixel_satd_32x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x64].satd = PFX(pixel_satd_16x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x8].satd = PFX(pixel_satd_16x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x16].satd = PFX(pixel_satd_32x16_avx2);

        p.cu[BLOCK_16x16].ssd_s = PFX(pixel_ssd_s_16_avx2);
        p.cu[BLOCK_32x32].ssd_s = PFX(pixel_ssd_s_32_avx2);

        p.cu[BLOCK_16x16].sse_ss = (pixel_sse_ss_t)PFX(pixel_ssd_16x16_avx2);
        p.cu[BLOCK_32x32].sse_ss = (pixel_sse_ss_t)PFX(pixel_ssd_32x32_avx2);
        p.cu[BLOCK_64x64].sse_ss = (pixel_sse_ss_t)PFX(pixel_ssd_64x64_avx2);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_16x16].sse_pp = (pixel_sse_t)PFX(pixel_ssd_16x16_avx2);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_32x32].sse_pp = (pixel_sse_t)PFX(pixel_ssd_32x32_avx2);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_16x32].sse_pp = (pixel_sse_t)PFX(pixel_ssd_ss_16x32_avx2);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_32x64].sse_pp = (pixel_sse_t)PFX(pixel_ssd_ss_32x64_avx2);
        p.quant = PFX(quant_avx2);
        p.nquant = PFX(nquant_avx2);
        p.dequant_normal  = PFX(dequant_normal_avx2);
        p.dequant_scaling = PFX(dequant_scaling_avx2);
        p.dst4x4 = PFX(dst4_avx2);
        p.idst4x4 = PFX(idst4_avx2);
        p.denoiseDct = PFX(denoise_dct_avx2);

        p.scale1D_128to64 = PFX(scale1D_128to64_avx2);
        p.scale2D_64to32 = PFX(scale2D_64to32_avx2);

        p.weight_pp = PFX(weight_pp_avx2);
        p.weight_sp = PFX(weight_sp_avx2);
        p.sign = PFX(calSign_avx2);
        p.planecopy_cp = PFX(upShift_8_avx2);

        p.cu[BLOCK_16x16].calcresidual = PFX(getResidual16_avx2);
        p.cu[BLOCK_32x32].calcresidual = PFX(getResidual32_avx2);

        p.cu[BLOCK_16x16].blockfill_s = PFX(blockfill_s_16x16_avx2);
        p.cu[BLOCK_32x32].blockfill_s = PFX(blockfill_s_32x32_avx2);

        ALL_LUMA_TU(count_nonzero, count_nonzero, avx2);
        ALL_LUMA_TU_S(cpy1Dto2D_shl, cpy1Dto2D_shl_, avx2);
        ALL_LUMA_TU_S(cpy1Dto2D_shr, cpy1Dto2D_shr_, avx2);

        p.cu[BLOCK_8x8].copy_cnt = PFX(copy_cnt_8_avx2);
        p.cu[BLOCK_16x16].copy_cnt = PFX(copy_cnt_16_avx2);
        p.cu[BLOCK_32x32].copy_cnt = PFX(copy_cnt_32_avx2);

        p.cu[BLOCK_8x8].cpy2Dto1D_shl = PFX(cpy2Dto1D_shl_8_avx2);
        p.cu[BLOCK_16x16].cpy2Dto1D_shl = PFX(cpy2Dto1D_shl_16_avx2);
        p.cu[BLOCK_32x32].cpy2Dto1D_shl = PFX(cpy2Dto1D_shl_32_avx2);

        p.cu[BLOCK_8x8].cpy2Dto1D_shr = PFX(cpy2Dto1D_shr_8_avx2);
        p.cu[BLOCK_16x16].cpy2Dto1D_shr = PFX(cpy2Dto1D_shr_16_avx2);
        p.cu[BLOCK_32x32].cpy2Dto1D_shr = PFX(cpy2Dto1D_shr_32_avx2);

        ALL_LUMA_TU_S(idct, idct, avx2);
        ALL_LUMA_TU_S(dct, dct, avx2);

        ALL_LUMA_CU_S(transpose, transpose, avx2);

        ALL_LUMA_PU(luma_vpp, interp_8tap_vert_pp, avx2);
        ALL_LUMA_PU(luma_vps, interp_8tap_vert_ps, avx2);
        ALL_LUMA_PU(luma_vsp, interp_8tap_vert_sp, avx2);
        ALL_LUMA_PU(luma_vss, interp_8tap_vert_ss, avx2);
        p.pu[LUMA_4x4].luma_vsp = PFX(interp_8tap_vert_sp_4x4_avx2);               // since ALL_LUMA_PU didn't declare 4x4 size, calling separately luma_vsp function to use 

        p.cu[BLOCK_16x16].add_ps = PFX(pixel_add_ps_16x16_avx2);
        p.cu[BLOCK_32x32].add_ps = PFX(pixel_add_ps_32x32_avx2);
        p.cu[BLOCK_64x64].add_ps = PFX(pixel_add_ps_64x64_avx2);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_16x16].add_ps = PFX(pixel_add_ps_16x16_avx2);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_32x32].add_ps = PFX(pixel_add_ps_32x32_avx2);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_16x32].add_ps = PFX(pixel_add_ps_16x32_avx2);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_32x64].add_ps = PFX(pixel_add_ps_32x64_avx2);

        p.cu[BLOCK_16x16].sub_ps = PFX(pixel_sub_ps_16x16_avx2);
        p.cu[BLOCK_32x32].sub_ps = PFX(pixel_sub_ps_32x32_avx2);
        p.cu[BLOCK_64x64].sub_ps = PFX(pixel_sub_ps_64x64_avx2);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_16x16].sub_ps = PFX(pixel_sub_ps_16x16_avx2);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_32x32].sub_ps = PFX(pixel_sub_ps_32x32_avx2);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_16x32].sub_ps = PFX(pixel_sub_ps_16x32_avx2);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_32x64].sub_ps = PFX(pixel_sub_ps_32x64_avx2);

        p.pu[LUMA_16x4].sad = PFX(pixel_sad_16x4_avx2);
        p.pu[LUMA_16x8].sad = PFX(pixel_sad_16x8_avx2);
        p.pu[LUMA_16x12].sad = PFX(pixel_sad_16x12_avx2);
        p.pu[LUMA_16x16].sad = PFX(pixel_sad_16x16_avx2);
        p.pu[LUMA_16x32].sad = PFX(pixel_sad_16x32_avx2);
        p.pu[LUMA_16x64].sad = PFX(pixel_sad_16x64_avx2);
        p.pu[LUMA_32x8].sad = PFX(pixel_sad_32x8_avx2);
        p.pu[LUMA_32x16].sad = PFX(pixel_sad_32x16_avx2);
        p.pu[LUMA_32x24].sad = PFX(pixel_sad_32x24_avx2);
        p.pu[LUMA_32x32].sad = PFX(pixel_sad_32x32_avx2);
        p.pu[LUMA_32x64].sad = PFX(pixel_sad_32x64_avx2);
        p.pu[LUMA_48x64].sad = PFX(pixel_sad_48x64_avx2);
        p.pu[LUMA_64x16].sad = PFX(pixel_sad_64x16_avx2);
        p.pu[LUMA_64x32].sad = PFX(pixel_sad_64x32_avx2);
        p.pu[LUMA_64x48].sad = PFX(pixel_sad_64x48_avx2);
        p.pu[LUMA_64x64].sad = PFX(pixel_sad_64x64_avx2);

        p.pu[LUMA_16x4].sad_x3 = PFX(pixel_sad_x3_16x4_avx2);
        p.pu[LUMA_16x8].sad_x3 = PFX(pixel_sad_x3_16x8_avx2);
        p.pu[LUMA_16x12].sad_x3 = PFX(pixel_sad_x3_16x12_avx2);
        p.pu[LUMA_16x16].sad_x3 = PFX(pixel_sad_x3_16x16_avx2);
        p.pu[LUMA_16x32].sad_x3 = PFX(pixel_sad_x3_16x32_avx2);
        p.pu[LUMA_16x64].sad_x3 = PFX(pixel_sad_x3_16x64_avx2);
        p.pu[LUMA_32x8].sad_x3 = PFX(pixel_sad_x3_32x8_avx2);
        p.pu[LUMA_32x16].sad_x3 = PFX(pixel_sad_x3_32x16_avx2);
        p.pu[LUMA_32x24].sad_x3 = PFX(pixel_sad_x3_32x24_avx2);
        p.pu[LUMA_32x32].sad_x3 = PFX(pixel_sad_x3_32x32_avx2);
        p.pu[LUMA_32x64].sad_x3 = PFX(pixel_sad_x3_32x64_avx2);
        p.pu[LUMA_48x64].sad_x3 = PFX(pixel_sad_x3_48x64_avx2);
        p.pu[LUMA_64x16].sad_x3 = PFX(pixel_sad_x3_64x16_avx2);
        p.pu[LUMA_64x32].sad_x3 = PFX(pixel_sad_x3_64x32_avx2);
        p.pu[LUMA_64x48].sad_x3 = PFX(pixel_sad_x3_64x48_avx2);
        p.pu[LUMA_64x64].sad_x3 = PFX(pixel_sad_x3_64x64_avx2);

        p.pu[LUMA_16x4].sad_x4 = PFX(pixel_sad_x4_16x4_avx2);
        p.pu[LUMA_16x8].sad_x4 = PFX(pixel_sad_x4_16x8_avx2);
        p.pu[LUMA_16x12].sad_x4 = PFX(pixel_sad_x4_16x12_avx2);
        p.pu[LUMA_16x16].sad_x4 = PFX(pixel_sad_x4_16x16_avx2);
        p.pu[LUMA_16x32].sad_x4 = PFX(pixel_sad_x4_16x32_avx2);
        p.pu[LUMA_16x64].sad_x4 = PFX(pixel_sad_x4_16x64_avx2);
        p.pu[LUMA_32x8].sad_x4 = PFX(pixel_sad_x4_32x8_avx2);
        p.pu[LUMA_32x16].sad_x4 = PFX(pixel_sad_x4_32x16_avx2);
        p.pu[LUMA_32x24].sad_x4 = PFX(pixel_sad_x4_32x24_avx2);
        p.pu[LUMA_32x32].sad_x4 = PFX(pixel_sad_x4_32x32_avx2);
        p.pu[LUMA_32x64].sad_x4 = PFX(pixel_sad_x4_32x64_avx2);
        p.pu[LUMA_48x64].sad_x4 = PFX(pixel_sad_x4_48x64_avx2);
        p.pu[LUMA_64x16].sad_x4 = PFX(pixel_sad_x4_64x16_avx2);
        p.pu[LUMA_64x32].sad_x4 = PFX(pixel_sad_x4_64x32_avx2);
        p.pu[LUMA_64x48].sad_x4 = PFX(pixel_sad_x4_64x48_avx2);
        p.pu[LUMA_64x64].sad_x4 = PFX(pixel_sad_x4_64x64_avx2);

        p.pu[LUMA_16x4].convert_p2s = PFX(filterPixelToShort_16x4_avx2);
        p.pu[LUMA_16x8].convert_p2s = PFX(filterPixelToShort_16x8_avx2);
        p.pu[LUMA_16x12].convert_p2s = PFX(filterPixelToShort_16x12_avx2);
        p.pu[LUMA_16x16].convert_p2s = PFX(filterPixelToShort_16x16_avx2);
        p.pu[LUMA_16x32].convert_p2s = PFX(filterPixelToShort_16x32_avx2);
        p.pu[LUMA_16x64].convert_p2s = PFX(filterPixelToShort_16x64_avx2);
        p.pu[LUMA_32x8].convert_p2s = PFX(filterPixelToShort_32x8_avx2);
        p.pu[LUMA_32x16].convert_p2s = PFX(filterPixelToShort_32x16_avx2);
        p.pu[LUMA_32x24].convert_p2s = PFX(filterPixelToShort_32x24_avx2);
        p.pu[LUMA_32x32].convert_p2s = PFX(filterPixelToShort_32x32_avx2);
        p.pu[LUMA_32x64].convert_p2s = PFX(filterPixelToShort_32x64_avx2);
        p.pu[LUMA_64x16].convert_p2s = PFX(filterPixelToShort_64x16_avx2);
        p.pu[LUMA_64x32].convert_p2s = PFX(filterPixelToShort_64x32_avx2);
        p.pu[LUMA_64x48].convert_p2s = PFX(filterPixelToShort_64x48_avx2);
        p.pu[LUMA_64x64].convert_p2s = PFX(filterPixelToShort_64x64_avx2);
        p.pu[LUMA_24x32].convert_p2s = PFX(filterPixelToShort_24x32_avx2);
        p.pu[LUMA_48x64].convert_p2s = PFX(filterPixelToShort_48x64_avx2);

        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x4].p2s = PFX(filterPixelToShort_16x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x8].p2s = PFX(filterPixelToShort_16x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x12].p2s = PFX(filterPixelToShort_16x12_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x16].p2s = PFX(filterPixelToShort_16x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x32].p2s = PFX(filterPixelToShort_16x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_24x32].p2s = PFX(filterPixelToShort_24x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x8].p2s = PFX(filterPixelToShort_32x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x16].p2s = PFX(filterPixelToShort_32x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x24].p2s = PFX(filterPixelToShort_32x24_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x32].p2s = PFX(filterPixelToShort_32x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x8].p2s = PFX(filterPixelToShort_16x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x16].p2s = PFX(filterPixelToShort_16x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x24].p2s = PFX(filterPixelToShort_16x24_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x32].p2s = PFX(filterPixelToShort_16x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x64].p2s = PFX(filterPixelToShort_16x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_24x64].p2s = PFX(filterPixelToShort_24x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x16].p2s = PFX(filterPixelToShort_32x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x32].p2s = PFX(filterPixelToShort_32x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x48].p2s = PFX(filterPixelToShort_32x48_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x64].p2s = PFX(filterPixelToShort_32x64_avx2);

        p.pu[LUMA_4x4].luma_hps = PFX(interp_8tap_horiz_ps_4x4_avx2);
        p.pu[LUMA_4x8].luma_hps = PFX(interp_8tap_horiz_ps_4x8_avx2);
        p.pu[LUMA_4x16].luma_hps = PFX(interp_8tap_horiz_ps_4x16_avx2);
        p.pu[LUMA_8x8].luma_hps = PFX(interp_8tap_horiz_ps_8x8_avx2);
        p.pu[LUMA_8x4].luma_hps = PFX(interp_8tap_horiz_ps_8x4_avx2);
        p.pu[LUMA_8x16].luma_hps = PFX(interp_8tap_horiz_ps_8x16_avx2);
        p.pu[LUMA_8x32].luma_hps = PFX(interp_8tap_horiz_ps_8x32_avx2);
        p.pu[LUMA_16x4].luma_hps = PFX(interp_8tap_horiz_ps_16x4_avx2);
        p.pu[LUMA_16x8].luma_hps = PFX(interp_8tap_horiz_ps_16x8_avx2);
        p.pu[LUMA_16x12].luma_hps = PFX(interp_8tap_horiz_ps_16x12_avx2);
        p.pu[LUMA_16x16].luma_hps = PFX(interp_8tap_horiz_ps_16x16_avx2);
        p.pu[LUMA_16x32].luma_hps = PFX(interp_8tap_horiz_ps_16x32_avx2);
        p.pu[LUMA_16x64].luma_hps = PFX(interp_8tap_horiz_ps_16x64_avx2);
        p.pu[LUMA_32x8].luma_hps = PFX(interp_8tap_horiz_ps_32x8_avx2);
        p.pu[LUMA_32x16].luma_hps = PFX(interp_8tap_horiz_ps_32x16_avx2);
        p.pu[LUMA_32x32].luma_hps = PFX(interp_8tap_horiz_ps_32x32_avx2);
        p.pu[LUMA_32x24].luma_hps = PFX(interp_8tap_horiz_ps_32x24_avx2);
        p.pu[LUMA_32x64].luma_hps = PFX(interp_8tap_horiz_ps_32x64_avx2);
        p.pu[LUMA_64x64].luma_hps = PFX(interp_8tap_horiz_ps_64x64_avx2);
        p.pu[LUMA_64x16].luma_hps = PFX(interp_8tap_horiz_ps_64x16_avx2);
        p.pu[LUMA_64x32].luma_hps = PFX(interp_8tap_horiz_ps_64x32_avx2);
        p.pu[LUMA_64x48].luma_hps = PFX(interp_8tap_horiz_ps_64x48_avx2);
        p.pu[LUMA_48x64].luma_hps = PFX(interp_8tap_horiz_ps_48x64_avx2);
        p.pu[LUMA_24x32].luma_hps = PFX(interp_8tap_horiz_ps_24x32_avx2);
        p.pu[LUMA_12x16].luma_hps = PFX(interp_8tap_horiz_ps_12x16_avx2);

        p.pu[LUMA_4x4].luma_hpp = PFX(interp_8tap_horiz_pp_4x4_avx2);
        p.pu[LUMA_4x8].luma_hpp = PFX(interp_8tap_horiz_pp_4x8_avx2);
        p.pu[LUMA_4x16].luma_hpp = PFX(interp_8tap_horiz_pp_4x16_avx2);
        p.pu[LUMA_8x4].luma_hpp = PFX(interp_8tap_horiz_pp_8x4_avx2);
        p.pu[LUMA_8x8].luma_hpp = PFX(interp_8tap_horiz_pp_8x8_avx2);
        p.pu[LUMA_8x16].luma_hpp = PFX(interp_8tap_horiz_pp_8x16_avx2);
        p.pu[LUMA_8x32].luma_hpp = PFX(interp_8tap_horiz_pp_8x32_avx2);
        p.pu[LUMA_16x4].luma_hpp = PFX(interp_8tap_horiz_pp_16x4_avx2);
        p.pu[LUMA_16x8].luma_hpp = PFX(interp_8tap_horiz_pp_16x8_avx2);
        p.pu[LUMA_16x12].luma_hpp = PFX(interp_8tap_horiz_pp_16x12_avx2);
        p.pu[LUMA_16x16].luma_hpp = PFX(interp_8tap_horiz_pp_16x16_avx2);
        p.pu[LUMA_16x32].luma_hpp = PFX(interp_8tap_horiz_pp_16x32_avx2);
        p.pu[LUMA_16x64].luma_hpp = PFX(interp_8tap_horiz_pp_16x64_avx2);
        p.pu[LUMA_32x8].luma_hpp = PFX(interp_8tap_horiz_pp_32x8_avx2);
        p.pu[LUMA_32x16].luma_hpp = PFX(interp_8tap_horiz_pp_32x16_avx2);
        p.pu[LUMA_32x24].luma_hpp = PFX(interp_8tap_horiz_pp_32x24_avx2);
        p.pu[LUMA_32x32].luma_hpp = PFX(interp_8tap_horiz_pp_32x32_avx2);
        p.pu[LUMA_32x64].luma_hpp = PFX(interp_8tap_horiz_pp_32x64_avx2);
        p.pu[LUMA_64x16].luma_hpp = PFX(interp_8tap_horiz_pp_64x16_avx2);
        p.pu[LUMA_64x32].luma_hpp = PFX(interp_8tap_horiz_pp_64x32_avx2);
        p.pu[LUMA_64x48].luma_hpp = PFX(interp_8tap_horiz_pp_64x48_avx2);
        p.pu[LUMA_64x64].luma_hpp = PFX(interp_8tap_horiz_pp_64x64_avx2);
        p.pu[LUMA_12x16].luma_hpp = PFX(interp_8tap_horiz_pp_12x16_avx2);
        p.pu[LUMA_24x32].luma_hpp = PFX(interp_8tap_horiz_pp_24x32_avx2);
        p.pu[LUMA_48x64].luma_hpp = PFX(interp_8tap_horiz_pp_48x64_avx2);

        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x8].filter_hps = PFX(interp_4tap_horiz_ps_8x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x4].filter_hps = PFX(interp_4tap_horiz_ps_8x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x16].filter_hps = PFX(interp_4tap_horiz_ps_8x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x6].filter_hps = PFX(interp_4tap_horiz_ps_8x6_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x2].filter_hps = PFX(interp_4tap_horiz_ps_8x2_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x32].filter_hps = PFX(interp_4tap_horiz_ps_8x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x16].filter_hps = PFX(interp_4tap_horiz_ps_16x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x8].filter_hps = PFX(interp_4tap_horiz_ps_16x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x32].filter_hps = PFX(interp_4tap_horiz_ps_16x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x12].filter_hps = PFX(interp_4tap_horiz_ps_16x12_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x4].filter_hps = PFX(interp_4tap_horiz_ps_16x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x32].filter_hps = PFX(interp_4tap_horiz_ps_32x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x16].filter_hps = PFX(interp_4tap_horiz_ps_32x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x24].filter_hps = PFX(interp_4tap_horiz_ps_32x24_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x8].filter_hps = PFX(interp_4tap_horiz_ps_32x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_24x32].filter_hps = PFX(interp_4tap_horiz_ps_24x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_12x16].filter_hps = PFX(interp_4tap_horiz_ps_12x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_6x8].filter_hps = PFX(interp_4tap_horiz_ps_6x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x8].filter_hps = PFX(interp_4tap_horiz_ps_8x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x16].filter_hps = PFX(interp_4tap_horiz_ps_8x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x32].filter_hps = PFX(interp_4tap_horiz_ps_8x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x12].filter_hps = PFX(interp_4tap_horiz_ps_8x12_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x64].filter_hps = PFX(interp_4tap_horiz_ps_8x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x4].filter_hps = PFX(interp_4tap_horiz_ps_8x4_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x32].filter_hps = PFX(interp_4tap_horiz_ps_16x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x16].filter_hps = PFX(interp_4tap_horiz_ps_16x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x64].filter_hps = PFX(interp_4tap_horiz_ps_16x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x24].filter_hps = PFX(interp_4tap_horiz_ps_16x24_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x8].filter_hps = PFX(interp_4tap_horiz_ps_16x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x64].filter_hps = PFX(interp_4tap_horiz_ps_32x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x32].filter_hps = PFX(interp_4tap_horiz_ps_32x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x48].filter_hps = PFX(interp_4tap_horiz_ps_32x48_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x16].filter_hps = PFX(interp_4tap_horiz_ps_32x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_12x32].filter_hps = PFX(interp_4tap_horiz_ps_12x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_24x64].filter_hps = PFX(interp_4tap_horiz_ps_24x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_6x16].filter_hps = PFX(interp_4tap_horiz_ps_6x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x8].filter_hps = PFX(interp_4tap_horiz_ps_8x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x4].filter_hps = PFX(interp_4tap_horiz_ps_8x4_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x16].filter_hps = PFX(interp_4tap_horiz_ps_8x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x32].filter_hps = PFX(interp_4tap_horiz_ps_8x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x16].filter_hps = PFX(interp_4tap_horiz_ps_16x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x8].filter_hps = PFX(interp_4tap_horiz_ps_16x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x32].filter_hps = PFX(interp_4tap_horiz_ps_16x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x12].filter_hps = PFX(interp_4tap_horiz_ps_16x12_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x4].filter_hps = PFX(interp_4tap_horiz_ps_16x4_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x64].filter_hps = PFX(interp_4tap_horiz_ps_16x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x32].filter_hps = PFX(interp_4tap_horiz_ps_32x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x16].filter_hps = PFX(interp_4tap_horiz_ps_32x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x64].filter_hps = PFX(interp_4tap_horiz_ps_32x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x24].filter_hps = PFX(interp_4tap_horiz_ps_32x24_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x8].filter_hps = PFX(interp_4tap_horiz_ps_32x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x64].filter_hps = PFX(interp_4tap_horiz_ps_64x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x48].filter_hps = PFX(interp_4tap_horiz_ps_64x48_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x32].filter_hps = PFX(interp_4tap_horiz_ps_64x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x16].filter_hps = PFX(interp_4tap_horiz_ps_64x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_48x64].filter_hps = PFX(interp_4tap_horiz_ps_48x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_24x32].filter_hps = PFX(interp_4tap_horiz_ps_24x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_12x16].filter_hps = PFX(interp_4tap_horiz_ps_12x16_avx2);

        p.chroma[X265_CSP_I420].pu[CHROMA_420_6x8].filter_hpp = PFX(interp_4tap_horiz_pp_6x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x2].filter_hpp = PFX(interp_4tap_horiz_pp_8x2_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x4].filter_hpp = PFX(interp_4tap_horiz_pp_8x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x6].filter_hpp = PFX(interp_4tap_horiz_pp_8x6_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x8].filter_hpp = PFX(interp_4tap_horiz_pp_8x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x16].filter_hpp = PFX(interp_4tap_horiz_pp_8x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x32].filter_hpp = PFX(interp_4tap_horiz_pp_8x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x4].filter_hpp = PFX(interp_4tap_horiz_pp_16x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x8].filter_hpp = PFX(interp_4tap_horiz_pp_16x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x12].filter_hpp = PFX(interp_4tap_horiz_pp_16x12_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x16].filter_hpp = PFX(interp_4tap_horiz_pp_16x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x32].filter_hpp = PFX(interp_4tap_horiz_pp_16x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x8].filter_hpp = PFX(interp_4tap_horiz_pp_32x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x16].filter_hpp = PFX(interp_4tap_horiz_pp_32x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x24].filter_hpp = PFX(interp_4tap_horiz_pp_32x24_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x32].filter_hpp = PFX(interp_4tap_horiz_pp_32x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_12x16].filter_hpp = PFX(interp_4tap_horiz_pp_12x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_24x32].filter_hpp = PFX(interp_4tap_horiz_pp_24x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_6x16].filter_hpp = PFX(interp_4tap_horiz_pp_6x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x4].filter_hpp = PFX(interp_4tap_horiz_pp_8x4_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x8].filter_hpp = PFX(interp_4tap_horiz_pp_8x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x12].filter_hpp = PFX(interp_4tap_horiz_pp_8x12_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x16].filter_hpp = PFX(interp_4tap_horiz_pp_8x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x32].filter_hpp = PFX(interp_4tap_horiz_pp_8x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x64].filter_hpp = PFX(interp_4tap_horiz_pp_8x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x8].filter_hpp = PFX(interp_4tap_horiz_pp_16x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x16].filter_hpp = PFX(interp_4tap_horiz_pp_16x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x24].filter_hpp = PFX(interp_4tap_horiz_pp_16x24_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x32].filter_hpp = PFX(interp_4tap_horiz_pp_16x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x64].filter_hpp = PFX(interp_4tap_horiz_pp_16x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x16].filter_hpp = PFX(interp_4tap_horiz_pp_32x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x32].filter_hpp = PFX(interp_4tap_horiz_pp_32x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x48].filter_hpp = PFX(interp_4tap_horiz_pp_32x48_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x64].filter_hpp = PFX(interp_4tap_horiz_pp_32x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_12x32].filter_hpp = PFX(interp_4tap_horiz_pp_12x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_24x64].filter_hpp = PFX(interp_4tap_horiz_pp_24x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x4].filter_hpp = PFX(interp_4tap_horiz_pp_8x4_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x8].filter_hpp = PFX(interp_4tap_horiz_pp_8x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x16].filter_hpp = PFX(interp_4tap_horiz_pp_8x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x32].filter_hpp = PFX(interp_4tap_horiz_pp_8x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x4].filter_hpp = PFX(interp_4tap_horiz_pp_16x4_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x8].filter_hpp = PFX(interp_4tap_horiz_pp_16x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x12].filter_hpp = PFX(interp_4tap_horiz_pp_16x12_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x16].filter_hpp = PFX(interp_4tap_horiz_pp_16x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x32].filter_hpp = PFX(interp_4tap_horiz_pp_16x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x64].filter_hpp = PFX(interp_4tap_horiz_pp_16x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x8].filter_hpp = PFX(interp_4tap_horiz_pp_32x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x16].filter_hpp = PFX(interp_4tap_horiz_pp_32x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x24].filter_hpp = PFX(interp_4tap_horiz_pp_32x24_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x32].filter_hpp = PFX(interp_4tap_horiz_pp_32x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x64].filter_hpp = PFX(interp_4tap_horiz_pp_32x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_12x16].filter_hpp = PFX(interp_4tap_horiz_pp_12x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_24x32].filter_hpp = PFX(interp_4tap_horiz_pp_24x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x16].filter_hpp = PFX(interp_4tap_horiz_pp_64x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x32].filter_hpp = PFX(interp_4tap_horiz_pp_64x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x48].filter_hpp = PFX(interp_4tap_horiz_pp_64x48_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x64].filter_hpp = PFX(interp_4tap_horiz_pp_64x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_48x64].filter_hpp = PFX(interp_4tap_horiz_pp_48x64_avx2);

        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x2].filter_vpp = PFX(interp_4tap_vert_pp_4x2_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x2].filter_vps = PFX(interp_4tap_vert_ps_4x2_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x2].filter_vsp = PFX(interp_4tap_vert_sp_4x2_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x2].filter_vss = PFX(interp_4tap_vert_ss_4x2_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x4].filter_vpp = PFX(interp_4tap_vert_pp_4x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x4].filter_vps = PFX(interp_4tap_vert_ps_4x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x4].filter_vsp = PFX(interp_4tap_vert_sp_4x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x4].filter_vss = PFX(interp_4tap_vert_ss_4x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x8].filter_vpp = PFX(interp_4tap_vert_pp_4x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x8].filter_vps = PFX(interp_4tap_vert_ps_4x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x8].filter_vsp = PFX(interp_4tap_vert_sp_4x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x8].filter_vss = PFX(interp_4tap_vert_ss_4x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x16].filter_vpp = PFX(interp_4tap_vert_pp_4x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x16].filter_vps = PFX(interp_4tap_vert_ps_4x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x16].filter_vsp = PFX(interp_4tap_vert_sp_4x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x16].filter_vss = PFX(interp_4tap_vert_ss_4x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x2].filter_vpp = PFX(interp_4tap_vert_pp_8x2_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x2].filter_vps = PFX(interp_4tap_vert_ps_8x2_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x2].filter_vsp = PFX(interp_4tap_vert_sp_8x2_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x2].filter_vss = PFX(interp_4tap_vert_ss_8x2_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x4].filter_vpp = PFX(interp_4tap_vert_pp_8x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x4].filter_vps = PFX(interp_4tap_vert_ps_8x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x4].filter_vsp = PFX(interp_4tap_vert_sp_8x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x4].filter_vss = PFX(interp_4tap_vert_ss_8x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x6].filter_vpp = PFX(interp_4tap_vert_pp_8x6_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x6].filter_vps = PFX(interp_4tap_vert_ps_8x6_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x6].filter_vsp = PFX(interp_4tap_vert_sp_8x6_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x6].filter_vss = PFX(interp_4tap_vert_ss_8x6_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x8].filter_vpp = PFX(interp_4tap_vert_pp_8x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x8].filter_vps = PFX(interp_4tap_vert_ps_8x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x8].filter_vsp = PFX(interp_4tap_vert_sp_8x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x8].filter_vss = PFX(interp_4tap_vert_ss_8x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x12].filter_vpp = PFX(interp_4tap_vert_pp_8x12_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x12].filter_vps = PFX(interp_4tap_vert_ps_8x12_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x12].filter_vsp = PFX(interp_4tap_vert_sp_8x12_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x12].filter_vss = PFX(interp_4tap_vert_ss_8x12_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x16].filter_vpp = PFX(interp_4tap_vert_pp_8x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x16].filter_vps = PFX(interp_4tap_vert_ps_8x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x16].filter_vsp = PFX(interp_4tap_vert_sp_8x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x16].filter_vss = PFX(interp_4tap_vert_ss_8x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x32].filter_vpp = PFX(interp_4tap_vert_pp_8x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x32].filter_vps = PFX(interp_4tap_vert_ps_8x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x32].filter_vsp = PFX(interp_4tap_vert_sp_8x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x32].filter_vss = PFX(interp_4tap_vert_ss_8x32_avx2);

        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x4].filter_vpp = PFX(interp_4tap_vert_pp_4x4_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x4].filter_vps = PFX(interp_4tap_vert_ps_4x4_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x4].filter_vsp = PFX(interp_4tap_vert_sp_4x4_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x4].filter_vss = PFX(interp_4tap_vert_ss_4x4_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x8].filter_vpp = PFX(interp_4tap_vert_pp_4x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x8].filter_vps = PFX(interp_4tap_vert_ps_4x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x8].filter_vsp = PFX(interp_4tap_vert_sp_4x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x8].filter_vss = PFX(interp_4tap_vert_ss_4x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x16].filter_vpp = PFX(interp_4tap_vert_pp_4x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x16].filter_vps = PFX(interp_4tap_vert_ps_4x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x16].filter_vsp = PFX(interp_4tap_vert_sp_4x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x16].filter_vss = PFX(interp_4tap_vert_ss_4x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x32].filter_vpp = PFX(interp_4tap_vert_pp_4x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x32].filter_vps = PFX(interp_4tap_vert_ps_4x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x32].filter_vsp = PFX(interp_4tap_vert_sp_4x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x32].filter_vss = PFX(interp_4tap_vert_ss_4x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x4].filter_vpp = PFX(interp_4tap_vert_pp_8x4_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x4].filter_vps = PFX(interp_4tap_vert_ps_8x4_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x4].filter_vsp = PFX(interp_4tap_vert_sp_8x4_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x4].filter_vss = PFX(interp_4tap_vert_ss_8x4_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x8].filter_vpp = PFX(interp_4tap_vert_pp_8x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x8].filter_vps = PFX(interp_4tap_vert_ps_8x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x8].filter_vsp = PFX(interp_4tap_vert_sp_8x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x8].filter_vss = PFX(interp_4tap_vert_ss_8x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x16].filter_vpp = PFX(interp_4tap_vert_pp_8x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x16].filter_vps = PFX(interp_4tap_vert_ps_8x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x16].filter_vsp = PFX(interp_4tap_vert_sp_8x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x16].filter_vss = PFX(interp_4tap_vert_ss_8x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x32].filter_vpp = PFX(interp_4tap_vert_pp_8x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x32].filter_vps = PFX(interp_4tap_vert_ps_8x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x32].filter_vsp = PFX(interp_4tap_vert_sp_8x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x32].filter_vss = PFX(interp_4tap_vert_ss_8x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x64].filter_vpp = PFX(interp_4tap_vert_pp_8x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x64].filter_vps = PFX(interp_4tap_vert_ps_8x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x64].filter_vsp = PFX(interp_4tap_vert_sp_8x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x64].filter_vss = PFX(interp_4tap_vert_ss_8x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_4x4].filter_vpp = PFX(interp_4tap_vert_pp_4x4_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_4x4].filter_vps = PFX(interp_4tap_vert_ps_4x4_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_4x4].filter_vsp = PFX(interp_4tap_vert_sp_4x4_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_4x4].filter_vss = PFX(interp_4tap_vert_ss_4x4_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_4x8].filter_vpp = PFX(interp_4tap_vert_pp_4x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_4x8].filter_vps = PFX(interp_4tap_vert_ps_4x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_4x8].filter_vsp = PFX(interp_4tap_vert_sp_4x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_4x8].filter_vss = PFX(interp_4tap_vert_ss_4x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_4x16].filter_vpp = PFX(interp_4tap_vert_pp_4x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_4x16].filter_vps = PFX(interp_4tap_vert_ps_4x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_4x16].filter_vsp = PFX(interp_4tap_vert_sp_4x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_4x16].filter_vss = PFX(interp_4tap_vert_ss_4x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x4].filter_vpp = PFX(interp_4tap_vert_pp_8x4_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x4].filter_vps = PFX(interp_4tap_vert_ps_8x4_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x4].filter_vsp = PFX(interp_4tap_vert_sp_8x4_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x4].filter_vss = PFX(interp_4tap_vert_ss_8x4_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x8].filter_vpp = PFX(interp_4tap_vert_pp_8x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x8].filter_vps = PFX(interp_4tap_vert_ps_8x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x8].filter_vsp = PFX(interp_4tap_vert_sp_8x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x8].filter_vss = PFX(interp_4tap_vert_ss_8x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x16].filter_vpp = PFX(interp_4tap_vert_pp_8x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x16].filter_vps = PFX(interp_4tap_vert_ps_8x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x16].filter_vsp = PFX(interp_4tap_vert_sp_8x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x16].filter_vss = PFX(interp_4tap_vert_ss_8x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x32].filter_vpp = PFX(interp_4tap_vert_pp_8x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x32].filter_vps = PFX(interp_4tap_vert_ps_8x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x32].filter_vsp = PFX(interp_4tap_vert_sp_8x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x32].filter_vss = PFX(interp_4tap_vert_ss_8x32_avx2);


        p.chroma[X265_CSP_I420].pu[CHROMA_420_6x8].filter_vss = PFX(interp_4tap_vert_ss_6x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_6x8].filter_vsp = PFX(interp_4tap_vert_sp_6x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_6x8].filter_vps = PFX(interp_4tap_vert_ps_6x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_6x8].filter_vpp = PFX(interp_4tap_vert_pp_6x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_12x16].filter_vpp = PFX(interp_4tap_vert_pp_12x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_12x16].filter_vps = PFX(interp_4tap_vert_ps_12x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_12x16].filter_vss = PFX(interp_4tap_vert_ss_12x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_12x16].filter_vsp = PFX(interp_4tap_vert_sp_12x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x4].filter_vpp = PFX(interp_4tap_vert_pp_16x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x8].filter_vpp = PFX(interp_4tap_vert_pp_16x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x12].filter_vpp = PFX(interp_4tap_vert_pp_16x12_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x16].filter_vpp = PFX(interp_4tap_vert_pp_16x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x32].filter_vpp = PFX(interp_4tap_vert_pp_16x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x4].filter_vps = PFX(interp_4tap_vert_ps_16x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x8].filter_vps = PFX(interp_4tap_vert_ps_16x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x12].filter_vps = PFX(interp_4tap_vert_ps_16x12_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x16].filter_vps = PFX(interp_4tap_vert_ps_16x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x32].filter_vps = PFX(interp_4tap_vert_ps_16x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x4].filter_vss = PFX(interp_4tap_vert_ss_16x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x8].filter_vss = PFX(interp_4tap_vert_ss_16x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x12].filter_vss = PFX(interp_4tap_vert_ss_16x12_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x16].filter_vss = PFX(interp_4tap_vert_ss_16x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x32].filter_vss = PFX(interp_4tap_vert_ss_16x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x4].filter_vsp = PFX(interp_4tap_vert_sp_16x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x8].filter_vsp = PFX(interp_4tap_vert_sp_16x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x12].filter_vsp = PFX(interp_4tap_vert_sp_16x12_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x16].filter_vsp = PFX(interp_4tap_vert_sp_16x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x32].filter_vsp = PFX(interp_4tap_vert_sp_16x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_24x32].filter_vpp = PFX(interp_4tap_vert_pp_24x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_24x32].filter_vps = PFX(interp_4tap_vert_ps_24x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_24x32].filter_vss = PFX(interp_4tap_vert_ss_24x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_24x32].filter_vsp = PFX(interp_4tap_vert_sp_24x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x8].filter_vpp = PFX(interp_4tap_vert_pp_32x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x16].filter_vpp = PFX(interp_4tap_vert_pp_32x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x24].filter_vpp = PFX(interp_4tap_vert_pp_32x24_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x32].filter_vpp = PFX(interp_4tap_vert_pp_32x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x8].filter_vps = PFX(interp_4tap_vert_ps_32x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x16].filter_vps = PFX(interp_4tap_vert_ps_32x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x24].filter_vps = PFX(interp_4tap_vert_ps_32x24_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x32].filter_vps = PFX(interp_4tap_vert_ps_32x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x8].filter_vss = PFX(interp_4tap_vert_ss_32x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x16].filter_vss = PFX(interp_4tap_vert_ss_32x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x24].filter_vss = PFX(interp_4tap_vert_ss_32x24_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x32].filter_vss = PFX(interp_4tap_vert_ss_32x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x8].filter_vsp = PFX(interp_4tap_vert_sp_32x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x16].filter_vsp = PFX(interp_4tap_vert_sp_32x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x24].filter_vsp = PFX(interp_4tap_vert_sp_32x24_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x32].filter_vsp = PFX(interp_4tap_vert_sp_32x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_6x16].filter_vss = PFX(interp_4tap_vert_ss_6x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_6x16].filter_vsp = PFX(interp_4tap_vert_sp_6x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_6x16].filter_vps = PFX(interp_4tap_vert_ps_6x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_6x16].filter_vpp = PFX(interp_4tap_vert_pp_6x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_12x32].filter_vpp = PFX(interp_4tap_vert_pp_12x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_12x32].filter_vps = PFX(interp_4tap_vert_ps_12x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_12x32].filter_vss = PFX(interp_4tap_vert_ss_12x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_12x32].filter_vsp = PFX(interp_4tap_vert_sp_12x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x8].filter_vpp = PFX(interp_4tap_vert_pp_16x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x16].filter_vpp = PFX(interp_4tap_vert_pp_16x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x24].filter_vpp = PFX(interp_4tap_vert_pp_16x24_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x32].filter_vpp = PFX(interp_4tap_vert_pp_16x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x64].filter_vpp = PFX(interp_4tap_vert_pp_16x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x8].filter_vps = PFX(interp_4tap_vert_ps_16x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x16].filter_vps = PFX(interp_4tap_vert_ps_16x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x24].filter_vps = PFX(interp_4tap_vert_ps_16x24_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x32].filter_vps = PFX(interp_4tap_vert_ps_16x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x64].filter_vps = PFX(interp_4tap_vert_ps_16x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x8].filter_vss = PFX(interp_4tap_vert_ss_16x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x16].filter_vss = PFX(interp_4tap_vert_ss_16x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x24].filter_vss = PFX(interp_4tap_vert_ss_16x24_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x32].filter_vss = PFX(interp_4tap_vert_ss_16x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x64].filter_vss = PFX(interp_4tap_vert_ss_16x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x8].filter_vsp = PFX(interp_4tap_vert_sp_16x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x16].filter_vsp = PFX(interp_4tap_vert_sp_16x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x24].filter_vsp = PFX(interp_4tap_vert_sp_16x24_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x32].filter_vsp = PFX(interp_4tap_vert_sp_16x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x64].filter_vsp = PFX(interp_4tap_vert_sp_16x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_24x64].filter_vpp = PFX(interp_4tap_vert_pp_24x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_24x64].filter_vps = PFX(interp_4tap_vert_ps_24x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_24x64].filter_vss = PFX(interp_4tap_vert_ss_24x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_24x64].filter_vsp = PFX(interp_4tap_vert_sp_24x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x16].filter_vpp = PFX(interp_4tap_vert_pp_32x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x32].filter_vpp = PFX(interp_4tap_vert_pp_32x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x48].filter_vpp = PFX(interp_4tap_vert_pp_32x48_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x64].filter_vpp = PFX(interp_4tap_vert_pp_32x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x16].filter_vps = PFX(interp_4tap_vert_ps_32x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x32].filter_vps = PFX(interp_4tap_vert_ps_32x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x48].filter_vps = PFX(interp_4tap_vert_ps_32x48_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x64].filter_vps = PFX(interp_4tap_vert_ps_32x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x16].filter_vss = PFX(interp_4tap_vert_ss_32x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x32].filter_vss = PFX(interp_4tap_vert_ss_32x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x48].filter_vss = PFX(interp_4tap_vert_ss_32x48_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x64].filter_vss = PFX(interp_4tap_vert_ss_32x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x16].filter_vsp = PFX(interp_4tap_vert_sp_32x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x32].filter_vsp = PFX(interp_4tap_vert_sp_32x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x48].filter_vsp = PFX(interp_4tap_vert_sp_32x48_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x64].filter_vsp = PFX(interp_4tap_vert_sp_32x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_12x16].filter_vpp = PFX(interp_4tap_vert_pp_12x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_12x16].filter_vps = PFX(interp_4tap_vert_ps_12x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_12x16].filter_vss = PFX(interp_4tap_vert_ss_12x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_12x16].filter_vsp = PFX(interp_4tap_vert_sp_12x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x4].filter_vpp = PFX(interp_4tap_vert_pp_16x4_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x8].filter_vpp = PFX(interp_4tap_vert_pp_16x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x12].filter_vpp = PFX(interp_4tap_vert_pp_16x12_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x16].filter_vpp = PFX(interp_4tap_vert_pp_16x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x32].filter_vpp = PFX(interp_4tap_vert_pp_16x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x64].filter_vpp = PFX(interp_4tap_vert_pp_16x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x4].filter_vps = PFX(interp_4tap_vert_ps_16x4_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x8].filter_vps = PFX(interp_4tap_vert_ps_16x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x12].filter_vps = PFX(interp_4tap_vert_ps_16x12_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x16].filter_vps = PFX(interp_4tap_vert_ps_16x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x32].filter_vps = PFX(interp_4tap_vert_ps_16x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x64].filter_vps = PFX(interp_4tap_vert_ps_16x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x4].filter_vss = PFX(interp_4tap_vert_ss_16x4_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x8].filter_vss = PFX(interp_4tap_vert_ss_16x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x12].filter_vss = PFX(interp_4tap_vert_ss_16x12_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x16].filter_vss = PFX(interp_4tap_vert_ss_16x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x32].filter_vss = PFX(interp_4tap_vert_ss_16x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x64].filter_vss = PFX(interp_4tap_vert_ss_16x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x4].filter_vsp = PFX(interp_4tap_vert_sp_16x4_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x8].filter_vsp = PFX(interp_4tap_vert_sp_16x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x12].filter_vsp = PFX(interp_4tap_vert_sp_16x12_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x16].filter_vsp = PFX(interp_4tap_vert_sp_16x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x32].filter_vsp = PFX(interp_4tap_vert_sp_16x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x64].filter_vsp = PFX(interp_4tap_vert_sp_16x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_24x32].filter_vpp = PFX(interp_4tap_vert_pp_24x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_24x32].filter_vps = PFX(interp_4tap_vert_ps_24x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_24x32].filter_vss = PFX(interp_4tap_vert_ss_24x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_24x32].filter_vsp = PFX(interp_4tap_vert_sp_24x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x8].filter_vpp = PFX(interp_4tap_vert_pp_32x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x16].filter_vpp = PFX(interp_4tap_vert_pp_32x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x24].filter_vpp = PFX(interp_4tap_vert_pp_32x24_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x32].filter_vpp = PFX(interp_4tap_vert_pp_32x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x64].filter_vpp = PFX(interp_4tap_vert_pp_32x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x8].filter_vps = PFX(interp_4tap_vert_ps_32x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x16].filter_vps = PFX(interp_4tap_vert_ps_32x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x24].filter_vps = PFX(interp_4tap_vert_ps_32x24_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x32].filter_vps = PFX(interp_4tap_vert_ps_32x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x64].filter_vps = PFX(interp_4tap_vert_ps_32x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x8].filter_vss = PFX(interp_4tap_vert_ss_32x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x16].filter_vss = PFX(interp_4tap_vert_ss_32x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x24].filter_vss = PFX(interp_4tap_vert_ss_32x24_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x32].filter_vss = PFX(interp_4tap_vert_ss_32x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x64].filter_vss = PFX(interp_4tap_vert_ss_32x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x8].filter_vsp = PFX(interp_4tap_vert_sp_32x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x16].filter_vsp = PFX(interp_4tap_vert_sp_32x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x24].filter_vsp = PFX(interp_4tap_vert_sp_32x24_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x32].filter_vsp = PFX(interp_4tap_vert_sp_32x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x64].filter_vsp = PFX(interp_4tap_vert_sp_32x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_48x64].filter_vpp = PFX(interp_4tap_vert_pp_48x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_48x64].filter_vps = PFX(interp_4tap_vert_ps_48x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_48x64].filter_vss = PFX(interp_4tap_vert_ss_48x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_48x64].filter_vsp = PFX(interp_4tap_vert_sp_48x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x16].filter_vpp = PFX(interp_4tap_vert_pp_64x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x32].filter_vpp = PFX(interp_4tap_vert_pp_64x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x48].filter_vpp = PFX(interp_4tap_vert_pp_64x48_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x64].filter_vpp = PFX(interp_4tap_vert_pp_64x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x16].filter_vps = PFX(interp_4tap_vert_ps_64x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x32].filter_vps = PFX(interp_4tap_vert_ps_64x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x48].filter_vps = PFX(interp_4tap_vert_ps_64x48_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x64].filter_vps = PFX(interp_4tap_vert_ps_64x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x16].filter_vss = PFX(interp_4tap_vert_ss_64x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x32].filter_vss = PFX(interp_4tap_vert_ss_64x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x48].filter_vss = PFX(interp_4tap_vert_ss_64x48_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x64].filter_vss = PFX(interp_4tap_vert_ss_64x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x16].filter_vsp = PFX(interp_4tap_vert_sp_64x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x32].filter_vsp = PFX(interp_4tap_vert_sp_64x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x48].filter_vsp = PFX(interp_4tap_vert_sp_64x48_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x64].filter_vsp = PFX(interp_4tap_vert_sp_64x64_avx2);

        p.frameInitLowres = PFX(frame_init_lowres_core_avx2);
        p.propagateCost = PFX(mbtree_propagate_cost_avx2);
        p.fix8Unpack = PFX(cutree_fix8_unpack_avx2);
        p.fix8Pack = PFX(cutree_fix8_pack_avx2);

        p.integral_initv[INTEGRAL_4] = PFX(integral4v_avx2);
        p.integral_initv[INTEGRAL_8] = PFX(integral8v_avx2);
        p.integral_initv[INTEGRAL_12] = PFX(integral12v_avx2);
        p.integral_initv[INTEGRAL_16] = PFX(integral16v_avx2);
        p.integral_initv[INTEGRAL_24] = PFX(integral24v_avx2);
        p.integral_initv[INTEGRAL_32] = PFX(integral32v_avx2);
        p.integral_inith[INTEGRAL_4] = PFX(integral4h_avx2);
        p.integral_inith[INTEGRAL_8] = PFX(integral8h_avx2);
        p.integral_inith[INTEGRAL_12] = PFX(integral12h_avx2);
        p.integral_inith[INTEGRAL_16] = PFX(integral16h_avx2);

        /* TODO: This kernel needs to be modified to work with HIGH_BIT_DEPTH only 
        p.planeClipAndMax = PFX(planeClipAndMax_avx2); */

        // TODO: depends on hps and vsp
        ALL_LUMA_PU_T(luma_hvpp, interp_8tap_hv_pp_cpu);                        // calling luma_hvpp for all sizes
        p.pu[LUMA_4x4].luma_hvpp = interp_8tap_hv_pp_cpu<LUMA_4x4>;             // ALL_LUMA_PU_T has declared all sizes except 4x4, hence calling luma_hvpp[4x4] 

#if X265_DEPTH == 10
        p.pu[LUMA_8x8].satd = PFX(pixel_satd_8x8_avx2);
        p.cu[LUMA_8x8].sa8d = PFX(pixel_sa8d_8x8_avx2);
        p.cu[LUMA_16x16].sa8d = PFX(pixel_sa8d_16x16_avx2);
        p.cu[LUMA_32x32].sa8d = PFX(pixel_sa8d_32x32_avx2);
#endif

        if (cpuMask & X265_CPU_BMI2)
        {
            p.scanPosLast = PFX(scanPosLast_avx2_bmi2);
            p.costCoeffNxN = PFX(costCoeffNxN_avx2_bmi2);
        }
    }
}
#else // if HIGH_BIT_DEPTH

void setupAssemblyPrimitives(EncoderPrimitives &p, int cpuMask) // Main
{
#if X86_64
    p.scanPosLast = PFX(scanPosLast_x64);
#endif

    if (cpuMask & X265_CPU_SSE2)
    {
        /* We do not differentiate CPUs which support MMX and not SSE2. We only check
         * for SSE2 and then use both MMX and SSE2 functions */
        AVC_LUMA_PU(sad, mmx2);
        AVC_LUMA_PU(sad_x3, mmx2);
        AVC_LUMA_PU(sad_x4, mmx2);

        p.pu[LUMA_16x16].sad = PFX(pixel_sad_16x16_sse2);
        p.pu[LUMA_16x16].sad_x3 = PFX(pixel_sad_x3_16x16_sse2);
        p.pu[LUMA_16x16].sad_x4 = PFX(pixel_sad_x4_16x16_sse2);
        p.pu[LUMA_16x8].sad  = PFX(pixel_sad_16x8_sse2);
        p.pu[LUMA_16x8].sad_x3  = PFX(pixel_sad_x3_16x8_sse2);
        p.pu[LUMA_16x8].sad_x4  = PFX(pixel_sad_x4_16x8_sse2);
        HEVC_SAD(sse2);

        p.pu[LUMA_4x4].satd = p.cu[BLOCK_4x4].sa8d = PFX(pixel_satd_4x4_mmx2);
        ALL_LUMA_PU(satd, pixel_satd, sse2);

        p.cu[BLOCK_4x4].sse_pp = PFX(pixel_ssd_4x4_mmx);
        p.cu[BLOCK_8x8].sse_pp = PFX(pixel_ssd_8x8_mmx);
        p.cu[BLOCK_16x16].sse_pp = PFX(pixel_ssd_16x16_mmx);

        PIXEL_AVG_W4(mmx2);
        PIXEL_AVG(sse2);
        LUMA_VAR(sse2);

        ASSIGN_SA8D(sse2);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_4x8].sse_pp = PFX(pixel_ssd_4x8_mmx);
        ASSIGN_SSE_PP(sse2);
        ASSIGN_SSE_SS(sse2);

        LUMA_PU_BLOCKCOPY(pp, sse2);
        CHROMA_420_PU_BLOCKCOPY(pp, sse2);
        CHROMA_422_PU_BLOCKCOPY(pp, sse2);

        LUMA_CU_BLOCKCOPY(ss, sse2);
        LUMA_CU_BLOCKCOPY(sp, sse2);
        CHROMA_420_CU_BLOCKCOPY(ss, sse2);
        CHROMA_422_CU_BLOCKCOPY(ss, sse2);
        CHROMA_420_CU_BLOCKCOPY(sp, sse2);
        CHROMA_422_CU_BLOCKCOPY(sp, sse2);

        LUMA_VSS_FILTERS(sse2);
        CHROMA_420_VSS_FILTERS(_sse2);
        CHROMA_422_VSS_FILTERS(_sse2);
        CHROMA_444_VSS_FILTERS(sse2);
        CHROMA_420_VSP_FILTERS(_sse2);
        CHROMA_422_VSP_FILTERS(_sse2);
        CHROMA_444_VSP_FILTERS(_sse2);
#if X86_64
        ALL_CHROMA_420_PU(filter_vpp, interp_4tap_vert_pp, sse2);
        ALL_CHROMA_422_PU(filter_vpp, interp_4tap_vert_pp, sse2);
        ALL_CHROMA_444_PU(filter_vpp, interp_4tap_vert_pp, sse2);
        ALL_CHROMA_420_PU(filter_vps, interp_4tap_vert_ps, sse2);
        ALL_CHROMA_422_PU(filter_vps, interp_4tap_vert_ps, sse2);
        ALL_CHROMA_444_PU(filter_vps, interp_4tap_vert_ps, sse2);
        ALL_LUMA_PU(luma_vpp, interp_8tap_vert_pp, sse2);
        ALL_LUMA_PU(luma_vps, interp_8tap_vert_ps, sse2);
#else
        p.chroma[X265_CSP_I420].pu[CHROMA_420_2x4].filter_vpp = PFX(interp_4tap_vert_pp_2x4_sse2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_2x8].filter_vpp = PFX(interp_4tap_vert_pp_2x8_sse2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x2].filter_vpp = PFX(interp_4tap_vert_pp_4x2_sse2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x4].filter_vpp = PFX(interp_4tap_vert_pp_4x4_sse2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x8].filter_vpp = PFX(interp_4tap_vert_pp_4x8_sse2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x16].filter_vpp = PFX(interp_4tap_vert_pp_4x16_sse2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_2x16].filter_vpp = PFX(interp_4tap_vert_pp_2x16_sse2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x4].filter_vpp = PFX(interp_4tap_vert_pp_4x4_sse2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x8].filter_vpp = PFX(interp_4tap_vert_pp_4x8_sse2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x16].filter_vpp = PFX(interp_4tap_vert_pp_4x16_sse2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x32].filter_vpp = PFX(interp_4tap_vert_pp_4x32_sse2);
        p.chroma[X265_CSP_I444].pu[LUMA_4x4].filter_vpp = PFX(interp_4tap_vert_pp_4x4_sse2);
        p.chroma[X265_CSP_I444].pu[LUMA_4x8].filter_vpp = PFX(interp_4tap_vert_pp_4x8_sse2);
        p.chroma[X265_CSP_I444].pu[LUMA_4x16].filter_vpp = PFX(interp_4tap_vert_pp_4x16_sse2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_2x4].filter_vps = PFX(interp_4tap_vert_ps_2x4_sse2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_2x8].filter_vps = PFX(interp_4tap_vert_ps_2x8_sse2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x2].filter_vps = PFX(interp_4tap_vert_ps_4x2_sse2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x4].filter_vps = PFX(interp_4tap_vert_ps_4x4_sse2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x8].filter_vps = PFX(interp_4tap_vert_ps_4x8_sse2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_2x16].filter_vps = PFX(interp_4tap_vert_ps_2x16_sse2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x4].filter_vps = PFX(interp_4tap_vert_ps_4x4_sse2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x8].filter_vps = PFX(interp_4tap_vert_ps_4x8_sse2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x16].filter_vps = PFX(interp_4tap_vert_ps_4x16_sse2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x32].filter_vps = PFX(interp_4tap_vert_ps_4x32_sse2);
        p.chroma[X265_CSP_I444].pu[LUMA_4x4].filter_vps = PFX(interp_4tap_vert_ps_4x4_sse2);
        p.chroma[X265_CSP_I444].pu[LUMA_4x8].filter_vps = PFX(interp_4tap_vert_ps_4x8_sse2);
        p.chroma[X265_CSP_I444].pu[LUMA_4x16].filter_vps = PFX(interp_4tap_vert_ps_4x16_sse2);
#endif

        ALL_LUMA_PU(luma_hpp, interp_8tap_horiz_pp, sse2);
        p.pu[LUMA_4x4].luma_hpp = PFX(interp_8tap_horiz_pp_4x4_sse2);
        ALL_LUMA_PU(luma_hps, interp_8tap_horiz_ps, sse2);
        p.pu[LUMA_4x4].luma_hps = PFX(interp_8tap_horiz_ps_4x4_sse2);
        p.pu[LUMA_8x8].luma_hvpp = PFX(interp_8tap_hv_pp_8x8_sse3);

        //p.frameInitLowres = PFX(frame_init_lowres_core_mmx2);
        p.frameInitLowres = PFX(frame_init_lowres_core_sse2);

        ALL_LUMA_TU(blockfill_s, blockfill_s, sse2);
        ALL_LUMA_TU_S(cpy2Dto1D_shl, cpy2Dto1D_shl_, sse2);
        ALL_LUMA_TU_S(cpy2Dto1D_shr, cpy2Dto1D_shr_, sse2);
        ALL_LUMA_TU_S(cpy1Dto2D_shl, cpy1Dto2D_shl_, sse2);
        ALL_LUMA_TU_S(cpy1Dto2D_shr, cpy1Dto2D_shr_, sse2);
        ALL_LUMA_TU_S(ssd_s, pixel_ssd_s_, sse2);

        ALL_LUMA_TU_S(intra_pred[PLANAR_IDX], intra_pred_planar, sse2);
        ALL_LUMA_TU_S(intra_pred[DC_IDX], intra_pred_dc, sse2);

        p.cu[BLOCK_4x4].intra_pred[2] = PFX(intra_pred_ang4_2_sse2);
        p.cu[BLOCK_4x4].intra_pred[3] = PFX(intra_pred_ang4_3_sse2);
        p.cu[BLOCK_4x4].intra_pred[4] = PFX(intra_pred_ang4_4_sse2);
        p.cu[BLOCK_4x4].intra_pred[5] = PFX(intra_pred_ang4_5_sse2);
        p.cu[BLOCK_4x4].intra_pred[6] = PFX(intra_pred_ang4_6_sse2);
        p.cu[BLOCK_4x4].intra_pred[7] = PFX(intra_pred_ang4_7_sse2);
        p.cu[BLOCK_4x4].intra_pred[8] = PFX(intra_pred_ang4_8_sse2);
        p.cu[BLOCK_4x4].intra_pred[9] = PFX(intra_pred_ang4_9_sse2);
        p.cu[BLOCK_4x4].intra_pred[10] = PFX(intra_pred_ang4_10_sse2);
        p.cu[BLOCK_4x4].intra_pred[11] = PFX(intra_pred_ang4_11_sse2);
        p.cu[BLOCK_4x4].intra_pred[12] = PFX(intra_pred_ang4_12_sse2);
        p.cu[BLOCK_4x4].intra_pred[13] = PFX(intra_pred_ang4_13_sse2);
        p.cu[BLOCK_4x4].intra_pred[14] = PFX(intra_pred_ang4_14_sse2);
        p.cu[BLOCK_4x4].intra_pred[15] = PFX(intra_pred_ang4_15_sse2);
        p.cu[BLOCK_4x4].intra_pred[16] = PFX(intra_pred_ang4_16_sse2);
        p.cu[BLOCK_4x4].intra_pred[17] = PFX(intra_pred_ang4_17_sse2);
        p.cu[BLOCK_4x4].intra_pred[18] = PFX(intra_pred_ang4_18_sse2);
        p.cu[BLOCK_4x4].intra_pred[19] = PFX(intra_pred_ang4_19_sse2);
        p.cu[BLOCK_4x4].intra_pred[20] = PFX(intra_pred_ang4_20_sse2);
        p.cu[BLOCK_4x4].intra_pred[21] = PFX(intra_pred_ang4_21_sse2);
        p.cu[BLOCK_4x4].intra_pred[22] = PFX(intra_pred_ang4_22_sse2);
        p.cu[BLOCK_4x4].intra_pred[23] = PFX(intra_pred_ang4_23_sse2);
        p.cu[BLOCK_4x4].intra_pred[24] = PFX(intra_pred_ang4_24_sse2);
        p.cu[BLOCK_4x4].intra_pred[25] = PFX(intra_pred_ang4_25_sse2);
        p.cu[BLOCK_4x4].intra_pred[26] = PFX(intra_pred_ang4_26_sse2);
        p.cu[BLOCK_4x4].intra_pred[27] = PFX(intra_pred_ang4_27_sse2);
        p.cu[BLOCK_4x4].intra_pred[28] = PFX(intra_pred_ang4_28_sse2);
        p.cu[BLOCK_4x4].intra_pred[29] = PFX(intra_pred_ang4_29_sse2);
        p.cu[BLOCK_4x4].intra_pred[30] = PFX(intra_pred_ang4_30_sse2);
        p.cu[BLOCK_4x4].intra_pred[31] = PFX(intra_pred_ang4_31_sse2);
        p.cu[BLOCK_4x4].intra_pred[32] = PFX(intra_pred_ang4_32_sse2);
        p.cu[BLOCK_4x4].intra_pred[33] = PFX(intra_pred_ang4_33_sse2);

        p.cu[BLOCK_4x4].intra_pred_allangs = PFX(all_angs_pred_4x4_sse2);

        p.cu[BLOCK_4x4].calcresidual = PFX(getResidual4_sse2);
        p.cu[BLOCK_8x8].calcresidual = PFX(getResidual8_sse2);

        ALL_LUMA_TU_S(transpose, transpose, sse2);
        p.cu[BLOCK_64x64].transpose = PFX(transpose64_sse2);

        p.ssim_4x4x2_core = PFX(pixel_ssim_4x4x2_core_sse2);
        p.ssim_end_4 = PFX(pixel_ssim_end4_sse2);

        p.cu[BLOCK_4x4].dct = PFX(dct4_sse2);
        p.cu[BLOCK_8x8].dct = PFX(dct8_sse2);
        p.cu[BLOCK_4x4].idct = PFX(idct4_sse2);
#if X86_64
        p.cu[BLOCK_8x8].idct = PFX(idct8_sse2);

        // TODO: it is passed smoke test, but we need testbench, so temporary disable
        p.costC1C2Flag = PFX(costC1C2Flag_sse2);
#endif
        p.idst4x4 = PFX(idst4_sse2);
        p.dst4x4 = PFX(dst4_sse2);

        p.planecopy_sp = PFX(downShift_16_sse2);
        ALL_CHROMA_420_PU(p2s, filterPixelToShort, sse2);
        ALL_CHROMA_422_PU(p2s, filterPixelToShort, sse2);
        ALL_CHROMA_444_PU(p2s, filterPixelToShort, sse2);
        ALL_LUMA_PU(convert_p2s, filterPixelToShort, sse2);
        ALL_LUMA_TU(count_nonzero, count_nonzero, sse2);
        p.propagateCost = PFX(mbtree_propagate_cost_sse2);
    }
    if (cpuMask & X265_CPU_SSE3)
    {
        ALL_CHROMA_420_PU(filter_hpp, interp_4tap_horiz_pp, sse3);
        ALL_CHROMA_422_PU(filter_hpp, interp_4tap_horiz_pp, sse3);
        ALL_CHROMA_444_PU(filter_hpp, interp_4tap_horiz_pp, sse3);
        ALL_CHROMA_420_PU(filter_hps, interp_4tap_horiz_ps, sse3);
        ALL_CHROMA_422_PU(filter_hps, interp_4tap_horiz_ps, sse3);
        ALL_CHROMA_444_PU(filter_hps, interp_4tap_horiz_ps, sse3);
    }
    if (cpuMask & X265_CPU_SSSE3)
    {
        p.pu[LUMA_8x16].sad_x3 = PFX(pixel_sad_x3_8x16_ssse3);
        p.pu[LUMA_8x32].sad_x3 = PFX(pixel_sad_x3_8x32_ssse3);
        p.pu[LUMA_12x16].sad_x3 = PFX(pixel_sad_x3_12x16_ssse3);
        HEVC_SAD_X3(ssse3);

        p.pu[LUMA_8x4].sad_x4  = PFX(pixel_sad_x4_8x4_ssse3);
        p.pu[LUMA_8x8].sad_x4  = PFX(pixel_sad_x4_8x8_ssse3);
        p.pu[LUMA_8x16].sad_x4 = PFX(pixel_sad_x4_8x16_ssse3);
        p.pu[LUMA_8x32].sad_x4 = PFX(pixel_sad_x4_8x32_ssse3);
        p.pu[LUMA_12x16].sad_x4 = PFX(pixel_sad_x4_12x16_ssse3);
        HEVC_SAD_X4(ssse3);

        p.pu[LUMA_4x4].satd = p.cu[BLOCK_4x4].sa8d = PFX(pixel_satd_4x4_ssse3);
        ALL_LUMA_PU(satd, pixel_satd, ssse3);

        ASSIGN_SA8D(ssse3);
        PIXEL_AVG(ssse3);
        PIXEL_AVG_W4(ssse3);
        INTRA_ANG_SSSE3(ssse3);

        ASSIGN_SSE_PP(ssse3);
        p.cu[BLOCK_4x4].sse_pp = PFX(pixel_ssd_4x4_ssse3);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_4x8].sse_pp = PFX(pixel_ssd_4x8_ssse3);

        p.dst4x4 = PFX(dst4_ssse3);
        p.cu[BLOCK_8x8].idct = PFX(idct8_ssse3);

        // MUST be done after LUMA_FILTERS() to overwrite default version
        p.pu[LUMA_8x8].luma_hvpp = PFX(interp_8tap_hv_pp_8x8_ssse3);

        p.frameInitLowres = PFX(frame_init_lowres_core_ssse3);
        p.scale1D_128to64 = PFX(scale1D_128to64_ssse3);
        p.scale2D_64to32 = PFX(scale2D_64to32_ssse3);

        p.pu[LUMA_8x4].convert_p2s = PFX(filterPixelToShort_8x4_ssse3);
        p.pu[LUMA_8x8].convert_p2s = PFX(filterPixelToShort_8x8_ssse3);
        p.pu[LUMA_8x16].convert_p2s = PFX(filterPixelToShort_8x16_ssse3);
        p.pu[LUMA_8x32].convert_p2s = PFX(filterPixelToShort_8x32_ssse3);
        p.pu[LUMA_16x4].convert_p2s = PFX(filterPixelToShort_16x4_ssse3);
        p.pu[LUMA_16x8].convert_p2s = PFX(filterPixelToShort_16x8_ssse3);
        p.pu[LUMA_16x12].convert_p2s = PFX(filterPixelToShort_16x12_ssse3);
        p.pu[LUMA_16x16].convert_p2s = PFX(filterPixelToShort_16x16_ssse3);
        p.pu[LUMA_16x32].convert_p2s = PFX(filterPixelToShort_16x32_ssse3);
        p.pu[LUMA_16x64].convert_p2s = PFX(filterPixelToShort_16x64_ssse3);
        p.pu[LUMA_32x8].convert_p2s = PFX(filterPixelToShort_32x8_ssse3);
        p.pu[LUMA_32x16].convert_p2s = PFX(filterPixelToShort_32x16_ssse3);
        p.pu[LUMA_32x24].convert_p2s = PFX(filterPixelToShort_32x24_ssse3);
        p.pu[LUMA_32x32].convert_p2s = PFX(filterPixelToShort_32x32_ssse3);
        p.pu[LUMA_32x64].convert_p2s = PFX(filterPixelToShort_32x64_ssse3);
        p.pu[LUMA_64x16].convert_p2s = PFX(filterPixelToShort_64x16_ssse3);
        p.pu[LUMA_64x32].convert_p2s = PFX(filterPixelToShort_64x32_ssse3);
        p.pu[LUMA_64x48].convert_p2s = PFX(filterPixelToShort_64x48_ssse3);
        p.pu[LUMA_64x64].convert_p2s = PFX(filterPixelToShort_64x64_ssse3);
        p.pu[LUMA_12x16].convert_p2s = PFX(filterPixelToShort_12x16_ssse3);
        p.pu[LUMA_24x32].convert_p2s = PFX(filterPixelToShort_24x32_ssse3);
        p.pu[LUMA_48x64].convert_p2s = PFX(filterPixelToShort_48x64_ssse3);

        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x2].p2s = PFX(filterPixelToShort_8x2_ssse3);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x4].p2s = PFX(filterPixelToShort_8x4_ssse3);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x6].p2s = PFX(filterPixelToShort_8x6_ssse3);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x8].p2s = PFX(filterPixelToShort_8x8_ssse3);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x16].p2s = PFX(filterPixelToShort_8x16_ssse3);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x32].p2s = PFX(filterPixelToShort_8x32_ssse3);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x4].p2s = PFX(filterPixelToShort_16x4_ssse3);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x8].p2s = PFX(filterPixelToShort_16x8_ssse3);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x12].p2s = PFX(filterPixelToShort_16x12_ssse3);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x16].p2s = PFX(filterPixelToShort_16x16_ssse3);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x32].p2s = PFX(filterPixelToShort_16x32_ssse3);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x8].p2s = PFX(filterPixelToShort_32x8_ssse3);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x16].p2s = PFX(filterPixelToShort_32x16_ssse3);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x24].p2s = PFX(filterPixelToShort_32x24_ssse3);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x32].p2s = PFX(filterPixelToShort_32x32_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x4].p2s = PFX(filterPixelToShort_8x4_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x8].p2s = PFX(filterPixelToShort_8x8_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x12].p2s = PFX(filterPixelToShort_8x12_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x16].p2s = PFX(filterPixelToShort_8x16_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x32].p2s = PFX(filterPixelToShort_8x32_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x64].p2s = PFX(filterPixelToShort_8x64_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_12x32].p2s = PFX(filterPixelToShort_12x32_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x8].p2s = PFX(filterPixelToShort_16x8_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x16].p2s = PFX(filterPixelToShort_16x16_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x24].p2s = PFX(filterPixelToShort_16x24_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x32].p2s = PFX(filterPixelToShort_16x32_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x64].p2s = PFX(filterPixelToShort_16x64_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_24x64].p2s = PFX(filterPixelToShort_24x64_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x16].p2s = PFX(filterPixelToShort_32x16_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x32].p2s = PFX(filterPixelToShort_32x32_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x48].p2s = PFX(filterPixelToShort_32x48_ssse3);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x64].p2s = PFX(filterPixelToShort_32x64_ssse3);
        p.findPosFirstLast = PFX(findPosFirstLast_ssse3);
        p.fix8Unpack = PFX(cutree_fix8_unpack_ssse3);
        p.fix8Pack = PFX(cutree_fix8_pack_ssse3);
    }
    if (cpuMask & X265_CPU_SSE4)
    {
        p.sign = PFX(calSign_sse4);
        p.saoCuOrgE0 = PFX(saoCuOrgE0_sse4);
        p.saoCuOrgE1 = PFX(saoCuOrgE1_sse4);
        p.saoCuOrgE1_2Rows = PFX(saoCuOrgE1_2Rows_sse4);
        p.saoCuOrgE2[0] = PFX(saoCuOrgE2_sse4);
        p.saoCuOrgE2[1] = PFX(saoCuOrgE2_sse4);
        p.saoCuOrgE3[0] = PFX(saoCuOrgE3_sse4);
        p.saoCuOrgE3[1] = PFX(saoCuOrgE3_sse4);
        p.saoCuOrgB0 = PFX(saoCuOrgB0_sse4);

        LUMA_ADDAVG(sse4);
        CHROMA_420_ADDAVG(sse4);
        CHROMA_422_ADDAVG(sse4);

        // TODO: check POPCNT flag!
        ALL_LUMA_TU_S(copy_cnt, copy_cnt_, sse4);

        p.pu[LUMA_4x4].satd = p.cu[BLOCK_4x4].sa8d = PFX(pixel_satd_4x4_sse4);
        ALL_LUMA_PU(satd, pixel_satd, sse4);
        ASSIGN_SA8D(sse4);
        ASSIGN_SSE_SS(sse4);
        p.cu[BLOCK_64x64].sse_pp = PFX(pixel_ssd_64x64_sse4);

        LUMA_PIXELSUB(sse4);
        CHROMA_420_PIXELSUB_PS(sse4);
        CHROMA_422_PIXELSUB_PS(sse4);

        LUMA_FILTERS(sse4);
        CHROMA_420_FILTERS(sse4);
        CHROMA_422_FILTERS(sse4);
        CHROMA_444_FILTERS(sse4);
        CHROMA_420_VSS_FILTERS_SSE4(_sse4);
        CHROMA_422_VSS_FILTERS_SSE4(_sse4);
        CHROMA_420_VSP_FILTERS_SSE4(_sse4);
        CHROMA_422_VSP_FILTERS_SSE4(_sse4);
        CHROMA_444_VSP_FILTERS_SSE4(_sse4);

        // MUST be done after LUMA_FILTERS() to overwrite default version
        p.pu[LUMA_8x8].luma_hvpp = PFX(interp_8tap_hv_pp_8x8_ssse3);

        LUMA_CU_BLOCKCOPY(ps, sse4);
        CHROMA_420_CU_BLOCKCOPY(ps, sse4);
        CHROMA_422_CU_BLOCKCOPY(ps, sse4);

        p.cu[BLOCK_16x16].calcresidual = PFX(getResidual16_sse4);
        p.cu[BLOCK_32x32].calcresidual = PFX(getResidual32_sse4);
        p.cu[BLOCK_8x8].dct = PFX(dct8_sse4);
        p.denoiseDct = PFX(denoise_dct_sse4);
        p.quant = PFX(quant_sse4);
        p.nquant = PFX(nquant_sse4);
        p.dequant_normal = PFX(dequant_normal_sse4);
        p.dequant_scaling = PFX(dequant_scaling_sse4);

        p.weight_pp = PFX(weight_pp_sse4);
        p.weight_sp = PFX(weight_sp_sse4);

        p.cu[BLOCK_4x4].intra_filter = PFX(intra_filter_4x4_sse4);
        p.cu[BLOCK_8x8].intra_filter = PFX(intra_filter_8x8_sse4);
        p.cu[BLOCK_16x16].intra_filter = PFX(intra_filter_16x16_sse4);
        p.cu[BLOCK_32x32].intra_filter = PFX(intra_filter_32x32_sse4);

        ALL_LUMA_TU_S(intra_pred[PLANAR_IDX], intra_pred_planar, sse4);
        ALL_LUMA_TU_S(intra_pred[DC_IDX], intra_pred_dc, sse4);
        ALL_LUMA_TU(intra_pred_allangs, all_angs_pred, sse4);

        INTRA_ANG_SSE4_COMMON(sse4);
        INTRA_ANG_SSE4(sse4);

        p.cu[BLOCK_4x4].psy_cost_pp = PFX(psyCost_pp_4x4_sse4);

        p.pu[LUMA_4x4].convert_p2s = PFX(filterPixelToShort_4x4_sse4);
        p.pu[LUMA_4x8].convert_p2s = PFX(filterPixelToShort_4x8_sse4);
        p.pu[LUMA_4x16].convert_p2s = PFX(filterPixelToShort_4x16_sse4);

        p.chroma[X265_CSP_I420].pu[CHROMA_420_2x4].p2s = PFX(filterPixelToShort_2x4_sse4);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_2x8].p2s = PFX(filterPixelToShort_2x8_sse4);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x2].p2s = PFX(filterPixelToShort_4x2_sse4);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x4].p2s = PFX(filterPixelToShort_4x4_sse4);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x8].p2s = PFX(filterPixelToShort_4x8_sse4);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x16].p2s = PFX(filterPixelToShort_4x16_sse4);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_6x8].p2s = PFX(filterPixelToShort_6x8_sse4);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_2x8].p2s = PFX(filterPixelToShort_2x8_sse4);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_2x16].p2s = PFX(filterPixelToShort_2x16_sse4);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x4].p2s = PFX(filterPixelToShort_4x4_sse4);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x8].p2s = PFX(filterPixelToShort_4x8_sse4);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x16].p2s = PFX(filterPixelToShort_4x16_sse4);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x32].p2s = PFX(filterPixelToShort_4x32_sse4);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_6x16].p2s = PFX(filterPixelToShort_6x16_sse4);

#if X86_64
        p.pelFilterLumaStrong[0] = PFX(pelFilterLumaStrong_V_sse4);
        p.pelFilterLumaStrong[1] = PFX(pelFilterLumaStrong_H_sse4);
        p.pelFilterChroma[0] = PFX(pelFilterChroma_V_sse4);
        p.pelFilterChroma[1] = PFX(pelFilterChroma_H_sse4);

//        p.saoCuStatsBO = PFX(saoCuStatsBO_sse4);
        p.saoCuStatsE0 = PFX(saoCuStatsE0_sse4);
        p.saoCuStatsE1 = PFX(saoCuStatsE1_sse4);
        p.saoCuStatsE2 = PFX(saoCuStatsE2_sse4);
        p.saoCuStatsE3 = PFX(saoCuStatsE3_sse4);

        ALL_LUMA_CU(psy_cost_pp, psyCost_pp, sse4);

        p.costCoeffNxN = PFX(costCoeffNxN_sse4);
#endif
        p.costCoeffRemain = PFX(costCoeffRemain_sse4);
    }
    if (cpuMask & X265_CPU_AVX)
    {
        p.pu[LUMA_4x4].satd = p.cu[BLOCK_4x4].sa8d = PFX(pixel_satd_4x4_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x24].satd = PFX(pixel_satd_16x24_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x48].satd = PFX(pixel_satd_32x48_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_24x64].satd = PFX(pixel_satd_24x64_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x64].satd = PFX(pixel_satd_8x64_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x12].satd = PFX(pixel_satd_8x12_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_12x32].satd = PFX(pixel_satd_12x32_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x32].satd = PFX(pixel_satd_4x32_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x32].satd = PFX(pixel_satd_16x32_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x64].satd = PFX(pixel_satd_32x64_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x16].satd = PFX(pixel_satd_16x16_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x32].satd = PFX(pixel_satd_32x32_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x64].satd = PFX(pixel_satd_16x64_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x8].satd = PFX(pixel_satd_16x8_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x16].satd = PFX(pixel_satd_32x16_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x4].satd = PFX(pixel_satd_8x4_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x16].satd = PFX(pixel_satd_8x16_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x8].satd = PFX(pixel_satd_8x8_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x32].satd = PFX(pixel_satd_8x32_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x8].satd = PFX(pixel_satd_4x8_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x16].satd = PFX(pixel_satd_4x16_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x4].satd = PFX(pixel_satd_4x4_avx);
        ALL_LUMA_PU(satd, pixel_satd, avx);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x4].satd = PFX(pixel_satd_4x4_avx);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x8].satd = PFX(pixel_satd_8x8_avx);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x16].satd = PFX(pixel_satd_16x16_avx);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x32].satd = PFX(pixel_satd_32x32_avx);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x4].satd = PFX(pixel_satd_8x4_avx);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x8].satd = PFX(pixel_satd_4x8_avx);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x8].satd = PFX(pixel_satd_16x8_avx);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x16].satd = PFX(pixel_satd_8x16_avx);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x16].satd = PFX(pixel_satd_32x16_avx);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x32].satd = PFX(pixel_satd_16x32_avx);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x12].satd = PFX(pixel_satd_16x12_avx);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_12x16].satd = PFX(pixel_satd_12x16_avx);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x4].satd = PFX(pixel_satd_16x4_avx);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x16].satd = PFX(pixel_satd_4x16_avx);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x24].satd = PFX(pixel_satd_32x24_avx);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_24x32].satd = PFX(pixel_satd_24x32_avx);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x8].satd = PFX(pixel_satd_32x8_avx);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x32].satd = PFX(pixel_satd_8x32_avx);
        ASSIGN_SA8D(avx);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_32x32].sa8d = PFX(pixel_sa8d_32x32_avx);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_16x16].sa8d = PFX(pixel_sa8d_16x16_avx);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_8x8].sa8d = PFX(pixel_sa8d_8x8_avx);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_4x4].sa8d = PFX(pixel_satd_4x4_avx);
        ASSIGN_SSE_PP(avx);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_8x8].sse_pp = PFX(pixel_ssd_8x8_avx);
        ASSIGN_SSE_SS(avx);
        LUMA_VAR(avx);

        p.pu[LUMA_12x16].sad_x3 = PFX(pixel_sad_x3_12x16_avx);
        p.pu[LUMA_16x4].sad_x3  = PFX(pixel_sad_x3_16x4_avx);
        HEVC_SAD_X3(avx);

        p.pu[LUMA_12x16].sad_x4 = PFX(pixel_sad_x4_12x16_avx);
        p.pu[LUMA_16x4].sad_x4  = PFX(pixel_sad_x4_16x4_avx);
        HEVC_SAD_X4(avx);

        p.ssim_4x4x2_core = PFX(pixel_ssim_4x4x2_core_avx);
        p.ssim_end_4 = PFX(pixel_ssim_end4_avx);

        p.cu[BLOCK_16x16].copy_ss = PFX(blockcopy_ss_16x16_avx);
        p.cu[BLOCK_32x32].copy_ss = PFX(blockcopy_ss_32x32_avx);
        p.cu[BLOCK_64x64].copy_ss = PFX(blockcopy_ss_64x64_avx);
        p.chroma[X265_CSP_I420].cu[CHROMA_420_16x16].copy_ss = PFX(blockcopy_ss_16x16_avx);
        p.chroma[X265_CSP_I420].cu[CHROMA_420_32x32].copy_ss = PFX(blockcopy_ss_32x32_avx);
        p.chroma[X265_CSP_I422].cu[CHROMA_422_16x32].copy_ss = PFX(blockcopy_ss_16x32_avx);
        p.chroma[X265_CSP_I422].cu[CHROMA_422_32x64].copy_ss = PFX(blockcopy_ss_32x64_avx);

        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x8].copy_pp = PFX(blockcopy_pp_32x8_avx);
        p.pu[LUMA_32x8].copy_pp = PFX(blockcopy_pp_32x8_avx);

        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x16].copy_pp = PFX(blockcopy_pp_32x16_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x16].copy_pp = PFX(blockcopy_pp_32x16_avx);
        p.pu[LUMA_32x16].copy_pp = PFX(blockcopy_pp_32x16_avx);

        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x24].copy_pp = PFX(blockcopy_pp_32x24_avx);
        p.pu[LUMA_32x24].copy_pp = PFX(blockcopy_pp_32x24_avx);

        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x32].copy_pp = PFX(blockcopy_pp_32x32_avx);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x32].copy_pp = PFX(blockcopy_pp_32x32_avx);
        p.pu[LUMA_32x32].copy_pp  = PFX(blockcopy_pp_32x32_avx);

        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x48].copy_pp = PFX(blockcopy_pp_32x48_avx);

        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x64].copy_pp = PFX(blockcopy_pp_32x64_avx);
        p.pu[LUMA_32x64].copy_pp = PFX(blockcopy_pp_32x64_avx);

        p.pu[LUMA_64x16].copy_pp = PFX(blockcopy_pp_64x16_avx);
        p.pu[LUMA_64x32].copy_pp = PFX(blockcopy_pp_64x32_avx);
        p.pu[LUMA_64x48].copy_pp = PFX(blockcopy_pp_64x48_avx);
        p.pu[LUMA_64x64].copy_pp = PFX(blockcopy_pp_64x64_avx);

        p.pu[LUMA_48x64].copy_pp = PFX(blockcopy_pp_48x64_avx);

        p.frameInitLowres = PFX(frame_init_lowres_core_avx);
        p.propagateCost = PFX(mbtree_propagate_cost_avx);
    }
    if (cpuMask & X265_CPU_XOP)
    {
        //p.pu[LUMA_4x4].satd = p.cu[BLOCK_4x4].sa8d = PFX(pixel_satd_4x4_xop); this one is broken
        ALL_LUMA_PU(satd, pixel_satd, xop);
        ASSIGN_SA8D(xop);
        LUMA_VAR(xop);
        p.cu[BLOCK_8x8].sse_pp = PFX(pixel_ssd_8x8_xop);
        p.cu[BLOCK_16x16].sse_pp = PFX(pixel_ssd_16x16_xop);
        p.frameInitLowres = PFX(frame_init_lowres_core_xop);
    }
#if X86_64
    if (cpuMask & X265_CPU_AVX2)
    {
        p.cu[BLOCK_16x16].sse_ss = (pixel_sse_ss_t)PFX(pixel_ssd_ss_16x16_avx2);
        p.cu[BLOCK_32x32].sse_ss = (pixel_sse_ss_t)PFX(pixel_ssd_ss_32x32_avx2);
        p.cu[BLOCK_64x64].sse_ss = (pixel_sse_ss_t)PFX(pixel_ssd_ss_64x64_avx2);

        p.cu[BLOCK_16x16].var = PFX(pixel_var_16x16_avx2);
        p.cu[BLOCK_32x32].var = PFX(pixel_var_32x32_avx2);
        p.cu[BLOCK_64x64].var = PFX(pixel_var_64x64_avx2);

        p.cu[BLOCK_4x4].intra_filter = PFX(intra_filter_4x4_avx2);

        p.planecopy_sp = PFX(downShift_16_avx2);

        p.cu[BLOCK_32x32].intra_pred[DC_IDX] = PFX(intra_pred_dc32_avx2);

        p.cu[BLOCK_16x16].intra_pred[PLANAR_IDX] = PFX(intra_pred_planar16_avx2);
        p.cu[BLOCK_32x32].intra_pred[PLANAR_IDX] = PFX(intra_pred_planar32_avx2);

        p.idst4x4 = PFX(idst4_avx2);
        p.dst4x4 = PFX(dst4_avx2);
        p.scale2D_64to32 = PFX(scale2D_64to32_avx2);
        p.saoCuOrgE0 = PFX(saoCuOrgE0_avx2);
        p.saoCuOrgE1 = PFX(saoCuOrgE1_avx2);
        p.saoCuOrgE1_2Rows = PFX(saoCuOrgE1_2Rows_avx2);
        p.saoCuOrgE2[0] = PFX(saoCuOrgE2_avx2);
        p.saoCuOrgE2[1] = PFX(saoCuOrgE2_32_avx2);
        p.saoCuOrgE3[0] = PFX(saoCuOrgE3_avx2);
        p.saoCuOrgE3[1] = PFX(saoCuOrgE3_32_avx2);
        p.saoCuOrgB0 = PFX(saoCuOrgB0_avx2);
        p.sign = PFX(calSign_avx2);

        p.cu[BLOCK_4x4].psy_cost_pp = PFX(psyCost_pp_4x4_avx2);
        p.cu[BLOCK_8x8].psy_cost_pp = PFX(psyCost_pp_8x8_avx2);
        p.cu[BLOCK_16x16].psy_cost_pp = PFX(psyCost_pp_16x16_avx2);
        p.cu[BLOCK_32x32].psy_cost_pp = PFX(psyCost_pp_32x32_avx2);
        p.cu[BLOCK_64x64].psy_cost_pp = PFX(psyCost_pp_64x64_avx2);

        p.pu[LUMA_8x4].addAvg = PFX(addAvg_8x4_avx2);
        p.pu[LUMA_8x8].addAvg = PFX(addAvg_8x8_avx2);
        p.pu[LUMA_8x16].addAvg = PFX(addAvg_8x16_avx2);
        p.pu[LUMA_8x32].addAvg = PFX(addAvg_8x32_avx2);

        p.pu[LUMA_12x16].addAvg = PFX(addAvg_12x16_avx2);

        p.pu[LUMA_16x4].addAvg = PFX(addAvg_16x4_avx2);
        p.pu[LUMA_16x8].addAvg = PFX(addAvg_16x8_avx2);
        p.pu[LUMA_16x12].addAvg = PFX(addAvg_16x12_avx2);
        p.pu[LUMA_16x16].addAvg = PFX(addAvg_16x16_avx2);
        p.pu[LUMA_16x32].addAvg = PFX(addAvg_16x32_avx2);
        p.pu[LUMA_16x64].addAvg = PFX(addAvg_16x64_avx2);

        p.pu[LUMA_24x32].addAvg = PFX(addAvg_24x32_avx2);

        p.pu[LUMA_32x8].addAvg = PFX(addAvg_32x8_avx2);
        p.pu[LUMA_32x16].addAvg = PFX(addAvg_32x16_avx2);
        p.pu[LUMA_32x24].addAvg = PFX(addAvg_32x24_avx2);
        p.pu[LUMA_32x32].addAvg = PFX(addAvg_32x32_avx2);
        p.pu[LUMA_32x64].addAvg = PFX(addAvg_32x64_avx2);

        p.pu[LUMA_48x64].addAvg = PFX(addAvg_48x64_avx2);

        p.pu[LUMA_64x16].addAvg = PFX(addAvg_64x16_avx2);
        p.pu[LUMA_64x32].addAvg = PFX(addAvg_64x32_avx2);
        p.pu[LUMA_64x48].addAvg = PFX(addAvg_64x48_avx2);
        p.pu[LUMA_64x64].addAvg = PFX(addAvg_64x64_avx2);

        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x2].addAvg = PFX(addAvg_8x2_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x4].addAvg = PFX(addAvg_8x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x6].addAvg = PFX(addAvg_8x6_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x8].addAvg = PFX(addAvg_8x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x16].addAvg = PFX(addAvg_8x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x32].addAvg = PFX(addAvg_8x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_12x16].addAvg = PFX(addAvg_12x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x4].addAvg = PFX(addAvg_16x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x8].addAvg = PFX(addAvg_16x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x12].addAvg = PFX(addAvg_16x12_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x16].addAvg = PFX(addAvg_16x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x32].addAvg = PFX(addAvg_16x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x8].addAvg = PFX(addAvg_32x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x16].addAvg = PFX(addAvg_32x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x24].addAvg = PFX(addAvg_32x24_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x32].addAvg = PFX(addAvg_32x32_avx2);

        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x4].addAvg = PFX(addAvg_8x4_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x8].addAvg = PFX(addAvg_8x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x12].addAvg = PFX(addAvg_8x12_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x16].addAvg = PFX(addAvg_8x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x32].addAvg = PFX(addAvg_8x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x64].addAvg = PFX(addAvg_8x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_12x32].addAvg = PFX(addAvg_12x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x8].addAvg = PFX(addAvg_16x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x16].addAvg = PFX(addAvg_16x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x24].addAvg = PFX(addAvg_16x24_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x32].addAvg = PFX(addAvg_16x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x64].addAvg = PFX(addAvg_16x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_24x64].addAvg = PFX(addAvg_24x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x16].addAvg = PFX(addAvg_32x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x32].addAvg = PFX(addAvg_32x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x48].addAvg = PFX(addAvg_32x48_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x64].addAvg = PFX(addAvg_32x64_avx2);

        p.cu[BLOCK_8x8].sa8d = PFX(pixel_sa8d_8x8_avx2);
        p.cu[BLOCK_16x16].sa8d = PFX(pixel_sa8d_16x16_avx2);
        p.cu[BLOCK_32x32].sa8d = PFX(pixel_sa8d_32x32_avx2);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_8x8].sa8d = PFX(pixel_sa8d_8x8_avx2);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_16x16].sa8d = PFX(pixel_sa8d_16x16_avx2);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_32x32].sa8d = PFX(pixel_sa8d_32x32_avx2);

        p.cu[BLOCK_16x16].add_ps = PFX(pixel_add_ps_16x16_avx2);
        p.cu[BLOCK_32x32].add_ps = PFX(pixel_add_ps_32x32_avx2);
        p.cu[BLOCK_64x64].add_ps = PFX(pixel_add_ps_64x64_avx2);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_16x16].add_ps = PFX(pixel_add_ps_16x16_avx2);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_32x32].add_ps = PFX(pixel_add_ps_32x32_avx2);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_16x32].add_ps = PFX(pixel_add_ps_16x32_avx2);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_32x64].add_ps = PFX(pixel_add_ps_32x64_avx2);

        p.cu[BLOCK_16x16].sub_ps = PFX(pixel_sub_ps_16x16_avx2);
        p.cu[BLOCK_32x32].sub_ps = PFX(pixel_sub_ps_32x32_avx2);
        p.cu[BLOCK_64x64].sub_ps = PFX(pixel_sub_ps_64x64_avx2);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_16x16].sub_ps = PFX(pixel_sub_ps_16x16_avx2);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_32x32].sub_ps = PFX(pixel_sub_ps_32x32_avx2);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_16x32].sub_ps = PFX(pixel_sub_ps_16x32_avx2);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_32x64].sub_ps = PFX(pixel_sub_ps_32x64_avx2);

        p.pu[LUMA_16x4].pixelavg_pp = PFX(pixel_avg_16x4_avx2);
        p.pu[LUMA_16x8].pixelavg_pp = PFX(pixel_avg_16x8_avx2);
        p.pu[LUMA_16x12].pixelavg_pp = PFX(pixel_avg_16x12_avx2);
        p.pu[LUMA_16x16].pixelavg_pp = PFX(pixel_avg_16x16_avx2);
        p.pu[LUMA_16x32].pixelavg_pp = PFX(pixel_avg_16x32_avx2);
        p.pu[LUMA_16x64].pixelavg_pp = PFX(pixel_avg_16x64_avx2);

        p.pu[LUMA_32x64].pixelavg_pp = PFX(pixel_avg_32x64_avx2);
        p.pu[LUMA_32x32].pixelavg_pp = PFX(pixel_avg_32x32_avx2);
        p.pu[LUMA_32x24].pixelavg_pp = PFX(pixel_avg_32x24_avx2);
        p.pu[LUMA_32x16].pixelavg_pp = PFX(pixel_avg_32x16_avx2);
        p.pu[LUMA_32x8].pixelavg_pp = PFX(pixel_avg_32x8_avx2);
        p.pu[LUMA_48x64].pixelavg_pp = PFX(pixel_avg_48x64_avx2);
        p.pu[LUMA_64x64].pixelavg_pp = PFX(pixel_avg_64x64_avx2);
        p.pu[LUMA_64x48].pixelavg_pp = PFX(pixel_avg_64x48_avx2);
        p.pu[LUMA_64x32].pixelavg_pp = PFX(pixel_avg_64x32_avx2);
        p.pu[LUMA_64x16].pixelavg_pp = PFX(pixel_avg_64x16_avx2);

        p.pu[LUMA_16x16].satd = PFX(pixel_satd_16x16_avx2);
        p.pu[LUMA_16x8].satd  = PFX(pixel_satd_16x8_avx2);
        p.pu[LUMA_8x16].satd  = PFX(pixel_satd_8x16_avx2);
        p.pu[LUMA_8x8].satd   = PFX(pixel_satd_8x8_avx2);

        p.pu[LUMA_16x4].satd  = PFX(pixel_satd_16x4_avx2);
        p.pu[LUMA_16x12].satd = PFX(pixel_satd_16x12_avx2);
        p.pu[LUMA_16x32].satd = PFX(pixel_satd_16x32_avx2);
        p.pu[LUMA_16x64].satd = PFX(pixel_satd_16x64_avx2);

        p.pu[LUMA_32x8].satd   = PFX(pixel_satd_32x8_avx2);
        p.pu[LUMA_32x16].satd   = PFX(pixel_satd_32x16_avx2);
        p.pu[LUMA_32x24].satd   = PFX(pixel_satd_32x24_avx2);
        p.pu[LUMA_32x32].satd   = PFX(pixel_satd_32x32_avx2);
        p.pu[LUMA_32x64].satd   = PFX(pixel_satd_32x64_avx2);
        p.pu[LUMA_48x64].satd   = PFX(pixel_satd_48x64_avx2);
        p.pu[LUMA_64x16].satd   = PFX(pixel_satd_64x16_avx2);
        p.pu[LUMA_64x32].satd   = PFX(pixel_satd_64x32_avx2);
        p.pu[LUMA_64x48].satd   = PFX(pixel_satd_64x48_avx2);
        p.pu[LUMA_64x64].satd   = PFX(pixel_satd_64x64_avx2);

        p.pu[LUMA_32x8].sad = PFX(pixel_sad_32x8_avx2);
        p.pu[LUMA_32x16].sad = PFX(pixel_sad_32x16_avx2);
        p.pu[LUMA_32x24].sad = PFX(pixel_sad_32x24_avx2);
        p.pu[LUMA_32x32].sad = PFX(pixel_sad_32x32_avx2);
        p.pu[LUMA_32x64].sad = PFX(pixel_sad_32x64_avx2);
        p.pu[LUMA_48x64].sad = PFX(pixel_sad_48x64_avx2);
        p.pu[LUMA_64x16].sad = PFX(pixel_sad_64x16_avx2);
        p.pu[LUMA_64x32].sad = PFX(pixel_sad_64x32_avx2);
        p.pu[LUMA_64x48].sad = PFX(pixel_sad_64x48_avx2);
        p.pu[LUMA_64x64].sad = PFX(pixel_sad_64x64_avx2);

        p.pu[LUMA_8x4].sad_x3 = PFX(pixel_sad_x3_8x4_avx2);
        p.pu[LUMA_8x8].sad_x3 = PFX(pixel_sad_x3_8x8_avx2);
        p.pu[LUMA_8x16].sad_x3 = PFX(pixel_sad_x3_8x16_avx2);

        p.pu[LUMA_8x8].sad_x4 = PFX(pixel_sad_x4_8x8_avx2);
        p.pu[LUMA_16x8].sad_x4  = PFX(pixel_sad_x4_16x8_avx2);
        p.pu[LUMA_16x12].sad_x4 = PFX(pixel_sad_x4_16x12_avx2);
        p.pu[LUMA_16x16].sad_x4 = PFX(pixel_sad_x4_16x16_avx2);
        p.pu[LUMA_16x32].sad_x4 = PFX(pixel_sad_x4_16x32_avx2);
        p.pu[LUMA_32x32].sad_x4 = PFX(pixel_sad_x4_32x32_avx2);
        p.pu[LUMA_32x16].sad_x4 = PFX(pixel_sad_x4_32x16_avx2);
        p.pu[LUMA_32x64].sad_x4 = PFX(pixel_sad_x4_32x64_avx2);
        p.pu[LUMA_32x24].sad_x4 = PFX(pixel_sad_x4_32x24_avx2);
        p.pu[LUMA_32x8].sad_x4 = PFX(pixel_sad_x4_32x8_avx2);
        p.pu[LUMA_48x64].sad_x4 = PFX(pixel_sad_x4_48x64_avx2);
        p.pu[LUMA_64x16].sad_x4 = PFX(pixel_sad_x4_64x16_avx2);
        p.pu[LUMA_64x32].sad_x4 = PFX(pixel_sad_x4_64x32_avx2);
        p.pu[LUMA_64x48].sad_x4 = PFX(pixel_sad_x4_64x48_avx2);
        p.pu[LUMA_64x64].sad_x4 = PFX(pixel_sad_x4_64x64_avx2);

        p.cu[BLOCK_16x16].sse_pp = PFX(pixel_ssd_16x16_avx2);
        p.cu[BLOCK_32x32].sse_pp = PFX(pixel_ssd_32x32_avx2);
        p.cu[BLOCK_64x64].sse_pp = PFX(pixel_ssd_64x64_avx2);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_16x16].sse_pp = PFX(pixel_ssd_16x16_avx2);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_32x32].sse_pp = PFX(pixel_ssd_32x32_avx2);

        p.cu[BLOCK_16x16].ssd_s = PFX(pixel_ssd_s_16_avx2);
        p.cu[BLOCK_32x32].ssd_s = PFX(pixel_ssd_s_32_avx2);

        p.cu[BLOCK_8x8].copy_cnt = PFX(copy_cnt_8_avx2);
        p.cu[BLOCK_16x16].copy_cnt = PFX(copy_cnt_16_avx2);
        p.cu[BLOCK_32x32].copy_cnt = PFX(copy_cnt_32_avx2);

        p.cu[BLOCK_16x16].blockfill_s = PFX(blockfill_s_16x16_avx2);
        p.cu[BLOCK_32x32].blockfill_s = PFX(blockfill_s_32x32_avx2);

        ALL_LUMA_TU_S(cpy1Dto2D_shl, cpy1Dto2D_shl_, avx2);
        ALL_LUMA_TU_S(cpy1Dto2D_shr, cpy1Dto2D_shr_, avx2);

        p.cu[BLOCK_8x8].cpy2Dto1D_shl = PFX(cpy2Dto1D_shl_8_avx2);
        p.cu[BLOCK_16x16].cpy2Dto1D_shl = PFX(cpy2Dto1D_shl_16_avx2);
        p.cu[BLOCK_32x32].cpy2Dto1D_shl = PFX(cpy2Dto1D_shl_32_avx2);

        p.cu[BLOCK_8x8].cpy2Dto1D_shr = PFX(cpy2Dto1D_shr_8_avx2);
        p.cu[BLOCK_16x16].cpy2Dto1D_shr = PFX(cpy2Dto1D_shr_16_avx2);
        p.cu[BLOCK_32x32].cpy2Dto1D_shr = PFX(cpy2Dto1D_shr_32_avx2);

        ALL_LUMA_TU(count_nonzero, count_nonzero, avx2);
        p.denoiseDct = PFX(denoise_dct_avx2);
        p.quant = PFX(quant_avx2);
        p.nquant = PFX(nquant_avx2);
        p.dequant_normal = PFX(dequant_normal_avx2);
        p.dequant_scaling = PFX(dequant_scaling_avx2);

        p.cu[BLOCK_16x16].calcresidual = PFX(getResidual16_avx2);
        p.cu[BLOCK_32x32].calcresidual = PFX(getResidual32_avx2);

        p.scale1D_128to64 = PFX(scale1D_128to64_avx2);
        p.weight_pp = PFX(weight_pp_avx2);
        p.weight_sp = PFX(weight_sp_avx2);

        // intra_pred functions
        p.cu[BLOCK_4x4].intra_pred[3] = PFX(intra_pred_ang4_3_avx2);
        p.cu[BLOCK_4x4].intra_pred[4] = PFX(intra_pred_ang4_4_avx2);
        p.cu[BLOCK_4x4].intra_pred[5] = PFX(intra_pred_ang4_5_avx2);
        p.cu[BLOCK_4x4].intra_pred[6] = PFX(intra_pred_ang4_6_avx2);
        p.cu[BLOCK_4x4].intra_pred[7] = PFX(intra_pred_ang4_7_avx2);
        p.cu[BLOCK_4x4].intra_pred[8] = PFX(intra_pred_ang4_8_avx2);
        p.cu[BLOCK_4x4].intra_pred[9] = PFX(intra_pred_ang4_9_avx2);
        p.cu[BLOCK_4x4].intra_pred[11] = PFX(intra_pred_ang4_11_avx2);
        p.cu[BLOCK_4x4].intra_pred[12] = PFX(intra_pred_ang4_12_avx2);
        p.cu[BLOCK_4x4].intra_pred[13] = PFX(intra_pred_ang4_13_avx2);
        p.cu[BLOCK_4x4].intra_pred[14] = PFX(intra_pred_ang4_14_avx2);
        p.cu[BLOCK_4x4].intra_pred[15] = PFX(intra_pred_ang4_15_avx2);
        p.cu[BLOCK_4x4].intra_pred[16] = PFX(intra_pred_ang4_16_avx2);
        p.cu[BLOCK_4x4].intra_pred[17] = PFX(intra_pred_ang4_17_avx2);
        p.cu[BLOCK_4x4].intra_pred[19] = PFX(intra_pred_ang4_19_avx2);
        p.cu[BLOCK_4x4].intra_pred[20] = PFX(intra_pred_ang4_20_avx2);
        p.cu[BLOCK_4x4].intra_pred[21] = PFX(intra_pred_ang4_21_avx2);
        p.cu[BLOCK_4x4].intra_pred[22] = PFX(intra_pred_ang4_22_avx2);
        p.cu[BLOCK_4x4].intra_pred[23] = PFX(intra_pred_ang4_23_avx2);
        p.cu[BLOCK_4x4].intra_pred[24] = PFX(intra_pred_ang4_24_avx2);
        p.cu[BLOCK_4x4].intra_pred[25] = PFX(intra_pred_ang4_25_avx2);
        p.cu[BLOCK_4x4].intra_pred[27] = PFX(intra_pred_ang4_27_avx2);
        p.cu[BLOCK_4x4].intra_pred[28] = PFX(intra_pred_ang4_28_avx2);
        p.cu[BLOCK_4x4].intra_pred[29] = PFX(intra_pred_ang4_29_avx2);
        p.cu[BLOCK_4x4].intra_pred[30] = PFX(intra_pred_ang4_30_avx2);
        p.cu[BLOCK_4x4].intra_pred[31] = PFX(intra_pred_ang4_31_avx2);
        p.cu[BLOCK_4x4].intra_pred[32] = PFX(intra_pred_ang4_32_avx2);
        p.cu[BLOCK_4x4].intra_pred[33] = PFX(intra_pred_ang4_33_avx2);
        p.cu[BLOCK_8x8].intra_pred[3] = PFX(intra_pred_ang8_3_avx2);
        p.cu[BLOCK_8x8].intra_pred[4] = PFX(intra_pred_ang8_4_avx2);
        p.cu[BLOCK_8x8].intra_pred[5] = PFX(intra_pred_ang8_5_avx2);
        p.cu[BLOCK_8x8].intra_pred[6] = PFX(intra_pred_ang8_6_avx2);
        p.cu[BLOCK_8x8].intra_pred[7] = PFX(intra_pred_ang8_7_avx2);
        p.cu[BLOCK_8x8].intra_pred[8] = PFX(intra_pred_ang8_8_avx2);
        p.cu[BLOCK_8x8].intra_pred[9] = PFX(intra_pred_ang8_9_avx2);
        p.cu[BLOCK_8x8].intra_pred[11] = PFX(intra_pred_ang8_11_avx2);
        p.cu[BLOCK_8x8].intra_pred[12] = PFX(intra_pred_ang8_12_avx2);
        p.cu[BLOCK_8x8].intra_pred[13] = PFX(intra_pred_ang8_13_avx2);
        p.cu[BLOCK_8x8].intra_pred[14] = PFX(intra_pred_ang8_14_avx2);
        p.cu[BLOCK_8x8].intra_pred[15] = PFX(intra_pred_ang8_15_avx2);
        p.cu[BLOCK_8x8].intra_pred[16] = PFX(intra_pred_ang8_16_avx2);
        p.cu[BLOCK_8x8].intra_pred[17] = PFX(intra_pred_ang8_17_avx2);
        p.cu[BLOCK_8x8].intra_pred[20] = PFX(intra_pred_ang8_20_avx2);
        p.cu[BLOCK_8x8].intra_pred[21] = PFX(intra_pred_ang8_21_avx2);
        p.cu[BLOCK_8x8].intra_pred[22] = PFX(intra_pred_ang8_22_avx2);
        p.cu[BLOCK_8x8].intra_pred[23] = PFX(intra_pred_ang8_23_avx2);
        p.cu[BLOCK_8x8].intra_pred[24] = PFX(intra_pred_ang8_24_avx2);
        p.cu[BLOCK_8x8].intra_pred[25] = PFX(intra_pred_ang8_25_avx2);
        p.cu[BLOCK_8x8].intra_pred[27] = PFX(intra_pred_ang8_27_avx2);
        p.cu[BLOCK_8x8].intra_pred[28] = PFX(intra_pred_ang8_28_avx2);
        p.cu[BLOCK_8x8].intra_pred[29] = PFX(intra_pred_ang8_29_avx2);
        p.cu[BLOCK_8x8].intra_pred[30] = PFX(intra_pred_ang8_30_avx2);
        p.cu[BLOCK_8x8].intra_pred[31] = PFX(intra_pred_ang8_31_avx2);
        p.cu[BLOCK_8x8].intra_pred[32] = PFX(intra_pred_ang8_32_avx2);
        p.cu[BLOCK_8x8].intra_pred[33] = PFX(intra_pred_ang8_33_avx2);
        p.cu[BLOCK_16x16].intra_pred[3] = PFX(intra_pred_ang16_3_avx2);
        p.cu[BLOCK_16x16].intra_pred[4] = PFX(intra_pred_ang16_4_avx2);
        p.cu[BLOCK_16x16].intra_pred[5] = PFX(intra_pred_ang16_5_avx2);
        p.cu[BLOCK_16x16].intra_pred[6] = PFX(intra_pred_ang16_6_avx2);
        p.cu[BLOCK_16x16].intra_pred[7] = PFX(intra_pred_ang16_7_avx2);
        p.cu[BLOCK_16x16].intra_pred[8] = PFX(intra_pred_ang16_8_avx2);
        p.cu[BLOCK_16x16].intra_pred[9] = PFX(intra_pred_ang16_9_avx2);
        p.cu[BLOCK_16x16].intra_pred[12] = PFX(intra_pred_ang16_12_avx2);
        p.cu[BLOCK_16x16].intra_pred[11] = PFX(intra_pred_ang16_11_avx2);
        p.cu[BLOCK_16x16].intra_pred[13] = PFX(intra_pred_ang16_13_avx2);
        p.cu[BLOCK_16x16].intra_pred[14] = PFX(intra_pred_ang16_14_avx2);
        p.cu[BLOCK_16x16].intra_pred[15] = PFX(intra_pred_ang16_15_avx2);
        p.cu[BLOCK_16x16].intra_pred[16] = PFX(intra_pred_ang16_16_avx2);
        p.cu[BLOCK_16x16].intra_pred[17] = PFX(intra_pred_ang16_17_avx2);
        p.cu[BLOCK_16x16].intra_pred[25] = PFX(intra_pred_ang16_25_avx2);
        p.cu[BLOCK_16x16].intra_pred[28] = PFX(intra_pred_ang16_28_avx2);
        p.cu[BLOCK_16x16].intra_pred[27] = PFX(intra_pred_ang16_27_avx2);
        p.cu[BLOCK_16x16].intra_pred[29] = PFX(intra_pred_ang16_29_avx2);
        p.cu[BLOCK_16x16].intra_pred[30] = PFX(intra_pred_ang16_30_avx2);
        p.cu[BLOCK_16x16].intra_pred[31] = PFX(intra_pred_ang16_31_avx2);
        p.cu[BLOCK_16x16].intra_pred[32] = PFX(intra_pred_ang16_32_avx2);
        p.cu[BLOCK_16x16].intra_pred[33] = PFX(intra_pred_ang16_33_avx2);
        p.cu[BLOCK_16x16].intra_pred[24] = PFX(intra_pred_ang16_24_avx2);
        p.cu[BLOCK_16x16].intra_pred[23] = PFX(intra_pred_ang16_23_avx2);
        p.cu[BLOCK_16x16].intra_pred[22] = PFX(intra_pred_ang16_22_avx2);
        p.cu[BLOCK_32x32].intra_pred[5]  = PFX(intra_pred_ang32_5_avx2);
        p.cu[BLOCK_32x32].intra_pred[6]  = PFX(intra_pred_ang32_6_avx2);
        p.cu[BLOCK_32x32].intra_pred[7]  = PFX(intra_pred_ang32_7_avx2);
        p.cu[BLOCK_32x32].intra_pred[8]  = PFX(intra_pred_ang32_8_avx2);
        p.cu[BLOCK_32x32].intra_pred[9]  = PFX(intra_pred_ang32_9_avx2);
        p.cu[BLOCK_32x32].intra_pred[10] = PFX(intra_pred_ang32_10_avx2);
        p.cu[BLOCK_32x32].intra_pred[11] = PFX(intra_pred_ang32_11_avx2);
        p.cu[BLOCK_32x32].intra_pred[12] = PFX(intra_pred_ang32_12_avx2);
        p.cu[BLOCK_32x32].intra_pred[13] = PFX(intra_pred_ang32_13_avx2);
        p.cu[BLOCK_32x32].intra_pred[14] = PFX(intra_pred_ang32_14_avx2);
        p.cu[BLOCK_32x32].intra_pred[15] = PFX(intra_pred_ang32_15_avx2);
        p.cu[BLOCK_32x32].intra_pred[16] = PFX(intra_pred_ang32_16_avx2);
        p.cu[BLOCK_32x32].intra_pred[17] = PFX(intra_pred_ang32_17_avx2);
        p.cu[BLOCK_32x32].intra_pred[19] = PFX(intra_pred_ang32_19_avx2);
        p.cu[BLOCK_32x32].intra_pred[20] = PFX(intra_pred_ang32_20_avx2);
        p.cu[BLOCK_32x32].intra_pred[34] = PFX(intra_pred_ang32_34_avx2);
        p.cu[BLOCK_32x32].intra_pred[2] = PFX(intra_pred_ang32_2_avx2);
        p.cu[BLOCK_32x32].intra_pred[26] = PFX(intra_pred_ang32_26_avx2);
        p.cu[BLOCK_32x32].intra_pred[27] = PFX(intra_pred_ang32_27_avx2);
        p.cu[BLOCK_32x32].intra_pred[28] = PFX(intra_pred_ang32_28_avx2);
        p.cu[BLOCK_32x32].intra_pred[29] = PFX(intra_pred_ang32_29_avx2);
        p.cu[BLOCK_32x32].intra_pred[30] = PFX(intra_pred_ang32_30_avx2);
        p.cu[BLOCK_32x32].intra_pred[31] = PFX(intra_pred_ang32_31_avx2);
        p.cu[BLOCK_32x32].intra_pred[32] = PFX(intra_pred_ang32_32_avx2);
        p.cu[BLOCK_32x32].intra_pred[33] = PFX(intra_pred_ang32_33_avx2);
        p.cu[BLOCK_32x32].intra_pred[25] = PFX(intra_pred_ang32_25_avx2);
        p.cu[BLOCK_32x32].intra_pred[24] = PFX(intra_pred_ang32_24_avx2);
        p.cu[BLOCK_32x32].intra_pred[23] = PFX(intra_pred_ang32_23_avx2);
        p.cu[BLOCK_32x32].intra_pred[22] = PFX(intra_pred_ang32_22_avx2);
        p.cu[BLOCK_32x32].intra_pred[21] = PFX(intra_pred_ang32_21_avx2);
        p.cu[BLOCK_32x32].intra_pred[18] = PFX(intra_pred_ang32_18_avx2);
        p.cu[BLOCK_32x32].intra_pred[3]  = PFX(intra_pred_ang32_3_avx2);

        // all_angs primitives
        p.cu[BLOCK_4x4].intra_pred_allangs = PFX(all_angs_pred_4x4_avx2);

        // copy_sp primitives
        p.cu[BLOCK_16x16].copy_sp = PFX(blockcopy_sp_16x16_avx2);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_16x16].copy_sp = PFX(blockcopy_sp_16x16_avx2);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_16x32].copy_sp = PFX(blockcopy_sp_16x32_avx2);

        p.cu[BLOCK_32x32].copy_sp = PFX(blockcopy_sp_32x32_avx2);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_32x32].copy_sp = PFX(blockcopy_sp_32x32_avx2);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_32x64].copy_sp = PFX(blockcopy_sp_32x64_avx2);

        p.cu[BLOCK_64x64].copy_sp = PFX(blockcopy_sp_64x64_avx2);

        // copy_ps primitives
        p.cu[BLOCK_16x16].copy_ps = PFX(blockcopy_ps_16x16_avx2);
        p.chroma[X265_CSP_I420].cu[CHROMA_420_16x16].copy_ps = PFX(blockcopy_ps_16x16_avx2);
        p.chroma[X265_CSP_I422].cu[CHROMA_422_16x32].copy_ps = PFX(blockcopy_ps_16x32_avx2);

        p.cu[BLOCK_32x32].copy_ps = PFX(blockcopy_ps_32x32_avx2);
        p.chroma[X265_CSP_I420].cu[CHROMA_420_32x32].copy_ps = PFX(blockcopy_ps_32x32_avx2);
        p.chroma[X265_CSP_I422].cu[CHROMA_422_32x64].copy_ps = PFX(blockcopy_ps_32x64_avx2);

        p.cu[BLOCK_64x64].copy_ps = PFX(blockcopy_ps_64x64_avx2);

        ALL_LUMA_TU_S(dct, dct, avx2);
        ALL_LUMA_TU_S(idct, idct, avx2);
        ALL_LUMA_CU_S(transpose, transpose, avx2);

        ALL_LUMA_PU(luma_vpp, interp_8tap_vert_pp, avx2);
        ALL_LUMA_PU(luma_vps, interp_8tap_vert_ps, avx2);
        ALL_LUMA_PU(luma_vsp, interp_8tap_vert_sp, avx2);
        ALL_LUMA_PU(luma_vss, interp_8tap_vert_ss, avx2);
        p.pu[LUMA_4x4].luma_vsp = PFX(interp_8tap_vert_sp_4x4_avx2);

        // missing 4x8, 4x16, 24x32, 12x16 for the fill set of luma PU
        p.pu[LUMA_4x4].luma_hpp = PFX(interp_8tap_horiz_pp_4x4_avx2);
        p.pu[LUMA_4x8].luma_hpp = PFX(interp_8tap_horiz_pp_4x8_avx2);
        p.pu[LUMA_4x16].luma_hpp = PFX(interp_8tap_horiz_pp_4x16_avx2);
        p.pu[LUMA_8x4].luma_hpp = PFX(interp_8tap_horiz_pp_8x4_avx2);
        p.pu[LUMA_8x8].luma_hpp = PFX(interp_8tap_horiz_pp_8x8_avx2);
        p.pu[LUMA_8x16].luma_hpp = PFX(interp_8tap_horiz_pp_8x16_avx2);
        p.pu[LUMA_8x32].luma_hpp = PFX(interp_8tap_horiz_pp_8x32_avx2);
        p.pu[LUMA_16x4].luma_hpp = PFX(interp_8tap_horiz_pp_16x4_avx2);
        p.pu[LUMA_16x8].luma_hpp = PFX(interp_8tap_horiz_pp_16x8_avx2);
        p.pu[LUMA_16x12].luma_hpp = PFX(interp_8tap_horiz_pp_16x12_avx2);
        p.pu[LUMA_16x16].luma_hpp = PFX(interp_8tap_horiz_pp_16x16_avx2);
        p.pu[LUMA_16x32].luma_hpp = PFX(interp_8tap_horiz_pp_16x32_avx2);
        p.pu[LUMA_16x64].luma_hpp = PFX(interp_8tap_horiz_pp_16x64_avx2);
        p.pu[LUMA_32x8].luma_hpp  = PFX(interp_8tap_horiz_pp_32x8_avx2);
        p.pu[LUMA_32x16].luma_hpp = PFX(interp_8tap_horiz_pp_32x16_avx2);
        p.pu[LUMA_32x24].luma_hpp = PFX(interp_8tap_horiz_pp_32x24_avx2);
        p.pu[LUMA_32x32].luma_hpp = PFX(interp_8tap_horiz_pp_32x32_avx2);
        p.pu[LUMA_32x64].luma_hpp = PFX(interp_8tap_horiz_pp_32x64_avx2);
        p.pu[LUMA_64x64].luma_hpp = PFX(interp_8tap_horiz_pp_64x64_avx2);
        p.pu[LUMA_64x48].luma_hpp = PFX(interp_8tap_horiz_pp_64x48_avx2);
        p.pu[LUMA_64x32].luma_hpp = PFX(interp_8tap_horiz_pp_64x32_avx2);
        p.pu[LUMA_64x16].luma_hpp = PFX(interp_8tap_horiz_pp_64x16_avx2);
        p.pu[LUMA_48x64].luma_hpp = PFX(interp_8tap_horiz_pp_48x64_avx2);
        p.pu[LUMA_24x32].luma_hpp = PFX(interp_8tap_horiz_pp_24x32_avx2);
        p.pu[LUMA_12x16].luma_hpp = PFX(interp_8tap_horiz_pp_12x16_avx2);

        p.pu[LUMA_4x4].luma_hps = PFX(interp_8tap_horiz_ps_4x4_avx2);
        p.pu[LUMA_4x8].luma_hps = PFX(interp_8tap_horiz_ps_4x8_avx2);
        p.pu[LUMA_4x16].luma_hps = PFX(interp_8tap_horiz_ps_4x16_avx2);
        p.pu[LUMA_8x4].luma_hps = PFX(interp_8tap_horiz_ps_8x4_avx2);
        p.pu[LUMA_8x8].luma_hps = PFX(interp_8tap_horiz_ps_8x8_avx2);
        p.pu[LUMA_8x16].luma_hps = PFX(interp_8tap_horiz_ps_8x16_avx2);
        p.pu[LUMA_8x32].luma_hps = PFX(interp_8tap_horiz_ps_8x32_avx2);
        p.pu[LUMA_16x8].luma_hps = PFX(interp_8tap_horiz_ps_16x8_avx2);
        p.pu[LUMA_16x16].luma_hps = PFX(interp_8tap_horiz_ps_16x16_avx2);
        p.pu[LUMA_16x12].luma_hps = PFX(interp_8tap_horiz_ps_16x12_avx2);
        p.pu[LUMA_16x4].luma_hps = PFX(interp_8tap_horiz_ps_16x4_avx2);
        p.pu[LUMA_16x32].luma_hps = PFX(interp_8tap_horiz_ps_16x32_avx2);
        p.pu[LUMA_16x64].luma_hps = PFX(interp_8tap_horiz_ps_16x64_avx2);

        p.pu[LUMA_32x32].luma_hps = PFX(interp_8tap_horiz_ps_32x32_avx2);
        p.pu[LUMA_32x16].luma_hps = PFX(interp_8tap_horiz_ps_32x16_avx2);
        p.pu[LUMA_32x24].luma_hps = PFX(interp_8tap_horiz_ps_32x24_avx2);
        p.pu[LUMA_32x8].luma_hps = PFX(interp_8tap_horiz_ps_32x8_avx2);
        p.pu[LUMA_32x64].luma_hps = PFX(interp_8tap_horiz_ps_32x64_avx2);
        p.pu[LUMA_48x64].luma_hps = PFX(interp_8tap_horiz_ps_48x64_avx2);
        p.pu[LUMA_64x64].luma_hps = PFX(interp_8tap_horiz_ps_64x64_avx2);
        p.pu[LUMA_64x48].luma_hps = PFX(interp_8tap_horiz_ps_64x48_avx2);
        p.pu[LUMA_64x32].luma_hps = PFX(interp_8tap_horiz_ps_64x32_avx2);
        p.pu[LUMA_64x16].luma_hps = PFX(interp_8tap_horiz_ps_64x16_avx2);
        p.pu[LUMA_12x16].luma_hps = PFX(interp_8tap_horiz_ps_12x16_avx2);
        p.pu[LUMA_24x32].luma_hps = PFX(interp_8tap_horiz_ps_24x32_avx2);

        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x8].filter_hpp = PFX(interp_4tap_horiz_pp_8x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x4].filter_hpp = PFX(interp_4tap_horiz_pp_4x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x32].filter_hpp = PFX(interp_4tap_horiz_pp_32x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x16].filter_hpp = PFX(interp_4tap_horiz_pp_16x16_avx2);

        p.chroma[X265_CSP_I420].pu[CHROMA_420_2x4].filter_hpp = PFX(interp_4tap_horiz_pp_2x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_2x8].filter_hpp = PFX(interp_4tap_horiz_pp_2x8_avx2);

        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x2].filter_hpp = PFX(interp_4tap_horiz_pp_4x2_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x8].filter_hpp = PFX(interp_4tap_horiz_pp_4x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x16].filter_hpp = PFX(interp_4tap_horiz_pp_4x16_avx2);

        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x4].filter_hpp = PFX(interp_4tap_horiz_pp_16x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x8].filter_hpp = PFX(interp_4tap_horiz_pp_16x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x12].filter_hpp = PFX(interp_4tap_horiz_pp_16x12_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x32].filter_hpp = PFX(interp_4tap_horiz_pp_16x32_avx2);

        p.chroma[X265_CSP_I420].pu[CHROMA_420_6x8].filter_hpp = PFX(interp_4tap_horiz_pp_6x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_6x16].filter_hpp = PFX(interp_4tap_horiz_pp_6x16_avx2);

        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x16].filter_hpp = PFX(interp_4tap_horiz_pp_32x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x24].filter_hpp = PFX(interp_4tap_horiz_pp_32x24_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x8].filter_hpp = PFX(interp_4tap_horiz_pp_32x8_avx2);

        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x2].filter_hpp = PFX(interp_4tap_horiz_pp_8x2_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x4].filter_hpp = PFX(interp_4tap_horiz_pp_8x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x6].filter_hpp = PFX(interp_4tap_horiz_pp_8x6_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x16].filter_hpp = PFX(interp_4tap_horiz_pp_8x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x32].filter_hpp = PFX(interp_4tap_horiz_pp_8x32_avx2);

        p.chroma[X265_CSP_I420].pu[CHROMA_420_12x16].filter_hpp = PFX(interp_4tap_horiz_pp_12x16_avx2);

        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x32].filter_hps = PFX(interp_4tap_horiz_ps_32x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x16].filter_hps = PFX(interp_4tap_horiz_ps_16x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x4].filter_hps = PFX(interp_4tap_horiz_ps_4x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x8].filter_hps = PFX(interp_4tap_horiz_ps_8x8_avx2);

        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x2].filter_hps = PFX(interp_4tap_horiz_ps_4x2_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x8].filter_hps = PFX(interp_4tap_horiz_ps_4x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x16].filter_hps = PFX(interp_4tap_horiz_ps_4x16_avx2);

        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x2].filter_hps = PFX(interp_4tap_horiz_ps_8x2_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x4].filter_hps = PFX(interp_4tap_horiz_ps_8x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x6].filter_hps = PFX(interp_4tap_horiz_ps_8x6_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x32].filter_hps = PFX(interp_4tap_horiz_ps_8x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x16].filter_hps = PFX(interp_4tap_horiz_ps_8x16_avx2);

        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x32].filter_hps = PFX(interp_4tap_horiz_ps_16x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x12].filter_hps = PFX(interp_4tap_horiz_ps_16x12_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x8].filter_hps = PFX(interp_4tap_horiz_ps_16x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x4].filter_hps = PFX(interp_4tap_horiz_ps_16x4_avx2);

        p.chroma[X265_CSP_I420].pu[CHROMA_420_24x32].filter_hps = PFX(interp_4tap_horiz_ps_24x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x16].filter_hps = PFX(interp_4tap_horiz_ps_32x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x24].filter_hps = PFX(interp_4tap_horiz_ps_32x24_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x8].filter_hps = PFX(interp_4tap_horiz_ps_32x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_2x4].filter_hps = PFX(interp_4tap_horiz_ps_2x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_2x8].filter_hps = PFX(interp_4tap_horiz_ps_2x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_6x8].filter_hps = PFX(interp_4tap_horiz_ps_6x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_24x32].filter_hpp = PFX(interp_4tap_horiz_pp_24x32_avx2);

        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x4].filter_vpp = PFX(interp_4tap_vert_pp_4x4_avx2);

        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x4].filter_vpp = PFX(interp_4tap_vert_pp_4x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x16].filter_vpp = PFX(interp_4tap_vert_pp_4x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x8].filter_vpp = PFX(interp_4tap_vert_pp_8x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_2x4].filter_vpp = PFX(interp_4tap_vert_pp_2x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_2x8].filter_vpp = PFX(interp_4tap_vert_pp_2x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x2].filter_vpp = PFX(interp_4tap_vert_pp_4x2_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x8].filter_vpp = PFX(interp_4tap_vert_pp_4x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_6x8].filter_vpp = PFX(interp_4tap_vert_pp_6x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x2].filter_vpp = PFX(interp_4tap_vert_pp_8x2_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x4].filter_vpp = PFX(interp_4tap_vert_pp_8x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x6].filter_vpp = PFX(interp_4tap_vert_pp_8x6_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x16].filter_vpp = PFX(interp_4tap_vert_pp_8x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x32].filter_vpp = PFX(interp_4tap_vert_pp_8x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_12x16].filter_vpp = PFX(interp_4tap_vert_pp_12x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x4].filter_vpp = PFX(interp_4tap_vert_pp_16x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x8].filter_vpp = PFX(interp_4tap_vert_pp_16x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x12].filter_vpp = PFX(interp_4tap_vert_pp_16x12_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x16].filter_vpp = PFX(interp_4tap_vert_pp_16x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x32].filter_vpp = PFX(interp_4tap_vert_pp_16x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_24x32].filter_vpp = PFX(interp_4tap_vert_pp_24x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x8].filter_vpp = PFX(interp_4tap_vert_pp_32x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x16].filter_vpp = PFX(interp_4tap_vert_pp_32x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x24].filter_vpp = PFX(interp_4tap_vert_pp_32x24_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x32].filter_vpp = PFX(interp_4tap_vert_pp_32x32_avx2);

        p.chroma[X265_CSP_I420].pu[CHROMA_420_2x4].filter_vps = PFX(interp_4tap_vert_ps_2x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_2x8].filter_vps = PFX(interp_4tap_vert_ps_2x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x2].filter_vps = PFX(interp_4tap_vert_ps_4x2_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x4].filter_vps = PFX(interp_4tap_vert_ps_4x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x8].filter_vps = PFX(interp_4tap_vert_ps_4x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_6x8].filter_vps = PFX(interp_4tap_vert_ps_6x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x2].filter_vps = PFX(interp_4tap_vert_ps_8x2_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x4].filter_vps = PFX(interp_4tap_vert_ps_8x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x6].filter_vps = PFX(interp_4tap_vert_ps_8x6_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x8].filter_vps = PFX(interp_4tap_vert_ps_8x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x16].filter_vps = PFX(interp_4tap_vert_ps_8x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x32].filter_vps = PFX(interp_4tap_vert_ps_8x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_12x16].filter_vps = PFX(interp_4tap_vert_ps_12x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x4].filter_vps = PFX(interp_4tap_vert_ps_16x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x8].filter_vps = PFX(interp_4tap_vert_ps_16x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x12].filter_vps = PFX(interp_4tap_vert_ps_16x12_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x16].filter_vps = PFX(interp_4tap_vert_ps_4x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x16].filter_vps = PFX(interp_4tap_vert_ps_16x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x32].filter_vps = PFX(interp_4tap_vert_ps_16x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_24x32].filter_vps = PFX(interp_4tap_vert_ps_24x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x32].filter_vps = PFX(interp_4tap_vert_ps_32x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x24].filter_vps = PFX(interp_4tap_vert_ps_32x24_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x16].filter_vps = PFX(interp_4tap_vert_ps_32x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x8].filter_vps = PFX(interp_4tap_vert_ps_32x8_avx2);

        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x4].filter_vsp = PFX(interp_4tap_vert_sp_4x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x8].filter_vsp = PFX(interp_4tap_vert_sp_8x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x16].filter_vsp = PFX(interp_4tap_vert_sp_16x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x32].filter_vsp = PFX(interp_4tap_vert_sp_32x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_2x4].filter_vsp = PFX(interp_4tap_vert_sp_2x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_2x8].filter_vsp = PFX(interp_4tap_vert_sp_2x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x2].filter_vsp = PFX(interp_4tap_vert_sp_4x2_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x8].filter_vsp = PFX(interp_4tap_vert_sp_4x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x16].filter_vsp = PFX(interp_4tap_vert_sp_4x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_6x8].filter_vsp = PFX(interp_4tap_vert_sp_6x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x2].filter_vsp = PFX(interp_4tap_vert_sp_8x2_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x4].filter_vsp = PFX(interp_4tap_vert_sp_8x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x6].filter_vsp = PFX(interp_4tap_vert_sp_8x6_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x16].filter_vsp = PFX(interp_4tap_vert_sp_8x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x32].filter_vsp = PFX(interp_4tap_vert_sp_8x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_12x16].filter_vsp = PFX(interp_4tap_vert_sp_12x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x4].filter_vsp = PFX(interp_4tap_vert_sp_16x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x8].filter_vsp = PFX(interp_4tap_vert_sp_16x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x12].filter_vsp = PFX(interp_4tap_vert_sp_16x12_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x32].filter_vsp = PFX(interp_4tap_vert_sp_16x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_24x32].filter_vsp = PFX(interp_4tap_vert_sp_24x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x8].filter_vsp = PFX(interp_4tap_vert_sp_32x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x16].filter_vsp = PFX(interp_4tap_vert_sp_32x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x24].filter_vsp = PFX(interp_4tap_vert_sp_32x24_avx2);

        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x4].filter_vss = PFX(interp_4tap_vert_ss_4x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x8].filter_vss = PFX(interp_4tap_vert_ss_8x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x16].filter_vss = PFX(interp_4tap_vert_ss_16x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x32].filter_vss = PFX(interp_4tap_vert_ss_32x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_2x4].filter_vss = PFX(interp_4tap_vert_ss_2x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_2x8].filter_vss = PFX(interp_4tap_vert_ss_2x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x2].filter_vss = PFX(interp_4tap_vert_ss_4x2_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x8].filter_vss = PFX(interp_4tap_vert_ss_4x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x16].filter_vss = PFX(interp_4tap_vert_ss_4x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_6x8].filter_vss = PFX(interp_4tap_vert_ss_6x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x2].filter_vss = PFX(interp_4tap_vert_ss_8x2_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x4].filter_vss = PFX(interp_4tap_vert_ss_8x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x6].filter_vss = PFX(interp_4tap_vert_ss_8x6_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x16].filter_vss = PFX(interp_4tap_vert_ss_8x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x32].filter_vss = PFX(interp_4tap_vert_ss_8x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_12x16].filter_vss = PFX(interp_4tap_vert_ss_12x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x4].filter_vss = PFX(interp_4tap_vert_ss_16x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x8].filter_vss = PFX(interp_4tap_vert_ss_16x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x12].filter_vss = PFX(interp_4tap_vert_ss_16x12_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x32].filter_vss = PFX(interp_4tap_vert_ss_16x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_24x32].filter_vss = PFX(interp_4tap_vert_ss_24x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x8].filter_vss = PFX(interp_4tap_vert_ss_32x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x16].filter_vss = PFX(interp_4tap_vert_ss_32x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x24].filter_vss = PFX(interp_4tap_vert_ss_32x24_avx2);

        //i422 for chroma_vss
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x8].filter_vss = PFX(interp_4tap_vert_ss_4x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x16].filter_vss = PFX(interp_4tap_vert_ss_8x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x32].filter_vss = PFX(interp_4tap_vert_ss_16x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x4].filter_vss = PFX(interp_4tap_vert_ss_4x4_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_2x8].filter_vss = PFX(interp_4tap_vert_ss_2x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x8].filter_vss = PFX(interp_4tap_vert_ss_8x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x16].filter_vss = PFX(interp_4tap_vert_ss_4x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x16].filter_vss = PFX(interp_4tap_vert_ss_16x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x32].filter_vss = PFX(interp_4tap_vert_ss_8x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x32].filter_vss = PFX(interp_4tap_vert_ss_32x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x4].filter_vss = PFX(interp_4tap_vert_ss_8x4_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x16].filter_vss = PFX(interp_4tap_vert_ss_32x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x64].filter_vss = PFX(interp_4tap_vert_ss_32x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x64].filter_vss = PFX(interp_4tap_vert_ss_16x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_24x64].filter_vss = PFX(interp_4tap_vert_ss_24x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x64].filter_vss = PFX(interp_4tap_vert_ss_8x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x48].filter_vss = PFX(interp_4tap_vert_ss_32x48_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x12].filter_vss = PFX(interp_4tap_vert_ss_8x12_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_6x16].filter_vss = PFX(interp_4tap_vert_ss_6x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_2x16].filter_vss = PFX(interp_4tap_vert_ss_2x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x24].filter_vss = PFX(interp_4tap_vert_ss_16x24_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_12x32].filter_vss = PFX(interp_4tap_vert_ss_12x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x32].filter_vss = PFX(interp_4tap_vert_ss_4x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_2x4].filter_vss = PFX(interp_4tap_vert_ss_2x4_avx2);

        //i444 for chroma_vss
        p.chroma[X265_CSP_I444].pu[LUMA_4x4].filter_vss = PFX(interp_4tap_vert_ss_4x4_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x8].filter_vss = PFX(interp_4tap_vert_ss_8x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x16].filter_vss = PFX(interp_4tap_vert_ss_16x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x32].filter_vss = PFX(interp_4tap_vert_ss_32x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x64].filter_vss = PFX(interp_4tap_vert_ss_64x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x4].filter_vss = PFX(interp_4tap_vert_ss_8x4_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_4x8].filter_vss = PFX(interp_4tap_vert_ss_4x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x8].filter_vss = PFX(interp_4tap_vert_ss_16x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x16].filter_vss = PFX(interp_4tap_vert_ss_8x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x16].filter_vss = PFX(interp_4tap_vert_ss_32x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x32].filter_vss = PFX(interp_4tap_vert_ss_16x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x12].filter_vss = PFX(interp_4tap_vert_ss_16x12_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_12x16].filter_vss = PFX(interp_4tap_vert_ss_12x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x4].filter_vss = PFX(interp_4tap_vert_ss_16x4_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_4x16].filter_vss = PFX(interp_4tap_vert_ss_4x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x24].filter_vss = PFX(interp_4tap_vert_ss_32x24_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_24x32].filter_vss = PFX(interp_4tap_vert_ss_24x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x8].filter_vss = PFX(interp_4tap_vert_ss_32x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x32].filter_vss = PFX(interp_4tap_vert_ss_8x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x32].filter_vss = PFX(interp_4tap_vert_ss_64x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x64].filter_vss = PFX(interp_4tap_vert_ss_32x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x48].filter_vss = PFX(interp_4tap_vert_ss_64x48_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_48x64].filter_vss = PFX(interp_4tap_vert_ss_48x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x16].filter_vss = PFX(interp_4tap_vert_ss_64x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x64].filter_vss = PFX(interp_4tap_vert_ss_16x64_avx2);

        p.pu[LUMA_16x16].luma_hvpp = PFX(interp_8tap_hv_pp_16x16_avx2);

        ALL_LUMA_PU_T(luma_hvpp, interp_8tap_hv_pp_cpu);
        p.pu[LUMA_4x4].luma_hvpp = interp_8tap_hv_pp_cpu<LUMA_4x4>;

        p.pu[LUMA_16x4].convert_p2s = PFX(filterPixelToShort_16x4_avx2);
        p.pu[LUMA_16x8].convert_p2s = PFX(filterPixelToShort_16x8_avx2);
        p.pu[LUMA_16x12].convert_p2s = PFX(filterPixelToShort_16x12_avx2);
        p.pu[LUMA_16x16].convert_p2s = PFX(filterPixelToShort_16x16_avx2);
        p.pu[LUMA_16x32].convert_p2s = PFX(filterPixelToShort_16x32_avx2);
        p.pu[LUMA_16x64].convert_p2s = PFX(filterPixelToShort_16x64_avx2);
        p.pu[LUMA_32x8].convert_p2s = PFX(filterPixelToShort_32x8_avx2);
        p.pu[LUMA_32x16].convert_p2s = PFX(filterPixelToShort_32x16_avx2);
        p.pu[LUMA_32x24].convert_p2s = PFX(filterPixelToShort_32x24_avx2);
        p.pu[LUMA_32x32].convert_p2s = PFX(filterPixelToShort_32x32_avx2);
        p.pu[LUMA_32x64].convert_p2s = PFX(filterPixelToShort_32x64_avx2);
        p.pu[LUMA_64x16].convert_p2s = PFX(filterPixelToShort_64x16_avx2);
        p.pu[LUMA_64x32].convert_p2s = PFX(filterPixelToShort_64x32_avx2);
        p.pu[LUMA_64x48].convert_p2s = PFX(filterPixelToShort_64x48_avx2);
        p.pu[LUMA_64x64].convert_p2s = PFX(filterPixelToShort_64x64_avx2);
        p.pu[LUMA_48x64].convert_p2s = PFX(filterPixelToShort_48x64_avx2);
        p.pu[LUMA_24x32].convert_p2s = PFX(filterPixelToShort_24x32_avx2);

        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x4].p2s = PFX(filterPixelToShort_16x4_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x8].p2s = PFX(filterPixelToShort_16x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x12].p2s = PFX(filterPixelToShort_16x12_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x16].p2s = PFX(filterPixelToShort_16x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x32].p2s = PFX(filterPixelToShort_16x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_24x32].p2s = PFX(filterPixelToShort_24x32_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x8].p2s = PFX(filterPixelToShort_32x8_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x16].p2s = PFX(filterPixelToShort_32x16_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x24].p2s = PFX(filterPixelToShort_32x24_avx2);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x32].p2s = PFX(filterPixelToShort_32x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x8].p2s = PFX(filterPixelToShort_16x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x16].p2s = PFX(filterPixelToShort_16x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x24].p2s = PFX(filterPixelToShort_16x24_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x32].p2s = PFX(filterPixelToShort_16x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x64].p2s = PFX(filterPixelToShort_16x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_24x64].p2s = PFX(filterPixelToShort_24x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x16].p2s = PFX(filterPixelToShort_32x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x32].p2s = PFX(filterPixelToShort_32x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x48].p2s = PFX(filterPixelToShort_32x48_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x64].p2s = PFX(filterPixelToShort_32x64_avx2);

        //i422 for chroma_hpp
        p.chroma[X265_CSP_I422].pu[CHROMA_422_12x32].filter_hpp = PFX(interp_4tap_horiz_pp_12x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_24x64].filter_hpp = PFX(interp_4tap_horiz_pp_24x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_2x16].filter_hpp = PFX(interp_4tap_horiz_pp_2x16_avx2);

        p.chroma[X265_CSP_I422].pu[CHROMA_422_2x16].filter_hpp = PFX(interp_4tap_horiz_pp_2x16_avx2);

        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x4].filter_hpp = PFX(interp_4tap_horiz_pp_4x4_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x8].filter_hpp = PFX(interp_4tap_horiz_pp_4x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x16].filter_hpp = PFX(interp_4tap_horiz_pp_4x16_avx2);
        
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x4].filter_hpp = PFX(interp_4tap_horiz_pp_8x4_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x8].filter_hpp = PFX(interp_4tap_horiz_pp_8x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x16].filter_hpp = PFX(interp_4tap_horiz_pp_8x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x32].filter_hpp = PFX(interp_4tap_horiz_pp_8x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x64].filter_hpp = PFX(interp_4tap_horiz_pp_8x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x12].filter_hpp = PFX(interp_4tap_horiz_pp_8x12_avx2);
        
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x8].filter_hpp = PFX(interp_4tap_horiz_pp_16x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x16].filter_hpp = PFX(interp_4tap_horiz_pp_16x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x32].filter_hpp = PFX(interp_4tap_horiz_pp_16x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x64].filter_hpp = PFX(interp_4tap_horiz_pp_16x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x24].filter_hpp = PFX(interp_4tap_horiz_pp_16x24_avx2);
        
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x16].filter_hpp = PFX(interp_4tap_horiz_pp_32x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x32].filter_hpp = PFX(interp_4tap_horiz_pp_32x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x64].filter_hpp = PFX(interp_4tap_horiz_pp_32x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x48].filter_hpp = PFX(interp_4tap_horiz_pp_32x48_avx2);
        
        p.chroma[X265_CSP_I422].pu[CHROMA_422_2x8].filter_hpp = PFX(interp_4tap_horiz_pp_2x8_avx2);

        //i444 filters hpp

        p.chroma[X265_CSP_I444].pu[LUMA_4x4].filter_hpp = PFX(interp_4tap_horiz_pp_4x4_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x8].filter_hpp = PFX(interp_4tap_horiz_pp_8x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x16].filter_hpp = PFX(interp_4tap_horiz_pp_16x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x32].filter_hpp = PFX(interp_4tap_horiz_pp_32x32_avx2);

        p.chroma[X265_CSP_I444].pu[LUMA_4x8].filter_hpp = PFX(interp_4tap_horiz_pp_4x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_4x16].filter_hpp = PFX(interp_4tap_horiz_pp_4x16_avx2);

        p.chroma[X265_CSP_I444].pu[LUMA_8x4].filter_hpp = PFX(interp_4tap_horiz_pp_8x4_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x16].filter_hpp = PFX(interp_4tap_horiz_pp_8x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x32].filter_hpp = PFX(interp_4tap_horiz_pp_8x32_avx2);

        p.chroma[X265_CSP_I444].pu[LUMA_16x8].filter_hpp = PFX(interp_4tap_horiz_pp_16x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x32].filter_hpp = PFX(interp_4tap_horiz_pp_16x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x12].filter_hpp = PFX(interp_4tap_horiz_pp_16x12_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x4].filter_hpp = PFX(interp_4tap_horiz_pp_16x4_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x64].filter_hpp = PFX(interp_4tap_horiz_pp_16x64_avx2);

        p.chroma[X265_CSP_I444].pu[LUMA_12x16].filter_hpp = PFX(interp_4tap_horiz_pp_12x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_24x32].filter_hpp = PFX(interp_4tap_horiz_pp_24x32_avx2);

        p.chroma[X265_CSP_I444].pu[LUMA_32x16].filter_hpp = PFX(interp_4tap_horiz_pp_32x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x64].filter_hpp = PFX(interp_4tap_horiz_pp_32x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x24].filter_hpp = PFX(interp_4tap_horiz_pp_32x24_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x8].filter_hpp = PFX(interp_4tap_horiz_pp_32x8_avx2);

        p.chroma[X265_CSP_I444].pu[LUMA_64x64].filter_hpp = PFX(interp_4tap_horiz_pp_64x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x32].filter_hpp = PFX(interp_4tap_horiz_pp_64x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x48].filter_hpp = PFX(interp_4tap_horiz_pp_64x48_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x16].filter_hpp = PFX(interp_4tap_horiz_pp_64x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_48x64].filter_hpp = PFX(interp_4tap_horiz_pp_48x64_avx2);

        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x4].filter_hps = PFX(interp_4tap_horiz_ps_4x4_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x8].filter_hps = PFX(interp_4tap_horiz_ps_4x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x16].filter_hps = PFX(interp_4tap_horiz_ps_4x16_avx2);

        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x4].filter_hps = PFX(interp_4tap_horiz_ps_8x4_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x8].filter_hps = PFX(interp_4tap_horiz_ps_8x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x16].filter_hps = PFX(interp_4tap_horiz_ps_8x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x32].filter_hps = PFX(interp_4tap_horiz_ps_8x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x64].filter_hps = PFX(interp_4tap_horiz_ps_8x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x12].filter_hps = PFX(interp_4tap_horiz_ps_8x12_avx2);

        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x8].filter_hps = PFX(interp_4tap_horiz_ps_16x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x16].filter_hps = PFX(interp_4tap_horiz_ps_16x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x32].filter_hps = PFX(interp_4tap_horiz_ps_16x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x64].filter_hps = PFX(interp_4tap_horiz_ps_16x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x24].filter_hps = PFX(interp_4tap_horiz_ps_16x24_avx2);

        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x16].filter_hps = PFX(interp_4tap_horiz_ps_32x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x32].filter_hps = PFX(interp_4tap_horiz_ps_32x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x64].filter_hps = PFX(interp_4tap_horiz_ps_32x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x48].filter_hps = PFX(interp_4tap_horiz_ps_32x48_avx2);

        p.chroma[X265_CSP_I422].pu[CHROMA_422_2x8].filter_hps = PFX(interp_4tap_horiz_ps_2x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_24x64].filter_hps = PFX(interp_4tap_horiz_ps_24x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_2x16].filter_hps = PFX(interp_4tap_horiz_ps_2x16_avx2);

        //i444 chroma_hps
        p.chroma[X265_CSP_I444].pu[LUMA_64x32].filter_hps = PFX(interp_4tap_horiz_ps_64x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x48].filter_hps = PFX(interp_4tap_horiz_ps_64x48_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x16].filter_hps = PFX(interp_4tap_horiz_ps_64x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x64].filter_hps = PFX(interp_4tap_horiz_ps_64x64_avx2);

        p.chroma[X265_CSP_I444].pu[LUMA_4x4].filter_hps = PFX(interp_4tap_horiz_ps_4x4_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x8].filter_hps = PFX(interp_4tap_horiz_ps_8x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x16].filter_hps = PFX(interp_4tap_horiz_ps_16x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x32].filter_hps = PFX(interp_4tap_horiz_ps_32x32_avx2);

        p.chroma[X265_CSP_I444].pu[LUMA_4x8].filter_hps = PFX(interp_4tap_horiz_ps_4x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_4x16].filter_hps = PFX(interp_4tap_horiz_ps_4x16_avx2);

        p.chroma[X265_CSP_I444].pu[LUMA_8x4].filter_hps = PFX(interp_4tap_horiz_ps_8x4_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x16].filter_hps = PFX(interp_4tap_horiz_ps_8x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x32].filter_hps = PFX(interp_4tap_horiz_ps_8x32_avx2);

        p.chroma[X265_CSP_I444].pu[LUMA_16x8].filter_hps = PFX(interp_4tap_horiz_ps_16x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x32].filter_hps = PFX(interp_4tap_horiz_ps_16x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x12].filter_hps = PFX(interp_4tap_horiz_ps_16x12_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x4].filter_hps = PFX(interp_4tap_horiz_ps_16x4_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x64].filter_hps = PFX(interp_4tap_horiz_ps_16x64_avx2);

        p.chroma[X265_CSP_I444].pu[LUMA_24x32].filter_hps = PFX(interp_4tap_horiz_ps_24x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_48x64].filter_hps = PFX(interp_4tap_horiz_ps_48x64_avx2);

        p.chroma[X265_CSP_I444].pu[LUMA_32x16].filter_hps = PFX(interp_4tap_horiz_ps_32x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x64].filter_hps = PFX(interp_4tap_horiz_ps_32x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x24].filter_hps = PFX(interp_4tap_horiz_ps_32x24_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x8].filter_hps = PFX(interp_4tap_horiz_ps_32x8_avx2);

        //i422 for chroma_vsp
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x8].filter_vsp = PFX(interp_4tap_vert_sp_4x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x16].filter_vsp = PFX(interp_4tap_vert_sp_8x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x32].filter_vsp = PFX(interp_4tap_vert_sp_16x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x4].filter_vsp = PFX(interp_4tap_vert_sp_4x4_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_2x8].filter_vsp = PFX(interp_4tap_vert_sp_2x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x8].filter_vsp = PFX(interp_4tap_vert_sp_8x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x16].filter_vsp = PFX(interp_4tap_vert_sp_4x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x16].filter_vsp = PFX(interp_4tap_vert_sp_16x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x32].filter_vsp = PFX(interp_4tap_vert_sp_8x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x32].filter_vsp = PFX(interp_4tap_vert_sp_32x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x4].filter_vsp = PFX(interp_4tap_vert_sp_8x4_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x8].filter_vsp = PFX(interp_4tap_vert_sp_16x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x16].filter_vsp = PFX(interp_4tap_vert_sp_32x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x64].filter_vsp = PFX(interp_4tap_vert_sp_32x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x64].filter_vsp = PFX(interp_4tap_vert_sp_16x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_24x64].filter_vsp = PFX(interp_4tap_vert_sp_24x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x64].filter_vsp = PFX(interp_4tap_vert_sp_8x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x48].filter_vsp = PFX(interp_4tap_vert_sp_32x48_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x12].filter_vsp = PFX(interp_4tap_vert_sp_8x12_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_6x16].filter_vsp = PFX(interp_4tap_vert_sp_6x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_2x16].filter_vsp = PFX(interp_4tap_vert_sp_2x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x24].filter_vsp = PFX(interp_4tap_vert_sp_16x24_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_12x32].filter_vsp = PFX(interp_4tap_vert_sp_12x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x32].filter_vsp = PFX(interp_4tap_vert_sp_4x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_2x4].filter_vsp = PFX(interp_4tap_vert_sp_2x4_avx2);

        //i444 for chroma_vsp
        p.chroma[X265_CSP_I444].pu[LUMA_4x4].filter_vsp = PFX(interp_4tap_vert_sp_4x4_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x8].filter_vsp = PFX(interp_4tap_vert_sp_8x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x16].filter_vsp = PFX(interp_4tap_vert_sp_16x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x32].filter_vsp = PFX(interp_4tap_vert_sp_32x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x64].filter_vsp = PFX(interp_4tap_vert_sp_64x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x4].filter_vsp = PFX(interp_4tap_vert_sp_8x4_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_4x8].filter_vsp = PFX(interp_4tap_vert_sp_4x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x8].filter_vsp = PFX(interp_4tap_vert_sp_16x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x16].filter_vsp = PFX(interp_4tap_vert_sp_8x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x16].filter_vsp = PFX(interp_4tap_vert_sp_32x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x32].filter_vsp = PFX(interp_4tap_vert_sp_16x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x12].filter_vsp = PFX(interp_4tap_vert_sp_16x12_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_12x16].filter_vsp = PFX(interp_4tap_vert_sp_12x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x4].filter_vsp = PFX(interp_4tap_vert_sp_16x4_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_4x16].filter_vsp = PFX(interp_4tap_vert_sp_4x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x24].filter_vsp = PFX(interp_4tap_vert_sp_32x24_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_24x32].filter_vsp = PFX(interp_4tap_vert_sp_24x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x8].filter_vsp = PFX(interp_4tap_vert_sp_32x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x32].filter_vsp = PFX(interp_4tap_vert_sp_8x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x32].filter_vsp = PFX(interp_4tap_vert_sp_64x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x64].filter_vsp = PFX(interp_4tap_vert_sp_32x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x48].filter_vsp = PFX(interp_4tap_vert_sp_64x48_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_48x64].filter_vsp = PFX(interp_4tap_vert_sp_48x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x16].filter_vsp = PFX(interp_4tap_vert_sp_64x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x64].filter_vsp = PFX(interp_4tap_vert_sp_16x64_avx2);

        //i422 for chroma_vps
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x8].filter_vps = PFX(interp_4tap_vert_ps_4x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x16].filter_vps = PFX(interp_4tap_vert_ps_8x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x32].filter_vps = PFX(interp_4tap_vert_ps_16x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x4].filter_vps = PFX(interp_4tap_vert_ps_4x4_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_2x8].filter_vps = PFX(interp_4tap_vert_ps_2x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x8].filter_vps = PFX(interp_4tap_vert_ps_8x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x16].filter_vps = PFX(interp_4tap_vert_ps_4x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x16].filter_vps = PFX(interp_4tap_vert_ps_16x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x32].filter_vps = PFX(interp_4tap_vert_ps_8x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x32].filter_vps = PFX(interp_4tap_vert_ps_32x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x4].filter_vps = PFX(interp_4tap_vert_ps_8x4_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x8].filter_vps = PFX(interp_4tap_vert_ps_16x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x16].filter_vps = PFX(interp_4tap_vert_ps_32x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x64].filter_vps = PFX(interp_4tap_vert_ps_16x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x64].filter_vps = PFX(interp_4tap_vert_ps_8x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x64].filter_vps = PFX(interp_4tap_vert_ps_32x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x48].filter_vps = PFX(interp_4tap_vert_ps_32x48_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_12x32].filter_vps = PFX(interp_4tap_vert_ps_12x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x12].filter_vps = PFX(interp_4tap_vert_ps_8x12_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_2x4].filter_vps = PFX(interp_4tap_vert_ps_2x4_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x24].filter_vps = PFX(interp_4tap_vert_ps_16x24_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_2x16].filter_vps = PFX(interp_4tap_vert_ps_2x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x32].filter_vps = PFX(interp_4tap_vert_ps_4x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_24x64].filter_vps = PFX(interp_4tap_vert_ps_24x64_avx2);

        //i444 for chroma_vps
        p.chroma[X265_CSP_I444].pu[LUMA_4x4].filter_vps = PFX(interp_4tap_vert_ps_4x4_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x8].filter_vps = PFX(interp_4tap_vert_ps_8x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x16].filter_vps = PFX(interp_4tap_vert_ps_16x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x32].filter_vps = PFX(interp_4tap_vert_ps_32x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x4].filter_vps = PFX(interp_4tap_vert_ps_8x4_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_4x8].filter_vps = PFX(interp_4tap_vert_ps_4x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x8].filter_vps = PFX(interp_4tap_vert_ps_16x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x16].filter_vps = PFX(interp_4tap_vert_ps_8x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x16].filter_vps = PFX(interp_4tap_vert_ps_32x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x32].filter_vps = PFX(interp_4tap_vert_ps_16x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x12].filter_vps = PFX(interp_4tap_vert_ps_16x12_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_12x16].filter_vps = PFX(interp_4tap_vert_ps_12x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x4].filter_vps = PFX(interp_4tap_vert_ps_16x4_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_4x16].filter_vps = PFX(interp_4tap_vert_ps_4x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x24].filter_vps = PFX(interp_4tap_vert_ps_32x24_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_24x32].filter_vps = PFX(interp_4tap_vert_ps_24x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x8].filter_vps = PFX(interp_4tap_vert_ps_32x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x32].filter_vps = PFX(interp_4tap_vert_ps_8x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x64].filter_vps = PFX(interp_4tap_vert_ps_16x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x64].filter_vps = PFX(interp_4tap_vert_ps_32x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_48x64].filter_vps = PFX(interp_4tap_vert_ps_48x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x64].filter_vps = PFX(interp_4tap_vert_ps_64x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x48].filter_vps = PFX(interp_4tap_vert_ps_64x48_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x32].filter_vps = PFX(interp_4tap_vert_ps_64x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x16].filter_vps = PFX(interp_4tap_vert_ps_64x16_avx2);

        //i422 for chroma_vpp
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x8].filter_vpp = PFX(interp_4tap_vert_pp_4x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x16].filter_vpp = PFX(interp_4tap_vert_pp_8x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x32].filter_vpp = PFX(interp_4tap_vert_pp_16x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x4].filter_vpp = PFX(interp_4tap_vert_pp_4x4_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_2x8].filter_vpp = PFX(interp_4tap_vert_pp_2x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x8].filter_vpp = PFX(interp_4tap_vert_pp_8x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x16].filter_vpp = PFX(interp_4tap_vert_pp_4x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x16].filter_vpp = PFX(interp_4tap_vert_pp_16x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x32].filter_vpp = PFX(interp_4tap_vert_pp_8x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x32].filter_vpp = PFX(interp_4tap_vert_pp_32x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x4].filter_vpp = PFX(interp_4tap_vert_pp_8x4_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x8].filter_vpp = PFX(interp_4tap_vert_pp_16x8_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x16].filter_vpp = PFX(interp_4tap_vert_pp_32x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x64].filter_vpp = PFX(interp_4tap_vert_pp_16x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x64].filter_vpp = PFX(interp_4tap_vert_pp_8x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x64].filter_vpp = PFX(interp_4tap_vert_pp_32x64_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x48].filter_vpp = PFX(interp_4tap_vert_pp_32x48_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_12x32].filter_vpp = PFX(interp_4tap_vert_pp_12x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x12].filter_vpp = PFX(interp_4tap_vert_pp_8x12_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_2x4].filter_vpp = PFX(interp_4tap_vert_pp_2x4_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x24].filter_vpp = PFX(interp_4tap_vert_pp_16x24_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_2x16].filter_vpp = PFX(interp_4tap_vert_pp_2x16_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x32].filter_vpp = PFX(interp_4tap_vert_pp_4x32_avx2);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_24x64].filter_vpp = PFX(interp_4tap_vert_pp_24x64_avx2);

        //i444 for chroma_vpp
        p.chroma[X265_CSP_I444].pu[LUMA_4x4].filter_vpp = PFX(interp_4tap_vert_pp_4x4_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x8].filter_vpp = PFX(interp_4tap_vert_pp_8x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x16].filter_vpp = PFX(interp_4tap_vert_pp_16x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x32].filter_vpp = PFX(interp_4tap_vert_pp_32x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x4].filter_vpp = PFX(interp_4tap_vert_pp_8x4_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_4x8].filter_vpp = PFX(interp_4tap_vert_pp_4x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x8].filter_vpp = PFX(interp_4tap_vert_pp_16x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x16].filter_vpp = PFX(interp_4tap_vert_pp_8x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x16].filter_vpp = PFX(interp_4tap_vert_pp_32x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x32].filter_vpp = PFX(interp_4tap_vert_pp_16x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x12].filter_vpp = PFX(interp_4tap_vert_pp_16x12_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_12x16].filter_vpp = PFX(interp_4tap_vert_pp_12x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x4].filter_vpp = PFX(interp_4tap_vert_pp_16x4_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_4x16].filter_vpp = PFX(interp_4tap_vert_pp_4x16_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x24].filter_vpp = PFX(interp_4tap_vert_pp_32x24_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_24x32].filter_vpp = PFX(interp_4tap_vert_pp_24x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x8].filter_vpp = PFX(interp_4tap_vert_pp_32x8_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_8x32].filter_vpp = PFX(interp_4tap_vert_pp_8x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_16x64].filter_vpp = PFX(interp_4tap_vert_pp_16x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_32x64].filter_vpp = PFX(interp_4tap_vert_pp_32x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_48x64].filter_vpp = PFX(interp_4tap_vert_pp_48x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x64].filter_vpp = PFX(interp_4tap_vert_pp_64x64_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x48].filter_vpp = PFX(interp_4tap_vert_pp_64x48_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x32].filter_vpp = PFX(interp_4tap_vert_pp_64x32_avx2);
        p.chroma[X265_CSP_I444].pu[LUMA_64x16].filter_vpp = PFX(interp_4tap_vert_pp_64x16_avx2);

        p.frameInitLowres = PFX(frame_init_lowres_core_avx2);
        p.propagateCost = PFX(mbtree_propagate_cost_avx2);
        p.saoCuStatsE0 = PFX(saoCuStatsE0_avx2);
        p.saoCuStatsE1 = PFX(saoCuStatsE1_avx2);
        p.saoCuStatsE2 = PFX(saoCuStatsE2_avx2);
        p.saoCuStatsE3 = PFX(saoCuStatsE3_avx2);

        if (cpuMask & X265_CPU_BMI2)
        {
            p.scanPosLast = PFX(scanPosLast_avx2_bmi2);
            p.costCoeffNxN = PFX(costCoeffNxN_avx2_bmi2);
        }
        p.cu[BLOCK_32x32].copy_ps = PFX(blockcopy_ps_32x32_avx2);
        p.chroma[X265_CSP_I420].cu[CHROMA_420_32x32].copy_ps = PFX(blockcopy_ps_32x32_avx2);
        p.chroma[X265_CSP_I422].cu[CHROMA_422_32x64].copy_ps = PFX(blockcopy_ps_32x64_avx2);
        p.cu[BLOCK_64x64].copy_ps = PFX(blockcopy_ps_64x64_avx2);

        p.pu[LUMA_32x8].sad_x3 = PFX(pixel_sad_x3_32x8_avx2);
        p.pu[LUMA_32x16].sad_x3 = PFX(pixel_sad_x3_32x16_avx2);
        p.pu[LUMA_32x24].sad_x3 = PFX(pixel_sad_x3_32x24_avx2);
        p.pu[LUMA_32x32].sad_x3 = PFX(pixel_sad_x3_32x32_avx2);
        p.pu[LUMA_32x64].sad_x3 = PFX(pixel_sad_x3_32x64_avx2);
        p.pu[LUMA_64x16].sad_x3 = PFX(pixel_sad_x3_64x16_avx2);
        p.pu[LUMA_64x32].sad_x3 = PFX(pixel_sad_x3_64x32_avx2);
        p.pu[LUMA_64x48].sad_x3 = PFX(pixel_sad_x3_64x48_avx2);
        p.pu[LUMA_64x64].sad_x3 = PFX(pixel_sad_x3_64x64_avx2);
        p.pu[LUMA_48x64].sad_x3 = PFX(pixel_sad_x3_48x64_avx2);
        p.fix8Unpack = PFX(cutree_fix8_unpack_avx2);
        p.fix8Pack = PFX(cutree_fix8_pack_avx2);

        p.integral_initv[INTEGRAL_4] = PFX(integral4v_avx2);
        p.integral_initv[INTEGRAL_8] = PFX(integral8v_avx2);
        p.integral_initv[INTEGRAL_12] = PFX(integral12v_avx2);
        p.integral_initv[INTEGRAL_16] = PFX(integral16v_avx2);
        p.integral_initv[INTEGRAL_24] = PFX(integral24v_avx2);
        p.integral_initv[INTEGRAL_32] = PFX(integral32v_avx2);
        p.integral_inith[INTEGRAL_4] = PFX(integral4h_avx2);
        p.integral_inith[INTEGRAL_8] = PFX(integral8h_avx2);
        p.integral_inith[INTEGRAL_12] = PFX(integral12h_avx2);
        p.integral_inith[INTEGRAL_16] = PFX(integral16h_avx2);
        p.integral_inith[INTEGRAL_24] = PFX(integral24h_avx2);
        p.integral_inith[INTEGRAL_32] = PFX(integral32h_avx2);

    }
#endif
}
#endif // if HIGH_BIT_DEPTH

} // namespace X265_NS

extern "C" {
#ifdef __INTEL_COMPILER

/* Agner's patch to Intel's CPU dispatcher from pages 131-132 of
 * http://agner.org/optimize/optimizing_cpp.pdf (2011-01-30)
 * adapted to x265's cpu schema. */

// Global variable indicating cpu
int __intel_cpu_indicator = 0;
// CPU dispatcher function
void PFX(intel_cpu_indicator_init)(void)
{
    uint32_t cpu = x265::cpu_detect();

    if (cpu & X265_CPU_AVX)
        __intel_cpu_indicator = 0x20000;
    else if (cpu & X265_CPU_SSE42)
        __intel_cpu_indicator = 0x8000;
    else if (cpu & X265_CPU_SSE4)
        __intel_cpu_indicator = 0x2000;
    else if (cpu & X265_CPU_SSSE3)
        __intel_cpu_indicator = 0x1000;
    else if (cpu & X265_CPU_SSE3)
        __intel_cpu_indicator = 0x800;
    else if (cpu & X265_CPU_SSE2 && !(cpu & X265_CPU_SSE2_IS_SLOW))
        __intel_cpu_indicator = 0x200;
    else if (cpu & X265_CPU_SSE)
        __intel_cpu_indicator = 0x80;
    else if (cpu & X265_CPU_MMX2)
        __intel_cpu_indicator = 8;
    else
        __intel_cpu_indicator = 1;
}

/* __intel_cpu_indicator_init appears to have a non-standard calling convention that
 * assumes certain registers aren't preserved, so we'll route it through a function
 * that backs up all the registers. */
void __intel_cpu_indicator_init(void)
{
    x265_safe_intel_cpu_indicator_init();
}

#else // ifdef __INTEL_COMPILER
#ifdef __GNUC__
__attribute__((visibility("hidden")))
#endif
void PFX(intel_cpu_indicator_init)(void) {}

#endif // ifdef __INTEL_COMPILER
}
