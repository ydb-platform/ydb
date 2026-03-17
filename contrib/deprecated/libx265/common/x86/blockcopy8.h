/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Steve Borho <steve@borho.org>
;*          Min Chen <chenm003@163.com>
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

#ifndef X265_BLOCKCOPY8_H
#define X265_BLOCKCOPY8_H

FUNCDEF_TU_S(void, cpy2Dto1D_shl, sse2, int16_t* dst, const int16_t* src, intptr_t srcStride, int shift);
FUNCDEF_TU_S(void, cpy2Dto1D_shl, sse4, int16_t* dst, const int16_t* src, intptr_t srcStride, int shift);
FUNCDEF_TU_S(void, cpy2Dto1D_shl, avx2, int16_t* dst, const int16_t* src, intptr_t srcStride, int shift);

FUNCDEF_TU_S(void, cpy2Dto1D_shr, sse2, int16_t* dst, const int16_t* src, intptr_t srcStride, int shift);
FUNCDEF_TU_S(void, cpy2Dto1D_shr, sse4, int16_t* dst, const int16_t* src, intptr_t srcStride, int shift);
FUNCDEF_TU_S(void, cpy2Dto1D_shr, avx2, int16_t* dst, const int16_t* src, intptr_t srcStride, int shift);

FUNCDEF_TU_S(void, cpy1Dto2D_shl, sse2, int16_t* dst, const int16_t* src, intptr_t srcStride, int shift);
FUNCDEF_TU_S(void, cpy1Dto2D_shl, sse4, int16_t* dst, const int16_t* src, intptr_t srcStride, int shift);
FUNCDEF_TU_S(void, cpy1Dto2D_shl, avx2, int16_t* dst, const int16_t* src, intptr_t srcStride, int shift);

FUNCDEF_TU_S(void, cpy1Dto2D_shr, sse2, int16_t* dst, const int16_t* src, intptr_t srcStride, int shift);
FUNCDEF_TU_S(void, cpy1Dto2D_shr, sse4, int16_t* dst, const int16_t* src, intptr_t srcStride, int shift);
FUNCDEF_TU_S(void, cpy1Dto2D_shr, avx2, int16_t* dst, const int16_t* src, intptr_t srcStride, int shift);

FUNCDEF_TU_S(uint32_t, copy_cnt, sse2, int16_t* dst, const int16_t* src, intptr_t srcStride);
FUNCDEF_TU_S(uint32_t, copy_cnt, sse4, int16_t* dst, const int16_t* src, intptr_t srcStride);
FUNCDEF_TU_S(uint32_t, copy_cnt, avx2, int16_t* dst, const int16_t* src, intptr_t srcStride);

FUNCDEF_TU(void, blockfill_s, sse2, int16_t* dst, intptr_t dstride, int16_t val);
FUNCDEF_TU(void, blockfill_s, avx2, int16_t* dst, intptr_t dstride, int16_t val);

FUNCDEF_CHROMA_PU(void, blockcopy_ss, sse2, int16_t* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride);
FUNCDEF_CHROMA_PU(void, blockcopy_ss, avx, int16_t* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride);

FUNCDEF_CHROMA_PU(void, blockcopy_pp, sse2, pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
FUNCDEF_CHROMA_PU(void, blockcopy_pp, avx, pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);

FUNCDEF_PU(void, blockcopy_sp, sse2, pixel* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride);
FUNCDEF_PU(void, blockcopy_sp, sse4, pixel* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride);
FUNCDEF_PU(void, blockcopy_sp, avx2, pixel* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride);
FUNCDEF_PU(void, blockcopy_ps, sse2, int16_t* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
FUNCDEF_PU(void, blockcopy_ps, sse4, int16_t* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
FUNCDEF_PU(void, blockcopy_ps, avx2, int16_t* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);

#endif // ifndef X265_I386_PIXEL_H
