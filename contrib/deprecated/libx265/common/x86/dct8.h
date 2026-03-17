/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Nabajit Deka <nabajit@multicorewareinc.com>
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

#ifndef X265_DCT8_H
#define X265_DCT8_H

FUNCDEF_TU_S2(void, dct, sse2, const int16_t* src, int16_t* dst, intptr_t srcStride);
FUNCDEF_TU_S2(void, dct, ssse3, const int16_t* src, int16_t* dst, intptr_t srcStride);
FUNCDEF_TU_S2(void, dct, sse4, const int16_t* src, int16_t* dst, intptr_t srcStride);
FUNCDEF_TU_S2(void, dct, avx2, const int16_t* src, int16_t* dst, intptr_t srcStride);

FUNCDEF_TU_S2(void, idct, sse2, const int16_t* src, int16_t* dst, intptr_t dstStride);
FUNCDEF_TU_S2(void, idct, ssse3, const int16_t* src, int16_t* dst, intptr_t dstStride);
FUNCDEF_TU_S2(void, idct, sse4, const int16_t* src, int16_t* dst, intptr_t dstStride);
FUNCDEF_TU_S2(void, idct, avx2, const int16_t* src, int16_t* dst, intptr_t dstStride);

void PFX(dst4_ssse3)(const int16_t* src, int16_t* dst, intptr_t srcStride);
void PFX(dst4_sse2)(const int16_t* src, int16_t* dst, intptr_t srcStride);
void PFX(idst4_sse2)(const int16_t* src, int16_t* dst, intptr_t srcStride);
void PFX(dst4_avx2)(const int16_t* src, int16_t* dst, intptr_t srcStride);
void PFX(idst4_avx2)(const int16_t* src, int16_t* dst, intptr_t srcStride);
void PFX(denoise_dct_sse4)(int16_t* dct, uint32_t* sum, const uint16_t* offset, int size);
void PFX(denoise_dct_avx2)(int16_t* dct, uint32_t* sum, const uint16_t* offset, int size);

#endif // ifndef X265_DCT8_H
