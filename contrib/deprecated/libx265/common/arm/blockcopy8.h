/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Steve Borho <steve@borho.org>
 *          Min Chen <chenm003@163.com>
 *          Dnyaneshwar Gorade <dnyaneshwar@multicorewareinc.com>
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

#ifndef X265_BLOCKCOPY8_ARM_H
#define X265_BLOCKCOPY8_ARM_H

void x265_blockcopy_pp_16x16_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_8x4_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_8x8_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_8x16_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_8x32_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_12x16_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_4x4_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_4x8_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_4x16_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_16x4_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_16x8_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_16x12_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_16x32_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_16x64_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_24x32_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_32x8_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_32x16_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_32x24_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_32x32_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_32x64_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_48x64_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_64x16_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_64x32_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_64x48_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_64x64_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_2x4_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_2x8_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_2x16_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_6x8_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_6x16_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_8x2_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_8x6_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_8x12_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_8x64_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_12x32_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_4x2_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_4x32_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_16x24_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_24x64_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
void x265_blockcopy_pp_32x48_neon(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);

void x265_cpy2Dto1D_shr_4x4_neon(int16_t* dst, const int16_t* src, intptr_t srcStride, int shift);
void x265_cpy2Dto1D_shr_8x8_neon(int16_t* dst, const int16_t* src, intptr_t srcStride, int shift);
void x265_cpy2Dto1D_shr_16x16_neon(int16_t* dst, const int16_t* src, intptr_t srcStride, int shift);
void x265_cpy2Dto1D_shr_32x32_neon(int16_t* dst, const int16_t* src, intptr_t srcStride, int shift);

void x265_blockcopy_sp_4x4_neon(pixel* a, intptr_t stridea, const int16_t* b, intptr_t strideb);
void x265_blockcopy_sp_8x8_neon(pixel* a, intptr_t stridea, const int16_t* b, intptr_t strideb);
void x265_blockcopy_sp_16x16_neon(pixel* a, intptr_t stridea, const int16_t* b, intptr_t strideb);
void x265_blockcopy_sp_32x32_neon(pixel* a, intptr_t stridea, const int16_t* b, intptr_t strideb);
void x265_blockcopy_sp_64x64_neon(pixel* a, intptr_t stridea, const int16_t* b, intptr_t strideb);

void x265_blockcopy_ps_4x4_neon(int16_t* a, intptr_t stridea, const pixel* b, intptr_t strideb);
void x265_blockcopy_ps_8x8_neon(int16_t* a, intptr_t stridea, const pixel* b, intptr_t strideb);
void x265_blockcopy_ps_16x16_neon(int16_t* a, intptr_t stridea, const pixel* b, intptr_t strideb);
void x265_blockcopy_ps_32x32_neon(int16_t* a, intptr_t stridea, const pixel* b, intptr_t strideb);
void x265_blockcopy_ps_64x64_neon(int16_t* a, intptr_t stridea, const pixel* b, intptr_t strideb);

void x265_blockcopy_ss_4x4_neon(int16_t* a, intptr_t stridea, const int16_t* b, intptr_t strideb);
void x265_blockcopy_ss_8x8_neon(int16_t* a, intptr_t stridea, const int16_t* b, intptr_t strideb);
void x265_blockcopy_ss_16x16_neon(int16_t* a, intptr_t stridea, const int16_t* b, intptr_t strideb);
void x265_blockcopy_ss_32x32_neon(int16_t* a, intptr_t stridea, const int16_t* b, intptr_t strideb);
void x265_blockcopy_ss_64x64_neon(int16_t* a, intptr_t stridea, const int16_t* b, intptr_t strideb);

// chroma blockcopy
void x265_blockcopy_ss_4x8_neon(int16_t* a, intptr_t stridea, const int16_t* b, intptr_t strideb);
void x265_blockcopy_ss_8x16_neon(int16_t* a, intptr_t stridea, const int16_t* b, intptr_t strideb);
void x265_blockcopy_ss_16x32_neon(int16_t* a, intptr_t stridea, const int16_t* b, intptr_t strideb);
void x265_blockcopy_ss_32x64_neon(int16_t* a, intptr_t stridea, const int16_t* b, intptr_t strideb);

void x265_blockcopy_sp_4x8_neon(pixel* a, intptr_t stridea, const int16_t* b, intptr_t strideb);
void x265_blockcopy_sp_8x16_neon(pixel* a, intptr_t stridea, const int16_t* b, intptr_t strideb);
void x265_blockcopy_sp_16x32_neon(pixel* a, intptr_t stridea, const int16_t* b, intptr_t strideb);
void x265_blockcopy_sp_32x64_neon(pixel* a, intptr_t stridea, const int16_t* b, intptr_t strideb);

void x265_blockcopy_ps_4x8_neon(int16_t* a, intptr_t stridea, const pixel* b, intptr_t strideb);
void x265_blockcopy_ps_8x16_neon(int16_t* a, intptr_t stridea, const pixel* b, intptr_t strideb);
void x265_blockcopy_ps_16x32_neon(int16_t* a, intptr_t stridea, const pixel* b, intptr_t strideb);
void x265_blockcopy_ps_32x64_neon(int16_t* a, intptr_t stridea, const pixel* b, intptr_t strideb);

void x265_blockfill_s_4x4_neon(int16_t* dst, intptr_t dstride, int16_t val);
void x265_blockfill_s_8x8_neon(int16_t* dst, intptr_t dstride, int16_t val);
void x265_blockfill_s_16x16_neon(int16_t* dst, intptr_t dstride, int16_t val);
void x265_blockfill_s_32x32_neon(int16_t* dst, intptr_t dstride, int16_t val);

uint32_t x265_copy_cnt_4_neon(int16_t* coeff, const int16_t* residual, intptr_t resiStride);
uint32_t x265_copy_cnt_8_neon(int16_t* coeff, const int16_t* residual, intptr_t resiStride);
uint32_t x265_copy_cnt_16_neon(int16_t* coeff, const int16_t* residual, intptr_t resiStride);
uint32_t x265_copy_cnt_32_neon(int16_t* coeff, const int16_t* residual, intptr_t resiStride);

int x265_count_nonzero_4_neon(const int16_t* quantCoeff);
int x265_count_nonzero_8_neon(const int16_t* quantCoeff);
int x265_count_nonzero_16_neon(const int16_t* quantCoeff);
int x265_count_nonzero_32_neon(const int16_t* quantCoeff);
#endif // ifndef X265_I386_PIXEL_ARM_H
