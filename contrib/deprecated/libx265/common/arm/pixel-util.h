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

#ifndef X265_PIXEL_UTIL_ARM_H
#define X265_PIXEL_UTIL_ARM_H

uint64_t x265_pixel_var_8x8_neon(const pixel* pix, intptr_t stride);
uint64_t x265_pixel_var_16x16_neon(const pixel* pix, intptr_t stride);
uint64_t x265_pixel_var_32x32_neon(const pixel* pix, intptr_t stride);
uint64_t x265_pixel_var_64x64_neon(const pixel* pix, intptr_t stride);

void x265_getResidual4_neon(const pixel* fenc, const pixel* pred, int16_t* residual, intptr_t stride);
void x265_getResidual8_neon(const pixel* fenc, const pixel* pred, int16_t* residual, intptr_t stride);
void x265_getResidual16_neon(const pixel* fenc, const pixel* pred, int16_t* residual, intptr_t stride);
void x265_getResidual32_neon(const pixel* fenc, const pixel* pred, int16_t* residual, intptr_t stride);

void x265_scale1D_128to64_neon(pixel *dst, const pixel *src);
void x265_scale2D_64to32_neon(pixel* dst, const pixel* src, intptr_t stride);

int x265_pixel_satd_4x4_neon(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);
int x265_pixel_satd_4x8_neon(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);
int x265_pixel_satd_4x16_neon(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);
int x265_pixel_satd_4x32_neon(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);
int x265_pixel_satd_8x4_neon(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);
int x265_pixel_satd_8x8_neon(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);
int x265_pixel_satd_8x12_neon(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);
int x265_pixel_satd_8x16_neon(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);
int x265_pixel_satd_8x32_neon(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);
int x265_pixel_satd_8x64_neon(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);
int x265_pixel_satd_12x16_neon(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);
int x265_pixel_satd_12x32_neon(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);
int x265_pixel_satd_16x4_neon(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);
int x265_pixel_satd_16x8_neon(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);
int x265_pixel_satd_16x12_neon(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);
int x265_pixel_satd_16x16_neon(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);
int x265_pixel_satd_16x24_neon(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);
int x265_pixel_satd_16x32_neon(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);
int x265_pixel_satd_16x64_neon(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);
int x265_pixel_satd_24x32_neon(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);
int x265_pixel_satd_24x64_neon(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);
int x265_pixel_satd_32x8_neon(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);
int x265_pixel_satd_32x16_neon(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);
int x265_pixel_satd_32x24_neon(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);
int x265_pixel_satd_32x32_neon(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);
int x265_pixel_satd_32x48_neon(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);
int x265_pixel_satd_32x64_neon(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);
int x265_pixel_satd_48x64_neon(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);
int x265_pixel_satd_64x16_neon(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);
int x265_pixel_satd_64x32_neon(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);
int x265_pixel_satd_64x48_neon(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);
int x265_pixel_satd_64x64_neon(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);

int x265_pixel_sa8d_8x8_neon(const pixel* pix1, intptr_t i_pix1, const pixel* pix2, intptr_t i_pix2);
int x265_pixel_sa8d_8x16_neon(const pixel* pix1, intptr_t i_pix1, const pixel* pix2, intptr_t i_pix2);
int x265_pixel_sa8d_16x16_neon(const pixel* pix1, intptr_t i_pix1, const pixel* pix2, intptr_t i_pix2);
int x265_pixel_sa8d_16x32_neon(const pixel* pix1, intptr_t i_pix1, const pixel* pix2, intptr_t i_pix2);
int x265_pixel_sa8d_32x32_neon(const pixel* pix1, intptr_t i_pix1, const pixel* pix2, intptr_t i_pix2);
int x265_pixel_sa8d_32x64_neon(const pixel* pix1, intptr_t i_pix1, const pixel* pix2, intptr_t i_pix2);
int x265_pixel_sa8d_64x64_neon(const pixel* pix1, intptr_t i_pix1, const pixel* pix2, intptr_t i_pix2);

uint32_t x265_quant_neon(const int16_t* coef, const int32_t* quantCoeff, int32_t* deltaU, int16_t* qCoef, int qBits, int add, int numCoeff);
uint32_t x265_nquant_neon(const int16_t* coef, const int32_t* quantCoeff, int16_t* qCoef, int qBits, int add, int numCoeff);

void x265_dequant_scaling_neon(const int16_t* quantCoef, const int32_t* deQuantCoef, int16_t* coef, int num, int per, int shift);
void x265_dequant_normal_neon(const int16_t* quantCoef, int16_t* coef, int num, int scale, int shift);

void x265_ssim_4x4x2_core_neon(const pixel* pix1, intptr_t stride1, const pixel* pix2, intptr_t stride2, int sums[2][4]);

int PFX(psyCost_4x4_neon)(const pixel* source, intptr_t sstride, const pixel* recon, intptr_t rstride);
int PFX(psyCost_8x8_neon)(const pixel* source, intptr_t sstride, const pixel* recon, intptr_t rstride);

#endif // ifndef X265_PIXEL_UTIL_ARM_H
