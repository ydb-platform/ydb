/*****************************************************************************
 * pixel.h: x86 pixel metrics
 *****************************************************************************
 * Copyright (C) 2003-2017 x265 project
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Laurent Aimar <fenrir@via.ecp.fr>
 *          Loren Merritt <lorenm@u.washington.edu>
 *          Fiona Glaser <fiona@x264.com>
 *          Min Chen <chenm003@163.com>
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

#ifndef X265_I386_PIXEL_ARM_H
#define X265_I386_PIXEL_ARM_H

int x265_pixel_sad_4x4_armv6(const pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
int x265_pixel_sad_4x8_armv6(const pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
int x265_pixel_sad_4x16_armv6(const pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
int x265_pixel_sad_8x4_neon(const pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
int x265_pixel_sad_8x8_neon(const pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
int x265_pixel_sad_8x16_neon(const pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
int x265_pixel_sad_8x32_neon(const pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
int x265_pixel_sad_16x4_neon(const pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
int x265_pixel_sad_16x8_neon(const pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
int x265_pixel_sad_16x16_neon(const pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
int x265_pixel_sad_16x12_neon(const pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
int x265_pixel_sad_16x32_neon(const pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
int x265_pixel_sad_16x64_neon(const pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
int x265_pixel_sad_32x8_neon(const pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
int x265_pixel_sad_32x16_neon(const pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
int x265_pixel_sad_32x32_neon(const pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
int x265_pixel_sad_32x64_neon(const pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
int x265_pixel_sad_32x24_neon(const pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
int x265_pixel_sad_64x16_neon(const pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
int x265_pixel_sad_64x32_neon(const pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
int x265_pixel_sad_64x64_neon(const pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
int x265_pixel_sad_64x48_neon(const pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
int x265_pixel_sad_12x16_neon(const pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
int x265_pixel_sad_24x32_neon(const pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
int x265_pixel_sad_48x64_neon(const pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);

void x265_pixel_avg_pp_4x4_neon (pixel* dst, intptr_t dstride, const pixel* src0, intptr_t sstride0, const pixel* src1, intptr_t sstride1, int);
void x265_pixel_avg_pp_4x8_neon (pixel* dst, intptr_t dstride, const pixel* src0, intptr_t sstride0, const pixel* src1, intptr_t sstride1, int);
void x265_pixel_avg_pp_4x16_neon (pixel* dst, intptr_t dstride, const pixel* src0, intptr_t sstride0, const pixel* src1, intptr_t sstride1, int);
void x265_pixel_avg_pp_8x4_neon (pixel* dst, intptr_t dstride, const pixel* src0, intptr_t sstride0, const pixel* src1, intptr_t sstride1, int);
void x265_pixel_avg_pp_8x8_neon (pixel* dst, intptr_t dstride, const pixel* src0, intptr_t sstride0, const pixel* src1, intptr_t sstride1, int);
void x265_pixel_avg_pp_8x16_neon (pixel* dst, intptr_t dstride, const pixel* src0, intptr_t sstride0, const pixel* src1, intptr_t sstride1, int);
void x265_pixel_avg_pp_8x32_neon (pixel* dst, intptr_t dstride, const pixel* src0, intptr_t sstride0, const pixel* src1, intptr_t sstride1, int);
void x265_pixel_avg_pp_12x16_neon (pixel* dst, intptr_t dstride, const pixel* src0, intptr_t sstride0, const pixel* src1, intptr_t sstride1, int);
void x265_pixel_avg_pp_16x4_neon (pixel* dst, intptr_t dstride, const pixel* src0, intptr_t sstride0, const pixel* src1, intptr_t sstride1, int);
void x265_pixel_avg_pp_16x8_neon (pixel* dst, intptr_t dstride, const pixel* src0, intptr_t sstride0, const pixel* src1, intptr_t sstride1, int);
void x265_pixel_avg_pp_16x12_neon (pixel* dst, intptr_t dstride, const pixel* src0, intptr_t sstride0, const pixel* src1, intptr_t sstride1, int);
void x265_pixel_avg_pp_16x16_neon (pixel* dst, intptr_t dstride, const pixel* src0, intptr_t sstride0, const pixel* src1, intptr_t sstride1, int);
void x265_pixel_avg_pp_16x32_neon (pixel* dst, intptr_t dstride, const pixel* src0, intptr_t sstride0, const pixel* src1, intptr_t sstride1, int);
void x265_pixel_avg_pp_16x64_neon (pixel* dst, intptr_t dstride, const pixel* src0, intptr_t sstride0, const pixel* src1, intptr_t sstride1, int);
void x265_pixel_avg_pp_24x32_neon (pixel* dst, intptr_t dstride, const pixel* src0, intptr_t sstride0, const pixel* src1, intptr_t sstride1, int);
void x265_pixel_avg_pp_32x8_neon (pixel* dst, intptr_t dstride, const pixel* src0, intptr_t sstride0, const pixel* src1, intptr_t sstride1, int);
void x265_pixel_avg_pp_32x16_neon (pixel* dst, intptr_t dstride, const pixel* src0, intptr_t sstride0, const pixel* src1, intptr_t sstride1, int);
void x265_pixel_avg_pp_32x24_neon (pixel* dst, intptr_t dstride, const pixel* src0, intptr_t sstride0, const pixel* src1, intptr_t sstride1, int);
void x265_pixel_avg_pp_32x32_neon (pixel* dst, intptr_t dstride, const pixel* src0, intptr_t sstride0, const pixel* src1, intptr_t sstride1, int);
void x265_pixel_avg_pp_32x64_neon (pixel* dst, intptr_t dstride, const pixel* src0, intptr_t sstride0, const pixel* src1, intptr_t sstride1, int);
void x265_pixel_avg_pp_48x64_neon (pixel* dst, intptr_t dstride, const pixel* src0, intptr_t sstride0, const pixel* src1, intptr_t sstride1, int);
void x265_pixel_avg_pp_64x16_neon (pixel* dst, intptr_t dstride, const pixel* src0, intptr_t sstride0, const pixel* src1, intptr_t sstride1, int);
void x265_pixel_avg_pp_64x32_neon (pixel* dst, intptr_t dstride, const pixel* src0, intptr_t sstride0, const pixel* src1, intptr_t sstride1, int);
void x265_pixel_avg_pp_64x48_neon (pixel* dst, intptr_t dstride, const pixel* src0, intptr_t sstride0, const pixel* src1, intptr_t sstride1, int);
void x265_pixel_avg_pp_64x64_neon (pixel* dst, intptr_t dstride, const pixel* src0, intptr_t sstride0, const pixel* src1, intptr_t sstride1, int);

void x265_sad_x3_4x4_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, intptr_t frefstride, int32_t* res);
void x265_sad_x3_4x8_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, intptr_t frefstride, int32_t* res);
void x265_sad_x3_4x16_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, intptr_t frefstride, int32_t* res);
void x265_sad_x3_8x4_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, intptr_t frefstride, int32_t* res);
void x265_sad_x3_8x8_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, intptr_t frefstride, int32_t* res);
void x265_sad_x3_8x16_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, intptr_t frefstride, int32_t* res);
void x265_sad_x3_8x32_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, intptr_t frefstride, int32_t* res);
void x265_sad_x3_12x16_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, intptr_t frefstride, int32_t* res);
void x265_sad_x3_16x4_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, intptr_t frefstride, int32_t* res);
void x265_sad_x3_16x8_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, intptr_t frefstride, int32_t* res);
void x265_sad_x3_16x12_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, intptr_t frefstride, int32_t* res);
void x265_sad_x3_16x16_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, intptr_t frefstride, int32_t* res);
void x265_sad_x3_16x32_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, intptr_t frefstride, int32_t* res);
void x265_sad_x3_16x64_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, intptr_t frefstride, int32_t* res);
void x265_sad_x3_24x32_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, intptr_t frefstride, int32_t* res);
void x265_sad_x3_32x8_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, intptr_t frefstride, int32_t* res);
void x265_sad_x3_32x16_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, intptr_t frefstride, int32_t* res);
void x265_sad_x3_32x24_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, intptr_t frefstride, int32_t* res);
void x265_sad_x3_32x32_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, intptr_t frefstride, int32_t* res);
void x265_sad_x3_32x64_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, intptr_t frefstride, int32_t* res);
void x265_sad_x3_48x64_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, intptr_t frefstride, int32_t* res);
void x265_sad_x3_64x16_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, intptr_t frefstride, int32_t* res);
void x265_sad_x3_64x32_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, intptr_t frefstride, int32_t* res);
void x265_sad_x3_64x48_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, intptr_t frefstride, int32_t* res);
void x265_sad_x3_64x64_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, intptr_t frefstride, int32_t* res);

void x265_sad_x4_4x4_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, const pixel* fref3, intptr_t frefstride, int32_t* res);
void x265_sad_x4_4x8_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, const pixel* fref3, intptr_t frefstride, int32_t* res);
void x265_sad_x4_4x16_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, const pixel* fref3, intptr_t frefstride, int32_t* res);
void x265_sad_x4_8x4_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, const pixel* fref3, intptr_t frefstride, int32_t* res);
void x265_sad_x4_8x8_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, const pixel* fref3, intptr_t frefstride, int32_t* res);
void x265_sad_x4_8x16_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, const pixel* fref3, intptr_t frefstride, int32_t* res);
void x265_sad_x4_8x32_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, const pixel* fref3, intptr_t frefstride, int32_t* res);
void x265_sad_x4_12x16_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, const pixel* fref3, intptr_t frefstride, int32_t* res);
void x265_sad_x4_16x4_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, const pixel* fref3, intptr_t frefstride, int32_t* res);
void x265_sad_x4_16x8_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, const pixel* fref3, intptr_t frefstride, int32_t* res);
void x265_sad_x4_16x12_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, const pixel* fref3, intptr_t frefstride, int32_t* res);
void x265_sad_x4_16x16_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, const pixel* fref3, intptr_t frefstride, int32_t* res);
void x265_sad_x4_16x32_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, const pixel* fref3, intptr_t frefstride, int32_t* res);
void x265_sad_x4_16x64_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, const pixel* fref3, intptr_t frefstride, int32_t* res);
void x265_sad_x4_24x32_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, const pixel* fref3, intptr_t frefstride, int32_t* res);
void x265_sad_x4_32x8_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, const pixel* fref3, intptr_t frefstride, int32_t* res);
void x265_sad_x4_32x16_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, const pixel* fref3, intptr_t frefstride, int32_t* res);
void x265_sad_x4_32x24_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, const pixel* fref3, intptr_t frefstride, int32_t* res);
void x265_sad_x4_32x32_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, const pixel* fref3, intptr_t frefstride, int32_t* res);
void x265_sad_x4_32x64_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, const pixel* fref3, intptr_t frefstride, int32_t* res);
void x265_sad_x4_48x64_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, const pixel* fref3, intptr_t frefstride, int32_t* res);
void x265_sad_x4_64x16_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, const pixel* fref3, intptr_t frefstride, int32_t* res);
void x265_sad_x4_64x32_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, const pixel* fref3, intptr_t frefstride, int32_t* res);
void x265_sad_x4_64x48_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, const pixel* fref3, intptr_t frefstride, int32_t* res);
void x265_sad_x4_64x64_neon(const pixel* fenc, const pixel* fref0, const pixel* fref1, const pixel* fref2, const pixel* fref3, intptr_t frefstride, int32_t* res);

sse_t x265_pixel_sse_pp_4x4_neon(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);
sse_t x265_pixel_sse_pp_8x8_neon(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);
sse_t x265_pixel_sse_pp_16x16_neon(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);
sse_t x265_pixel_sse_pp_32x32_neon(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);
sse_t x265_pixel_sse_pp_64x64_neon(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);

sse_t x265_pixel_sse_ss_4x4_neon(const int16_t* pix1, intptr_t stride_pix1, const int16_t* pix2, intptr_t stride_pix2);
sse_t x265_pixel_sse_ss_8x8_neon(const int16_t* pix1, intptr_t stride_pix1, const int16_t* pix2, intptr_t stride_pix2);
sse_t x265_pixel_sse_ss_16x16_neon(const int16_t* pix1, intptr_t stride_pix1, const int16_t* pix2, intptr_t stride_pix2);
sse_t x265_pixel_sse_ss_32x32_neon(const int16_t* pix1, intptr_t stride_pix1, const int16_t* pix2, intptr_t stride_pix2);
sse_t x265_pixel_sse_ss_64x64_neon(const int16_t* pix1, intptr_t stride_pix1, const int16_t* pix2, intptr_t stride_pix2);

sse_t x265_pixel_ssd_s_4x4_neon(const int16_t* a, intptr_t dstride);
sse_t x265_pixel_ssd_s_8x8_neon(const int16_t* a, intptr_t dstride);
sse_t x265_pixel_ssd_s_16x16_neon(const int16_t* a, intptr_t dstride);
sse_t x265_pixel_ssd_s_32x32_neon(const int16_t* a, intptr_t dstride);
sse_t x265_pixel_ssd_s_64x64_neon(const int16_t* a, intptr_t dstride);

void x265_pixel_sub_ps_4x4_neon(int16_t* a, intptr_t dstride, const pixel* b0, const pixel* b1, intptr_t sstride0, intptr_t sstride1);
void x265_pixel_sub_ps_8x8_neon(int16_t* a, intptr_t dstride, const pixel* b0, const pixel* b1, intptr_t sstride0, intptr_t sstride1);
void x265_pixel_sub_ps_16x16_neon(int16_t* a, intptr_t dstride, const pixel* b0, const pixel* b1, intptr_t sstride0, intptr_t sstride1);
void x265_pixel_sub_ps_32x32_neon(int16_t* a, intptr_t dstride, const pixel* b0, const pixel* b1, intptr_t sstride0, intptr_t sstride1);
void x265_pixel_sub_ps_64x64_neon(int16_t* a, intptr_t dstride, const pixel* b0, const pixel* b1, intptr_t sstride0, intptr_t sstride1);
void x265_pixel_sub_ps_4x8_neon(int16_t* a, intptr_t dstride, const pixel* b0, const pixel* b1, intptr_t sstride0, intptr_t sstride1);
void x265_pixel_sub_ps_8x16_neon(int16_t* a, intptr_t dstride, const pixel* b0, const pixel* b1, intptr_t sstride0, intptr_t sstride1);
void x265_pixel_sub_ps_16x32_neon(int16_t* a, intptr_t dstride, const pixel* b0, const pixel* b1, intptr_t sstride0, intptr_t sstride1);
void x265_pixel_sub_ps_32x64_neon(int16_t* a, intptr_t dstride, const pixel* b0, const pixel* b1, intptr_t sstride0, intptr_t sstride1);

void x265_pixel_add_ps_4x4_neon(pixel* a, intptr_t dstride, const pixel* b0, const int16_t* b1, intptr_t sstride0, intptr_t sstride1);
void x265_pixel_add_ps_8x8_neon(pixel* a, intptr_t dstride, const pixel* b0, const int16_t* b1, intptr_t sstride0, intptr_t sstride1);
void x265_pixel_add_ps_16x16_neon(pixel* a, intptr_t dstride, const pixel* b0, const int16_t* b1, intptr_t sstride0, intptr_t sstride1);
void x265_pixel_add_ps_32x32_neon(pixel* a, intptr_t dstride, const pixel* b0, const int16_t* b1, intptr_t sstride0, intptr_t sstride1);
void x265_pixel_add_ps_64x64_neon(pixel* a, intptr_t dstride, const pixel* b0, const int16_t* b1, intptr_t sstride0, intptr_t sstride1);
void x265_pixel_add_ps_4x8_neon(pixel* a, intptr_t dstride, const pixel* b0, const int16_t* b1, intptr_t sstride0, intptr_t sstride1);
void x265_pixel_add_ps_8x16_neon(pixel* a, intptr_t dstride, const pixel* b0, const int16_t* b1, intptr_t sstride0, intptr_t sstride1);
void x265_pixel_add_ps_16x32_neon(pixel* a, intptr_t dstride, const pixel* b0, const int16_t* b1, intptr_t sstride0, intptr_t sstride1);
void x265_pixel_add_ps_32x64_neon(pixel* a, intptr_t dstride, const pixel* b0, const int16_t* b1, intptr_t sstride0, intptr_t sstride1);

void x265_pixel_planecopy_cp_neon(const uint8_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int width, int height, int shift);

void x265_addAvg_4x4_neon(const int16_t* src0, const int16_t* src1, pixel* dst, intptr_t src0Stride, intptr_t src1Stride, intptr_t dstStride);
void x265_addAvg_4x8_neon(const int16_t* src0, const int16_t* src1, pixel* dst, intptr_t src0Stride, intptr_t src1Stride, intptr_t dstStride);
void x265_addAvg_4x16_neon(const int16_t* src0, const int16_t* src1, pixel* dst, intptr_t src0Stride, intptr_t src1Stride, intptr_t dstStride);
void x265_addAvg_8x4_neon(const int16_t* src0, const int16_t* src1, pixel* dst, intptr_t src0Stride, intptr_t src1Stride, intptr_t dstStride);
void x265_addAvg_8x8_neon(const int16_t* src0, const int16_t* src1, pixel* dst, intptr_t src0Stride, intptr_t src1Stride, intptr_t dstStride);
void x265_addAvg_8x16_neon(const int16_t* src0, const int16_t* src1, pixel* dst, intptr_t src0Stride, intptr_t src1Stride, intptr_t dstStride);
void x265_addAvg_8x32_neon(const int16_t* src0, const int16_t* src1, pixel* dst, intptr_t src0Stride, intptr_t src1Stride, intptr_t dstStride);
void x265_addAvg_12x16_neon(const int16_t* src0, const int16_t* src1, pixel* dst, intptr_t src0Stride, intptr_t src1Stride, intptr_t dstStride);
void x265_addAvg_16x4_neon(const int16_t* src0, const int16_t* src1, pixel* dst, intptr_t src0Stride, intptr_t src1Stride, intptr_t dstStride);
void x265_addAvg_16x8_neon(const int16_t* src0, const int16_t* src1, pixel* dst, intptr_t src0Stride, intptr_t src1Stride, intptr_t dstStride);
void x265_addAvg_16x12_neon(const int16_t* src0, const int16_t* src1, pixel* dst, intptr_t src0Stride, intptr_t src1Stride, intptr_t dstStride);
void x265_addAvg_16x16_neon(const int16_t* src0, const int16_t* src1, pixel* dst, intptr_t src0Stride, intptr_t src1Stride, intptr_t dstStride);
void x265_addAvg_16x32_neon(const int16_t* src0, const int16_t* src1, pixel* dst, intptr_t src0Stride, intptr_t src1Stride, intptr_t dstStride);
void x265_addAvg_16x64_neon(const int16_t* src0, const int16_t* src1, pixel* dst, intptr_t src0Stride, intptr_t src1Stride, intptr_t dstStride);
void x265_addAvg_24x32_neon(const int16_t* src0, const int16_t* src1, pixel* dst, intptr_t src0Stride, intptr_t src1Stride, intptr_t dstStride);
void x265_addAvg_32x8_neon(const int16_t* src0, const int16_t* src1, pixel* dst, intptr_t src0Stride, intptr_t src1Stride, intptr_t dstStride);
void x265_addAvg_32x16_neon(const int16_t* src0, const int16_t* src1, pixel* dst, intptr_t src0Stride, intptr_t src1Stride, intptr_t dstStride);
void x265_addAvg_32x24_neon(const int16_t* src0, const int16_t* src1, pixel* dst, intptr_t src0Stride, intptr_t src1Stride, intptr_t dstStride);
void x265_addAvg_32x32_neon(const int16_t* src0, const int16_t* src1, pixel* dst, intptr_t src0Stride, intptr_t src1Stride, intptr_t dstStride);
void x265_addAvg_32x64_neon(const int16_t* src0, const int16_t* src1, pixel* dst, intptr_t src0Stride, intptr_t src1Stride, intptr_t dstStride);
void x265_addAvg_48x64_neon(const int16_t* src0, const int16_t* src1, pixel* dst, intptr_t src0Stride, intptr_t src1Stride, intptr_t dstStride);
void x265_addAvg_64x16_neon(const int16_t* src0, const int16_t* src1, pixel* dst, intptr_t src0Stride, intptr_t src1Stride, intptr_t dstStride);
void x265_addAvg_64x32_neon(const int16_t* src0, const int16_t* src1, pixel* dst, intptr_t src0Stride, intptr_t src1Stride, intptr_t dstStride);
void x265_addAvg_64x48_neon(const int16_t* src0, const int16_t* src1, pixel* dst, intptr_t src0Stride, intptr_t src1Stride, intptr_t dstStride);
void x265_addAvg_64x64_neon(const int16_t* src0, const int16_t* src1, pixel* dst, intptr_t src0Stride, intptr_t src1Stride, intptr_t dstStride);

void x265_addAvg_4x2_neon(const int16_t* src0, const int16_t* src1, pixel* dst, intptr_t src0Stride, intptr_t src1Stride, intptr_t dstStride);
void x265_addAvg_4x32_neon(const int16_t* src0, const int16_t* src1, pixel* dst, intptr_t src0Stride, intptr_t src1Stride, intptr_t dstStride);
void x265_addAvg_6x8_neon(const int16_t* src0, const int16_t* src1, pixel* dst, intptr_t src0Stride, intptr_t src1Stride, intptr_t dstStride);
void x265_addAvg_6x16_neon(const int16_t* src0, const int16_t* src1, pixel* dst, intptr_t src0Stride, intptr_t src1Stride, intptr_t dstStride);
void x265_addAvg_8x2_neon(const int16_t* src0, const int16_t* src1, pixel* dst, intptr_t src0Stride, intptr_t src1Stride, intptr_t dstStride);
void x265_addAvg_8x6_neon(const int16_t* src0, const int16_t* src1, pixel* dst, intptr_t src0Stride, intptr_t src1Stride, intptr_t dstStride);
void x265_addAvg_8x12_neon(const int16_t* src0, const int16_t* src1, pixel* dst, intptr_t src0Stride, intptr_t src1Stride, intptr_t dstStride);
void x265_addAvg_8x64_neon(const int16_t* src0, const int16_t* src1, pixel* dst, intptr_t src0Stride, intptr_t src1Stride, intptr_t dstStride);
void x265_addAvg_12x32_neon(const int16_t* src0, const int16_t* src1, pixel* dst, intptr_t src0Stride, intptr_t src1Stride, intptr_t dstStride);
void x265_addAvg_16x24_neon(const int16_t* src0, const int16_t* src1, pixel* dst, intptr_t src0Stride, intptr_t src1Stride, intptr_t dstStride);
void x265_addAvg_24x64_neon(const int16_t* src0, const int16_t* src1, pixel* dst, intptr_t src0Stride, intptr_t src1Stride, intptr_t dstStride);
void x265_addAvg_32x48_neon(const int16_t* src0, const int16_t* src1, pixel* dst, intptr_t src0Stride, intptr_t src1Stride, intptr_t dstStride);
#endif // ifndef X265_I386_PIXEL_ARM_H
