/*****************************************************************************
 * pixel.h: x86 pixel metrics
 *****************************************************************************
 * Copyright (C) 2003-2013 x264 project
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Laurent Aimar <fenrir@via.ecp.fr>
 *          Loren Merritt <lorenm@u.washington.edu>
 *          Fiona Glaser <fiona@x264.com>
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

#ifndef X265_I386_PIXEL_H
#define X265_I386_PIXEL_H

void PFX(downShift_16_sse2)(const uint16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int width, int height, int shift, uint16_t mask);
void PFX(downShift_16_avx2)(const uint16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int width, int height, int shift, uint16_t mask);
void PFX(upShift_16_sse2)(const uint16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int width, int height, int shift, uint16_t mask);
void PFX(upShift_16_avx2)(const uint16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int width, int height, int shift, uint16_t mask);
void PFX(upShift_8_sse4)(const uint8_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int width, int height, int shift);
void PFX(upShift_8_avx2)(const uint8_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int width, int height, int shift);
pixel PFX(planeClipAndMax_avx2)(pixel *src, intptr_t stride, int width, int height, uint64_t *outsum, const pixel minPix, const pixel maxPix);

#define DECL_PIXELS(cpu) \
    FUNCDEF_PU(sse_t, pixel_ssd, cpu, const pixel*, intptr_t, const pixel*, intptr_t); \
    FUNCDEF_PU(int, pixel_sa8d, cpu, const pixel*, intptr_t, const pixel*, intptr_t); \
    FUNCDEF_PU(void, pixel_sad_x3, cpu, const pixel*, const pixel*, const pixel*, const pixel*, intptr_t, int32_t*); \
    FUNCDEF_PU(void, pixel_sad_x4, cpu, const pixel*, const pixel*, const pixel*, const pixel*, const pixel*, intptr_t, int32_t*); \
    FUNCDEF_PU(void, pixel_avg, cpu, pixel* dst, intptr_t dstride, const pixel* src0, intptr_t sstride0, const pixel* src1, intptr_t sstride1, int); \
    FUNCDEF_PU(void, pixel_add_ps, cpu, pixel* a, intptr_t dstride, const pixel* b0, const int16_t* b1, intptr_t sstride0, intptr_t sstride1); \
    FUNCDEF_PU(void, pixel_sub_ps, cpu, int16_t* a, intptr_t dstride, const pixel* b0, const pixel* b1, intptr_t sstride0, intptr_t sstride1); \
    FUNCDEF_CHROMA_PU(int, pixel_satd, cpu, const pixel*, intptr_t, const pixel*, intptr_t); \
    FUNCDEF_CHROMA_PU(int, pixel_sad, cpu, const pixel*, intptr_t, const pixel*, intptr_t); \
    FUNCDEF_CHROMA_PU(sse_t, pixel_ssd_ss, cpu, const int16_t*, intptr_t, const int16_t*, intptr_t); \
    FUNCDEF_CHROMA_PU(void, addAvg, cpu, const int16_t*, const int16_t*, pixel*, intptr_t, intptr_t, intptr_t); \
    FUNCDEF_CHROMA_PU(sse_t, pixel_ssd_s, cpu, const int16_t*, intptr_t); \
    FUNCDEF_TU_S(sse_t, pixel_ssd_s, cpu, const int16_t*, intptr_t); \
    FUNCDEF_TU(uint64_t, pixel_var, cpu, const pixel*, intptr_t); \
    FUNCDEF_TU(int, psyCost_pp, cpu, const pixel* source, intptr_t sstride, const pixel* recon, intptr_t rstride); \
    FUNCDEF_TU(int, psyCost_ss, cpu, const int16_t* source, intptr_t sstride, const int16_t* recon, intptr_t rstride)

DECL_PIXELS(mmx);
DECL_PIXELS(mmx2);
DECL_PIXELS(sse2);
DECL_PIXELS(sse3);
DECL_PIXELS(sse4);
DECL_PIXELS(ssse3);
DECL_PIXELS(avx);
DECL_PIXELS(xop);
DECL_PIXELS(avx2);

#undef DECL_PIXELS

#endif // ifndef X265_I386_PIXEL_H
