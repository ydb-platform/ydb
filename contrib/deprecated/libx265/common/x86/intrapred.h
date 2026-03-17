/*****************************************************************************
 * intrapred.h: Intra Prediction metrics
 *****************************************************************************
 * Copyright (C) 2003-2013 x264 project
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Min Chen <chenm003@163.com> <min.chen@multicorewareinc.com>
 *          Praveen Kumar Tiwari <praveen@multicorewareinc.com>
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

#ifndef X265_INTRAPRED_H
#define X265_INTRAPRED_H

#define DECL_ANG(bsize, mode, cpu) \
    void PFX(intra_pred_ang ## bsize ## _ ## mode ## _ ## cpu)(pixel* dst, intptr_t dstStride, const pixel* srcPix, int dirMode, int bFilter);

#define DECL_ANGS(bsize, cpu) \
    DECL_ANG(bsize, 2, cpu); \
    DECL_ANG(bsize, 3, cpu); \
    DECL_ANG(bsize, 4, cpu); \
    DECL_ANG(bsize, 5, cpu); \
    DECL_ANG(bsize, 6, cpu); \
    DECL_ANG(bsize, 7, cpu); \
    DECL_ANG(bsize, 8, cpu); \
    DECL_ANG(bsize, 9, cpu); \
    DECL_ANG(bsize, 10, cpu); \
    DECL_ANG(bsize, 11, cpu); \
    DECL_ANG(bsize, 12, cpu); \
    DECL_ANG(bsize, 13, cpu); \
    DECL_ANG(bsize, 14, cpu); \
    DECL_ANG(bsize, 15, cpu); \
    DECL_ANG(bsize, 16, cpu); \
    DECL_ANG(bsize, 17, cpu); \
    DECL_ANG(bsize, 18, cpu); \
    DECL_ANG(bsize, 19, cpu); \
    DECL_ANG(bsize, 20, cpu); \
    DECL_ANG(bsize, 21, cpu); \
    DECL_ANG(bsize, 22, cpu); \
    DECL_ANG(bsize, 23, cpu); \
    DECL_ANG(bsize, 24, cpu); \
    DECL_ANG(bsize, 25, cpu); \
    DECL_ANG(bsize, 26, cpu); \
    DECL_ANG(bsize, 27, cpu); \
    DECL_ANG(bsize, 28, cpu); \
    DECL_ANG(bsize, 29, cpu); \
    DECL_ANG(bsize, 30, cpu); \
    DECL_ANG(bsize, 31, cpu); \
    DECL_ANG(bsize, 32, cpu); \
    DECL_ANG(bsize, 33, cpu); \
    DECL_ANG(bsize, 34, cpu)

#define DECL_ALL(cpu) \
    FUNCDEF_TU(void, all_angs_pred, cpu, pixel *dest, pixel *refPix, pixel *filtPix, int bLuma); \
    FUNCDEF_TU(void, intra_filter, cpu, const pixel *samples, pixel *filtered); \
    DECL_ANGS(4, cpu); \
    DECL_ANGS(8, cpu); \
    DECL_ANGS(16, cpu); \
    DECL_ANGS(32, cpu)

FUNCDEF_TU_S2(void, intra_pred_dc, sse2, pixel* dst, intptr_t dstStride, const pixel*srcPix, int, int filter);
FUNCDEF_TU_S2(void, intra_pred_dc, sse4, pixel* dst, intptr_t dstStride, const pixel*srcPix, int, int filter);
FUNCDEF_TU_S2(void, intra_pred_dc, avx2, pixel* dst, intptr_t dstStride, const pixel*srcPix, int, int filter);

FUNCDEF_TU_S2(void, intra_pred_planar, sse2, pixel* dst, intptr_t dstStride, const pixel*srcPix, int, int filter);
FUNCDEF_TU_S2(void, intra_pred_planar, sse4, pixel* dst, intptr_t dstStride, const pixel*srcPix, int, int filter);
FUNCDEF_TU_S2(void, intra_pred_planar, avx2, pixel* dst, intptr_t dstStride, const pixel*srcPix, int, int filter);

DECL_ALL(sse2);
DECL_ALL(ssse3);
DECL_ALL(sse4);
DECL_ALL(avx2);

#undef DECL_ALL
#undef DECL_ANGS
#undef DECL_ANG


#endif // ifndef X265_INTRAPRED_H
