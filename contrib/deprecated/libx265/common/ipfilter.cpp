/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Deepthi Devaki <deepthidevaki@multicorewareinc.com>,
 *          Rajesh Paulraj <rajesh@multicorewareinc.com>
 *          Praveen Kumar Tiwari <praveen@multicorewareinc.com>
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

#include "common.h"
#include "primitives.h"
#include "x265.h"

using namespace X265_NS;

#if _MSC_VER
#pragma warning(disable: 4127) // conditional expression is constant, typical for templated functions
#endif

namespace {
// file local namespace

template<int width, int height>
void filterPixelToShort_c(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride)
{
    int shift = IF_INTERNAL_PREC - X265_DEPTH;
    int row, col;

    for (row = 0; row < height; row++)
    {
        for (col = 0; col < width; col++)
        {
            int16_t val = src[col] << shift;
            dst[col] = val - (int16_t)IF_INTERNAL_OFFS;
        }

        src += srcStride;
        dst += dstStride;
    }
}

static void extendCURowColBorder(pixel* txt, intptr_t stride, int width, int height, int marginX)
{
    for (int y = 0; y < height; y++)
    {
#if HIGH_BIT_DEPTH
        for (int x = 0; x < marginX; x++)
        {
            txt[-marginX + x] = txt[0];
            txt[width + x] = txt[width - 1];
        }

#else
        memset(txt - marginX, txt[0], marginX);
        memset(txt + width, txt[width - 1], marginX);
#endif

        txt += stride;
    }
}

template<int N, int width, int height>
void interp_horiz_pp_c(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx)
{
    const int16_t* coeff = (N == 4) ? g_chromaFilter[coeffIdx] : g_lumaFilter[coeffIdx];
    int headRoom = IF_FILTER_PREC;
    int offset =  (1 << (headRoom - 1));
    uint16_t maxVal = (1 << X265_DEPTH) - 1;
    int cStride = 1;

    src -= (N / 2 - 1) * cStride;

    int row, col;
    for (row = 0; row < height; row++)
    {
        for (col = 0; col < width; col++)
        {
            int sum;

            sum  = src[col + 0 * cStride] * coeff[0];
            sum += src[col + 1 * cStride] * coeff[1];
            sum += src[col + 2 * cStride] * coeff[2];
            sum += src[col + 3 * cStride] * coeff[3];
            if (N == 8)
            {
                sum += src[col + 4 * cStride] * coeff[4];
                sum += src[col + 5 * cStride] * coeff[5];
                sum += src[col + 6 * cStride] * coeff[6];
                sum += src[col + 7 * cStride] * coeff[7];
            }
            int16_t val = (int16_t)((sum + offset) >> headRoom);

            if (val < 0) val = 0;
            if (val > maxVal) val = maxVal;
            dst[col] = (pixel)val;
        }

        src += srcStride;
        dst += dstStride;
    }
}

template<int N, int width, int height>
void interp_horiz_ps_c(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt)
{
    const int16_t* coeff = (N == 4) ? g_chromaFilter[coeffIdx] : g_lumaFilter[coeffIdx];
    int headRoom = IF_INTERNAL_PREC - X265_DEPTH;
    int shift = IF_FILTER_PREC - headRoom;
    int offset = (unsigned)-IF_INTERNAL_OFFS << shift;
    int blkheight = height;
    src -= N / 2 - 1;

    if (isRowExt)
    {
        src -= (N / 2 - 1) * srcStride;
        blkheight += N - 1;
    }

    int row, col;
    for (row = 0; row < blkheight; row++)
    {
        for (col = 0; col < width; col++)
        {
            int sum;

            sum  = src[col + 0] * coeff[0];
            sum += src[col + 1] * coeff[1];
            sum += src[col + 2] * coeff[2];
            sum += src[col + 3] * coeff[3];
            if (N == 8)
            {
                sum += src[col + 4] * coeff[4];
                sum += src[col + 5] * coeff[5];
                sum += src[col + 6] * coeff[6];
                sum += src[col + 7] * coeff[7];
            }

            int16_t val = (int16_t)((sum + offset) >> shift);
            dst[col] = val;
        }

        src += srcStride;
        dst += dstStride;
    }
}

template<int N, int width, int height>
void interp_vert_pp_c(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx)
{
    const int16_t* c = (N == 4) ? g_chromaFilter[coeffIdx] : g_lumaFilter[coeffIdx];
    int shift = IF_FILTER_PREC;
    int offset = 1 << (shift - 1);
    uint16_t maxVal = (1 << X265_DEPTH) - 1;

    src -= (N / 2 - 1) * srcStride;

    int row, col;
    for (row = 0; row < height; row++)
    {
        for (col = 0; col < width; col++)
        {
            int sum;

            sum  = src[col + 0 * srcStride] * c[0];
            sum += src[col + 1 * srcStride] * c[1];
            sum += src[col + 2 * srcStride] * c[2];
            sum += src[col + 3 * srcStride] * c[3];
            if (N == 8)
            {
                sum += src[col + 4 * srcStride] * c[4];
                sum += src[col + 5 * srcStride] * c[5];
                sum += src[col + 6 * srcStride] * c[6];
                sum += src[col + 7 * srcStride] * c[7];
            }

            int16_t val = (int16_t)((sum + offset) >> shift);
            val = (val < 0) ? 0 : val;
            val = (val > maxVal) ? maxVal : val;

            dst[col] = (pixel)val;
        }

        src += srcStride;
        dst += dstStride;
    }
}

template<int N, int width, int height>
void interp_vert_ps_c(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx)
{
    const int16_t* c = (N == 4) ? g_chromaFilter[coeffIdx] : g_lumaFilter[coeffIdx];
    int headRoom = IF_INTERNAL_PREC - X265_DEPTH;
    int shift = IF_FILTER_PREC - headRoom;
    int offset = (unsigned)-IF_INTERNAL_OFFS << shift;
    src -= (N / 2 - 1) * srcStride;
    int row, col;
    for (row = 0; row < height; row++)
    {
        for (col = 0; col < width; col++)
        {
            int sum;

            sum  = src[col + 0 * srcStride] * c[0];
            sum += src[col + 1 * srcStride] * c[1];
            sum += src[col + 2 * srcStride] * c[2];
            sum += src[col + 3 * srcStride] * c[3];
            if (N == 8)
            {
                sum += src[col + 4 * srcStride] * c[4];
                sum += src[col + 5 * srcStride] * c[5];
                sum += src[col + 6 * srcStride] * c[6];
                sum += src[col + 7 * srcStride] * c[7];
            }

            int16_t val = (int16_t)((sum + offset) >> shift);
            dst[col] = val;
        }

        src += srcStride;
        dst += dstStride;
    }
}

template<int N, int width, int height>
void interp_vert_sp_c(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx)
{
    int headRoom = IF_INTERNAL_PREC - X265_DEPTH;
    int shift = IF_FILTER_PREC + headRoom;
    int offset = (1 << (shift - 1)) + (IF_INTERNAL_OFFS << IF_FILTER_PREC);
    uint16_t maxVal = (1 << X265_DEPTH) - 1;
    const int16_t* coeff = (N == 8 ? g_lumaFilter[coeffIdx] : g_chromaFilter[coeffIdx]);

    src -= (N / 2 - 1) * srcStride;

    int row, col;
    for (row = 0; row < height; row++)
    {
        for (col = 0; col < width; col++)
        {
            int sum;

            sum  = src[col + 0 * srcStride] * coeff[0];
            sum += src[col + 1 * srcStride] * coeff[1];
            sum += src[col + 2 * srcStride] * coeff[2];
            sum += src[col + 3 * srcStride] * coeff[3];
            if (N == 8)
            {
                sum += src[col + 4 * srcStride] * coeff[4];
                sum += src[col + 5 * srcStride] * coeff[5];
                sum += src[col + 6 * srcStride] * coeff[6];
                sum += src[col + 7 * srcStride] * coeff[7];
            }

            int16_t val = (int16_t)((sum + offset) >> shift);

            val = (val < 0) ? 0 : val;
            val = (val > maxVal) ? maxVal : val;

            dst[col] = (pixel)val;
        }

        src += srcStride;
        dst += dstStride;
    }
}

template<int N, int width, int height>
void interp_vert_ss_c(const int16_t* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx)
{
    const int16_t* c = (N == 8 ? g_lumaFilter[coeffIdx] : g_chromaFilter[coeffIdx]);
    int shift = IF_FILTER_PREC;
    int row, col;

    src -= (N / 2 - 1) * srcStride;
    for (row = 0; row < height; row++)
    {
        for (col = 0; col < width; col++)
        {
            int sum;

            sum  = src[col + 0 * srcStride] * c[0];
            sum += src[col + 1 * srcStride] * c[1];
            sum += src[col + 2 * srcStride] * c[2];
            sum += src[col + 3 * srcStride] * c[3];
            if (N == 8)
            {
                sum += src[col + 4 * srcStride] * c[4];
                sum += src[col + 5 * srcStride] * c[5];
                sum += src[col + 6 * srcStride] * c[6];
                sum += src[col + 7 * srcStride] * c[7];
            }

            int16_t val = (int16_t)((sum) >> shift);
            dst[col] = val;
        }

        src += srcStride;
        dst += dstStride;
    }
}

template<int N>
void filterVertical_sp_c(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int width, int height, int coeffIdx)
{
    int headRoom = IF_INTERNAL_PREC - X265_DEPTH;
    int shift = IF_FILTER_PREC + headRoom;
    int offset = (1 << (shift - 1)) + (IF_INTERNAL_OFFS << IF_FILTER_PREC);
    uint16_t maxVal = (1 << X265_DEPTH) - 1;
    const int16_t* coeff = (N == 8 ? g_lumaFilter[coeffIdx] : g_chromaFilter[coeffIdx]);

    src -= (N / 2 - 1) * srcStride;

    int row, col;
    for (row = 0; row < height; row++)
    {
        for (col = 0; col < width; col++)
        {
            int sum;

            sum  = src[col + 0 * srcStride] * coeff[0];
            sum += src[col + 1 * srcStride] * coeff[1];
            sum += src[col + 2 * srcStride] * coeff[2];
            sum += src[col + 3 * srcStride] * coeff[3];
            if (N == 8)
            {
                sum += src[col + 4 * srcStride] * coeff[4];
                sum += src[col + 5 * srcStride] * coeff[5];
                sum += src[col + 6 * srcStride] * coeff[6];
                sum += src[col + 7 * srcStride] * coeff[7];
            }

            int16_t val = (int16_t)((sum + offset) >> shift);

            val = (val < 0) ? 0 : val;
            val = (val > maxVal) ? maxVal : val;

            dst[col] = (pixel)val;
        }

        src += srcStride;
        dst += dstStride;
    }
}

template<int N, int width, int height>
void interp_hv_pp_c(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int idxX, int idxY)
{
    ALIGN_VAR_32(int16_t, immed[width * (height + N - 1)]);

    interp_horiz_ps_c<N, width, height>(src, srcStride, immed, width, idxX, 1);
    filterVertical_sp_c<N>(immed + (N / 2 - 1) * width, width, dst, dstStride, width, height, idxY);
}
}

namespace X265_NS {
// x265 private namespace

#define CHROMA_420(W, H) \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_ ## W ## x ## H].filter_hpp = interp_horiz_pp_c<4, W, H>; \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_ ## W ## x ## H].filter_hps = interp_horiz_ps_c<4, W, H>; \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_ ## W ## x ## H].filter_vpp = interp_vert_pp_c<4, W, H>;  \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_ ## W ## x ## H].filter_vps = interp_vert_ps_c<4, W, H>;  \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_ ## W ## x ## H].filter_vsp = interp_vert_sp_c<4, W, H>;  \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_ ## W ## x ## H].filter_vss = interp_vert_ss_c<4, W, H>; \
    p.chroma[X265_CSP_I420].pu[CHROMA_420_ ## W ## x ## H].p2s = filterPixelToShort_c<W, H>;

#define CHROMA_422(W, H) \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_ ## W ## x ## H].filter_hpp = interp_horiz_pp_c<4, W, H>; \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_ ## W ## x ## H].filter_hps = interp_horiz_ps_c<4, W, H>; \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_ ## W ## x ## H].filter_vpp = interp_vert_pp_c<4, W, H>;  \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_ ## W ## x ## H].filter_vps = interp_vert_ps_c<4, W, H>;  \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_ ## W ## x ## H].filter_vsp = interp_vert_sp_c<4, W, H>;  \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_ ## W ## x ## H].filter_vss = interp_vert_ss_c<4, W, H>; \
    p.chroma[X265_CSP_I422].pu[CHROMA_422_ ## W ## x ## H].p2s = filterPixelToShort_c<W, H>;

#define CHROMA_444(W, H) \
    p.chroma[X265_CSP_I444].pu[LUMA_ ## W ## x ## H].filter_hpp = interp_horiz_pp_c<4, W, H>; \
    p.chroma[X265_CSP_I444].pu[LUMA_ ## W ## x ## H].filter_hps = interp_horiz_ps_c<4, W, H>; \
    p.chroma[X265_CSP_I444].pu[LUMA_ ## W ## x ## H].filter_vpp = interp_vert_pp_c<4, W, H>;  \
    p.chroma[X265_CSP_I444].pu[LUMA_ ## W ## x ## H].filter_vps = interp_vert_ps_c<4, W, H>;  \
    p.chroma[X265_CSP_I444].pu[LUMA_ ## W ## x ## H].filter_vsp = interp_vert_sp_c<4, W, H>;  \
    p.chroma[X265_CSP_I444].pu[LUMA_ ## W ## x ## H].filter_vss = interp_vert_ss_c<4, W, H>; \
    p.chroma[X265_CSP_I444].pu[LUMA_ ## W ## x ## H].p2s = filterPixelToShort_c<W, H>;

#define LUMA(W, H) \
    p.pu[LUMA_ ## W ## x ## H].luma_hpp     = interp_horiz_pp_c<8, W, H>; \
    p.pu[LUMA_ ## W ## x ## H].luma_hps     = interp_horiz_ps_c<8, W, H>; \
    p.pu[LUMA_ ## W ## x ## H].luma_vpp     = interp_vert_pp_c<8, W, H>;  \
    p.pu[LUMA_ ## W ## x ## H].luma_vps     = interp_vert_ps_c<8, W, H>;  \
    p.pu[LUMA_ ## W ## x ## H].luma_vsp     = interp_vert_sp_c<8, W, H>;  \
    p.pu[LUMA_ ## W ## x ## H].luma_vss     = interp_vert_ss_c<8, W, H>;  \
    p.pu[LUMA_ ## W ## x ## H].luma_hvpp    = interp_hv_pp_c<8, W, H>; \
    p.pu[LUMA_ ## W ## x ## H].convert_p2s = filterPixelToShort_c<W, H>;

void setupFilterPrimitives_c(EncoderPrimitives& p)
{
    LUMA(4, 4);
    LUMA(8, 8);
    CHROMA_420(4,  4);
    LUMA(4, 8);
    CHROMA_420(2,  4);
    LUMA(8, 4);
    CHROMA_420(4,  2);
    LUMA(16, 16);
    CHROMA_420(8,  8);
    LUMA(16,  8);
    CHROMA_420(8,  4);
    LUMA(8, 16);
    CHROMA_420(4,  8);
    LUMA(16, 12);
    CHROMA_420(8,  6);
    LUMA(12, 16);
    CHROMA_420(6,  8);
    LUMA(16,  4);
    CHROMA_420(8,  2);
    LUMA(4, 16);
    CHROMA_420(2,  8);
    LUMA(32, 32);
    CHROMA_420(16, 16);
    LUMA(32, 16);
    CHROMA_420(16, 8);
    LUMA(16, 32);
    CHROMA_420(8,  16);
    LUMA(32, 24);
    CHROMA_420(16, 12);
    LUMA(24, 32);
    CHROMA_420(12, 16);
    LUMA(32,  8);
    CHROMA_420(16, 4);
    LUMA(8, 32);
    CHROMA_420(4,  16);
    LUMA(64, 64);
    CHROMA_420(32, 32);
    LUMA(64, 32);
    CHROMA_420(32, 16);
    LUMA(32, 64);
    CHROMA_420(16, 32);
    LUMA(64, 48);
    CHROMA_420(32, 24);
    LUMA(48, 64);
    CHROMA_420(24, 32);
    LUMA(64, 16);
    CHROMA_420(32, 8);
    LUMA(16, 64);
    CHROMA_420(8,  32);

    CHROMA_422(4, 8);
    CHROMA_422(4, 4);
    CHROMA_422(2, 4);
    CHROMA_422(2, 8);
    CHROMA_422(8,  16);
    CHROMA_422(8,  8);
    CHROMA_422(4,  16);
    CHROMA_422(8,  12);
    CHROMA_422(6,  16);
    CHROMA_422(8,  4);
    CHROMA_422(2,  16);
    CHROMA_422(16, 32);
    CHROMA_422(16, 16);
    CHROMA_422(8,  32);
    CHROMA_422(16, 24);
    CHROMA_422(12, 32);
    CHROMA_422(16, 8);
    CHROMA_422(4,  32);
    CHROMA_422(32, 64);
    CHROMA_422(32, 32);
    CHROMA_422(16, 64);
    CHROMA_422(32, 48);
    CHROMA_422(24, 64);
    CHROMA_422(32, 16);
    CHROMA_422(8,  64);

    CHROMA_444(4,  4);
    CHROMA_444(8,  8);
    CHROMA_444(4,  8);
    CHROMA_444(8,  4);
    CHROMA_444(16, 16);
    CHROMA_444(16, 8);
    CHROMA_444(8,  16);
    CHROMA_444(16, 12);
    CHROMA_444(12, 16);
    CHROMA_444(16, 4);
    CHROMA_444(4,  16);
    CHROMA_444(32, 32);
    CHROMA_444(32, 16);
    CHROMA_444(16, 32);
    CHROMA_444(32, 24);
    CHROMA_444(24, 32);
    CHROMA_444(32, 8);
    CHROMA_444(8,  32);
    CHROMA_444(64, 64);
    CHROMA_444(64, 32);
    CHROMA_444(32, 64);
    CHROMA_444(64, 48);
    CHROMA_444(48, 64);
    CHROMA_444(64, 16);
    CHROMA_444(16, 64);

    p.extendRowBorder = extendCURowColBorder;
}
}
