/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Min Chen <chenm003@163.com>
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

using namespace X265_NS;

namespace {

template<int tuSize>
void intraFilter(const pixel* samples, pixel* filtered) /* 1:2:1 filtering of left and top reference samples */
{
    const int tuSize2 = tuSize << 1;

    pixel topLeft = samples[0], topLast = samples[tuSize2], leftLast = samples[tuSize2 + tuSize2];

    // filtering top
    for (int i = 1; i < tuSize2; i++)
        filtered[i] = ((samples[i] << 1) + samples[i - 1] + samples[i + 1] + 2) >> 2;
    filtered[tuSize2] = topLast;
    
    // filtering top-left
    filtered[0] = ((topLeft << 1) + samples[1] + samples[tuSize2 + 1] + 2) >> 2;

    // filtering left
    filtered[tuSize2 + 1] = ((samples[tuSize2 + 1] << 1) + topLeft + samples[tuSize2 + 2] + 2) >> 2;
    for (int i = tuSize2 + 2; i < tuSize2 + tuSize2; i++)
        filtered[i] = ((samples[i] << 1) + samples[i - 1] + samples[i + 1] + 2) >> 2;
    filtered[tuSize2 + tuSize2] = leftLast;
}

static void dcPredFilter(const pixel* above, const pixel* left, pixel* dst, intptr_t dststride, int size)
{
    // boundary pixels processing
    dst[0] = (pixel)((above[0] + left[0] + 2 * dst[0] + 2) >> 2);

    for (int x = 1; x < size; x++)
        dst[x] = (pixel)((above[x] +  3 * dst[x] + 2) >> 2);

    dst += dststride;
    for (int y = 1; y < size; y++)
    {
        *dst = (pixel)((left[y] + 3 * *dst + 2) >> 2);
        dst += dststride;
    }
}

template<int width>
void intra_pred_dc_c(pixel* dst, intptr_t dstStride, const pixel* srcPix, int /*dirMode*/, int bFilter)
{
    int k, l;

    int dcVal = width;
    for (int i = 0; i < width; i++)
        dcVal += srcPix[1 + i] + srcPix[2 * width + 1 + i];

    dcVal = dcVal / (width + width);
    for (k = 0; k < width; k++)
        for (l = 0; l < width; l++)
            dst[k * dstStride + l] = (pixel)dcVal;

    if (bFilter)
        dcPredFilter(srcPix + 1, srcPix + (2 * width + 1), dst, dstStride, width);
}

template<int log2Size>
void planar_pred_c(pixel* dst, intptr_t dstStride, const pixel* srcPix, int /*dirMode*/, int /*bFilter*/)
{
    const int blkSize = 1 << log2Size;

    const pixel* above = srcPix + 1;
    const pixel* left  = srcPix + (2 * blkSize + 1);

    pixel topRight = above[blkSize];
    pixel bottomLeft = left[blkSize];
    for (int y = 0; y < blkSize; y++)
        for (int x = 0; x < blkSize; x++)
            dst[y * dstStride + x] = (pixel) (((blkSize - 1 - x) * left[y] + (blkSize - 1 -y) * above[x] + (x + 1) * topRight + (y + 1) * bottomLeft + blkSize) >> (log2Size + 1));
}

template<int width>
void intra_pred_ang_c(pixel* dst, intptr_t dstStride, const pixel *srcPix0, int dirMode, int bFilter)
{
    int width2 = width << 1;
    // Flip the neighbours in the horizontal case.
    int horMode = dirMode < 18;
    pixel neighbourBuf[129];
    const pixel *srcPix = srcPix0;

    if (horMode)
    {
        neighbourBuf[0] = srcPix[0];
        for (int i = 0; i < width << 1; i++)
        {
            neighbourBuf[1 + i] = srcPix[width2 + 1 + i];
            neighbourBuf[width2 + 1 + i] = srcPix[1 + i];
        }
        srcPix = neighbourBuf;
    }

    // Intra prediction angle and inverse angle tables.
    const int8_t angleTable[17] = { -32, -26, -21, -17, -13, -9, -5, -2, 0, 2, 5, 9, 13, 17, 21, 26, 32 };
    const int16_t invAngleTable[8] = { 4096, 1638, 910, 630, 482, 390, 315, 256 };

    // Get the prediction angle.
    int angleOffset = horMode ? 10 - dirMode : dirMode - 26;
    int angle = angleTable[8 + angleOffset];

    // Vertical Prediction.
    if (!angle)
    {
        for (int y = 0; y < width; y++)
            for (int x = 0; x < width; x++)
                dst[y * dstStride + x] = srcPix[1 + x];

        if (bFilter)
        {
            int topLeft = srcPix[0], top = srcPix[1];
            for (int y = 0; y < width; y++)
                dst[y * dstStride] = x265_clip((int16_t)(top + ((srcPix[width2 + 1 + y] - topLeft) >> 1)));
        }
    }
    else // Angular prediction.
    {
        // Get the reference pixels. The reference base is the first pixel to the top (neighbourBuf[1]).
        pixel refBuf[64];
        const pixel *ref;

        // Use the projected left neighbours and the top neighbours.
        if (angle < 0)
        {
            // Number of neighbours projected. 
            int nbProjected = -((width * angle) >> 5) - 1;
            pixel *ref_pix = refBuf + nbProjected + 1;

            // Project the neighbours.
            int invAngle = invAngleTable[- angleOffset - 1];
            int invAngleSum = 128;
            for (int i = 0; i < nbProjected; i++)
            {
                invAngleSum += invAngle;
                ref_pix[- 2 - i] = srcPix[width2 + (invAngleSum >> 8)];
            }

            // Copy the top-left and top pixels.
            for (int i = 0; i < width + 1; i++)
                ref_pix[-1 + i] = srcPix[i];
            ref = ref_pix;
        }
        else // Use the top and top-right neighbours.
            ref = srcPix + 1;

        // Pass every row.
        int angleSum = 0;
        for (int y = 0; y < width; y++)
        {
            angleSum += angle;
            int offset = angleSum >> 5;
            int fraction = angleSum & 31;

            if (fraction) // Interpolate
                for (int x = 0; x < width; x++)
                    dst[y * dstStride + x] = (pixel)(((32 - fraction) * ref[offset + x] + fraction * ref[offset + x + 1] + 16) >> 5);
            else // Copy.
                for (int x = 0; x < width; x++)
                    dst[y * dstStride + x] = ref[offset + x];
        }
    }

    // Flip for horizontal.
    if (horMode)
    {
        for (int y = 0; y < width - 1; y++)
        {
            for (int x = y + 1; x < width; x++)
            {
                pixel tmp              = dst[y * dstStride + x];
                dst[y * dstStride + x] = dst[x * dstStride + y];
                dst[x * dstStride + y] = tmp;
            }
        }
    }
}

template<int log2Size>
void all_angs_pred_c(pixel *dest, pixel *refPix, pixel *filtPix, int bLuma)
{
    const int size = 1 << log2Size;
    for (int mode = 2; mode <= 34; mode++)
    {
        pixel *srcPix  = (g_intraFilterFlags[mode] & size ? filtPix  : refPix);
        pixel *out = dest + ((mode - 2) << (log2Size * 2));

        intra_pred_ang_c<size>(out, size, srcPix, mode, bLuma);

        // Optimize code don't flip buffer
        bool modeHor = (mode < 18);

        // transpose the block if this is a horizontal mode
        if (modeHor)
        {
            for (int k = 0; k < size - 1; k++)
            {
                for (int l = k + 1; l < size; l++)
                {
                    pixel tmp         = out[k * size + l];
                    out[k * size + l] = out[l * size + k];
                    out[l * size + k] = tmp;
                }
            }
        }
    }
}
}

namespace X265_NS {
// x265 private namespace

void setupIntraPrimitives_c(EncoderPrimitives& p)
{
    p.cu[BLOCK_4x4].intra_filter = intraFilter<4>;
    p.cu[BLOCK_8x8].intra_filter = intraFilter<8>;
    p.cu[BLOCK_16x16].intra_filter = intraFilter<16>;
    p.cu[BLOCK_32x32].intra_filter = intraFilter<32>;

    p.cu[BLOCK_4x4].intra_pred[PLANAR_IDX] = planar_pred_c<2>;
    p.cu[BLOCK_8x8].intra_pred[PLANAR_IDX] = planar_pred_c<3>;
    p.cu[BLOCK_16x16].intra_pred[PLANAR_IDX] = planar_pred_c<4>;
    p.cu[BLOCK_32x32].intra_pred[PLANAR_IDX] = planar_pred_c<5>;

    p.cu[BLOCK_4x4].intra_pred[DC_IDX] = intra_pred_dc_c<4>;
    p.cu[BLOCK_8x8].intra_pred[DC_IDX] = intra_pred_dc_c<8>;
    p.cu[BLOCK_16x16].intra_pred[DC_IDX] = intra_pred_dc_c<16>;
    p.cu[BLOCK_32x32].intra_pred[DC_IDX] = intra_pred_dc_c<32>;

    for (int i = 2; i < NUM_INTRA_MODE; i++)
    {
        p.cu[BLOCK_4x4].intra_pred[i] = intra_pred_ang_c<4>;
        p.cu[BLOCK_8x8].intra_pred[i] = intra_pred_ang_c<8>;
        p.cu[BLOCK_16x16].intra_pred[i] = intra_pred_ang_c<16>;
        p.cu[BLOCK_32x32].intra_pred[i] = intra_pred_ang_c<32>;
    }

    p.cu[BLOCK_4x4].intra_pred_allangs = all_angs_pred_c<2>;
    p.cu[BLOCK_8x8].intra_pred_allangs = all_angs_pred_c<3>;
    p.cu[BLOCK_16x16].intra_pred_allangs = all_angs_pred_c<4>;
    p.cu[BLOCK_32x32].intra_pred_allangs = all_angs_pred_c<5>;
}
}
