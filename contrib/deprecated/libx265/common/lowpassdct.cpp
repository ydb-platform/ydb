/*****************************************************************************
 * Copyright (C) 2017 
 *
 * Authors: Humberto Ribeiro Filho <mont3z.claro5@gmail.com>
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

/* standard dct transformations */
static dct_t* s_dct4x4;
static dct_t* s_dct8x8;
static dct_t* s_dct16x16;

static void lowPassDct8_c(const int16_t* src, int16_t* dst, intptr_t srcStride)
{
    ALIGN_VAR_32(int16_t, coef[4 * 4]);
    ALIGN_VAR_32(int16_t, avgBlock[4 * 4]);
    int16_t totalSum = 0;
    int16_t sum = 0;
    
    for (int i = 0; i < 4; i++)
        for (int j =0; j < 4; j++)
        {
            // Calculate average of 2x2 cells
            sum = src[2*i*srcStride + 2*j] + src[2*i*srcStride + 2*j + 1]
                    + src[(2*i+1)*srcStride + 2*j] + src[(2*i+1)*srcStride + 2*j + 1];
            avgBlock[i*4 + j] = sum >> 2;

            totalSum += sum; // use to calculate total block average
        }

    //dct4
    (*s_dct4x4)(avgBlock, coef, 4);
    memset(dst, 0, 64 * sizeof(int16_t));
    for (int i = 0; i < 4; i++)
    {
        memcpy(&dst[i * 8], &coef[i * 4], 4 * sizeof(int16_t));
    }

    // replace first coef with total block average
    dst[0] = totalSum << 1;
}

static void lowPassDct16_c(const int16_t* src, int16_t* dst, intptr_t srcStride)
{
    ALIGN_VAR_32(int16_t, coef[8 * 8]);
    ALIGN_VAR_32(int16_t, avgBlock[8 * 8]);
    int32_t totalSum = 0;
    int16_t sum = 0;
    for (int i = 0; i < 8; i++)
        for (int j =0; j < 8; j++)
        {
            sum = src[2*i*srcStride + 2*j] + src[2*i*srcStride + 2*j + 1]
                    + src[(2*i+1)*srcStride + 2*j] + src[(2*i+1)*srcStride + 2*j + 1];
            avgBlock[i*8 + j] = sum >> 2;

            totalSum += sum;
        }

    (*s_dct8x8)(avgBlock, coef, 8);
    memset(dst, 0, 256 * sizeof(int16_t));
    for (int i = 0; i < 8; i++)
    {
        memcpy(&dst[i * 16], &coef[i * 8], 8 * sizeof(int16_t));
    }
    dst[0] = static_cast<int16_t>(totalSum >> 1);
}

static void lowPassDct32_c(const int16_t* src, int16_t* dst, intptr_t srcStride)
{
    ALIGN_VAR_32(int16_t, coef[16 * 16]);
    ALIGN_VAR_32(int16_t, avgBlock[16 * 16]);
    int32_t totalSum = 0;
    int16_t sum = 0;
    for (int i = 0; i < 16; i++)
        for (int j =0; j < 16; j++)
        {
            sum = src[2*i*srcStride + 2*j] + src[2*i*srcStride + 2*j + 1]
                    + src[(2*i+1)*srcStride + 2*j] + src[(2*i+1)*srcStride + 2*j + 1];
            avgBlock[i*16 + j] = sum >> 2;

            totalSum += sum;
        }

    (*s_dct16x16)(avgBlock, coef, 16);
    memset(dst, 0, 1024 * sizeof(int16_t));
    for (int i = 0; i < 16; i++)
    {
        memcpy(&dst[i * 32], &coef[i * 16], 16 * sizeof(int16_t));
    }
    dst[0] = static_cast<int16_t>(totalSum >> 3);
}

namespace X265_NS {
// x265 private namespace

void setupLowPassPrimitives_c(EncoderPrimitives& p)
{
    s_dct4x4 = &(p.cu[BLOCK_4x4].standard_dct);
    s_dct8x8 = &(p.cu[BLOCK_8x8].standard_dct);
    s_dct16x16 = &(p.cu[BLOCK_16x16].standard_dct);

    p.cu[BLOCK_8x8].lowpass_dct = lowPassDct8_c;
    p.cu[BLOCK_16x16].lowpass_dct = lowPassDct16_c;
    p.cu[BLOCK_32x32].lowpass_dct = lowPassDct32_c;
}
}
