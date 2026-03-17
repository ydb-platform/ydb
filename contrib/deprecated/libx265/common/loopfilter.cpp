/*****************************************************************************
* Copyright (C) 2013-2017 MulticoreWare, Inc
*
* Authors: Praveen Kumar Tiwari <praveen@multicorewareinc.com>
*          Dnyaneshwar Gorade <dnyaneshwar@multicorewareinc.com>
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

#define PIXEL_MIN 0

namespace {

/* get the sign of input variable (TODO: this is a dup, make common) */
inline int8_t signOf(int x)
{
    return (x >> 31) | ((int)((((uint32_t)-x)) >> 31));
}

static void calSign(int8_t *dst, const pixel *src1, const pixel *src2, const int endX)
{
    for (int x = 0; x < endX; x++)
        dst[x] = signOf(src1[x] - src2[x]);
}

static void processSaoCUE0(pixel * rec, int8_t * offsetEo, int width, int8_t* signLeft, intptr_t stride)
{
    int x, y;
    int8_t signRight, signLeft0;
    int8_t edgeType;

    for (y = 0; y < 2; y++)
    {
        signLeft0 = signLeft[y];
        for (x = 0; x < width; x++)
        {
            signRight = ((rec[x] - rec[x + 1]) < 0) ? -1 : ((rec[x] - rec[x + 1]) > 0) ? 1 : 0;
            edgeType = signRight + signLeft0 + 2;
            signLeft0 = -signRight;
            rec[x] = x265_clip(rec[x] + offsetEo[edgeType]);
        }
        rec += stride;
    }
}

static void processSaoCUE1(pixel* rec, int8_t* upBuff1, int8_t* offsetEo, intptr_t stride, int width)
{
    int x;
    int8_t signDown;
    int edgeType;

    for (x = 0; x < width; x++)
    {
        signDown = signOf(rec[x] - rec[x + stride]);
        edgeType = signDown + upBuff1[x] + 2;
        upBuff1[x] = -signDown;
        rec[x] = x265_clip(rec[x] + offsetEo[edgeType]);
    }
}

static void processSaoCUE1_2Rows(pixel* rec, int8_t* upBuff1, int8_t* offsetEo, intptr_t stride, int width)
{
    int x, y;
    int8_t signDown;
    int edgeType;

    for (y = 0; y < 2; y++)
    {
        for (x = 0; x < width; x++)
        {
            signDown = signOf(rec[x] - rec[x + stride]);
            edgeType = signDown + upBuff1[x] + 2;
            upBuff1[x] = -signDown;
            rec[x] = x265_clip(rec[x] + offsetEo[edgeType]);
        }
        rec += stride;
    }
}

static void processSaoCUE2(pixel * rec, int8_t * bufft, int8_t * buff1, int8_t * offsetEo, int width, intptr_t stride)
{
    int x;
    for (x = 0; x < width; x++)
    {
        int8_t signDown = signOf(rec[x] - rec[x + stride + 1]);
        int edgeType = signDown + buff1[x] + 2;
        bufft[x + 1] = -signDown;
        rec[x] = x265_clip(rec[x] + offsetEo[edgeType]);;
    }
}

static void processSaoCUE3(pixel *rec, int8_t *upBuff1, int8_t *offsetEo, intptr_t stride, int startX, int endX)
{
    int8_t signDown;
    int8_t edgeType;

    for (int x = startX + 1; x < endX; x++)
    {
        signDown = signOf(rec[x] - rec[x + stride]);
        edgeType = signDown + upBuff1[x] + 2;
        upBuff1[x - 1] = -signDown;
        rec[x] = x265_clip(rec[x] + offsetEo[edgeType]);
    }
}

static void processSaoCUB0(pixel* rec, const int8_t* offset, int ctuWidth, int ctuHeight, intptr_t stride)
{
    #define SAO_BO_BITS 5
    const int boShift = X265_DEPTH - SAO_BO_BITS;
    int x, y;
    for (y = 0; y < ctuHeight; y++)
    {
        for (x = 0; x < ctuWidth; x++)
        {
            rec[x] = x265_clip(rec[x] + offset[rec[x] >> boShift]);
        }
        rec += stride;
    }
}

static void pelFilterLumaStrong_c(pixel* src, intptr_t srcStep, intptr_t offset, int32_t tcP, int32_t tcQ)
{
    for (int32_t i = 0; i < UNIT_SIZE; i++, src += srcStep)
    {
        int16_t m4  = (int16_t)src[0];
        int16_t m3  = (int16_t)src[-offset];
        int16_t m5  = (int16_t)src[offset];
        int16_t m2  = (int16_t)src[-offset * 2];
        int16_t m6  = (int16_t)src[offset * 2];
        int16_t m1  = (int16_t)src[-offset * 3];
        int16_t m7  = (int16_t)src[offset * 3];
        int16_t m0  = (int16_t)src[-offset * 4];
        src[-offset * 3] = (pixel)(x265_clip3(-tcP, tcP, ((2 * m0 + 3 * m1 + m2 + m3 + m4 + 4) >> 3) - m1) + m1);
        src[-offset * 2] = (pixel)(x265_clip3(-tcP, tcP, ((m1 + m2 + m3 + m4 + 2) >> 2) - m2) + m2);
        src[-offset]     = (pixel)(x265_clip3(-tcP, tcP, ((m1 + 2 * m2 + 2 * m3 + 2 * m4 + m5 + 4) >> 3) - m3) + m3);
        src[0]           = (pixel)(x265_clip3(-tcQ, tcQ, ((m2 + 2 * m3 + 2 * m4 + 2 * m5 + m6 + 4) >> 3) - m4) + m4);
        src[offset]      = (pixel)(x265_clip3(-tcQ, tcQ, ((m3 + m4 + m5 + m6 + 2) >> 2) - m5) + m5);
        src[offset * 2]  = (pixel)(x265_clip3(-tcQ, tcQ, ((m3 + m4 + m5 + 3 * m6 + 2 * m7 + 4) >> 3) - m6) + m6);
    }
}

/* Deblocking of one line/column for the chrominance component
* \param src     pointer to picture data
* \param offset  offset value for picture data
* \param tc      tc value
* \param maskP   indicator to disable filtering on partP
* \param maskQ   indicator to disable filtering on partQ */
static void pelFilterChroma_c(pixel* src, intptr_t srcStep, intptr_t offset, int32_t tc, int32_t maskP, int32_t maskQ)
{
    for (int32_t i = 0; i < UNIT_SIZE; i++, src += srcStep)
    {
        int16_t m4 = (int16_t)src[0];
        int16_t m3 = (int16_t)src[-offset];
        int16_t m5 = (int16_t)src[offset];
        int16_t m2 = (int16_t)src[-offset * 2];

        int32_t delta = x265_clip3(-tc, tc, ((((m4 - m3) * 4) + m2 - m5 + 4) >> 3));
        src[-offset]  = x265_clip(m3 + (delta & maskP));
        src[0]        = x265_clip(m4 - (delta & maskQ));
    }
}
}

namespace X265_NS {
void setupLoopFilterPrimitives_c(EncoderPrimitives &p)
{
    p.saoCuOrgE0 = processSaoCUE0;
    p.saoCuOrgE1 = processSaoCUE1;
    p.saoCuOrgE1_2Rows = processSaoCUE1_2Rows;
    p.saoCuOrgE2[0] = processSaoCUE2;
    p.saoCuOrgE2[1] = processSaoCUE2;
    p.saoCuOrgE3[0] = processSaoCUE3;
    p.saoCuOrgE3[1] = processSaoCUE3;
    p.saoCuOrgB0 = processSaoCUB0;
    p.sign = calSign;

    // C code is same for EDGE_VER and EDGE_HOR only asm code is different
    p.pelFilterLumaStrong[0] = pelFilterLumaStrong_c;
    p.pelFilterLumaStrong[1] = pelFilterLumaStrong_c;
    p.pelFilterChroma[0]     = pelFilterChroma_c;
    p.pelFilterChroma[1]     = pelFilterChroma_c;
}
}
