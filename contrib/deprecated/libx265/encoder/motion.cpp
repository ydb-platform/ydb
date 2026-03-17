/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Steve Borho <steve@borho.org>
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
#include "lowres.h"
#include "motion.h"
#include "x265.h"

#if _MSC_VER
#pragma warning(disable: 4127) // conditional  expression is constant (macros use this construct)
#endif

using namespace X265_NS;

namespace {

struct SubpelWorkload
{
    int hpel_iters;
    int hpel_dirs;
    int qpel_iters;
    int qpel_dirs;
    bool hpel_satd;
};

const SubpelWorkload workload[X265_MAX_SUBPEL_LEVEL + 1] =
{
    { 1, 4, 0, 4, false }, // 4 SAD HPEL only
    { 1, 4, 1, 4, false }, // 4 SAD HPEL + 4 SATD QPEL
    { 1, 4, 1, 4, true },  // 4 SATD HPEL + 4 SATD QPEL
    { 2, 4, 1, 4, true },  // 2x4 SATD HPEL + 4 SATD QPEL
    { 2, 4, 2, 4, true },  // 2x4 SATD HPEL + 2x4 SATD QPEL
    { 1, 8, 1, 8, true },  // 8 SATD HPEL + 8 SATD QPEL (default)
    { 2, 8, 1, 8, true },  // 2x8 SATD HPEL + 8 SATD QPEL
    { 2, 8, 2, 8, true },  // 2x8 SATD HPEL + 2x8 SATD QPEL
};

static int sizeScale[NUM_PU_SIZES];
#define SAD_THRESH(v) (bcost < (((v >> 4) * sizeScale[partEnum])))

/* radius 2 hexagon. repeated entries are to avoid having to compute mod6 every time. */
const MV hex2[8] = { MV(-1, -2), MV(-2, 0), MV(-1, 2), MV(1, 2), MV(2, 0), MV(1, -2), MV(-1, -2), MV(-2, 0) };
const uint8_t mod6m1[8] = { 5, 0, 1, 2, 3, 4, 5, 0 };  /* (x-1)%6 */
const MV square1[9] = { MV(0, 0), MV(0, -1), MV(0, 1), MV(-1, 0), MV(1, 0), MV(-1, -1), MV(-1, 1), MV(1, -1), MV(1, 1) };
const MV hex4[16] =
{
    MV(0, -4), MV(0, 4), MV(-2, -3), MV(2, -3),
    MV(-4, -2), MV(4, -2), MV(-4, -1), MV(4, -1),
    MV(-4, 0), MV(4, 0), MV(-4, 1), MV(4, 1),
    MV(-4, 2), MV(4, 2), MV(-2, 3), MV(2, 3),
};
const MV offsets[] =
{
    MV(-1, 0), MV(0, -1),
    MV(-1, -1), MV(1, -1),
    MV(-1, 0), MV(1, 0),
    MV(-1, 1), MV(-1, -1),
    MV(1, -1), MV(1, 1),
    MV(-1, 0), MV(0, 1),
    MV(-1, 1), MV(1, 1),
    MV(1, 0), MV(0, 1),
}; // offsets for Two Point Search

/* sum of absolute differences between MV candidates, used for adaptive ME range */
inline int predictorDifference(const MV *mvc, intptr_t numCandidates)
{
    int sum = 0;

    for (int i = 0; i < numCandidates - 1; i++)
    {
        sum += abs(mvc[i].x - mvc[i + 1].x)
            +  abs(mvc[i].y - mvc[i + 1].y);
    }

    return sum;
}

}

MotionEstimate::MotionEstimate()
{
    ctuAddr = -1;
    absPartIdx = -1;
    searchMethod = X265_HEX_SEARCH;
    subpelRefine = 2;
    blockwidth = blockheight = 0;
    blockOffset = 0;
    bChromaSATD = false;
    chromaSatd = NULL;
    for (int i = 0; i < INTEGRAL_PLANE_NUM; i++)
        integral[i] = NULL;
}

void MotionEstimate::init(int csp)
{
    fencPUYuv.create(FENC_STRIDE, csp);
}

void MotionEstimate::initScales(void)
{
#define SETUP_SCALE(W, H) \
    sizeScale[LUMA_ ## W ## x ## H] = (H * H) >> 4;
    SETUP_SCALE(4, 4);
    SETUP_SCALE(8, 8);
    SETUP_SCALE(8, 4);
    SETUP_SCALE(4, 8);
    SETUP_SCALE(16, 16);
    SETUP_SCALE(16, 8);
    SETUP_SCALE(8, 16);
    SETUP_SCALE(16, 12);
    SETUP_SCALE(12, 16);
    SETUP_SCALE(4, 16);
    SETUP_SCALE(16, 4);
    SETUP_SCALE(32, 32);
    SETUP_SCALE(32, 16);
    SETUP_SCALE(16, 32);
    SETUP_SCALE(32, 24);
    SETUP_SCALE(24, 32);
    SETUP_SCALE(32, 8);
    SETUP_SCALE(8, 32);
    SETUP_SCALE(64, 64);
    SETUP_SCALE(64, 32);
    SETUP_SCALE(32, 64);
    SETUP_SCALE(64, 48);
    SETUP_SCALE(48, 64);
    SETUP_SCALE(64, 16);
    SETUP_SCALE(16, 64);
#undef SETUP_SCALE
}

int MotionEstimate::hpelIterationCount(int subme)
{
    return workload[subme].hpel_iters +
           workload[subme].qpel_iters / 2;
}

MotionEstimate::~MotionEstimate()
{
    fencPUYuv.destroy();
}

/* Called by lookahead, luma only, no use of PicYuv */
void MotionEstimate::setSourcePU(pixel *fencY, intptr_t stride, intptr_t offset, int pwidth, int pheight, const int method, const int refine)
{
    partEnum = partitionFromSizes(pwidth, pheight);
    X265_CHECK(LUMA_4x4 != partEnum, "4x4 inter partition detected!\n");
    sad = primitives.pu[partEnum].sad;
    ads = primitives.pu[partEnum].ads;
    satd = primitives.pu[partEnum].satd;
    sad_x3 = primitives.pu[partEnum].sad_x3;
    sad_x4 = primitives.pu[partEnum].sad_x4;


    blockwidth = pwidth;
    blockOffset = offset;
    absPartIdx = ctuAddr = -1;

    /* Search params */
    searchMethod = method;
    subpelRefine = refine;

    /* copy PU block into cache */
    primitives.pu[partEnum].copy_pp(fencPUYuv.m_buf[0], FENC_STRIDE, fencY + offset, stride);
    X265_CHECK(!bChromaSATD, "chroma distortion measurements impossible in this code path\n");
}

/* Called by Search::predInterSearch() or --pme equivalent, chroma residual might be considered */
void MotionEstimate::setSourcePU(const Yuv& srcFencYuv, int _ctuAddr, int cuPartIdx, int puPartIdx, int pwidth, int pheight, const int method, const int refine, bool bChroma)
{
    partEnum = partitionFromSizes(pwidth, pheight);
    X265_CHECK(LUMA_4x4 != partEnum, "4x4 inter partition detected!\n");
    sad = primitives.pu[partEnum].sad;
    ads = primitives.pu[partEnum].ads;
    satd = primitives.pu[partEnum].satd;
    sad_x3 = primitives.pu[partEnum].sad_x3;
    sad_x4 = primitives.pu[partEnum].sad_x4;

    chromaSatd = primitives.chroma[fencPUYuv.m_csp].pu[partEnum].satd;

    /* Set search characteristics */
    searchMethod = method;
    subpelRefine = refine;

    /* Enable chroma residual cost if subpelRefine level is greater than 2 and chroma block size
     * is an even multiple of 4x4 pixels (indicated by non-null chromaSatd pointer) */
    bChromaSATD = subpelRefine > 2 && chromaSatd && (srcFencYuv.m_csp != X265_CSP_I400 && bChroma);
    X265_CHECK(!(bChromaSATD && !workload[subpelRefine].hpel_satd), "Chroma SATD cannot be used with SAD hpel\n");

    ctuAddr = _ctuAddr;
    absPartIdx = cuPartIdx + puPartIdx;
    blockwidth = pwidth;
    blockOffset = 0;

    /* copy PU from CU Yuv */
    fencPUYuv.copyPUFromYuv(srcFencYuv, puPartIdx, partEnum, bChromaSATD);
}

#define COST_MV_PT_DIST(mx, my, point, dist) \
    do \
    { \
        MV tmv(mx, my); \
        int cost = sad(fenc, FENC_STRIDE, fref + mx + my * stride, stride); \
        cost += mvcost(tmv << 2); \
        if (cost < bcost) { \
            bcost = cost; \
            bmv = tmv; \
            bPointNr = point; \
            bDistance = dist; \
        } \
    } while (0)

#define COST_MV(mx, my) \
    do \
    { \
        int cost = sad(fenc, FENC_STRIDE, fref + (mx) + (my) * stride, stride); \
        cost += mvcost(MV(mx, my) << 2); \
        COPY2_IF_LT(bcost, cost, bmv, MV(mx, my)); \
    } while (0)

#define COST_MV_X3_DIR(m0x, m0y, m1x, m1y, m2x, m2y, costs) \
    { \
        pixel *pix_base = fref + bmv.x + bmv.y * stride; \
        sad_x3(fenc, \
               pix_base + (m0x) + (m0y) * stride, \
               pix_base + (m1x) + (m1y) * stride, \
               pix_base + (m2x) + (m2y) * stride, \
               stride, costs); \
        (costs)[0] += mvcost((bmv + MV(m0x, m0y)) << 2); \
        (costs)[1] += mvcost((bmv + MV(m1x, m1y)) << 2); \
        (costs)[2] += mvcost((bmv + MV(m2x, m2y)) << 2); \
    }

#define COST_MV_PT_DIST_X4(m0x, m0y, p0, d0, m1x, m1y, p1, d1, m2x, m2y, p2, d2, m3x, m3y, p3, d3) \
    { \
        sad_x4(fenc, \
               fref + (m0x) + (m0y) * stride, \
               fref + (m1x) + (m1y) * stride, \
               fref + (m2x) + (m2y) * stride, \
               fref + (m3x) + (m3y) * stride, \
               stride, costs); \
        (costs)[0] += mvcost(MV(m0x, m0y) << 2); \
        (costs)[1] += mvcost(MV(m1x, m1y) << 2); \
        (costs)[2] += mvcost(MV(m2x, m2y) << 2); \
        (costs)[3] += mvcost(MV(m3x, m3y) << 2); \
        COPY4_IF_LT(bcost, costs[0], bmv, MV(m0x, m0y), bPointNr, p0, bDistance, d0); \
        COPY4_IF_LT(bcost, costs[1], bmv, MV(m1x, m1y), bPointNr, p1, bDistance, d1); \
        COPY4_IF_LT(bcost, costs[2], bmv, MV(m2x, m2y), bPointNr, p2, bDistance, d2); \
        COPY4_IF_LT(bcost, costs[3], bmv, MV(m3x, m3y), bPointNr, p3, bDistance, d3); \
    }

#define COST_MV_X4(m0x, m0y, m1x, m1y, m2x, m2y, m3x, m3y) \
    { \
        pixel *pix_base = fref + omv.x + omv.y * stride; \
        sad_x4(fenc, \
               pix_base + (m0x) + (m0y) * stride, \
               pix_base + (m1x) + (m1y) * stride, \
               pix_base + (m2x) + (m2y) * stride, \
               pix_base + (m3x) + (m3y) * stride, \
               stride, costs); \
        costs[0] += mvcost((omv + MV(m0x, m0y)) << 2); \
        costs[1] += mvcost((omv + MV(m1x, m1y)) << 2); \
        costs[2] += mvcost((omv + MV(m2x, m2y)) << 2); \
        costs[3] += mvcost((omv + MV(m3x, m3y)) << 2); \
        if ((omv.y + m0y >= mvmin.y) & (omv.y + m0y <= mvmax.y)) \
            COPY2_IF_LT(bcost, costs[0], bmv, omv + MV(m0x, m0y)); \
        if ((omv.y + m1y >= mvmin.y) & (omv.y + m1y <= mvmax.y)) \
            COPY2_IF_LT(bcost, costs[1], bmv, omv + MV(m1x, m1y)); \
        if ((omv.y + m2y >= mvmin.y) & (omv.y + m2y <= mvmax.y)) \
            COPY2_IF_LT(bcost, costs[2], bmv, omv + MV(m2x, m2y)); \
        if ((omv.y + m3y >= mvmin.y) & (omv.y + m3y <= mvmax.y)) \
            COPY2_IF_LT(bcost, costs[3], bmv, omv + MV(m3x, m3y)); \
    }

#define COST_MV_X3_ABS( m0x, m0y, m1x, m1y, m2x, m2y )\
{\
    sad_x3(fenc, \
    fref + (m0x) + (m0y) * stride, \
    fref + (m1x) + (m1y) * stride, \
    fref + (m2x) + (m2y) * stride, \
    stride, costs); \
    costs[0] += p_cost_mvx[(m0x) << 2]; /* no cost_mvy */\
    costs[1] += p_cost_mvx[(m1x) << 2]; \
    costs[2] += p_cost_mvx[(m2x) << 2]; \
    COPY3_IF_LT(bcost, costs[0], bmv.x, m0x, bmv.y, m0y); \
    COPY3_IF_LT(bcost, costs[1], bmv.x, m1x, bmv.y, m1y); \
    COPY3_IF_LT(bcost, costs[2], bmv.x, m2x, bmv.y, m2y); \
}

#define COST_MV_X4_DIR(m0x, m0y, m1x, m1y, m2x, m2y, m3x, m3y, costs) \
    { \
        pixel *pix_base = fref + bmv.x + bmv.y * stride; \
        sad_x4(fenc, \
               pix_base + (m0x) + (m0y) * stride, \
               pix_base + (m1x) + (m1y) * stride, \
               pix_base + (m2x) + (m2y) * stride, \
               pix_base + (m3x) + (m3y) * stride, \
               stride, costs); \
        (costs)[0] += mvcost((bmv + MV(m0x, m0y)) << 2); \
        (costs)[1] += mvcost((bmv + MV(m1x, m1y)) << 2); \
        (costs)[2] += mvcost((bmv + MV(m2x, m2y)) << 2); \
        (costs)[3] += mvcost((bmv + MV(m3x, m3y)) << 2); \
    }

#define DIA1_ITER(mx, my) \
    { \
        omv.x = mx; omv.y = my; \
        COST_MV_X4(0, -1, 0, 1, -1, 0, 1, 0); \
    }

#define CROSS(start, x_max, y_max) \
    { \
        int16_t i = start; \
        if ((x_max) <= X265_MIN(mvmax.x - omv.x, omv.x - mvmin.x)) \
            for (; i < (x_max) - 2; i += 4) { \
                COST_MV_X4(i, 0, -i, 0, i + 2, 0, -i - 2, 0); } \
        for (; i < (x_max); i += 2) \
        { \
            if (omv.x + i <= mvmax.x) \
                COST_MV(omv.x + i, omv.y); \
            if (omv.x - i >= mvmin.x) \
                COST_MV(omv.x - i, omv.y); \
        } \
        i = start; \
        if ((y_max) <= X265_MIN(mvmax.y - omv.y, omv.y - mvmin.y)) \
            for (; i < (y_max) - 2; i += 4) { \
                COST_MV_X4(0, i, 0, -i, 0, i + 2, 0, -i - 2); } \
        for (; i < (y_max); i += 2) \
        { \
            if (omv.y + i <= mvmax.y) \
                COST_MV(omv.x, omv.y + i); \
            if (omv.y - i >= mvmin.y) \
                COST_MV(omv.x, omv.y - i); \
        } \
    }

void MotionEstimate::StarPatternSearch(ReferencePlanes *ref,
                                       const MV &       mvmin,
                                       const MV &       mvmax,
                                       MV &             bmv,
                                       int &            bcost,
                                       int &            bPointNr,
                                       int &            bDistance,
                                       int              earlyExitIters,
                                       int              merange)
{
    ALIGN_VAR_16(int, costs[16]);
    pixel* fenc = fencPUYuv.m_buf[0];
    pixel* fref = ref->fpelPlane[0] + blockOffset;
    intptr_t stride = ref->lumaStride;

    MV omv = bmv;
    int saved = bcost;
    int rounds = 0;

    {
        int16_t dist = 1;

        /* bPointNr
              2
            4 * 5
              7
         */
        const int16_t top    = omv.y - dist;
        const int16_t bottom = omv.y + dist;
        const int16_t left   = omv.x - dist;
        const int16_t right  = omv.x + dist;

        if (top >= mvmin.y && left >= mvmin.x && right <= mvmax.x && bottom <= mvmax.y)
        {
            COST_MV_PT_DIST_X4(omv.x,  top,    2, dist,
                               left,  omv.y,   4, dist,
                               right, omv.y,   5, dist,
                               omv.x,  bottom, 7, dist);
        }
        else
        {
            if (top >= mvmin.y) // check top
            {
                COST_MV_PT_DIST(omv.x, top, 2, dist);
            }
            if (left >= mvmin.x) // check middle left
            {
                COST_MV_PT_DIST(left, omv.y, 4, dist);
            }
            if (right <= mvmax.x) // check middle right
            {
                COST_MV_PT_DIST(right, omv.y, 5, dist);
            }
            if (bottom <= mvmax.y) // check bottom
            {
                COST_MV_PT_DIST(omv.x, bottom, 7, dist);
            }
        }
        if (bcost < saved)
            rounds = 0;
        else if (++rounds >= earlyExitIters)
            return;
    }

    for (int16_t dist = 2; dist <= 8; dist <<= 1)
    {
        /* bPointNr
              2
             1 3
            4 * 5
             6 8
              7
         Points 2, 4, 5, 7 are dist
         Points 1, 3, 6, 8 are dist>>1
         */
        const int16_t top     = omv.y - dist;
        const int16_t bottom  = omv.y + dist;
        const int16_t left    = omv.x - dist;
        const int16_t right   = omv.x + dist;
        const int16_t top2    = omv.y - (dist >> 1);
        const int16_t bottom2 = omv.y + (dist >> 1);
        const int16_t left2   = omv.x - (dist >> 1);
        const int16_t right2  = omv.x + (dist >> 1);
        saved = bcost;

        if (top >= mvmin.y && left >= mvmin.x &&
            right <= mvmax.x && bottom <= mvmax.y) // check border
        {
            COST_MV_PT_DIST_X4(omv.x,  top,   2, dist,
                               left2,  top2,  1, dist >> 1,
                               right2, top2,  3, dist >> 1,
                               left,   omv.y, 4, dist);
            COST_MV_PT_DIST_X4(right,  omv.y,   5, dist,
                               left2,  bottom2, 6, dist >> 1,
                               right2, bottom2, 8, dist >> 1,
                               omv.x,  bottom,  7, dist);
        }
        else // check border for each mv
        {
            if (top >= mvmin.y) // check top
            {
                COST_MV_PT_DIST(omv.x, top, 2, dist);
            }
            if (top2 >= mvmin.y) // check half top
            {
                if (left2 >= mvmin.x) // check half left
                {
                    COST_MV_PT_DIST(left2, top2, 1, (dist >> 1));
                }
                if (right2 <= mvmax.x) // check half right
                {
                    COST_MV_PT_DIST(right2, top2, 3, (dist >> 1));
                }
            }
            if (left >= mvmin.x) // check left
            {
                COST_MV_PT_DIST(left, omv.y, 4, dist);
            }
            if (right <= mvmax.x) // check right
            {
                COST_MV_PT_DIST(right, omv.y, 5, dist);
            }
            if (bottom2 <= mvmax.y) // check half bottom
            {
                if (left2 >= mvmin.x) // check half left
                {
                    COST_MV_PT_DIST(left2, bottom2, 6, (dist >> 1));
                }
                if (right2 <= mvmax.x) // check half right
                {
                    COST_MV_PT_DIST(right2, bottom2, 8, (dist >> 1));
                }
            }
            if (bottom <= mvmax.y) // check bottom
            {
                COST_MV_PT_DIST(omv.x, bottom, 7, dist);
            }
        }

        if (bcost < saved)
            rounds = 0;
        else if (++rounds >= earlyExitIters)
            return;
    }

    for (int16_t dist = 16; dist <= (int16_t)merange; dist <<= 1)
    {
        const int16_t top    = omv.y - dist;
        const int16_t bottom = omv.y + dist;
        const int16_t left   = omv.x - dist;
        const int16_t right  = omv.x + dist;

        saved = bcost;
        if (top >= mvmin.y && left >= mvmin.x &&
            right <= mvmax.x && bottom <= mvmax.y) // check border
        {
            /* index
                  0
                  3
                  2
                  1
          0 3 2 1 * 1 2 3 0
                  1
                  2
                  3
                  0
            */

            COST_MV_PT_DIST_X4(omv.x,  top,    0, dist,
                               left,   omv.y,  0, dist,
                               right,  omv.y,  0, dist,
                               omv.x,  bottom, 0, dist);

            for (int16_t index = 1; index < 4; index++)
            {
                int16_t posYT = top    + ((dist >> 2) * index);
                int16_t posYB = bottom - ((dist >> 2) * index);
                int16_t posXL = omv.x  - ((dist >> 2) * index);
                int16_t posXR = omv.x  + ((dist >> 2) * index);

                COST_MV_PT_DIST_X4(posXL, posYT, 0, dist,
                                   posXR, posYT, 0, dist,
                                   posXL, posYB, 0, dist,
                                   posXR, posYB, 0, dist);
            }
        }
        else // check border for each mv
        {
            if (top >= mvmin.y) // check top
            {
                COST_MV_PT_DIST(omv.x, top, 0, dist);
            }
            if (left >= mvmin.x) // check left
            {
                COST_MV_PT_DIST(left, omv.y, 0, dist);
            }
            if (right <= mvmax.x) // check right
            {
                COST_MV_PT_DIST(right, omv.y, 0, dist);
            }
            if (bottom <= mvmax.y) // check bottom
            {
                COST_MV_PT_DIST(omv.x, bottom, 0, dist);
            }
            for (int16_t index = 1; index < 4; index++)
            {
                int16_t posYT = top    + ((dist >> 2) * index);
                int16_t posYB = bottom - ((dist >> 2) * index);
                int16_t posXL = omv.x - ((dist >> 2) * index);
                int16_t posXR = omv.x + ((dist >> 2) * index);

                if (posYT >= mvmin.y) // check top
                {
                    if (posXL >= mvmin.x) // check left
                    {
                        COST_MV_PT_DIST(posXL, posYT, 0, dist);
                    }
                    if (posXR <= mvmax.x) // check right
                    {
                        COST_MV_PT_DIST(posXR, posYT, 0, dist);
                    }
                }
                if (posYB <= mvmax.y) // check bottom
                {
                    if (posXL >= mvmin.x) // check left
                    {
                        COST_MV_PT_DIST(posXL, posYB, 0, dist);
                    }
                    if (posXR <= mvmax.x) // check right
                    {
                        COST_MV_PT_DIST(posXR, posYB, 0, dist);
                    }
                }
            }
        }

        if (bcost < saved)
            rounds = 0;
        else if (++rounds >= earlyExitIters)
            return;
    }
}

void MotionEstimate::refineMV(ReferencePlanes* ref,
                              const MV&        mvmin,
                              const MV&        mvmax,
                              const MV&        qmvp,
                              MV&              outQMv)
{
    ALIGN_VAR_16(int, costs[16]);
    if (ctuAddr >= 0)
        blockOffset = ref->reconPic->getLumaAddr(ctuAddr, absPartIdx) - ref->reconPic->getLumaAddr(0);
    intptr_t stride = ref->lumaStride;
    pixel* fenc = fencPUYuv.m_buf[0];
    pixel* fref = ref->fpelPlane[0] + blockOffset;
    
    setMVP(qmvp);
    
    MV qmvmin = mvmin.toQPel();
    MV qmvmax = mvmax.toQPel();
   
    /* The term cost used here means satd/sad values for that particular search.
     * The costs used in ME integer search only includes the SAD cost of motion
     * residual and sqrtLambda times MVD bits.  The subpel refine steps use SATD
     * cost of residual and sqrtLambda * MVD bits.
    */
             
    // measure SATD cost at clipped QPEL MVP
    MV pmv = qmvp.clipped(qmvmin, qmvmax);
    MV bestpre = pmv;
    int bprecost;

    bprecost = subpelCompare(ref, pmv, sad);

    /* re-measure full pel rounded MVP with SAD as search start point */
    MV bmv = pmv.roundToFPel();
    int bcost = bprecost;
    if (pmv.isSubpel())
        bcost = sad(fenc, FENC_STRIDE, fref + bmv.x + bmv.y * stride, stride) + mvcost(bmv << 2);

    /* square refine */
    int dir = 0;
    COST_MV_X4_DIR(0, -1, 0, 1, -1, 0, 1, 0, costs);
    if ((bmv.y - 1 >= mvmin.y) & (bmv.y - 1 <= mvmax.y))
        COPY2_IF_LT(bcost, costs[0], dir, 1);
    if ((bmv.y + 1 >= mvmin.y) & (bmv.y + 1 <= mvmax.y))
        COPY2_IF_LT(bcost, costs[1], dir, 2);
    COPY2_IF_LT(bcost, costs[2], dir, 3);
    COPY2_IF_LT(bcost, costs[3], dir, 4);
    COST_MV_X4_DIR(-1, -1, -1, 1, 1, -1, 1, 1, costs);
    if ((bmv.y - 1 >= mvmin.y) & (bmv.y - 1 <= mvmax.y))
        COPY2_IF_LT(bcost, costs[0], dir, 5);
    if ((bmv.y + 1 >= mvmin.y) & (bmv.y + 1 <= mvmax.y))
        COPY2_IF_LT(bcost, costs[1], dir, 6);
    if ((bmv.y - 1 >= mvmin.y) & (bmv.y - 1 <= mvmax.y))
        COPY2_IF_LT(bcost, costs[2], dir, 7);
    if ((bmv.y + 1 >= mvmin.y) & (bmv.y + 1 <= mvmax.y))
        COPY2_IF_LT(bcost, costs[3], dir, 8);
    bmv += square1[dir];

    if (bprecost < bcost)
    {
        bmv = bestpre;
        bcost = bprecost;
    }
    else
        bmv = bmv.toQPel(); // promote search bmv to qpel

    // TO DO: Change SubpelWorkload to fine tune MV
    // Now it is set to 5 for experiment.
    // const SubpelWorkload& wl = workload[this->subpelRefine];
    const SubpelWorkload& wl = workload[5];

    pixelcmp_t hpelcomp;

    if (wl.hpel_satd)
    {
        bcost = subpelCompare(ref, bmv, satd) + mvcost(bmv);
        hpelcomp = satd;
    }
    else
        hpelcomp = sad;

    for (int iter = 0; iter < wl.hpel_iters; iter++)
    {
        int bdir = 0;
        for (int i = 1; i <= wl.hpel_dirs; i++)
        {
            MV qmv = bmv + square1[i] * 2;            

            // check mv range for slice bound
            if ((qmv.y < qmvmin.y) | (qmv.y > qmvmax.y))
                continue;

            int cost = subpelCompare(ref, qmv, hpelcomp) + mvcost(qmv);
            COPY2_IF_LT(bcost, cost, bdir, i);
        }

        if (bdir)
            bmv += square1[bdir] * 2;            
        else
            break;
    }

    /* if HPEL search used SAD, remeasure with SATD before QPEL */
    if (!wl.hpel_satd)
        bcost = subpelCompare(ref, bmv, satd) + mvcost(bmv);

    for (int iter = 0; iter < wl.qpel_iters; iter++)
    {
        int bdir = 0;
        for (int i = 1; i <= wl.qpel_dirs; i++)
        {
            MV qmv = bmv + square1[i];
            
            // check mv range for slice bound
            if ((qmv.y < qmvmin.y) | (qmv.y > qmvmax.y))
                continue;

            int cost = subpelCompare(ref, qmv, satd) + mvcost(qmv);
            COPY2_IF_LT(bcost, cost, bdir, i);
        }

        if (bdir)
            bmv += square1[bdir];
        else
            break;
    }

    // check mv range for slice bound
    X265_CHECK(((pmv.y >= qmvmin.y) & (pmv.y <= qmvmax.y)), "mv beyond range!");
    
    x265_emms();
    outQMv = bmv;
}

int MotionEstimate::motionEstimate(ReferencePlanes *ref,
                                   const MV &       mvmin,
                                   const MV &       mvmax,
                                   const MV &       qmvp,
                                   int              numCandidates,
                                   const MV *       mvc,
                                   int              merange,
                                   MV &             outQMv,
                                   uint32_t         maxSlices,
                                   pixel *          srcReferencePlane)
{
    ALIGN_VAR_16(int, costs[16]);
    if (ctuAddr >= 0)
        blockOffset = ref->reconPic->getLumaAddr(ctuAddr, absPartIdx) - ref->reconPic->getLumaAddr(0);
    intptr_t stride = ref->lumaStride;
    pixel* fenc = fencPUYuv.m_buf[0];
    pixel* fref = srcReferencePlane == 0 ? ref->fpelPlane[0] + blockOffset : srcReferencePlane + blockOffset;

    setMVP(qmvp);

    MV qmvmin = mvmin.toQPel();
    MV qmvmax = mvmax.toQPel();

    /* The term cost used here means satd/sad values for that particular search.
     * The costs used in ME integer search only includes the SAD cost of motion
     * residual and sqrtLambda times MVD bits.  The subpel refine steps use SATD
     * cost of residual and sqrtLambda * MVD bits.  Mode decision will be based
     * on video distortion cost (SSE/PSNR) plus lambda times all signaling bits
     * (mode + MVD bits). */

    // measure SAD cost at clipped QPEL MVP
    MV pmv = qmvp.clipped(qmvmin, qmvmax);
    MV bestpre = pmv;
    int bprecost;

    if (ref->isLowres)
        bprecost = ref->lowresQPelCost(fenc, blockOffset, pmv, sad);
    else
        bprecost = subpelCompare(ref, pmv, sad);

    /* re-measure full pel rounded MVP with SAD as search start point */
    MV bmv = pmv.roundToFPel();
    int bcost = bprecost;
    if (pmv.isSubpel())
        bcost = sad(fenc, FENC_STRIDE, fref + bmv.x + bmv.y * stride, stride) + mvcost(bmv << 2);

    // measure SAD cost at MV(0) if MVP is not zero
    if (pmv.notZero())
    {
        int cost = sad(fenc, FENC_STRIDE, fref, stride) + mvcost(MV(0, 0));
        if (cost < bcost)
        {
            bcost = cost;
            bmv = 0;
            bmv.y = X265_MAX(X265_MIN(0, mvmax.y), mvmin.y);
        }
    }

    X265_CHECK(!(ref->isLowres && numCandidates), "lowres motion candidates not allowed\n")
    // measure SAD cost at each QPEL motion vector candidate
    for (int i = 0; i < numCandidates; i++)
    {
        MV m = mvc[i].clipped(qmvmin, qmvmax);
        if (m.notZero() & (m != pmv ? 1 : 0) & (m != bestpre ? 1 : 0)) // check already measured
        {
            int cost = subpelCompare(ref, m, sad) + mvcost(m);
            if (cost < bprecost)
            {
                bprecost = cost;
                bestpre = m;
            }
        }
    }

    pmv = pmv.roundToFPel();
    MV omv = bmv;  // current search origin or starting point

    switch (searchMethod)
    {
    case X265_DIA_SEARCH:
    {
        /* diamond search, radius 1 */
        bcost <<= 4;
        int i = merange;
        do
        {
            COST_MV_X4_DIR(0, -1, 0, 1, -1, 0, 1, 0, costs);
            if ((bmv.y - 1 >= mvmin.y) & (bmv.y - 1 <= mvmax.y))
                COPY1_IF_LT(bcost, (costs[0] << 4) + 1);
            if ((bmv.y + 1 >= mvmin.y) & (bmv.y + 1 <= mvmax.y))
                COPY1_IF_LT(bcost, (costs[1] << 4) + 3);
            COPY1_IF_LT(bcost, (costs[2] << 4) + 4);
            COPY1_IF_LT(bcost, (costs[3] << 4) + 12);
            if (!(bcost & 15))
                break;
            bmv.x -= (bcost << 28) >> 30;
            bmv.y -= (bcost << 30) >> 30;
            bcost &= ~15;
        }
        while (--i && bmv.checkRange(mvmin, mvmax));
        bcost >>= 4;
        break;
    }

    case X265_HEX_SEARCH:
    {
me_hex2:
        /* hexagon search, radius 2 */
#if 0
        for (int i = 0; i < merange / 2; i++)
        {
            omv = bmv;
            COST_MV(omv.x - 2, omv.y);
            COST_MV(omv.x - 1, omv.y + 2);
            COST_MV(omv.x + 1, omv.y + 2);
            COST_MV(omv.x + 2, omv.y);
            COST_MV(omv.x + 1, omv.y - 2);
            COST_MV(omv.x - 1, omv.y - 2);
            if (omv == bmv)
                break;
            if (!bmv.checkRange(mvmin, mvmax))
                break;
        }

#else // if 0
      /* equivalent to the above, but eliminates duplicate candidates */
        COST_MV_X3_DIR(-2, 0, -1, 2,  1, 2, costs);
        bcost <<= 3;
        if ((bmv.y >= mvmin.y) & (bmv.y <= mvmax.y))
            COPY1_IF_LT(bcost, (costs[0] << 3) + 2);
        if ((bmv.y + 2 >= mvmin.y) & (bmv.y + 2 <= mvmax.y))
        {
            COPY1_IF_LT(bcost, (costs[1] << 3) + 3);
            COPY1_IF_LT(bcost, (costs[2] << 3) + 4);
        }

        COST_MV_X3_DIR(2, 0,  1, -2, -1, -2, costs);
        if ((bmv.y >= mvmin.y) & (bmv.y <= mvmax.y))
            COPY1_IF_LT(bcost, (costs[0] << 3) + 5);
        if ((bmv.y - 2 >= mvmin.y) & (bmv.y - 2 <= mvmax.y))
        {
            COPY1_IF_LT(bcost, (costs[1] << 3) + 6);
            COPY1_IF_LT(bcost, (costs[2] << 3) + 7);
        }

        if (bcost & 7)
        {
            int dir = (bcost & 7) - 2;

            if ((bmv.y + hex2[dir + 1].y >= mvmin.y) & (bmv.y + hex2[dir + 1].y <= mvmax.y))
            {
                bmv += hex2[dir + 1];

                /* half hexagon, not overlapping the previous iteration */
                for (int i = (merange >> 1) - 1; i > 0 && bmv.checkRange(mvmin, mvmax); i--)
                {
                    COST_MV_X3_DIR(hex2[dir + 0].x, hex2[dir + 0].y,
                        hex2[dir + 1].x, hex2[dir + 1].y,
                        hex2[dir + 2].x, hex2[dir + 2].y,
                        costs);
                    bcost &= ~7;

                    if ((bmv.y + hex2[dir + 0].y >= mvmin.y) & (bmv.y + hex2[dir + 0].y <= mvmax.y))
                        COPY1_IF_LT(bcost, (costs[0] << 3) + 1);

                    if ((bmv.y + hex2[dir + 1].y >= mvmin.y) & (bmv.y + hex2[dir + 1].y <= mvmax.y))
                        COPY1_IF_LT(bcost, (costs[1] << 3) + 2);

                    if ((bmv.y + hex2[dir + 2].y >= mvmin.y) & (bmv.y + hex2[dir + 2].y <= mvmax.y))
                        COPY1_IF_LT(bcost, (costs[2] << 3) + 3);

                    if (!(bcost & 7))
                        break;

                    dir += (bcost & 7) - 2;
                    dir = mod6m1[dir + 1];
                    bmv += hex2[dir + 1];
                }
            } // if ((bmv.y + hex2[dir + 1].y >= mvmin.y) & (bmv.y + hex2[dir + 1].y <= mvmax.y))
        }
        bcost >>= 3;
#endif // if 0

        /* square refine */
        int dir = 0;
        COST_MV_X4_DIR(0, -1,  0, 1, -1, 0, 1, 0, costs);
        if ((bmv.y - 1 >= mvmin.y) & (bmv.y - 1 <= mvmax.y))
            COPY2_IF_LT(bcost, costs[0], dir, 1);
        if ((bmv.y + 1 >= mvmin.y) & (bmv.y + 1 <= mvmax.y))
            COPY2_IF_LT(bcost, costs[1], dir, 2);
        COPY2_IF_LT(bcost, costs[2], dir, 3);
        COPY2_IF_LT(bcost, costs[3], dir, 4);
        COST_MV_X4_DIR(-1, -1, -1, 1, 1, -1, 1, 1, costs);
        if ((bmv.y - 1 >= mvmin.y) & (bmv.y - 1 <= mvmax.y))
            COPY2_IF_LT(bcost, costs[0], dir, 5);
        if ((bmv.y + 1 >= mvmin.y) & (bmv.y + 1 <= mvmax.y))
            COPY2_IF_LT(bcost, costs[1], dir, 6);
        if ((bmv.y - 1 >= mvmin.y) & (bmv.y - 1 <= mvmax.y))
            COPY2_IF_LT(bcost, costs[2], dir, 7);
        if ((bmv.y + 1 >= mvmin.y) & (bmv.y + 1 <= mvmax.y))
            COPY2_IF_LT(bcost, costs[3], dir, 8);
        bmv += square1[dir];
        break;
    }

    case X265_UMH_SEARCH:
    {
        int ucost1, ucost2;
        int16_t cross_start = 1;

        /* refine predictors */
        omv = bmv;
        ucost1 = bcost;
        X265_CHECK(((pmv.y >= mvmin.y) & (pmv.y <= mvmax.y)), "pmv outside of search range!");
        DIA1_ITER(pmv.x, pmv.y);
        if (pmv.notZero())
            DIA1_ITER(0, 0);

        ucost2 = bcost;
        if (bmv.notZero() && bmv != pmv)
            DIA1_ITER(bmv.x, bmv.y);
        if (bcost == ucost2)
            cross_start = 3;

        /* Early Termination */
        omv = bmv;
        if (bcost == ucost2 && SAD_THRESH(2000))
        {
            COST_MV_X4(0, -2, -1, -1, 1, -1, -2, 0);
            COST_MV_X4(2, 0, -1, 1, 1, 1,  0, 2);
            if (bcost == ucost1 && SAD_THRESH(500))
                break;
            if (bcost == ucost2)
            {
                int16_t range = (int16_t)(merange >> 1) | 1;
                CROSS(3, range, range);
                COST_MV_X4(-1, -2, 1, -2, -2, -1, 2, -1);
                COST_MV_X4(-2, 1, 2, 1, -1, 2, 1, 2);
                if (bcost == ucost2)
                    break;
                cross_start = range + 2;
            }
        }

        // TODO: Need to study x264's logic for building mvc list to understand why they
        //       have special cases here for 16x16, and whether they apply to HEVC CTU

        // adaptive search range based on mvc variability
        if (numCandidates)
        {
            /* range multipliers based on casual inspection of some statistics of
             * average distance between current predictor and final mv found by ESA.
             * these have not been tuned much by actual encoding. */
            static const uint8_t range_mul[4][4] =
            {
                { 3, 3, 4, 4 },
                { 3, 4, 4, 4 },
                { 4, 4, 4, 5 },
                { 4, 4, 5, 6 },
            };

            int mvd;
            int sad_ctx, mvd_ctx;
            int denom = 1;

            if (numCandidates == 1)
            {
                if (LUMA_64x64 == partEnum)
                    /* mvc is probably the same as mvp, so the difference isn't meaningful.
                     * but prediction usually isn't too bad, so just use medium range */
                    mvd = 25;
                else
                    mvd = abs(qmvp.x - mvc[0].x) + abs(qmvp.y - mvc[0].y);
            }
            else
            {
                /* calculate the degree of agreement between predictors. */

                /* in 64x64, mvc includes all the neighbors used to make mvp,
                 * so don't count mvp separately. */

                denom = numCandidates - 1;
                mvd = 0;
                if (partEnum != LUMA_64x64)
                {
                    mvd = abs(qmvp.x - mvc[0].x) + abs(qmvp.y - mvc[0].y);
                    denom++;
                }
                mvd += predictorDifference(mvc, numCandidates);
            }

            sad_ctx = SAD_THRESH(1000) ? 0
                : SAD_THRESH(2000) ? 1
                : SAD_THRESH(4000) ? 2 : 3;
            mvd_ctx = mvd < 10 * denom ? 0
                : mvd < 20 * denom ? 1
                : mvd < 40 * denom ? 2 : 3;

            merange = (merange * range_mul[mvd_ctx][sad_ctx]) >> 2;
        }

        /* FIXME if the above DIA2/OCT2/CROSS found a new mv, it has not updated omx/omy.
         * we are still centered on the same place as the DIA2. is this desirable? */
        CROSS(cross_start, merange, merange >> 1);
        COST_MV_X4(-2, -2, -2, 2, 2, -2, 2, 2);

        /* hexagon grid */
        omv = bmv;
        const uint16_t *p_cost_omvx = m_cost_mvx + omv.x * 4;
        const uint16_t *p_cost_omvy = m_cost_mvy + omv.y * 4;
        uint16_t i = 1;
        do
        {
            if (4 * i > X265_MIN4(mvmax.x - omv.x, omv.x - mvmin.x,
                                  mvmax.y - omv.y, omv.y - mvmin.y))
            {
                for (int j = 0; j < 16; j++)
                {
                    MV mv = omv + (hex4[j] * i);
                    if (mv.checkRange(mvmin, mvmax))
                        COST_MV(mv.x, mv.y);
                }
            }
            else
            {
                int16_t dir = 0;
                pixel *fref_base = fref + omv.x + (omv.y - 4 * i) * stride;
                size_t dy = (size_t)i * stride;
#define SADS(k, x0, y0, x1, y1, x2, y2, x3, y3) \
    sad_x4(fenc, \
           fref_base x0 * i + (y0 - 2 * k + 4) * dy, \
           fref_base x1 * i + (y1 - 2 * k + 4) * dy, \
           fref_base x2 * i + (y2 - 2 * k + 4) * dy, \
           fref_base x3 * i + (y3 - 2 * k + 4) * dy, \
           stride, costs + 4 * k); \
    fref_base += 2 * dy;
#define ADD_MVCOST(k, x, y) costs[k] += p_cost_omvx[x * 4 * i] + p_cost_omvy[y * 4 * i]
#define MIN_MV(k, dx, dy)     if ((omv.y + (dy) >= mvmin.y) & (omv.y + (dy) <= mvmax.y)) { COPY2_IF_LT(bcost, costs[k], dir, dx * 16 + (dy & 15)) }

                SADS(0, +0, -4, +0, +4, -2, -3, +2, -3);
                SADS(1, -4, -2, +4, -2, -4, -1, +4, -1);
                SADS(2, -4, +0, +4, +0, -4, +1, +4, +1);
                SADS(3, -4, +2, +4, +2, -2, +3, +2, +3);
                ADD_MVCOST(0, 0, -4);
                ADD_MVCOST(1, 0, 4);
                ADD_MVCOST(2, -2, -3);
                ADD_MVCOST(3, 2, -3);
                ADD_MVCOST(4, -4, -2);
                ADD_MVCOST(5, 4, -2);
                ADD_MVCOST(6, -4, -1);
                ADD_MVCOST(7, 4, -1);
                ADD_MVCOST(8, -4, 0);
                ADD_MVCOST(9, 4, 0);
                ADD_MVCOST(10, -4, 1);
                ADD_MVCOST(11, 4, 1);
                ADD_MVCOST(12, -4, 2);
                ADD_MVCOST(13, 4, 2);
                ADD_MVCOST(14, -2, 3);
                ADD_MVCOST(15, 2, 3);
                MIN_MV(0, 0, -4);
                MIN_MV(1, 0, 4);
                MIN_MV(2, -2, -3);
                MIN_MV(3, 2, -3);
                MIN_MV(4, -4, -2);
                MIN_MV(5, 4, -2);
                MIN_MV(6, -4, -1);
                MIN_MV(7, 4, -1);
                MIN_MV(8, -4, 0);
                MIN_MV(9, 4, 0);
                MIN_MV(10, -4, 1);
                MIN_MV(11, 4, 1);
                MIN_MV(12, -4, 2);
                MIN_MV(13, 4, 2);
                MIN_MV(14, -2, 3);
                MIN_MV(15, 2, 3);
#undef SADS
#undef ADD_MVCOST
#undef MIN_MV
                if (dir)
                {
                    bmv.x = omv.x + i * (dir >> 4);
                    bmv.y = omv.y + i * ((dir << 28) >> 28);
                }
            }
        }
        while (++i <= merange >> 2);
        if (bmv.checkRange(mvmin, mvmax))
            goto me_hex2;
        break;
    }

    case X265_STAR_SEARCH: // Adapted from HM ME
    {
        int bPointNr = 0;
        int bDistance = 0;

        const int EarlyExitIters = 3;
        StarPatternSearch(ref, mvmin, mvmax, bmv, bcost, bPointNr, bDistance, EarlyExitIters, merange);
        if (bDistance == 1)
        {
            // if best distance was only 1, check two missing points.  If no new point is found, stop
            if (bPointNr)
            {
                /* For a given direction 1 to 8, check nearest two outer X pixels
                     X   X
                   X 1 2 3 X
                     4 * 5
                   X 6 7 8 X
                     X   X
                */
                int saved = bcost;
                const MV mv1 = bmv + offsets[(bPointNr - 1) * 2];
                const MV mv2 = bmv + offsets[(bPointNr - 1) * 2 + 1];
                if (mv1.checkRange(mvmin, mvmax))
                {
                    COST_MV(mv1.x, mv1.y);
                }
                if (mv2.checkRange(mvmin, mvmax))
                {
                    COST_MV(mv2.x, mv2.y);
                }
                if (bcost == saved)
                    break;
            }
            else
                break;
        }

        const int RasterDistance = 5;
        if (bDistance > RasterDistance)
        {
            // raster search refinement if original search distance was too big
            MV tmv;
            for (tmv.y = mvmin.y; tmv.y <= mvmax.y; tmv.y += RasterDistance)
            {
                for (tmv.x = mvmin.x; tmv.x <= mvmax.x; tmv.x += RasterDistance)
                {
                    if (tmv.x + (RasterDistance * 3) <= mvmax.x)
                    {
                        pixel *pix_base = fref + tmv.y * stride + tmv.x;
                        sad_x4(fenc,
                               pix_base,
                               pix_base + RasterDistance,
                               pix_base + RasterDistance * 2,
                               pix_base + RasterDistance * 3,
                               stride, costs);
                        costs[0] += mvcost(tmv << 2);
                        COPY2_IF_LT(bcost, costs[0], bmv, tmv);
                        tmv.x += RasterDistance;
                        costs[1] += mvcost(tmv << 2);
                        COPY2_IF_LT(bcost, costs[1], bmv, tmv);
                        tmv.x += RasterDistance;
                        costs[2] += mvcost(tmv << 2);
                        COPY2_IF_LT(bcost, costs[2], bmv, tmv);
                        tmv.x += RasterDistance;
                        costs[3] += mvcost(tmv << 3);
                        COPY2_IF_LT(bcost, costs[3], bmv, tmv);
                    }
                    else
                        COST_MV(tmv.x, tmv.y);
                }
            }
        }

        while (bDistance > 0)
        {
            // center a new search around current best
            bDistance = 0;
            bPointNr = 0;
            const int MaxIters = 32;
            StarPatternSearch(ref, mvmin, mvmax, bmv, bcost, bPointNr, bDistance, MaxIters, merange);

            if (bDistance == 1)
            {
                if (!bPointNr)
                    break;

                /* For a given direction 1 to 8, check nearest 2 outer X pixels
                        X   X
                    X 1 2 3 X
                        4 * 5
                    X 6 7 8 X
                        X   X
                */
                const MV mv1 = bmv + offsets[(bPointNr - 1) * 2];
                const MV mv2 = bmv + offsets[(bPointNr - 1) * 2 + 1];
                if (mv1.checkRange(mvmin, mvmax))
                {
                    COST_MV(mv1.x, mv1.y);
                }
                if (mv2.checkRange(mvmin, mvmax))
                {
                    COST_MV(mv2.x, mv2.y);
                }
                break;
            }
        }

        break;
    }

    case X265_SEA:
    {
        // Successive Elimination Algorithm
        const int16_t minX = X265_MAX(omv.x - (int16_t)merange, mvmin.x);
        const int16_t minY = X265_MAX(omv.y - (int16_t)merange, mvmin.y);
        const int16_t maxX = X265_MIN(omv.x + (int16_t)merange, mvmax.x);
        const int16_t maxY = X265_MIN(omv.y + (int16_t)merange, mvmax.y);
        const uint16_t *p_cost_mvx = m_cost_mvx - qmvp.x;
        const uint16_t *p_cost_mvy = m_cost_mvy - qmvp.y;
        int16_t* meScratchBuffer = NULL;
        int scratchSize = merange * 2 + 4;
        if (scratchSize)
        {
            meScratchBuffer = X265_MALLOC(int16_t, scratchSize);
            memset(meScratchBuffer, 0, sizeof(int16_t)* scratchSize);
        }

        /* SEA is fastest in multiples of 4 */
        int meRangeWidth = (maxX - minX + 3) & ~3;
        int w = 0, h = 0;                    // Width and height of the PU
        ALIGN_VAR_32(pixel, zero[64 * FENC_STRIDE]) = { 0 };
        ALIGN_VAR_32(int, encDC[4]);
        uint16_t *fpelCostMvX = m_fpelMvCosts[-qmvp.x & 3] + (-qmvp.x >> 2);
        sizesFromPartition(partEnum, &w, &h);
        int deltaX = (w <= 8) ? (w) : (w >> 1);
        int deltaY = (h <= 8) ? (h) : (h >> 1);

        /* Check if very small rectangular blocks which cannot be sub-divided anymore */
        bool smallRectPartition = partEnum == LUMA_4x4 || partEnum == LUMA_16x12 ||
            partEnum == LUMA_12x16 || partEnum == LUMA_16x4 || partEnum == LUMA_4x16;
        /* Check if vertical partition */
        bool verticalRect = partEnum == LUMA_32x64 || partEnum == LUMA_16x32 || partEnum == LUMA_8x16 ||
            partEnum == LUMA_4x8;
        /* Check if horizontal partition */
        bool horizontalRect = partEnum == LUMA_64x32 || partEnum == LUMA_32x16 || partEnum == LUMA_16x8 ||
            partEnum == LUMA_8x4;
        /* Check if assymetric vertical partition */
        bool assymetricVertical = partEnum == LUMA_12x16 || partEnum == LUMA_4x16 || partEnum == LUMA_24x32 ||
            partEnum == LUMA_8x32 || partEnum == LUMA_48x64 || partEnum == LUMA_16x64;
        /* Check if assymetric horizontal partition */
        bool assymetricHorizontal = partEnum == LUMA_16x12 || partEnum == LUMA_16x4 || partEnum == LUMA_32x24 ||
            partEnum == LUMA_32x8 || partEnum == LUMA_64x48 || partEnum == LUMA_64x16;

        int tempPartEnum = 0;

        /* If a vertical rectangular partition, it is horizontally split into two, for ads_x2() */
        if (verticalRect)
            tempPartEnum = partitionFromSizes(w, h >> 1);
        /* If a horizontal rectangular partition, it is vertically split into two, for ads_x2() */
        else if (horizontalRect)
            tempPartEnum = partitionFromSizes(w >> 1, h);
        /* We have integral planes introduced to account for assymetric partitions.
         * Hence all assymetric partitions except those which cannot be split into legal sizes,
         * are split into four for ads_x4() */
        else if (assymetricVertical || assymetricHorizontal)
            tempPartEnum = smallRectPartition ? partEnum : partitionFromSizes(w >> 1, h >> 1);
        /* General case: Square partitions. All partitions with width > 8 are split into four
         * for ads_x4(), for 4x4 and 8x8 we do ads_x1() */
        else
            tempPartEnum = (w <= 8) ? partEnum : partitionFromSizes(w >> 1, h >> 1);

        /* Successive elimination by comparing DC before a full SAD,
         * because sum(abs(diff)) >= abs(diff(sum)). */
        primitives.pu[tempPartEnum].sad_x4(zero,
                         fenc,
                         fenc + deltaX,
                         fenc + deltaY * FENC_STRIDE,
                         fenc + deltaX + deltaY * FENC_STRIDE,
                         FENC_STRIDE,
                         encDC);

        /* Assigning appropriate integral plane */
        uint32_t *sumsBase = NULL;
        switch (deltaX)
        {
            case 32: if (deltaY % 24 == 0)
                         sumsBase = integral[1];
                     else if (deltaY == 8)
                         sumsBase = integral[2];
                     else
                         sumsBase = integral[0];
               break;
            case 24: sumsBase = integral[3];
               break;
            case 16: if (deltaY % 12 == 0)
                         sumsBase = integral[5];
                     else if (deltaY == 4)
                         sumsBase = integral[6];
                     else
                         sumsBase = integral[4];
               break;
            case 12: sumsBase = integral[7];
                break;
            case 8: if (deltaY == 32)
                        sumsBase = integral[8];
                    else
                        sumsBase = integral[9];
                break;
            case 4: if (deltaY == 16)
                        sumsBase = integral[10];
                    else
                        sumsBase = integral[11];
                break;
            default: sumsBase = integral[11];
                break;
        }

        if (partEnum == LUMA_64x64 || partEnum == LUMA_32x32 || partEnum == LUMA_16x16 ||
            partEnum == LUMA_32x64 || partEnum == LUMA_16x32 || partEnum == LUMA_8x16 ||
            partEnum == LUMA_4x8 || partEnum == LUMA_12x16 || partEnum == LUMA_4x16 ||
            partEnum == LUMA_24x32 || partEnum == LUMA_8x32 || partEnum == LUMA_48x64 ||
            partEnum == LUMA_16x64)
            deltaY *= (int)stride;

        if (verticalRect)
            encDC[1] = encDC[2];

        if (horizontalRect)
            deltaY = deltaX;

        /* ADS and SAD */
        MV tmv;
        for (tmv.y = minY; tmv.y <= maxY; tmv.y++)
        {
            int i, xn;
            int ycost = p_cost_mvy[tmv.y] << 2;
            if (bcost <= ycost)
                continue;
            bcost -= ycost;

            /* ADS_4 for 16x16, 32x32, 64x64, 24x32, 32x24, 48x64, 64x48, 32x8, 8x32, 64x16, 16x64 partitions
             * ADS_1 for 4x4, 8x8, 16x4, 4x16, 16x12, 12x16 partitions
             * ADS_2 for all other rectangular partitions */
            xn = ads(encDC,
                    sumsBase + minX + tmv.y * stride,
                    deltaY,
                    fpelCostMvX + minX,
                    meScratchBuffer,
                    meRangeWidth,
                    bcost);

            for (i = 0; i < xn - 2; i += 3)
                COST_MV_X3_ABS(minX + meScratchBuffer[i], tmv.y,
                             minX + meScratchBuffer[i + 1], tmv.y,
                             minX + meScratchBuffer[i + 2], tmv.y);

            bcost += ycost;
            for (; i < xn; i++)
                COST_MV(minX + meScratchBuffer[i], tmv.y);
        }
        if (meScratchBuffer)
            x265_free(meScratchBuffer);
        break;
    }

    case X265_FULL_SEARCH:
    {
        // dead slow exhaustive search, but at least it uses sad_x4()
        MV tmv;
        for (tmv.y = mvmin.y; tmv.y <= mvmax.y; tmv.y++)
        {
            for (tmv.x = mvmin.x; tmv.x <= mvmax.x; tmv.x++)
            {
                if (tmv.x + 3 <= mvmax.x)
                {
                    pixel *pix_base = fref + tmv.y * stride + tmv.x;
                    sad_x4(fenc,
                           pix_base,
                           pix_base + 1,
                           pix_base + 2,
                           pix_base + 3,
                           stride, costs);
                    costs[0] += mvcost(tmv << 2);
                    COPY2_IF_LT(bcost, costs[0], bmv, tmv);
                    tmv.x++;
                    costs[1] += mvcost(tmv << 2);
                    COPY2_IF_LT(bcost, costs[1], bmv, tmv);
                    tmv.x++;
                    costs[2] += mvcost(tmv << 2);
                    COPY2_IF_LT(bcost, costs[2], bmv, tmv);
                    tmv.x++;
                    costs[3] += mvcost(tmv << 2);
                    COPY2_IF_LT(bcost, costs[3], bmv, tmv);
                }
                else
                    COST_MV(tmv.x, tmv.y);
            }
        }

        break;
    }

    default:
        X265_CHECK(0, "invalid motion estimate mode\n");
        break;
    }

    if (bprecost < bcost)
    {
        bmv = bestpre;
        bcost = bprecost;
    }
    else
        bmv = bmv.toQPel(); // promote search bmv to qpel

    const SubpelWorkload& wl = workload[this->subpelRefine];

    // check mv range for slice bound
    if ((maxSlices > 1) & ((bmv.y < qmvmin.y) | (bmv.y > qmvmax.y)))
    {
        bmv.y = x265_min(x265_max(bmv.y, qmvmin.y), qmvmax.y);
        bcost = subpelCompare(ref, bmv, satd) + mvcost(bmv);
    }

    if (!bcost)
    {
        /* if there was zero residual at the clipped MVP, we can skip subpel
         * refine, but we do need to include the mvcost in the returned cost */
        bcost = mvcost(bmv);
    }
    else if (ref->isLowres)
    {
        int bdir = 0;
        for (int i = 1; i <= wl.hpel_dirs; i++)
        {
            MV qmv = bmv + square1[i] * 2;

            /* skip invalid range */
            if ((qmv.y < qmvmin.y) | (qmv.y > qmvmax.y))
                continue;

            int cost = ref->lowresQPelCost(fenc, blockOffset, qmv, sad) + mvcost(qmv);
            COPY2_IF_LT(bcost, cost, bdir, i);
        }

        bmv += square1[bdir] * 2;
        bcost = ref->lowresQPelCost(fenc, blockOffset, bmv, satd) + mvcost(bmv);

        bdir = 0;
        for (int i = 1; i <= wl.qpel_dirs; i++)
        {
            MV qmv = bmv + square1[i];

            /* skip invalid range */
            if ((qmv.y < qmvmin.y) | (qmv.y > qmvmax.y))
                continue;

            int cost = ref->lowresQPelCost(fenc, blockOffset, qmv, satd) + mvcost(qmv);
            COPY2_IF_LT(bcost, cost, bdir, i);
        }

        bmv += square1[bdir];
    }
    else
    {
        pixelcmp_t hpelcomp;

        if (wl.hpel_satd)
        {
            bcost = subpelCompare(ref, bmv, satd) + mvcost(bmv);
            hpelcomp = satd;
        }
        else
            hpelcomp = sad;

        for (int iter = 0; iter < wl.hpel_iters; iter++)
        {
            int bdir = 0;
            for (int i = 1; i <= wl.hpel_dirs; i++)
            {
                MV qmv = bmv + square1[i] * 2;

                // check mv range for slice bound
                if ((qmv.y < qmvmin.y) | (qmv.y > qmvmax.y))
                    continue;

                int cost = subpelCompare(ref, qmv, hpelcomp) + mvcost(qmv);
                COPY2_IF_LT(bcost, cost, bdir, i);
            }

            if (bdir)
                bmv += square1[bdir] * 2;
            else
                break;
        }

        /* if HPEL search used SAD, remeasure with SATD before QPEL */
        if (!wl.hpel_satd)
            bcost = subpelCompare(ref, bmv, satd) + mvcost(bmv);

        for (int iter = 0; iter < wl.qpel_iters; iter++)
        {
            int bdir = 0;
            for (int i = 1; i <= wl.qpel_dirs; i++)
            {
                MV qmv = bmv + square1[i];

                // check mv range for slice bound
                if ((qmv.y < qmvmin.y) | (qmv.y > qmvmax.y))
                    continue;

                int cost = subpelCompare(ref, qmv, satd) + mvcost(qmv);
                COPY2_IF_LT(bcost, cost, bdir, i);
            }

            if (bdir)
                bmv += square1[bdir];
            else
                break;
        }
    }

    // check mv range for slice bound
    X265_CHECK(((bmv.y >= qmvmin.y) & (bmv.y <= qmvmax.y)), "mv beyond range!");

    x265_emms();
    outQMv = bmv;
    return bcost;
}

int MotionEstimate::subpelCompare(ReferencePlanes *ref, const MV& qmv, pixelcmp_t cmp)
{
    intptr_t refStride = ref->lumaStride;
    const pixel* fref = ref->fpelPlane[0] + blockOffset + (qmv.x >> 2) + (qmv.y >> 2) * refStride;
    int xFrac = qmv.x & 0x3;
    int yFrac = qmv.y & 0x3;
    int cost;
    const intptr_t fencStride = FENC_STRIDE;
    X265_CHECK(fencPUYuv.m_size == FENC_STRIDE, "fenc buffer is assumed to have FENC_STRIDE by sad_x3 and sad_x4\n");

    ALIGN_VAR_32(pixel, subpelbuf[MAX_CU_SIZE * MAX_CU_SIZE]);
    
    if (!(yFrac | xFrac))
        cost = cmp(fencPUYuv.m_buf[0], fencStride, fref, refStride);
    else
    {
        /* we are taking a short-cut here if the reference is weighted. To be
         * accurate we should be interpolating unweighted pixels and weighting
         * the final 16bit values prior to rounding and down shifting. Instead we
         * are simply interpolating the weighted full-pel pixels. Not 100%
         * accurate but good enough for fast qpel ME */
        if (!yFrac)
            primitives.pu[partEnum].luma_hpp(fref, refStride, subpelbuf, blockwidth, xFrac);
        else if (!xFrac)
            primitives.pu[partEnum].luma_vpp(fref, refStride, subpelbuf, blockwidth, yFrac);
        else
            primitives.pu[partEnum].luma_hvpp(fref, refStride, subpelbuf, blockwidth, xFrac, yFrac);
        cost = cmp(fencPUYuv.m_buf[0], fencStride, subpelbuf, blockwidth);
    }

    if (bChromaSATD)
    {
        int csp    = fencPUYuv.m_csp;
        int hshift = fencPUYuv.m_hChromaShift;
        int vshift = fencPUYuv.m_vChromaShift;
        int mvx = qmv.x << (1 - hshift);
        int mvy = qmv.y << (1 - vshift);
        intptr_t fencStrideC = fencPUYuv.m_csize;

        intptr_t refStrideC = ref->reconPic->m_strideC;
        intptr_t refOffset = (mvx >> 3) + (mvy >> 3) * refStrideC;

        const pixel* refCb = ref->getCbAddr(ctuAddr, absPartIdx) + refOffset;
        const pixel* refCr = ref->getCrAddr(ctuAddr, absPartIdx) + refOffset;

        X265_CHECK((hshift == 0) || (hshift == 1), "hshift must be 0 or 1\n");
        X265_CHECK((vshift == 0) || (vshift == 1), "vshift must be 0 or 1\n");

        xFrac = mvx & 7;
        yFrac = mvy & 7;

        if (!(yFrac | xFrac))
        {
            cost += chromaSatd(fencPUYuv.m_buf[1], fencStrideC, refCb, refStrideC);
            cost += chromaSatd(fencPUYuv.m_buf[2], fencStrideC, refCr, refStrideC);
        }
        else
        {
            int blockwidthC = blockwidth >> hshift;

            if (!yFrac)
            {
                primitives.chroma[csp].pu[partEnum].filter_hpp(refCb, refStrideC, subpelbuf, blockwidthC, xFrac);
                cost += chromaSatd(fencPUYuv.m_buf[1], fencStrideC, subpelbuf, blockwidthC);

                primitives.chroma[csp].pu[partEnum].filter_hpp(refCr, refStrideC, subpelbuf, blockwidthC, xFrac);
                cost += chromaSatd(fencPUYuv.m_buf[2], fencStrideC, subpelbuf, blockwidthC);
            }
            else if (!xFrac)
            {
                primitives.chroma[csp].pu[partEnum].filter_vpp(refCb, refStrideC, subpelbuf, blockwidthC, yFrac);
                cost += chromaSatd(fencPUYuv.m_buf[1], fencStrideC, subpelbuf, blockwidthC);

                primitives.chroma[csp].pu[partEnum].filter_vpp(refCr, refStrideC, subpelbuf, blockwidthC, yFrac);
                cost += chromaSatd(fencPUYuv.m_buf[2], fencStrideC, subpelbuf, blockwidthC);
            }
            else
            {
                ALIGN_VAR_32(int16_t, immed[MAX_CU_SIZE * (MAX_CU_SIZE + NTAPS_LUMA - 1)]);
                const int halfFilterSize = (NTAPS_CHROMA >> 1);

                primitives.chroma[csp].pu[partEnum].filter_hps(refCb, refStrideC, immed, blockwidthC, xFrac, 1);
                primitives.chroma[csp].pu[partEnum].filter_vsp(immed + (halfFilterSize - 1) * blockwidthC, blockwidthC, subpelbuf, blockwidthC, yFrac);
                cost += chromaSatd(fencPUYuv.m_buf[1], fencStrideC, subpelbuf, blockwidthC);

                primitives.chroma[csp].pu[partEnum].filter_hps(refCr, refStrideC, immed, blockwidthC, xFrac, 1);
                primitives.chroma[csp].pu[partEnum].filter_vsp(immed + (halfFilterSize - 1) * blockwidthC, blockwidthC, subpelbuf, blockwidthC, yFrac);
                cost += chromaSatd(fencPUYuv.m_buf[2], fencStrideC, subpelbuf, blockwidthC);
            }
        }
    }

    return cost;
}
