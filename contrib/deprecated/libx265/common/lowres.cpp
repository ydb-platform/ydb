/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Gopu Govindaswamy <gopu@multicorewareinc.com>
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

#include "picyuv.h"
#include "lowres.h"
#include "mv.h"

using namespace X265_NS;

bool Lowres::create(PicYuv *origPic, int _bframes, bool bAQEnabled, uint32_t qgSize)
{
    isLowres = true;
    bframes = _bframes;
    width = origPic->m_picWidth / 2;
    lines = origPic->m_picHeight / 2;
    lumaStride = width + 2 * origPic->m_lumaMarginX;
    if (lumaStride & 31)
        lumaStride += 32 - (lumaStride & 31);
    maxBlocksInRow = (width + X265_LOWRES_CU_SIZE - 1) >> X265_LOWRES_CU_BITS;
    maxBlocksInCol = (lines + X265_LOWRES_CU_SIZE - 1) >> X265_LOWRES_CU_BITS;
    maxBlocksInRowFullRes = maxBlocksInRow * 2;
    maxBlocksInColFullRes = maxBlocksInCol * 2;
    int cuCount = maxBlocksInRow * maxBlocksInCol;
    int cuCountFullRes;
    if (qgSize == 8)
        cuCountFullRes = maxBlocksInRowFullRes * maxBlocksInColFullRes;
    else
        cuCountFullRes = cuCount;

    /* rounding the width to multiple of lowres CU size */
    width = maxBlocksInRow * X265_LOWRES_CU_SIZE;
    lines = maxBlocksInCol * X265_LOWRES_CU_SIZE;

    size_t planesize = lumaStride * (lines + 2 * origPic->m_lumaMarginY);
    size_t padoffset = lumaStride * origPic->m_lumaMarginY + origPic->m_lumaMarginX;
    if (bAQEnabled)
    {
        CHECKED_MALLOC_ZERO(qpAqOffset, double, cuCountFullRes);
        CHECKED_MALLOC_ZERO(qpAqMotionOffset, double, cuCountFullRes);
        CHECKED_MALLOC_ZERO(invQscaleFactor, int, cuCountFullRes);
        CHECKED_MALLOC_ZERO(qpCuTreeOffset, double, cuCountFullRes);
        CHECKED_MALLOC_ZERO(blockVariance, uint32_t, cuCountFullRes);
        if (qgSize == 8)
            CHECKED_MALLOC_ZERO(invQscaleFactor8x8, int, cuCount);
    }
    CHECKED_MALLOC(propagateCost, uint16_t, cuCount);

    /* allocate lowres buffers */
    CHECKED_MALLOC_ZERO(buffer[0], pixel, 4 * planesize);

    buffer[1] = buffer[0] + planesize;
    buffer[2] = buffer[1] + planesize;
    buffer[3] = buffer[2] + planesize;

    lowresPlane[0] = buffer[0] + padoffset;
    lowresPlane[1] = buffer[1] + padoffset;
    lowresPlane[2] = buffer[2] + padoffset;
    lowresPlane[3] = buffer[3] + padoffset;

    CHECKED_MALLOC(intraCost, int32_t, cuCount);
    CHECKED_MALLOC(intraMode, uint8_t, cuCount);

    for (int i = 0; i < bframes + 2; i++)
    {
        for (int j = 0; j < bframes + 2; j++)
        {
            CHECKED_MALLOC(rowSatds[i][j], int32_t, maxBlocksInCol);
            CHECKED_MALLOC(lowresCosts[i][j], uint16_t, cuCount);
        }
    }

    for (int i = 0; i < bframes + 1; i++)
    {
        CHECKED_MALLOC(lowresMvs[0][i], MV, cuCount);
        CHECKED_MALLOC(lowresMvs[1][i], MV, cuCount);
        CHECKED_MALLOC(lowresMvCosts[0][i], int32_t, cuCount);
        CHECKED_MALLOC(lowresMvCosts[1][i], int32_t, cuCount);
    }

    return true;

fail:
    return false;
}

void Lowres::destroy()
{
    X265_FREE(buffer[0]);
    X265_FREE(intraCost);
    X265_FREE(intraMode);

    for (int i = 0; i < bframes + 2; i++)
    {
        for (int j = 0; j < bframes + 2; j++)
        {
            X265_FREE(rowSatds[i][j]);
            X265_FREE(lowresCosts[i][j]);
        }
    }

    for (int i = 0; i < bframes + 1; i++)
    {
        X265_FREE(lowresMvs[0][i]);
        X265_FREE(lowresMvs[1][i]);
        X265_FREE(lowresMvCosts[0][i]);
        X265_FREE(lowresMvCosts[1][i]);
    }
    X265_FREE(qpAqOffset);
    X265_FREE(qpAqMotionOffset);
    X265_FREE(invQscaleFactor);
    X265_FREE(qpCuTreeOffset);
    X265_FREE(propagateCost);
    X265_FREE(blockVariance);
    X265_FREE(invQscaleFactor8x8);
}

// (re) initialize lowres state
void Lowres::init(PicYuv *origPic, int poc)
{
    bLastMiniGopBFrame = false;
    bKeyframe = false; // Not a keyframe unless identified by lookahead
    frameNum = poc;
    leadingBframes = 0;
    indB = 0;
    memset(costEst, -1, sizeof(costEst));
    memset(weightedCostDelta, 0, sizeof(weightedCostDelta));

    if (qpAqOffset && invQscaleFactor)
        memset(costEstAq, -1, sizeof(costEstAq));

    for (int y = 0; y < bframes + 2; y++)
        for (int x = 0; x < bframes + 2; x++)
            rowSatds[y][x][0] = -1;

    for (int i = 0; i < bframes + 1; i++)
    {
        lowresMvs[0][i][0].x = 0x7FFF;
        lowresMvs[1][i][0].x = 0x7FFF;
    }

    for (int i = 0; i < bframes + 2; i++)
        intraMbs[i] = 0;
    if (origPic->m_param->rc.vbvBufferSize)
        for (int i = 0; i < X265_LOOKAHEAD_MAX + 1; i++)
            plannedType[i] = X265_TYPE_AUTO;

    /* downscale and generate 4 hpel planes for lookahead */
    primitives.frameInitLowres(origPic->m_picOrg[0],
                               lowresPlane[0], lowresPlane[1], lowresPlane[2], lowresPlane[3],
                               origPic->m_stride, lumaStride, width, lines);

    /* extend hpel planes for motion search */
    extendPicBorder(lowresPlane[0], lumaStride, width, lines, origPic->m_lumaMarginX, origPic->m_lumaMarginY);
    extendPicBorder(lowresPlane[1], lumaStride, width, lines, origPic->m_lumaMarginX, origPic->m_lumaMarginY);
    extendPicBorder(lowresPlane[2], lumaStride, width, lines, origPic->m_lumaMarginX, origPic->m_lumaMarginY);
    extendPicBorder(lowresPlane[3], lumaStride, width, lines, origPic->m_lumaMarginX, origPic->m_lumaMarginY);
    fpelPlane[0] = lowresPlane[0];
}
