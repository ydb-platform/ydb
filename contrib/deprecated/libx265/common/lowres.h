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

#ifndef X265_LOWRES_H
#define X265_LOWRES_H

#include "primitives.h"
#include "common.h"
#include "picyuv.h"
#include "mv.h"

namespace X265_NS {
// private namespace

struct ReferencePlanes
{
    ReferencePlanes() { memset(this, 0, sizeof(ReferencePlanes)); }

    pixel*   fpelPlane[3];
    pixel*   lowresPlane[4];
    PicYuv*  reconPic;

    bool     isWeighted;
    bool     isLowres;

    intptr_t lumaStride;
    intptr_t chromaStride;

    struct {
        int      weight;
        int      offset;
        int      shift;
        int      round;
    } w[3];

    pixel* getLumaAddr(uint32_t ctuAddr, uint32_t absPartIdx) { return fpelPlane[0] + reconPic->m_cuOffsetY[ctuAddr] + reconPic->m_buOffsetY[absPartIdx]; }
    pixel* getCbAddr(uint32_t ctuAddr, uint32_t absPartIdx)   { return fpelPlane[1] + reconPic->m_cuOffsetC[ctuAddr] + reconPic->m_buOffsetC[absPartIdx]; }
    pixel* getCrAddr(uint32_t ctuAddr, uint32_t absPartIdx)   { return fpelPlane[2] + reconPic->m_cuOffsetC[ctuAddr] + reconPic->m_buOffsetC[absPartIdx]; }

    /* lowres motion compensation, you must provide a buffer and stride for QPEL averaged pixels
     * in case QPEL is required.  Else it returns a pointer to the HPEL pixels */
    inline pixel *lowresMC(intptr_t blockOffset, const MV& qmv, pixel *buf, intptr_t& outstride)
    {
        if ((qmv.x | qmv.y) & 1)
        {
            int hpelA = (qmv.y & 2) | ((qmv.x & 2) >> 1);
            pixel *frefA = lowresPlane[hpelA] + blockOffset + (qmv.x >> 2) + (qmv.y >> 2) * lumaStride;
            int qmvx = qmv.x + (qmv.x & 1);
            int qmvy = qmv.y + (qmv.y & 1);
            int hpelB = (qmvy & 2) | ((qmvx & 2) >> 1);
            pixel *frefB = lowresPlane[hpelB] + blockOffset + (qmvx >> 2) + (qmvy >> 2) * lumaStride;
            primitives.pu[LUMA_8x8].pixelavg_pp(buf, outstride, frefA, lumaStride, frefB, lumaStride, 32);
            return buf;
        }
        else
        {
            outstride = lumaStride;
            int hpel = (qmv.y & 2) | ((qmv.x & 2) >> 1);
            return lowresPlane[hpel] + blockOffset + (qmv.x >> 2) + (qmv.y >> 2) * lumaStride;
        }
    }

    inline int lowresQPelCost(pixel *fenc, intptr_t blockOffset, const MV& qmv, pixelcmp_t comp)
    {
        if ((qmv.x | qmv.y) & 1)
        {
            ALIGN_VAR_16(pixel, subpelbuf[8 * 8]);
            int hpelA = (qmv.y & 2) | ((qmv.x & 2) >> 1);
            pixel *frefA = lowresPlane[hpelA] + blockOffset + (qmv.x >> 2) + (qmv.y >> 2) * lumaStride;
            int qmvx = qmv.x + (qmv.x & 1);
            int qmvy = qmv.y + (qmv.y & 1);
            int hpelB = (qmvy & 2) | ((qmvx & 2) >> 1);
            pixel *frefB = lowresPlane[hpelB] + blockOffset + (qmvx >> 2) + (qmvy >> 2) * lumaStride;
            primitives.pu[LUMA_8x8].pixelavg_pp(subpelbuf, 8, frefA, lumaStride, frefB, lumaStride, 32);
            return comp(fenc, FENC_STRIDE, subpelbuf, 8);
        }
        else
        {
            int hpel = (qmv.y & 2) | ((qmv.x & 2) >> 1);
            pixel *fref = lowresPlane[hpel] + blockOffset + (qmv.x >> 2) + (qmv.y >> 2) * lumaStride;
            return comp(fenc, FENC_STRIDE, fref, lumaStride);
        }
    }
};

/* lowres buffers, sizes and strides */
struct Lowres : public ReferencePlanes
{
    pixel *buffer[4];

    int    frameNum;         // Presentation frame number
    int    sliceType;        // Slice type decided by lookahead
    int    width;            // width of lowres frame in pixels
    int    lines;            // height of lowres frame in pixel lines
    int    leadingBframes;   // number of leading B frames for P or I

    bool   bScenecut;        // Set to false if the frame cannot possibly be part of a real scenecut.
    bool   bKeyframe;
    bool   bLastMiniGopBFrame;

    double ipCostRatio;

    /* lookahead output data */
    int64_t   costEst[X265_BFRAME_MAX + 2][X265_BFRAME_MAX + 2];
    int64_t   costEstAq[X265_BFRAME_MAX + 2][X265_BFRAME_MAX + 2];
    int32_t*  rowSatds[X265_BFRAME_MAX + 2][X265_BFRAME_MAX + 2];
    int       intraMbs[X265_BFRAME_MAX + 2];
    int32_t*  intraCost;
    uint8_t*  intraMode;
    int64_t   satdCost;
    uint16_t* lowresCostForRc;
    uint16_t(*lowresCosts[X265_BFRAME_MAX + 2][X265_BFRAME_MAX + 2]);
    int32_t*  lowresMvCosts[2][X265_BFRAME_MAX + 1];
    MV*       lowresMvs[2][X265_BFRAME_MAX + 1];
    uint32_t  maxBlocksInRow;
    uint32_t  maxBlocksInCol;
    uint32_t  maxBlocksInRowFullRes;
    uint32_t  maxBlocksInColFullRes;

    /* used for vbvLookahead */
    int       plannedType[X265_LOOKAHEAD_MAX + 1];
    int64_t   plannedSatd[X265_LOOKAHEAD_MAX + 1];
    int       indB;
    int       bframes;

    /* rate control / adaptive quant data */
    double*   qpAqOffset;      // AQ QP offset values for each 16x16 CU
    double*   qpCuTreeOffset;  // cuTree QP offset values for each 16x16 CU
    double*   qpAqMotionOffset;
    int*      invQscaleFactor; // qScale values for qp Aq Offsets
    int*      invQscaleFactor8x8; // temporary buffer for qg-size 8
    uint32_t* blockVariance;
    uint64_t  wp_ssd[3];       // This is different than SSDY, this is sum(pixel^2) - sum(pixel)^2 for entire frame
    uint64_t  wp_sum[3];
    uint64_t  frameVariance;

    /* cutree intermediate data */
    uint16_t* propagateCost;
    double    weightedCostDelta[X265_BFRAME_MAX + 2];
    ReferencePlanes weightedRef[X265_BFRAME_MAX + 2];

    bool create(PicYuv *origPic, int _bframes, bool bAqEnabled, uint32_t qgSize);
    void destroy();
    void init(PicYuv *origPic, int poc);
};
}

#endif // ifndef X265_LOWRES_H
