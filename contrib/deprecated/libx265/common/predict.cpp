/*****************************************************************************
* Copyright (C) 2013-2017 MulticoreWare, Inc
*
* Authors: Deepthi Nandakumar <deepthi@multicorewareinc.com>
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
#include "slice.h"
#include "framedata.h"
#include "picyuv.h"
#include "predict.h"
#include "primitives.h"

using namespace X265_NS;

#if _MSC_VER
#pragma warning(disable: 4127) // conditional expression is constant
#endif

PredictionUnit::PredictionUnit(const CUData& cu, const CUGeom& cuGeom, int puIdx)
{
    /* address of CTU */
    ctuAddr = cu.m_cuAddr;

    /* offset of CU */
    cuAbsPartIdx = cuGeom.absPartIdx;

    /* offset and dimensions of PU */
    cu.getPartIndexAndSize(puIdx, puAbsPartIdx, width, height);
}

namespace
{
inline pixel weightBidir(int w0, int16_t P0, int w1, int16_t P1, int round, int shift, int offset)
{
    return x265_clip((w0 * (P0 + IF_INTERNAL_OFFS) + w1 * (P1 + IF_INTERNAL_OFFS) + round + (offset * (1 << (shift - 1)))) >> shift);
}
}

Predict::Predict()
{
}

Predict::~Predict()
{
    m_predShortYuv[0].destroy();
    m_predShortYuv[1].destroy();
}

bool Predict::allocBuffers(int csp)
{
    m_csp = csp;
    m_hChromaShift = CHROMA_H_SHIFT(csp);
    m_vChromaShift = CHROMA_V_SHIFT(csp);

    return m_predShortYuv[0].create(MAX_CU_SIZE, csp) && m_predShortYuv[1].create(MAX_CU_SIZE, csp);
}

void Predict::motionCompensation(const CUData& cu, const PredictionUnit& pu, Yuv& predYuv, bool bLuma, bool bChroma)
{
    int refIdx0 = cu.m_refIdx[0][pu.puAbsPartIdx];
    int refIdx1 = cu.m_refIdx[1][pu.puAbsPartIdx];

    if (cu.m_slice->isInterP())
    {
        /* P Slice */
        WeightValues wv0[3];

        X265_CHECK(refIdx0 >= 0, "invalid P refidx\n");
        X265_CHECK(refIdx0 < cu.m_slice->m_numRefIdx[0], "P refidx out of range\n");
        const WeightParam *wp0 = cu.m_slice->m_weightPredTable[0][refIdx0];

        MV mv0 = cu.m_mv[0][pu.puAbsPartIdx];
        cu.clipMv(mv0);

        if (cu.m_slice->m_pps->bUseWeightPred && wp0->bPresentFlag)
        {
            for (int plane = 0; plane < (bChroma ? 3 : 1); plane++)
            {
                wv0[plane].w      = wp0[plane].inputWeight;
                wv0[plane].offset = wp0[plane].inputOffset * (1 << (X265_DEPTH - 8));
                wv0[plane].shift  = wp0[plane].log2WeightDenom;
                wv0[plane].round  = wp0[plane].log2WeightDenom >= 1 ? 1 << (wp0[plane].log2WeightDenom - 1) : 0;
            }

            ShortYuv& shortYuv = m_predShortYuv[0];

            if (bLuma)
                predInterLumaShort(pu, shortYuv, *cu.m_slice->m_refReconPicList[0][refIdx0], mv0);
            if (bChroma)
                predInterChromaShort(pu, shortYuv, *cu.m_slice->m_refReconPicList[0][refIdx0], mv0);

            addWeightUni(pu, predYuv, shortYuv, wv0, bLuma, bChroma);
        }
        else
        {
            if (bLuma)
                predInterLumaPixel(pu, predYuv, *cu.m_slice->m_refReconPicList[0][refIdx0], mv0);
            if (bChroma)
                predInterChromaPixel(pu, predYuv, *cu.m_slice->m_refReconPicList[0][refIdx0], mv0);
        }
    }
    else
    {
        /* B Slice */

        WeightValues wv0[3], wv1[3];
        const WeightParam *pwp0, *pwp1;

        X265_CHECK(refIdx0 < cu.m_slice->m_numRefIdx[0], "bidir refidx0 out of range\n");
        X265_CHECK(refIdx1 < cu.m_slice->m_numRefIdx[1], "bidir refidx1 out of range\n");

        if (cu.m_slice->m_pps->bUseWeightedBiPred)
        {
            pwp0 = refIdx0 >= 0 ? cu.m_slice->m_weightPredTable[0][refIdx0] : NULL;
            pwp1 = refIdx1 >= 0 ? cu.m_slice->m_weightPredTable[1][refIdx1] : NULL;

            if (pwp0 && pwp1 && (pwp0->bPresentFlag || pwp1->bPresentFlag))
            {
                /* biprediction weighting */
                for (int plane = 0; plane < (bChroma ? 3 : 1); plane++)
                {
                    wv0[plane].w = pwp0[plane].inputWeight;
                    wv0[plane].o = pwp0[plane].inputOffset * (1 << (X265_DEPTH - 8));
                    wv0[plane].shift = pwp0[plane].log2WeightDenom;
                    wv0[plane].round = 1 << pwp0[plane].log2WeightDenom;

                    wv1[plane].w = pwp1[plane].inputWeight;
                    wv1[plane].o = pwp1[plane].inputOffset * (1 << (X265_DEPTH - 8));
                    wv1[plane].shift = wv0[plane].shift;
                    wv1[plane].round = wv0[plane].round;
                }
            }
            else
            {
                /* uniprediction weighting, always outputs to wv0 */
                const WeightParam* pwp = (refIdx0 >= 0) ? pwp0 : pwp1;
                for (int plane = 0; plane < (bChroma ? 3 : 1); plane++)
                {
                    wv0[plane].w = pwp[plane].inputWeight;
                    wv0[plane].offset = pwp[plane].inputOffset * (1 << (X265_DEPTH - 8));
                    wv0[plane].shift = pwp[plane].log2WeightDenom;
                    wv0[plane].round = pwp[plane].log2WeightDenom >= 1 ? 1 << (pwp[plane].log2WeightDenom - 1) : 0;
                }
            }
        }
        else
            pwp0 = pwp1 = NULL;

        if (refIdx0 >= 0 && refIdx1 >= 0)
        {
            MV mv0 = cu.m_mv[0][pu.puAbsPartIdx];
            MV mv1 = cu.m_mv[1][pu.puAbsPartIdx];
            cu.clipMv(mv0);
            cu.clipMv(mv1);

            if (bLuma)
            {
                predInterLumaShort(pu, m_predShortYuv[0], *cu.m_slice->m_refReconPicList[0][refIdx0], mv0);
                predInterLumaShort(pu, m_predShortYuv[1], *cu.m_slice->m_refReconPicList[1][refIdx1], mv1);
            }
            if (bChroma)
            {
                predInterChromaShort(pu, m_predShortYuv[0], *cu.m_slice->m_refReconPicList[0][refIdx0], mv0);
                predInterChromaShort(pu, m_predShortYuv[1], *cu.m_slice->m_refReconPicList[1][refIdx1], mv1);
            }

            if (pwp0 && pwp1 && (pwp0->bPresentFlag || pwp1->bPresentFlag))
                addWeightBi(pu, predYuv, m_predShortYuv[0], m_predShortYuv[1], wv0, wv1, bLuma, bChroma);
            else
                predYuv.addAvg(m_predShortYuv[0], m_predShortYuv[1], pu.puAbsPartIdx, pu.width, pu.height, bLuma, bChroma);
        }
        else if (refIdx0 >= 0)
        {
            MV mv0 = cu.m_mv[0][pu.puAbsPartIdx];
            cu.clipMv(mv0);

            if (pwp0 && pwp0->bPresentFlag)
            {
                ShortYuv& shortYuv = m_predShortYuv[0];

                if (bLuma)
                    predInterLumaShort(pu, shortYuv, *cu.m_slice->m_refReconPicList[0][refIdx0], mv0);
                if (bChroma)
                    predInterChromaShort(pu, shortYuv, *cu.m_slice->m_refReconPicList[0][refIdx0], mv0);

                addWeightUni(pu, predYuv, shortYuv, wv0, bLuma, bChroma);
            }
            else
            {
                if (bLuma)
                    predInterLumaPixel(pu, predYuv, *cu.m_slice->m_refReconPicList[0][refIdx0], mv0);
                if (bChroma)
                    predInterChromaPixel(pu, predYuv, *cu.m_slice->m_refReconPicList[0][refIdx0], mv0);
            }
        }
        else
        {
            MV mv1 = cu.m_mv[1][pu.puAbsPartIdx];
            cu.clipMv(mv1);

            /* uniprediction to L1 */
            X265_CHECK(refIdx1 >= 0, "refidx1 was not positive\n");

            if (pwp1 && pwp1->bPresentFlag)
            {
                ShortYuv& shortYuv = m_predShortYuv[0];

                if (bLuma)
                    predInterLumaShort(pu, shortYuv, *cu.m_slice->m_refReconPicList[1][refIdx1], mv1);
                if (bChroma)
                    predInterChromaShort(pu, shortYuv, *cu.m_slice->m_refReconPicList[1][refIdx1], mv1);

                addWeightUni(pu, predYuv, shortYuv, wv0, bLuma, bChroma);
            }
            else
            {
                if (bLuma)
                    predInterLumaPixel(pu, predYuv, *cu.m_slice->m_refReconPicList[1][refIdx1], mv1);
                if (bChroma)
                    predInterChromaPixel(pu, predYuv, *cu.m_slice->m_refReconPicList[1][refIdx1], mv1);
            }
        }
    }
}

void Predict::predInterLumaPixel(const PredictionUnit& pu, Yuv& dstYuv, const PicYuv& refPic, const MV& mv) const
{
    pixel* dst = dstYuv.getLumaAddr(pu.puAbsPartIdx);
    intptr_t dstStride = dstYuv.m_size;

    intptr_t srcStride = refPic.m_stride;
    intptr_t srcOffset = (mv.x >> 2) + (mv.y >> 2) * srcStride;
    int partEnum = partitionFromSizes(pu.width, pu.height);
    const pixel* src = refPic.getLumaAddr(pu.ctuAddr, pu.cuAbsPartIdx + pu.puAbsPartIdx) + srcOffset;

    int xFrac = mv.x & 3;
    int yFrac = mv.y & 3;

    if (!(yFrac | xFrac))
        primitives.pu[partEnum].copy_pp(dst, dstStride, src, srcStride);
    else if (!yFrac)
        primitives.pu[partEnum].luma_hpp(src, srcStride, dst, dstStride, xFrac);
    else if (!xFrac)
        primitives.pu[partEnum].luma_vpp(src, srcStride, dst, dstStride, yFrac);
    else
        primitives.pu[partEnum].luma_hvpp(src, srcStride, dst, dstStride, xFrac, yFrac);
}

void Predict::predInterLumaShort(const PredictionUnit& pu, ShortYuv& dstSYuv, const PicYuv& refPic, const MV& mv) const
{
    int16_t* dst = dstSYuv.getLumaAddr(pu.puAbsPartIdx);
    intptr_t dstStride = dstSYuv.m_size;

    intptr_t srcStride = refPic.m_stride;
    intptr_t srcOffset = (mv.x >> 2) + (mv.y >> 2) * srcStride;
    const pixel* src = refPic.getLumaAddr(pu.ctuAddr, pu.cuAbsPartIdx + pu.puAbsPartIdx) + srcOffset;

    int partEnum = partitionFromSizes(pu.width, pu.height);

    X265_CHECK((pu.width % 4) + (pu.height % 4) == 0, "width or height not divisible by 4\n");
    X265_CHECK(dstStride == MAX_CU_SIZE, "stride expected to be max cu size\n");

    int xFrac = mv.x & 3;
    int yFrac = mv.y & 3;

    if (!(yFrac | xFrac))
        primitives.pu[partEnum].convert_p2s(src, srcStride, dst, dstStride);
    else if (!yFrac)
        primitives.pu[partEnum].luma_hps(src, srcStride, dst, dstStride, xFrac, 0);
    else if (!xFrac)
        primitives.pu[partEnum].luma_vps(src, srcStride, dst, dstStride, yFrac);
    else
    {
        ALIGN_VAR_32(int16_t, immed[MAX_CU_SIZE * (MAX_CU_SIZE + NTAPS_LUMA - 1)]);
        int immedStride = pu.width;
        int halfFilterSize = NTAPS_LUMA >> 1;

        primitives.pu[partEnum].luma_hps(src, srcStride, immed, immedStride, xFrac, 1);
        primitives.pu[partEnum].luma_vss(immed + (halfFilterSize - 1) * immedStride, immedStride, dst, dstStride, yFrac);
    }
}

void Predict::predInterChromaPixel(const PredictionUnit& pu, Yuv& dstYuv, const PicYuv& refPic, const MV& mv) const
{
    intptr_t dstStride = dstYuv.m_csize;
    intptr_t refStride = refPic.m_strideC;

    int mvx = mv.x << (1 - m_hChromaShift);
    int mvy = mv.y << (1 - m_vChromaShift);

    intptr_t refOffset = (mvx >> 3) + (mvy >> 3) * refStride;

    const pixel* refCb = refPic.getCbAddr(pu.ctuAddr, pu.cuAbsPartIdx + pu.puAbsPartIdx) + refOffset;
    const pixel* refCr = refPic.getCrAddr(pu.ctuAddr, pu.cuAbsPartIdx + pu.puAbsPartIdx) + refOffset;

    pixel* dstCb = dstYuv.getCbAddr(pu.puAbsPartIdx);
    pixel* dstCr = dstYuv.getCrAddr(pu.puAbsPartIdx);

    int partEnum = partitionFromSizes(pu.width, pu.height);

    int xFrac = mvx & 7;
    int yFrac = mvy & 7;

    if (!(yFrac | xFrac))
    {
        primitives.chroma[m_csp].pu[partEnum].copy_pp(dstCb, dstStride, refCb, refStride);
        primitives.chroma[m_csp].pu[partEnum].copy_pp(dstCr, dstStride, refCr, refStride);
    }
    else if (!yFrac)
    {
        primitives.chroma[m_csp].pu[partEnum].filter_hpp(refCb, refStride, dstCb, dstStride, xFrac);
        primitives.chroma[m_csp].pu[partEnum].filter_hpp(refCr, refStride, dstCr, dstStride, xFrac);
    }
    else if (!xFrac)
    {
        primitives.chroma[m_csp].pu[partEnum].filter_vpp(refCb, refStride, dstCb, dstStride, yFrac);
        primitives.chroma[m_csp].pu[partEnum].filter_vpp(refCr, refStride, dstCr, dstStride, yFrac);
    }
    else
    {
        ALIGN_VAR_32(int16_t, immed[MAX_CU_SIZE * (MAX_CU_SIZE + NTAPS_CHROMA - 1)]);
        int immedStride = pu.width >> m_hChromaShift;
        int halfFilterSize = NTAPS_CHROMA >> 1;

        primitives.chroma[m_csp].pu[partEnum].filter_hps(refCb, refStride, immed, immedStride, xFrac, 1);
        primitives.chroma[m_csp].pu[partEnum].filter_vsp(immed + (halfFilterSize - 1) * immedStride, immedStride, dstCb, dstStride, yFrac);
        primitives.chroma[m_csp].pu[partEnum].filter_hps(refCr, refStride, immed, immedStride, xFrac, 1);
        primitives.chroma[m_csp].pu[partEnum].filter_vsp(immed + (halfFilterSize - 1) * immedStride, immedStride, dstCr, dstStride, yFrac);
    }
}

void Predict::predInterChromaShort(const PredictionUnit& pu, ShortYuv& dstSYuv, const PicYuv& refPic, const MV& mv) const
{
    intptr_t dstStride = dstSYuv.m_csize;
    intptr_t refStride = refPic.m_strideC;

    int mvx = mv.x << (1 - m_hChromaShift);
    int mvy = mv.y << (1 - m_vChromaShift);

    intptr_t refOffset = (mvx >> 3) + (mvy >> 3) * refStride;

    const pixel* refCb = refPic.getCbAddr(pu.ctuAddr, pu.cuAbsPartIdx + pu.puAbsPartIdx) + refOffset;
    const pixel* refCr = refPic.getCrAddr(pu.ctuAddr, pu.cuAbsPartIdx + pu.puAbsPartIdx) + refOffset;

    int16_t* dstCb = dstSYuv.getCbAddr(pu.puAbsPartIdx);
    int16_t* dstCr = dstSYuv.getCrAddr(pu.puAbsPartIdx);

    int partEnum = partitionFromSizes(pu.width, pu.height);
    
    uint32_t cxWidth  = pu.width >> m_hChromaShift;

    X265_CHECK(((cxWidth | (pu.height >> m_vChromaShift)) % 2) == 0, "chroma block size expected to be multiple of 2\n");

    int xFrac = mvx & 7;
    int yFrac = mvy & 7;

    if (!(yFrac | xFrac))
    {
        primitives.chroma[m_csp].pu[partEnum].p2s(refCb, refStride, dstCb, dstStride);
        primitives.chroma[m_csp].pu[partEnum].p2s(refCr, refStride, dstCr, dstStride);
    }
    else if (!yFrac)
    {
        primitives.chroma[m_csp].pu[partEnum].filter_hps(refCb, refStride, dstCb, dstStride, xFrac, 0);
        primitives.chroma[m_csp].pu[partEnum].filter_hps(refCr, refStride, dstCr, dstStride, xFrac, 0);
    }
    else if (!xFrac)
    {
        primitives.chroma[m_csp].pu[partEnum].filter_vps(refCb, refStride, dstCb, dstStride, yFrac);
        primitives.chroma[m_csp].pu[partEnum].filter_vps(refCr, refStride, dstCr, dstStride, yFrac);
    }
    else
    {
        ALIGN_VAR_32(int16_t, immed[MAX_CU_SIZE * (MAX_CU_SIZE + NTAPS_CHROMA - 1)]);
        int immedStride = cxWidth;
        int halfFilterSize = NTAPS_CHROMA >> 1;

        primitives.chroma[m_csp].pu[partEnum].filter_hps(refCb, refStride, immed, immedStride, xFrac, 1);
        primitives.chroma[m_csp].pu[partEnum].filter_vss(immed + (halfFilterSize - 1) * immedStride, immedStride, dstCb, dstStride, yFrac);
        primitives.chroma[m_csp].pu[partEnum].filter_hps(refCr, refStride, immed, immedStride, xFrac, 1);
        primitives.chroma[m_csp].pu[partEnum].filter_vss(immed + (halfFilterSize - 1) * immedStride, immedStride, dstCr, dstStride, yFrac);
    }
}

/* weighted averaging for bi-pred */
void Predict::addWeightBi(const PredictionUnit& pu, Yuv& predYuv, const ShortYuv& srcYuv0, const ShortYuv& srcYuv1, const WeightValues wp0[3], const WeightValues wp1[3], bool bLuma, bool bChroma) const
{
    int x, y;

    int w0, w1, offset, shiftNum, shift, round;
    uint32_t src0Stride, src1Stride, dststride;

    if (bLuma)
    {
        pixel* dstY = predYuv.getLumaAddr(pu.puAbsPartIdx);
        const int16_t* srcY0 = srcYuv0.getLumaAddr(pu.puAbsPartIdx);
        const int16_t* srcY1 = srcYuv1.getLumaAddr(pu.puAbsPartIdx);

        // Luma
        w0      = wp0[0].w;
        offset  = wp0[0].o + wp1[0].o;
        shiftNum = IF_INTERNAL_PREC - X265_DEPTH;
        shift   = wp0[0].shift + shiftNum + 1;
        round   = shift ? (1 << (shift - 1)) : 0;
        w1      = wp1[0].w;

        src0Stride = srcYuv0.m_size;
        src1Stride = srcYuv1.m_size;
        dststride = predYuv.m_size;

        // TODO: can we use weight_sp here?
        for (y = pu.height - 1; y >= 0; y--)
        {
            for (x = pu.width - 1; x >= 0; )
            {
                // note: luma min width is 4
                dstY[x] = weightBidir(w0, srcY0[x], w1, srcY1[x], round, shift, offset);
                x--;
                dstY[x] = weightBidir(w0, srcY0[x], w1, srcY1[x], round, shift, offset);
                x--;
                dstY[x] = weightBidir(w0, srcY0[x], w1, srcY1[x], round, shift, offset);
                x--;
                dstY[x] = weightBidir(w0, srcY0[x], w1, srcY1[x], round, shift, offset);
                x--;
            }

            srcY0 += src0Stride;
            srcY1 += src1Stride;
            dstY  += dststride;
        }
    }

    if (bChroma)
    {
        pixel* dstU = predYuv.getCbAddr(pu.puAbsPartIdx);
        pixel* dstV = predYuv.getCrAddr(pu.puAbsPartIdx);
        const int16_t* srcU0 = srcYuv0.getCbAddr(pu.puAbsPartIdx);
        const int16_t* srcV0 = srcYuv0.getCrAddr(pu.puAbsPartIdx);
        const int16_t* srcU1 = srcYuv1.getCbAddr(pu.puAbsPartIdx);
        const int16_t* srcV1 = srcYuv1.getCrAddr(pu.puAbsPartIdx);

        // Chroma U
        w0      = wp0[1].w;
        offset  = wp0[1].o + wp1[1].o;
        shiftNum = IF_INTERNAL_PREC - X265_DEPTH;
        shift   = wp0[1].shift + shiftNum + 1;
        round   = shift ? (1 << (shift - 1)) : 0;
        w1      = wp1[1].w;

        src0Stride = srcYuv0.m_csize;
        src1Stride = srcYuv1.m_csize;
        dststride  = predYuv.m_csize;

        uint32_t cwidth = pu.width >> srcYuv0.m_hChromaShift;
        uint32_t cheight = pu.height >> srcYuv0.m_vChromaShift;

        // TODO: can we use weight_sp here?
        for (y = cheight - 1; y >= 0; y--)
        {
            for (x = cwidth - 1; x >= 0;)
            {
                // note: chroma min width is 2
                dstU[x] = weightBidir(w0, srcU0[x], w1, srcU1[x], round, shift, offset);
                x--;
                dstU[x] = weightBidir(w0, srcU0[x], w1, srcU1[x], round, shift, offset);
                x--;
            }

            srcU0 += src0Stride;
            srcU1 += src1Stride;
            dstU  += dststride;
        }

        // Chroma V
        w0     = wp0[2].w;
        offset = wp0[2].o + wp1[2].o;
        shift  = wp0[2].shift + shiftNum + 1;
        round  = shift ? (1 << (shift - 1)) : 0;
        w1     = wp1[2].w;

        for (y = cheight - 1; y >= 0; y--)
        {
            for (x = cwidth - 1; x >= 0;)
            {
                // note: chroma min width is 2
                dstV[x] = weightBidir(w0, srcV0[x], w1, srcV1[x], round, shift, offset);
                x--;
                dstV[x] = weightBidir(w0, srcV0[x], w1, srcV1[x], round, shift, offset);
                x--;
            }

            srcV0 += src0Stride;
            srcV1 += src1Stride;
            dstV  += dststride;
        }
    }
}

/* weighted averaging for uni-pred */
void Predict::addWeightUni(const PredictionUnit& pu, Yuv& predYuv, const ShortYuv& srcYuv, const WeightValues wp[3], bool bLuma, bool bChroma) const
{
    int w0, offset, shiftNum, shift, round;
    uint32_t srcStride, dstStride;

    if (bLuma)
    {
        pixel* dstY = predYuv.getLumaAddr(pu.puAbsPartIdx);
        const int16_t* srcY0 = srcYuv.getLumaAddr(pu.puAbsPartIdx);

        // Luma
        w0      = wp[0].w;
        offset  = wp[0].offset;
        shiftNum = IF_INTERNAL_PREC - X265_DEPTH;
        shift   = wp[0].shift + shiftNum;
        round   = shift ? (1 << (shift - 1)) : 0;
        srcStride = srcYuv.m_size;
        dstStride = predYuv.m_size;

        primitives.weight_sp(srcY0, dstY, srcStride, dstStride, pu.width, pu.height, w0, round, shift, offset);
    }

    if (bChroma)
    {
        pixel* dstU = predYuv.getCbAddr(pu.puAbsPartIdx);
        pixel* dstV = predYuv.getCrAddr(pu.puAbsPartIdx);
        const int16_t* srcU0 = srcYuv.getCbAddr(pu.puAbsPartIdx);
        const int16_t* srcV0 = srcYuv.getCrAddr(pu.puAbsPartIdx);

        // Chroma U
        w0      = wp[1].w;
        offset  = wp[1].offset;
        shiftNum = IF_INTERNAL_PREC - X265_DEPTH;
        shift   = wp[1].shift + shiftNum;
        round   = shift ? (1 << (shift - 1)) : 0;

        srcStride = srcYuv.m_csize;
        dstStride = predYuv.m_csize;

        uint32_t cwidth = pu.width >> srcYuv.m_hChromaShift;
        uint32_t cheight = pu.height >> srcYuv.m_vChromaShift;

        primitives.weight_sp(srcU0, dstU, srcStride, dstStride, cwidth, cheight, w0, round, shift, offset);

        // Chroma V
        w0     = wp[2].w;
        offset = wp[2].offset;
        shift  = wp[2].shift + shiftNum;
        round  = shift ? (1 << (shift - 1)) : 0;

        primitives.weight_sp(srcV0, dstV, srcStride, dstStride, cwidth, cheight, w0, round, shift, offset);
    }
}

void Predict::predIntraLumaAng(uint32_t dirMode, pixel* dst, intptr_t stride, uint32_t log2TrSize)
{
    int tuSize = 1 << log2TrSize;
    int sizeIdx = log2TrSize - 2;
    X265_CHECK(sizeIdx >= 0 && sizeIdx < 4, "intra block size is out of range\n");

    int filter = !!(g_intraFilterFlags[dirMode] & tuSize);
    bool bFilter = log2TrSize <= 4;
    primitives.cu[sizeIdx].intra_pred[dirMode](dst, stride, intraNeighbourBuf[filter], dirMode, bFilter);
}

void Predict::predIntraChromaAng(uint32_t dirMode, pixel* dst, intptr_t stride, uint32_t log2TrSizeC)
{
    int tuSize = 1 << log2TrSizeC;
    int sizeIdx = log2TrSizeC - 2;
    X265_CHECK(sizeIdx >= 0 && sizeIdx < 4, "intra block size is out of range\n");

    int filter = !!(m_csp == X265_CSP_I444 && (g_intraFilterFlags[dirMode] & tuSize));
    primitives.cu[sizeIdx].intra_pred[dirMode](dst, stride, intraNeighbourBuf[filter], dirMode, 0);
}

void Predict::initAdiPattern(const CUData& cu, const CUGeom& cuGeom, uint32_t puAbsPartIdx, const IntraNeighbors& intraNeighbors, int dirMode)
{
    int tuSize = 1 << intraNeighbors.log2TrSize;
    int tuSize2 = tuSize << 1;

    PicYuv* reconPic = cu.m_encData->m_reconPic;
    pixel* adiOrigin = reconPic->getLumaAddr(cu.m_cuAddr, cuGeom.absPartIdx + puAbsPartIdx);
    intptr_t picStride = reconPic->m_stride;

    fillReferenceSamples(adiOrigin, picStride, intraNeighbors, intraNeighbourBuf[0]);

    pixel* refBuf = intraNeighbourBuf[0];
    pixel* fltBuf = intraNeighbourBuf[1];

    pixel topLeft = refBuf[0], topLast = refBuf[tuSize2], leftLast = refBuf[tuSize2 + tuSize2];

    if (dirMode == ALL_IDX ? (8 | 16 | 32) & tuSize : g_intraFilterFlags[dirMode] & tuSize)
    {
        // generate filtered intra prediction samples

        if (cu.m_slice->m_sps->bUseStrongIntraSmoothing && tuSize == 32)
        {
            const int threshold = 1 << (X265_DEPTH - 5);

            pixel topMiddle = refBuf[32], leftMiddle = refBuf[tuSize2 + 32];

            if (abs(topLeft + topLast  - (topMiddle  << 1)) < threshold &&
                abs(topLeft + leftLast - (leftMiddle << 1)) < threshold)
            {
                // "strong" bilinear interpolation
                const int shift = 5 + 1;
                int init = (topLeft << shift) + tuSize;
                int deltaL, deltaR;

                deltaL = leftLast - topLeft; deltaR = topLast - topLeft;

                fltBuf[0] = topLeft;
                for (int i = 1; i < tuSize2; i++)
                {
                    fltBuf[i + tuSize2] = (pixel)((init + deltaL * i) >> shift); // Left Filtering
                    fltBuf[i] = (pixel)((init + deltaR * i) >> shift);           // Above Filtering
                }
                fltBuf[tuSize2] = topLast;
                fltBuf[tuSize2 + tuSize2] = leftLast;
                return;
            }
        }

        primitives.cu[intraNeighbors.log2TrSize - 2].intra_filter(refBuf, fltBuf);
    }
}

void Predict::initAdiPatternChroma(const CUData& cu, const CUGeom& cuGeom, uint32_t puAbsPartIdx, const IntraNeighbors& intraNeighbors, uint32_t chromaId)
{
    PicYuv* reconPic = cu.m_encData->m_reconPic;
    const pixel* adiOrigin = reconPic->getChromaAddr(chromaId, cu.m_cuAddr, cuGeom.absPartIdx + puAbsPartIdx);
    intptr_t picStride = reconPic->m_strideC;

    fillReferenceSamples(adiOrigin, picStride, intraNeighbors, intraNeighbourBuf[0]);

    if (m_csp == X265_CSP_I444)
        primitives.cu[intraNeighbors.log2TrSize - 2].intra_filter(intraNeighbourBuf[0], intraNeighbourBuf[1]);
}

void Predict::initIntraNeighbors(const CUData& cu, uint32_t absPartIdx, uint32_t tuDepth, bool isLuma, IntraNeighbors *intraNeighbors)
{
    uint32_t log2TrSize = cu.m_log2CUSize[0] - tuDepth;
    int log2UnitWidth = LOG2_UNIT_SIZE;
    int log2UnitHeight = LOG2_UNIT_SIZE;

    if (!isLuma)
    {
        log2TrSize -= cu.m_hChromaShift;
        log2UnitWidth -= cu.m_hChromaShift;
        log2UnitHeight -= cu.m_vChromaShift;
    }

    int numIntraNeighbor;
    bool* bNeighborFlags = intraNeighbors->bNeighborFlags;

    uint32_t tuSize = 1 << log2TrSize;
    int  tuWidthInUnits = tuSize >> log2UnitWidth;
    int  tuHeightInUnits = tuSize >> log2UnitHeight;
    int  aboveUnits = tuWidthInUnits << 1;
    int  leftUnits = tuHeightInUnits << 1;
    uint32_t partIdxLT = cu.m_absIdxInCTU + absPartIdx;
    uint32_t partIdxRT = g_rasterToZscan[g_zscanToRaster[partIdxLT] + tuWidthInUnits - 1];
    uint32_t partIdxLB = g_rasterToZscan[g_zscanToRaster[partIdxLT] + ((tuHeightInUnits - 1) << LOG2_RASTER_SIZE)];

    if (cu.m_slice->isIntra() || !cu.m_slice->m_pps->bConstrainedIntraPred)
    {
        bNeighborFlags[leftUnits] = isAboveLeftAvailable<false>(cu, partIdxLT);
        numIntraNeighbor  = (int)(bNeighborFlags[leftUnits]);
        numIntraNeighbor += isAboveAvailable<false>(cu, partIdxLT, partIdxRT, bNeighborFlags + leftUnits + 1);
        numIntraNeighbor += isAboveRightAvailable<false>(cu, partIdxRT, bNeighborFlags + leftUnits + 1 + tuWidthInUnits, tuWidthInUnits);
        numIntraNeighbor += isLeftAvailable<false>(cu, partIdxLT, partIdxLB, bNeighborFlags + leftUnits - 1);
        numIntraNeighbor += isBelowLeftAvailable<false>(cu, partIdxLB, bNeighborFlags + tuHeightInUnits - 1, tuHeightInUnits);
    }
    else
    {
        bNeighborFlags[leftUnits] = isAboveLeftAvailable<true>(cu, partIdxLT);
        numIntraNeighbor  = (int)(bNeighborFlags[leftUnits]);
        numIntraNeighbor += isAboveAvailable<true>(cu, partIdxLT, partIdxRT, bNeighborFlags + leftUnits + 1);
        numIntraNeighbor += isAboveRightAvailable<true>(cu, partIdxRT, bNeighborFlags + leftUnits + 1 + tuWidthInUnits, tuWidthInUnits);
        numIntraNeighbor += isLeftAvailable<true>(cu, partIdxLT, partIdxLB, bNeighborFlags + leftUnits - 1);
        numIntraNeighbor += isBelowLeftAvailable<true>(cu, partIdxLB, bNeighborFlags + tuHeightInUnits - 1, tuHeightInUnits);
    }

    intraNeighbors->numIntraNeighbor = numIntraNeighbor;
    intraNeighbors->totalUnits = aboveUnits + leftUnits + 1;
    intraNeighbors->aboveUnits = aboveUnits;
    intraNeighbors->leftUnits = leftUnits;
    intraNeighbors->unitWidth = 1 << log2UnitWidth;
    intraNeighbors->unitHeight = 1 << log2UnitHeight;
    intraNeighbors->log2TrSize = log2TrSize;
}

void Predict::fillReferenceSamples(const pixel* adiOrigin, intptr_t picStride, const IntraNeighbors& intraNeighbors, pixel dst[258])
{
    const pixel dcValue = (pixel)(1 << (X265_DEPTH - 1));
    int numIntraNeighbor = intraNeighbors.numIntraNeighbor;
    int totalUnits = intraNeighbors.totalUnits;
    uint32_t tuSize = 1 << intraNeighbors.log2TrSize;
    uint32_t refSize = tuSize * 2 + 1;

    // Nothing is available, perform DC prediction.
    if (numIntraNeighbor == 0)
    {
        // Fill top border with DC value
        for (uint32_t i = 0; i < refSize; i++)
            dst[i] = dcValue;

        // Fill left border with DC value
        for (uint32_t i = 0; i < refSize - 1; i++)
            dst[i + refSize] = dcValue;
    }
    else if (numIntraNeighbor == totalUnits)
    {
        // Fill top border with rec. samples
        const pixel* adiTemp = adiOrigin - picStride - 1;
        memcpy(dst, adiTemp, refSize * sizeof(pixel));

        // Fill left border with rec. samples
        adiTemp = adiOrigin - 1;
        for (uint32_t i = 0; i < refSize - 1; i++)
        {
            dst[i + refSize] = adiTemp[0];
            adiTemp += picStride;
        }
    }
    else // reference samples are partially available
    {
        const bool *bNeighborFlags = intraNeighbors.bNeighborFlags;
        const bool *pNeighborFlags;
        int aboveUnits = intraNeighbors.aboveUnits;
        int leftUnits = intraNeighbors.leftUnits;
        int unitWidth = intraNeighbors.unitWidth;
        int unitHeight = intraNeighbors.unitHeight;
        int totalSamples = (leftUnits * unitHeight) + ((aboveUnits + 1) * unitWidth);
        pixel adiLineBuffer[5 * MAX_CU_SIZE];
        pixel *adi;

        // Initialize
        for (int i = 0; i < totalSamples; i++)
            adiLineBuffer[i] = dcValue;

        // Fill top-left sample
        const pixel* adiTemp = adiOrigin - picStride - 1;
        adi = adiLineBuffer + (leftUnits * unitHeight);
        pNeighborFlags = bNeighborFlags + leftUnits;
        if (*pNeighborFlags)
        {
            pixel topLeftVal = adiTemp[0];
            for (int i = 0; i < unitWidth; i++)
                adi[i] = topLeftVal;
        }

        // Fill left & below-left samples
        adiTemp += picStride;
        adi--;
        // NOTE: over copy here, but reduce condition operators
        for (int j = 0; j < leftUnits * unitHeight; j++)
        {
            adi[-j] = adiTemp[j * picStride];
        }

        // Fill above & above-right samples
        adiTemp = adiOrigin - picStride;
        adi = adiLineBuffer + (leftUnits * unitHeight) + unitWidth;
        // NOTE: over copy here, but reduce condition operators
        memcpy(adi, adiTemp, aboveUnits * unitWidth * sizeof(*adiTemp));

        // Pad reference samples when necessary
        int curr = 0;
        int next = 1;
        adi = adiLineBuffer;
        int pAdiLineTopRowOffset = leftUnits * (unitHeight - unitWidth);
        if (!bNeighborFlags[0])
        {
            // very bottom unit of bottom-left; at least one unit will be valid.
            while (next < totalUnits && !bNeighborFlags[next])
                next++;

            pixel* pAdiLineNext = adiLineBuffer + ((next < leftUnits) ? (next * unitHeight) : (pAdiLineTopRowOffset + (next * unitWidth)));
            const pixel refSample = *pAdiLineNext;
            // Pad unavailable samples with new value
            int nextOrTop = X265_MIN(next, leftUnits);

            // fill left column
#if HIGH_BIT_DEPTH
            while (curr < nextOrTop)
            {
                for (int i = 0; i < unitHeight; i++)
                    adi[i] = refSample;

                adi += unitHeight;
                curr++;
            }

            // fill top row
            while (curr < next)
            {
                for (int i = 0; i < unitWidth; i++)
                    adi[i] = refSample;

                adi += unitWidth;
                curr++;
            }
#else
            X265_CHECK(curr <= nextOrTop, "curr must be less than or equal to nextOrTop\n");
            if (curr < nextOrTop)
            {
                const int fillSize = unitHeight * (nextOrTop - curr);
                memset(adi, refSample, fillSize * sizeof(pixel));
                curr = nextOrTop;
                adi += fillSize;
            }

            if (curr < next)
            {
                const int fillSize = unitWidth * (next - curr);
                memset(adi, refSample, fillSize * sizeof(pixel));
                curr = next;
                adi += fillSize;
            }
#endif
        }

        // pad all other reference samples.
        while (curr < totalUnits)
        {
            if (!bNeighborFlags[curr]) // samples not available
            {
                int numSamplesInCurrUnit = (curr >= leftUnits) ? unitWidth : unitHeight;
                const pixel refSample = *(adi - 1);
                for (int i = 0; i < numSamplesInCurrUnit; i++)
                    adi[i] = refSample;

                adi += numSamplesInCurrUnit;
                curr++;
            }
            else
            {
                adi += (curr >= leftUnits) ? unitWidth : unitHeight;
                curr++;
            }
        }

        // Copy processed samples
        adi = adiLineBuffer + refSize + unitWidth - 2;
        memcpy(dst, adi, refSize * sizeof(pixel));

        adi = adiLineBuffer + refSize - 1;
        for (int i = 0; i < (int)refSize - 1; i++)
            dst[i + refSize] = adi[-(i + 1)];
    }
}

template<bool cip>
bool Predict::isAboveLeftAvailable(const CUData& cu, uint32_t partIdxLT)
{
    uint32_t partAboveLeft;
    const CUData* cuAboveLeft = cu.getPUAboveLeft(partAboveLeft, partIdxLT);

    return cuAboveLeft && (!cip || cuAboveLeft->isIntra(partAboveLeft));
}

template<bool cip>
int Predict::isAboveAvailable(const CUData& cu, uint32_t partIdxLT, uint32_t partIdxRT, bool* bValidFlags)
{
    const uint32_t rasterPartBegin = g_zscanToRaster[partIdxLT];
    const uint32_t rasterPartEnd = g_zscanToRaster[partIdxRT];
    const uint32_t idxStep = 1;
    int numIntra = 0;

    for (uint32_t rasterPart = rasterPartBegin; rasterPart <= rasterPartEnd; rasterPart += idxStep, bValidFlags++)
    {
        uint32_t partAbove;
        const CUData* cuAbove = cu.getPUAbove(partAbove, g_rasterToZscan[rasterPart]);
        if (cuAbove && (!cip || cuAbove->isIntra(partAbove)))
        {
            numIntra++;
            *bValidFlags = true;
        }
        else
            *bValidFlags = false;
    }

    return numIntra;
}

template<bool cip>
int Predict::isLeftAvailable(const CUData& cu, uint32_t partIdxLT, uint32_t partIdxLB, bool* bValidFlags)
{
    const uint32_t rasterPartBegin = g_zscanToRaster[partIdxLT];
    const uint32_t rasterPartEnd = g_zscanToRaster[partIdxLB];
    const uint32_t idxStep = RASTER_SIZE;
    int numIntra = 0;

    for (uint32_t rasterPart = rasterPartBegin; rasterPart <= rasterPartEnd; rasterPart += idxStep, bValidFlags--) // opposite direction
    {
        uint32_t partLeft;
        const CUData* cuLeft = cu.getPULeft(partLeft, g_rasterToZscan[rasterPart]);
        if (cuLeft && (!cip || cuLeft->isIntra(partLeft)))
        {
            numIntra++;
            *bValidFlags = true;
        }
        else
            *bValidFlags = false;
    }

    return numIntra;
}

template<bool cip>
int Predict::isAboveRightAvailable(const CUData& cu, uint32_t partIdxRT, bool* bValidFlags, uint32_t numUnits)
{
    int numIntra = 0;

    for (uint32_t offset = 1; offset <= numUnits; offset++, bValidFlags++)
    {
        uint32_t partAboveRight;
        const CUData* cuAboveRight = cu.getPUAboveRightAdi(partAboveRight, partIdxRT, offset);
        if (cuAboveRight && (!cip || cuAboveRight->isIntra(partAboveRight)))
        {
            numIntra++;
            *bValidFlags = true;
        }
        else
            *bValidFlags = false;
    }

    return numIntra;
}

template<bool cip>
int Predict::isBelowLeftAvailable(const CUData& cu, uint32_t partIdxLB, bool* bValidFlags, uint32_t numUnits)
{
    int numIntra = 0;

    for (uint32_t offset = 1; offset <= numUnits; offset++, bValidFlags--) // opposite direction
    {
        uint32_t partBelowLeft;
        const CUData* cuBelowLeft = cu.getPUBelowLeftAdi(partBelowLeft, partIdxLB, offset);
        if (cuBelowLeft && (!cip || cuBelowLeft->isIntra(partBelowLeft)))
        {
            numIntra++;
            *bValidFlags = true;
        }
        else
            *bValidFlags = false;
    }

    return numIntra;
}
