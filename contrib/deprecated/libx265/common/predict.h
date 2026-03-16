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

#ifndef X265_PREDICT_H
#define X265_PREDICT_H

#include "common.h"
#include "frame.h"
#include "quant.h"
#include "shortyuv.h"
#include "yuv.h"

namespace X265_NS {

class CUData;
class Slice;
struct CUGeom;

struct PredictionUnit
{
    uint32_t     ctuAddr;      // raster index of current CTU within its picture
    uint32_t     cuAbsPartIdx; // z-order offset of current CU within its CTU
    uint32_t     puAbsPartIdx; // z-order offset of current PU with its CU
    int          width;
    int          height;

    PredictionUnit(const CUData& cu, const CUGeom& cuGeom, int puIdx);
};

class Predict
{
public:

    enum { ADI_BUF_STRIDE = (2 * MAX_CU_SIZE + 1 + 15) }; // alignment to 16 bytes

    /* Weighted prediction scaling values built from slice parameters (bitdepth scaled) */
    struct WeightValues
    {
        int w, o, offset, shift, round;
    };

    struct IntraNeighbors
    {
        int      numIntraNeighbor;
        int      totalUnits;
        int      aboveUnits;
        int      leftUnits;
        int      unitWidth;
        int      unitHeight;
        int      log2TrSize;
        bool     bNeighborFlags[4 * MAX_NUM_SPU_W + 1];
    };

    ShortYuv  m_predShortYuv[2]; /* temporary storage for weighted prediction */

    // Unfiltered/filtered neighbours of the current partition.
    pixel     intraNeighbourBuf[2][258];

    /* Slice information */
    int       m_csp;
    int       m_hChromaShift;
    int       m_vChromaShift;

    Predict();
    ~Predict();

    bool allocBuffers(int csp);

    // motion compensation functions
    void predInterLumaPixel(const PredictionUnit& pu, Yuv& dstYuv, const PicYuv& refPic, const MV& mv) const;
    void predInterChromaPixel(const PredictionUnit& pu, Yuv& dstYuv, const PicYuv& refPic, const MV& mv) const;

    void predInterLumaShort(const PredictionUnit& pu, ShortYuv& dstSYuv, const PicYuv& refPic, const MV& mv) const;
    void predInterChromaShort(const PredictionUnit& pu, ShortYuv& dstSYuv, const PicYuv& refPic, const MV& mv) const;

    void addWeightBi(const PredictionUnit& pu, Yuv& predYuv, const ShortYuv& srcYuv0, const ShortYuv& srcYuv1, const WeightValues wp0[3], const WeightValues wp1[3], bool bLuma, bool bChroma) const;
    void addWeightUni(const PredictionUnit& pu, Yuv& predYuv, const ShortYuv& srcYuv, const WeightValues wp[3], bool bLuma, bool bChroma) const;

    void motionCompensation(const CUData& cu, const PredictionUnit& pu, Yuv& predYuv, bool bLuma, bool bChroma);

    /* Angular Intra */
    void predIntraLumaAng(uint32_t dirMode, pixel* pred, intptr_t stride, uint32_t log2TrSize);
    void predIntraChromaAng(uint32_t dirMode, pixel* pred, intptr_t stride, uint32_t log2TrSizeC);
    void initAdiPattern(const CUData& cu, const CUGeom& cuGeom, uint32_t puAbsPartIdx, const IntraNeighbors& intraNeighbors, int dirMode);
    void initAdiPatternChroma(const CUData& cu, const CUGeom& cuGeom, uint32_t puAbsPartIdx, const IntraNeighbors& intraNeighbors, uint32_t chromaId);

    /* Intra prediction helper functions */
    static void initIntraNeighbors(const CUData& cu, uint32_t absPartIdx, uint32_t tuDepth, bool isLuma, IntraNeighbors *IntraNeighbors);
    static void fillReferenceSamples(const pixel* adiOrigin, intptr_t picStride, const IntraNeighbors& intraNeighbors, pixel dst[258]);
    template<bool cip>
    static bool isAboveLeftAvailable(const CUData& cu, uint32_t partIdxLT);
    template<bool cip>
    static int  isAboveAvailable(const CUData& cu, uint32_t partIdxLT, uint32_t partIdxRT, bool* bValidFlags);
    template<bool cip>
    static int  isLeftAvailable(const CUData& cu, uint32_t partIdxLT, uint32_t partIdxLB, bool* bValidFlags);
    template<bool cip>
    static int  isAboveRightAvailable(const CUData& cu, uint32_t partIdxRT, bool* bValidFlags, uint32_t numUnits);
    template<bool cip>
    static int  isBelowLeftAvailable(const CUData& cu, uint32_t partIdxLB, bool* bValidFlags, uint32_t numUnits);
};
}

#endif // ifndef X265_PREDICT_H
