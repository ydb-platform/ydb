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

#ifndef X265_SLICE_H
#define X265_SLICE_H

#include "common.h"

namespace X265_NS {
// private namespace

class Frame;
class PicList;
class PicYuv;
class MotionReference;

enum SliceType
{
    B_SLICE,
    P_SLICE,
    I_SLICE
};

struct RPS
{
    int  numberOfPictures;
    int  numberOfNegativePictures;
    int  numberOfPositivePictures;

    int  poc[MAX_NUM_REF_PICS];
    int  deltaPOC[MAX_NUM_REF_PICS];
    bool bUsed[MAX_NUM_REF_PICS];

    RPS()
        : numberOfPictures(0)
        , numberOfNegativePictures(0)
        , numberOfPositivePictures(0)
    {
        memset(deltaPOC, 0, sizeof(deltaPOC));
        memset(poc, 0, sizeof(poc));
        memset(bUsed, 0, sizeof(bUsed));
    }

    void sortDeltaPOC();
};

namespace Profile {
    enum Name
    {
        NONE = 0,
        MAIN = 1,
        MAIN10 = 2,
        MAINSTILLPICTURE = 3,
        MAINREXT = 4,
        HIGHTHROUGHPUTREXT = 5
    };
}

namespace Level {
    enum Tier
    {
        MAIN = 0,
        HIGH = 1,
    };

    enum Name
    {
        NONE = 0,
        LEVEL1 = 30,
        LEVEL2 = 60,
        LEVEL2_1 = 63,
        LEVEL3 = 90,
        LEVEL3_1 = 93,
        LEVEL4 = 120,
        LEVEL4_1 = 123,
        LEVEL5 = 150,
        LEVEL5_1 = 153,
        LEVEL5_2 = 156,
        LEVEL6 = 180,
        LEVEL6_1 = 183,
        LEVEL6_2 = 186,
        LEVEL8_5 = 255,
    };
}

struct ProfileTierLevel
{
    int      profileIdc;
    int      levelIdc;
    uint32_t minCrForLevel;
    uint32_t maxLumaSrForLevel;
    uint32_t bitDepthConstraint;
    int      chromaFormatConstraint;
    bool     tierFlag;
    bool     progressiveSourceFlag;
    bool     interlacedSourceFlag;
    bool     nonPackedConstraintFlag;
    bool     frameOnlyConstraintFlag;
    bool     profileCompatibilityFlag[32];
    bool     intraConstraintFlag;
    bool     onePictureOnlyConstraintFlag;
    bool     lowerBitRateConstraintFlag;
};

struct HRDInfo
{
    uint32_t bitRateScale;
    uint32_t cpbSizeScale;
    uint32_t initialCpbRemovalDelayLength;
    uint32_t cpbRemovalDelayLength;
    uint32_t dpbOutputDelayLength;
    uint32_t bitRateValue;
    uint32_t cpbSizeValue;
    bool     cbrFlag;

    HRDInfo()
        : bitRateScale(0)
        , cpbSizeScale(0)
        , initialCpbRemovalDelayLength(1)
        , cpbRemovalDelayLength(1)
        , dpbOutputDelayLength(1)
        , cbrFlag(false)
    {
    }
};

struct TimingInfo
{
    uint32_t numUnitsInTick;
    uint32_t timeScale;
};

struct VPS
{
    HRDInfo          hrdParameters;
    ProfileTierLevel ptl;
    uint32_t         maxTempSubLayers;
    uint32_t         numReorderPics;
    uint32_t         maxDecPicBuffering;
    uint32_t         maxLatencyIncrease;
};

struct Window
{
    int  leftOffset;
    int  rightOffset;
    int  topOffset;
    int  bottomOffset;
    bool bEnabled;

    Window()
    {
        bEnabled = false;
    }
};

struct VUI
{
    int        aspectRatioIdc;
    int        sarWidth;
    int        sarHeight;
    int        videoFormat;
    int        colourPrimaries;
    int        transferCharacteristics;
    int        matrixCoefficients;
    int        chromaSampleLocTypeTopField;
    int        chromaSampleLocTypeBottomField;

    bool       aspectRatioInfoPresentFlag;
    bool       overscanInfoPresentFlag;
    bool       overscanAppropriateFlag;
    bool       videoSignalTypePresentFlag;
    bool       videoFullRangeFlag;
    bool       colourDescriptionPresentFlag;
    bool       chromaLocInfoPresentFlag;
    bool       frameFieldInfoPresentFlag;
    bool       fieldSeqFlag;
    bool       hrdParametersPresentFlag;

    HRDInfo    hrdParameters;
    Window     defaultDisplayWindow;
    TimingInfo timingInfo;
};

struct SPS
{
    /* cached PicYuv offset arrays, shared by all instances of
     * PicYuv created by this encoder */
    intptr_t* cuOffsetY;
    intptr_t* cuOffsetC;
    intptr_t* buOffsetY;
    intptr_t* buOffsetC;

    int      chromaFormatIdc;        // use param
    uint32_t picWidthInLumaSamples;  // use param
    uint32_t picHeightInLumaSamples; // use param

    uint32_t numCuInWidth;
    uint32_t numCuInHeight;
    uint32_t numCUsInFrame;
    uint32_t numPartitions;
    uint32_t numPartInCUSize;

    int      log2MinCodingBlockSize;
    int      log2DiffMaxMinCodingBlockSize;
    int      log2MaxPocLsb;

    uint32_t quadtreeTULog2MaxSize;
    uint32_t quadtreeTULog2MinSize;

    uint32_t quadtreeTUMaxDepthInter; // use param
    uint32_t quadtreeTUMaxDepthIntra; // use param

    uint32_t maxAMPDepth;

    uint32_t maxTempSubLayers;   // max number of Temporal Sub layers
    uint32_t maxDecPicBuffering; // these are dups of VPS values
    uint32_t maxLatencyIncrease;
    int      numReorderPics;

    RPS      spsrps[MAX_NUM_SHORT_TERM_RPS];
    int      spsrpsNum;
    int      numGOPBegin;

    bool     bUseSAO; // use param
    bool     bUseAMP; // use param
    bool     bUseStrongIntraSmoothing; // use param
    bool     bTemporalMVPEnabled;
    bool     bEmitVUITimingInfo;
    bool     bEmitVUIHRDInfo;

    Window   conformanceWindow;
    VUI      vuiParameters;

    SPS()
    {
        memset(this, 0, sizeof(*this));
    }

    ~SPS()
    {
        X265_FREE(cuOffsetY);
        X265_FREE(cuOffsetC);
        X265_FREE(buOffsetY);
        X265_FREE(buOffsetC);
    }
};

struct PPS
{
    uint32_t maxCuDQPDepth;

    int      chromaQpOffset[2];      // use param
    int      deblockingFilterBetaOffsetDiv2;
    int      deblockingFilterTcOffsetDiv2;

    bool     bUseWeightPred;         // use param
    bool     bUseWeightedBiPred;     // use param
    bool     bUseDQP;
    bool     bConstrainedIntraPred;  // use param

    bool     bTransquantBypassEnabled;  // Indicates presence of cu_transquant_bypass_flag in CUs.
    bool     bTransformSkipEnabled;     // use param
    bool     bEntropyCodingSyncEnabled; // use param
    bool     bSignHideEnabled;          // use param

    bool     bDeblockingFilterControlPresent;
    bool     bPicDisableDeblockingFilter;

    int      numRefIdxDefault[2];
    bool     pps_slice_chroma_qp_offsets_present_flag;
};

struct WeightParam
{
    // Explicit weighted prediction parameters parsed in slice header,
    uint32_t log2WeightDenom;
    int      inputWeight;
    int      inputOffset;
    bool     bPresentFlag;

    /* makes a non-h265 weight (i.e. fix7), into an h265 weight */
    void setFromWeightAndOffset(int w, int o, int denom, bool bNormalize)
    {
        inputOffset = o;
        log2WeightDenom = denom;
        inputWeight = w;
        while (bNormalize && log2WeightDenom > 0 && (inputWeight > 127))
        {
            log2WeightDenom--;
            inputWeight >>= 1;
        }

        inputWeight = X265_MIN(inputWeight, 127);
    }
};

#define SET_WEIGHT(w, b, s, d, o) \
    { \
        (w).inputWeight = (s); \
        (w).log2WeightDenom = (d); \
        (w).inputOffset = (o); \
        (w).bPresentFlag = (b); \
    }

class Slice
{
public:

    const SPS*  m_sps;
    const PPS*  m_pps;
    Frame*      m_refFrameList[2][MAX_NUM_REF + 1];
    PicYuv*     m_refReconPicList[2][MAX_NUM_REF + 1];

    WeightParam m_weightPredTable[2][MAX_NUM_REF][3]; // [list][refIdx][0:Y, 1:U, 2:V]
    MotionReference (*m_mref)[MAX_NUM_REF + 1];
    RPS         m_rps;

    NalUnitType m_nalUnitType;
    SliceType   m_sliceType;
    int         m_sliceQp;
    int         m_chromaQpOffset[2];
    int         m_poc;
    int         m_lastIDR;
    int         m_rpsIdx;

    uint32_t    m_colRefIdx;       // never modified

    int         m_numRefIdx[2];
    int         m_refPOCList[2][MAX_NUM_REF + 1];

    uint32_t    m_maxNumMergeCand; // use param
    uint32_t    m_endCUAddr;

    bool        m_bCheckLDC;       // TODO: is this necessary?
    bool        m_sLFaseFlag;      // loop filter boundary flag
    bool        m_colFromL0Flag;   // collocated picture from List0 or List1 flag

    int         m_iPPSQpMinus26;
    int         numRefIdxDefault[2];
    int         m_iNumRPSInSPS;
    const x265_param *m_param;

    Slice()
    {
        m_lastIDR = 0;
        m_sLFaseFlag = true;
        m_numRefIdx[0] = m_numRefIdx[1] = 0;
        memset(m_refFrameList, 0, sizeof(m_refFrameList));
        memset(m_refReconPicList, 0, sizeof(m_refReconPicList));
        memset(m_refPOCList, 0, sizeof(m_refPOCList));
        disableWeights();
        m_iPPSQpMinus26 = 0;
        numRefIdxDefault[0] = 1;
        numRefIdxDefault[1] = 1;
        m_rpsIdx = -1;
        m_chromaQpOffset[0] = m_chromaQpOffset[1] = 0;
    }

    void disableWeights();

    void setRefPicList(PicList& picList);

    bool getRapPicFlag() const
    {
        return m_nalUnitType == NAL_UNIT_CODED_SLICE_IDR_W_RADL
            || m_nalUnitType == NAL_UNIT_CODED_SLICE_CRA;
    }

    bool getIdrPicFlag() const
    {
        return m_nalUnitType == NAL_UNIT_CODED_SLICE_IDR_W_RADL;
    }

    bool isIRAP() const   { return m_nalUnitType >= 16 && m_nalUnitType <= 23; }

    bool isIntra()  const { return m_sliceType == I_SLICE; }

    bool isInterB() const { return m_sliceType == B_SLICE; }

    bool isInterP() const { return m_sliceType == P_SLICE; }

    uint32_t realEndAddress(uint32_t endCUAddr) const;
};

}

#endif // ifndef X265_SLICE_H
