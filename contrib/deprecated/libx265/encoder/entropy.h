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

#ifndef X265_ENTROPY_H
#define X265_ENTROPY_H

#include "common.h"
#include "bitstream.h"
#include "frame.h"
#include "cudata.h"
#include "contexts.h"
#include "slice.h"

namespace X265_NS {
// private namespace

struct SaoCtuParam;
struct EstBitsSbac;
class ScalingList;

enum SplitType
{
    DONT_SPLIT            = 0,
    VERTICAL_SPLIT        = 1,
    QUAD_SPLIT            = 2,
    NUMBER_OF_SPLIT_MODES = 3
};

struct TURecurse
{
    uint32_t section;
    uint32_t splitMode;
    uint32_t absPartIdxTURelCU;
    uint32_t absPartIdxStep;

    TURecurse(SplitType splitType, uint32_t _absPartIdxStep, uint32_t _absPartIdxTU)
    {
        static const uint32_t partIdxStepShift[NUMBER_OF_SPLIT_MODES] = { 0, 1, 2 };
        section           = 0;
        absPartIdxTURelCU = _absPartIdxTU;
        splitMode         = (uint32_t)splitType;
        absPartIdxStep    = _absPartIdxStep >> partIdxStepShift[splitMode];
    }

    bool isNextSection()
    {
        if (splitMode == DONT_SPLIT)
        {
            section++;
            return false;
        }
        else
        {
            absPartIdxTURelCU += absPartIdxStep;

            section++;
            return section < (uint32_t)(1 << splitMode);
        }
    }

    bool isLastSection() const
    {
        return (section + 1) >= (uint32_t)(1 << splitMode);
    }
};

struct EstBitsSbac
{
    int significantCoeffGroupBits[NUM_SIG_CG_FLAG_CTX][2];
    int significantBits[2][NUM_SIG_FLAG_CTX];
    int lastBits[2][10];
    int greaterOneBits[NUM_ONE_FLAG_CTX][2];
    int levelAbsBits[NUM_ABS_FLAG_CTX][2];
    int blockCbpBits[NUM_QT_CBF_CTX][2];
    int blockRootCbpBits[2];
};

class Entropy : public SyntaxElementWriter
{
public:

    uint64_t      m_pad;
    uint8_t       m_contextState[160]; // MAX_OFF_CTX_MOD + padding

    /* CABAC state */
    uint32_t      m_low;
    uint32_t      m_range;
    uint32_t      m_bufferedByte;
    int           m_numBufferedBytes;
    int           m_bitsLeft;
    uint64_t      m_fracBits;
    EstBitsSbac   m_estBitsSbac;
    double        m_meanQP;

    Entropy();

    void setBitstream(Bitstream* p)    { m_bitIf = p; }

    uint32_t getNumberOfWrittenBits()
    {
        X265_CHECK(!m_bitIf, "bit counting mode expected\n");
        return (uint32_t)(m_fracBits >> 15);
    }

#if CHECKED_BUILD || _DEBUG
    bool m_valid;
    void markInvalid()                 { m_valid = false; }
    void markValid()                   { m_valid = true; }
#else
    void markValid()                   { }
#endif
    void zeroFract()                   { m_fracBits = 0; }
    void resetBits();
    void resetEntropy(const Slice& slice);

    // SBAC RD
    void load(const Entropy& src)            { copyFrom(src); }
    void store(Entropy& dest) const          { dest.copyFrom(*this); }
    void loadContexts(const Entropy& src)    { copyContextsFrom(src); }
    void loadIntraDirModeLuma(const Entropy& src);
    void copyState(const Entropy& other);

    void codeVPS(const VPS& vps);
    void codeSPS(const SPS& sps, const ScalingList& scalingList, const ProfileTierLevel& ptl);
    void codePPS( const PPS& pps, bool filerAcross, int iPPSInitQpMinus26 );
    void codeVUI(const VUI& vui, int maxSubTLayers, bool bEmitVUITimingInfo, bool bEmitVUIHRDInfo);
    void codeAUD(const Slice& slice);
    void codeHrdParameters(const HRDInfo& hrd, int maxSubTLayers);

    void codeSliceHeader(const Slice& slice, FrameData& encData, uint32_t slice_addr, uint32_t slice_addr_bits, int sliceQp);
    void codeSliceHeaderWPPEntryPoints(const uint32_t *substreamSizes, uint32_t numSubStreams, uint32_t maxOffset);
    void codeShortTermRefPicSet(const RPS& rps, int idx);
    void finishSlice()                 { encodeBinTrm(1); finish(); dynamic_cast<Bitstream*>(m_bitIf)->writeByteAlignment(); }

    void encodeCTU(const CUData& cu, const CUGeom& cuGeom);

    void codeIntraDirLumaAng(const CUData& cu, uint32_t absPartIdx, bool isMultiple);
    void codeIntraDirChroma(const CUData& cu, uint32_t absPartIdx, uint32_t *chromaDirMode);

    void codeMergeIndex(const CUData& cu, uint32_t absPartIdx);
    void codeMvd(const CUData& cu, uint32_t absPartIdx, int list);

    void codePartSize(const CUData& cu, uint32_t absPartIdx, uint32_t depth);
    void codePredInfo(const CUData& cu, uint32_t absPartIdx);

    void codeQtCbfChroma(const CUData& cu, uint32_t absPartIdx, TextType ttype, uint32_t tuDepth, bool lowestLevel);
    void codeCoeff(const CUData& cu, uint32_t absPartIdx, bool& bCodeDQP, const uint32_t depthRange[2]);
    void codeCoeffNxN(const CUData& cu, const coeff_t* coef, uint32_t absPartIdx, uint32_t log2TrSize, TextType ttype);

    inline void codeSaoMerge(uint32_t code)                          { encodeBin(code, m_contextState[OFF_SAO_MERGE_FLAG_CTX]); }
    inline void codeSaoType(uint32_t code)                           { encodeBin(code, m_contextState[OFF_SAO_TYPE_IDX_CTX]); }
    inline void codeMVPIdx(uint32_t symbol)                          { encodeBin(symbol, m_contextState[OFF_MVP_IDX_CTX]); }
    inline void codeMergeFlag(const CUData& cu, uint32_t absPartIdx) { encodeBin(cu.m_mergeFlag[absPartIdx], m_contextState[OFF_MERGE_FLAG_EXT_CTX]); }
    inline void codeSkipFlag(const CUData& cu, uint32_t absPartIdx)  { encodeBin(cu.isSkipped(absPartIdx), m_contextState[OFF_SKIP_FLAG_CTX + cu.getCtxSkipFlag(absPartIdx)]); }
    inline void codeSplitFlag(const CUData& cu, uint32_t absPartIdx, uint32_t depth) { encodeBin(cu.m_cuDepth[absPartIdx] > depth, m_contextState[OFF_SPLIT_FLAG_CTX + cu.getCtxSplitFlag(absPartIdx, depth)]); }
    inline void codeTransformSubdivFlag(uint32_t symbol, uint32_t ctx)    { encodeBin(symbol, m_contextState[OFF_TRANS_SUBDIV_FLAG_CTX + ctx]); }
    inline void codePredMode(int predMode)                                { encodeBin(predMode == MODE_INTRA ? 1 : 0, m_contextState[OFF_PRED_MODE_CTX]); }
    inline void codeCUTransquantBypassFlag(uint32_t symbol)               { encodeBin(symbol, m_contextState[OFF_TQUANT_BYPASS_FLAG_CTX]); }
    inline void codeQtCbfLuma(uint32_t cbf, uint32_t tuDepth)             { encodeBin(cbf, m_contextState[OFF_QT_CBF_CTX + !tuDepth]); }
    inline void codeQtCbfChroma(uint32_t cbf, uint32_t tuDepth)           { encodeBin(cbf, m_contextState[OFF_QT_CBF_CTX + 2 + tuDepth]); }
    inline void codeQtRootCbf(uint32_t cbf)                               { encodeBin(cbf, m_contextState[OFF_QT_ROOT_CBF_CTX]); }
    inline void codeTransformSkipFlags(uint32_t transformSkip, TextType ttype) { encodeBin(transformSkip, m_contextState[OFF_TRANSFORMSKIP_FLAG_CTX + (ttype ? NUM_TRANSFORMSKIP_FLAG_CTX : 0)]); }
    void codeDeltaQP(const CUData& cu, uint32_t absPartIdx);
    void codeSaoOffset(const SaoCtuParam& ctuParam, int plane);
    void codeSaoOffsetEO(int *offset, int typeIdx, int plane);
    void codeSaoOffsetBO(int *offset, int bandPos, int plane);

    /* RDO functions */
    void estBit(EstBitsSbac& estBitsSbac, uint32_t log2TrSize, bool bIsLuma) const;
    void estCBFBit(EstBitsSbac& estBitsSbac) const;
    void estSignificantCoeffGroupMapBit(EstBitsSbac& estBitsSbac, bool bIsLuma) const;
    void estSignificantMapBit(EstBitsSbac& estBitsSbac, uint32_t log2TrSize, bool bIsLuma) const;
    void estSignificantCoefficientsBit(EstBitsSbac& estBitsSbac, bool bIsLuma) const;

    inline uint32_t bitsIntraModeNonMPM() const { return bitsCodeBin(0, m_contextState[OFF_ADI_CTX]) + 5; }
    inline uint32_t bitsIntraModeMPM(const uint32_t preds[3], uint32_t dir) const { return bitsCodeBin(1, m_contextState[OFF_ADI_CTX]) + (dir == preds[0] ? 1 : 2); }
    inline uint32_t estimateCbfBits(uint32_t cbf, TextType ttype, uint32_t tuDepth) const { return bitsCodeBin(cbf, m_contextState[OFF_QT_CBF_CTX + ctxCbf[ttype][tuDepth]]); }
    uint32_t bitsInterMode(const CUData& cu, uint32_t absPartIdx, uint32_t depth) const;
    uint32_t bitsIntraMode(const CUData& cu, uint32_t absPartIdx) const
    {
        return bitsCodeBin(0, m_contextState[OFF_SKIP_FLAG_CTX + cu.getCtxSkipFlag(absPartIdx)]) + /* not skip */
               bitsCodeBin(1, m_contextState[OFF_PRED_MODE_CTX]); /* intra */
    }

    /* these functions are only used to estimate the bits when cbf is 0 and will never be called when writing the bistream. */
    inline void codeQtRootCbfZero() { encodeBin(0, m_contextState[OFF_QT_ROOT_CBF_CTX]); }

private:

    /* CABAC private methods */
    void start();
    void finish();

    void encodeBin(uint32_t binValue, uint8_t& ctxModel);
    void encodeBinEP(uint32_t binValue);
    void encodeBinsEP(uint32_t binValues, int numBins);
    void encodeBinTrm(uint32_t binValue);

    /* return the bits of encoding the context bin without updating */
    inline uint32_t bitsCodeBin(uint32_t binValue, uint32_t ctxModel) const
    {
        uint64_t fracBits = (m_fracBits & 32767) + sbacGetEntropyBits(ctxModel, binValue);
        return (uint32_t)(fracBits >> 15);
    }

    void encodeCU(const CUData& ctu, const CUGeom &cuGeom, uint32_t absPartIdx, uint32_t depth, bool& bEncodeDQP);
    void finishCU(const CUData& ctu, uint32_t absPartIdx, uint32_t depth, bool bEncodeDQP);

    void writeOut();

    /* SBac private methods */
    void writeUnaryMaxSymbol(uint32_t symbol, uint8_t* scmModel, int offset, uint32_t maxSymbol);
    void writeEpExGolomb(uint32_t symbol, uint32_t count);
    void writeCoefRemainExGolomb(uint32_t symbol, const uint32_t absGoRice);

    void codeProfileTier(const ProfileTierLevel& ptl, int maxTempSubLayers);
    void codeScalingList(const ScalingList&);
    void codeScalingList(const ScalingList& scalingList, uint32_t sizeId, uint32_t listId);

    void codePredWeightTable(const Slice& slice);
    void codeInterDir(const CUData& cu, uint32_t absPartIdx);
    void codePUWise(const CUData& cu, uint32_t absPartIdx);
    void codeRefFrmIdxPU(const CUData& cu, uint32_t absPartIdx, int list);
    void codeRefFrmIdx(const CUData& cu, uint32_t absPartIdx, int list);

    void codeSaoMaxUvlc(uint32_t code, uint32_t maxSymbol);

    void codeLastSignificantXY(uint32_t posx, uint32_t posy, uint32_t log2TrSize, bool bIsLuma, uint32_t scanIdx);

    void encodeTransform(const CUData& cu, uint32_t absPartIdx, uint32_t tuDepth, uint32_t log2TrSize,
                         bool& bCodeDQP, const uint32_t depthRange[2]);
    void encodeTransformLuma(const CUData& cu, uint32_t absPartIdx, uint32_t tuDepth, uint32_t log2TrSize,
                         bool& bCodeDQP, const uint32_t depthRange[2]);

    void copyFrom(const Entropy& src);
    void copyContextsFrom(const Entropy& src);
};
}

#endif // ifndef X265_ENTROPY_H
