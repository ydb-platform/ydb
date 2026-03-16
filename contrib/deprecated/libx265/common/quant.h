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

#ifndef X265_QUANT_H
#define X265_QUANT_H

#include "common.h"
#include "scalinglist.h"
#include "contexts.h"

namespace X265_NS {
// private namespace

class CUData;
class Entropy;
struct TUEntropyCodingParameters;

struct QpParam
{
    int rem;
    int per;
    int qp;
    int64_t lambda2; /* FIX8 */
    int32_t lambda;  /* FIX8, dynamic range is 18-bits in Main and 20-bits in Main10 */

    QpParam() : qp(MAX_INT) {}

    void setQpParam(int qpScaled)
    {
        if (qp != qpScaled)
        {
            rem = qpScaled % 6;
            per = qpScaled / 6;
            qp  = qpScaled;
            lambda2 = (int64_t)(x265_lambda2_tab[qp - QP_BD_OFFSET] * 256. + 0.5);
            lambda  = (int32_t)(x265_lambda_tab[qp - QP_BD_OFFSET] * 256. + 0.5);
            X265_CHECK((x265_lambda_tab[qp - QP_BD_OFFSET] * 256. + 0.5) < (double)MAX_INT, "x265_lambda_tab[] value too large\n");
        }
    }
};

// NOTE: MUST be 16-byte aligned for asm code
struct NoiseReduction
{
    /* 0 = luma 4x4,   1 = luma 8x8,   2 = luma 16x16,   3 = luma 32x32
     * 4 = chroma 4x4, 5 = chroma 8x8, 6 = chroma 16x16, 7 = chroma 32x32
     * Intra 0..7 - Inter 8..15 */
    ALIGN_VAR_16(uint32_t, nrResidualSum[MAX_NUM_TR_CATEGORIES][MAX_NUM_TR_COEFFS]);
    uint32_t nrCount[MAX_NUM_TR_CATEGORIES];
    uint16_t nrOffsetDenoise[MAX_NUM_TR_CATEGORIES][MAX_NUM_TR_COEFFS];
    uint16_t (*offset)[MAX_NUM_TR_COEFFS];
    uint32_t (*residualSum)[MAX_NUM_TR_COEFFS];
    uint32_t *count;
};

class Quant
{
protected:

    const ScalingList* m_scalingList;
    Entropy*           m_entropyCoder;

    QpParam            m_qpParam[3];

    int                m_rdoqLevel;
    int32_t            m_psyRdoqScale;  // dynamic range [0,50] * 256 = 14-bits
    int16_t*           m_resiDctCoeff;
    int16_t*           m_fencDctCoeff;
    int16_t*           m_fencShortBuf;

    enum { IEP_RATE = 32768 }; /* FIX15 cost of an equal probable bit */

public:

    NoiseReduction*    m_nr;
    NoiseReduction*    m_frameNr; // Array of NR structures, one for each frameEncoder

    Quant();
    ~Quant();

    /* one-time setup */
    bool init(double psyScale, const ScalingList& scalingList, Entropy& entropy);
    bool allocNoiseReduction(const x265_param& param);

    /* CU setup */
    void setQPforQuant(const CUData& ctu, int qp);

    uint32_t transformNxN(const CUData& cu, const pixel* fenc, uint32_t fencStride, const int16_t* residual, uint32_t resiStride, coeff_t* coeff,
                          uint32_t log2TrSize, TextType ttype, uint32_t absPartIdx, bool useTransformSkip);

    void invtransformNxN(const CUData& cu, int16_t* residual, uint32_t resiStride, const coeff_t* coeff,
                         uint32_t log2TrSize, TextType ttype, bool bIntra, bool useTransformSkip, uint32_t numSig);
    uint64_t ssimDistortion(const CUData& cu, const pixel* fenc, uint32_t fStride, const pixel* recon, intptr_t rstride,
                            uint32_t log2TrSize, TextType ttype, uint32_t absPartIdx);

    /* Pattern decision for context derivation process of significant_coeff_flag */
    static uint32_t calcPatternSigCtx(uint64_t sigCoeffGroupFlag64, uint32_t cgPosX, uint32_t cgPosY, uint32_t cgBlkPos, uint32_t trSizeCG)
    {
        if (trSizeCG == 1)
            return 0;

        X265_CHECK(trSizeCG <= 8, "transform CG is too large\n");
        X265_CHECK(cgBlkPos < 64, "cgBlkPos is too large\n");
        // NOTE: cgBlkPos+1 may more than 63, it is invalid for shift,
        //       but in this case, both cgPosX and cgPosY equal to (trSizeCG - 1),
        //       the sigRight and sigLower will clear value to zero, the final result will be correct
        const uint32_t sigPos = (uint32_t)(sigCoeffGroupFlag64 >> (cgBlkPos + 1)); // just need lowest 7-bits valid

        // TODO: instruction BT is faster, but _bittest64 still generate instruction 'BT m, r' in VS2012
        const uint32_t sigRight = (cgPosX != (trSizeCG - 1)) & sigPos;
        const uint32_t sigLower = (cgPosY != (trSizeCG - 1)) & (sigPos >> (trSizeCG - 1));
        return sigRight + sigLower * 2;
    }

    /* Context derivation process of coeff_abs_significant_flag */
    static uint32_t getSigCoeffGroupCtxInc(uint64_t cgGroupMask, uint32_t cgPosX, uint32_t cgPosY, uint32_t cgBlkPos, uint32_t trSizeCG)
    {
        X265_CHECK(cgBlkPos < 64, "cgBlkPos is too large\n");
        // NOTE: unsafe shift operator, see NOTE in calcPatternSigCtx
        const uint32_t sigPos = (uint32_t)(cgGroupMask >> (cgBlkPos + 1)); // just need lowest 8-bits valid
        const uint32_t sigRight = (cgPosX != (trSizeCG - 1)) & sigPos;
        const uint32_t sigLower = (cgPosY != (trSizeCG - 1)) & (sigPos >> (trSizeCG - 1));

        return (sigRight | sigLower);
    }

    /* static methods shared with entropy.cpp */
    static uint32_t getSigCtxInc(uint32_t patternSigCtx, uint32_t log2TrSize, uint32_t trSize, uint32_t blkPos, bool bIsLuma, uint32_t firstSignificanceMapContext);

protected:

    void setChromaQP(int qpin, TextType ttype, int chFmt);

    uint32_t signBitHidingHDQ(int16_t* qcoeff, int32_t* deltaU, uint32_t numSig, const TUEntropyCodingParameters &codingParameters, uint32_t log2TrSize);

    template<uint32_t log2TrSize>
    uint32_t rdoQuant(const CUData& cu, int16_t* dstCoeff, TextType ttype, uint32_t absPartIdx, bool usePsy);

public:
    typedef uint32_t (Quant::*rdoQuant_t)(const CUData& cu, int16_t* dstCoeff, TextType ttype, uint32_t absPartIdx, bool usePsy);

private:
    static rdoQuant_t rdoQuant_func[NUM_CU_DEPTH];
};
}

#endif // ifndef X265_QUANT_H
