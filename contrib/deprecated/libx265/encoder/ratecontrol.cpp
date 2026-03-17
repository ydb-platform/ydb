/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Sumalatha Polureddy <sumalatha@multicorewareinc.com>
 *          Aarthi Priya Thirumalai <aarthi@multicorewareinc.com>
 *          Xun Xu, PPLive Corporation <xunxu@pptv.com>
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

#if _MSC_VER
#pragma warning(disable: 4127) // conditional expression is constant, yes I know
#endif

#include "common.h"
#include "param.h"
#include "frame.h"
#include "framedata.h"
#include "picyuv.h"

#include "encoder.h"
#include "slicetype.h"
#include "ratecontrol.h"
#include "sei.h"

#define BR_SHIFT  6
#define CPB_SHIFT 4

using namespace X265_NS;

/* Amortize the partial cost of I frames over the next N frames */

const int RateControl::s_slidingWindowFrames = 20;
const char *RateControl::s_defaultStatFileName = "x265_2pass.log";

namespace {
#define CMP_OPT_FIRST_PASS(opt, param_val)\
{\
    bErr = 0;\
    p = strstr(opts, opt "=");\
    char* q = strstr(opts, "no-" opt);\
    if (p && sscanf(p, opt "=%d" , &i) && param_val != i)\
        bErr = 1;\
    else if (!param_val && !q && !p)\
        bErr = 1;\
    else if (param_val && (q || !strstr(opts, opt)))\
        bErr = 1;\
    if (bErr)\
    {\
        x265_log(m_param, X265_LOG_ERROR, "different " opt " setting than first pass (%d vs %d)\n", param_val, i);\
        return false;\
    }\
}

inline int calcScale(uint32_t x)
{
    static uint8_t lut[16] = {4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0};
    int y, z = (((x & 0xffff) - 1) >> 27) & 16;
    x >>= z;
    z += y = (((x & 0xff) - 1) >> 28) & 8;
    x >>= y;
    z += y = (((x & 0xf) - 1) >> 29) & 4;
    x >>= y;
    return z + lut[x&0xf];
}

inline int calcLength(uint32_t x)
{
    static uint8_t lut[16] = {4, 3, 2, 2, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0};
    int y, z = (((x >> 16) - 1) >> 27) & 16;
    x >>= z ^ 16;
    z += y = ((x - 0x100) >> 28) & 8;
    x >>= y ^ 8;
    z += y = ((x - 0x10) >> 29) & 4;
    x >>= y ^ 4;
    return z + lut[x];
}

inline char *strcatFilename(const char *input, const char *suffix)
{
    char *output = X265_MALLOC(char, strlen(input) + strlen(suffix) + 1);
    if (!output)
    {
        x265_log(NULL, X265_LOG_ERROR, "unable to allocate memory for filename\n");
        return NULL;
    }
    strcpy(output, input);
    strcat(output, suffix);
    return output;
}

inline double qScale2bits(RateControlEntry *rce, double qScale)
{
    if (qScale < 0.1)
        qScale = 0.1;
    return (rce->coeffBits + .1) * pow(rce->qScale / qScale, 1.1)
           + rce->mvBits * pow(X265_MAX(rce->qScale, 1) / X265_MAX(qScale, 1), 0.5)
           + rce->miscBits;
}

inline void copyRceData(RateControlEntry* rce, RateControlEntry* rce2Pass)
{
    rce->coeffBits = rce2Pass->coeffBits;
    rce->mvBits = rce2Pass->mvBits;
    rce->miscBits = rce2Pass->miscBits;
    rce->iCuCount = rce2Pass->iCuCount;
    rce->pCuCount = rce2Pass->pCuCount;
    rce->skipCuCount = rce2Pass->skipCuCount;
    rce->keptAsRef = rce2Pass->keptAsRef;
    rce->qScale = rce2Pass->qScale;
    rce->newQScale = rce2Pass->newQScale;
    rce->expectedBits = rce2Pass->expectedBits;
    rce->expectedVbv = rce2Pass->expectedVbv;
    rce->blurredComplexity = rce2Pass->blurredComplexity;
    rce->sliceType = rce2Pass->sliceType;
    rce->qpNoVbv = rce2Pass->qpNoVbv;
    rce->newQp = rce2Pass->newQp;
    rce->qRceq = rce2Pass->qRceq;
}

}  // end anonymous namespace
/* Returns the zone for the current frame */
x265_zone* RateControl::getZone()
{
    for (int i = m_param->rc.zoneCount - 1; i >= 0; i--)
    {
        x265_zone *z = &m_param->rc.zones[i];
        if (m_framesDone + 1 >= z->startFrame && m_framesDone < z->endFrame)
            return z;
    }
    return NULL;
}

RateControl::RateControl(x265_param& p)
{
    m_param = &p;
    int lowresCuWidth = ((m_param->sourceWidth / 2) + X265_LOWRES_CU_SIZE - 1) >> X265_LOWRES_CU_BITS;
    int lowresCuHeight = ((m_param->sourceHeight / 2) + X265_LOWRES_CU_SIZE - 1) >> X265_LOWRES_CU_BITS;
    m_ncu = lowresCuWidth * lowresCuHeight;

    if (m_param->rc.cuTree)
        m_qCompress = 1;
    else
        m_qCompress = m_param->rc.qCompress;

    // validate for param->rc, maybe it is need to add a function like x265_parameters_valiate()
    m_residualFrames = 0;
    m_partialResidualFrames = 0;
    m_residualCost = 0;
    m_partialResidualCost = 0;
    m_rateFactorMaxIncrement = 0;
    m_rateFactorMaxDecrement = 0;
    m_fps = (double)m_param->fpsNum / m_param->fpsDenom;
    m_startEndOrder.set(0);
    m_bTerminated = false;
    m_finalFrameCount = 0;
    m_numEntries = 0;
    m_isSceneTransition = false;
    m_lastPredictorReset = 0;
    m_avgPFrameQp = 0;
    m_isFirstMiniGop = false;
    if (m_param->rc.rateControlMode == X265_RC_CRF)
    {
        m_param->rc.qp = (int)m_param->rc.rfConstant;
        m_param->rc.bitrate = 0;

        double baseCplx = m_ncu * (m_param->bframes ? 120 : 80);
        double mbtree_offset = m_param->rc.cuTree ? (1.0 - m_param->rc.qCompress) * 13.5 : 0;
        m_rateFactorConstant = pow(baseCplx, 1 - m_qCompress) /
            x265_qp2qScale(m_param->rc.rfConstant + mbtree_offset);
        if (m_param->rc.rfConstantMax)
        {
            m_rateFactorMaxIncrement = m_param->rc.rfConstantMax - m_param->rc.rfConstant;
            if (m_rateFactorMaxIncrement <= 0)
            {
                x265_log(m_param, X265_LOG_WARNING, "CRF max must be greater than CRF\n");
                m_rateFactorMaxIncrement = 0;
            }
        }
        if (m_param->rc.rfConstantMin)
            m_rateFactorMaxDecrement = m_param->rc.rfConstant - m_param->rc.rfConstantMin;
    }
    m_isAbr = m_param->rc.rateControlMode != X265_RC_CQP && !m_param->rc.bStatRead;
    m_2pass = m_param->rc.rateControlMode != X265_RC_CQP && m_param->rc.bStatRead;
    m_bitrate = m_param->rc.bitrate * 1000;
    m_frameDuration = (double)m_param->fpsDenom / m_param->fpsNum;
    m_qp = m_param->rc.qp;
    m_lastRceq = 1; /* handles the cmplxrsum when the previous frame cost is zero */
    m_shortTermCplxSum = 0;
    m_shortTermCplxCount = 0;
    m_lastNonBPictType = I_SLICE;
    m_isAbrReset = false;
    m_lastAbrResetPoc = -1;
    m_statFileOut = NULL;
    m_cutreeStatFileOut = m_cutreeStatFileIn = NULL;
    m_rce2Pass = NULL;
    m_encOrder = NULL;
    m_lastBsliceSatdCost = 0;
    m_movingAvgSum = 0.0;
    m_isNextGop = false;

    // vbv initialization
    m_param->rc.vbvBufferSize = x265_clip3(0, 2000000, m_param->rc.vbvBufferSize);
    m_param->rc.vbvMaxBitrate = x265_clip3(0, 2000000, m_param->rc.vbvMaxBitrate);
    m_param->rc.vbvBufferInit = x265_clip3(0.0, 2000000.0, m_param->rc.vbvBufferInit);
    m_param->vbvBufferEnd = x265_clip3(0.0, 2000000.0, m_param->vbvBufferEnd);
    m_singleFrameVbv = 0;
    m_rateTolerance = 1.0;

    if (m_param->rc.vbvBufferSize)
    {
        if (m_param->rc.rateControlMode == X265_RC_CQP)
        {
            x265_log(m_param, X265_LOG_WARNING, "VBV is incompatible with constant QP, ignored.\n");
            m_param->rc.vbvBufferSize = 0;
            m_param->rc.vbvMaxBitrate = 0;
        }
        else if (m_param->rc.vbvMaxBitrate == 0)
        {
            if (m_param->rc.rateControlMode == X265_RC_ABR)
            {
                x265_log(m_param, X265_LOG_WARNING, "VBV maxrate unspecified, assuming CBR\n");
                m_param->rc.vbvMaxBitrate = m_param->rc.bitrate;
            }
            else
            {
                x265_log(m_param, X265_LOG_WARNING, "VBV bufsize set but maxrate unspecified, ignored\n");
                m_param->rc.vbvBufferSize = 0;
            }
        }
        else if (m_param->rc.vbvMaxBitrate < m_param->rc.bitrate &&
                 m_param->rc.rateControlMode == X265_RC_ABR)
        {
            x265_log(m_param, X265_LOG_WARNING, "max bitrate less than average bitrate, assuming CBR\n");
            m_param->rc.bitrate = m_param->rc.vbvMaxBitrate;
        }
    }
    else if (m_param->rc.vbvMaxBitrate)
    {
        x265_log(m_param, X265_LOG_WARNING, "VBV maxrate specified, but no bufsize, ignored\n");
        m_param->rc.vbvMaxBitrate = 0;
    }
    m_isVbv = m_param->rc.vbvMaxBitrate > 0 && m_param->rc.vbvBufferSize > 0;
    if (m_param->vbvBufferEnd && !m_isVbv)
    {
        x265_log(m_param, X265_LOG_WARNING, "vbv-end requires VBV parameters, ignored\n");
        m_param->vbvBufferEnd = 0;
    }
    if (m_param->bEmitHRDSEI && !m_isVbv)
    {
        x265_log(m_param, X265_LOG_WARNING, "NAL HRD parameters require VBV parameters, ignored\n");
        m_param->bEmitHRDSEI = 0;
    }
    m_isCbr = m_param->rc.rateControlMode == X265_RC_ABR && m_isVbv && !m_2pass && m_param->rc.vbvMaxBitrate <= m_param->rc.bitrate;
    if (m_param->rc.bStrictCbr && !m_isCbr)
    {
        x265_log(m_param, X265_LOG_WARNING, "strict CBR set without CBR mode, ignored\n");
        m_param->rc.bStrictCbr = 0;
    }
    if(m_param->rc.bStrictCbr)
        m_rateTolerance = 0.7;

    m_bframeBits = 0;
    m_leadingNoBSatd = 0;
    m_ipOffset = 6.0 * X265_LOG2(m_param->rc.ipFactor);
    m_pbOffset = 6.0 * X265_LOG2(m_param->rc.pbFactor);

    for (int i = 0; i < QP_MAX_MAX; i++)
        m_qpToEncodedBits[i] = 0;

    /* Adjust the first frame in order to stabilize the quality level compared to the rest */
#define ABR_INIT_QP_MIN (24)
#define ABR_INIT_QP_MAX (37)
#define ABR_INIT_QP_GRAIN_MAX (33)
#define ABR_SCENECUT_INIT_QP_MIN (12)
#define CRF_INIT_QP (int)m_param->rc.rfConstant
    for (int i = 0; i < 3; i++)
    {
        m_lastQScaleFor[i] = x265_qp2qScale(m_param->rc.rateControlMode == X265_RC_CRF ? CRF_INIT_QP : ABR_INIT_QP_MIN);
        m_lmin[i] = x265_qp2qScale(m_param->rc.qpMin);
        m_lmax[i] = x265_qp2qScale(m_param->rc.qpMax);
    }

    if (m_param->rc.rateControlMode == X265_RC_CQP)
    {
        if (m_qp && !m_param->bLossless)
        {
            m_qpConstant[P_SLICE] = m_qp;
            m_qpConstant[I_SLICE] = x265_clip3(QP_MIN, QP_MAX_MAX, (int)(m_qp - m_ipOffset + 0.5));
            m_qpConstant[B_SLICE] = x265_clip3(QP_MIN, QP_MAX_MAX, (int)(m_qp + m_pbOffset + 0.5));
        }
        else
        {
            m_qpConstant[P_SLICE] = m_qpConstant[I_SLICE] = m_qpConstant[B_SLICE] = m_qp;
        }
    }

    /* qpstep - value set as encoder specific */
    m_lstep = pow(2, m_param->rc.qpStep / 6.0);

    for (int i = 0; i < 2; i++)
        m_cuTreeStats.qpBuffer[i] = NULL;
}

bool RateControl::init(const SPS& sps)
{
    if (m_isVbv)
    {
        /* We don't support changing the ABR bitrate right now,
         * so if the stream starts as CBR, keep it CBR. */
        if (m_param->rc.vbvBufferSize < (int)(m_param->rc.vbvMaxBitrate / m_fps))
        {
            m_param->rc.vbvBufferSize = (int)(m_param->rc.vbvMaxBitrate / m_fps);
            x265_log(m_param, X265_LOG_WARNING, "VBV buffer size cannot be smaller than one frame, using %d kbit\n",
                     m_param->rc.vbvBufferSize);
        }
        int vbvBufferSize = m_param->rc.vbvBufferSize * 1000;
        int vbvMaxBitrate = m_param->rc.vbvMaxBitrate * 1000;

        if (m_param->bEmitHRDSEI)
        {
            const HRDInfo* hrd = &sps.vuiParameters.hrdParameters;
            vbvBufferSize = hrd->cpbSizeValue << (hrd->cpbSizeScale + CPB_SHIFT);
            vbvMaxBitrate = hrd->bitRateValue << (hrd->bitRateScale + BR_SHIFT);
        }
        m_bufferRate = vbvMaxBitrate / m_fps;
        m_vbvMaxRate = vbvMaxBitrate;
        m_bufferSize = vbvBufferSize;
        m_singleFrameVbv = m_bufferRate * 1.1 > m_bufferSize;

        if (m_param->rc.vbvBufferInit > 1.)
            m_param->rc.vbvBufferInit = x265_clip3(0.0, 1.0, m_param->rc.vbvBufferInit / m_param->rc.vbvBufferSize);
        if (m_param->vbvBufferEnd > 1.)
            m_param->vbvBufferEnd = x265_clip3(0.0, 1.0, m_param->vbvBufferEnd / m_param->rc.vbvBufferSize);
        if (m_param->vbvEndFrameAdjust > 1.)
            m_param->vbvEndFrameAdjust = x265_clip3(0.0, 1.0, m_param->vbvEndFrameAdjust);
        m_param->rc.vbvBufferInit = x265_clip3(0.0, 1.0, X265_MAX(m_param->rc.vbvBufferInit, m_bufferRate / m_bufferSize));
        m_bufferFillFinal = m_bufferSize * m_param->rc.vbvBufferInit;
        m_bufferFillActual = m_bufferFillFinal;
        m_bufferExcess = 0;
    }

    m_totalBits = 0;
    m_encodedBits = 0;
    m_framesDone = 0;
    m_residualCost = 0;
    m_partialResidualCost = 0;
    m_amortizeFraction = 0.85;
    m_amortizeFrames = 75;
    if (m_param->totalFrames && m_param->totalFrames <= 2 * m_fps && m_param->rc.bStrictCbr) /* Strict CBR segment encode */
    {
        m_amortizeFraction = 0.85;
        m_amortizeFrames = m_param->totalFrames / 2;
    }

    for (int i = 0; i < s_slidingWindowFrames; i++)
    {
        m_satdCostWindow[i] = 0;
        m_encodedBitsWindow[i] = 0;
    }
    m_sliderPos = 0;
    m_isPatternPresent = false;
    m_numBframesInPattern = 0;

    m_isGrainEnabled = false;
    if(m_param->rc.bEnableGrain) // tune for grainy content OR equal p-b frame sizes
    m_isGrainEnabled = true;
    for (int i = 0; i < 3; i++)
    m_lastQScaleFor[i] = x265_qp2qScale(m_param->rc.rateControlMode == X265_RC_CRF ? CRF_INIT_QP : ABR_INIT_QP_MIN);
    m_avgPFrameQp = 0 ;

    /* 720p videos seem to be a good cutoff for cplxrSum */
    double tuneCplxFactor = (m_ncu > 3600 && m_param->rc.cuTree) ? 2.5 : m_isGrainEnabled ? 1.9 : 1;
    /* estimated ratio that produces a reasonable QP for the first I-frame */
    m_cplxrSum = .01 * pow(7.0e5, m_qCompress) * pow(m_ncu, 0.5) * tuneCplxFactor;
    m_wantedBitsWindow = m_bitrate * m_frameDuration;
    m_accumPNorm = .01;
    m_accumPQp = (m_param->rc.rateControlMode == X265_RC_CRF ? CRF_INIT_QP : ABR_INIT_QP_MIN) * m_accumPNorm;


    /* Frame Predictors used in vbv */
    initFramePredictors();
    if (!m_statFileOut && (m_param->rc.bStatWrite || m_param->rc.bStatRead))
    {
        /* If the user hasn't defined the stat filename, use the default value */
        const char *fileName = m_param->rc.statFileName;
        if (!fileName)
            fileName = s_defaultStatFileName;
        /* Load stat file and init 2pass algo */
        if (m_param->rc.bStatRead)
        {
            m_expectedBitsSum = 0;
            char *p, *statsIn, *statsBuf;
            /* read 1st pass stats */
            statsIn = statsBuf = x265_slurp_file(fileName);
            if (!statsBuf)
                return false;
            if (m_param->rc.cuTree)
            {
                char *tmpFile = strcatFilename(fileName, ".cutree");
                if (!tmpFile)
                    return false;
                m_cutreeStatFileIn = x265_fopen(tmpFile, "rb");
                X265_FREE(tmpFile);
                if (!m_cutreeStatFileIn)
                {
                    x265_log_file(m_param, X265_LOG_ERROR, "can't open stats file %s.cutree\n", fileName);
                    return false;
                }
            }

            /* check whether 1st pass options were compatible with current options */
            if (strncmp(statsBuf, "#options:", 9))
            {
                x265_log(m_param, X265_LOG_ERROR,"options list in stats file not valid\n");
                return false;
            }
            {
                int i, j, m;
                uint32_t k , l;
                bool bErr = false;
                char *opts = statsBuf;
                statsIn = strchr(statsBuf, '\n');
                if (!statsIn)
                {
                    x265_log(m_param, X265_LOG_ERROR, "Malformed stats file\n");
                    return false;
                }
                *statsIn = '\0';
                statsIn++;
                if ((p = strstr(opts, " input-res=")) == 0 || sscanf(p, " input-res=%dx%d", &i, &j) != 2)
                {
                    x265_log(m_param, X265_LOG_ERROR, "Resolution specified in stats file not valid\n");
                    return false;
                }
                if ((p = strstr(opts, " fps=")) == 0 || sscanf(p, " fps=%u/%u", &k, &l) != 2)
                {
                    x265_log(m_param, X265_LOG_ERROR, "fps specified in stats file not valid\n");
                    return false;
                }
                if (((p = strstr(opts, " vbv-maxrate=")) == 0 || sscanf(p, " vbv-maxrate=%d", &m) != 1) && m_param->rc.rateControlMode == X265_RC_CRF)
                {
                    x265_log(m_param, X265_LOG_ERROR, "Constant rate-factor is incompatible with 2pass without vbv-maxrate in the previous pass\n");
                    return false;
                }
                if (k != m_param->fpsNum || l != m_param->fpsDenom)
                {
                    x265_log(m_param, X265_LOG_ERROR, "fps mismatch with 1st pass (%u/%u vs %u/%u)\n",
                              m_param->fpsNum, m_param->fpsDenom, k, l);
                    return false;
                }
                if (m_param->analysisMultiPassRefine)
                {
                    p = strstr(opts, "ref=");
                    sscanf(p, "ref=%d", &i);
                    if (i > m_param->maxNumReferences)
                    {
                        x265_log(m_param, X265_LOG_ERROR, "maxNumReferences cannot be less than 1st pass (%d vs %d)\n",
                            i, m_param->maxNumReferences);
                        return false;
                    }
                }
                if (m_param->analysisMultiPassRefine || m_param->analysisMultiPassDistortion)
                {
                    p = strstr(opts, "ctu=");
                    sscanf(p, "ctu=%u", &k);
                    if (k != m_param->maxCUSize)
                    {
                        x265_log(m_param, X265_LOG_ERROR, "maxCUSize mismatch with 1st pass (%u vs %u)\n",
                            k, m_param->maxCUSize);
                        return false;
                    }
                }
                CMP_OPT_FIRST_PASS("bitdepth", m_param->internalBitDepth);
                CMP_OPT_FIRST_PASS("weightp", m_param->bEnableWeightedPred);
                CMP_OPT_FIRST_PASS("bframes", m_param->bframes);
                CMP_OPT_FIRST_PASS("b-pyramid", m_param->bBPyramid);
                CMP_OPT_FIRST_PASS("open-gop", m_param->bOpenGOP);
                CMP_OPT_FIRST_PASS(" keyint", m_param->keyframeMax);
                CMP_OPT_FIRST_PASS("scenecut", m_param->scenecutThreshold);
                CMP_OPT_FIRST_PASS("intra-refresh", m_param->bIntraRefresh);
                if (m_param->bMultiPassOptRPS)
                {
                    CMP_OPT_FIRST_PASS("multi-pass-opt-rps", m_param->bMultiPassOptRPS);
                    CMP_OPT_FIRST_PASS("repeat-headers", m_param->bRepeatHeaders);
                    CMP_OPT_FIRST_PASS("min-keyint", m_param->keyframeMin);
                }

                if ((p = strstr(opts, "b-adapt=")) != 0 && sscanf(p, "b-adapt=%d", &i) && i >= X265_B_ADAPT_NONE && i <= X265_B_ADAPT_TRELLIS)
                {
                    m_param->bFrameAdaptive = i;
                }
                else if (m_param->bframes)
                {
                    x265_log(m_param, X265_LOG_ERROR, "b-adapt method specified in stats file not valid\n");
                    return false;
                }

                if ((p = strstr(opts, "rc-lookahead=")) != 0 && sscanf(p, "rc-lookahead=%d", &i))
                    m_param->lookaheadDepth = i;
            }
            /* find number of pics */
            p = statsIn;
            int numEntries;
            for (numEntries = -1; p; numEntries++)
                p = strchr(p + 1, ';');
            if (!numEntries)
            {
                x265_log(m_param, X265_LOG_ERROR, "empty stats file\n");
                return false;
            }
            m_numEntries = numEntries;

            if (m_param->totalFrames < m_numEntries && m_param->totalFrames > 0)
            {
                x265_log(m_param, X265_LOG_WARNING, "2nd pass has fewer frames than 1st pass (%d vs %d)\n",
                         m_param->totalFrames, m_numEntries);
            }
            if (m_param->totalFrames > m_numEntries)
            {
                x265_log(m_param, X265_LOG_ERROR, "2nd pass has more frames than 1st pass (%d vs %d)\n",
                         m_param->totalFrames, m_numEntries);
                return false;
            }

            m_rce2Pass = X265_MALLOC(RateControlEntry, m_numEntries);
            if (!m_rce2Pass)
            {
                 x265_log(m_param, X265_LOG_ERROR, "Rce Entries for 2 pass cannot be allocated\n");
                 return false;
            }
            m_encOrder = X265_MALLOC(int, m_numEntries);
            if (!m_encOrder)
            {
                x265_log(m_param, X265_LOG_ERROR, "Encode order for 2 pass cannot be allocated\n");
                return false;
            }
            /* init all to skipped p frames */
            for (int i = 0; i < m_numEntries; i++)
            {
                RateControlEntry *rce = &m_rce2Pass[i];
                rce->sliceType = P_SLICE;
                rce->qScale = rce->newQScale = x265_qp2qScale(20);
                rce->miscBits = m_ncu + 10;
                rce->newQp = 0;
            }
            /* read stats */
            p = statsIn;
            double totalQpAq = 0;
            for (int i = 0; i < m_numEntries; i++)
            {
                RateControlEntry *rce;
                int frameNumber;
                int encodeOrder;
                char picType;
                int e;
                char *next;
                double qpRc, qpAq, qNoVbv, qRceq;
                next = strstr(p, ";");
                if (next)
                    *next++ = 0;
                e = sscanf(p, " in:%d out:%d", &frameNumber, &encodeOrder);
                if (frameNumber < 0 || frameNumber >= m_numEntries)
                {
                    x265_log(m_param, X265_LOG_ERROR, "bad frame number (%d) at stats line %d\n", frameNumber, i);
                    return false;
                }
                rce = &m_rce2Pass[encodeOrder];
                m_encOrder[frameNumber] = encodeOrder;
                if (!m_param->bMultiPassOptRPS)
                {
                    e += sscanf(p, " in:%*d out:%*d type:%c q:%lf q-aq:%lf q-noVbv:%lf q-Rceq:%lf tex:%d mv:%d misc:%d icu:%lf pcu:%lf scu:%lf",
                        &picType, &qpRc, &qpAq, &qNoVbv, &qRceq, &rce->coeffBits,
                        &rce->mvBits, &rce->miscBits, &rce->iCuCount, &rce->pCuCount,
                        &rce->skipCuCount);
                }
                else
                {
                    char deltaPOC[128];
                    char bUsed[40];
                    memset(deltaPOC, 0, sizeof(deltaPOC));
                    memset(bUsed, 0, sizeof(bUsed));
                    e += sscanf(p, " in:%*d out:%*d type:%c q:%lf q-aq:%lf q-noVbv:%lf q-Rceq:%lf tex:%d mv:%d misc:%d icu:%lf pcu:%lf scu:%lf nump:%d numnegp:%d numposp:%d deltapoc:%s bused:%s",
                        &picType, &qpRc, &qpAq, &qNoVbv, &qRceq, &rce->coeffBits,
                        &rce->mvBits, &rce->miscBits, &rce->iCuCount, &rce->pCuCount,
                        &rce->skipCuCount, &rce->rpsData.numberOfPictures, &rce->rpsData.numberOfNegativePictures, &rce->rpsData.numberOfPositivePictures, deltaPOC, bUsed);
                    splitdeltaPOC(deltaPOC, rce);
                    splitbUsed(bUsed, rce);
                    rce->rpsIdx = -1;
                }
                rce->keptAsRef = true;
                rce->isIdr = false;
                if (picType == 'b' || picType == 'p')
                    rce->keptAsRef = false;
                if (picType == 'I')
                    rce->isIdr = true;
                if (picType == 'I' || picType == 'i')
                    rce->sliceType = I_SLICE;
                else if (picType == 'P' || picType == 'p')
                    rce->sliceType = P_SLICE;
                else if (picType == 'B' || picType == 'b')
                    rce->sliceType = B_SLICE;
                else
                    e = -1;
                if (e < 10)
                {
                    x265_log(m_param, X265_LOG_ERROR, "statistics are damaged at line %d, parser out=%d\n", i, e);
                    return false;
                }
                rce->qScale = rce->newQScale = x265_qp2qScale(qpRc);
                totalQpAq += qpAq;
                rce->qpNoVbv = qNoVbv;
                rce->qpaRc = qpRc;
                rce->qpAq = qpAq;
                rce->qRceq = qRceq;
                p = next;
            }
            X265_FREE(statsBuf);
            if (m_param->rc.rateControlMode != X265_RC_CQP)
            {
                m_start = 0;
                m_isQpModified = true;
                if (!initPass2())
                    return false;
            } /* else we're using constant quant, so no need to run the bitrate allocation */
        }
        /* Open output file */
        /* If input and output files are the same, output to a temp file
         * and move it to the real name only when it's complete */
        if (m_param->rc.bStatWrite)
        {
            char *p, *statFileTmpname;
            statFileTmpname = strcatFilename(fileName, ".temp");
            if (!statFileTmpname)
                return false;
            m_statFileOut = x265_fopen(statFileTmpname, "wb");
            X265_FREE(statFileTmpname);
            if (!m_statFileOut)
            {
                x265_log_file(m_param, X265_LOG_ERROR, "can't open stats file %s.temp\n", fileName);
                return false;
            }
            p = x265_param2string(m_param, sps.conformanceWindow.rightOffset, sps.conformanceWindow.bottomOffset);
            if (p)
                fprintf(m_statFileOut, "#options: %s\n", p);
            X265_FREE(p);
            if (m_param->rc.cuTree && !m_param->rc.bStatRead)
            {
                statFileTmpname = strcatFilename(fileName, ".cutree.temp");
                if (!statFileTmpname)
                    return false;
                m_cutreeStatFileOut = x265_fopen(statFileTmpname, "wb");
                X265_FREE(statFileTmpname);
                if (!m_cutreeStatFileOut)
                {
                    x265_log_file(m_param, X265_LOG_ERROR, "can't open mbtree stats file %s.cutree.temp\n", fileName);
                    return false;
                }
            }
        }
        if (m_param->rc.cuTree)
        {
            if (m_param->rc.qgSize == 8)
            {
                m_cuTreeStats.qpBuffer[0] = X265_MALLOC(uint16_t, m_ncu * 4 * sizeof(uint16_t));
                if (m_param->bBPyramid && m_param->rc.bStatRead)
                    m_cuTreeStats.qpBuffer[1] = X265_MALLOC(uint16_t, m_ncu * 4 * sizeof(uint16_t));
            }
            else
            {
                m_cuTreeStats.qpBuffer[0] = X265_MALLOC(uint16_t, m_ncu * sizeof(uint16_t));
                if (m_param->bBPyramid && m_param->rc.bStatRead)
                    m_cuTreeStats.qpBuffer[1] = X265_MALLOC(uint16_t, m_ncu * sizeof(uint16_t));
            }
            m_cuTreeStats.qpBufPos = -1;
        }
    }
    return true;
}

void RateControl::reconfigureRC()
{
    if (m_isVbv)
    {
        m_param->rc.vbvBufferSize = x265_clip3(0, 2000000, m_param->rc.vbvBufferSize);
        m_param->rc.vbvMaxBitrate = x265_clip3(0, 2000000, m_param->rc.vbvMaxBitrate);
        if (m_param->rc.vbvMaxBitrate < m_param->rc.bitrate &&
            m_param->rc.rateControlMode == X265_RC_ABR)
        {
            x265_log(m_param, X265_LOG_WARNING, "max bitrate less than average bitrate, assuming CBR\n");
            m_param->rc.bitrate = m_param->rc.vbvMaxBitrate;
        }

        if (m_param->rc.vbvBufferSize < (int)(m_param->rc.vbvMaxBitrate / m_fps))
        {
            m_param->rc.vbvBufferSize = (int)(m_param->rc.vbvMaxBitrate / m_fps);
            x265_log(m_param, X265_LOG_WARNING, "VBV buffer size cannot be smaller than one frame, using %d kbit\n",
                m_param->rc.vbvBufferSize);
        }
        int vbvBufferSize = m_param->rc.vbvBufferSize * 1000;
        int vbvMaxBitrate = m_param->rc.vbvMaxBitrate * 1000;
        m_bufferRate = vbvMaxBitrate / m_fps;
        m_vbvMaxRate = vbvMaxBitrate;
        m_bufferSize = vbvBufferSize;
        m_singleFrameVbv = m_bufferRate * 1.1 > m_bufferSize;
    }
    if (m_param->rc.rateControlMode == X265_RC_CRF)
    {
        #define CRF_INIT_QP (int)m_param->rc.rfConstant
        m_param->rc.bitrate = 0;
        double baseCplx = m_ncu * (m_param->bframes ? 120 : 80);
        double mbtree_offset = m_param->rc.cuTree ? (1.0 - m_param->rc.qCompress) * 13.5 : 0;
        m_rateFactorConstant = pow(baseCplx, 1 - m_qCompress) /
            x265_qp2qScale(m_param->rc.rfConstant + mbtree_offset);
        if (m_param->rc.rfConstantMax)
        {
            m_rateFactorMaxIncrement = m_param->rc.rfConstantMax - m_param->rc.rfConstant;
            if (m_rateFactorMaxIncrement <= 0)
            {
                x265_log(m_param, X265_LOG_WARNING, "CRF max must be greater than CRF\n");
                m_rateFactorMaxIncrement = 0;
            }
        }
        if (m_param->rc.rfConstantMin)
            m_rateFactorMaxDecrement = m_param->rc.rfConstant - m_param->rc.rfConstantMin;
    }
    m_bitrate = m_param->rc.bitrate * 1000;
}

void RateControl::initHRD(SPS& sps)
{
    int vbvBufferSize = m_param->rc.vbvBufferSize * 1000;
    int vbvMaxBitrate = m_param->rc.vbvMaxBitrate * 1000;

    // Init HRD
    HRDInfo* hrd = &sps.vuiParameters.hrdParameters;
    hrd->cbrFlag = m_isCbr;

    // normalize HRD size and rate to the value / scale notation
    hrd->bitRateScale = x265_clip3(0, 15, calcScale(vbvMaxBitrate) - BR_SHIFT);
    hrd->bitRateValue = (vbvMaxBitrate >> (hrd->bitRateScale + BR_SHIFT));

    hrd->cpbSizeScale = x265_clip3(0, 15, calcScale(vbvBufferSize) - CPB_SHIFT);
    hrd->cpbSizeValue = (vbvBufferSize >> (hrd->cpbSizeScale + CPB_SHIFT));
    int bitRateUnscale = hrd->bitRateValue << (hrd->bitRateScale + BR_SHIFT);
    int cpbSizeUnscale = hrd->cpbSizeValue << (hrd->cpbSizeScale + CPB_SHIFT);

    // arbitrary
    #define MAX_DURATION 0.5

    TimingInfo *time = &sps.vuiParameters.timingInfo;
    int maxCpbOutputDelay = (int)(X265_MIN(m_param->keyframeMax * MAX_DURATION * time->timeScale / time->numUnitsInTick, INT_MAX));
    int maxDpbOutputDelay = (int)(sps.maxDecPicBuffering * MAX_DURATION * time->timeScale / time->numUnitsInTick);
    int maxDelay = (int)(90000.0 * cpbSizeUnscale / bitRateUnscale + 0.5);

    hrd->initialCpbRemovalDelayLength = 2 + x265_clip3(4, 22, 32 - calcLength(maxDelay));
    hrd->cpbRemovalDelayLength = x265_clip3(4, 31, 32 - calcLength(maxCpbOutputDelay));
    hrd->dpbOutputDelayLength = x265_clip3(4, 31, 32 - calcLength(maxDpbOutputDelay));

    #undef MAX_DURATION
}

bool RateControl::analyseABR2Pass(uint64_t allAvailableBits)
{
    double rateFactor, stepMult;
    double qBlur = m_param->rc.qblur;
    double cplxBlur = m_param->rc.complexityBlur;
    const int filterSize = (int)(qBlur * 4) | 1;
    double expectedBits;
    double *qScale, *blurredQscale;
    double baseCplx = m_ncu * (m_param->bframes ? 120 : 80);
    double clippedDuration = CLIP_DURATION(m_frameDuration) / BASE_FRAME_DURATION;
    /* Blur complexities, to reduce local fluctuation of QP.
     * We don't blur the QPs directly, because then one very simple frame
     * could drag down the QP of a nearby complex frame and give it more
     * bits than intended. */
    for (int i = 0; i < m_numEntries; i++)
    {
        double weightSum = 0;
        double cplxSum = 0;
        double weight = 1.0;
        double gaussianWeight;
        /* weighted average of cplx of future frames */
        for (int j = 1; j < cplxBlur * 2 && j < m_numEntries - i; j++)
        {
            int index = m_encOrder[i + j];
            RateControlEntry *rcj = &m_rce2Pass[index];
            weight *= 1 - pow(rcj->iCuCount / m_ncu, 2);
            if (weight < 0.0001)
                break;
            gaussianWeight = weight * exp(-j * j / 200.0);
            weightSum += gaussianWeight;
            cplxSum += gaussianWeight * (qScale2bits(rcj, 1) - rcj->miscBits) / clippedDuration;
        }
        /* weighted average of cplx of past frames */
        weight = 1.0;
        for (int j = 0; j <= cplxBlur * 2 && j <= i; j++)
        {
            int index = m_encOrder[i - j];
            RateControlEntry *rcj = &m_rce2Pass[index];
            gaussianWeight = weight * exp(-j * j / 200.0);
            weightSum += gaussianWeight;
            cplxSum += gaussianWeight * (qScale2bits(rcj, 1) - rcj->miscBits) / clippedDuration;
            weight *= 1 - pow(rcj->iCuCount / m_ncu, 2);
            if (weight < .0001)
                break;
        }
        m_rce2Pass[m_encOrder[i]].blurredComplexity = cplxSum / weightSum;
    }
    CHECKED_MALLOC(qScale, double, m_numEntries);
    if (filterSize > 1)
    {
        CHECKED_MALLOC(blurredQscale, double, m_numEntries);
    }
    else
        blurredQscale = qScale;

    /* Search for a factor which, when multiplied by the RCEQ values from
     * each frame, adds up to the desired total size.
     * There is no exact closed-form solution because of VBV constraints and
     * because qscale2bits is not invertible, but we can start with the simple
     * approximation of scaling the 1st pass by the ratio of bitrates.
     * The search range is probably overkill, but speed doesn't matter here. */
    expectedBits = 1;
    for (int i = 0; i < m_numEntries; i++)
    {
        RateControlEntry* rce = &m_rce2Pass[m_encOrder[i]];
        double q = getQScale(rce, 1.0);
        expectedBits += qScale2bits(rce, q);
        m_lastQScaleFor[rce->sliceType] = q;
    }
    stepMult = allAvailableBits / expectedBits;

    rateFactor = 0;
    for (double step = 1E4 * stepMult; step > 1E-7 * stepMult; step *= 0.5)
    {
        expectedBits = 0;
        rateFactor += step;

        m_lastNonBPictType = -1;
        m_lastAccumPNorm = 1;
        m_accumPNorm = 0;

        m_lastQScaleFor[0] = m_lastQScaleFor[1] =
        m_lastQScaleFor[2] = pow(baseCplx, 1 - m_qCompress) / rateFactor;

        /* find qscale */
        for (int i = 0; i < m_numEntries; i++)
        {
            RateControlEntry *rce = &m_rce2Pass[m_encOrder[i]];
            qScale[i] = getQScale(rce, rateFactor);
            m_lastQScaleFor[rce->sliceType] = qScale[i];
        }

        /* fixed I/B qscale relative to P */
        for (int i = m_numEntries - 1; i >= 0; i--)
        {
            qScale[i] = getDiffLimitedQScale(&m_rce2Pass[m_encOrder[i]], qScale[i]);
            X265_CHECK(qScale[i] >= 0, "qScale became negative\n");
        }

        /* smooth curve */
        if (filterSize > 1)
        {
            X265_CHECK(filterSize % 2 == 1, "filterSize not an odd number\n");
            for (int i = 0; i < m_numEntries; i++)
            {
                double q = 0.0, sum = 0.0;

                for (int j = 0; j < filterSize; j++)
                {
                    int idx = i + j - filterSize / 2;
                    double d = idx - i;
                    double coeff = qBlur == 0 ? 1.0 : exp(-d * d / (qBlur * qBlur));
                    if (idx < 0 || idx >= m_numEntries)
                        continue;
                    if (m_rce2Pass[m_encOrder[i]].sliceType != m_rce2Pass[m_encOrder[idx]].sliceType)
                        continue;
                    q += qScale[idx] * coeff;
                    sum += coeff;
                }
                blurredQscale[i] = q / sum;
            }
        }

        /* find expected bits */
        for (int i = 0; i < m_numEntries; i++)
        {
            RateControlEntry *rce = &m_rce2Pass[m_encOrder[i]];
            rce->newQScale = clipQscale(NULL, rce, blurredQscale[i]); // check if needed
            X265_CHECK(rce->newQScale >= 0, "new Qscale is negative\n");
            expectedBits += qScale2bits(rce, rce->newQScale);
        }

        if (expectedBits > allAvailableBits)
            rateFactor -= step;
    }

    X265_FREE(qScale);
    if (filterSize > 1)
        X265_FREE(blurredQscale);
    if (m_isVbv)
    if (!vbv2Pass(allAvailableBits, m_numEntries - 1, 0))
            return false;
    expectedBits = countExpectedBits(0, m_numEntries - 1);
    if (fabs(expectedBits / allAvailableBits - 1.0) > 0.01)
    {
        double avgq = 0;
        for (int i = 0; i < m_numEntries; i++)
            avgq += m_rce2Pass[i].newQScale;
        avgq = x265_qScale2qp(avgq / m_numEntries);

        if (expectedBits > allAvailableBits || !m_isVbv)
            x265_log(m_param, X265_LOG_WARNING, "Error: 2pass curve failed to converge\n");
        x265_log(m_param, X265_LOG_WARNING, "target: %.2f kbit/s, expected: %.2f kbit/s, avg QP: %.4f\n",
                 (double)m_param->rc.bitrate,
                 expectedBits * m_fps / (m_numEntries * 1000.),
                 avgq);
        if (expectedBits < allAvailableBits && avgq < m_param->rc.qpMin + 2)
        {
            if (m_param->rc.qpMin > 0)
                x265_log(m_param, X265_LOG_WARNING, "try reducing target bitrate or reducing qp_min (currently %d)\n", m_param->rc.qpMin);
            else
                x265_log(m_param, X265_LOG_WARNING, "try reducing target bitrate\n");
        }
        else if (expectedBits > allAvailableBits && avgq > m_param->rc.qpMax - 2)
        {
            if (m_param->rc.qpMax < QP_MAX_MAX)
                x265_log(m_param, X265_LOG_WARNING, "try increasing target bitrate or increasing qp_max (currently %d)\n", m_param->rc.qpMax);
            else
                x265_log(m_param, X265_LOG_WARNING, "try increasing target bitrate\n");
        }
        else if (!(m_2pass && m_isVbv))
            x265_log(m_param, X265_LOG_WARNING, "internal error\n");
    }

    return true;

fail:
    x265_log(m_param, X265_LOG_WARNING, "two-pass ABR initialization failed\n");
    return false;
}

bool RateControl::initPass2()
{
    uint64_t allConstBits = 0, allCodedBits = 0;
    uint64_t allAvailableBits = uint64_t(m_param->rc.bitrate * 1000. * m_numEntries * m_frameDuration);
    int startIndex, framesCount, endIndex;
    int fps = X265_MIN(m_param->keyframeMax, (int)(m_fps + 0.5));
    startIndex = endIndex = framesCount = 0;
    int diffQp = 0;
    double targetBits = 0;
    double expectedBits = 0;
    for (startIndex = m_start, endIndex = m_start; endIndex < m_numEntries; endIndex++)
    {
        allConstBits += m_rce2Pass[endIndex].miscBits;
        allCodedBits += m_rce2Pass[endIndex].coeffBits + m_rce2Pass[endIndex].mvBits;
        if (m_param->rc.rateControlMode == X265_RC_CRF)
        {
            framesCount = endIndex - startIndex + 1;
            diffQp += int (m_rce2Pass[endIndex].qpaRc - m_rce2Pass[endIndex].qpNoVbv);
            if (framesCount > fps)
                diffQp -= int (m_rce2Pass[endIndex - fps].qpaRc - m_rce2Pass[endIndex - fps].qpNoVbv);
            if (framesCount >= fps)
            {
                if (diffQp >= 1)
                {
                    if (!m_isQpModified && endIndex > fps)
                    {
                        double factor = 2;
                        double step = 0;
                        if (endIndex + fps >= m_numEntries)
                        {
                            m_start = endIndex - (endIndex % fps);
                            return true;
                        }
                        for (int start = endIndex + 1; start <= endIndex + fps && start < m_numEntries; start++)
                        {
                            RateControlEntry *rce = &m_rce2Pass[start];
                            targetBits += qScale2bits(rce, x265_qp2qScale(rce->qpNoVbv));
                            expectedBits += qScale2bits(rce, rce->qScale);
                        }
                        if (expectedBits < 0.95 * targetBits)
                        {
                            m_isQpModified = true;
                            m_isGopReEncoded = true;
                            while (endIndex + fps < m_numEntries)
                            {
                                step = pow(2, factor / 6.0);
                                expectedBits = 0;
                                for (int start = endIndex + 1; start <= endIndex + fps; start++)
                                {
                                    RateControlEntry *rce = &m_rce2Pass[start];
                                    rce->newQScale = rce->qScale / step;
                                    X265_CHECK(rce->newQScale >= 0, "new Qscale is negative\n");
                                    expectedBits += qScale2bits(rce, rce->newQScale);
                                    rce->newQp = x265_qScale2qp(rce->newQScale);
                                }
                                if (expectedBits >= targetBits && step > 1)
                                    factor *= 0.90;
                                else
                                    break;
                            }

                            if (m_isVbv && endIndex + fps < m_numEntries)
                                if (!vbv2Pass((uint64_t)targetBits, endIndex + fps, endIndex + 1))
                                    return false;

                            targetBits = 0;
                            expectedBits = 0;

                            for (int start = endIndex - fps + 1; start <= endIndex; start++)
                            {
                                RateControlEntry *rce = &m_rce2Pass[start];
                                targetBits += qScale2bits(rce, x265_qp2qScale(rce->qpNoVbv));
                            }
                            while (1)
                            {
                                step = pow(2, factor / 6.0);
                                expectedBits = 0;
                                for (int start = endIndex - fps + 1; start <= endIndex; start++)
                                {
                                    RateControlEntry *rce = &m_rce2Pass[start];
                                    rce->newQScale = rce->qScale * step;
                                    X265_CHECK(rce->newQScale >= 0, "new Qscale is negative\n");
                                    expectedBits += qScale2bits(rce, rce->newQScale);
                                    rce->newQp = x265_qScale2qp(rce->newQScale);
                                }
                                if (expectedBits > targetBits && step > 1)
                                    factor *= 1.1;
                                else
                                     break;
                            }
                            if (m_isVbv)
                                if (!vbv2Pass((uint64_t)targetBits, endIndex, endIndex - fps + 1))
                                    return false;
                            diffQp = 0;
                            m_reencode = endIndex - fps + 1;
                            endIndex = endIndex + fps;
                            startIndex = endIndex + 1;
                            m_start = startIndex;
                            targetBits = expectedBits = 0;
                        }
                        else
                            targetBits = expectedBits = 0;
                    }
                }
                else
                    m_isQpModified = false;
            }
        }
    }

    if (m_param->rc.rateControlMode == X265_RC_ABR)
    {
        if (allAvailableBits < allConstBits)
        {
            x265_log(m_param, X265_LOG_ERROR, "requested bitrate is too low. estimated minimum is %d kbps\n",
                     (int)(allConstBits * m_fps / framesCount * 1000.));
            return false;
        }
        if (!analyseABR2Pass(allAvailableBits))
            return false;
    }

    m_start = X265_MAX(m_start, endIndex - fps);

    return true;
}

bool RateControl::vbv2Pass(uint64_t allAvailableBits, int endPos, int startPos)
{
    /* for each interval of bufferFull .. underflow, uniformly increase the qp of all
     * frames in the interval until either buffer is full at some intermediate frame or the
     * last frame in the interval no longer underflows.  Recompute intervals and repeat.
     * Then do the converse to put bits back into overflow areas until target size is met */

    double *fills;
    double expectedBits = 0;
    double adjustment;
    double prevBits = 0;
    int t0, t1;
    double qScaleMin = x265_qp2qScale(m_param->rc.qpMin);
    double qScaleMax = x265_qp2qScale(m_param->rc.qpMax);
    int iterations = 0 , adjMin, adjMax;
    CHECKED_MALLOC(fills, double, m_numEntries + 1);
    fills++;

    /* adjust overall stream size */
    do
    {
        iterations++;
        prevBits = expectedBits;

        if (expectedBits)
        {   /* not first iteration */
            adjustment = X265_MAX(X265_MIN(expectedBits / allAvailableBits, 0.999), 0.9);
            fills[-1] = m_bufferSize * m_param->rc.vbvBufferInit;
            t0 = startPos;
            /* fix overflows */
            adjMin = 1;
            while (adjMin && findUnderflow(fills, &t0, &t1, 1, endPos))
            {
                adjMin = fixUnderflow(t0, t1, adjustment, qScaleMin, qScaleMax);
                t0 = t1;
            }
        }

        fills[-1] = m_bufferSize * (1. - m_param->rc.vbvBufferInit);
        t0 = 0;
        /* fix underflows -- should be done after overflow, as we'd better undersize target than underflowing VBV */
        adjMax = 1;
        while (adjMax && findUnderflow(fills, &t0, &t1, 0, endPos))
            adjMax = fixUnderflow(t0, t1, 1.001, qScaleMin, qScaleMax);
        expectedBits = countExpectedBits(startPos, endPos);
    }
    while ((expectedBits < .995 * allAvailableBits) && ((int64_t)(expectedBits+.5) > (int64_t)(prevBits+.5)) && !(m_param->rc.rateControlMode == X265_RC_CRF));
    if (!adjMax)
        x265_log(m_param, X265_LOG_WARNING, "vbv-maxrate issue, qpmax or vbv-maxrate too low\n");
    /* store expected vbv filling values for tracking when encoding */
    for (int i = startPos; i <= endPos; i++)
        m_rce2Pass[i].expectedVbv = m_bufferSize - fills[i];
    X265_FREE(fills - 1);
    return true;

fail:
    x265_log(m_param, X265_LOG_ERROR, "malloc failure in two-pass VBV init\n");
    return false;
}

/* In 2pass, force the same frame types as in the 1st pass */
int RateControl::rateControlSliceType(int frameNum)
{
    if (m_param->rc.bStatRead)
    {
        if (frameNum >= m_numEntries)
        {
            /* We could try to initialize everything required for ABR and
             * adaptive B-frames, but that would be complicated.
             * So just calculate the average QP used so far. */
            m_param->rc.qp = (m_accumPQp < 1) ? ABR_INIT_QP_MAX : (int)(m_accumPQp + 0.5);
            m_qpConstant[P_SLICE] = x265_clip3(QP_MIN, QP_MAX_MAX, m_param->rc.qp);
            m_qpConstant[I_SLICE] = x265_clip3(QP_MIN, QP_MAX_MAX, (int)(m_param->rc.qp - m_ipOffset + 0.5));
            m_qpConstant[B_SLICE] = x265_clip3(QP_MIN, QP_MAX_MAX, (int)(m_param->rc.qp + m_pbOffset + 0.5));

            x265_log(m_param, X265_LOG_ERROR, "2nd pass has more frames than 1st pass (%d)\n", m_numEntries);
            x265_log(m_param, X265_LOG_ERROR, "continuing anyway, at constant QP=%d\n", m_param->rc.qp);
            if (m_param->bFrameAdaptive)
                x265_log(m_param, X265_LOG_ERROR, "disabling adaptive B-frames\n");

            m_isAbr = 0;
            m_2pass = 0;
            m_param->rc.rateControlMode = X265_RC_CQP;
            m_param->rc.bStatRead = 0;
            m_param->bFrameAdaptive = 0;
            m_param->scenecutThreshold = 0;
            m_param->rc.cuTree = 0;
            if (m_param->bframes > 1)
                m_param->bframes = 1;
            return X265_TYPE_AUTO;
        }
        int index = m_encOrder[frameNum];
        int frameType = m_rce2Pass[index].sliceType == I_SLICE ? (m_rce2Pass[index].isIdr ? X265_TYPE_IDR : X265_TYPE_I)
                        : m_rce2Pass[index].sliceType == P_SLICE ? X265_TYPE_P
                        : (m_rce2Pass[index].sliceType == B_SLICE && m_rce2Pass[index].keptAsRef ? X265_TYPE_BREF : X265_TYPE_B);
        return frameType;
    }
    else
        return X265_TYPE_AUTO;
}

void RateControl::initFramePredictors()
{
    /* Frame Predictors used in vbv */
    for (int i = 0; i < 4; i++)
    {
        m_pred[i].coeffMin = 1.0 / 4;
        m_pred[i].coeff = 1.0;
        m_pred[i].count = 1.0;
        m_pred[i].decay = 0.5;
        m_pred[i].offset = 0.0;
    }
    m_pred[0].coeff = m_pred[3].coeff = 0.75;
    m_pred[0].coeffMin = m_pred[3].coeffMin = 0.75 / 4;
    if (m_isGrainEnabled) // when tuned for grain 
    {
        m_pred[1].coeffMin = 0.75 / 4;
        m_pred[1].coeff = 0.75;
        m_pred[0].coeff = m_pred[3].coeff = 0.75;
        m_pred[0].coeffMin = m_pred[3].coeffMin = 0.75 / 4;
    }
}

int RateControl::rateControlStart(Frame* curFrame, RateControlEntry* rce, Encoder* enc)
{
    int orderValue = m_startEndOrder.get();
    int startOrdinal = rce->encodeOrder * 2;

    while (orderValue < startOrdinal && !m_bTerminated)
        orderValue = m_startEndOrder.waitForChange(orderValue);

    if (!curFrame)
    {
        // faked rateControlStart calls when the encoder is flushing
        m_startEndOrder.incr();
        return 0;
    }

    FrameData& curEncData = *curFrame->m_encData;
    m_curSlice = curEncData.m_slice;
    m_sliceType = m_curSlice->m_sliceType;
    rce->sliceType = m_sliceType;
    if (!m_2pass)
        rce->keptAsRef = IS_REFERENCED(curFrame);
    m_predType = getPredictorType(curFrame->m_lowres.sliceType, m_sliceType);
    rce->poc = m_curSlice->m_poc;
    if (m_param->rc.bStatRead)
    {
        X265_CHECK(rce->poc >= 0 && rce->poc < m_numEntries, "bad encode ordinal\n");
        int index = m_encOrder[rce->poc];
        copyRceData(rce, &m_rce2Pass[index]);
    }
    rce->isActive = true;
    rce->scenecut = false;
    bool isRefFrameScenecut = m_sliceType!= I_SLICE && m_curSlice->m_refFrameList[0][0]->m_lowres.bScenecut;
    m_isFirstMiniGop = m_sliceType == I_SLICE ? true : m_isFirstMiniGop;
    if (curFrame->m_lowres.bScenecut)
    {
        m_isSceneTransition = true;
        rce->scenecut = true;
        m_lastPredictorReset = rce->encodeOrder;

        initFramePredictors();
    }
    else if (m_sliceType != B_SLICE && !isRefFrameScenecut)
        m_isSceneTransition = false;

    if (rce->encodeOrder < m_lastPredictorReset + m_param->frameNumThreads)
    {
        rce->rowPreds[0][0].count = 0;
    }

    rce->bLastMiniGopBFrame = curFrame->m_lowres.bLastMiniGopBFrame;
    rce->bufferRate = m_bufferRate;
    rce->rowCplxrSum = 0.0;
    rce->rowTotalBits = 0;
    if (m_isVbv)
    {
        if (rce->rowPreds[0][0].count == 0)
        {
            for (int i = 0; i < 3; i++)
            {
                for (int j = 0; j < 2; j++)
                {
                    rce->rowPreds[i][j].coeffMin = 0.25 / 4;
                    rce->rowPreds[i][j].coeff = 0.25;
                    rce->rowPreds[i][j].count = 1.0;
                    rce->rowPreds[i][j].decay = 0.5;
                    rce->rowPreds[i][j].offset = 0.0;
                }
            }
        }
        rce->rowPred[0] = &rce->rowPreds[m_sliceType][0];
        rce->rowPred[1] = &rce->rowPreds[m_sliceType][1];
        m_predictedBits = m_totalBits;
        updateVbvPlan(enc);
        rce->bufferFill = m_bufferFill;

        int mincr = enc->m_vps.ptl.minCrForLevel;
        /* Profiles above Main10 don't require maxAU size check, so just set the maximum to a large value. */
        if (enc->m_vps.ptl.profileIdc > Profile::MAIN10 || enc->m_vps.ptl.levelIdc == Level::NONE)
            rce->frameSizeMaximum = 1e9;
        else
        {
            /* The spec has a special case for the first frame. */
            if (rce->encodeOrder == 0)
            {
                /* 1.5 * (Max( PicSizeInSamplesY, fR * MaxLumaSr) + MaxLumaSr * (AuCpbRemovalTime[ 0 ] -AuNominalRemovalTime[ 0 ])) ? MinCr */
                double fr = 1. / 300;
                int picSizeInSamplesY = m_param->sourceWidth * m_param->sourceHeight;
                rce->frameSizeMaximum = 8 * 1.5 * X265_MAX(picSizeInSamplesY, fr * enc->m_vps.ptl.maxLumaSrForLevel) / mincr;
            }
            else
            {
                /* 1.5 * MaxLumaSr * (AuCpbRemovalTime[ n ] - AuCpbRemovalTime[ n - 1 ]) / MinCr */
                rce->frameSizeMaximum = 8 * 1.5 * enc->m_vps.ptl.maxLumaSrForLevel * m_frameDuration / mincr;
            }
        }
    }
    if (!m_isAbr && m_2pass && m_param->rc.rateControlMode == X265_RC_CRF)
    {
        rce->qpPrev = x265_qScale2qp(rce->qScale);
        rce->qScale = rce->newQScale;
        rce->qpaRc = curEncData.m_avgQpRc = curEncData.m_avgQpAq = x265_qScale2qp(rce->newQScale);
        m_qp = int(rce->qpaRc + 0.5);
        rce->frameSizePlanned = qScale2bits(rce, rce->qScale);
        m_framesDone++;
        return m_qp;
    }

    if (m_isAbr || m_2pass) // ABR,CRF
    {
        if (m_isAbr || m_isVbv)
        {
            m_currentSatd = curFrame->m_lowres.satdCost >> (X265_DEPTH - 8);
            /* Update rce for use in rate control VBV later */
            rce->lastSatd = m_currentSatd;
            X265_CHECK(rce->lastSatd, "satdcost cannot be zero\n");
            /* Detect a pattern for B frames with same SATDcost to identify a series of static frames
             * and the P frame at the end of the series marks a possible case for ABR reset logic */
            if (m_param->bframes)
            {
                if (m_sliceType != B_SLICE && m_numBframesInPattern > m_param->bframes)
                {
                    m_isPatternPresent = true;
                }
                else if (m_sliceType == B_SLICE && !IS_REFERENCED(curFrame))
                {
                    if (m_currentSatd != m_lastBsliceSatdCost && !rce->bLastMiniGopBFrame)
                    {
                        m_isPatternPresent = false;
                        m_lastBsliceSatdCost = m_currentSatd;
                        m_numBframesInPattern = 0;
                    }
                    else if (m_currentSatd == m_lastBsliceSatdCost)
                        m_numBframesInPattern++;
                }
            }
        }
        /* For a scenecut that occurs within the mini-gop, enable scene transition
         * switch until the next mini-gop to ensure a min qp for all the frames within 
         * the scene-transition mini-gop */

        double q = x265_qScale2qp(rateEstimateQscale(curFrame, rce));
        q = x265_clip3((double)m_param->rc.qpMin, (double)m_param->rc.qpMax, q);
        m_qp = int(q + 0.5);
        q = m_isGrainEnabled ? m_qp : q;
        rce->qpaRc = curEncData.m_avgQpRc = curEncData.m_avgQpAq = q;
        /* copy value of lastRceq into thread local rce struct *to be used in RateControlEnd() */
        rce->qRceq = m_lastRceq;
        accumPQpUpdate();
        curFrame->m_rcData->cumulativePQp = m_accumPQp;
        curFrame->m_rcData->cumulativePNorm = m_accumPNorm;
        for (int i = 0; i < 3; i++)
            curFrame->m_rcData->lastQScaleFor[i] = m_lastQScaleFor[i];
        curFrame->m_rcData->shortTermCplxSum = m_shortTermCplxSum;
        curFrame->m_rcData->shortTermCplxCount = m_shortTermCplxCount;
    }
    else // CQP
    {
        if (m_sliceType == B_SLICE && IS_REFERENCED(curFrame))
            m_qp = (m_qpConstant[B_SLICE] + m_qpConstant[P_SLICE]) / 2;
        else
            m_qp = m_qpConstant[m_sliceType];
        curEncData.m_avgQpAq = curEncData.m_avgQpRc = m_qp;
        
        x265_zone* zone = getZone();
        if (zone)
        {
            if (zone->bForceQp)
                m_qp += zone->qp - m_qpConstant[P_SLICE];
            else
                m_qp -= (int)(6.0 * X265_LOG2(zone->bitrateFactor));
        }
    }
    if (m_sliceType != B_SLICE)
    {
        m_lastNonBPictType = m_sliceType;
        m_leadingNoBSatd = m_currentSatd;
    }
    rce->leadingNoBSatd = m_leadingNoBSatd;
    if (curFrame->m_forceqp)
    {
        m_qp = (int32_t)(curFrame->m_forceqp + 0.5) - 1;
        m_qp = x265_clip3(m_param->rc.qpMin, m_param->rc.qpMax, m_qp);
        rce->qpaRc = curEncData.m_avgQpRc = curEncData.m_avgQpAq = m_qp;
        if (m_isAbr || m_2pass)
        {
            rce->qpNoVbv = rce->qpaRc;
            m_lastQScaleFor[m_sliceType] = x265_qp2qScale(rce->qpaRc);
            if (rce->poc == 0)
                 m_lastQScaleFor[P_SLICE] = m_lastQScaleFor[m_sliceType] * fabs(m_param->rc.ipFactor);
            rce->frameSizePlanned = predictSize(&m_pred[m_predType], m_qp, (double)m_currentSatd);
        }
    }
    m_framesDone++;

    return m_qp;
}

void RateControl::accumPQpUpdate()
{
    m_accumPQp   *= .95;
    m_accumPNorm *= .95;
    m_accumPNorm += 1;
    if (m_sliceType == I_SLICE)
        m_accumPQp += m_qp + m_ipOffset;
    else
        m_accumPQp += m_qp;
}

int RateControl::getPredictorType(int lowresSliceType, int sliceType)
{
    /* Use a different predictor for B Ref and B frames for vbv frame size predictions */
    if (lowresSliceType == X265_TYPE_BREF)
        return 3;
    return sliceType;
}

double RateControl::getDiffLimitedQScale(RateControlEntry *rce, double q)
{
    // force I/B quants as a function of P quants
    const double lastPqScale    = m_lastQScaleFor[P_SLICE];
    const double lastNonBqScale = m_lastQScaleFor[m_lastNonBPictType];
    if (rce->sliceType == I_SLICE)
    {
        double iq = q;
        double pq = x265_qp2qScale(m_accumPQp / m_accumPNorm);
        double ipFactor = fabs(m_param->rc.ipFactor);
        /* don't apply ipFactor if the following frame is also I */
        if (m_accumPNorm <= 0)
            q = iq;
        else if (m_param->rc.ipFactor < 0)
            q = iq / ipFactor;
        else if (m_accumPNorm >= 1)
            q = pq / ipFactor;
        else
            q = m_accumPNorm * pq / ipFactor + (1 - m_accumPNorm) * iq;
    }
    else if (rce->sliceType == B_SLICE)
    {
        if (m_param->rc.pbFactor > 0)
            q = lastNonBqScale;
        if (!rce->keptAsRef)
            q *= fabs(m_param->rc.pbFactor);
    }
    else if (rce->sliceType == P_SLICE
             && m_lastNonBPictType == P_SLICE
             && rce->coeffBits == 0)
    {
        q = lastPqScale;
    }

    /* last qscale / qdiff stuff */
    if (m_lastNonBPictType == rce->sliceType &&
        (rce->sliceType != I_SLICE || m_lastAccumPNorm < 1))
    {
        double maxQscale = m_lastQScaleFor[rce->sliceType] * m_lstep;
        double minQscale = m_lastQScaleFor[rce->sliceType] / m_lstep;
        q = x265_clip3(minQscale, maxQscale, q);
    }

    m_lastQScaleFor[rce->sliceType] = q;
    if (rce->sliceType != B_SLICE)
        m_lastNonBPictType = rce->sliceType;
    if (rce->sliceType == I_SLICE)
    {
        m_lastAccumPNorm = m_accumPNorm;
        m_accumPNorm = 0;
        m_accumPQp = 0;
    }
    if (rce->sliceType == P_SLICE)
    {
        double mask = 1 - pow(rce->iCuCount / m_ncu, 2);
        m_accumPQp   = mask * (x265_qScale2qp(q) + m_accumPQp);
        m_accumPNorm = mask * (1 + m_accumPNorm);
    }

    return q;
}

double RateControl::countExpectedBits(int startPos, int endPos)
{
    double expectedBits = 0;
    for (int i = startPos; i <= endPos; i++)
    {
        RateControlEntry *rce = &m_rce2Pass[i];
        rce->expectedBits = (uint64_t)expectedBits;
        expectedBits += qScale2bits(rce, rce->newQScale);
    }
    return expectedBits;
}

bool RateControl::findUnderflow(double *fills, int *t0, int *t1, int over, int endPos)
{
    /* find an interval ending on an overflow or underflow (depending on whether
     * we're adding or removing bits), and starting on the earliest frame that
     * can influence the buffer fill of that end frame. */
    const double bufferMin = .1 * m_bufferSize;
    const double bufferMax = .9 * m_bufferSize;
    double fill = fills[*t0 - 1];
    double parity = over ? 1. : -1.;
    int start = -1, end = -1;
    for (int i = *t0; i <= endPos; i++)
    {
        fill += (m_frameDuration * m_vbvMaxRate -
                 qScale2bits(&m_rce2Pass[i], m_rce2Pass[i].newQScale)) * parity;
        fill = x265_clip3(0.0, m_bufferSize, fill);
        fills[i] = fill;
        if (fill <= bufferMin || i == 0)
        {
            if (end >= 0)
                break;
            start = i;
        }
        else if (fill >= bufferMax && start >= 0)
            end = i;
    }
    *t0 = start;
    *t1 = end;
    return start >= 0 && end >= 0;
}

bool RateControl::fixUnderflow(int t0, int t1, double adjustment, double qscaleMin, double qscaleMax)
{
    double qscaleOrig, qscaleNew;
    bool adjusted = false;
    if (t0 > 0)
        t0++;
    for (int i = t0; i <= t1; i++)
    {
        qscaleOrig = m_rce2Pass[i].newQScale;
        qscaleOrig = x265_clip3(qscaleMin, qscaleMax, qscaleOrig);
        qscaleNew  = qscaleOrig * adjustment;
        qscaleNew  = x265_clip3(qscaleMin, qscaleMax, qscaleNew);
        m_rce2Pass[i].newQScale = qscaleNew;
        adjusted = adjusted || (qscaleNew != qscaleOrig);
    }
    return adjusted;
}

bool RateControl::cuTreeReadFor2Pass(Frame* frame)
{
    int index = m_encOrder[frame->m_poc];
    uint8_t sliceTypeActual = (uint8_t)m_rce2Pass[index].sliceType;
    int ncu;
    if (m_param->rc.qgSize == 8)
        ncu = m_ncu * 4;
    else
        ncu = m_ncu;
    if (m_rce2Pass[index].keptAsRef)
    {
        /* TODO: We don't need pre-lookahead to measure AQ offsets, but there is currently
         * no way to signal this */
        uint8_t type;
        if (m_cuTreeStats.qpBufPos < 0)
        {
            do
            {
                m_cuTreeStats.qpBufPos++;

                if (!fread(&type, 1, 1, m_cutreeStatFileIn))
                    goto fail;
                if (fread(m_cuTreeStats.qpBuffer[m_cuTreeStats.qpBufPos], sizeof(uint16_t), ncu, m_cutreeStatFileIn) != (size_t)ncu)
                    goto fail;

                if (type != sliceTypeActual && m_cuTreeStats.qpBufPos == 1)
                {
                    x265_log(m_param, X265_LOG_ERROR, "CU-tree frametype %d doesn't match actual frametype %d.\n", type, sliceTypeActual);
                    return false;
                }
            }
            while(type != sliceTypeActual);
        }
        primitives.fix8Unpack(frame->m_lowres.qpCuTreeOffset, m_cuTreeStats.qpBuffer[m_cuTreeStats.qpBufPos], ncu);
        for (int i = 0; i < ncu; i++)
            frame->m_lowres.invQscaleFactor[i] = x265_exp2fix8(frame->m_lowres.qpCuTreeOffset[i]);
        m_cuTreeStats.qpBufPos--;
    }
    return true;

fail:
    x265_log(m_param, X265_LOG_ERROR, "Incomplete CU-tree stats file.\n");
    return false;
}

double RateControl::tuneAbrQScaleFromFeedback(double qScale)
{
    double abrBuffer = 2 * m_rateTolerance * m_bitrate;
    /* use framesDone instead of POC as poc count is not serial with bframes enabled */
    double overflow = 1.0;
    double timeDone = (double)(m_framesDone - m_param->frameNumThreads + 1) * m_frameDuration;
    double wantedBits = timeDone * m_bitrate;
    int64_t encodedBits = m_totalBits;
    if (m_param->totalFrames && m_param->totalFrames <= 2 * m_fps)
    {
        abrBuffer = m_param->totalFrames * (m_bitrate / m_fps);
        encodedBits = m_encodedBits;
    }

    if (wantedBits > 0 && encodedBits > 0 && (!m_partialResidualFrames || 
        m_param->rc.bStrictCbr || m_isGrainEnabled))
    {
        abrBuffer *= X265_MAX(1, sqrt(timeDone));
        overflow = x265_clip3(.5, 2.0, 1.0 + (encodedBits - wantedBits) / abrBuffer);
        qScale *= overflow;
    }
    return qScale;
}

double RateControl::tuneQScaleForGrain(double rcOverflow)
{
    double qpstep = rcOverflow > 1.1 ? rcOverflow : m_lstep;
    double qScaleAvg = x265_qp2qScale(m_avgPFrameQp);
    double  q = m_lastQScaleFor[P_SLICE];
    int curQp = int (x265_qScale2qp(m_lastQScaleFor[P_SLICE]) + 0.5);
    double curBitrate = m_qpToEncodedBits[curQp] * int(m_fps + 0.5);
    int newQp = rcOverflow > 1.1 ? curQp + 2 : rcOverflow > 1 ? curQp + 1 : curQp - 1 ;
    double projectedBitrate =  int(m_fps + 0.5) * m_qpToEncodedBits[newQp];
    if (curBitrate > 0 && projectedBitrate > 0)
        q =  abs(projectedBitrate - m_bitrate) < abs (curBitrate - m_bitrate) ? x265_qp2qScale(newQp) : m_lastQScaleFor[P_SLICE];
    else
        q = rcOverflow > 1 ? qScaleAvg * qpstep : rcOverflow < 1 ?  qScaleAvg / qpstep : m_lastQScaleFor[P_SLICE];
    return q;
}

double RateControl::rateEstimateQscale(Frame* curFrame, RateControlEntry *rce)
{
    double q;

    if (m_2pass)
    {
        if (m_sliceType != rce->sliceType)
        {
            x265_log(m_param, X265_LOG_ERROR, "slice=%c but 2pass stats say %c\n",
                     g_sliceTypeToChar[m_sliceType], g_sliceTypeToChar[rce->sliceType]);
        }
    }
    else
    {
        if (m_isAbr)
        {
            int pos = m_sliderPos % s_slidingWindowFrames;
            int addPos = (pos + s_slidingWindowFrames - 1) % s_slidingWindowFrames;
            if (m_sliderPos > s_slidingWindowFrames)
            {
                const static double base = pow(0.5, s_slidingWindowFrames - 1);
                m_movingAvgSum -= m_lastRemovedSatdCost * base;
                m_movingAvgSum *= 0.5;
                m_movingAvgSum += m_satdCostWindow[addPos];
            }
            else if (m_sliderPos == s_slidingWindowFrames)
            {
                m_movingAvgSum += m_satdCostWindow[addPos];
            }
            else if (m_sliderPos > 0)
            {
                m_movingAvgSum += m_satdCostWindow[addPos];
                m_movingAvgSum *= 0.5;
            }

            rce->movingAvgSum = m_movingAvgSum;
            m_lastRemovedSatdCost = m_satdCostWindow[pos];
            m_satdCostWindow[pos] = rce->lastSatd;
            m_sliderPos++;
        }
    }

    if (m_sliceType == B_SLICE)
    {
        /* B-frames don't have independent rate control, but rather get the
         * average QP of the two adjacent P-frames + an offset */
        Slice* prevRefSlice = m_curSlice->m_refFrameList[0][0]->m_encData->m_slice;
        Slice* nextRefSlice = m_curSlice->m_refFrameList[1][0]->m_encData->m_slice;
        double q0 = m_curSlice->m_refFrameList[0][0]->m_encData->m_avgQpRc;
        double q1 = m_curSlice->m_refFrameList[1][0]->m_encData->m_avgQpRc;
        bool i0 = prevRefSlice->m_sliceType == I_SLICE;
        bool i1 = nextRefSlice->m_sliceType == I_SLICE;
        int dt0 = abs(m_curSlice->m_poc - prevRefSlice->m_poc);
        int dt1 = abs(m_curSlice->m_poc - nextRefSlice->m_poc);

        // Skip taking a reference frame before the Scenecut if ABR has been reset.
        if (m_lastAbrResetPoc >= 0)
        {
            if (prevRefSlice->m_sliceType == P_SLICE && prevRefSlice->m_poc < m_lastAbrResetPoc)
            {
                i0 = i1;
                dt0 = dt1;
                q0 = q1;
            }
        }

        if (prevRefSlice->m_sliceType == B_SLICE && IS_REFERENCED(m_curSlice->m_refFrameList[0][0]))
            q0 -= m_pbOffset / 2;
        if (nextRefSlice->m_sliceType == B_SLICE && IS_REFERENCED(m_curSlice->m_refFrameList[1][0]))
            q1 -= m_pbOffset / 2;
        if (i0 && i1)
            q = (q0 + q1) / 2 + m_ipOffset;
        else if (i0)
            q = q1;
        else if (i1)
            q = q0;
        else if(m_isGrainEnabled && !m_2pass)
                q = q1;
            else
            q = (q0 * dt1 + q1 * dt0) / (dt0 + dt1);

        if (IS_REFERENCED(curFrame))
            q += m_pbOffset / 2;
        else
            q += m_pbOffset;

                /* Set a min qp at scenechanges and transitions */
        if (m_isSceneTransition)
        {
            q = X265_MAX(ABR_SCENECUT_INIT_QP_MIN, q);
            double minScenecutQscale =x265_qp2qScale(ABR_SCENECUT_INIT_QP_MIN); 
            m_lastQScaleFor[P_SLICE] = X265_MAX(minScenecutQscale, m_lastQScaleFor[P_SLICE]);
        }
        double qScale = x265_qp2qScale(q);
        rce->qpNoVbv = q;
        double lmin = 0, lmax = 0;
        if (m_isGrainEnabled && m_isFirstMiniGop)
        {
            lmin = m_lastQScaleFor[P_SLICE] / m_lstep;
            lmax = m_lastQScaleFor[P_SLICE] * m_lstep;
            double tunedQscale = tuneAbrQScaleFromFeedback(qScale);
            double overflow = tunedQscale / qScale;
            if (!m_isAbrReset)
                qScale = x265_clip3(lmin, lmax, qScale);
            m_avgPFrameQp = m_avgPFrameQp == 0 ? rce->qpNoVbv : m_avgPFrameQp;
            if (overflow != 1)
            {
                qScale = tuneQScaleForGrain(overflow);
                q = x265_qScale2qp(qScale);
            }
            rce->qpNoVbv = q;
        }
        if (m_isVbv)
        {
            lmin = m_lastQScaleFor[P_SLICE] / m_lstep;
            lmax = m_lastQScaleFor[P_SLICE] * m_lstep;

            if (m_isCbr && !m_isGrainEnabled)
            {
                qScale = tuneAbrQScaleFromFeedback(qScale);
                if (!m_isAbrReset)
                    qScale = x265_clip3(lmin, lmax, qScale);
                q = x265_qScale2qp(qScale);
            }
            if (!m_2pass)
            {
                qScale = clipQscale(curFrame, rce, qScale);
                /* clip qp to permissible range after vbv-lookahead estimation to avoid possible 
                 * mispredictions by initial frame size predictors */
                if (m_pred[m_predType].count == 1)
                    qScale = x265_clip3(lmin, lmax, qScale);
                m_lastQScaleFor[m_sliceType] = qScale;
            }
        }

        if (m_2pass)
            rce->frameSizePlanned = qScale2bits(rce, qScale);
        else
            rce->frameSizePlanned = predictSize(&m_pred[m_predType], qScale, (double)m_currentSatd);

        /* Limit planned size by MinCR */
        if (m_isVbv)
            rce->frameSizePlanned = X265_MIN(rce->frameSizePlanned, rce->frameSizeMaximum);
        rce->frameSizeEstimated = rce->frameSizePlanned;

        rce->newQScale = qScale;
        if(rce->bLastMiniGopBFrame)
        {
            if (m_isFirstMiniGop && m_isGrainEnabled)
            {
                m_avgPFrameQp = (m_avgPFrameQp + rce->qpNoVbv) / 2;
                m_lastQScaleFor[P_SLICE] = x265_qp2qScale(m_avgPFrameQp);
            }
            m_isFirstMiniGop = false;
        }
        return qScale;
    }
    else
    {
        double abrBuffer = 2 * m_rateTolerance * m_bitrate;
        if (m_2pass)
        {
            double lmin = m_lmin[m_sliceType];
            double lmax = m_lmax[m_sliceType];
            int64_t diff;
            if (!m_isVbv)
            {
                m_predictedBits = m_totalBits;
                if (rce->encodeOrder < m_param->frameNumThreads)
                    m_predictedBits += (int64_t)(rce->encodeOrder * m_bitrate / m_fps);
                else
                    m_predictedBits += (int64_t)(m_param->frameNumThreads * m_bitrate / m_fps);
            }
            /* Adjust ABR buffer based on distance to the end of the video. */
            if (m_numEntries > rce->encodeOrder)
            {
                uint64_t finalBits = m_rce2Pass[m_numEntries - 1].expectedBits;
                double videoPos = (double)rce->expectedBits / finalBits;
                double scaleFactor = sqrt((1 - videoPos) * m_numEntries);
                abrBuffer *= 0.5 * X265_MAX(scaleFactor, 0.5);
            }
            diff = m_predictedBits - (int64_t)rce->expectedBits;
            q = rce->newQScale;
            x265_zone* zone = getZone();
            if (zone)
            {
                if (zone->bForceQp)
                    q = x265_qp2qScale(zone->qp);
                else
                    q /= zone->bitrateFactor;
            }
            q /= x265_clip3(0.5, 2.0, (double)(abrBuffer - diff) / abrBuffer);
            if (m_expectedBitsSum > 0)
            {
                /* Adjust quant based on the difference between
                 * achieved and expected bitrate so far */
                double curTime = (double)rce->encodeOrder / m_numEntries;
                double w = x265_clip3(0.0, 1.0, curTime * 100);
                q *= pow((double)m_totalBits / m_expectedBitsSum, w);
            }
            if (m_framesDone == 0 && m_param->rc.rateControlMode == X265_RC_ABR && m_isGrainEnabled)
                q = X265_MIN(x265_qp2qScale(ABR_INIT_QP_GRAIN_MAX), q);
            rce->qpNoVbv = x265_qScale2qp(q);
            if ((m_sliceType == I_SLICE && m_param->keyframeMax > 1
                && m_lastNonBPictType != I_SLICE && !m_isAbrReset) || (m_isNextGop && !m_framesDone))
                m_avgPFrameQp = 0;
            if (m_sliceType == P_SLICE)
            {
                m_avgPFrameQp = m_avgPFrameQp == 0 ? rce->qpNoVbv : m_avgPFrameQp;
                m_avgPFrameQp = (m_avgPFrameQp + rce->qpNoVbv) / 2;
            }

            if (m_isVbv)
            {
                /* Do not overflow vbv */
                double expectedSize = qScale2bits(rce, q);
                double expectedVbv = m_bufferFill + m_bufferRate - expectedSize;
                double expectedFullness = rce->expectedVbv / m_bufferSize;
                double qmax = q * (2 - expectedFullness);
                double sizeConstraint = 1 + expectedFullness;
                qmax = X265_MAX(qmax, rce->newQScale);
                if (expectedFullness < .05)
                    qmax = lmax;
                qmax = X265_MIN(qmax, lmax);
                while (((expectedVbv < rce->expectedVbv/sizeConstraint) && (q < qmax)) ||
                        ((expectedVbv < 0) && (q < lmax)))
                {
                    q *= 1.05;
                    expectedSize = qScale2bits(rce, q);
                    expectedVbv = m_bufferFill + m_bufferRate - expectedSize;
                }
            }
            q = x265_clip3(lmin, lmax, q);
        }
        else
        {
            /* 1pass ABR */

            /* Calculate the quantizer which would have produced the desired
             * average bitrate if it had been applied to all frames so far.
             * Then modulate that quant based on the current frame's complexity
             * relative to the average complexity so far (using the 2pass RCEQ).
             * Then bias the quant up or down if total size so far was far from
             * the target.
             * Result: Depending on the value of rate_tolerance, there is a
             * trade-off between quality and bitrate precision. But at large
             * tolerances, the bit distribution approaches that of 2pass. */

            double overflow = 1;
            double lqmin = m_lmin[m_sliceType];
            double lqmax = m_lmax[m_sliceType];
            m_shortTermCplxSum *= 0.5;
            m_shortTermCplxCount *= 0.5;
            m_shortTermCplxSum += m_currentSatd / (CLIP_DURATION(m_frameDuration) / BASE_FRAME_DURATION);
            m_shortTermCplxCount++;
            /* coeffBits to be used in 2-pass */
            rce->coeffBits = (int)m_currentSatd;
            rce->blurredComplexity = m_shortTermCplxSum / m_shortTermCplxCount;
            rce->mvBits = 0;
            rce->sliceType = m_sliceType;

            if (m_param->rc.rateControlMode == X265_RC_CRF)
            {
                q = getQScale(rce, m_rateFactorConstant);
                x265_zone* zone = getZone();
                if (zone)
                {
                    if (zone->bForceQp)
                        q = x265_qp2qScale(zone->qp);
                    else
                        q /= zone->bitrateFactor;
                }
            }
            else
            {
                if (!m_param->rc.bStatRead)
                    checkAndResetABR(rce, false);
                double initialQScale = getQScale(rce, m_wantedBitsWindow / m_cplxrSum);
                x265_zone* zone = getZone();
                if (zone)
                {
                    if (zone->bForceQp)
                        initialQScale = x265_qp2qScale(zone->qp);
                    else
                        initialQScale /= zone->bitrateFactor;
                }
                double tunedQScale = tuneAbrQScaleFromFeedback(initialQScale);
                overflow = tunedQScale / initialQScale;
                q = !m_partialResidualFrames? tunedQScale : initialQScale;
                bool isEncodeEnd = (m_param->totalFrames && 
                    m_framesDone > 0.75 * m_param->totalFrames) ? 1 : 0;
                bool isEncodeBeg = m_framesDone < (int)(m_fps + 0.5);
                if (m_isGrainEnabled)
                {
                    if(m_sliceType!= I_SLICE && m_framesDone && !isEncodeEnd &&
                        ((overflow < 1.05 && overflow > 0.95) || isEncodeBeg))
                    {
                        q = tuneQScaleForGrain(overflow);
                    }
                }
            }
            if ((m_sliceType == I_SLICE && m_param->keyframeMax > 1
                && m_lastNonBPictType != I_SLICE && !m_isAbrReset) || (m_isNextGop && !m_framesDone))
            {
                if (!m_param->rc.bStrictCbr)
                    q = x265_qp2qScale(m_accumPQp / m_accumPNorm);
                q /= fabs(m_param->rc.ipFactor);
                m_avgPFrameQp = 0;
            }
            else if (m_framesDone > 0)
            {
                if (m_param->rc.rateControlMode != X265_RC_CRF)
                {
                    lqmin = m_lastQScaleFor[m_sliceType] / m_lstep;
                    lqmax = m_lastQScaleFor[m_sliceType] * m_lstep;
                    if (!m_partialResidualFrames || m_isGrainEnabled)
                    {
                        if (overflow > 1.1 && m_framesDone > 3)
                            lqmax *= m_lstep;
                        else if (overflow < 0.9)
                            lqmin /= m_lstep;
                    }
                    q = x265_clip3(lqmin, lqmax, q);
                }
            }
            else if (m_qCompress != 1 && m_param->rc.rateControlMode == X265_RC_CRF)
            {
                q = x265_qp2qScale(CRF_INIT_QP) / fabs(m_param->rc.ipFactor);
            }
            else if (m_framesDone == 0 && !m_isVbv && m_param->rc.rateControlMode == X265_RC_ABR)
            {
                /* for ABR alone, clip the first I frame qp */
                lqmax = (m_isGrainEnabled && m_lstep) ? x265_qp2qScale(ABR_INIT_QP_GRAIN_MAX) :
                        x265_qp2qScale(ABR_INIT_QP_MAX);
                q = X265_MIN(lqmax, q);
            }
            q = x265_clip3(lqmin, lqmax, q);
            /* Set a min qp at scenechanges and transitions */
            if (m_isSceneTransition)
            {
                double minScenecutQscale = x265_qp2qScale(ABR_SCENECUT_INIT_QP_MIN);
                q = X265_MAX(minScenecutQscale, q);
                m_lastQScaleFor[P_SLICE] = X265_MAX(minScenecutQscale, m_lastQScaleFor[P_SLICE]);
            }
            rce->qpNoVbv = x265_qScale2qp(q);
            if (m_sliceType == P_SLICE)
            {
                m_avgPFrameQp = m_avgPFrameQp == 0 ? rce->qpNoVbv : m_avgPFrameQp;
                m_avgPFrameQp = (m_avgPFrameQp + rce->qpNoVbv) / 2;
            }
            q = clipQscale(curFrame, rce, q);
            /*  clip qp to permissible range after vbv-lookahead estimation to avoid possible
             * mispredictions by initial frame size predictors, after each scenecut */
            bool isFrameAfterScenecut = m_sliceType!= I_SLICE && m_curSlice->m_refFrameList[0][0]->m_lowres.bScenecut;
            if (!m_2pass && m_isVbv && isFrameAfterScenecut)
                q = x265_clip3(lqmin, lqmax, q);
        }
        m_lastQScaleFor[m_sliceType] = q;
        if ((m_curSlice->m_poc == 0 || m_lastQScaleFor[P_SLICE] < q) && !(m_2pass && !m_isVbv))
            m_lastQScaleFor[P_SLICE] = q * fabs(m_param->rc.ipFactor);

        if (m_2pass)
            rce->frameSizePlanned = qScale2bits(rce, q);
        else
            rce->frameSizePlanned = predictSize(&m_pred[m_predType], q, (double)m_currentSatd);

        /* Always use up the whole VBV in this case. */
        if (m_singleFrameVbv)
            rce->frameSizePlanned = m_bufferRate;
        /* Limit planned size by MinCR */
        if (m_isVbv)
            rce->frameSizePlanned = X265_MIN(rce->frameSizePlanned, rce->frameSizeMaximum);
        rce->frameSizeEstimated = rce->frameSizePlanned;
        rce->newQScale = q;
        return q;
    }
}

void RateControl::rateControlUpdateStats(RateControlEntry* rce)
{
    if (!m_param->rc.bStatWrite && !m_param->rc.bStatRead)
    {
        if (rce->sliceType == I_SLICE)
        {
            /* previous I still had a residual; roll it into the new loan */
            if (m_partialResidualFrames)
                rce->rowTotalBits += m_partialResidualCost * m_partialResidualFrames;
            if ((m_param->totalFrames != 0) && (m_amortizeFrames > (m_param->totalFrames - m_framesDone)))
            {
                m_amortizeFrames = 0;
                m_amortizeFraction = 0;
            }
            else
            {
                double depreciateRate = 1.1;
                m_amortizeFrames = (int)(m_amortizeFrames / depreciateRate);
                m_amortizeFraction /= depreciateRate;
                m_amortizeFrames = X265_MAX(m_amortizeFrames, MIN_AMORTIZE_FRAME);
                m_amortizeFraction = X265_MAX(m_amortizeFraction, MIN_AMORTIZE_FRACTION);
            }
            rce->amortizeFrames = m_amortizeFrames;
            rce->amortizeFraction = m_amortizeFraction;
            m_partialResidualFrames = X265_MIN((int)rce->amortizeFrames, m_param->keyframeMax);
            m_partialResidualCost = (int)((rce->rowTotalBits * rce->amortizeFraction) / m_partialResidualFrames);
            rce->rowTotalBits -= m_partialResidualCost * m_partialResidualFrames;
        }
        else if (m_partialResidualFrames)
        {
             rce->rowTotalBits += m_partialResidualCost;
             m_partialResidualFrames--;
        }
    }
    if (rce->sliceType != B_SLICE)
        rce->rowCplxrSum = rce->rowTotalBits * x265_qp2qScale(rce->qpaRc) / rce->qRceq;
    else
        rce->rowCplxrSum = rce->rowTotalBits * x265_qp2qScale(rce->qpaRc) / (rce->qRceq * fabs(m_param->rc.pbFactor));

    m_cplxrSum += rce->rowCplxrSum;
    m_totalBits += rce->rowTotalBits;

    /* do not allow the next frame to enter rateControlStart() until this
     * frame has updated its mid-frame statistics */
    if (m_param->rc.rateControlMode == X265_RC_ABR || m_isVbv)
    {
        m_startEndOrder.incr();

        if (rce->encodeOrder < m_param->frameNumThreads - 1)
            m_startEndOrder.incr(); // faked rateControlEnd calls for negative frames
    }
}

void RateControl::checkAndResetABR(RateControlEntry* rce, bool isFrameDone)
{
    double abrBuffer = 2 * m_rateTolerance * m_bitrate;

    // Check if current Slice is a scene cut that follows low detailed/blank frames
    if (rce->lastSatd > 4 * rce->movingAvgSum || rce->scenecut)
    {
        if (!m_isAbrReset && rce->movingAvgSum > 0
            && (m_isPatternPresent || !m_param->bframes))
        {
            int pos = X265_MAX(m_sliderPos - m_param->frameNumThreads, 0);
            int64_t shrtTermWantedBits = (int64_t) (X265_MIN(pos, s_slidingWindowFrames) * m_bitrate * m_frameDuration);
            int64_t shrtTermTotalBitsSum = 0;
            // Reset ABR if prev frames are blank to prevent further sudden overflows/ high bit rate spikes.
            for (int i = 0; i < s_slidingWindowFrames ; i++)
                shrtTermTotalBitsSum += m_encodedBitsWindow[i];
            double underflow = (shrtTermTotalBitsSum - shrtTermWantedBits) / abrBuffer;
            const double epsilon = 0.0001f;
            if (underflow < epsilon && !isFrameDone)
            {
                init(*m_curSlice->m_sps);
                m_shortTermCplxSum = rce->lastSatd / (CLIP_DURATION(m_frameDuration) / BASE_FRAME_DURATION);
                m_shortTermCplxCount = 1;
                m_isAbrReset = true;
                m_lastAbrResetPoc = rce->poc;
            }
        }
        else if (m_isAbrReset && isFrameDone)
        {
            // Clear flag to reset ABR and continue as usual.
            m_isAbrReset = false;
        }
    }
}

void RateControl::hrdFullness(SEIBufferingPeriod *seiBP)
{
    const VUI* vui = &m_curSlice->m_sps->vuiParameters;
    const HRDInfo* hrd = &vui->hrdParameters;
    int num = 90000;
    int denom = hrd->bitRateValue << (hrd->bitRateScale + BR_SHIFT);
    int64_t cpbState = (int64_t)m_bufferFillFinal;
    int64_t cpbSize = (int64_t)hrd->cpbSizeValue << (hrd->cpbSizeScale + CPB_SHIFT);

    if (cpbState < 0 || cpbState > cpbSize)
    {
        x265_log(m_param, X265_LOG_WARNING, "CPB %s: %.0lf bits in a %.0lf-bit buffer\n",
                 cpbState < 0 ? "underflow" : "overflow", (float)cpbState, (float)cpbSize);
    }

    seiBP->m_initialCpbRemovalDelay = (uint32_t)(num * cpbState / denom);
    seiBP->m_initialCpbRemovalDelayOffset = (uint32_t)(num * cpbSize / denom - seiBP->m_initialCpbRemovalDelay);
}

void RateControl::updateVbvPlan(Encoder* enc)
{
    m_bufferFill = m_bufferFillFinal;
    enc->updateVbvPlan(this);
}

double RateControl::predictSize(Predictor *p, double q, double var)
{
    return (p->coeff * var + p->offset) / (q * p->count);
}

double RateControl::clipQscale(Frame* curFrame, RateControlEntry* rce, double q)
{
    // B-frames are not directly subject to VBV,
    // since they are controlled by referenced P-frames' QPs.
    double lmin = m_lmin[rce->sliceType];
    double lmax = m_lmax[rce->sliceType];
    double q0 = q;
    if (m_isVbv && m_currentSatd > 0 && curFrame)
    {
        if (m_param->lookaheadDepth || m_param->rc.cuTree ||
            m_param->scenecutThreshold ||
            (m_param->bFrameAdaptive && m_param->bframes))
        {
           /* Lookahead VBV: If lookahead is done, raise the quantizer as necessary
            * such that no frames in the lookahead overflow and such that the buffer
            * is in a reasonable state by the end of the lookahead. */
            int loopTerminate = 0;
            /* Avoid an infinite loop. */
            for (int iterations = 0; iterations < 1000 && loopTerminate != 3; iterations++)
            {
                double frameQ[3];
                double curBits;
                curBits = predictSize(&m_pred[m_predType], q, (double)m_currentSatd);
                double bufferFillCur = m_bufferFill - curBits;
                double targetFill;
                double totalDuration = m_frameDuration;
                frameQ[P_SLICE] = m_sliceType == I_SLICE ? q * m_param->rc.ipFactor : (m_sliceType == B_SLICE ? q / m_param->rc.pbFactor : q);
                frameQ[B_SLICE] = frameQ[P_SLICE] * m_param->rc.pbFactor;
                frameQ[I_SLICE] = frameQ[P_SLICE] / m_param->rc.ipFactor;
                /* Loop over the planned future frames. */
                for (int j = 0; bufferFillCur >= 0; j++)
                {
                    int type = curFrame->m_lowres.plannedType[j];
                    if (type == X265_TYPE_AUTO || totalDuration >= 1.0)
                        break;
                    totalDuration += m_frameDuration;
                    double wantedFrameSize = m_vbvMaxRate * m_frameDuration;
                    if (bufferFillCur + wantedFrameSize <= m_bufferSize)
                        bufferFillCur += wantedFrameSize;
                    int64_t satd = curFrame->m_lowres.plannedSatd[j] >> (X265_DEPTH - 8);
                    type = IS_X265_TYPE_I(type) ? I_SLICE : IS_X265_TYPE_B(type) ? B_SLICE : P_SLICE;
                    int predType = getPredictorType(curFrame->m_lowres.plannedType[j], type);
                    curBits = predictSize(&m_pred[predType], frameQ[type], (double)satd);
                    bufferFillCur -= curBits;
                }
                if (m_param->vbvBufferEnd && rce->encodeOrder >= m_param->vbvEndFrameAdjust * m_param->totalFrames)
                {
                    bool loopBreak = false;
                    double bufferDiff = m_param->vbvBufferEnd - (m_bufferFill / m_bufferSize);
                    targetFill = m_bufferFill + m_bufferSize * (bufferDiff / (m_param->totalFrames - rce->encodeOrder));
                    if (bufferFillCur < targetFill)
                    {
                        q *= 1.01;
                        loopTerminate |= 1;
                        loopBreak = true;
                    }
                    if (bufferFillCur > m_param->vbvBufferEnd * m_bufferSize)
                    {
                        q /= 1.01;
                        loopTerminate |= 2;
                        loopBreak = true;
                    }
                    if (!loopBreak)
                        break;
                }
                else
                {
                    /* Try to get the buffer at least 50% filled, but don't set an impossible goal. */
                    double finalDur = 1;
                    if (m_param->rc.bStrictCbr)
                    {
                        finalDur = x265_clip3(0.4, 1.0, totalDuration);
                    }
                    targetFill = X265_MIN(m_bufferFill + totalDuration * m_vbvMaxRate * 0.5, m_bufferSize * (1 - 0.5 * finalDur));
                    if (bufferFillCur < targetFill)
                    {
                        q *= 1.01;
                        loopTerminate |= 1;
                        continue;
                    }
                    /* Try to get the buffer not more than 80% filled, but don't set an impossible goal. */
                    targetFill = x265_clip3(m_bufferSize * (1 - 0.2 * finalDur), m_bufferSize, m_bufferFill - totalDuration * m_vbvMaxRate * 0.5);
                    if (m_isCbr && bufferFillCur > targetFill && !m_isSceneTransition)
                    {
                        q /= 1.01;
                        loopTerminate |= 2;
                        continue;
                    }
                    break;
                }
            }
            q = X265_MAX(q0 / 2, q);
        }
        else
        {
            /* Fallback to old purely-reactive algorithm: no lookahead. */
            if ((m_sliceType == P_SLICE || m_sliceType == B_SLICE ||
                    (m_sliceType == I_SLICE && m_lastNonBPictType == I_SLICE)) &&
                m_bufferFill / m_bufferSize < 0.5)
            {
                q /= x265_clip3(0.5, 1.0, 2.0 * m_bufferFill / m_bufferSize);
            }
            // Now a hard threshold to make sure the frame fits in VBV.
            // This one is mostly for I-frames.
            double bits = predictSize(&m_pred[m_predType], q, (double)m_currentSatd);

            // For small VBVs, allow the frame to use up the entire VBV.
            double maxFillFactor;
            maxFillFactor = m_bufferSize >= 5 * m_bufferRate ? 2 : 1;
            // For single-frame VBVs, request that the frame use up the entire VBV.
            double minFillFactor = m_singleFrameVbv ? 1 : 2;

            for (int iterations = 0; iterations < 10; iterations++)
            {
                double qf = 1.0;
                if (bits > m_bufferFill / maxFillFactor)
                    qf = x265_clip3(0.2, 1.0, m_bufferFill / (maxFillFactor * bits));
                q /= qf;
                bits *= qf;
                if (bits < m_bufferRate / minFillFactor)
                    q *= bits * minFillFactor / m_bufferRate;
                bits = predictSize(&m_pred[m_predType], q, (double)m_currentSatd);
            }

            q = X265_MAX(q0, q);
        }

        /* Apply MinCR restrictions */
        double pbits = predictSize(&m_pred[m_predType], q, (double)m_currentSatd);
        if (pbits > rce->frameSizeMaximum)
            q *= pbits / rce->frameSizeMaximum;
        /* To detect frames that are more complex in SATD costs compared to prev window, yet 
         * lookahead vbv reduces its qscale by half its value. Be on safer side and avoid drastic 
         * qscale reductions for frames high in complexity */
        bool mispredCheck = rce->movingAvgSum && m_currentSatd >= rce->movingAvgSum && q <= q0 / 2;
        if (!m_isCbr || (m_isAbr && mispredCheck))
            q = X265_MAX(q0, q);

        if (m_rateFactorMaxIncrement)
        {
            double qpNoVbv = x265_qScale2qp(q0);
            double qmax = X265_MIN(lmax,x265_qp2qScale(qpNoVbv + m_rateFactorMaxIncrement));
            return x265_clip3(lmin, qmax, q);
        }
    }
    if (m_2pass)
    {
        double min = log(lmin);
        double max = log(lmax);
        q = (log(q) - min) / (max - min) - 0.5;
        q = 1.0 / (1.0 + exp(-4 * q));
        q = q*(max - min) + min;
        return exp(q);
    }
    return x265_clip3(lmin, lmax, q);
}

double RateControl::predictRowsSizeSum(Frame* curFrame, RateControlEntry* rce, double qpVbv, int32_t& encodedBitsSoFar)
{
    uint32_t rowSatdCostSoFar = 0, totalSatdBits = 0;
    encodedBitsSoFar = 0;

    double qScale = x265_qp2qScale(qpVbv);
    FrameData& curEncData = *curFrame->m_encData;
    int picType = curEncData.m_slice->m_sliceType;
    Frame* refFrame = curEncData.m_slice->m_refFrameList[0][0];

    uint32_t maxRows = curEncData.m_slice->m_sps->numCuInHeight;
    uint32_t maxCols = curEncData.m_slice->m_sps->numCuInWidth;

    for (uint32_t row = 0; row < maxRows; row++)
    {
        encodedBitsSoFar += curEncData.m_rowStat[row].encodedBits;
        rowSatdCostSoFar = curEncData.m_rowStat[row].rowSatd;
        uint32_t satdCostForPendingCus = curEncData.m_rowStat[row].satdForVbv - rowSatdCostSoFar;
        satdCostForPendingCus >>= X265_DEPTH - 8;
        if (satdCostForPendingCus  > 0)
        {
            double pred_s = predictSize(rce->rowPred[0], qScale, satdCostForPendingCus);
            uint32_t refRowSatdCost = 0, refRowBits = 0, intraCostForPendingCus = 0;
            double refQScale = 0;

            if (picType != I_SLICE && !m_param->rc.bEnableConstVbv)
            {
                FrameData& refEncData = *refFrame->m_encData;
                uint32_t endCuAddr = maxCols * (row + 1);
                uint32_t startCuAddr = curEncData.m_rowStat[row].numEncodedCUs;
                if (startCuAddr)
                {
                    for (uint32_t cuAddr = startCuAddr + 1 ; cuAddr < endCuAddr; cuAddr++)
                    {
                        refRowSatdCost += refEncData.m_cuStat[cuAddr].vbvCost;
                        refRowBits += refEncData.m_cuStat[cuAddr].totalBits;
                    }
                }
                else
                {
                    refRowBits = refEncData.m_rowStat[row].encodedBits;
                    refRowSatdCost = refEncData.m_rowStat[row].satdForVbv;
                }

                refRowSatdCost >>= X265_DEPTH - 8;
                refQScale = refEncData.m_rowStat[row].rowQpScale;
            }

            if (picType == I_SLICE || qScale >= refQScale)
            {
                if (picType == P_SLICE 
                    && refFrame 
                    && refFrame->m_encData->m_slice->m_sliceType == picType
                    && refQScale > 0
                    && refRowBits > 0
                    && !m_param->rc.bEnableConstVbv)
                {
                    if (abs((int32_t)(refRowSatdCost - satdCostForPendingCus)) < (int32_t)satdCostForPendingCus / 2)
                    {
                        double predTotal = refRowBits * satdCostForPendingCus / refRowSatdCost * refQScale / qScale;
                        totalSatdBits += (int32_t)((pred_s + predTotal) * 0.5);
                        continue;
                    }
                }
                totalSatdBits += (int32_t)pred_s;
            }
            else if (picType == P_SLICE)
            {
                intraCostForPendingCus = curEncData.m_rowStat[row].intraSatdForVbv - curEncData.m_rowStat[row].rowIntraSatd;
                intraCostForPendingCus >>= X265_DEPTH - 8;
                /* Our QP is lower than the reference! */
                double pred_intra = predictSize(rce->rowPred[1], qScale, intraCostForPendingCus);
                /* Sum: better to overestimate than underestimate by using only one of the two predictors. */
                totalSatdBits += (int32_t)(pred_intra + pred_s);
            }
            else
                totalSatdBits += (int32_t)pred_s;
        }
    }

    return totalSatdBits + encodedBitsSoFar;
}

int RateControl::rowVbvRateControl(Frame* curFrame, uint32_t row, RateControlEntry* rce, double& qpVbv, uint32_t* m_sliceBaseRow, uint32_t sliceId)
{
    FrameData& curEncData = *curFrame->m_encData;
    double qScaleVbv = x265_qp2qScale(qpVbv);
    uint64_t rowSatdCost = curEncData.m_rowStat[row].rowSatd;
    double encodedBits = curEncData.m_rowStat[row].encodedBits;
    uint32_t rowInSlice = row - m_sliceBaseRow[sliceId];

    if (m_param->bEnableWavefront && rowInSlice == 1)
    {
        rowSatdCost += curEncData.m_rowStat[row - 1].rowSatd;
        encodedBits += curEncData.m_rowStat[row - 1].encodedBits;
    }
    rowSatdCost >>= X265_DEPTH - 8;
    updatePredictor(rce->rowPred[0], qScaleVbv, (double)rowSatdCost, encodedBits);
    if (curEncData.m_slice->m_sliceType != I_SLICE && !m_param->rc.bEnableConstVbv)
    {
        Frame* refFrame = curEncData.m_slice->m_refFrameList[0][0];
        if (qpVbv < refFrame->m_encData->m_rowStat[row].rowQp)
        {
            uint64_t intraRowSatdCost = curEncData.m_rowStat[row].rowIntraSatd;
            if (m_param->bEnableWavefront && rowInSlice == 1)
                intraRowSatdCost += curEncData.m_rowStat[row - 1].rowIntraSatd;
            intraRowSatdCost >>= X265_DEPTH - 8;
            updatePredictor(rce->rowPred[1], qScaleVbv, (double)intraRowSatdCost, encodedBits);
        }
    }

    int canReencodeRow = 1;
    /* tweak quality based on difference from predicted size */
    double prevRowQp = qpVbv;
    double qpAbsoluteMax = m_param->rc.qpMax;
    double qpAbsoluteMin = m_param->rc.qpMin;
    if (m_rateFactorMaxIncrement)
        qpAbsoluteMax = X265_MIN(qpAbsoluteMax, rce->qpNoVbv + m_rateFactorMaxIncrement);

    if (m_rateFactorMaxDecrement)
        qpAbsoluteMin = X265_MAX(qpAbsoluteMin, rce->qpNoVbv - m_rateFactorMaxDecrement);

    double qpMax = X265_MIN(prevRowQp + m_param->rc.qpStep, qpAbsoluteMax);
    double qpMin = X265_MAX(prevRowQp - m_param->rc.qpStep, qpAbsoluteMin);
    double stepSize = 0.5;
    double bufferLeftPlanned = rce->bufferFill - rce->frameSizePlanned;

    const SPS& sps = *curEncData.m_slice->m_sps;
    double maxFrameError = X265_MAX(0.05, 1.0 / sps.numCuInHeight);

    if (row < m_sliceBaseRow[sliceId + 1] - 1)
    {
        /* More threads means we have to be more cautious in letting ratecontrol use up extra bits. */
        double rcTol = bufferLeftPlanned / m_param->frameNumThreads * m_rateTolerance;
        int32_t encodedBitsSoFar = 0;
        double accFrameBits = predictRowsSizeSum(curFrame, rce, qpVbv, encodedBitsSoFar);

        /* * Don't increase the row QPs until a sufficent amount of the bits of
         * the frame have been processed, in case a flat area at the top of the
         * frame was measured inaccurately. */
        if (encodedBitsSoFar < 0.05f * rce->frameSizePlanned)
            qpMax = qpAbsoluteMax = prevRowQp;

        if (rce->sliceType != I_SLICE || (m_param->rc.bStrictCbr && rce->poc > 0))
            rcTol *= 0.5;

        if (!m_isCbr)
            qpMin = X265_MAX(qpMin, rce->qpNoVbv);

        double totalBitsNeeded = m_wantedBitsWindow;
        if (m_param->totalFrames)
            totalBitsNeeded = (m_param->totalFrames * m_bitrate) / m_fps;
        double abrOvershoot = (accFrameBits + m_totalBits - m_wantedBitsWindow) / totalBitsNeeded;

        while (qpVbv < qpMax
               && (((accFrameBits > rce->frameSizePlanned + rcTol) ||
                   (rce->bufferFill - accFrameBits < bufferLeftPlanned * 0.5) ||
                   (accFrameBits > rce->frameSizePlanned && qpVbv < rce->qpNoVbv))
                   && (!m_param->rc.bStrictCbr ? 1 : abrOvershoot > 0.1)))
        {
            qpVbv += stepSize;
            accFrameBits = predictRowsSizeSum(curFrame, rce, qpVbv, encodedBitsSoFar);
            abrOvershoot = (accFrameBits + m_totalBits - m_wantedBitsWindow) / totalBitsNeeded;
        }

        while (qpVbv > qpMin
               && (qpVbv > curEncData.m_rowStat[0].rowQp || m_singleFrameVbv)
               && (((accFrameBits < rce->frameSizePlanned * 0.8f && qpVbv <= prevRowQp)
                   || accFrameBits < (rce->bufferFill - m_bufferSize + m_bufferRate) * 1.1)
                   && (!m_param->rc.bStrictCbr ? 1 : abrOvershoot < 0)))
        {
            qpVbv -= stepSize;
            accFrameBits = predictRowsSizeSum(curFrame, rce, qpVbv, encodedBitsSoFar);
            abrOvershoot = (accFrameBits + m_totalBits - m_wantedBitsWindow) / totalBitsNeeded;
        }

        if (m_param->rc.bStrictCbr && m_param->totalFrames)
        {
            double timeDone = (double)(m_framesDone) / m_param->totalFrames;
            while (qpVbv < qpMax && (qpVbv < rce->qpNoVbv + (m_param->rc.qpStep * timeDone)) &&
                   (timeDone > 0.75 && abrOvershoot > 0))
            {
                qpVbv += stepSize;
                accFrameBits = predictRowsSizeSum(curFrame, rce, qpVbv, encodedBitsSoFar);
                abrOvershoot = (accFrameBits + m_totalBits - m_wantedBitsWindow) / totalBitsNeeded;
            }
            if (qpVbv > curEncData.m_rowStat[0].rowQp &&
                abrOvershoot < -0.1 && timeDone > 0.5 && accFrameBits < rce->frameSizePlanned - rcTol)
            {
                qpVbv -= stepSize;
                accFrameBits = predictRowsSizeSum(curFrame, rce, qpVbv, encodedBitsSoFar);
            }
        }

        /* avoid VBV underflow or MinCr violation */
        while ((qpVbv < qpAbsoluteMax)
               && ((rce->bufferFill - accFrameBits < m_bufferRate * maxFrameError) ||
                   (rce->frameSizeMaximum - accFrameBits < rce->frameSizeMaximum * maxFrameError)))
        {
            qpVbv += stepSize;
            accFrameBits = predictRowsSizeSum(curFrame, rce, qpVbv, encodedBitsSoFar);
        }

        rce->frameSizeEstimated = accFrameBits;

        /* If the current row was large enough to cause a large QP jump, try re-encoding it. */
        if (qpVbv > qpMax && prevRowQp < qpMax && canReencodeRow)
        {
            /* Bump QP to halfway in between... close enough. */
            qpVbv = x265_clip3(prevRowQp + 1.0f, qpMax, (prevRowQp + qpVbv) * 0.5);
            return -1;
        }

        if (m_param->rc.rfConstantMin)
        {
            if (qpVbv < qpMin && prevRowQp > qpMin && canReencodeRow)
            {
                qpVbv = x265_clip3(qpMin, prevRowQp, (prevRowQp + qpVbv) * 0.5);
                return -1;
            }
        }
    }
    else
    {
        int32_t encodedBitsSoFar = 0;
        rce->frameSizeEstimated = predictRowsSizeSum(curFrame, rce, qpVbv, encodedBitsSoFar);

        /* Last-ditch attempt: if the last row of the frame underflowed the VBV,
         * try again. */
        if ((rce->frameSizeEstimated > (rce->bufferFill - m_bufferRate * maxFrameError) &&
             qpVbv < qpMax && canReencodeRow))
        {
            qpVbv = qpMax;
            return -1;
        }
    }
    return 0;
}

/* modify the bitrate curve from pass1 for one frame */
double RateControl::getQScale(RateControlEntry *rce, double rateFactor)
{
    double q;

    if (m_param->rc.cuTree)
    {
        // Scale and units are obtained from rateNum and rateDenom for videos with fixed frame rates.
        double timescale = (double)m_param->fpsDenom / (2 * m_param->fpsNum);
        q = pow(BASE_FRAME_DURATION / CLIP_DURATION(2 * timescale), 1 - m_param->rc.qCompress);
    }
    else
        q = pow(rce->blurredComplexity, 1 - m_param->rc.qCompress);
    // avoid NaN's in the Rceq
    if (rce->coeffBits + rce->mvBits == 0)
        q = m_lastQScaleFor[rce->sliceType];
    else
    {
        m_lastRceq = q;
        q /= rateFactor;
    }

    return q;
}

void RateControl::updatePredictor(Predictor *p, double q, double var, double bits)
{
    if (var < 10)
        return;
    const double range = 2;
    double old_coeff = p->coeff / p->count;
    double old_offset = p->offset / p->count;
    double new_coeff = X265_MAX((bits * q - old_offset) / var, p->coeffMin );
    double new_coeff_clipped = x265_clip3(old_coeff / range, old_coeff * range, new_coeff);
    double new_offset = bits * q - new_coeff_clipped * var;
    if (new_offset >= 0)
        new_coeff = new_coeff_clipped;
    else
        new_offset = 0;
    p->count  *= p->decay;
    p->coeff  *= p->decay;
    p->offset *= p->decay;
    p->count++;
    p->coeff  += new_coeff;
    p->offset += new_offset;
}

int RateControl::updateVbv(int64_t bits, RateControlEntry* rce)
{
    int predType = rce->sliceType;
    int filler = 0;
    double bufferBits;
    predType = rce->sliceType == B_SLICE && rce->keptAsRef ? 3 : predType;
    if (rce->lastSatd >= m_ncu && rce->encodeOrder >= m_lastPredictorReset)
        updatePredictor(&m_pred[predType], x265_qp2qScale(rce->qpaRc), (double)rce->lastSatd, (double)bits);
    if (!m_isVbv)
        return 0;

    m_bufferFillFinal -= bits;

    if (m_bufferFillFinal < 0)
        x265_log(m_param, X265_LOG_WARNING, "poc:%d, VBV underflow (%.0f bits)\n", rce->poc, m_bufferFillFinal);

    m_bufferFillFinal = X265_MAX(m_bufferFillFinal, 0);
    m_bufferFillFinal += m_bufferRate;

    if (m_param->rc.bStrictCbr)
    {
        if (m_bufferFillFinal > m_bufferSize)
        {
            filler = (int)(m_bufferFillFinal - m_bufferSize);
            filler += FILLER_OVERHEAD * 8;
        }
        m_bufferFillFinal -= filler;
        bufferBits = X265_MIN(bits + filler + m_bufferExcess, m_bufferRate);
        m_bufferExcess = X265_MAX(m_bufferExcess - bufferBits + bits + filler, 0);
        m_bufferFillActual += bufferBits - bits - filler;
    }
    else
    {
        m_bufferFillFinal = X265_MIN(m_bufferFillFinal, m_bufferSize);
        bufferBits = X265_MIN(bits + m_bufferExcess, m_bufferRate);
        m_bufferExcess = X265_MAX(m_bufferExcess - bufferBits + bits, 0);
        m_bufferFillActual += bufferBits - bits;
        m_bufferFillActual = X265_MIN(m_bufferFillActual, m_bufferSize);
    }
    return filler;
}

/* After encoding one frame, update rate control state */
int RateControl::rateControlEnd(Frame* curFrame, int64_t bits, RateControlEntry* rce, int *filler)
{
    int orderValue = m_startEndOrder.get();
    int endOrdinal = (rce->encodeOrder + m_param->frameNumThreads) * 2 - 1;
    while (orderValue < endOrdinal && !m_bTerminated)
    {
        /* no more frames are being encoded, so fake the start event if we would
         * have blocked on it. Note that this does not enforce rateControlEnd()
         * ordering during flush, but this has no impact on the outputs */
        if (m_finalFrameCount && orderValue >= 2 * m_finalFrameCount)
            break;
        orderValue = m_startEndOrder.waitForChange(orderValue);
    }

    FrameData& curEncData = *curFrame->m_encData;
    int64_t actualBits = bits;
    Slice *slice = curEncData.m_slice;

    if (m_param->rc.aqMode || m_isVbv || m_param->bAQMotion)
    {
        if (m_isVbv && !(m_2pass && m_param->rc.rateControlMode == X265_RC_CRF))
        {
            double avgQpRc = 0;
            /* determine avg QP decided by VBV rate control */
            for (uint32_t i = 0; i < slice->m_sps->numCuInHeight; i++)
                avgQpRc += curEncData.m_rowStat[i].sumQpRc;

            avgQpRc /= slice->m_sps->numCUsInFrame;
            curEncData.m_avgQpRc = x265_clip3((double)m_param->rc.qpMin, (double)m_param->rc.qpMax, avgQpRc);
            rce->qpaRc = curEncData.m_avgQpRc;
        }

        if (m_param->rc.aqMode || m_param->bAQMotion)
        {
            double avgQpAq = 0;
            /* determine actual avg encoded QP, after AQ/cutree adjustments */
            for (uint32_t i = 0; i < slice->m_sps->numCuInHeight; i++)
                avgQpAq += curEncData.m_rowStat[i].sumQpAq;

            avgQpAq /= (slice->m_sps->numCUsInFrame * m_param->num4x4Partitions);
            curEncData.m_avgQpAq = avgQpAq;
        }
        else
            curEncData.m_avgQpAq = curEncData.m_avgQpRc;
    }

    if (m_isAbr)
    {
        if (m_param->rc.rateControlMode == X265_RC_ABR && !m_param->rc.bStatRead)
            checkAndResetABR(rce, true);
    }
    if (m_param->rc.rateControlMode == X265_RC_CRF)
    {
        double crfVal, qpRef = curEncData.m_avgQpRc;
        bool is2passCrfChange = false;
        if (m_2pass)
        {
            if (fabs(curEncData.m_avgQpRc - rce->qpPrev) > 0.1)
            {
                qpRef = rce->qpPrev;
                is2passCrfChange = true;
            }
        }
        if (is2passCrfChange || fabs(qpRef - rce->qpNoVbv) > 0.5)
        {
            double crfFactor = rce->qRceq /x265_qp2qScale(qpRef);
            double baseCplx = m_ncu * (m_param->bframes ? 120 : 80);
            double mbtree_offset = m_param->rc.cuTree ? (1.0 - m_param->rc.qCompress) * 13.5 : 0;
            crfVal = x265_qScale2qp(pow(baseCplx, 1 - m_qCompress) / crfFactor) - mbtree_offset;
        }
        else
            crfVal = rce->sliceType == I_SLICE ? m_param->rc.rfConstant - m_ipOffset : 
            (rce->sliceType == B_SLICE ? m_param->rc.rfConstant + m_pbOffset : m_param->rc.rfConstant);

        curEncData.m_rateFactor = crfVal;
    }

    if (m_isAbr && !m_isAbrReset)
    {
        /* amortize part of each I slice over the next several frames, up to
         * keyint-max, to avoid over-compensating for the large I slice cost */
        if (!m_param->rc.bStatWrite && !m_param->rc.bStatRead)
        {
            if (rce->sliceType == I_SLICE)
            {
                /* previous I still had a residual; roll it into the new loan */
                if (m_residualFrames)
                    bits += m_residualCost * m_residualFrames;
                m_residualFrames = X265_MIN((int)rce->amortizeFrames, m_param->keyframeMax);
                m_residualCost = (int)((bits * rce->amortizeFraction) / m_residualFrames);
                bits -= m_residualCost * m_residualFrames;
            }
            else if (m_residualFrames)
            {
                bits += m_residualCost;
                m_residualFrames--;
            }
        }
        if (rce->sliceType != B_SLICE)
        {
            /* The factor 1.5 is to tune up the actual bits, otherwise the cplxrSum is scaled too low
                * to improve short term compensation for next frame. */
            m_cplxrSum += (bits * x265_qp2qScale(rce->qpaRc) / rce->qRceq) - (rce->rowCplxrSum);
        }
        else
        {
            /* Depends on the fact that B-frame's QP is an offset from the following P-frame's.
                * Not perfectly accurate with B-refs, but good enough. */
            m_cplxrSum += (bits * x265_qp2qScale(rce->qpaRc) / (rce->qRceq * fabs(m_param->rc.pbFactor))) - (rce->rowCplxrSum);
        }
        m_wantedBitsWindow += m_frameDuration * m_bitrate;
        m_totalBits += bits - rce->rowTotalBits;
        m_encodedBits += actualBits;
        int pos = m_sliderPos - m_param->frameNumThreads;
        if (pos >= 0)
            m_encodedBitsWindow[pos % s_slidingWindowFrames] = actualBits;
        if(rce->sliceType != I_SLICE)
        {
            int qp = int (rce->qpaRc + 0.5);
            m_qpToEncodedBits[qp] =  m_qpToEncodedBits[qp] == 0 ? actualBits : (m_qpToEncodedBits[qp] + actualBits) * 0.5;
        }
        curFrame->m_rcData->wantedBitsWindow = m_wantedBitsWindow;
        curFrame->m_rcData->cplxrSum = m_cplxrSum;
        curFrame->m_rcData->totalBits = m_totalBits;
        curFrame->m_rcData->encodedBits = m_encodedBits;
    }

    if (m_2pass)
    {
        m_expectedBitsSum += qScale2bits(rce, x265_qp2qScale(rce->newQp));
        m_totalBits += bits - rce->rowTotalBits;
    }

    if (m_isVbv)
    {
        *filler = updateVbv(actualBits, rce);

        curFrame->m_rcData->bufferFillFinal = m_bufferFillFinal;
        for (int i = 0; i < 4; i++)
        {
            curFrame->m_rcData->coeff[i] = m_pred[i].coeff;
            curFrame->m_rcData->count[i] = m_pred[i].count;
            curFrame->m_rcData->offset[i] = m_pred[i].offset;
        }
        if (m_param->bEmitHRDSEI)
        {
            const VUI *vui = &curEncData.m_slice->m_sps->vuiParameters;
            const HRDInfo *hrd = &vui->hrdParameters;
            const TimingInfo *time = &vui->timingInfo;
            if (!curFrame->m_poc)
            {
                // first access unit initializes the HRD
                rce->hrdTiming->cpbInitialAT = 0;
                rce->hrdTiming->cpbRemovalTime = m_nominalRemovalTime = (double)m_bufPeriodSEI.m_initialCpbRemovalDelay / 90000;
            }
            else
            {
                rce->hrdTiming->cpbRemovalTime = m_nominalRemovalTime + (double)rce->picTimingSEI->m_auCpbRemovalDelay * time->numUnitsInTick / time->timeScale;
                double cpbEarliestAT = rce->hrdTiming->cpbRemovalTime - (double)m_bufPeriodSEI.m_initialCpbRemovalDelay / 90000;
                if (!curFrame->m_lowres.bKeyframe)
                    cpbEarliestAT -= (double)m_bufPeriodSEI.m_initialCpbRemovalDelayOffset / 90000;

                rce->hrdTiming->cpbInitialAT = hrd->cbrFlag ? m_prevCpbFinalAT : X265_MAX(m_prevCpbFinalAT, cpbEarliestAT);
            }
            int filler_bits = *filler ? (*filler - START_CODE_OVERHEAD * 8)  : 0; 
            uint32_t cpbsizeUnscale = hrd->cpbSizeValue << (hrd->cpbSizeScale + CPB_SHIFT);
            rce->hrdTiming->cpbFinalAT = m_prevCpbFinalAT = rce->hrdTiming->cpbInitialAT + (actualBits + filler_bits)/ cpbsizeUnscale;
            rce->hrdTiming->dpbOutputTime = (double)rce->picTimingSEI->m_picDpbOutputDelay * time->numUnitsInTick / time->timeScale + rce->hrdTiming->cpbRemovalTime;
        }
    }
    rce->isActive = false;
    // Allow rateControlStart of next frame only when rateControlEnd of previous frame is over
    m_startEndOrder.incr();
    return 0;
}

/* called to write out the rate control frame stats info in multipass encodes */
int RateControl::writeRateControlFrameStats(Frame* curFrame, RateControlEntry* rce)
{
    FrameData& curEncData = *curFrame->m_encData;
    int ncu;
    if (m_param->rc.qgSize == 8)
        ncu = m_ncu * 4;
    else
        ncu = m_ncu;
    char cType = rce->sliceType == I_SLICE ? (curFrame->m_lowres.sliceType == X265_TYPE_IDR ? 'I' : 'i')
        : rce->sliceType == P_SLICE ? 'P'
        : IS_REFERENCED(curFrame) ? 'B' : 'b';
    
    if (!curEncData.m_param->bMultiPassOptRPS)
    {
        if (fprintf(m_statFileOut,
            "in:%d out:%d type:%c q:%.2f q-aq:%.2f q-noVbv:%.2f q-Rceq:%.2f tex:%d mv:%d misc:%d icu:%.2f pcu:%.2f scu:%.2f ;\n",
            rce->poc, rce->encodeOrder,
            cType, curEncData.m_avgQpRc, curEncData.m_avgQpAq,
            rce->qpNoVbv, rce->qRceq,
            curFrame->m_encData->m_frameStats.coeffBits,
            curFrame->m_encData->m_frameStats.mvBits,
            curFrame->m_encData->m_frameStats.miscBits,
            curFrame->m_encData->m_frameStats.percent8x8Intra * m_ncu,
            curFrame->m_encData->m_frameStats.percent8x8Inter * m_ncu,
            curFrame->m_encData->m_frameStats.percent8x8Skip  * m_ncu) < 0)
            goto writeFailure;
    }
    else
    {
        RPS* rpsWriter = &curFrame->m_encData->m_slice->m_rps;
        int i, num = rpsWriter->numberOfPictures;
        char deltaPOC[128];
        char bUsed[40];
        memset(deltaPOC, 0, sizeof(deltaPOC));
        memset(bUsed, 0, sizeof(bUsed));
        sprintf(deltaPOC, "deltapoc:~");
        sprintf(bUsed, "bused:~");

        for (i = 0; i < num; i++)
        {
            sprintf(deltaPOC, "%s%d~", deltaPOC, rpsWriter->deltaPOC[i]);
            sprintf(bUsed, "%s%d~", bUsed, rpsWriter->bUsed[i]);
        }

        if (fprintf(m_statFileOut,
            "in:%d out:%d type:%c q:%.2f q-aq:%.2f q-noVbv:%.2f q-Rceq:%.2f tex:%d mv:%d misc:%d icu:%.2f pcu:%.2f scu:%.2f nump:%d numnegp:%d numposp:%d %s %s ;\n",
            rce->poc, rce->encodeOrder,
            cType, curEncData.m_avgQpRc, curEncData.m_avgQpAq,
            rce->qpNoVbv, rce->qRceq,
            curFrame->m_encData->m_frameStats.coeffBits,
            curFrame->m_encData->m_frameStats.mvBits,
            curFrame->m_encData->m_frameStats.miscBits,
            curFrame->m_encData->m_frameStats.percent8x8Intra * m_ncu,
            curFrame->m_encData->m_frameStats.percent8x8Inter * m_ncu,
            curFrame->m_encData->m_frameStats.percent8x8Skip  * m_ncu,
            rpsWriter->numberOfPictures,
            rpsWriter->numberOfNegativePictures,
            rpsWriter->numberOfPositivePictures,
            deltaPOC, bUsed) < 0)
            goto writeFailure;
    }
    /* Don't re-write the data in multi-pass mode. */
    if (m_param->rc.cuTree && IS_REFERENCED(curFrame) && !m_param->rc.bStatRead)
    {
        uint8_t sliceType = (uint8_t)rce->sliceType;
        primitives.fix8Pack(m_cuTreeStats.qpBuffer[0], curFrame->m_lowres.qpCuTreeOffset, ncu);
        if (fwrite(&sliceType, 1, 1, m_cutreeStatFileOut) < 1)
            goto writeFailure;
        if (fwrite(m_cuTreeStats.qpBuffer[0], sizeof(uint16_t), ncu, m_cutreeStatFileOut) < (size_t)ncu)
            goto writeFailure;
    }
    return 0;

    writeFailure:
    x265_log(m_param, X265_LOG_ERROR, "RatecontrolEnd: stats file write failure\n");
    return 1;
}
#if defined(_MSC_VER)
#pragma warning(disable: 4996) // POSIX function names are just fine, thank you
#endif

/* called when the encoder is flushing, and thus the final frame count is
 * unambiguously known */
void RateControl::setFinalFrameCount(int count)
{
    m_finalFrameCount = count;
    /* unblock waiting threads */
    m_startEndOrder.poke();
}

/* called when the encoder is closing, and no more frames will be output.
 * all blocked functions must finish so the frame encoder threads can be
 * closed */
void RateControl::terminate()
{
    m_bTerminated = true;
    /* unblock waiting threads */
    m_startEndOrder.poke();
}

void RateControl::destroy()
{
    const char *fileName = m_param->rc.statFileName;
    if (!fileName)
        fileName = s_defaultStatFileName;

    if (m_statFileOut)
    {
        fclose(m_statFileOut);
        char *tmpFileName = strcatFilename(fileName, ".temp");
        int bError = 1;
        if (tmpFileName)
        {
            x265_unlink(fileName);
            bError = x265_rename(tmpFileName, fileName);
        }
        if (bError)
        {
            x265_log_file(m_param, X265_LOG_ERROR, "failed to rename output stats file to \"%s\"\n", fileName);
        }
        X265_FREE(tmpFileName);
    }

    if (m_cutreeStatFileOut)
    {
        fclose(m_cutreeStatFileOut);
        char *tmpFileName = strcatFilename(fileName, ".cutree.temp");
        char *newFileName = strcatFilename(fileName, ".cutree");
        int bError = 1;
        if (tmpFileName && newFileName)
        {
            x265_unlink(newFileName);
            bError = x265_rename(tmpFileName, newFileName);
        }
        if (bError)
        {
            x265_log_file(m_param, X265_LOG_ERROR, "failed to rename cutree output stats file to \"%s\"\n", newFileName);
        }
        X265_FREE(tmpFileName);
        X265_FREE(newFileName);
    }

    if (m_cutreeStatFileIn)
        fclose(m_cutreeStatFileIn);

    X265_FREE(m_rce2Pass);
    X265_FREE(m_encOrder);
    for (int i = 0; i < 2; i++)
        X265_FREE(m_cuTreeStats.qpBuffer[i]);
    
    X265_FREE(m_param->rc.zones);
}

void RateControl::splitdeltaPOC(char deltapoc[], RateControlEntry *rce)
{
    int idx = 0, length = 0;
    char tmpStr[128];
    char* src = deltapoc;
    char* buf = strstr(src, "~");
    while (buf)
    {
        memset(tmpStr, 0, sizeof(tmpStr));
        length = (int)(buf - src);
        if (length != 0)
        {
            strncpy(tmpStr, src, length);
            rce->rpsData.deltaPOC[idx] = atoi(tmpStr);
            idx++;
            if (idx == rce->rpsData.numberOfPictures)
                break;
        }
        src += (length + 1);
        buf = strstr(src, "~");
    }
}

void RateControl::splitbUsed(char bused[], RateControlEntry *rce)
{
    int idx = 0, length = 0;
    char tmpStr[128];
    char* src = bused;
    char* buf = strstr(src, "~");
    while (buf)
    {
        memset(tmpStr, 0, sizeof(tmpStr));
        length = (int)(buf - src);
        if (length != 0)
        {
            strncpy(tmpStr, src, length);
            rce->rpsData.bUsed[idx] = atoi(tmpStr) > 0;
            idx++;
            if (idx == rce->rpsData.numberOfPictures)
                break;
        }
        src += (length + 1);
        buf = strstr(src, "~");
    }
}
