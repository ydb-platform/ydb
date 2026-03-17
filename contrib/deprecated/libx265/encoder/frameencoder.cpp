/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Chung Shin Yee <shinyee@multicorewareinc.com>
 *          Min Chen <chenm003@163.com>
 *          Steve Borho <steve@borho.org>
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
#include "frame.h"
#include "framedata.h"
#include "wavefront.h"
#include "param.h"

#include "encoder.h"
#include "frameencoder.h"
#include "common.h"
#include "slicetype.h"
#include "nal.h"

namespace X265_NS {
void weightAnalyse(Slice& slice, Frame& frame, x265_param& param);

FrameEncoder::FrameEncoder()
{
    m_prevOutputTime = x265_mdate();
    m_reconfigure = false;
    m_isFrameEncoder = true;
    m_threadActive = true;
    m_slicetypeWaitTime = 0;
    m_activeWorkerCount = 0;
    m_completionCount = 0;
    m_bAllRowsStop = false;
    m_vbvResetTriggerRow = -1;
    m_outStreams = NULL;
    m_backupStreams = NULL;
    m_substreamSizes = NULL;
    m_nr = NULL;
    m_tld = NULL;
    m_rows = NULL;
    m_top = NULL;
    m_param = NULL;
    m_frame = NULL;
    m_cuGeoms = NULL;
    m_ctuGeomMap = NULL;
    m_localTldIdx = 0;
    memset(&m_rce, 0, sizeof(RateControlEntry));
}

void FrameEncoder::destroy()
{
    if (m_pool)
    {
        if (!m_jpId)
        {
            int numTLD = m_pool->m_numWorkers;
            if (!m_param->bEnableWavefront)
                numTLD += m_pool->m_numProviders;
            for (int i = 0; i < numTLD; i++)
                m_tld[i].destroy();
            delete [] m_tld;
        }
    }
    else
    {
        m_tld->destroy();
        delete m_tld;
    }

    delete[] m_rows;
    delete[] m_outStreams;
    delete[] m_backupStreams;
    X265_FREE(m_sliceBaseRow);
    X265_FREE(m_sliceMaxBlockRow);
    X265_FREE(m_cuGeoms);
    X265_FREE(m_ctuGeomMap);
    X265_FREE(m_substreamSizes);
    X265_FREE(m_nr);

    m_frameFilter.destroy();

    if (m_param->bEmitHRDSEI || !!m_param->interlaceMode)
    {
        delete m_rce.picTimingSEI;
        delete m_rce.hrdTiming;
    }
}

bool FrameEncoder::init(Encoder *top, int numRows, int numCols)
{
    m_top = top;
    m_param = top->m_param;
    m_numRows = numRows;
    m_numCols = numCols;
    m_reconfigure = false;
    m_filterRowDelay = ((m_param->bEnableSAO && m_param->bSaoNonDeblocked)
                        || (!m_param->bEnableLoopFilter && m_param->bEnableSAO)) ?
                        2 : (m_param->bEnableSAO || m_param->bEnableLoopFilter ? 1 : 0);
    m_filterRowDelayCus = m_filterRowDelay * numCols;
    m_rows = new CTURow[m_numRows];
    bool ok = !!m_numRows;

    m_sliceBaseRow = X265_MALLOC(uint32_t, m_param->maxSlices + 1);
    ok &= !!m_sliceBaseRow;
    m_sliceGroupSize = (uint16_t)(m_numRows + m_param->maxSlices - 1) / m_param->maxSlices;
    uint32_t sliceGroupSizeAccu = (m_numRows << 8) / m_param->maxSlices;    
    uint32_t rowSum = sliceGroupSizeAccu;
    uint32_t sidx = 0;
    for (uint32_t i = 0; i < m_numRows; i++)
    {
        const uint32_t rowRange = (rowSum >> 8);
        if ((i >= rowRange) & (sidx != m_param->maxSlices - 1))
        {
            rowSum += sliceGroupSizeAccu;
            m_sliceBaseRow[++sidx] = i;
        }        
    }
    X265_CHECK(sidx < m_param->maxSlices, "sliceID check failed!");
    m_sliceBaseRow[0] = 0;
    m_sliceBaseRow[m_param->maxSlices] = m_numRows;

    m_sliceMaxBlockRow = X265_MALLOC(uint32_t, m_param->maxSlices + 1);
    ok &= !!m_sliceMaxBlockRow;
    uint32_t maxBlockRows = (m_param->sourceHeight + (16 - 1)) / 16;
    sliceGroupSizeAccu = (maxBlockRows << 8) / m_param->maxSlices;
    rowSum = sliceGroupSizeAccu;
    sidx = 0;
    for (uint32_t i = 0; i < maxBlockRows; i++)
    {
        const uint32_t rowRange = (rowSum >> 8);
        if ((i >= rowRange) & (sidx != m_param->maxSlices - 1))
        {
            rowSum += sliceGroupSizeAccu;
            m_sliceMaxBlockRow[++sidx] = i;
        }
    }
    m_sliceMaxBlockRow[0] = 0;
    m_sliceMaxBlockRow[m_param->maxSlices] = maxBlockRows;

    /* determine full motion search range */
    int range  = m_param->searchRange;       /* fpel search */
    range += !!(m_param->searchMethod < 2);  /* diamond/hex range check lag */
    range += NTAPS_LUMA / 2;                 /* subpel filter half-length */
    range += 2 + (MotionEstimate::hpelIterationCount(m_param->subpelRefine) + 1) / 2; /* subpel refine steps */
    m_refLagRows = /*(m_param->maxSlices > 1 ? 1 : 0) +*/ 1 + ((range + m_param->maxCUSize - 1) / m_param->maxCUSize);

    // NOTE: 2 times of numRows because both Encoder and Filter in same queue
    if (!WaveFront::init(m_numRows * 2))
    {
        x265_log(m_param, X265_LOG_ERROR, "unable to initialize wavefront queue\n");
        m_pool = NULL;
    }

    m_frameFilter.init(top, this, numRows, numCols);

    // initialize HRD parameters of SPS
    if (m_param->bEmitHRDSEI || !!m_param->interlaceMode)
    {
        m_rce.picTimingSEI = new SEIPictureTiming;
        m_rce.hrdTiming = new HRDTiming;

        ok &= m_rce.picTimingSEI && m_rce.hrdTiming;
    }

    if (m_param->noiseReductionIntra || m_param->noiseReductionInter || m_param->rc.vbvBufferSize)
        m_nr = X265_MALLOC(NoiseReduction, 1);
    if (m_nr)
        memset(m_nr, 0, sizeof(NoiseReduction));
    else
        m_param->noiseReductionIntra = m_param->noiseReductionInter = 0;

    // 7.4.7.1 - Ceil( Log2( PicSizeInCtbsY ) ) bits
    {
        unsigned long tmp;
        CLZ(tmp, (numRows * numCols - 1));
        m_sliceAddrBits = (uint16_t)(tmp + 1);
    }

    return ok;
}

/* Generate a complete list of unique geom sets for the current picture dimensions */
bool FrameEncoder::initializeGeoms()
{
    /* Geoms only vary between CTUs in the presence of picture edges */
    int maxCUSize = m_param->maxCUSize;
    int minCUSize = m_param->minCUSize;
    int heightRem = m_param->sourceHeight & (maxCUSize - 1);
    int widthRem = m_param->sourceWidth & (maxCUSize - 1);
    int allocGeoms = 1; // body
    if (heightRem && widthRem)
        allocGeoms = 4; // body, right, bottom, corner
    else if (heightRem || widthRem)
        allocGeoms = 2; // body, right or bottom

    m_ctuGeomMap = X265_MALLOC(uint32_t, m_numRows * m_numCols);
    m_cuGeoms = X265_MALLOC(CUGeom, allocGeoms * CUGeom::MAX_GEOMS);
    if (!m_cuGeoms || !m_ctuGeomMap)
        return false;

    // body
    CUData::calcCTUGeoms(maxCUSize, maxCUSize, maxCUSize, minCUSize, m_cuGeoms);
    memset(m_ctuGeomMap, 0, sizeof(uint32_t) * m_numRows * m_numCols);
    if (allocGeoms == 1)
        return true;

    int countGeoms = 1;
    if (widthRem)
    {
        // right
        CUData::calcCTUGeoms(widthRem, maxCUSize, maxCUSize, minCUSize, m_cuGeoms + countGeoms * CUGeom::MAX_GEOMS);
        for (uint32_t i = 0; i < m_numRows; i++)
        {
            uint32_t ctuAddr = m_numCols * (i + 1) - 1;
            m_ctuGeomMap[ctuAddr] = countGeoms * CUGeom::MAX_GEOMS;
        }
        countGeoms++;
    }
    if (heightRem)
    {
        // bottom
        CUData::calcCTUGeoms(maxCUSize, heightRem, maxCUSize, minCUSize, m_cuGeoms + countGeoms * CUGeom::MAX_GEOMS);
        for (uint32_t i = 0; i < m_numCols; i++)
        {
            uint32_t ctuAddr = m_numCols * (m_numRows - 1) + i;
            m_ctuGeomMap[ctuAddr] = countGeoms * CUGeom::MAX_GEOMS;
        }
        countGeoms++;

        if (widthRem)
        {
            // corner
            CUData::calcCTUGeoms(widthRem, heightRem, maxCUSize, minCUSize, m_cuGeoms + countGeoms * CUGeom::MAX_GEOMS);

            uint32_t ctuAddr = m_numCols * m_numRows - 1;
            m_ctuGeomMap[ctuAddr] = countGeoms * CUGeom::MAX_GEOMS;
            countGeoms++;
        }
        X265_CHECK(countGeoms == allocGeoms, "geometry match check failure\n");
    }

    return true;
}

bool FrameEncoder::startCompressFrame(Frame* curFrame)
{
    m_slicetypeWaitTime = x265_mdate() - m_prevOutputTime;
    m_frame = curFrame;
    m_sliceType = curFrame->m_lowres.sliceType;
    curFrame->m_encData->m_frameEncoderID = m_jpId;
    curFrame->m_encData->m_jobProvider = this;
    curFrame->m_encData->m_slice->m_mref = m_mref;

    if (!m_cuGeoms)
    {
        if (!initializeGeoms())
            return false;
    }

    m_enable.trigger();
    return true;
}

void FrameEncoder::threadMain()
{
    THREAD_NAME("Frame", m_jpId);

    if (m_pool)
    {
        m_pool->setCurrentThreadAffinity();

        /* the first FE on each NUMA node is responsible for allocating thread
         * local data for all worker threads in that pool. If WPP is disabled, then
         * each FE also needs a TLD instance */
        if (!m_jpId)
        {
            int numTLD = m_pool->m_numWorkers;
            if (!m_param->bEnableWavefront)
                numTLD += m_pool->m_numProviders;

            m_tld = new ThreadLocalData[numTLD];
            for (int i = 0; i < numTLD; i++)
            {
                m_tld[i].analysis.initSearch(*m_param, m_top->m_scalingList);
                m_tld[i].analysis.create(m_tld);
            }

            for (int i = 0; i < m_pool->m_numProviders; i++)
            {
                if (m_pool->m_jpTable[i]->m_isFrameEncoder) /* ugh; over-allocation and other issues here */
                {
                    FrameEncoder *peer = dynamic_cast<FrameEncoder*>(m_pool->m_jpTable[i]);
                    peer->m_tld = m_tld;
                }
            }
        }

        if (m_param->bEnableWavefront)
            m_localTldIdx = -1; // cause exception if used
        else
            m_localTldIdx = m_pool->m_numWorkers + m_jpId;
    }
    else
    {
        m_tld = new ThreadLocalData;
        m_tld->analysis.initSearch(*m_param, m_top->m_scalingList);
        m_tld->analysis.create(NULL);
        m_localTldIdx = 0;
    }

    m_done.trigger();     /* signal that thread is initialized */
    m_enable.wait();      /* Encoder::encode() triggers this event */

    while (m_threadActive)
    {
        if (m_param->bCTUInfo)
        {
            while (!m_frame->m_ctuInfo)
                m_frame->m_copied.wait();
        }
        if ((m_param->bMVType == AVC_INFO) && !m_param->analysisReuseMode && !(IS_X265_TYPE_I(m_frame->m_lowres.sliceType)))
        {
            while (((m_frame->m_analysisData.interData == NULL && m_frame->m_analysisData.intraData == NULL) || (uint32_t)m_frame->m_poc != m_frame->m_analysisData.poc))
                m_frame->m_copyMVType.wait();
        }
        compressFrame();
        m_done.trigger(); /* FrameEncoder::getEncodedPicture() blocks for this event */
        if (m_frame != NULL)
            m_frame->m_reconEncoded.trigger();
        m_enable.wait();
    }
}

void FrameEncoder::WeightAnalysis::processTasks(int /* workerThreadId */)
{
    Frame* frame = master.m_frame;
    weightAnalyse(*frame->m_encData->m_slice, *frame, *master.m_param);
}


uint32_t getBsLength( int32_t code )
{
    uint32_t ucode = (code <= 0) ? -code << 1 : (code << 1) - 1;

    ++ucode;
    unsigned long idx;
    CLZ( idx, ucode );
    uint32_t length = (uint32_t)idx * 2 + 1;

    return length;
}

void FrameEncoder::compressFrame()
{
    ProfileScopeEvent(frameThread);

    m_startCompressTime = x265_mdate();
    m_totalActiveWorkerCount = 0;
    m_activeWorkerCountSamples = 0;
    m_totalWorkerElapsedTime = 0;
    m_totalNoWorkerTime = 0;
    m_countRowBlocks = 0;
    m_allRowsAvailableTime = 0;
    m_stallStartTime = 0;

    m_completionCount = 0;
    m_bAllRowsStop = false;
    m_vbvResetTriggerRow = -1;
    m_rowSliceTotalBits[0] = 0;
    m_rowSliceTotalBits[1] = 0;

    m_SSDY = m_SSDU = m_SSDV = 0;
    m_ssim = 0;
    m_ssimCnt = 0;
    memset(&(m_frame->m_encData->m_frameStats), 0, sizeof(m_frame->m_encData->m_frameStats));

    /* Emit access unit delimiter unless this is the first frame and the user is
     * not repeating headers (since AUD is supposed to be the first NAL in the access
     * unit) */
    Slice* slice = m_frame->m_encData->m_slice;
    if (m_param->bEnableAccessUnitDelimiters && (m_frame->m_poc || m_param->bRepeatHeaders))
    {
        m_bs.resetBits();
        m_entropyCoder.setBitstream(&m_bs);
        m_entropyCoder.codeAUD(*slice);
        m_bs.writeByteAlignment();
        m_nalList.serialize(NAL_UNIT_ACCESS_UNIT_DELIMITER, m_bs);
    }
    if (m_frame->m_lowres.bKeyframe && m_param->bRepeatHeaders)
    {
        if (m_param->bOptRefListLengthPPS)
        {
            ScopedLock refIdxLock(m_top->m_sliceRefIdxLock);
            m_top->updateRefIdx();
        }
        if (m_top->m_param->rc.bStatRead  && m_top->m_param->bMultiPassOptRPS)
        {
            ScopedLock refIdxLock(m_top->m_rpsInSpsLock);
            if (!m_top->computeSPSRPSIndex())
            {
                x265_log(m_param, X265_LOG_ERROR, "compute commonly RPS failed!\n");
                m_top->m_aborted = true;
            }
            m_top->getStreamHeaders(m_nalList, m_entropyCoder, m_bs);
        }
        else
            m_top->getStreamHeaders(m_nalList, m_entropyCoder, m_bs);
    }

    if (m_top->m_param->rc.bStatRead && m_top->m_param->bMultiPassOptRPS)
        m_frame->m_encData->m_slice->m_rpsIdx = (m_top->m_rateControl->m_rce2Pass + m_frame->m_encodeOrder)->rpsIdx;

    // Weighted Prediction parameters estimation.
    bool bUseWeightP = slice->m_sliceType == P_SLICE && slice->m_pps->bUseWeightPred;
    bool bUseWeightB = slice->m_sliceType == B_SLICE && slice->m_pps->bUseWeightedBiPred;

    WeightParam* reuseWP = NULL;
    if (m_param->analysisReuseMode && (bUseWeightP || bUseWeightB))
        reuseWP = (WeightParam*)m_frame->m_analysisData.wt;

    if (bUseWeightP || bUseWeightB)
    {
#if DETAILED_CU_STATS
        m_cuStats.countWeightAnalyze++;
        ScopedElapsedTime time(m_cuStats.weightAnalyzeTime);
#endif
        if (m_param->analysisReuseMode == X265_ANALYSIS_LOAD)
        {
            for (int list = 0; list < slice->isInterB() + 1; list++) 
            {
                for (int plane = 0; plane < (m_param->internalCsp != X265_CSP_I400 ? 3 : 1); plane++)
                {
                    for (int ref = 1; ref < slice->m_numRefIdx[list]; ref++)
                        SET_WEIGHT(slice->m_weightPredTable[list][ref][plane], false, 1 << reuseWP->log2WeightDenom, reuseWP->log2WeightDenom, 0);
                    slice->m_weightPredTable[list][0][plane] = *(reuseWP++);
                }
            }
        }
        else
        {
            WeightAnalysis wa(*this);
            if (m_pool && wa.tryBondPeers(*this, 1))
                /* use an idle worker for weight analysis */
                wa.waitForExit();
            else
                weightAnalyse(*slice, *m_frame, *m_param);

        }

    }
    else
        slice->disableWeights();

    // Generate motion references
    int numPredDir = slice->isInterP() ? 1 : slice->isInterB() ? 2 : 0;
    for (int l = 0; l < numPredDir; l++)
    {
        for (int ref = 0; ref < slice->m_numRefIdx[l]; ref++)
        {
            WeightParam *w = NULL;
            if ((bUseWeightP || bUseWeightB) && slice->m_weightPredTable[l][ref][0].bPresentFlag)
                w = slice->m_weightPredTable[l][ref];
            slice->m_refReconPicList[l][ref] = slice->m_refFrameList[l][ref]->m_reconPic;
            m_mref[l][ref].init(slice->m_refReconPicList[l][ref], w, *m_param);
        }
        if (m_param->analysisReuseMode == X265_ANALYSIS_SAVE && (bUseWeightP || bUseWeightB))
        {
            for (int i = 0; i < (m_param->internalCsp != X265_CSP_I400 ? 3 : 1); i++)
                *(reuseWP++) = slice->m_weightPredTable[l][0][i];
        }

    }

    int numTLD;
    if (m_pool)
        numTLD = m_param->bEnableWavefront ? m_pool->m_numWorkers : m_pool->m_numWorkers + m_pool->m_numProviders;
    else
        numTLD = 1;

    /* Get the QP for this frame from rate control. This call may block until
     * frames ahead of it in encode order have called rateControlEnd() */
    m_rce.encodeOrder = m_frame->m_encodeOrder;
    bool payloadChange = false;
    bool writeSei = true;
    if (m_param->bDhdr10opt)
    {
        for (int i = 0; i < m_frame->m_userSEI.numPayloads; i++)
        {
            x265_sei_payload *payload = &m_frame->m_userSEI.payloads[i];
            if(payload->payloadType == USER_DATA_REGISTERED_ITU_T_T35)
            {
                if (m_top->m_prevTonemapPayload.payload != NULL && payload->payloadSize == m_top->m_prevTonemapPayload.payloadSize)
                {
                    if (memcmp(m_top->m_prevTonemapPayload.payload, payload->payload, payload->payloadSize) != 0)
                        payloadChange = true;
                }
                else
                {
                    payloadChange = true;
                    if (m_top->m_prevTonemapPayload.payload != NULL)
                        x265_free(m_top->m_prevTonemapPayload.payload);
                    m_top->m_prevTonemapPayload.payload = (uint8_t*)x265_malloc(sizeof(uint8_t) * payload->payloadSize);
                }

                if (payloadChange)
                {
                    m_top->m_prevTonemapPayload.payloadType = payload->payloadType;
                    m_top->m_prevTonemapPayload.payloadSize = payload->payloadSize;
                    memcpy(m_top->m_prevTonemapPayload.payload, payload->payload, payload->payloadSize);
                }

                bool isIDR = m_frame->m_lowres.sliceType == X265_TYPE_IDR;
                writeSei = payloadChange || isIDR;
            }
        }
    }
    int qp = m_top->m_rateControl->rateControlStart(m_frame, &m_rce, m_top);
    m_rce.newQp = qp;

    if (m_nr)
    {
        if (qp > QP_MAX_SPEC && m_frame->m_param->rc.vbvBufferSize)
        {
            for (int i = 0; i < numTLD; i++)
            {
                m_tld[i].analysis.m_quant.m_frameNr[m_jpId].offset = m_top->m_offsetEmergency[qp - QP_MAX_SPEC - 1];
                m_tld[i].analysis.m_quant.m_frameNr[m_jpId].residualSum = m_top->m_residualSumEmergency;
                m_tld[i].analysis.m_quant.m_frameNr[m_jpId].count = m_top->m_countEmergency;
            }
        }
        else
        {
            if (m_param->noiseReductionIntra || m_param->noiseReductionInter)
            {
                for (int i = 0; i < numTLD; i++)
                {
                    m_tld[i].analysis.m_quant.m_frameNr[m_jpId].offset = m_tld[i].analysis.m_quant.m_frameNr[m_jpId].nrOffsetDenoise;
                    m_tld[i].analysis.m_quant.m_frameNr[m_jpId].residualSum = m_tld[i].analysis.m_quant.m_frameNr[m_jpId].nrResidualSum;
                    m_tld[i].analysis.m_quant.m_frameNr[m_jpId].count = m_tld[i].analysis.m_quant.m_frameNr[m_jpId].nrCount;
                }
            }
            else
            {
                for (int i = 0; i < numTLD; i++)
                    m_tld[i].analysis.m_quant.m_frameNr[m_jpId].offset = NULL;
            }
        }
    }

    /* Clip slice QP to 0-51 spec range before encoding */
    slice->m_sliceQp = x265_clip3(-QP_BD_OFFSET, QP_MAX_SPEC, qp);
    if (m_param->bHDROpt)
    {
        int qpCb = x265_clip3(-12, 0, (int)floor((m_top->m_cB * ((-.46) * qp + 9.26)) + 0.5 ));
        int qpCr = x265_clip3(-12, 0, (int)floor((m_top->m_cR * ((-.46) * qp + 9.26)) + 0.5 ));
        slice->m_chromaQpOffset[0] = slice->m_pps->chromaQpOffset[0] + qpCb < -12 ? (qpCb + (-12 - (slice->m_pps->chromaQpOffset[0] + qpCb))) : qpCb;
        slice->m_chromaQpOffset[1] = slice->m_pps->chromaQpOffset[1] + qpCr < -12 ? (qpCr + (-12 - (slice->m_pps->chromaQpOffset[1] + qpCr))) : qpCr;
    }

    if (m_param->bOptQpPPS && m_param->bRepeatHeaders)
    {
        ScopedLock qpLock(m_top->m_sliceQpLock);
        for (int i = 0; i < (QP_MAX_MAX + 1); i++)
        {
            int delta = slice->m_sliceQp - (i + 1);
            int codeLength = getBsLength( delta );
            m_top->m_iBitsCostSum[i] += codeLength;
        }
        m_top->m_iFrameNum++;
    }
    m_initSliceContext.resetEntropy(*slice);

    m_frameFilter.start(m_frame, m_initSliceContext);

    /* ensure all rows are blocked prior to initializing row CTU counters */
    WaveFront::clearEnabledRowMask();

    /* reset entropy coders and compute slice id */
    m_entropyCoder.load(m_initSliceContext);
	
    for (uint32_t sliceId = 0; sliceId < m_param->maxSlices; sliceId++)   
        for (uint32_t row = m_sliceBaseRow[sliceId]; row < m_sliceBaseRow[sliceId + 1]; row++)
            m_rows[row].init(m_initSliceContext, sliceId);   

    // reset slice counter for rate control update
    m_sliceCnt = 0;

    uint32_t numSubstreams = m_param->bEnableWavefront ? slice->m_sps->numCuInHeight : m_param->maxSlices;
    X265_CHECK(m_param->bEnableWavefront || (m_param->maxSlices == 1), "Multiple slices without WPP unsupport now!");
    if (!m_outStreams)
    {
        m_outStreams = new Bitstream[numSubstreams];
        if (!m_param->bEnableWavefront)
            m_backupStreams = new Bitstream[numSubstreams];
        m_substreamSizes = X265_MALLOC(uint32_t, numSubstreams);
        if (!m_param->bEnableSAO)
            for (uint32_t i = 0; i < numSubstreams; i++)
                m_rows[i].rowGoOnCoder.setBitstream(&m_outStreams[i]);
    }
    else
    {
        for (uint32_t i = 0; i < numSubstreams; i++)
            m_outStreams[i].resetBits();
    }

    int prevBPSEI = m_rce.encodeOrder ? m_top->m_lastBPSEI : 0;

    if (m_frame->m_lowres.bKeyframe)
    {
        if (m_param->bEmitHRDSEI)
        {
            SEIBufferingPeriod* bpSei = &m_top->m_rateControl->m_bufPeriodSEI;

            // since the temporal layer HRD is not ready, we assumed it is fixed
            bpSei->m_auCpbRemovalDelayDelta = 1;
            bpSei->m_cpbDelayOffset = 0;
            bpSei->m_dpbDelayOffset = 0;

            // hrdFullness() calculates the initial CPB removal delay and offset
            m_top->m_rateControl->hrdFullness(bpSei);

            m_bs.resetBits();
            bpSei->write(m_bs, *slice->m_sps);
            m_bs.writeByteAlignment();

            m_nalList.serialize(NAL_UNIT_PREFIX_SEI, m_bs);

            m_top->m_lastBPSEI = m_rce.encodeOrder;
        }
    }

    if ((m_param->bEmitHRDSEI || !!m_param->interlaceMode))
    {
        SEIPictureTiming *sei = m_rce.picTimingSEI;
        const VUI *vui = &slice->m_sps->vuiParameters;
        const HRDInfo *hrd = &vui->hrdParameters;
        int poc = slice->m_poc;

        if (vui->frameFieldInfoPresentFlag)
        {
            if (m_param->interlaceMode == 2)
                sei->m_picStruct = (poc & 1) ? 1 /* top */ : 2 /* bottom */;
            else if (m_param->interlaceMode == 1)
                sei->m_picStruct = (poc & 1) ? 2 /* bottom */ : 1 /* top */;
            else
                sei->m_picStruct = 0;
            sei->m_sourceScanType = 0;
            sei->m_duplicateFlag = false;
        }

        if (vui->hrdParametersPresentFlag)
        {
            // The m_aucpbremoval delay specifies how many clock ticks the
            // access unit associated with the picture timing SEI message has to
            // wait after removal of the access unit with the most recent
            // buffering period SEI message
            sei->m_auCpbRemovalDelay = X265_MIN(X265_MAX(1, m_rce.encodeOrder - prevBPSEI), (1 << hrd->cpbRemovalDelayLength));
            sei->m_picDpbOutputDelay = slice->m_sps->numReorderPics + poc - m_rce.encodeOrder;
        }

        m_bs.resetBits();
        sei->write(m_bs, *slice->m_sps);
        m_bs.writeByteAlignment();
        m_nalList.serialize(NAL_UNIT_PREFIX_SEI, m_bs);
    }

    /* Write user SEI */
    for (int i = 0; i < m_frame->m_userSEI.numPayloads; i++)
    {
        x265_sei_payload *payload = &m_frame->m_userSEI.payloads[i];
        if (payload->payloadType == USER_DATA_UNREGISTERED)
        {
            SEIuserDataUnregistered sei;
            sei.m_userData = payload->payload;
            m_bs.resetBits();
            sei.setSize(payload->payloadSize);
            sei.write(m_bs, *slice->m_sps);
            m_bs.writeByteAlignment();
            m_nalList.serialize(NAL_UNIT_PREFIX_SEI, m_bs);
        }
        else if (payload->payloadType == USER_DATA_REGISTERED_ITU_T_T35)
        {
            if (writeSei)
            {
                SEICreativeIntentMeta sei;
                sei.m_payload = payload->payload;
                m_bs.resetBits();
                sei.setSize(payload->payloadSize);
                sei.write(m_bs, *slice->m_sps);
                m_bs.writeByteAlignment();
                m_nalList.serialize(NAL_UNIT_PREFIX_SEI, m_bs);
            }
        }
        else
            x265_log(m_param, X265_LOG_ERROR, "Unrecognized SEI type\n");
    }
    /* CQP and CRF (without capped VBV) doesn't use mid-frame statistics to 
     * tune RateControl parameters for other frames.
     * Hence, for these modes, update m_startEndOrder and unlock RC for previous threads waiting in
     * RateControlEnd here, after the slice contexts are initialized. For the rest - ABR
     * and VBV, unlock only after rateControlUpdateStats of this frame is called */
    if (m_param->rc.rateControlMode != X265_RC_ABR && !m_top->m_rateControl->m_isVbv)
    {
        m_top->m_rateControl->m_startEndOrder.incr();

        if (m_rce.encodeOrder < m_param->frameNumThreads - 1)
            m_top->m_rateControl->m_startEndOrder.incr(); // faked rateControlEnd calls for negative frames
    }

    /* Analyze CTU rows, most of the hard work is done here.  Frame is
     * compressed in a wave-front pattern if WPP is enabled. Row based loop
     * filters runs behind the CTU compression and reconstruction */

    for (uint32_t sliceId = 0; sliceId < m_param->maxSlices; sliceId++)    
        m_rows[m_sliceBaseRow[sliceId]].active = true;
    
    if (m_param->bEnableWavefront)
    {
        int i = 0;
        for (uint32_t rowInSlice = 0; rowInSlice < m_sliceGroupSize; rowInSlice++)
        {
            for (uint32_t sliceId = 0; sliceId < m_param->maxSlices; sliceId++)
            {
                const uint32_t sliceStartRow = m_sliceBaseRow[sliceId];
                const uint32_t sliceEndRow = m_sliceBaseRow[sliceId + 1] - 1;
                const uint32_t row = sliceStartRow + rowInSlice;
                if (row > sliceEndRow)
                    continue;
                m_row_to_idx[row] = i;
                m_idx_to_row[i] = row;
                i += 1;
            }
        }
    }

    if (m_param->bEnableWavefront)
    {
        for (uint32_t rowInSlice = 0; rowInSlice < m_sliceGroupSize; rowInSlice++)
        {
            for (uint32_t sliceId = 0; sliceId < m_param->maxSlices; sliceId++)
            {
                const uint32_t sliceStartRow = m_sliceBaseRow[sliceId];
                const uint32_t sliceEndRow = m_sliceBaseRow[sliceId + 1] - 1;
                const uint32_t row = sliceStartRow + rowInSlice;

                X265_CHECK(row < m_numRows, "slices row fault was detected");

                if (row > sliceEndRow)
                    continue;

                // block until all reference frames have reconstructed the rows we need
                for (int l = 0; l < numPredDir; l++)
                {
                    for (int ref = 0; ref < slice->m_numRefIdx[l]; ref++)
                    {
                        Frame *refpic = slice->m_refFrameList[l][ref];

                        // NOTE: we unnecessary wait row that beyond current slice boundary
                        const int rowIdx = X265_MIN(sliceEndRow, (row + m_refLagRows));

                        while (refpic->m_reconRowFlag[rowIdx].get() == 0)
                            refpic->m_reconRowFlag[rowIdx].waitForChange(0);

                        if ((bUseWeightP || bUseWeightB) && m_mref[l][ref].isWeighted)
                            m_mref[l][ref].applyWeight(rowIdx, m_numRows, sliceEndRow, sliceId);
                    }
                }

                enableRowEncoder(m_row_to_idx[row]); /* clear external dependency for this row */
                if (!rowInSlice)
                {
                    m_row0WaitTime = x265_mdate();
                    enqueueRowEncoder(m_row_to_idx[row]); /* clear internal dependency, start wavefront */
                }
                tryWakeOne();
            } // end of loop rowInSlice
        } // end of loop sliceId

        m_allRowsAvailableTime = x265_mdate();
        tryWakeOne(); /* ensure one thread is active or help-wanted flag is set prior to blocking */
        static const int block_ms = 250;
        while (m_completionEvent.timedWait(block_ms))
            tryWakeOne();
    }
    else
    {
        for (uint32_t i = 0; i < m_numRows + m_filterRowDelay; i++)
        {
            // compress
            if (i < m_numRows)
            {
                // block until all reference frames have reconstructed the rows we need
                for (int l = 0; l < numPredDir; l++)
                {
                    int list = l;
                    for (int ref = 0; ref < slice->m_numRefIdx[list]; ref++)
                    {
                        Frame *refpic = slice->m_refFrameList[list][ref];

                        const int rowIdx = X265_MIN(m_numRows - 1, (i + m_refLagRows));
                        while (refpic->m_reconRowFlag[rowIdx].get() == 0)
                            refpic->m_reconRowFlag[rowIdx].waitForChange(0);

                        if ((bUseWeightP || bUseWeightB) && m_mref[l][ref].isWeighted)
                            m_mref[list][ref].applyWeight(rowIdx, m_numRows, m_numRows, 0);
                    }
                }

                if (!i)
                    m_row0WaitTime = x265_mdate();
                else if (i == m_numRows - 1)
                    m_allRowsAvailableTime = x265_mdate();
                processRowEncoder(i, m_tld[m_localTldIdx]);
            }

            // filter
            if (i >= m_filterRowDelay)
                m_frameFilter.processRow(i - m_filterRowDelay);
        }
    }

    if (m_param->maxSlices > 1)
    {
        PicYuv *reconPic = m_frame->m_reconPic;
        uint32_t height = reconPic->m_picHeight;
        uint32_t width = reconPic->m_picWidth;
        intptr_t stride = reconPic->m_stride;
        const uint32_t hChromaShift = CHROMA_H_SHIFT(m_param->internalCsp);
        const uint32_t vChromaShift = CHROMA_V_SHIFT(m_param->internalCsp);

        if (m_param->decodedPictureHashSEI == 1)
        {

            MD5Init(&m_state[0]);

            updateMD5Plane(m_state[0], reconPic->m_picOrg[0], width, height, stride);

            if (m_param->internalCsp != X265_CSP_I400)
            {
                MD5Init(&m_state[1]);
                MD5Init(&m_state[2]);

                width >>= hChromaShift;
                height >>= vChromaShift;
                stride = reconPic->m_strideC;

                updateMD5Plane(m_state[1], reconPic->m_picOrg[1], width, height, stride);
                updateMD5Plane(m_state[2], reconPic->m_picOrg[2], width, height, stride);
            }
        }
        // TODO: NOT verify code in below mode
        else if (m_param->decodedPictureHashSEI == 2)
        {
            m_crc[0] = 0xffff;

            updateCRC(reconPic->m_picOrg[0], m_crc[0], height, width, stride);

            if (m_param->internalCsp != X265_CSP_I400)
            {
                width >>= hChromaShift;
                height >>= vChromaShift;
                stride = reconPic->m_strideC;
                m_crc[1] = m_crc[2] = 0xffff;

                updateCRC(reconPic->m_picOrg[1], m_crc[1], height, width, stride);
                updateCRC(reconPic->m_picOrg[2], m_crc[2], height, width, stride);
            }
        }
        else if (m_param->decodedPictureHashSEI == 3)
        {
            uint32_t cuHeight = m_param->maxCUSize;

            m_checksum[0] = 0;

            updateChecksum(reconPic->m_picOrg[0], m_checksum[0], height, width, stride, 0, cuHeight);

            if (m_param->internalCsp != X265_CSP_I400)
            {
                width >>= hChromaShift;
                height >>= vChromaShift;
                stride = reconPic->m_strideC;
                cuHeight >>= vChromaShift;

                m_checksum[1] = m_checksum[2] = 0;

                updateChecksum(reconPic->m_picOrg[1], m_checksum[1], height, width, stride, 0, cuHeight);
                updateChecksum(reconPic->m_picOrg[2], m_checksum[2], height, width, stride, 0, cuHeight);
            }
        }
    } // end of (m_param->maxSlices > 1)

    if (m_param->rc.bStatWrite)
    {
        int totalI = 0, totalP = 0, totalSkip = 0;

        // accumulate intra,inter,skip cu count per frame for 2 pass
        for (uint32_t i = 0; i < m_numRows; i++)
        {
            m_frame->m_encData->m_frameStats.mvBits    += m_rows[i].rowStats.mvBits;
            m_frame->m_encData->m_frameStats.coeffBits += m_rows[i].rowStats.coeffBits;
            m_frame->m_encData->m_frameStats.miscBits  += m_rows[i].rowStats.miscBits;
            totalI                                     += m_rows[i].rowStats.intra8x8Cnt;
            totalP                                     += m_rows[i].rowStats.inter8x8Cnt;
            totalSkip                                  += m_rows[i].rowStats.skip8x8Cnt;
        }
        int totalCuCount = totalI + totalP + totalSkip;
        m_frame->m_encData->m_frameStats.percent8x8Intra = (double)totalI / totalCuCount;
        m_frame->m_encData->m_frameStats.percent8x8Inter = (double)totalP / totalCuCount;
        m_frame->m_encData->m_frameStats.percent8x8Skip  = (double)totalSkip / totalCuCount;
    }

    if (m_param->csvLogLevel >= 1)
    {
        for (uint32_t i = 0; i < m_numRows; i++)
        {
            m_frame->m_encData->m_frameStats.cntIntraNxN += m_rows[i].rowStats.cntIntraNxN;
            m_frame->m_encData->m_frameStats.totalCu += m_rows[i].rowStats.totalCu;
            m_frame->m_encData->m_frameStats.totalCtu += m_rows[i].rowStats.totalCtu;
            m_frame->m_encData->m_frameStats.lumaDistortion += m_rows[i].rowStats.lumaDistortion;
            m_frame->m_encData->m_frameStats.chromaDistortion += m_rows[i].rowStats.chromaDistortion;
            m_frame->m_encData->m_frameStats.psyEnergy += m_rows[i].rowStats.psyEnergy;
            m_frame->m_encData->m_frameStats.ssimEnergy += m_rows[i].rowStats.ssimEnergy;
            m_frame->m_encData->m_frameStats.resEnergy += m_rows[i].rowStats.resEnergy;
            for (uint32_t depth = 0; depth <= m_param->maxCUDepth; depth++)
            {
                m_frame->m_encData->m_frameStats.cntSkipCu[depth] += m_rows[i].rowStats.cntSkipCu[depth];
                m_frame->m_encData->m_frameStats.cntMergeCu[depth] += m_rows[i].rowStats.cntMergeCu[depth];
                for (int m = 0; m < INTER_MODES; m++)
                    m_frame->m_encData->m_frameStats.cuInterDistribution[depth][m] += m_rows[i].rowStats.cuInterDistribution[depth][m];
                for (int n = 0; n < INTRA_MODES; n++)
                    m_frame->m_encData->m_frameStats.cuIntraDistribution[depth][n] += m_rows[i].rowStats.cuIntraDistribution[depth][n];
            }
        }
        m_frame->m_encData->m_frameStats.percentIntraNxN = (double)(m_frame->m_encData->m_frameStats.cntIntraNxN * 100) / m_frame->m_encData->m_frameStats.totalCu;

        for (uint32_t depth = 0; depth <= m_param->maxCUDepth; depth++)
        {
            m_frame->m_encData->m_frameStats.percentSkipCu[depth] = (double)(m_frame->m_encData->m_frameStats.cntSkipCu[depth] * 100) / m_frame->m_encData->m_frameStats.totalCu;
            m_frame->m_encData->m_frameStats.percentMergeCu[depth] = (double)(m_frame->m_encData->m_frameStats.cntMergeCu[depth] * 100) / m_frame->m_encData->m_frameStats.totalCu;
            for (int n = 0; n < INTRA_MODES; n++)
                m_frame->m_encData->m_frameStats.percentIntraDistribution[depth][n] = (double)(m_frame->m_encData->m_frameStats.cuIntraDistribution[depth][n] * 100) / m_frame->m_encData->m_frameStats.totalCu;
            uint64_t cuInterRectCnt = 0; // sum of Nx2N, 2NxN counts
            cuInterRectCnt += m_frame->m_encData->m_frameStats.cuInterDistribution[depth][1] + m_frame->m_encData->m_frameStats.cuInterDistribution[depth][2];
            m_frame->m_encData->m_frameStats.percentInterDistribution[depth][0] = (double)(m_frame->m_encData->m_frameStats.cuInterDistribution[depth][0] * 100) / m_frame->m_encData->m_frameStats.totalCu;
            m_frame->m_encData->m_frameStats.percentInterDistribution[depth][1] = (double)(cuInterRectCnt * 100) / m_frame->m_encData->m_frameStats.totalCu;
            m_frame->m_encData->m_frameStats.percentInterDistribution[depth][2] = (double)(m_frame->m_encData->m_frameStats.cuInterDistribution[depth][3] * 100) / m_frame->m_encData->m_frameStats.totalCu;
        }
    }

    if (m_param->csvLogLevel >= 2)
    {
        m_frame->m_encData->m_frameStats.avgLumaDistortion = (double)(m_frame->m_encData->m_frameStats.lumaDistortion) / m_frame->m_encData->m_frameStats.totalCtu;
        m_frame->m_encData->m_frameStats.avgChromaDistortion = (double)(m_frame->m_encData->m_frameStats.chromaDistortion) / m_frame->m_encData->m_frameStats.totalCtu;
        m_frame->m_encData->m_frameStats.avgPsyEnergy = (double)(m_frame->m_encData->m_frameStats.psyEnergy) / m_frame->m_encData->m_frameStats.totalCtu;
        m_frame->m_encData->m_frameStats.avgSsimEnergy = (double)(m_frame->m_encData->m_frameStats.ssimEnergy) / m_frame->m_encData->m_frameStats.totalCtu;
        m_frame->m_encData->m_frameStats.avgResEnergy = (double)(m_frame->m_encData->m_frameStats.resEnergy) / m_frame->m_encData->m_frameStats.totalCtu;
    }

    m_bs.resetBits();
    m_entropyCoder.load(m_initSliceContext);
    m_entropyCoder.setBitstream(&m_bs);

    // finish encode of each CTU row, only required when SAO is enabled
    if (m_param->bEnableSAO)
        encodeSlice(0);

    m_entropyCoder.setBitstream(&m_bs);

    if (m_param->maxSlices > 1)
    {
        uint32_t nextSliceRow = 0;

        for(uint32_t sliceId = 0; sliceId < m_param->maxSlices; sliceId++)
        {
            m_bs.resetBits();

            const uint32_t sliceAddr = nextSliceRow * m_numCols;
            //CUData* ctu = m_frame->m_encData->getPicCTU(sliceAddr);
            //const int sliceQp = ctu->m_qp[0];
            if (m_param->bOptRefListLengthPPS)
            {
                ScopedLock refIdxLock(m_top->m_sliceRefIdxLock);
                m_top->analyseRefIdx(slice->m_numRefIdx);
            }
            m_entropyCoder.codeSliceHeader(*slice, *m_frame->m_encData, sliceAddr, m_sliceAddrBits, slice->m_sliceQp);

            // Find rows of current slice
            const uint32_t prevSliceRow = nextSliceRow;
            while(nextSliceRow < m_numRows && m_rows[nextSliceRow].sliceId == sliceId)
                nextSliceRow++;

            // serialize each row, record final lengths in slice header
            uint32_t maxStreamSize = m_nalList.serializeSubstreams(&m_substreamSizes[prevSliceRow], (nextSliceRow - prevSliceRow), &m_outStreams[prevSliceRow]);

            // complete the slice header by writing WPP row-starts
            m_entropyCoder.setBitstream(&m_bs);
            if (slice->m_pps->bEntropyCodingSyncEnabled)
                m_entropyCoder.codeSliceHeaderWPPEntryPoints(&m_substreamSizes[prevSliceRow], (nextSliceRow - prevSliceRow - 1), maxStreamSize);
            
            m_bs.writeByteAlignment();

            m_nalList.serialize(slice->m_nalUnitType, m_bs);
        }
    }
    else
    {
        if (m_param->bOptRefListLengthPPS)
        {
            ScopedLock refIdxLock(m_top->m_sliceRefIdxLock);
            m_top->analyseRefIdx(slice->m_numRefIdx);
        }
        m_entropyCoder.codeSliceHeader(*slice, *m_frame->m_encData, 0, 0, slice->m_sliceQp);

        // serialize each row, record final lengths in slice header
        uint32_t maxStreamSize = m_nalList.serializeSubstreams(m_substreamSizes, numSubstreams, m_outStreams);

        // complete the slice header by writing WPP row-starts
        m_entropyCoder.setBitstream(&m_bs);
        if (slice->m_pps->bEntropyCodingSyncEnabled)
            m_entropyCoder.codeSliceHeaderWPPEntryPoints(m_substreamSizes, (slice->m_sps->numCuInHeight - 1), maxStreamSize);
        m_bs.writeByteAlignment();

        m_nalList.serialize(slice->m_nalUnitType, m_bs);
    }


    if (m_param->decodedPictureHashSEI)
    {
        int planes = (m_frame->m_param->internalCsp != X265_CSP_I400) ? 3 : 1;
        int32_t payloadSize = 0;
        if (m_param->decodedPictureHashSEI == 1)
        {
            m_seiReconPictureDigest.m_method = SEIDecodedPictureHash::MD5;
            for (int i = 0; i < planes; i++)
                MD5Final(&m_state[i], m_seiReconPictureDigest.m_digest[i]);
            payloadSize = 1 + 16 * planes;
        }
        else if (m_param->decodedPictureHashSEI == 2)
        {
            m_seiReconPictureDigest.m_method = SEIDecodedPictureHash::CRC;
            for (int i = 0; i < planes; i++)
                crcFinish(m_crc[i], m_seiReconPictureDigest.m_digest[i]);
            payloadSize = 1 + 2 * planes;
        }
        else if (m_param->decodedPictureHashSEI == 3)
        {
            m_seiReconPictureDigest.m_method = SEIDecodedPictureHash::CHECKSUM;
            for (int i = 0; i < planes; i++)
                checksumFinish(m_checksum[i], m_seiReconPictureDigest.m_digest[i]);
            payloadSize = 1 + 4 * planes;
        }
        m_bs.resetBits();
        m_seiReconPictureDigest.setSize(payloadSize);
        m_seiReconPictureDigest.write(m_bs, *slice->m_sps);
        m_bs.writeByteAlignment();
        m_nalList.serialize(NAL_UNIT_SUFFIX_SEI, m_bs);
    }

    uint64_t bytes = 0;
    for (uint32_t i = 0; i < m_nalList.m_numNal; i++)
    {
        int type = m_nalList.m_nal[i].type;

        // exclude SEI
        if (type != NAL_UNIT_PREFIX_SEI && type != NAL_UNIT_SUFFIX_SEI)
        {
            bytes += m_nalList.m_nal[i].sizeBytes;
            // and exclude start code prefix
            bytes -= (!i || type == NAL_UNIT_SPS || type == NAL_UNIT_PPS) ? 4 : 3;
        }
    }
    m_accessUnitBits = bytes << 3;

    int filler = 0;
    /* rateControlEnd may also block for earlier frames to call rateControlUpdateStats */
    if (m_top->m_rateControl->rateControlEnd(m_frame, m_accessUnitBits, &m_rce, &filler) < 0)
        m_top->m_aborted = true;

    if (filler > 0)
    {
        filler = (filler - FILLER_OVERHEAD * 8) >> 3;
        m_bs.resetBits();
        while (filler > 0)
        {
            m_bs.write(0xff, 8);
            filler--;
        }
        m_bs.writeByteAlignment();
        m_nalList.serialize(NAL_UNIT_FILLER_DATA, m_bs);
        bytes += m_nalList.m_nal[m_nalList.m_numNal - 1].sizeBytes;
        bytes -= 3; //exclude start code prefix
        m_accessUnitBits = bytes << 3;
    }

    m_endCompressTime = x265_mdate();

    /* Decrement referenced frame reference counts, allow them to be recycled */
    for (int l = 0; l < numPredDir; l++)
    {
        for (int ref = 0; ref < slice->m_numRefIdx[l]; ref++)
        {
            Frame *refpic = slice->m_refFrameList[l][ref];
            ATOMIC_DEC(&refpic->m_countRefEncoders);
        }
    }

    if (m_nr)
    {
        bool nrEnabled = (m_rce.newQp < QP_MAX_SPEC || !m_param->rc.vbvBufferSize) && (m_param->noiseReductionIntra || m_param->noiseReductionInter);

        if (nrEnabled)
        {
            /* Accumulate NR statistics from all worker threads */
            for (int i = 0; i < numTLD; i++)
            {
                NoiseReduction* nr = &m_tld[i].analysis.m_quant.m_frameNr[m_jpId];
                for (int cat = 0; cat < MAX_NUM_TR_CATEGORIES; cat++)
                {
                    for (int coeff = 0; coeff < MAX_NUM_TR_COEFFS; coeff++)
                        m_nr->nrResidualSum[cat][coeff] += nr->nrResidualSum[cat][coeff];

                    m_nr->nrCount[cat] += nr->nrCount[cat];
                }
            }

            noiseReductionUpdate();

            /* Copy updated NR coefficients back to all worker threads */
            for (int i = 0; i < numTLD; i++)
            {
                NoiseReduction* nr = &m_tld[i].analysis.m_quant.m_frameNr[m_jpId];
                memcpy(nr->nrOffsetDenoise, m_nr->nrOffsetDenoise, sizeof(uint16_t)* MAX_NUM_TR_CATEGORIES * MAX_NUM_TR_COEFFS);
                memset(nr->nrCount, 0, sizeof(uint32_t)* MAX_NUM_TR_CATEGORIES);
                memset(nr->nrResidualSum, 0, sizeof(uint32_t)* MAX_NUM_TR_CATEGORIES * MAX_NUM_TR_COEFFS);
            }
        }
    }

#if DETAILED_CU_STATS
    /* Accumulate CU statistics from each worker thread, we could report
     * per-frame stats here, but currently we do not. */
    for (int i = 0; i < numTLD; i++)
        m_cuStats.accumulate(m_tld[i].analysis.m_stats[m_jpId], *m_param);
#endif

    m_endFrameTime = x265_mdate();
}

void FrameEncoder::encodeSlice(uint32_t sliceAddr)
{
    Slice* slice = m_frame->m_encData->m_slice;
    const uint32_t widthInLCUs = slice->m_sps->numCuInWidth;
    const uint32_t lastCUAddr = (slice->m_endCUAddr + m_param->num4x4Partitions - 1) / m_param->num4x4Partitions;
    const uint32_t numSubstreams = m_param->bEnableWavefront ? slice->m_sps->numCuInHeight : 1;

    SAOParam* saoParam = slice->m_sps->bUseSAO ? m_frame->m_encData->m_saoParam : NULL;
    for (uint32_t cuAddr = sliceAddr; cuAddr < lastCUAddr; cuAddr++)
    {
        uint32_t col = cuAddr % widthInLCUs;
        uint32_t row = cuAddr / widthInLCUs;
        uint32_t subStrm = row % numSubstreams;
        CUData* ctu = m_frame->m_encData->getPicCTU(cuAddr);

        m_entropyCoder.setBitstream(&m_outStreams[subStrm]);

        // Synchronize cabac probabilities with upper-right CTU if it's available and we're at the start of a line.
        if (m_param->bEnableWavefront && !col && row)
        {
            m_entropyCoder.copyState(m_initSliceContext);
            m_entropyCoder.loadContexts(m_rows[row - 1].bufferedEntropy);
        }

        // Initialize slice context
        if (ctu->m_bFirstRowInSlice && !col)
            m_entropyCoder.load(m_initSliceContext);

        if (saoParam)
        {
            if (saoParam->bSaoFlag[0] || saoParam->bSaoFlag[1])
            {
                int mergeLeft = col && saoParam->ctuParam[0][cuAddr].mergeMode == SAO_MERGE_LEFT;
                int mergeUp = !ctu->m_bFirstRowInSlice && saoParam->ctuParam[0][cuAddr].mergeMode == SAO_MERGE_UP;
                if (col)
                    m_entropyCoder.codeSaoMerge(mergeLeft);
                if (!ctu->m_bFirstRowInSlice && !mergeLeft)
                    m_entropyCoder.codeSaoMerge(mergeUp);
                if (!mergeLeft && !mergeUp)
                {
                    if (saoParam->bSaoFlag[0])
                        m_entropyCoder.codeSaoOffset(saoParam->ctuParam[0][cuAddr], 0);
                    if (saoParam->bSaoFlag[1])
                    {
                        m_entropyCoder.codeSaoOffset(saoParam->ctuParam[1][cuAddr], 1);
                        m_entropyCoder.codeSaoOffset(saoParam->ctuParam[2][cuAddr], 2);
                    }
                }
            }
            else
            {
                for (int i = 0; i < (m_param->internalCsp != X265_CSP_I400 ? 3 : 1); i++)
                    saoParam->ctuParam[i][cuAddr].reset();
            }
        }

        // final coding (bitstream generation) for this CU
        m_entropyCoder.encodeCTU(*ctu, m_cuGeoms[m_ctuGeomMap[cuAddr]]);

        if (m_param->bEnableWavefront)
        {
            if (col == 1)
                // Store probabilities of second CTU in line into buffer
                m_rows[row].bufferedEntropy.loadContexts(m_entropyCoder);

            if (col == widthInLCUs - 1)
                m_entropyCoder.finishSlice();
        }
    }

    if (!m_param->bEnableWavefront)
        m_entropyCoder.finishSlice();
}

void FrameEncoder::processRow(int row, int threadId)
{
    int64_t startTime = x265_mdate();
    if (ATOMIC_INC(&m_activeWorkerCount) == 1 && m_stallStartTime)
        m_totalNoWorkerTime += x265_mdate() - m_stallStartTime;

    const uint32_t realRow = m_idx_to_row[row >> 1];
    const uint32_t typeNum = m_idx_to_row[row & 1];

    if (!typeNum)
        processRowEncoder(realRow, m_tld[threadId]);
    else
    {
        m_frameFilter.processRow(realRow);

        // NOTE: Active next row
        if (realRow != m_sliceBaseRow[m_rows[realRow].sliceId + 1] - 1)
            enqueueRowFilter(m_row_to_idx[realRow + 1]);
    }

    if (ATOMIC_DEC(&m_activeWorkerCount) == 0)
        m_stallStartTime = x265_mdate();

    m_totalWorkerElapsedTime += x265_mdate() - startTime; // not thread safe, but good enough
}

// Called by worker threads
void FrameEncoder::processRowEncoder(int intRow, ThreadLocalData& tld)
{
    const uint32_t row = (uint32_t)intRow;
    CTURow& curRow = m_rows[row];

    if (m_param->bEnableWavefront)
    {
        ScopedLock self(curRow.lock);
        if (!curRow.active)
            /* VBV restart is in progress, exit out */
            return;
        if (curRow.busy)
        {
            /* On multi-socket Windows servers, we have seen problems with
             * ATOMIC_CAS which resulted in multiple worker threads processing
             * the same CU row, which often resulted in bad pointer accesses. We
             * believe the problem is fixed, but are leaving this check in place
             * to prevent crashes in case it is not */
            x265_log(m_param, X265_LOG_WARNING,
                     "internal error - simultaneous row access detected. Please report HW to x265-devel@videolan.org\n");
            return;
        }
        curRow.busy = true;
    }

    /* When WPP is enabled, every row has its own row coder instance. Otherwise
     * they share row 0 */
    Entropy& rowCoder = m_param->bEnableWavefront ? curRow.rowGoOnCoder : m_rows[0].rowGoOnCoder;
    FrameData& curEncData = *m_frame->m_encData;
    Slice *slice = curEncData.m_slice;

    const uint32_t numCols = m_numCols;
    const uint32_t lineStartCUAddr = row * numCols;
    bool bIsVbv = m_param->rc.vbvBufferSize > 0 && m_param->rc.vbvMaxBitrate > 0;

    const uint32_t sliceId = curRow.sliceId;
    uint32_t maxBlockCols = (m_frame->m_fencPic->m_picWidth + (16 - 1)) / 16;
    uint32_t noOfBlocks = m_param->maxCUSize / 16;
    const uint32_t bFirstRowInSlice = ((row == 0) || (m_rows[row - 1].sliceId != curRow.sliceId)) ? 1 : 0;
    const uint32_t bLastRowInSlice = ((row == m_numRows - 1) || (m_rows[row + 1].sliceId != curRow.sliceId)) ? 1 : 0;
    const uint32_t endRowInSlicePlus1 = m_sliceBaseRow[sliceId + 1];
    const uint32_t rowInSlice = row - m_sliceBaseRow[sliceId];

    // Load SBAC coder context from previous row and initialize row state.
    if (bFirstRowInSlice && !curRow.completed)        
        rowCoder.load(m_initSliceContext);     

    // calculate mean QP for consistent deltaQP signalling calculation
    if (m_param->bOptCUDeltaQP)
    {
        ScopedLock self(curRow.lock);
        if (!curRow.avgQPComputed)
        {
            if (m_param->bEnableWavefront || !row)
            {
                double meanQPOff = 0;
                bool isReferenced = IS_REFERENCED(m_frame);
                double *qpoffs = (isReferenced && m_param->rc.cuTree) ? m_frame->m_lowres.qpCuTreeOffset : m_frame->m_lowres.qpAqOffset;
                if (qpoffs)
                {
                    uint32_t loopIncr = (m_param->rc.qgSize == 8) ? 8 : 16;

                    uint32_t cuYStart = 0, height = m_frame->m_fencPic->m_picHeight;
                    if (m_param->bEnableWavefront)
                    {
                        cuYStart = intRow * m_param->maxCUSize;
                        height = cuYStart + m_param->maxCUSize;
                    }

                    uint32_t qgSize = m_param->rc.qgSize, width = m_frame->m_fencPic->m_picWidth;
                    uint32_t maxOffsetCols = (m_frame->m_fencPic->m_picWidth + (loopIncr - 1)) / loopIncr;
                    uint32_t count = 0;
                    for (uint32_t cuY = cuYStart; cuY < height && (cuY < m_frame->m_fencPic->m_picHeight); cuY += qgSize)
                    {
                        for (uint32_t cuX = 0; cuX < width; cuX += qgSize)
                        {
                            double qp_offset = 0;
                            uint32_t cnt = 0;

                            for (uint32_t block_yy = cuY; block_yy < cuY + qgSize && block_yy < m_frame->m_fencPic->m_picHeight; block_yy += loopIncr)
                            {
                                for (uint32_t block_xx = cuX; block_xx < cuX + qgSize && block_xx < width; block_xx += loopIncr)
                                {
                                    int idx = ((block_yy / loopIncr) * (maxOffsetCols)) + (block_xx / loopIncr);
                                    qp_offset += qpoffs[idx];
                                    cnt++;
                                }
                            }
                            qp_offset /= cnt;
                            meanQPOff += qp_offset;
                            count++;
                        }
                    }
                    meanQPOff /= count;
                }
                rowCoder.m_meanQP = slice->m_sliceQp + meanQPOff;
            }
            else
            {
                rowCoder.m_meanQP = m_rows[0].rowGoOnCoder.m_meanQP;
            }
            curRow.avgQPComputed = 1;
        }
    }    

    // Initialize restrict on MV range in slices
    tld.analysis.m_sliceMinY = -(int16_t)(rowInSlice * m_param->maxCUSize * 4) + 3 * 4;
    tld.analysis.m_sliceMaxY = (int16_t)((endRowInSlicePlus1 - 1 - row) * (m_param->maxCUSize * 4) - 4 * 4);

    // Handle single row slice
    if (tld.analysis.m_sliceMaxY < tld.analysis.m_sliceMinY)
        tld.analysis.m_sliceMaxY = tld.analysis.m_sliceMinY = 0;


    while (curRow.completed < numCols)
    {
        ProfileScopeEvent(encodeCTU);

        const uint32_t col = curRow.completed;
        const uint32_t cuAddr = lineStartCUAddr + col;
        CUData* ctu = curEncData.getPicCTU(cuAddr);
        const uint32_t bLastCuInSlice = (bLastRowInSlice & (col == numCols - 1)) ? 1 : 0;
        ctu->initCTU(*m_frame, cuAddr, slice->m_sliceQp, bFirstRowInSlice, bLastRowInSlice, bLastCuInSlice);

        if (bIsVbv)
        {
            if (col == 0 && !m_param->bEnableWavefront)
            {
                m_backupStreams[0].copyBits(&m_outStreams[0]);
                curRow.bufferedEntropy.copyState(rowCoder);
                curRow.bufferedEntropy.loadContexts(rowCoder);
            }
            if (bFirstRowInSlice && m_vbvResetTriggerRow != intRow)            
            {
                curEncData.m_rowStat[row].rowQp = curEncData.m_avgQpRc;
                curEncData.m_rowStat[row].rowQpScale = x265_qp2qScale(curEncData.m_avgQpRc);
            }

            FrameData::RCStatCU& cuStat = curEncData.m_cuStat[cuAddr];
            if (m_param->bEnableWavefront && rowInSlice >= col && !bFirstRowInSlice && m_vbvResetTriggerRow != intRow)
                cuStat.baseQp = curEncData.m_cuStat[cuAddr - numCols + 1].baseQp;
            else if (!m_param->bEnableWavefront && !bFirstRowInSlice && m_vbvResetTriggerRow != intRow)
                cuStat.baseQp = curEncData.m_rowStat[row - 1].rowQp;
            else
                cuStat.baseQp = curEncData.m_rowStat[row].rowQp;

            /* TODO: use defines from slicetype.h for lowres block size */
            uint32_t block_y = (ctu->m_cuPelY >> m_param->maxLog2CUSize) * noOfBlocks;
            uint32_t block_x = (ctu->m_cuPelX >> m_param->maxLog2CUSize) * noOfBlocks;
            if (m_param->analysisReuseMode != X265_ANALYSIS_LOAD || !m_param->bDisableLookahead)
            {
                cuStat.vbvCost = 0;
                cuStat.intraVbvCost = 0;

                for (uint32_t h = 0; h < noOfBlocks && block_y < m_sliceMaxBlockRow[sliceId + 1]; h++, block_y++)
                {
                    uint32_t idx = block_x + (block_y * maxBlockCols);

                    for (uint32_t w = 0; w < noOfBlocks && (block_x + w) < maxBlockCols; w++, idx++)
                    {
                        cuStat.vbvCost += m_frame->m_lowres.lowresCostForRc[idx] & LOWRES_COST_MASK;
                        cuStat.intraVbvCost += m_frame->m_lowres.intraCost[idx];
                    }
                }
            }
        }
        else
            curEncData.m_cuStat[cuAddr].baseQp = curEncData.m_avgQpRc;

        if (m_param->bEnableWavefront && !col && !bFirstRowInSlice)
        {
            // Load SBAC coder context from previous row and initialize row state.
            rowCoder.copyState(m_initSliceContext);
            rowCoder.loadContexts(m_rows[row - 1].bufferedEntropy);
        }
        if (m_param->dynamicRd && (int32_t)(m_rce.qpaRc - m_rce.qpNoVbv) > 0)
            ctu->m_vbvAffected = true;

        // Does all the CU analysis, returns best top level mode decision
        Mode& best = tld.analysis.compressCTU(*ctu, *m_frame, m_cuGeoms[m_ctuGeomMap[cuAddr]], rowCoder);

        // take a sample of the current active worker count
        ATOMIC_ADD(&m_totalActiveWorkerCount, m_activeWorkerCount);
        ATOMIC_INC(&m_activeWorkerCountSamples);

        /* advance top-level row coder to include the context of this CTU.
         * if SAO is disabled, rowCoder writes the final CTU bitstream */
        rowCoder.encodeCTU(*ctu, m_cuGeoms[m_ctuGeomMap[cuAddr]]);

        if (m_param->bEnableWavefront && col == 1)
            // Save CABAC state for next row
            curRow.bufferedEntropy.loadContexts(rowCoder);

        /* SAO parameter estimation using non-deblocked pixels for CTU bottom and right boundary areas */
        if (m_param->bEnableSAO && m_param->bSaoNonDeblocked)
            m_frameFilter.m_parallelFilter[row].m_sao.calcSaoStatsCu_BeforeDblk(m_frame, col, row);

        /* Deblock with idle threading */
        if (m_param->bEnableLoopFilter | m_param->bEnableSAO)
        {
            // NOTE: in VBV mode, we may reencode anytime, so we can't do Deblock stage-Horizon and SAO
            if (!bIsVbv)
            {                
                // Delay one row to avoid intra prediction conflict
                if (m_pool && !bFirstRowInSlice)
                {                    
                    int allowCol = col;

                    // avoid race condition on last column
                    if (rowInSlice >= 2)
                    {
                        allowCol = X265_MIN(((col == numCols - 1) ? m_frameFilter.m_parallelFilter[row - 2].m_lastDeblocked.get()
                                                                  : m_frameFilter.m_parallelFilter[row - 2].m_lastCol.get()), (int)col);
                    }
                    m_frameFilter.m_parallelFilter[row - 1].m_allowedCol.set(allowCol);
                }

                // Last Row may start early
                if (m_pool && bLastRowInSlice)
                {
                    // Deblocking last row
                    int allowCol = col;

                    // avoid race condition on last column
                    if (rowInSlice >= 2)
                    {
                        allowCol = X265_MIN(((col == numCols - 1) ? m_frameFilter.m_parallelFilter[row - 1].m_lastDeblocked.get()
                                                                  : m_frameFilter.m_parallelFilter[row - 1].m_lastCol.get()), (int)col);
                    }
                    m_frameFilter.m_parallelFilter[row].m_allowedCol.set(allowCol);
                }
            } // end of !bIsVbv
        }
        // Both Loopfilter and SAO Disabled
        else
        {
            m_frameFilter.m_parallelFilter[row].processPostCu(col);
        }

        // Completed CU processing
        curRow.completed++;

        FrameStats frameLog;
        curEncData.m_rowStat[row].sumQpAq += collectCTUStatistics(*ctu, &frameLog);

        // copy number of intra, inter cu per row into frame stats for 2 pass
        if (m_param->rc.bStatWrite)
        {
            curRow.rowStats.mvBits    += best.mvBits;
            curRow.rowStats.coeffBits += best.coeffBits;
            curRow.rowStats.miscBits  += best.totalBits - (best.mvBits + best.coeffBits);

            for (uint32_t depth = 0; depth <= m_param->maxCUDepth; depth++)
            {
                /* 1 << shift == number of 8x8 blocks at current depth */
                int shift = 2 * (m_param->maxCUDepth - depth);
                int cuSize = m_param->maxCUSize >> depth;

                curRow.rowStats.intra8x8Cnt += (cuSize == 8) ? (int)(frameLog.cntIntra[depth] + frameLog.cntIntraNxN) :
                                                               (int)(frameLog.cntIntra[depth] << shift);

                curRow.rowStats.inter8x8Cnt += (int)(frameLog.cntInter[depth] << shift);
                curRow.rowStats.skip8x8Cnt += (int)((frameLog.cntSkipCu[depth] + frameLog.cntMergeCu[depth]) << shift);
            }
        }
        curRow.rowStats.totalCtu++;
        curRow.rowStats.lumaDistortion   += best.lumaDistortion;
        curRow.rowStats.chromaDistortion += best.chromaDistortion;
        curRow.rowStats.psyEnergy        += best.psyEnergy;
        curRow.rowStats.ssimEnergy       += best.ssimEnergy;
        curRow.rowStats.resEnergy        += best.resEnergy;
        curRow.rowStats.cntIntraNxN      += frameLog.cntIntraNxN;
        curRow.rowStats.totalCu          += frameLog.totalCu;
        for (uint32_t depth = 0; depth <= m_param->maxCUDepth; depth++)
        {
            curRow.rowStats.cntSkipCu[depth] += frameLog.cntSkipCu[depth];
            curRow.rowStats.cntMergeCu[depth] += frameLog.cntMergeCu[depth];
            for (int m = 0; m < INTER_MODES; m++)
                curRow.rowStats.cuInterDistribution[depth][m] += frameLog.cuInterDistribution[depth][m];
            for (int n = 0; n < INTRA_MODES; n++)
                curRow.rowStats.cuIntraDistribution[depth][n] += frameLog.cuIntraDistribution[depth][n];
        }

        curEncData.m_cuStat[cuAddr].totalBits = best.totalBits;
        x265_emms();

        if (bIsVbv)
        {   
            // Update encoded bits, satdCost, baseQP for each CU if tune grain is disabled
            FrameData::RCStatCU& cuStat = curEncData.m_cuStat[cuAddr];    
            if ((m_param->bEnableWavefront && ((cuAddr == m_sliceBaseRow[sliceId] * numCols) || !m_param->rc.bEnableConstVbv)) || !m_param->bEnableWavefront)
            {
                curEncData.m_rowStat[row].rowSatd += cuStat.vbvCost;
                curEncData.m_rowStat[row].rowIntraSatd += cuStat.intraVbvCost;
                curEncData.m_rowStat[row].encodedBits += cuStat.totalBits;
                curEncData.m_rowStat[row].sumQpRc += cuStat.baseQp;
                curEncData.m_rowStat[row].numEncodedCUs = cuAddr;
            }
            
            // If current block is at row end checkpoint, call vbv ratecontrol.
            if (!m_param->bEnableWavefront && col == numCols - 1)
            {
                double qpBase = curEncData.m_cuStat[cuAddr].baseQp;
                int reEncode = m_top->m_rateControl->rowVbvRateControl(m_frame, row, &m_rce, qpBase, m_sliceBaseRow, sliceId);
                qpBase = x265_clip3((double)m_param->rc.qpMin, (double)m_param->rc.qpMax, qpBase);
                curEncData.m_rowStat[row].rowQp = qpBase;
                curEncData.m_rowStat[row].rowQpScale = x265_qp2qScale(qpBase);
                if (reEncode < 0)
                {
                    x265_log(m_param, X265_LOG_DEBUG, "POC %d row %d - encode restart required for VBV, to %.2f from %.2f\n",
                        m_frame->m_poc, row, qpBase, curEncData.m_cuStat[cuAddr].baseQp);

                    m_vbvResetTriggerRow = row;
                    m_outStreams[0].copyBits(&m_backupStreams[0]);

                    rowCoder.copyState(curRow.bufferedEntropy);
                    rowCoder.loadContexts(curRow.bufferedEntropy);

                    curRow.completed = 0;
                    memset(&curRow.rowStats, 0, sizeof(curRow.rowStats));
                    curEncData.m_rowStat[row].numEncodedCUs = 0;
                    curEncData.m_rowStat[row].encodedBits = 0;
                    curEncData.m_rowStat[row].rowSatd = 0;
                    curEncData.m_rowStat[row].rowIntraSatd = 0;
                    curEncData.m_rowStat[row].sumQpRc = 0;
                    curEncData.m_rowStat[row].sumQpAq = 0;
                }
            }
            // If current block is at row diagonal checkpoint, call vbv ratecontrol.
            else if (m_param->bEnableWavefront && rowInSlice == col && !bFirstRowInSlice)
            {
                if (m_param->rc.bEnableConstVbv)
                {
                    uint32_t startCuAddr = numCols * row;
                    uint32_t EndCuAddr = startCuAddr + col;

                    for (int32_t r = row; r >= (int32_t)m_sliceBaseRow[sliceId]; r--)
                    {
                        for (uint32_t c = startCuAddr; c <= EndCuAddr && c <= numCols * (r + 1) - 1; c++)
                        {
                            curEncData.m_rowStat[r].rowSatd += curEncData.m_cuStat[c].vbvCost;
                            curEncData.m_rowStat[r].rowIntraSatd += curEncData.m_cuStat[c].intraVbvCost;
                            curEncData.m_rowStat[r].encodedBits += curEncData.m_cuStat[c].totalBits;
                            curEncData.m_rowStat[r].sumQpRc += curEncData.m_cuStat[c].baseQp;
                            curEncData.m_rowStat[r].numEncodedCUs = c;
                        }
                        startCuAddr = EndCuAddr - numCols;
                        EndCuAddr = startCuAddr + 1;
                    }
                }
                double qpBase = curEncData.m_cuStat[cuAddr].baseQp;
                int reEncode = m_top->m_rateControl->rowVbvRateControl(m_frame, row, &m_rce, qpBase, m_sliceBaseRow, sliceId);
                qpBase = x265_clip3((double)m_param->rc.qpMin, (double)m_param->rc.qpMax, qpBase);
                curEncData.m_rowStat[row].rowQp = qpBase;
                curEncData.m_rowStat[row].rowQpScale = x265_qp2qScale(qpBase);

                if (reEncode < 0)
                {
                    x265_log(m_param, X265_LOG_DEBUG, "POC %d row %d - encode restart required for VBV, to %.2f from %.2f\n",
                             m_frame->m_poc, row, qpBase, curEncData.m_cuStat[cuAddr].baseQp);

                    // prevent the WaveFront::findJob() method from providing new jobs
                    m_vbvResetTriggerRow = row;
                    m_bAllRowsStop = true;

                    for (uint32_t r = m_sliceBaseRow[sliceId + 1] - 1; r >= row; r--)
                    {
                        CTURow& stopRow = m_rows[r];

                        if (r != row)
                        {
                            /* if row was active (ready to be run) clear active bit and bitmap bit for this row */
                            stopRow.lock.acquire();
                            while (stopRow.active)
                            {
                                if (dequeueRow(r * 2))
                                    stopRow.active = false;
                                else
                                {
                                    /* we must release the row lock to allow the thread to exit */
                                    stopRow.lock.release();
                                    GIVE_UP_TIME();
                                    stopRow.lock.acquire();
                                }
                            }
                            stopRow.lock.release();

                            bool bRowBusy = true;
                            do
                            {
                                stopRow.lock.acquire();
                                bRowBusy = stopRow.busy;
                                stopRow.lock.release();

                                if (bRowBusy)
                                {
                                    GIVE_UP_TIME();
                                }
                            }
                            while (bRowBusy);
                        }

                        m_outStreams[r].resetBits();
                        stopRow.completed = 0;
                        memset(&stopRow.rowStats, 0, sizeof(stopRow.rowStats));
                        curEncData.m_rowStat[r].numEncodedCUs = 0;
                        curEncData.m_rowStat[r].encodedBits = 0;
                        curEncData.m_rowStat[r].rowSatd = 0;
                        curEncData.m_rowStat[r].rowIntraSatd = 0;
                        curEncData.m_rowStat[r].sumQpRc = 0;
                        curEncData.m_rowStat[r].sumQpAq = 0;
                    }

                    m_bAllRowsStop = false;
                }
            }
        }

        if (m_param->bEnableWavefront && curRow.completed >= 2 && !bLastRowInSlice &&
            (!m_bAllRowsStop || intRow + 1 < m_vbvResetTriggerRow))
        {
            /* activate next row */
            ScopedLock below(m_rows[row + 1].lock);

            if (m_rows[row + 1].active == false &&
                m_rows[row + 1].completed + 2 <= curRow.completed)
            {
                m_rows[row + 1].active = true;
                enqueueRowEncoder(m_row_to_idx[row + 1]);
                tryWakeOne(); /* wake up a sleeping thread or set the help wanted flag */
            }
        }

        ScopedLock self(curRow.lock);
        if ((m_bAllRowsStop && intRow > m_vbvResetTriggerRow) ||
            (!bFirstRowInSlice && ((curRow.completed < numCols - 1) || (m_rows[row - 1].completed < numCols)) && m_rows[row - 1].completed < curRow.completed + 2))
        {
            curRow.active = false;
            curRow.busy = false;
            ATOMIC_INC(&m_countRowBlocks);
            return;
        }
    }

    /* this row of CTUs has been compressed */
    if (m_param->bEnableWavefront && m_param->rc.bEnableConstVbv)
    {
        if (bLastRowInSlice)       
        {
            for (uint32_t r = m_sliceBaseRow[sliceId]; r < m_sliceBaseRow[sliceId + 1]; r++)
            {
                for (uint32_t c = curEncData.m_rowStat[r].numEncodedCUs + 1; c < numCols * (r + 1); c++)
                {
                    curEncData.m_rowStat[r].rowSatd += curEncData.m_cuStat[c].vbvCost;
                    curEncData.m_rowStat[r].rowIntraSatd += curEncData.m_cuStat[c].intraVbvCost;
                    curEncData.m_rowStat[r].encodedBits += curEncData.m_cuStat[c].totalBits;
                    curEncData.m_rowStat[r].sumQpRc += curEncData.m_cuStat[c].baseQp;
                    curEncData.m_rowStat[r].numEncodedCUs = c;
                }
            }
        }
    }

    /* If encoding with ABR, update update bits and complexity in rate control
     * after a number of rows so the next frame's rateControlStart has more
     * accurate data for estimation. At the start of the encode we update stats
     * after half the frame is encoded, but after this initial period we update
     * after refLagRows (the number of rows reference frames must have completed
     * before referencees may begin encoding) */
    if (m_param->rc.rateControlMode == X265_RC_ABR || bIsVbv)
    {
        uint32_t rowCount = 0;
        uint32_t maxRows = m_sliceBaseRow[sliceId + 1] - m_sliceBaseRow[sliceId];

        if (!m_rce.encodeOrder)
            rowCount = maxRows - 1; 
        else if ((uint32_t)m_rce.encodeOrder <= 2 * (m_param->fpsNum / m_param->fpsDenom))
            rowCount = X265_MIN((maxRows + 1) / 2, maxRows - 1);
        else
			rowCount = X265_MIN(m_refLagRows / m_param->maxSlices, maxRows - 1);

        if (rowInSlice == rowCount)
        {
            m_rowSliceTotalBits[sliceId] = 0;
            if (bIsVbv)
            {                
                for (uint32_t i = m_sliceBaseRow[sliceId]; i < rowCount + m_sliceBaseRow[sliceId]; i++)
                    m_rowSliceTotalBits[sliceId] += curEncData.m_rowStat[i].encodedBits;
            }
            else
            {
                uint32_t startAddr = m_sliceBaseRow[sliceId] * numCols;
				uint32_t finishAddr = startAddr + rowCount * numCols;
                
				for (uint32_t cuAddr = startAddr; cuAddr < finishAddr; cuAddr++)
                    m_rowSliceTotalBits[sliceId] += curEncData.m_cuStat[cuAddr].totalBits;
            }            

            if (ATOMIC_INC(&m_sliceCnt) == (int)m_param->maxSlices)
            {
                m_rce.rowTotalBits = 0;
                for (uint32_t i = 0; i < m_param->maxSlices; i++)
                    m_rce.rowTotalBits += m_rowSliceTotalBits[i];
                m_top->m_rateControl->rateControlUpdateStats(&m_rce);
            }
        }
    }

    /* flush row bitstream (if WPP and no SAO) or flush frame if no WPP and no SAO */
    /* end_of_sub_stream_one_bit / end_of_slice_segment_flag */
    if (!m_param->bEnableSAO && (m_param->bEnableWavefront || bLastRowInSlice))
        rowCoder.finishSlice();


    /* Processing left Deblock block with current threading */
    if ((m_param->bEnableLoopFilter | m_param->bEnableSAO) & (rowInSlice >= 2))
    {
        /* Check conditional to start previous row process with current threading */
        if (m_frameFilter.m_parallelFilter[row - 2].m_lastDeblocked.get() == (int)numCols)
        {
            /* stop threading on current row and restart it */
            m_frameFilter.m_parallelFilter[row - 1].m_allowedCol.set(numCols);
            m_frameFilter.m_parallelFilter[row - 1].processTasks(-1);
        }
    }

    /* trigger row-wise loop filters */
    if (m_param->bEnableWavefront)
    {
        if (rowInSlice >= m_filterRowDelay)
        {
            enableRowFilter(m_row_to_idx[row - m_filterRowDelay]);

            /* NOTE: Activate filter if first row (row 0) */
            if (rowInSlice == m_filterRowDelay)
                enqueueRowFilter(m_row_to_idx[row - m_filterRowDelay]);
            tryWakeOne();
        }

        if (bLastRowInSlice)
        {
            for (uint32_t i = endRowInSlicePlus1 - m_filterRowDelay; i < endRowInSlicePlus1; i++)
            {
                enableRowFilter(m_row_to_idx[i]);
            }
            tryWakeOne();
        }

        // handle specially case - single row slice
        if  (bFirstRowInSlice & bLastRowInSlice)
        {
            enqueueRowFilter(m_row_to_idx[row]);
            tryWakeOne();
        }
    }

    curRow.busy = false;

    // CHECK_ME: Does it always FALSE condition?
    if (ATOMIC_INC(&m_completionCount) == 2 * (int)m_numRows)
        m_completionEvent.trigger();
}

/* collect statistics about CU coding decisions, return total QP */
int FrameEncoder::collectCTUStatistics(const CUData& ctu, FrameStats* log)
{
    int totQP = 0;
    uint32_t depth = 0;
    for (uint32_t absPartIdx = 0; absPartIdx < ctu.m_numPartitions; absPartIdx += ctu.m_numPartitions >> (depth * 2))
    {
        depth = ctu.m_cuDepth[absPartIdx];
        totQP += ctu.m_qp[absPartIdx] * (ctu.m_numPartitions >> (depth * 2));
    }

    if (m_param->csvLogLevel >= 1 || m_param->rc.bStatWrite)
    {
        if (ctu.m_slice->m_sliceType == I_SLICE)
        {
            depth = 0;
            for (uint32_t absPartIdx = 0; absPartIdx < ctu.m_numPartitions; absPartIdx += ctu.m_numPartitions >> (depth * 2))
            {
                depth = ctu.m_cuDepth[absPartIdx];

                log->totalCu++;
                log->cntIntra[depth]++;

                if (ctu.m_predMode[absPartIdx] == MODE_NONE)
                {
                    log->totalCu--;
                    log->cntIntra[depth]--;
                }
                else if (ctu.m_partSize[absPartIdx] != SIZE_2Nx2N)
                {
                    /* TODO: log intra modes at absPartIdx +0 to +3 */
                    X265_CHECK(ctu.m_log2CUSize[absPartIdx] == 3 && ctu.m_slice->m_sps->quadtreeTULog2MinSize < 3, "Intra NxN found at improbable depth\n");
                    log->cntIntraNxN++;
                    log->cntIntra[depth]--;
                }
                else if (ctu.m_lumaIntraDir[absPartIdx] > 1)
                    log->cuIntraDistribution[depth][ANGULAR_MODE_ID]++;
                else
                    log->cuIntraDistribution[depth][ctu.m_lumaIntraDir[absPartIdx]]++;
            }
        }
        else
        {
            depth = 0;
            for (uint32_t absPartIdx = 0; absPartIdx < ctu.m_numPartitions; absPartIdx += ctu.m_numPartitions >> (depth * 2))
            {
                depth = ctu.m_cuDepth[absPartIdx];

                log->totalCu++;

                if (ctu.m_predMode[absPartIdx] == MODE_NONE)
                    log->totalCu--;
                else if (ctu.isSkipped(absPartIdx))
                {
                    if (ctu.m_mergeFlag[0])
                        log->cntMergeCu[depth]++;
                    else
                        log->cntSkipCu[depth]++;
                }
                else if (ctu.isInter(absPartIdx))
                {
                    log->cntInter[depth]++;

                    if (ctu.m_partSize[absPartIdx] < AMP_ID)
                        log->cuInterDistribution[depth][ctu.m_partSize[absPartIdx]]++;
                    else
                        log->cuInterDistribution[depth][AMP_ID]++;
                }
                else if (ctu.isIntra(absPartIdx))
                {
                    log->cntIntra[depth]++;

                    if (ctu.m_partSize[absPartIdx] != SIZE_2Nx2N)
                    {
                        X265_CHECK(ctu.m_log2CUSize[absPartIdx] == 3 && ctu.m_slice->m_sps->quadtreeTULog2MinSize < 3, "Intra NxN found at improbable depth\n");
                        log->cntIntraNxN++;
                        log->cntIntra[depth]--;
                        /* TODO: log intra modes at absPartIdx +0 to +3 */
                    }
                    else if (ctu.m_lumaIntraDir[absPartIdx] > 1)
                        log->cuIntraDistribution[depth][ANGULAR_MODE_ID]++;
                    else
                        log->cuIntraDistribution[depth][ctu.m_lumaIntraDir[absPartIdx]]++;
                }
            }
        }
    }

    return totQP;
}

/* DCT-domain noise reduction / adaptive deadzone from libavcodec */
void FrameEncoder::noiseReductionUpdate()
{
    static const uint32_t maxBlocksPerTrSize[4] = {1 << 18, 1 << 16, 1 << 14, 1 << 12};

    for (int cat = 0; cat < MAX_NUM_TR_CATEGORIES; cat++)
    {
        int trSize = cat & 3;
        int coefCount = 1 << ((trSize + 2) * 2);

        if (m_nr->nrCount[cat] > maxBlocksPerTrSize[trSize])
        {
            for (int i = 0; i < coefCount; i++)
                m_nr->nrResidualSum[cat][i] >>= 1;
            m_nr->nrCount[cat] >>= 1;
        }

        int nrStrength = cat < 8 ? m_param->noiseReductionIntra : m_param->noiseReductionInter;
        uint64_t scaledCount = (uint64_t)nrStrength * m_nr->nrCount[cat];

        for (int i = 0; i < coefCount; i++)
        {
            uint64_t value = scaledCount + m_nr->nrResidualSum[cat][i] / 2;
            uint64_t denom = m_nr->nrResidualSum[cat][i] + 1;
            m_nr->nrOffsetDenoise[cat][i] = (uint16_t)(value / denom);
        }

        // Don't denoise DC coefficients
        m_nr->nrOffsetDenoise[cat][0] = 0;
    }
}

Frame *FrameEncoder::getEncodedPicture(NALList& output)
{
    if (m_frame)
    {
        /* block here until worker thread completes */
        m_done.wait();

        Frame *ret = m_frame;
        m_frame = NULL;
        output.takeContents(m_nalList);
        m_prevOutputTime = x265_mdate();
        return ret;
    }

    return NULL;
}
}
