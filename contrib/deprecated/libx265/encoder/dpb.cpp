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
#include "frame.h"
#include "framedata.h"
#include "picyuv.h"
#include "slice.h"

#include "dpb.h"

using namespace X265_NS;

DPB::~DPB()
{
    while (!m_freeList.empty())
    {
        Frame* curFrame = m_freeList.popFront();
        curFrame->destroy();
        delete curFrame;
    }

    while (!m_picList.empty())
    {
        Frame* curFrame = m_picList.popFront();
        curFrame->destroy();
        delete curFrame;
    }

    while (m_frameDataFreeList)
    {
        FrameData* next = m_frameDataFreeList->m_freeListNext;
        m_frameDataFreeList->destroy();

        m_frameDataFreeList->m_reconPic->destroy();
        delete m_frameDataFreeList->m_reconPic;

        delete m_frameDataFreeList;
        m_frameDataFreeList = next;
    }
}

// move unreferenced pictures from picList to freeList for recycle
void DPB::recycleUnreferenced()
{
    Frame *iterFrame = m_picList.first();

    while (iterFrame)
    {
        Frame *curFrame = iterFrame;
        iterFrame = iterFrame->m_next;
        if (!curFrame->m_encData->m_bHasReferences && !curFrame->m_countRefEncoders)
        {
            curFrame->m_bChromaExtended = false;

            // Reset column counter
            X265_CHECK(curFrame->m_reconRowFlag != NULL, "curFrame->m_reconRowFlag check failure");
            X265_CHECK(curFrame->m_reconColCount != NULL, "curFrame->m_reconColCount check failure");
            X265_CHECK(curFrame->m_numRows > 0, "curFrame->m_numRows check failure");

            for(int32_t row = 0; row < curFrame->m_numRows; row++)
            {
                curFrame->m_reconRowFlag[row].set(0);
                curFrame->m_reconColCount[row].set(0);
            }

            // iterator is invalidated by remove, restart scan
            m_picList.remove(*curFrame);
            iterFrame = m_picList.first();

            m_freeList.pushBack(*curFrame);
            curFrame->m_encData->m_freeListNext = m_frameDataFreeList;
            m_frameDataFreeList = curFrame->m_encData;

            if (curFrame->m_encData->m_meBuffer)
            {
                for (int i = 0; i < INTEGRAL_PLANE_NUM; i++)
                {
                    if (curFrame->m_encData->m_meBuffer[i] != NULL)
                    {
                        X265_FREE(curFrame->m_encData->m_meBuffer[i]);
                        curFrame->m_encData->m_meBuffer[i] = NULL;
                    }
                }
            }

            if (curFrame->m_ctuInfo != NULL)
            {
                uint32_t widthInCU = (curFrame->m_param->sourceWidth + curFrame->m_param->maxCUSize - 1) >> curFrame->m_param->maxLog2CUSize;
                uint32_t heightInCU = (curFrame->m_param->sourceHeight + curFrame->m_param->maxCUSize - 1) >> curFrame->m_param->maxLog2CUSize;
                uint32_t numCUsInFrame = widthInCU * heightInCU;
                for (uint32_t i = 0; i < numCUsInFrame; i++)
                {
                    X265_FREE((*curFrame->m_ctuInfo + i)->ctuInfo);
                    (*curFrame->m_ctuInfo + i)->ctuInfo = NULL;
                }
                X265_FREE(*curFrame->m_ctuInfo);
                *(curFrame->m_ctuInfo) = NULL;
                X265_FREE(curFrame->m_ctuInfo);
                curFrame->m_ctuInfo = NULL;
                X265_FREE(curFrame->m_prevCtuInfoChange);
                curFrame->m_prevCtuInfoChange = NULL;
            }
            curFrame->m_encData = NULL;
            curFrame->m_reconPic = NULL;
        }
    }
}

void DPB::prepareEncode(Frame *newFrame)
{
    Slice* slice = newFrame->m_encData->m_slice;
    slice->m_poc = newFrame->m_poc;

    int pocCurr = slice->m_poc;
    int type = newFrame->m_lowres.sliceType;
    bool bIsKeyFrame = newFrame->m_lowres.bKeyframe;

    slice->m_nalUnitType = getNalUnitType(pocCurr, bIsKeyFrame);
    if (slice->m_nalUnitType == NAL_UNIT_CODED_SLICE_IDR_W_RADL)
        m_lastIDR = pocCurr;
    slice->m_lastIDR = m_lastIDR;
    slice->m_sliceType = IS_X265_TYPE_B(type) ? B_SLICE : (type == X265_TYPE_P) ? P_SLICE : I_SLICE;

    if (type == X265_TYPE_B)
    {
        newFrame->m_encData->m_bHasReferences = false;

        // Adjust NAL type for unreferenced B frames (change from _R "referenced"
        // to _N "non-referenced" NAL unit type)
        switch (slice->m_nalUnitType)
        {
        case NAL_UNIT_CODED_SLICE_TRAIL_R:
            slice->m_nalUnitType = m_bTemporalSublayer ? NAL_UNIT_CODED_SLICE_TSA_N : NAL_UNIT_CODED_SLICE_TRAIL_N;
            break;
        case NAL_UNIT_CODED_SLICE_RADL_R:
            slice->m_nalUnitType = NAL_UNIT_CODED_SLICE_RADL_N;
            break;
        case NAL_UNIT_CODED_SLICE_RASL_R:
            slice->m_nalUnitType = NAL_UNIT_CODED_SLICE_RASL_N;
            break;
        default:
            break;
        }
    }
    else
    {
        /* m_bHasReferences starts out as true for non-B pictures, and is set to false
         * once no more pictures reference it */
        newFrame->m_encData->m_bHasReferences = true;
    }

    m_picList.pushFront(*newFrame);

    // Do decoding refresh marking if any
    decodingRefreshMarking(pocCurr, slice->m_nalUnitType);

    computeRPS(pocCurr, slice->isIRAP(), &slice->m_rps, slice->m_sps->maxDecPicBuffering);

    // Mark pictures in m_piclist as unreferenced if they are not included in RPS
    applyReferencePictureSet(&slice->m_rps, pocCurr);

    slice->m_numRefIdx[0] = X265_MIN(newFrame->m_param->maxNumReferences, slice->m_rps.numberOfNegativePictures); // Ensuring L0 contains just the -ve POC
    slice->m_numRefIdx[1] = X265_MIN(newFrame->m_param->bBPyramid ? 2 : 1, slice->m_rps.numberOfPositivePictures);
    slice->setRefPicList(m_picList);

    X265_CHECK(slice->m_sliceType != B_SLICE || slice->m_numRefIdx[1], "B slice without L1 references (non-fatal)\n");

    if (slice->m_sliceType == B_SLICE)
    {
        /* TODO: the lookahead should be able to tell which reference picture
         * had the least motion residual.  We should be able to use that here to
         * select a colocation reference list and index */
        slice->m_colFromL0Flag = false;
        slice->m_colRefIdx = 0;
        slice->m_bCheckLDC = false;
    }
    else
    {
        slice->m_bCheckLDC = true;
        slice->m_colFromL0Flag = true;
        slice->m_colRefIdx = 0;
    }

    // Disable Loopfilter in bound area, because we will do slice-parallelism in future
    slice->m_sLFaseFlag = (newFrame->m_param->maxSlices > 1) ? false : ((SLFASE_CONSTANT & (1 << (pocCurr % 31))) > 0);

    /* Increment reference count of all motion-referenced frames to prevent them
     * from being recycled. These counts are decremented at the end of
     * compressFrame() */
    int numPredDir = slice->isInterP() ? 1 : slice->isInterB() ? 2 : 0;
    for (int l = 0; l < numPredDir; l++)
    {
        for (int ref = 0; ref < slice->m_numRefIdx[l]; ref++)
        {
            Frame *refpic = slice->m_refFrameList[l][ref];
            ATOMIC_INC(&refpic->m_countRefEncoders);
        }
    }
}

void DPB::computeRPS(int curPoc, bool isRAP, RPS * rps, unsigned int maxDecPicBuffer)
{
    unsigned int poci = 0, numNeg = 0, numPos = 0;

    Frame* iterPic = m_picList.first();

    while (iterPic && (poci < maxDecPicBuffer - 1))
    {
        if ((iterPic->m_poc != curPoc) && iterPic->m_encData->m_bHasReferences)
        {
            rps->poc[poci] = iterPic->m_poc;
            rps->deltaPOC[poci] = rps->poc[poci] - curPoc;
            (rps->deltaPOC[poci] < 0) ? numNeg++ : numPos++;
            rps->bUsed[poci] = !isRAP;
            poci++;
        }
        iterPic = iterPic->m_next;
    }

    rps->numberOfPictures = poci;
    rps->numberOfPositivePictures = numPos;
    rps->numberOfNegativePictures = numNeg;

    rps->sortDeltaPOC();
}

/* Marking reference pictures when an IDR/CRA is encountered. */
void DPB::decodingRefreshMarking(int pocCurr, NalUnitType nalUnitType)
{
    if (nalUnitType == NAL_UNIT_CODED_SLICE_IDR_W_RADL)
    {
        /* If the nal_unit_type is IDR, all pictures in the reference picture
         * list are marked as "unused for reference" */
        Frame* iterFrame = m_picList.first();
        while (iterFrame)
        {
            if (iterFrame->m_poc != pocCurr)
                iterFrame->m_encData->m_bHasReferences = false;
            iterFrame = iterFrame->m_next;
        }
    }
    else // CRA or No DR
    {
        if (m_bRefreshPending && pocCurr > m_pocCRA)
        {
            /* If the bRefreshPending flag is true (a deferred decoding refresh
             * is pending) and the current temporal reference is greater than
             * the temporal reference of the latest CRA picture (pocCRA), mark
             * all reference pictures except the latest CRA picture as "unused
             * for reference" and set the bRefreshPending flag to false */
            Frame* iterFrame = m_picList.first();
            while (iterFrame)
            {
                if (iterFrame->m_poc != pocCurr && iterFrame->m_poc != m_pocCRA)
                    iterFrame->m_encData->m_bHasReferences = false;
                iterFrame = iterFrame->m_next;
            }

            m_bRefreshPending = false;
        }
        if (nalUnitType == NAL_UNIT_CODED_SLICE_CRA)
        {
            /* If the nal_unit_type is CRA, set the bRefreshPending flag to true
             * and pocCRA to the temporal reference of the current picture */
            m_bRefreshPending = true;
            m_pocCRA = pocCurr;
        }
    }

    /* Note that the current picture is already placed in the reference list and
     * its marking is not changed.  If the current picture has a nal_ref_idc
     * that is not 0, it will remain marked as "used for reference" */
}

/** Function for applying picture marking based on the Reference Picture Set */
void DPB::applyReferencePictureSet(RPS *rps, int curPoc)
{
    // loop through all pictures in the reference picture buffer
    Frame* iterFrame = m_picList.first();
    while (iterFrame)
    {
        if (iterFrame->m_poc != curPoc && iterFrame->m_encData->m_bHasReferences)
        {
            // loop through all pictures in the Reference Picture Set
            // to see if the picture should be kept as reference picture
            bool referenced = false;
            for (int i = 0; i < rps->numberOfPositivePictures + rps->numberOfNegativePictures; i++)
            {
                if (iterFrame->m_poc == curPoc + rps->deltaPOC[i])
                {
                    referenced = true;
                    break;
                }
            }
            if (!referenced)
                iterFrame->m_encData->m_bHasReferences = false;
        }
        iterFrame = iterFrame->m_next;
    }
}

/* deciding the nal_unit_type */
NalUnitType DPB::getNalUnitType(int curPOC, bool bIsKeyFrame)
{
    if (!curPOC)
        return NAL_UNIT_CODED_SLICE_IDR_W_RADL;

    if (bIsKeyFrame)
        return m_bOpenGOP ? NAL_UNIT_CODED_SLICE_CRA : NAL_UNIT_CODED_SLICE_IDR_W_RADL;

    if (m_pocCRA && curPOC < m_pocCRA)
        // All leading pictures are being marked as TFD pictures here since
        // current encoder uses all reference pictures while encoding leading
        // pictures. An encoder can ensure that a leading picture can be still
        // decodable when random accessing to a CRA/CRANT/BLA/BLANT picture by
        // controlling the reference pictures used for encoding that leading
        // picture. Such a leading picture need not be marked as a TFD picture.
        return NAL_UNIT_CODED_SLICE_RASL_R;

    if (m_lastIDR && curPOC < m_lastIDR)
        return NAL_UNIT_CODED_SLICE_RADL_R;

    return NAL_UNIT_CODED_SLICE_TRAIL_R;
}
