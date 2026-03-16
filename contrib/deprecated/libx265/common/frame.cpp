/*****************************************************************************
* Copyright (C) 2013-2017 MulticoreWare, Inc
*
* Author: Steve Borho <steve@borho.org>
*         Min Chen <chenm003@163.com>
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
#include "picyuv.h"
#include "framedata.h"

using namespace X265_NS;

Frame::Frame()
{
    m_bChromaExtended = false;
    m_lowresInit = false;
    m_reconRowFlag = NULL;
    m_reconColCount = NULL;
    m_countRefEncoders = 0;
    m_encData = NULL;
    m_reconPic = NULL;
    m_quantOffsets = NULL;
    m_next = NULL;
    m_prev = NULL;
    m_param = NULL;
    m_userSEI.numPayloads = 0;
    m_userSEI.payloads = NULL;
    memset(&m_lowres, 0, sizeof(m_lowres));
    m_rcData = NULL;
    m_encodeStartTime = 0;
    m_reconfigureRc = false;
    m_ctuInfo = NULL;
    m_prevCtuInfoChange = NULL;
    m_addOnDepth = NULL;
    m_addOnCtuInfo = NULL;
    m_addOnPrevChange = NULL;
}

bool Frame::create(x265_param *param, float* quantOffsets)
{
    m_fencPic = new PicYuv;
    m_param = param;
    CHECKED_MALLOC_ZERO(m_rcData, RcStats, 1);

    if (param->bCTUInfo)
    {
        uint32_t widthInCTU = (m_param->sourceWidth + param->maxCUSize - 1) >> m_param->maxLog2CUSize;
        uint32_t heightInCTU = (m_param->sourceHeight +  param->maxCUSize - 1) >> m_param->maxLog2CUSize;
        uint32_t numCTUsInFrame = widthInCTU * heightInCTU;
        CHECKED_MALLOC_ZERO(m_addOnDepth, uint8_t *, numCTUsInFrame);
        CHECKED_MALLOC_ZERO(m_addOnCtuInfo, uint8_t *, numCTUsInFrame);
        CHECKED_MALLOC_ZERO(m_addOnPrevChange, int *, numCTUsInFrame);
        for (uint32_t i = 0; i < numCTUsInFrame; i++)
        {
            CHECKED_MALLOC_ZERO(m_addOnDepth[i], uint8_t, uint32_t(param->num4x4Partitions));
            CHECKED_MALLOC_ZERO(m_addOnCtuInfo[i], uint8_t, uint32_t(param->num4x4Partitions));
            CHECKED_MALLOC_ZERO(m_addOnPrevChange[i], int, uint32_t(param->num4x4Partitions));
        }
    }

    if (param->bMVType == AVC_INFO)
    {
        m_analysisData.wt = NULL;
        m_analysisData.intraData = NULL;
        m_analysisData.interData = NULL;
        m_analysis2Pass.analysisFramedata = NULL;
    }

    if (m_fencPic->create(param, !!m_param->bCopyPicToFrame) && m_lowres.create(m_fencPic, param->bframes, !!param->rc.aqMode || !!param->bAQMotion, param->rc.qgSize))
    {
        X265_CHECK((m_reconColCount == NULL), "m_reconColCount was initialized");
        m_numRows = (m_fencPic->m_picHeight + param->maxCUSize - 1)  / param->maxCUSize;
        m_reconRowFlag = new ThreadSafeInteger[m_numRows];
        m_reconColCount = new ThreadSafeInteger[m_numRows];

        if (quantOffsets)
        {
            int32_t cuCount;
            if (param->rc.qgSize == 8)
                cuCount = m_lowres.maxBlocksInRowFullRes * m_lowres.maxBlocksInColFullRes;
            else
                cuCount = m_lowres.maxBlocksInRow * m_lowres.maxBlocksInCol;
            m_quantOffsets = new float[cuCount];
        }
        return true;
    }
    return false;
fail:
    return false;
}

bool Frame::allocEncodeData(x265_param *param, const SPS& sps)
{
    m_encData = new FrameData;
    m_reconPic = new PicYuv;
    m_param = param;
    m_encData->m_reconPic = m_reconPic;
    bool ok = m_encData->create(*param, sps, m_fencPic->m_picCsp) && m_reconPic->create(param);
    if (ok)
    {
        /* initialize right border of m_reconpicYuv as SAO may read beyond the
         * end of the picture accessing uninitialized pixels */
        int maxHeight = sps.numCuInHeight * param->maxCUSize;
        memset(m_reconPic->m_picOrg[0], 0, sizeof(pixel)* m_reconPic->m_stride * maxHeight);

        /* use pre-calculated cu/pu offsets cached in the SPS structure */
        m_reconPic->m_cuOffsetY = sps.cuOffsetY;
        m_reconPic->m_buOffsetY = sps.buOffsetY;

        if (param->internalCsp != X265_CSP_I400)
        {
            memset(m_reconPic->m_picOrg[1], 0, sizeof(pixel) * m_reconPic->m_strideC * (maxHeight >> m_reconPic->m_vChromaShift));
            memset(m_reconPic->m_picOrg[2], 0, sizeof(pixel) * m_reconPic->m_strideC * (maxHeight >> m_reconPic->m_vChromaShift));

            /* use pre-calculated cu/pu offsets cached in the SPS structure */
            m_reconPic->m_cuOffsetC = sps.cuOffsetC;
            m_reconPic->m_buOffsetC = sps.buOffsetC;
        }
    }
    return ok;
}

/* prepare to re-use a FrameData instance to encode a new picture */
void Frame::reinit(const SPS& sps)
{
    m_bChromaExtended = false;
    m_reconPic = m_encData->m_reconPic;
    m_encData->reinit(sps);
}

void Frame::destroy()
{
    if (m_encData)
    {
        m_encData->destroy();
        delete m_encData;
        m_encData = NULL;
    }

    if (m_fencPic)
    {
        if (m_param->bCopyPicToFrame)
            m_fencPic->destroy();
        delete m_fencPic;
        m_fencPic = NULL;
    }

    if (m_reconPic)
    {
        m_reconPic->destroy();
        delete m_reconPic;
        m_reconPic = NULL;
    }

    if (m_reconRowFlag)
    {
        delete[] m_reconRowFlag;
        m_reconRowFlag = NULL;
    }

    if (m_reconColCount)
    {
        delete[] m_reconColCount;
        m_reconColCount = NULL;
    }

    if (m_quantOffsets)
    {
        delete[] m_quantOffsets;
    }

    if (m_userSEI.numPayloads)
    {
        for (int i = 0; i < m_userSEI.numPayloads; i++)
            delete[] m_userSEI.payloads[i].payload;
        delete[] m_userSEI.payloads;
    }

    if (m_ctuInfo)
    {
        uint32_t widthInCU = (m_param->sourceWidth + m_param->maxCUSize - 1) >> m_param->maxLog2CUSize;
        uint32_t heightInCU = (m_param->sourceHeight + m_param->maxCUSize - 1) >> m_param->maxLog2CUSize;
        uint32_t numCUsInFrame = widthInCU * heightInCU;
        for (uint32_t i = 0; i < numCUsInFrame; i++)
        {
            X265_FREE((*m_ctuInfo + i)->ctuInfo);
            (*m_ctuInfo + i)->ctuInfo = NULL;
            X265_FREE(m_addOnDepth[i]);
            m_addOnDepth[i] = NULL;
            X265_FREE(m_addOnCtuInfo[i]);
            m_addOnCtuInfo[i] = NULL;
            X265_FREE(m_addOnPrevChange[i]);
            m_addOnPrevChange[i] = NULL;
        }
        X265_FREE(*m_ctuInfo);
        *m_ctuInfo = NULL;
        X265_FREE(m_ctuInfo);
        m_ctuInfo = NULL;
        X265_FREE(m_prevCtuInfoChange);
        m_prevCtuInfoChange = NULL;
        X265_FREE(m_addOnDepth);
        m_addOnDepth = NULL;
        X265_FREE(m_addOnCtuInfo);
        m_addOnCtuInfo = NULL;
        X265_FREE(m_addOnPrevChange);
        m_addOnPrevChange = NULL;
    }
    m_lowres.destroy();
    X265_FREE(m_rcData);
}
