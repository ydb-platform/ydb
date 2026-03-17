/*****************************************************************************
* Copyright (C) 2013-2017 MulticoreWare, Inc
*
* Author: Steve Borho <steve@borho.org>
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

#include "framedata.h"
#include "picyuv.h"

using namespace X265_NS;

FrameData::FrameData()
{
    memset(this, 0, sizeof(*this));
}

bool FrameData::create(const x265_param& param, const SPS& sps, int csp)
{
    m_param = &param;
    m_slice  = new Slice;
    m_picCTU = new CUData[sps.numCUsInFrame];
    m_picCsp = csp;
    m_spsrpsIdx = -1;
    if (param.rc.bStatWrite)
        m_spsrps = const_cast<RPS*>(sps.spsrps);

    m_cuMemPool.create(0, param.internalCsp, sps.numCUsInFrame, param);
    for (uint32_t ctuAddr = 0; ctuAddr < sps.numCUsInFrame; ctuAddr++)
        m_picCTU[ctuAddr].initialize(m_cuMemPool, 0, param, ctuAddr);

    CHECKED_MALLOC_ZERO(m_cuStat, RCStatCU, sps.numCUsInFrame);
    CHECKED_MALLOC(m_rowStat, RCStatRow, sps.numCuInHeight);
    reinit(sps);
    
    for (int i = 0; i < INTEGRAL_PLANE_NUM; i++)
    {
        m_meBuffer[i] = NULL;
        m_meIntegral[i] = NULL;
    }
    return true;

fail:
    return false;
}

void FrameData::reinit(const SPS& sps)
{
    memset(m_cuStat, 0, sps.numCUsInFrame * sizeof(*m_cuStat));
    memset(m_rowStat, 0, sps.numCuInHeight * sizeof(*m_rowStat));
}

void FrameData::destroy()
{
    delete [] m_picCTU;
    delete m_slice;
    delete m_saoParam;

    m_cuMemPool.destroy();

    X265_FREE(m_cuStat);
    X265_FREE(m_rowStat);

    if (m_meBuffer)
    {
        for (int i = 0; i < INTEGRAL_PLANE_NUM; i++)
        {
            if (m_meBuffer[i] != NULL)
            {
                X265_FREE(m_meBuffer[i]);
                m_meBuffer[i] = NULL;
            }
        }
    }
}
