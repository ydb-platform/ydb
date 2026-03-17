/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Steve Borho <steve@borho.org>
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

#ifndef X265_DPB_H
#define X265_DPB_H

#include "piclist.h"

namespace X265_NS {
// private namespace for x265

class Frame;
class FrameData;
class Slice;

class DPB
{
public:

    int                m_lastIDR;
    int                m_pocCRA;
    int                m_bOpenGOP;
    bool               m_bRefreshPending;
    bool               m_bTemporalSublayer;
    PicList            m_picList;
    PicList            m_freeList;
    FrameData*         m_frameDataFreeList;

    DPB(x265_param *param)
    {
        m_lastIDR = 0;
        m_pocCRA = 0;
        m_bRefreshPending = false;
        m_frameDataFreeList = NULL;
        m_bOpenGOP = param->bOpenGOP;
        m_bTemporalSublayer = !!param->bEnableTemporalSubLayers;
    }

    ~DPB();

    void prepareEncode(Frame*);

    void recycleUnreferenced();

protected:

    void computeRPS(int curPoc, bool isRAP, RPS * rps, unsigned int maxDecPicBuffer);

    void applyReferencePictureSet(RPS *rps, int curPoc);
    void decodingRefreshMarking(int pocCurr, NalUnitType nalUnitType);

    NalUnitType getNalUnitType(int curPoc, bool bIsKeyFrame);
};
}

#endif // X265_DPB_H
