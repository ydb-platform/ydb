/*****************************************************************************
 * x265: ShortYUV class for short sized YUV-style frames
 *****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Deepthi Nandakumar <deepthi@multicorewareinc.com>
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
 * For more information, contact us at license @ x265.com
 *****************************************************************************/

#ifndef X265_SHORTYUV_H
#define X265_SHORTYUV_H

#include "common.h"

namespace X265_NS {
// private namespace

class Yuv;

/* A ShortYuv instance holds int16_ts for a square CU (64x64 down to 8x8) for all three planes,
 * these are typically used to hold residual or coefficients */
class ShortYuv
{
public:

    int16_t* m_buf[3];

    uint32_t m_size;
    uint32_t m_csize;

    int      m_csp;
    int      m_hChromaShift;
    int      m_vChromaShift;

    ShortYuv();

    bool create(uint32_t size, int csp);
    void destroy();
    void clear();

    int16_t* getLumaAddr(uint32_t absPartIdx)                       { return m_buf[0] + getAddrOffset(absPartIdx, m_size); }
    int16_t* getCbAddr(uint32_t absPartIdx)                         { return m_buf[1] + getChromaAddrOffset(absPartIdx); }
    int16_t* getCrAddr(uint32_t absPartIdx)                         { return m_buf[2] + getChromaAddrOffset(absPartIdx); }
    int16_t* getChromaAddr(uint32_t chromaId, uint32_t partUnitIdx) { return m_buf[chromaId] + getChromaAddrOffset(partUnitIdx); }

    const int16_t* getLumaAddr(uint32_t absPartIdx) const                       { return m_buf[0] + getAddrOffset(absPartIdx, m_size); }
    const int16_t* getCbAddr(uint32_t absPartIdx) const                         { return m_buf[1] + getChromaAddrOffset(absPartIdx); }
    const int16_t* getCrAddr(uint32_t absPartIdx) const                         { return m_buf[2] + getChromaAddrOffset(absPartIdx); }
    const int16_t* getChromaAddr(uint32_t chromaId, uint32_t partUnitIdx) const { return m_buf[chromaId] + getChromaAddrOffset(partUnitIdx); }

    void subtract(const Yuv& srcYuv0, const Yuv& srcYuv1, uint32_t log2Size, int picCsp);

    void copyPartToPartLuma(ShortYuv& dstYuv, uint32_t absPartIdx, uint32_t log2Size) const;
    void copyPartToPartChroma(ShortYuv& dstYuv, uint32_t absPartIdx, uint32_t log2SizeL) const;

    void copyPartToPartLuma(Yuv& dstYuv, uint32_t absPartIdx, uint32_t log2Size) const;
    void copyPartToPartChroma(Yuv& dstYuv, uint32_t absPartIdx, uint32_t log2SizeL) const;

    int getChromaAddrOffset(uint32_t idx) const
    {
        int blkX = g_zscanToPelX[idx] >> m_hChromaShift;
        int blkY = g_zscanToPelY[idx] >> m_vChromaShift;

        return blkX + blkY * m_csize;
    }

    static int getAddrOffset(uint32_t idx, uint32_t width)
    {
        int blkX = g_zscanToPelX[idx];
        int blkY = g_zscanToPelY[idx];

        return blkX + blkY * width;
    }
};
}

#endif // ifndef X265_SHORTYUV_H
