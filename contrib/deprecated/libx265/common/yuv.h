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

#ifndef X265_YUV_H
#define X265_YUV_H

#include "common.h"
#include "primitives.h"

namespace X265_NS {
// private namespace

class ShortYuv;
class PicYuv;

/* A Yuv instance holds pixels for a square CU (64x64 down to 8x8) for all three planes
 * these are typically used to hold fenc, predictions, or reconstructed blocks */
class Yuv
{
public:

    pixel*   m_buf[3];

    uint32_t m_size;
    uint32_t m_csize;
    int      m_part;         // cached partition enum size

    int      m_csp;
    int      m_hChromaShift;
    int      m_vChromaShift;
    uint32_t *m_integral[2][MAX_NUM_REF][INTEGRAL_PLANE_NUM];

    Yuv();

    bool   create(uint32_t size, int csp);
    void   destroy();

    // Copy YUV buffer to picture buffer
    void   copyToPicYuv(PicYuv& destPicYuv, uint32_t cuAddr, uint32_t absPartIdx) const;

    // Copy YUV buffer from picture buffer
    void   copyFromPicYuv(const PicYuv& srcPicYuv, uint32_t cuAddr, uint32_t absPartIdx);

    // Copy from same size YUV buffer
    void   copyFromYuv(const Yuv& srcYuv);

    // Copy portion of srcYuv into ME prediction buffer
    void   copyPUFromYuv(const Yuv& srcYuv, uint32_t absPartIdx, int partEnum, bool bChroma);

    // Copy Small YUV buffer to the part of other Big YUV buffer
    void   copyToPartYuv(Yuv& dstYuv, uint32_t absPartIdx) const;

    // Copy the part of Big YUV buffer to other Small YUV buffer
    void   copyPartToYuv(Yuv& dstYuv, uint32_t absPartIdx) const;

    // Clip(srcYuv0 + srcYuv1) -> m_buf .. aka recon = clip(pred + residual)
    void   addClip(const Yuv& srcYuv0, const ShortYuv& srcYuv1, uint32_t log2SizeL, int picCsp);

    // (srcYuv0 + srcYuv1)/2 for YUV partition (bidir averaging)
    void   addAvg(const ShortYuv& srcYuv0, const ShortYuv& srcYuv1, uint32_t absPartIdx, uint32_t width, uint32_t height, bool bLuma, bool bChroma);

    void copyPartToPartLuma(Yuv& dstYuv, uint32_t absPartIdx, uint32_t log2Size) const;
    void copyPartToPartChroma(Yuv& dstYuv, uint32_t absPartIdx, uint32_t log2SizeL) const;

    pixel* getLumaAddr(uint32_t absPartIdx)                      { return m_buf[0] + getAddrOffset(absPartIdx, m_size); }
    pixel* getCbAddr(uint32_t absPartIdx)                        { return m_buf[1] + getChromaAddrOffset(absPartIdx); }
    pixel* getCrAddr(uint32_t absPartIdx)                        { return m_buf[2] + getChromaAddrOffset(absPartIdx); }
    pixel* getChromaAddr(uint32_t chromaId, uint32_t absPartIdx) { return m_buf[chromaId] + getChromaAddrOffset(absPartIdx); }

    const pixel* getLumaAddr(uint32_t absPartIdx) const                      { return m_buf[0] + getAddrOffset(absPartIdx, m_size); }
    const pixel* getCbAddr(uint32_t absPartIdx) const                        { return m_buf[1] + getChromaAddrOffset(absPartIdx); }
    const pixel* getCrAddr(uint32_t absPartIdx) const                        { return m_buf[2] + getChromaAddrOffset(absPartIdx); }
    const pixel* getChromaAddr(uint32_t chromaId, uint32_t absPartIdx) const { return m_buf[chromaId] + getChromaAddrOffset(absPartIdx); }

    int getChromaAddrOffset(uint32_t absPartIdx) const
    {
        int blkX = g_zscanToPelX[absPartIdx] >> m_hChromaShift;
        int blkY = g_zscanToPelY[absPartIdx] >> m_vChromaShift;

        return blkX + blkY * m_csize;
    }

    static int getAddrOffset(uint32_t absPartIdx, uint32_t width)
    {
        int blkX = g_zscanToPelX[absPartIdx];
        int blkY = g_zscanToPelY[absPartIdx];

        return blkX + blkY * width;
    }
};
}

#endif
