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
#include "yuv.h"
#include "shortyuv.h"
#include "picyuv.h"
#include "primitives.h"

using namespace X265_NS;

Yuv::Yuv()
{
    m_buf[0] = NULL;
    m_buf[1] = NULL;
    m_buf[2] = NULL;
}

bool Yuv::create(uint32_t size, int csp)
{
    m_csp = csp;
    m_hChromaShift = CHROMA_H_SHIFT(csp);
    m_vChromaShift = CHROMA_V_SHIFT(csp);

    m_size  = size;
    m_part = partitionFromSizes(size, size);

    for (int i = 0; i < 2; i++)
        for (int j = 0; j < MAX_NUM_REF; j++)
            for (int k = 0; k < INTEGRAL_PLANE_NUM; k++)
                m_integral[i][j][k] = NULL;

    if (csp == X265_CSP_I400)
    {
        CHECKED_MALLOC(m_buf[0], pixel, size * size + 8);
        m_buf[1] = m_buf[2] = 0;
        m_csize = 0;
        return true;
    }
    else
    {
        m_csize = size >> m_hChromaShift;

        size_t sizeL = size * size;
        size_t sizeC = sizeL >> (m_vChromaShift + m_hChromaShift);

        X265_CHECK((sizeC & 15) == 0, "invalid size");

        // memory allocation (padded for SIMD reads)
        CHECKED_MALLOC(m_buf[0], pixel, sizeL + sizeC * 2 + 8);
        m_buf[1] = m_buf[0] + sizeL;
        m_buf[2] = m_buf[0] + sizeL + sizeC;
        return true;
    }

fail:
    return false;
}

void Yuv::destroy()
{
    X265_FREE(m_buf[0]);
}

void Yuv::copyToPicYuv(PicYuv& dstPic, uint32_t cuAddr, uint32_t absPartIdx) const
{
    pixel* dstY = dstPic.getLumaAddr(cuAddr, absPartIdx);
    primitives.cu[m_part].copy_pp(dstY, dstPic.m_stride, m_buf[0], m_size);
    if (m_csp != X265_CSP_I400)
    {
        pixel* dstU = dstPic.getCbAddr(cuAddr, absPartIdx);
        pixel* dstV = dstPic.getCrAddr(cuAddr, absPartIdx);
        primitives.chroma[m_csp].cu[m_part].copy_pp(dstU, dstPic.m_strideC, m_buf[1], m_csize);
        primitives.chroma[m_csp].cu[m_part].copy_pp(dstV, dstPic.m_strideC, m_buf[2], m_csize);
    }
}

void Yuv::copyFromPicYuv(const PicYuv& srcPic, uint32_t cuAddr, uint32_t absPartIdx)
{
    const pixel* srcY = srcPic.getLumaAddr(cuAddr, absPartIdx);
    primitives.cu[m_part].copy_pp(m_buf[0], m_size, srcY, srcPic.m_stride);
    if (m_csp != X265_CSP_I400)
    {
        const pixel* srcU = srcPic.getCbAddr(cuAddr, absPartIdx);
        const pixel* srcV = srcPic.getCrAddr(cuAddr, absPartIdx);
        primitives.chroma[m_csp].cu[m_part].copy_pp(m_buf[1], m_csize, srcU, srcPic.m_strideC);
        primitives.chroma[m_csp].cu[m_part].copy_pp(m_buf[2], m_csize, srcV, srcPic.m_strideC);
    }
}

void Yuv::copyFromYuv(const Yuv& srcYuv)
{
    X265_CHECK(m_size >= srcYuv.m_size, "invalid size\n");

    primitives.cu[m_part].copy_pp(m_buf[0], m_size, srcYuv.m_buf[0], srcYuv.m_size);
    if (m_csp != X265_CSP_I400)
    {
        primitives.chroma[m_csp].cu[m_part].copy_pp(m_buf[1], m_csize, srcYuv.m_buf[1], srcYuv.m_csize);
        primitives.chroma[m_csp].cu[m_part].copy_pp(m_buf[2], m_csize, srcYuv.m_buf[2], srcYuv.m_csize);
    }
}

/* This version is intended for use by ME, which required FENC_STRIDE for luma fenc pixels */
void Yuv::copyPUFromYuv(const Yuv& srcYuv, uint32_t absPartIdx, int partEnum, bool bChroma)
{
    X265_CHECK(m_size == FENC_STRIDE && m_size >= srcYuv.m_size, "PU buffer size mismatch\n");

    const pixel* srcY = srcYuv.m_buf[0] + getAddrOffset(absPartIdx, srcYuv.m_size);
    primitives.pu[partEnum].copy_pp(m_buf[0], m_size, srcY, srcYuv.m_size);

    if (bChroma)
    {
        const pixel* srcU = srcYuv.m_buf[1] + srcYuv.getChromaAddrOffset(absPartIdx);
        const pixel* srcV = srcYuv.m_buf[2] + srcYuv.getChromaAddrOffset(absPartIdx);
        primitives.chroma[m_csp].pu[partEnum].copy_pp(m_buf[1], m_csize, srcU, srcYuv.m_csize);
        primitives.chroma[m_csp].pu[partEnum].copy_pp(m_buf[2], m_csize, srcV, srcYuv.m_csize);
    }
}

void Yuv::copyToPartYuv(Yuv& dstYuv, uint32_t absPartIdx) const
{
    pixel* dstY = dstYuv.getLumaAddr(absPartIdx);
    primitives.cu[m_part].copy_pp(dstY, dstYuv.m_size, m_buf[0], m_size);
    if (m_csp != X265_CSP_I400)
    {
        pixel* dstU = dstYuv.getCbAddr(absPartIdx);
        pixel* dstV = dstYuv.getCrAddr(absPartIdx);
        primitives.chroma[m_csp].cu[m_part].copy_pp(dstU, dstYuv.m_csize, m_buf[1], m_csize);
        primitives.chroma[m_csp].cu[m_part].copy_pp(dstV, dstYuv.m_csize, m_buf[2], m_csize);
    }
}

void Yuv::copyPartToYuv(Yuv& dstYuv, uint32_t absPartIdx) const
{
    pixel* srcY = m_buf[0] + getAddrOffset(absPartIdx, m_size);
    pixel* dstY = dstYuv.m_buf[0];
    primitives.cu[dstYuv.m_part].copy_pp(dstY, dstYuv.m_size, srcY, m_size);
    if (m_csp != X265_CSP_I400)
    {
        pixel* srcU = m_buf[1] + getChromaAddrOffset(absPartIdx);
        pixel* srcV = m_buf[2] + getChromaAddrOffset(absPartIdx);
        pixel* dstU = dstYuv.m_buf[1];
        pixel* dstV = dstYuv.m_buf[2];
        primitives.chroma[m_csp].cu[dstYuv.m_part].copy_pp(dstU, dstYuv.m_csize, srcU, m_csize);
        primitives.chroma[m_csp].cu[dstYuv.m_part].copy_pp(dstV, dstYuv.m_csize, srcV, m_csize);
    }
}

void Yuv::addClip(const Yuv& srcYuv0, const ShortYuv& srcYuv1, uint32_t log2SizeL, int picCsp)
{
    primitives.cu[log2SizeL - 2].add_ps(m_buf[0], m_size, srcYuv0.m_buf[0], srcYuv1.m_buf[0], srcYuv0.m_size, srcYuv1.m_size);
    if (m_csp != X265_CSP_I400 && picCsp != X265_CSP_I400)
    {
        primitives.chroma[m_csp].cu[log2SizeL - 2].add_ps(m_buf[1], m_csize, srcYuv0.m_buf[1], srcYuv1.m_buf[1], srcYuv0.m_csize, srcYuv1.m_csize);
        primitives.chroma[m_csp].cu[log2SizeL - 2].add_ps(m_buf[2], m_csize, srcYuv0.m_buf[2], srcYuv1.m_buf[2], srcYuv0.m_csize, srcYuv1.m_csize);
    }
    if (picCsp == X265_CSP_I400 && m_csp != X265_CSP_I400)
    {
        primitives.chroma[m_csp].cu[m_part].copy_pp(m_buf[1], m_csize, srcYuv0.m_buf[1], srcYuv0.m_csize);
        primitives.chroma[m_csp].cu[m_part].copy_pp(m_buf[2], m_csize, srcYuv0.m_buf[2], srcYuv0.m_csize);
    }
}

void Yuv::addAvg(const ShortYuv& srcYuv0, const ShortYuv& srcYuv1, uint32_t absPartIdx, uint32_t width, uint32_t height, bool bLuma, bool bChroma)
{
    int part = partitionFromSizes(width, height);

    if (bLuma)
    {
        const int16_t* srcY0 = srcYuv0.getLumaAddr(absPartIdx);
        const int16_t* srcY1 = srcYuv1.getLumaAddr(absPartIdx);
        pixel* dstY = getLumaAddr(absPartIdx);
        primitives.pu[part].addAvg(srcY0, srcY1, dstY, srcYuv0.m_size, srcYuv1.m_size, m_size);
    }
    if (bChroma)
    {
        const int16_t* srcU0 = srcYuv0.getCbAddr(absPartIdx);
        const int16_t* srcV0 = srcYuv0.getCrAddr(absPartIdx);
        const int16_t* srcU1 = srcYuv1.getCbAddr(absPartIdx);
        const int16_t* srcV1 = srcYuv1.getCrAddr(absPartIdx);
        pixel* dstU = getCbAddr(absPartIdx);
        pixel* dstV = getCrAddr(absPartIdx);
        primitives.chroma[m_csp].pu[part].addAvg(srcU0, srcU1, dstU, srcYuv0.m_csize, srcYuv1.m_csize, m_csize);
        primitives.chroma[m_csp].pu[part].addAvg(srcV0, srcV1, dstV, srcYuv0.m_csize, srcYuv1.m_csize, m_csize);
    }
}

void Yuv::copyPartToPartLuma(Yuv& dstYuv, uint32_t absPartIdx, uint32_t log2Size) const
{
    const pixel* src = getLumaAddr(absPartIdx);
    pixel* dst = dstYuv.getLumaAddr(absPartIdx);
    primitives.cu[log2Size - 2].copy_pp(dst, dstYuv.m_size, src, m_size);
}

void Yuv::copyPartToPartChroma(Yuv& dstYuv, uint32_t absPartIdx, uint32_t log2SizeL) const
{
    const pixel* srcU = getCbAddr(absPartIdx);
    const pixel* srcV = getCrAddr(absPartIdx);
    pixel* dstU = dstYuv.getCbAddr(absPartIdx);
    pixel* dstV = dstYuv.getCrAddr(absPartIdx);
    primitives.chroma[m_csp].cu[log2SizeL - 2].copy_pp(dstU, dstYuv.m_csize, srcU, m_csize);
    primitives.chroma[m_csp].cu[log2SizeL - 2].copy_pp(dstV, dstYuv.m_csize, srcV, m_csize);
}
