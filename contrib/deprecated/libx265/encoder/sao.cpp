/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Steve Borho <steve@borho.org>
 *          Min Chen <chenm003@163.com>
 *          Praveen Kumar Tiwari <praveen@multicorewareinc.com>
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
#include "sao.h"

namespace {

inline int32_t roundIBDI(int32_t num, int32_t den)
{
    return num >= 0 ? ((num * 2 + den) / (den * 2)) : -((-num * 2 + den) / (den * 2));
}

/* get the sign of input variable (TODO: this is a dup, make common) */
inline int8_t signOf(int x)
{
    return (x >> 31) | ((int)((((uint32_t)-x)) >> 31));
}

inline int signOf2(const int a, const int b)
{
    // NOTE: don't reorder below compare, both ICL, VC, GCC optimize strong depends on order!
    int r = 0;
    if (a < b)
        r = -1;
    if (a > b)
        r = 1;
    return r;
}

inline int64_t estSaoDist(int32_t count, int32_t offset, int32_t offsetOrg)
{
    return (count * offset - offsetOrg * 2) * offset;
}
} // end anonymous namespace


namespace X265_NS {

const uint32_t SAO::s_eoTable[NUM_EDGETYPE] =
{
    1, // 0
    2, // 1
    0, // 2
    3, // 3
    4  // 4
};

SAO::SAO()
{
    m_countPreDblk = NULL;
    m_offsetOrgPreDblk = NULL;
    m_refDepth = 0;
    m_param = NULL;
    m_clipTable = NULL;
    m_clipTableBase = NULL;
    m_tmpU[0] = NULL;
    m_tmpU[1] = NULL;
    m_tmpU[2] = NULL;
    m_tmpL1[0] = NULL;
    m_tmpL1[1] = NULL;
    m_tmpL1[2] = NULL;
    m_tmpL2[0] = NULL;
    m_tmpL2[1] = NULL;
    m_tmpL2[2] = NULL;
    m_depthSaoRate = NULL;
}

bool SAO::create(x265_param* param, int initCommon)
{
    m_param = param;
    m_chromaFormat = param->internalCsp;
    m_hChromaShift = CHROMA_H_SHIFT(param->internalCsp);
    m_vChromaShift = CHROMA_V_SHIFT(param->internalCsp);

    m_numCuInWidth =  (m_param->sourceWidth + m_param->maxCUSize - 1) / m_param->maxCUSize;
    m_numCuInHeight = (m_param->sourceHeight + m_param->maxCUSize - 1) / m_param->maxCUSize;

    const pixel maxY = (1 << X265_DEPTH) - 1;
    const pixel rangeExt = maxY >> 1;
    int numCtu = m_numCuInWidth * m_numCuInHeight;

    for (int i = 0; i < (param->internalCsp != X265_CSP_I400 ? 3 : 1); i++)
    {
        CHECKED_MALLOC(m_tmpL1[i], pixel, m_param->maxCUSize + 1);
        CHECKED_MALLOC(m_tmpL2[i], pixel, m_param->maxCUSize + 1);

        // SAO asm code will read 1 pixel before and after, so pad by 2
        // NOTE: m_param->sourceWidth+2 enough, to avoid condition check in copySaoAboveRef(), I alloc more up to 63 bytes in here
        CHECKED_MALLOC(m_tmpU[i], pixel, m_numCuInWidth * m_param->maxCUSize + 2 + 32);
        m_tmpU[i] += 1;
    }

    if (initCommon)
    {
        if (m_param->bSaoNonDeblocked)
        {
            CHECKED_MALLOC(m_countPreDblk, PerPlane, numCtu);
            CHECKED_MALLOC(m_offsetOrgPreDblk, PerPlane, numCtu);
        }
        CHECKED_MALLOC(m_depthSaoRate, double, 2 * SAO_DEPTHRATE_SIZE);

        m_depthSaoRate[0 * SAO_DEPTHRATE_SIZE + 0] = 0;
        m_depthSaoRate[0 * SAO_DEPTHRATE_SIZE + 1] = 0;
        m_depthSaoRate[0 * SAO_DEPTHRATE_SIZE + 2] = 0;
        m_depthSaoRate[0 * SAO_DEPTHRATE_SIZE + 3] = 0;
        m_depthSaoRate[1 * SAO_DEPTHRATE_SIZE + 0] = 0;
        m_depthSaoRate[1 * SAO_DEPTHRATE_SIZE + 1] = 0;
        m_depthSaoRate[1 * SAO_DEPTHRATE_SIZE + 2] = 0;
        m_depthSaoRate[1 * SAO_DEPTHRATE_SIZE + 3] = 0;

        CHECKED_MALLOC(m_clipTableBase,  pixel, maxY + 2 * rangeExt);
        m_clipTable = &(m_clipTableBase[rangeExt]);

        // Share with fast clip lookup table

        for (int i = 0; i < rangeExt; i++)
            m_clipTableBase[i] = 0;

        for (int i = 0; i < maxY; i++)
            m_clipTable[i] = (pixel)i;

        for (int i = maxY; i < maxY + rangeExt; i++)
            m_clipTable[i] = maxY;

    }
    else
    {
        // must initialize these common pointer outside of function
        m_countPreDblk = NULL;
        m_offsetOrgPreDblk = NULL;
        m_clipTableBase = NULL;
        m_clipTable = NULL;
    }

    return true;

fail:
    return false;
}

void SAO::createFromRootNode(SAO* root)
{
    X265_CHECK(m_countPreDblk == NULL, "duplicate initialize on m_countPreDblk");
    X265_CHECK(m_offsetOrgPreDblk == NULL, "duplicate initialize on m_offsetOrgPreDblk");
    X265_CHECK(m_depthSaoRate == NULL, "duplicate initialize on m_depthSaoRate");
    X265_CHECK(m_clipTableBase == NULL, "duplicate initialize on m_clipTableBase");
    X265_CHECK(m_clipTable == NULL, "duplicate initialize on m_clipTable");

    m_countPreDblk = root->m_countPreDblk;
    m_offsetOrgPreDblk = root->m_offsetOrgPreDblk;
    m_depthSaoRate = root->m_depthSaoRate;
    m_clipTableBase = root->m_clipTableBase; // Unnecessary
    m_clipTable = root->m_clipTable;
}

void SAO::destroy(int destoryCommon)
{
    for (int i = 0; i < 3; i++)
    {
        if (m_tmpL1[i])
        {
            X265_FREE(m_tmpL1[i]);
            m_tmpL1[i] = NULL;
        }

        if (m_tmpL2[i])
        {
            X265_FREE(m_tmpL2[i]);
            m_tmpL2[i] = NULL;
        }

        if (m_tmpU[i])
        {
            X265_FREE(m_tmpU[i] - 1);
            m_tmpU[i] = NULL;
        }
    }

    if (destoryCommon)
    {
        if (m_param->bSaoNonDeblocked)
        {
            X265_FREE_ZERO(m_countPreDblk);
            X265_FREE_ZERO(m_offsetOrgPreDblk);
        }
        X265_FREE_ZERO(m_depthSaoRate);
        X265_FREE_ZERO(m_clipTableBase);
    }
}

/* allocate memory for SAO parameters */
void SAO::allocSaoParam(SAOParam* saoParam) const
{
    int planes = (m_param->internalCsp != X265_CSP_I400) ? 3 : 1;
    saoParam->numCuInWidth  = m_numCuInWidth;

    for (int i = 0; i < planes; i++)
        saoParam->ctuParam[i] = new SaoCtuParam[m_numCuInHeight * m_numCuInWidth];
}

void SAO::startSlice(Frame* frame, Entropy& initState)
{
    m_frame = frame;
    Slice* slice = m_frame->m_encData->m_slice;

    switch (slice->m_sliceType)
    {
    case I_SLICE:
        m_refDepth = 0;
        break;
    case P_SLICE:
        m_refDepth = 1;
        break;
    case B_SLICE:
        m_refDepth = 2 + !IS_REFERENCED(frame);
        break;
    }

    m_entropyCoder.load(initState);
    m_rdContexts.next.load(initState);
    m_rdContexts.cur.load(initState);

    SAOParam* saoParam = frame->m_encData->m_saoParam;
    if (!saoParam)
    {
        saoParam = new SAOParam;
        allocSaoParam(saoParam);
        frame->m_encData->m_saoParam = saoParam;
    }

    saoParam->bSaoFlag[0] = true;
    saoParam->bSaoFlag[1] = m_param->internalCsp != X265_CSP_I400 && m_frame->m_fencPic->m_picCsp != X265_CSP_I400;

    m_numNoSao[0] = 0; // Luma
    m_numNoSao[1] = 0; // Chroma

    // NOTE: Allow SAO automatic turn-off only when frame parallelism is disabled.
    if (m_param->frameNumThreads == 1)
    {
        if (m_refDepth > 0 && m_depthSaoRate[0 * SAO_DEPTHRATE_SIZE + m_refDepth - 1] > SAO_ENCODING_RATE)
            saoParam->bSaoFlag[0] = false;
        if (m_refDepth > 0 && m_depthSaoRate[1 * SAO_DEPTHRATE_SIZE + m_refDepth - 1] > SAO_ENCODING_RATE_CHROMA)
            saoParam->bSaoFlag[1] = false;
    }
}

// CTU-based SAO process without slice granularity
void SAO::applyPixelOffsets(int addr, int typeIdx, int plane)
{
    PicYuv* reconPic = m_frame->m_reconPic;
    pixel* rec = reconPic->getPlaneAddr(plane, addr);
    intptr_t stride = plane ? reconPic->m_strideC : reconPic->m_stride;
    uint32_t picWidth  = m_param->sourceWidth;
    uint32_t picHeight = m_param->sourceHeight;
    const CUData* cu = m_frame->m_encData->getPicCTU(addr);
    int ctuWidth = m_param->maxCUSize;
    int ctuHeight = m_param->maxCUSize;
    uint32_t lpelx = cu->m_cuPelX;
    uint32_t tpely = cu->m_cuPelY;
    const uint32_t firstRowInSlice = cu->m_bFirstRowInSlice;
    const uint32_t lastRowInSlice = cu->m_bLastRowInSlice;
    const uint32_t bAboveUnavail = (!tpely) | firstRowInSlice;

    // NOTE: Careful! the picHeight for Equal operator only, so I may safe to hack it
    if (lastRowInSlice)
    {
        picHeight = x265_min(picHeight, (tpely + ctuHeight));
    }

    if (plane)
    {
        picWidth  >>= m_hChromaShift;
        picHeight >>= m_vChromaShift;
        ctuWidth  >>= m_hChromaShift;
        ctuHeight >>= m_vChromaShift;
        lpelx     >>= m_hChromaShift;
        tpely     >>= m_vChromaShift;
    }
    uint32_t rpelx = x265_min(lpelx + ctuWidth,  picWidth);
    uint32_t bpely = x265_min(tpely + ctuHeight, picHeight);
    ctuWidth  = rpelx - lpelx;
    ctuHeight = bpely - tpely;

    int8_t _upBuff1[MAX_CU_SIZE + 2], *upBuff1 = _upBuff1 + 1, signLeft1[2];
    int8_t _upBufft[MAX_CU_SIZE + 2], *upBufft = _upBufft + 1;

    memset(_upBuff1 + MAX_CU_SIZE, 0, 2 * sizeof(int8_t)); /* avoid valgrind uninit warnings */

    pixel* tmpL = m_tmpL1[plane];
    pixel* tmpU = &(m_tmpU[plane][lpelx]);

    int8_t* offsetEo = m_offsetEo[plane];

    switch (typeIdx)
    {
    case SAO_EO_0: // dir: -
    {
        pixel firstPxl = 0, lastPxl = 0, row1FirstPxl = 0, row1LastPxl = 0;
        int startX = !lpelx;
        int endX   = (rpelx == picWidth) ? ctuWidth - 1 : ctuWidth;
        if (ctuWidth & 15)
        {
            for (int y = 0; y < ctuHeight; y++, rec += stride)
            {
                int signLeft = signOf(rec[startX] - tmpL[y]);
                for (int x = startX; x < endX; x++)
                {
                    int signRight = signOf(rec[x] - rec[x + 1]);
                    int edgeType = signRight + signLeft + 2;
                    signLeft = -signRight;

                    rec[x] = m_clipTable[rec[x] + offsetEo[edgeType]];
                }
            }
        }
        else
        {
            for (int y = 0; y < ctuHeight; y += 2, rec += 2 * stride)
            {
                signLeft1[0] = signOf(rec[startX] - tmpL[y]);
                signLeft1[1] = signOf(rec[stride + startX] - tmpL[y + 1]);

                if (!lpelx)
                {
                    firstPxl = rec[0];
                    row1FirstPxl = rec[stride];
                }

                if (rpelx == picWidth)
                {
                    lastPxl = rec[ctuWidth - 1];
                    row1LastPxl = rec[stride + ctuWidth - 1];
                }

                primitives.saoCuOrgE0(rec, offsetEo, ctuWidth, signLeft1, stride);

                if (!lpelx)
                {
                    rec[0] = firstPxl;
                    rec[stride] = row1FirstPxl;
                }

                if (rpelx == picWidth)
                {
                    rec[ctuWidth - 1] = lastPxl;
                    rec[stride + ctuWidth - 1] = row1LastPxl;
                }
            }
        }
        break;
    }
    case SAO_EO_1: // dir: |
    {
        int startY = bAboveUnavail;
        int endY   = (bpely == picHeight) ? ctuHeight - 1 : ctuHeight;
        if (startY)
            rec += stride;

        if (ctuWidth & 15)
        {
            for (int x = 0; x < ctuWidth; x++)
                upBuff1[x] = signOf(rec[x] - tmpU[x]);

            for (int y = startY; y < endY; y++, rec += stride)
            {
                for (int x = 0; x < ctuWidth; x++)
                {
                    int8_t signDown = signOf(rec[x] - rec[x + stride]);
                    int edgeType = signDown + upBuff1[x] + 2;
                    upBuff1[x] = -signDown;

                    rec[x] = m_clipTable[rec[x] + offsetEo[edgeType]];
                }
            }
        }
        else
        {
            primitives.sign(upBuff1, rec, tmpU, ctuWidth);

            int diff = (endY - startY) % 2;
            for (int y = startY; y < endY - diff; y += 2, rec += 2 * stride)
                primitives.saoCuOrgE1_2Rows(rec, upBuff1, offsetEo, stride, ctuWidth);

            if (diff & 1)
                primitives.saoCuOrgE1(rec, upBuff1, offsetEo, stride, ctuWidth);
        }

        break;
    }
    case SAO_EO_2: // dir: 135
    {
        int startX = !lpelx;
        int endX   = (rpelx == picWidth) ? ctuWidth - 1 : ctuWidth;

        int startY = bAboveUnavail;
        int endY   = (bpely == picHeight) ? ctuHeight - 1 : ctuHeight;

        if (startY)
            rec += stride;

        if (!(ctuWidth & 15))
        {
            int8_t firstSign, lastSign;

            if (!lpelx)
                firstSign = upBuff1[0];

            if (rpelx == picWidth)
                lastSign = upBuff1[ctuWidth - 1];

            primitives.sign(upBuff1, rec, &tmpU[- 1], ctuWidth);

            if (!lpelx)
                upBuff1[0] = firstSign;

            if (rpelx == picWidth)
                upBuff1[ctuWidth - 1] = lastSign;
        }
        else
        {
            for (int x = startX; x < endX; x++)
                upBuff1[x] = signOf(rec[x] - tmpU[x - 1]);
        }

        if (ctuWidth & 15)
        {
             for (int y = startY; y < endY; y++, rec += stride)
             {
                 upBufft[startX] = signOf(rec[stride + startX] - tmpL[y]);
                 for (int x = startX; x < endX; x++)
                 {
                     int8_t signDown = signOf(rec[x] - rec[x + stride + 1]);
                     int edgeType = signDown + upBuff1[x] + 2;
                     upBufft[x + 1] = -signDown;
                     rec[x] = m_clipTable[rec[x] + offsetEo[edgeType]];
                 }

                 std::swap(upBuff1, upBufft);
             }
        }
        else
        {
            for (int y = startY; y < endY; y++, rec += stride)
            {
                int8_t iSignDown2 = signOf(rec[stride + startX] - tmpL[y]);

                primitives.saoCuOrgE2[endX > 16](rec + startX, upBufft + startX, upBuff1 + startX, offsetEo, endX - startX, stride);

                upBufft[startX] = iSignDown2;

                std::swap(upBuff1, upBufft);
            }
        }
        break;
    }
    case SAO_EO_3: // dir: 45
    {
        int startX = !lpelx;
        int endX   = (rpelx == picWidth) ? ctuWidth - 1 : ctuWidth;

        int startY = bAboveUnavail;
        int endY   = (bpely == picHeight) ? ctuHeight - 1 : ctuHeight;

        if (startY)
            rec += stride;

        if (ctuWidth & 15)
        {
            for (int x = startX - 1; x < endX; x++)
                upBuff1[x] = signOf(rec[x] - tmpU[x + 1]);

            for (int y = startY; y < endY; y++, rec += stride)
            {
                int x = startX;
                int8_t signDown = signOf(rec[x] - tmpL[y + 1]);
                int edgeType = signDown + upBuff1[x] + 2;
                upBuff1[x - 1] = -signDown;
                rec[x] = m_clipTable[rec[x] + offsetEo[edgeType]];

                for (x = startX + 1; x < endX; x++)
                {
                    signDown = signOf(rec[x] - rec[x + stride - 1]);
                    edgeType = signDown + upBuff1[x] + 2;
                    upBuff1[x - 1] = -signDown;
                    rec[x] = m_clipTable[rec[x] + offsetEo[edgeType]];
                }

                upBuff1[endX - 1] = signOf(rec[endX - 1 + stride] - rec[endX]);
            }
        }
        else
        {
            int8_t firstSign, lastSign;

            if (lpelx)
                firstSign = signOf(rec[-1] - tmpU[0]);
            if (rpelx == picWidth)
                lastSign = upBuff1[ctuWidth - 1];

            primitives.sign(upBuff1, rec, &tmpU[1], ctuWidth);

            if (lpelx)
                upBuff1[-1] = firstSign;
            if (rpelx == picWidth)
                upBuff1[ctuWidth - 1] = lastSign;

            for (int y = startY; y < endY; y++, rec += stride)
            {
                int x = startX;
                int8_t signDown = signOf(rec[x] - tmpL[y + 1]);
                int edgeType = signDown + upBuff1[x] + 2;
                upBuff1[x - 1] = -signDown;
                rec[x] = m_clipTable[rec[x] + offsetEo[edgeType]];

                primitives.saoCuOrgE3[endX > 16](rec, upBuff1, offsetEo, stride - 1, startX, endX);

                upBuff1[endX - 1] = signOf(rec[endX - 1 + stride] - rec[endX]);
            }
        }

        break;
    }
    case SAO_BO:
    {
        const int8_t* offsetBo = m_offsetBo[plane];

        if (ctuWidth & 15)
        {
            #define SAO_BO_BITS 5
            const int boShift = X265_DEPTH - SAO_BO_BITS;

            for (int y = 0; y < ctuHeight; y++, rec += stride)
                for (int x = 0; x < ctuWidth; x++)
                    rec[x] = x265_clip(rec[x] + offsetBo[rec[x] >> boShift]);
        }
        else
            primitives.saoCuOrgB0(rec, offsetBo, ctuWidth, ctuHeight, stride);

        break;
    }
    default: break;
    }
}

/* Process SAO unit */
void SAO::generateLumaOffsets(SaoCtuParam* ctuParam, int idxY, int idxX)
{
    PicYuv* reconPic = m_frame->m_reconPic;
    intptr_t stride = reconPic->m_stride;
    int ctuWidth = m_param->maxCUSize;
    int ctuHeight = m_param->maxCUSize;

    int addr = idxY * m_numCuInWidth + idxX;
    pixel* rec = reconPic->getLumaAddr(addr);

    if (idxX == 0)
    {
        for (int i = 0; i < ctuHeight + 1; i++)
        {
            m_tmpL1[0][i] = rec[0];
            rec += stride;
        }
    }

    bool mergeLeftFlag = (ctuParam[addr].mergeMode == SAO_MERGE_LEFT);
    int typeIdx = ctuParam[addr].typeIdx;

    if (idxX != (m_numCuInWidth - 1))
    {
        rec = reconPic->getLumaAddr(addr);
        for (int i = 0; i < ctuHeight + 1; i++)
        {
            m_tmpL2[0][i] = rec[ctuWidth - 1];
            rec += stride;
        }
    }

    if (typeIdx >= 0)
    {
        if (!mergeLeftFlag)
        {
            if (typeIdx == SAO_BO)
            {
                memset(m_offsetBo[0], 0, sizeof(m_offsetBo[0]));

                for (int i = 0; i < SAO_NUM_OFFSET; i++)
                    m_offsetBo[0][((ctuParam[addr].bandPos + i) & (MAX_NUM_SAO_CLASS - 1))] = (int8_t)(ctuParam[addr].offset[i] << SAO_BIT_INC);
            }
            else // if (typeIdx == SAO_EO_0 || typeIdx == SAO_EO_1 || typeIdx == SAO_EO_2 || typeIdx == SAO_EO_3)
            {
                int offset[NUM_EDGETYPE];
                offset[0] = 0;
                for (int i = 0; i < SAO_NUM_OFFSET; i++)
                    offset[i + 1] = ctuParam[addr].offset[i] << SAO_BIT_INC;

                for (int edgeType = 0; edgeType < NUM_EDGETYPE; edgeType++)
                    m_offsetEo[0][edgeType] = (int8_t)offset[s_eoTable[edgeType]];
            }
        }
        applyPixelOffsets(addr, typeIdx, 0);
    }
    std::swap(m_tmpL1[0], m_tmpL2[0]);
}

/* Process SAO unit (Chroma only) */
void SAO::generateChromaOffsets(SaoCtuParam* ctuParam[3], int idxY, int idxX)
{
    PicYuv* reconPic = m_frame->m_reconPic;
    intptr_t stride = reconPic->m_strideC;
    int ctuWidth  = m_param->maxCUSize;
    int ctuHeight = m_param->maxCUSize;

    {
        ctuWidth  >>= m_hChromaShift;
        ctuHeight >>= m_vChromaShift;
    }

    int addr = idxY * m_numCuInWidth + idxX;
    pixel* recCb = reconPic->getCbAddr(addr);
    pixel* recCr = reconPic->getCrAddr(addr);

    if (idxX == 0)
    {
        for (int i = 0; i < ctuHeight + 1; i++)
        {
            m_tmpL1[1][i] = recCb[0];
            m_tmpL1[2][i] = recCr[0];
            recCb += stride;
            recCr += stride;
        }
    }

    bool mergeLeftFlagCb = (ctuParam[1][addr].mergeMode == SAO_MERGE_LEFT);
    int typeIdxCb = ctuParam[1][addr].typeIdx;

    bool mergeLeftFlagCr = (ctuParam[2][addr].mergeMode == SAO_MERGE_LEFT);
    int typeIdxCr = ctuParam[2][addr].typeIdx;

    if (idxX != (m_numCuInWidth - 1))
    {
        recCb = reconPic->getCbAddr(addr);
        recCr = reconPic->getCrAddr(addr);
        for (int i = 0; i < ctuHeight + 1; i++)
        {
            m_tmpL2[1][i] = recCb[ctuWidth - 1];
            m_tmpL2[2][i] = recCr[ctuWidth - 1];
            recCb += stride;
            recCr += stride;
        }
    }

    // Process U
    if (typeIdxCb >= 0)
    {
        if (!mergeLeftFlagCb)
        {
            if (typeIdxCb == SAO_BO)
            {
                memset(m_offsetBo[1], 0, sizeof(m_offsetBo[0]));

                for (int i = 0; i < SAO_NUM_OFFSET; i++)
                    m_offsetBo[1][((ctuParam[1][addr].bandPos + i) & (MAX_NUM_SAO_CLASS - 1))] = (int8_t)(ctuParam[1][addr].offset[i] << SAO_BIT_INC);
            }
            else // if (typeIdx == SAO_EO_0 || typeIdx == SAO_EO_1 || typeIdx == SAO_EO_2 || typeIdx == SAO_EO_3)
            {
                int offset[NUM_EDGETYPE];
                offset[0] = 0;
                for (int i = 0; i < SAO_NUM_OFFSET; i++)
                    offset[i + 1] = ctuParam[1][addr].offset[i] << SAO_BIT_INC;

                for (int edgeType = 0; edgeType < NUM_EDGETYPE; edgeType++)
                    m_offsetEo[1][edgeType] = (int8_t)offset[s_eoTable[edgeType]];
            }
        }
        applyPixelOffsets(addr, typeIdxCb, 1);
    }

    // Process V
    if (typeIdxCr >= 0)
    {
        if (!mergeLeftFlagCr)
        {
            if (typeIdxCr == SAO_BO)
            {
                memset(m_offsetBo[2], 0, sizeof(m_offsetBo[0]));

                for (int i = 0; i < SAO_NUM_OFFSET; i++)
                    m_offsetBo[2][((ctuParam[2][addr].bandPos + i) & (MAX_NUM_SAO_CLASS - 1))] = (int8_t)(ctuParam[2][addr].offset[i] << SAO_BIT_INC);
            }
            else // if (typeIdx == SAO_EO_0 || typeIdx == SAO_EO_1 || typeIdx == SAO_EO_2 || typeIdx == SAO_EO_3)
            {
                int offset[NUM_EDGETYPE];
                offset[0] = 0;
                for (int i = 0; i < SAO_NUM_OFFSET; i++)
                    offset[i + 1] = ctuParam[2][addr].offset[i] << SAO_BIT_INC;

                for (int edgeType = 0; edgeType < NUM_EDGETYPE; edgeType++)
                    m_offsetEo[2][edgeType] = (int8_t)offset[s_eoTable[edgeType]];
            }
        }
        applyPixelOffsets(addr, typeIdxCb, 2);
    }

    std::swap(m_tmpL1[1], m_tmpL2[1]);
    std::swap(m_tmpL1[2], m_tmpL2[2]);
}

/* Calculate SAO statistics for current CTU without non-crossing slice */
void SAO::calcSaoStatsCTU(int addr, int plane)
{
    Slice* slice = m_frame->m_encData->m_slice;
    const PicYuv* reconPic = m_frame->m_reconPic;
    const CUData* cu = m_frame->m_encData->getPicCTU(addr);
    const pixel* fenc0 = m_frame->m_fencPic->getPlaneAddr(plane, addr);
    const pixel* rec0  = reconPic->getPlaneAddr(plane, addr);
    const pixel* fenc;
    const pixel* rec;
    intptr_t stride = plane ? reconPic->m_strideC : reconPic->m_stride;
    uint32_t picWidth  = m_param->sourceWidth;
    uint32_t picHeight = m_param->sourceHeight;
    int ctuWidth  = m_param->maxCUSize;
    int ctuHeight = m_param->maxCUSize;
    uint32_t lpelx = cu->m_cuPelX;
    uint32_t tpely = cu->m_cuPelY;
    const uint32_t firstRowInSlice = cu->m_bFirstRowInSlice;
    const uint32_t lastRowInSlice = cu->m_bLastRowInSlice;
    const uint32_t bAboveUnavail = (!tpely) | firstRowInSlice;

    if (plane)
    {
        picWidth  >>= m_hChromaShift;
        picHeight >>= m_vChromaShift;
        ctuWidth  >>= m_hChromaShift;
        ctuHeight >>= m_vChromaShift;
        lpelx     >>= m_hChromaShift;
        tpely     >>= m_vChromaShift;
    }
    uint32_t rpelx = x265_min(lpelx + ctuWidth,  picWidth);
    uint32_t bpely = x265_min(tpely + ctuHeight, picHeight);
    ctuWidth  = rpelx - lpelx;
    ctuHeight = bpely - tpely;

    // NOTE: Careful! the picHeight apply for Equal operator only in below, so I may safe to hack it
    if (lastRowInSlice)
    {
        picHeight = bpely;
    }

    int startX;
    int startY;
    int endX;
    int endY;

    const int plane_offset = plane ? 2 : 0;
    int skipB = 4;
    int skipR = 5;

    int8_t _upBuff[2 * (MAX_CU_SIZE + 16 + 16)], *upBuff1 = _upBuff + 16, *upBufft = upBuff1 + (MAX_CU_SIZE + 16 + 16);

    ALIGN_VAR_32(int16_t, diff[MAX_CU_SIZE * MAX_CU_SIZE]);

    // Calculate (fenc - frec) and put into diff[]
    if ((lpelx + ctuWidth <  picWidth) & (tpely + ctuHeight < picHeight))
    {
        // WARNING: *) May read beyond bound on video than ctuWidth or ctuHeight is NOT multiple of cuSize
        X265_CHECK((ctuWidth == ctuHeight) || (m_chromaFormat != X265_CSP_I420), "video size check failure\n");
        if (plane)
            primitives.chroma[m_chromaFormat].cu[m_param->maxLog2CUSize - 2].sub_ps(diff, MAX_CU_SIZE, fenc0, rec0, stride, stride);
        else
           primitives.cu[m_param->maxLog2CUSize - 2].sub_ps(diff, MAX_CU_SIZE, fenc0, rec0, stride, stride);
    }
    else
    {
        // path for non-square area (most in edge)
        for(int y = 0; y < ctuHeight; y++)
        {
            for(int x = 0; x < ctuWidth; x++)
            {
                diff[y * MAX_CU_SIZE + x] = (fenc0[y * stride + x] - rec0[y * stride + x]);
            }
        }
    }

    // SAO_BO:
    {
        if (m_param->bSaoNonDeblocked)
        {
            skipB = 3;
            skipR = 4;
        }

        endX = (rpelx == picWidth) ? ctuWidth : ctuWidth - skipR + plane_offset;
        endY = (bpely == picHeight) ? ctuHeight : ctuHeight - skipB + plane_offset;

        primitives.saoCuStatsBO(diff, rec0, stride, endX, endY, m_offsetOrg[plane][SAO_BO], m_count[plane][SAO_BO]);
    }

    {
        // SAO_EO_0: // dir: -
        {
            if (m_param->bSaoNonDeblocked)
            {
                skipB = 3;
                skipR = 5;
            }

            startX = !lpelx;
            endX   = (rpelx == picWidth) ? ctuWidth - 1 : ctuWidth - skipR + plane_offset;

            primitives.saoCuStatsE0(diff + startX, rec0 + startX, stride, endX - startX, ctuHeight - skipB + plane_offset, m_offsetOrg[plane][SAO_EO_0], m_count[plane][SAO_EO_0]);
        }

        // SAO_EO_1: // dir: |
        {
            if (m_param->bSaoNonDeblocked)
            {
                skipB = 4;
                skipR = 4;
            }

            rec  = rec0;

            startY = bAboveUnavail;
            endX   = (rpelx == picWidth) ? ctuWidth : ctuWidth - skipR + plane_offset;
            endY   = (bpely == picHeight) ? ctuHeight - 1 : ctuHeight - skipB + plane_offset;
            if (startY)
            {
                rec += stride;
            }

            primitives.sign(upBuff1, rec, &rec[- stride], ctuWidth);

            primitives.saoCuStatsE1(diff + startY * MAX_CU_SIZE, rec0 + startY * stride, stride, upBuff1, endX, endY - startY, m_offsetOrg[plane][SAO_EO_1], m_count[plane][SAO_EO_1]);
        }
        if (!m_param->bLimitSAO || ((slice->m_sliceType == P_SLICE && !cu->isSkipped(0)) ||
            (slice->m_sliceType != B_SLICE)))
        {
            // SAO_EO_2: // dir: 135
            {
                if (m_param->bSaoNonDeblocked)
                {
                    skipB = 4;
                    skipR = 5;
                }

                fenc = fenc0;
                rec  = rec0;

                startX = !lpelx;
                endX   = (rpelx == picWidth) ? ctuWidth - 1 : ctuWidth - skipR + plane_offset;

                startY = bAboveUnavail;
                endY   = (bpely == picHeight) ? ctuHeight - 1 : ctuHeight - skipB + plane_offset;
                if (startY)
                {
                    fenc += stride;
                    rec += stride;
                }

                primitives.sign(upBuff1, &rec[startX], &rec[startX - stride - 1], (endX - startX));

                primitives.saoCuStatsE2(diff + startX + startY * MAX_CU_SIZE, rec0  + startX + startY * stride, stride, upBuff1, upBufft, endX - startX, endY - startY, m_offsetOrg[plane][SAO_EO_2], m_count[plane][SAO_EO_2]);
            }
            // SAO_EO_3: // dir: 45
            {
                if (m_param->bSaoNonDeblocked)
                {
                    skipB = 4;
                    skipR = 5;
                }
                fenc = fenc0;
                rec  = rec0;
                startX = !lpelx;
                endX   = (rpelx == picWidth) ? ctuWidth - 1 : ctuWidth - skipR + plane_offset;

                startY = bAboveUnavail;
                endY   = (bpely == picHeight) ? ctuHeight - 1 : ctuHeight - skipB + plane_offset;

                if (startY)
                {
                    fenc += stride;
                    rec += stride;
                }

                primitives.sign(upBuff1, &rec[startX - 1], &rec[startX - 1 - stride + 1], (endX - startX + 1));

                primitives.saoCuStatsE3(diff + startX + startY * MAX_CU_SIZE, rec0  + startX + startY * stride, stride, upBuff1 + 1, endX - startX, endY - startY, m_offsetOrg[plane][SAO_EO_3], m_count[plane][SAO_EO_3]);
            }
        }
    }
}

void SAO::calcSaoStatsCu_BeforeDblk(Frame* frame, int idxX, int idxY)
{
    int addr = idxX + m_numCuInWidth * idxY;

    int x, y;
    const CUData* cu = frame->m_encData->getPicCTU(addr);
    const PicYuv* reconPic = m_frame->m_reconPic;
    const pixel* fenc;
    const pixel* rec;
    intptr_t stride = reconPic->m_stride;
    uint32_t picWidth  = m_param->sourceWidth;
    uint32_t picHeight = m_param->sourceHeight;
    int ctuWidth  = m_param->maxCUSize;
    int ctuHeight = m_param->maxCUSize;
    uint32_t lpelx = cu->m_cuPelX;
    uint32_t tpely = cu->m_cuPelY;
    const uint32_t firstRowInSlice = cu->m_bFirstRowInSlice;
    const uint32_t lastRowInSlice = cu->m_bLastRowInSlice;
    const uint32_t bAboveAvail = (!tpely) | firstRowInSlice;

    // NOTE: Careful! the picHeight for Equal operator only, so I may safe to hack it
    if (lastRowInSlice)
    {
        picHeight = x265_min(picHeight, (tpely + ctuHeight));
    }

    uint32_t rpelx = x265_min(lpelx + ctuWidth,  picWidth);
    uint32_t bpely = x265_min(tpely + ctuHeight, picHeight);
    ctuWidth  = rpelx - lpelx;
    ctuHeight = bpely - tpely;

    int startX;
    int startY;
    int endX;
    int endY;
    int firstX, firstY;
    int32_t* stats;
    int32_t* count;

    int skipB, skipR;

    int32_t _upBuff1[MAX_CU_SIZE + 2], *upBuff1 = _upBuff1 + 1;
    int32_t _upBufft[MAX_CU_SIZE + 2], *upBufft = _upBufft + 1;

    const int boShift = X265_DEPTH - SAO_BO_BITS;

    memset(m_countPreDblk[addr], 0, sizeof(PerPlane));
    memset(m_offsetOrgPreDblk[addr], 0, sizeof(PerPlane));

    int plane_offset = 0;
    for (int plane = 0; plane < (frame->m_param->internalCsp != X265_CSP_I400 && m_frame->m_fencPic->m_picCsp != X265_CSP_I400? NUM_PLANE : 1); plane++)
    {
        if (plane == 1)
        {
            stride = reconPic->m_strideC;
            picWidth  >>= m_hChromaShift;
            picHeight >>= m_vChromaShift;
            ctuWidth  >>= m_hChromaShift;
            ctuHeight >>= m_vChromaShift;
            lpelx     >>= m_hChromaShift;
            tpely     >>= m_vChromaShift;
            rpelx     >>= m_hChromaShift;
            bpely     >>= m_vChromaShift;
        }

        // SAO_BO:

        skipB = 3 - plane_offset;
        skipR = 4 - plane_offset;

        stats = m_offsetOrgPreDblk[addr][plane][SAO_BO];
        count = m_countPreDblk[addr][plane][SAO_BO];

        const pixel* fenc0 = m_frame->m_fencPic->getPlaneAddr(plane, addr);
        const pixel* rec0 = reconPic->getPlaneAddr(plane, addr);
        fenc = fenc0;
        rec  = rec0;

        startX = (rpelx == picWidth) ? ctuWidth : ctuWidth - skipR;
        startY = (bpely == picHeight) ? ctuHeight : ctuHeight - skipB;

        for (y = 0; y < ctuHeight; y++)
        {
            for (x = (y < startY ? startX : 0); x < ctuWidth; x++)
            {
                int classIdx = rec[x] >> boShift;
                stats[classIdx] += (fenc[x] - rec[x]);
                count[classIdx]++;
            }

            fenc += stride;
            rec += stride;
        }

        // SAO_EO_0: // dir: -
        {
            skipB = 3 - plane_offset;
            skipR = 5 - plane_offset;

            stats = m_offsetOrgPreDblk[addr][plane][SAO_EO_0];
            count = m_countPreDblk[addr][plane][SAO_EO_0];

            fenc = fenc0;
            rec  = rec0;

            startX = (rpelx == picWidth) ? ctuWidth - 1 : ctuWidth - skipR;
            startY = (bpely == picHeight) ? ctuHeight : ctuHeight - skipB;
            firstX = !lpelx;
            // endX   = (rpelx == picWidth) ? ctuWidth - 1 : ctuWidth;
            endX   = ctuWidth - 1;  // not refer right CTU

            for (y = 0; y < ctuHeight; y++)
            {
                x = (y < startY ? startX : firstX);
                int signLeft = signOf(rec[x] - rec[x - 1]);
                for (; x < endX; x++)
                {
                    int signRight = signOf(rec[x] - rec[x + 1]);
                    int edgeType = signRight + signLeft + 2;
                    signLeft = -signRight;

                    stats[s_eoTable[edgeType]] += (fenc[x] - rec[x]);
                    count[s_eoTable[edgeType]]++;
                }

                fenc += stride;
                rec += stride;
            }
        }

        // SAO_EO_1: // dir: |
        {
            skipB = 4 - plane_offset;
            skipR = 4 - plane_offset;

            stats = m_offsetOrgPreDblk[addr][plane][SAO_EO_1];
            count = m_countPreDblk[addr][plane][SAO_EO_1];

            fenc = fenc0;
            rec  = rec0;

            startX = (rpelx == picWidth) ? ctuWidth : ctuWidth - skipR;
            startY = (bpely == picHeight) ? ctuHeight - 1 : ctuHeight - skipB;
            firstY = bAboveAvail;
            // endY   = (bpely == picHeight) ? ctuHeight - 1 : ctuHeight;
            endY   = ctuHeight - 1; // not refer below CTU
            if (firstY)
            {
                fenc += stride;
                rec += stride;
            }

            for (x = startX; x < ctuWidth; x++)
                upBuff1[x] = signOf(rec[x] - rec[x - stride]);

            for (y = firstY; y < endY; y++)
            {
                for (x = (y < startY - 1 ? startX : 0); x < ctuWidth; x++)
                {
                    int signDown = signOf(rec[x] - rec[x + stride]);
                    int edgeType = signDown + upBuff1[x] + 2;
                    upBuff1[x] = -signDown;

                    if (x < startX && y < startY)
                        continue;

                    stats[s_eoTable[edgeType]] += (fenc[x] - rec[x]);
                    count[s_eoTable[edgeType]]++;
                }

                fenc += stride;
                rec += stride;
            }
        }

        // SAO_EO_2: // dir: 135
        {
            skipB = 4 - plane_offset;
            skipR = 5 - plane_offset;

            stats = m_offsetOrgPreDblk[addr][plane][SAO_EO_2];
            count = m_countPreDblk[addr][plane][SAO_EO_2];

            fenc = fenc0;
            rec  = rec0;

            startX = (rpelx == picWidth) ? ctuWidth - 1 : ctuWidth - skipR;
            startY = (bpely == picHeight) ? ctuHeight - 1 : ctuHeight - skipB;
            firstX = !lpelx;
            firstY = bAboveAvail;
            // endX   = (rpelx == picWidth) ? ctuWidth - 1 : ctuWidth;
            // endY   = (bpely == picHeight) ? ctuHeight - 1 : ctuHeight;
            endX   = ctuWidth - 1;  // not refer right CTU
            endY   = ctuHeight - 1; // not refer below CTU
            if (firstY)
            {
                fenc += stride;
                rec += stride;
            }

            for (x = startX; x < endX; x++)
                upBuff1[x] = signOf(rec[x] - rec[x - stride - 1]);

            for (y = firstY; y < endY; y++)
            {
                x = (y < startY - 1 ? startX : firstX);
                upBufft[x] = signOf(rec[x + stride] - rec[x - 1]);
                for (; x < endX; x++)
                {
                    int signDown = signOf(rec[x] - rec[x + stride + 1]);
                    int edgeType = signDown + upBuff1[x] + 2;
                    upBufft[x + 1] = -signDown;

                    if (x < startX && y < startY)
                        continue;

                    stats[s_eoTable[edgeType]] += (fenc[x] - rec[x]);
                    count[s_eoTable[edgeType]]++;
                }

                std::swap(upBuff1, upBufft);

                rec += stride;
                fenc += stride;
            }
        }

        // SAO_EO_3: // dir: 45
        {
            skipB = 4 - plane_offset;
            skipR = 5 - plane_offset;

            stats = m_offsetOrgPreDblk[addr][plane][SAO_EO_3];
            count = m_countPreDblk[addr][plane][SAO_EO_3];

            fenc = fenc0;
            rec  = rec0;

            startX = (rpelx == picWidth) ? ctuWidth - 1 : ctuWidth - skipR;
            startY = (bpely == picHeight) ? ctuHeight - 1 : ctuHeight - skipB;
            firstX = !lpelx;
            firstY = bAboveAvail;
            // endX   = (rpelx == picWidth) ? ctuWidth - 1 : ctuWidth;
            // endY   = (bpely == picHeight) ? ctuHeight - 1 : ctuHeight;
            endX   = ctuWidth - 1;  // not refer right CTU
            endY   = ctuHeight - 1; // not refer below CTU
            if (firstY)
            {
                fenc += stride;
                rec += stride;
            }

            for (x = startX - 1; x < endX; x++)
                upBuff1[x] = signOf(rec[x] - rec[x - stride + 1]);

            for (y = firstY; y < endY; y++)
            {
                for (x = (y < startY - 1 ? startX : firstX); x < endX; x++)
                {
                    int signDown = signOf(rec[x] - rec[x + stride - 1]);
                    int edgeType = signDown + upBuff1[x] + 2;
                    upBuff1[x - 1] = -signDown;

                    if (x < startX && y < startY)
                        continue;

                    stats[s_eoTable[edgeType]] += (fenc[x] - rec[x]);
                    count[s_eoTable[edgeType]]++;
                }

                upBuff1[endX - 1] = signOf(rec[endX - 1 + stride] - rec[endX]);

                rec += stride;
                fenc += stride;
            }
        }
        plane_offset = 2;
    }
}

/* reset offset statistics */
void SAO::resetStats()
{
    memset(m_count, 0, sizeof(m_count));
    memset(m_offset, 0, sizeof(m_offset));
    memset(m_offsetOrg, 0, sizeof(m_offsetOrg));
}

void SAO::rdoSaoUnitRowEnd(const SAOParam* saoParam, int numctus)
{
    if (!saoParam->bSaoFlag[0])
        m_depthSaoRate[0 * SAO_DEPTHRATE_SIZE + m_refDepth] = 1.0;
    else
    {
        X265_CHECK(m_numNoSao[0] <= numctus, "m_numNoSao check failure!");
        m_depthSaoRate[0 * SAO_DEPTHRATE_SIZE + m_refDepth] = m_numNoSao[0] / ((double)numctus);
    }

    if (!saoParam->bSaoFlag[1])
    {
        m_depthSaoRate[1 * SAO_DEPTHRATE_SIZE + m_refDepth] = 1.0;
    }
    else
        m_depthSaoRate[1 * SAO_DEPTHRATE_SIZE + m_refDepth] = m_numNoSao[1] / ((double)numctus);
}

void SAO::rdoSaoUnitCu(SAOParam* saoParam, int rowBaseAddr, int idxX, int addr)
{
    Slice* slice = m_frame->m_encData->m_slice;
    const CUData* cu = m_frame->m_encData->getPicCTU(addr);
    int qp = cu->m_qp[0];
    int64_t lambda[2] = { 0 };

    int qpCb = qp + slice->m_pps->chromaQpOffset[0] + slice->m_chromaQpOffset[0];
    if (m_param->internalCsp == X265_CSP_I420)
        qpCb = x265_clip3(m_param->rc.qpMin, m_param->rc.qpMax, (int)g_chromaScale[x265_clip3(QP_MIN, QP_MAX_MAX, qpCb)]);
    else
        qpCb = x265_clip3(m_param->rc.qpMin, m_param->rc.qpMax, qpCb);
    lambda[0] = (int64_t)floor(256.0 * x265_lambda2_tab[qp]);
    lambda[1] = (int64_t)floor(256.0 * x265_lambda2_tab[qpCb]); // Use Cb QP for SAO chroma

    const bool allowMerge[2] = {(idxX != 0), (rowBaseAddr != 0)}; // left, up

    const int addrMerge[2] = {(idxX ? addr - 1 : -1), (rowBaseAddr ? addr - m_numCuInWidth : -1)};// left, up

    bool chroma = m_param->internalCsp != X265_CSP_I400 && m_frame->m_fencPic->m_picCsp != X265_CSP_I400;
    int planes = chroma ? 3 : 1;

    // reset stats Y, Cb, Cr
    X265_CHECK(sizeof(PerPlane) == (sizeof(int32_t) * (NUM_PLANE * MAX_NUM_SAO_TYPE * MAX_NUM_SAO_CLASS)), "Found Padding space in struct PerPlane");

    // TODO: Confirm the address space is continuous
    if (m_param->bSaoNonDeblocked)
    {
        memcpy(m_count, m_countPreDblk[addr], sizeof(m_count));
        memcpy(m_offsetOrg, m_offsetOrgPreDblk[addr], sizeof(m_offsetOrg));
    }
    else
    {
        memset(m_count, 0, sizeof(m_count));
        memset(m_offsetOrg, 0, sizeof(m_offsetOrg));
    }

    for (int i = 0; i < planes; i++)
        saoParam->ctuParam[i][addr].reset();
    // SAO distortion calculation
    m_entropyCoder.load(m_rdContexts.cur);
    m_entropyCoder.resetBits();
    if (allowMerge[0])
        m_entropyCoder.codeSaoMerge(0);
    if (allowMerge[1])
        m_entropyCoder.codeSaoMerge(0);
    m_entropyCoder.store(m_rdContexts.temp);
    memset(m_offset, 0, sizeof(m_offset));
    int64_t bestCost = 0;
    int64_t rateDist = 0;

    bool bAboveLeftAvail = true;
    for (int mergeIdx = 0; mergeIdx < 2; ++mergeIdx)
    {
        if (!allowMerge[mergeIdx])
            continue;

        SaoCtuParam* mergeSrcParam = &(saoParam->ctuParam[0][addrMerge[mergeIdx]]);
        bAboveLeftAvail = bAboveLeftAvail && (mergeSrcParam->typeIdx == -1);
    }
    // Don't apply sao if ctu is skipped or ajacent ctus are sao off
    bool bSaoOff = (slice->m_sliceType == B_SLICE) && (cu->isSkipped(0) || bAboveLeftAvail);

    // Estimate distortion and cost of new SAO params
    if (saoParam->bSaoFlag[0])
    {
        if (!m_param->bLimitSAO || !bSaoOff)
        {
            calcSaoStatsCTU(addr, 0);
            saoStatsInitialOffset(addr, 0);
            saoLumaComponentParamDist(saoParam, addr, rateDist, lambda, bestCost);
        }
    }

    SaoCtuParam* lclCtuParam = &saoParam->ctuParam[0][addr];
    if (saoParam->bSaoFlag[1])
    {
        if (!m_param->bLimitSAO || ((lclCtuParam->typeIdx != -1) && !bSaoOff))
        {
            calcSaoStatsCTU(addr, 1);
            calcSaoStatsCTU(addr, 2);
            saoStatsInitialOffset(addr, 1);
            saoChromaComponentParamDist(saoParam, addr, rateDist, lambda, bestCost);
        }
    }
    if (saoParam->bSaoFlag[0] || saoParam->bSaoFlag[1])
    {
        // Cost of merge left or Up
        for (int mergeIdx = 0; mergeIdx < 2; ++mergeIdx)
        {
            if (!allowMerge[mergeIdx])
                continue;

            int64_t mergeDist = 0; 
            for (int plane = 0; plane < planes; plane++)
            {
                int64_t estDist = 0;
                SaoCtuParam* mergeSrcParam = &(saoParam->ctuParam[plane][addrMerge[mergeIdx]]);
                int typeIdx = mergeSrcParam->typeIdx;
                if (typeIdx >= 0)
                {
                    int bandPos = (typeIdx == SAO_BO) ? mergeSrcParam->bandPos : 1;
                    for (int classIdx = 0; classIdx < SAO_NUM_OFFSET; classIdx++)
                    {
                        int mergeOffset = mergeSrcParam->offset[classIdx];
                        estDist += estSaoDist(m_count[plane][typeIdx][classIdx + bandPos], mergeOffset, m_offsetOrg[plane][typeIdx][classIdx + bandPos]);
                    }
                }
                mergeDist += (estDist << 8) / lambda[!!plane];
            }

            m_entropyCoder.load(m_rdContexts.cur);
            m_entropyCoder.resetBits();
            if (allowMerge[0])
                m_entropyCoder.codeSaoMerge(1 - mergeIdx);
            if (allowMerge[1] && (mergeIdx == 1))
                m_entropyCoder.codeSaoMerge(1);

            uint32_t estRate = m_entropyCoder.getNumberOfWrittenBits();
            int64_t mergeCost = mergeDist + estRate;
            if (mergeCost < bestCost)
            {
                SaoMergeMode mergeMode = mergeIdx ? SAO_MERGE_UP : SAO_MERGE_LEFT;
                bestCost = mergeCost;
                m_entropyCoder.store(m_rdContexts.temp);
                for (int plane = 0; plane < planes; plane++)
                {
                    if (saoParam->bSaoFlag[plane > 0])
                    {
                        SaoCtuParam* dstCtuParam   = &saoParam->ctuParam[plane][addr];
                        SaoCtuParam* mergeSrcParam = &(saoParam->ctuParam[plane][addrMerge[mergeIdx]]);
                        dstCtuParam->mergeMode = mergeMode;
                        dstCtuParam->typeIdx   = mergeSrcParam->typeIdx;
                        dstCtuParam->bandPos   = mergeSrcParam->bandPos;

                        for (int i = 0; i < SAO_NUM_OFFSET; i++)
                            dstCtuParam->offset[i] = mergeSrcParam->offset[i];
                    }
                }
            }
        }

        if (saoParam->ctuParam[0][addr].typeIdx < 0)
            m_numNoSao[0]++;
        if (chroma && saoParam->ctuParam[1][addr].typeIdx < 0)
            m_numNoSao[1]++;
        m_entropyCoder.load(m_rdContexts.temp);
        m_entropyCoder.store(m_rdContexts.cur);
    }
}

// Rounds the division of initial offsets by the number of samples in
// each of the statistics table entries.
void SAO::saoStatsInitialOffset(int addr, int planes)
{
    Slice* slice = m_frame->m_encData->m_slice;
    const CUData* cu = m_frame->m_encData->getPicCTU(addr);

    int maxSaoType;
    if (m_param->bLimitSAO && ((slice->m_sliceType == P_SLICE && cu->isSkipped(0)) ||
       (slice->m_sliceType == B_SLICE)))
    {
        maxSaoType = MAX_NUM_SAO_TYPE - 3;
    }
    else
    {
        maxSaoType = MAX_NUM_SAO_TYPE - 1;
    }
    // EO
    for (int plane = planes; plane <= planes * 2; plane++)
    {
        for (int typeIdx = 0; typeIdx < maxSaoType; typeIdx++)
        {
            for (int classIdx = 1; classIdx < SAO_NUM_OFFSET + 1; classIdx++)
            {
                int32_t&  count     = m_count[plane][typeIdx][classIdx];
                int32_t& offsetOrg = m_offsetOrg[plane][typeIdx][classIdx];
                int32_t& offsetOut = m_offset[plane][typeIdx][classIdx];

                if (count)
                {
                    offsetOut = roundIBDI(offsetOrg, count << SAO_BIT_INC);
                    offsetOut = x265_clip3(-OFFSET_THRESH + 1, OFFSET_THRESH - 1, offsetOut);

                    if (classIdx < 3) 
                        offsetOut = X265_MAX(offsetOut, 0);
                    else
                        offsetOut = X265_MIN(offsetOut, 0);
                }
            }
        }
    }
    // BO
    for (int plane = planes; plane <= planes * 2; plane++)
    {
        for (int classIdx = 0; classIdx < MAX_NUM_SAO_CLASS; classIdx++)
        {
            int32_t&  count     = m_count[plane][SAO_BO][classIdx];
            int32_t& offsetOrg = m_offsetOrg[plane][SAO_BO][classIdx];
            int32_t& offsetOut = m_offset[plane][SAO_BO][classIdx];

            if (count)
            {
                offsetOut = roundIBDI(offsetOrg, count << SAO_BIT_INC);
                offsetOut = x265_clip3(-OFFSET_THRESH + 1, OFFSET_THRESH - 1, offsetOut);
            }
        }
    }
}

inline int64_t SAO::calcSaoRdoCost(int64_t distortion, uint32_t bits, int64_t lambda)
{
#if X265_DEPTH < 10
        X265_CHECK(bits <= (INT64_MAX - 128) / lambda,
                   "calcRdCost wrap detected dist: " X265_LL ", bits %u, lambda: " X265_LL "\n",
                   distortion, bits, lambda);
#else
        X265_CHECK(bits <= (INT64_MAX - 128) / lambda,
                   "calcRdCost wrap detected dist: " X265_LL ", bits %u, lambda: " X265_LL "\n",
                   distortion, bits, lambda);
#endif
        return distortion + ((bits * lambda + 128) >> 8);
}

void SAO::estIterOffset(int typeIdx, int64_t lambda, int32_t count, int32_t offsetOrg, int32_t& offset, int32_t& distClasses, int64_t& costClasses)
{
    int bestOffset = 0;
    distClasses    = 0;

    // Assuming sending quantized value 0 results in zero offset and sending the value zero needs 1 bit.
    // entropy coder can be used to measure the exact rate here.
    int64_t bestCost = calcSaoRdoCost(0, 1, lambda);
    while (offset != 0)
    {
        // Calculate the bits required for signalling the offset
        uint32_t rate = (typeIdx == SAO_BO) ? (abs(offset) + 2) : (abs(offset) + 1);
        if (abs(offset) == OFFSET_THRESH - 1)
            rate--;

        // Do the dequntization before distorion calculation
        int64_t dist = estSaoDist(count, offset << SAO_BIT_INC, offsetOrg);
        int64_t cost  = calcSaoRdoCost(dist, rate, lambda);
        if (cost < bestCost)
        {
            bestCost = cost;
            bestOffset = offset;
            distClasses = (int)dist;
        }
        offset = (offset > 0) ? (offset - 1) : (offset + 1);
    }

    costClasses = bestCost;
    offset = bestOffset;
}
void SAO::saoLumaComponentParamDist(SAOParam* saoParam, int32_t addr, int64_t& rateDist, int64_t* lambda, int64_t &bestCost)
{
    Slice* slice = m_frame->m_encData->m_slice;
    const CUData* cu = m_frame->m_encData->getPicCTU(addr);
    int64_t bestDist = 0;
    int bestTypeIdx = -1;
    SaoCtuParam* lclCtuParam = &saoParam->ctuParam[0][addr];

    int32_t distClasses[MAX_NUM_SAO_CLASS];
    int64_t costClasses[MAX_NUM_SAO_CLASS];

    // RDO SAO_NA
    m_entropyCoder.load(m_rdContexts.temp);
    m_entropyCoder.resetBits();
    m_entropyCoder.codeSaoType(0);
    int64_t costPartBest = calcSaoRdoCost(0, m_entropyCoder.getNumberOfWrittenBits(), lambda[0]);
    int maxSaoType;
    if (m_param->bLimitSAO && ((slice->m_sliceType == P_SLICE && cu->isSkipped(0)) ||
        (slice->m_sliceType == B_SLICE)))
    {
        maxSaoType = MAX_NUM_SAO_TYPE - 3;
    }
    else
    {
        maxSaoType = MAX_NUM_SAO_TYPE - 1;
    }

    //EO distortion calculation
    for (int typeIdx = 0; typeIdx < maxSaoType; typeIdx++)
    {
        int64_t estDist = 0;
        for (int classIdx = 1; classIdx < SAO_NUM_OFFSET + 1; classIdx++)
        {
            int32_t&  count    = m_count[0][typeIdx][classIdx];
            int32_t& offsetOrg = m_offsetOrg[0][typeIdx][classIdx];
            int32_t& offsetOut = m_offset[0][typeIdx][classIdx];
            estIterOffset(typeIdx, lambda[0], count, offsetOrg, offsetOut, distClasses[classIdx], costClasses[classIdx]);

            //Calculate distortion
            estDist += distClasses[classIdx];
        }

        m_entropyCoder.load(m_rdContexts.temp);
        m_entropyCoder.resetBits();
        m_entropyCoder.codeSaoOffsetEO(m_offset[0][typeIdx] + 1, typeIdx, 0);

        int64_t cost = calcSaoRdoCost(estDist, m_entropyCoder.getNumberOfWrittenBits(), lambda[0]);

        if (cost < costPartBest)
        {
            costPartBest = cost;
            bestDist = estDist;
            bestTypeIdx = typeIdx;
        }
    }

    if (bestTypeIdx != -1)
    {
        lclCtuParam->mergeMode = SAO_MERGE_NONE;
        lclCtuParam->typeIdx = bestTypeIdx;
        lclCtuParam->bandPos = 0;
        for (int classIdx = 0; classIdx < SAO_NUM_OFFSET; classIdx++)
            lclCtuParam->offset[classIdx] = m_offset[0][bestTypeIdx][classIdx + 1];
    }

    //BO RDO
    int64_t estDist = 0;
    for (int classIdx = 0; classIdx < MAX_NUM_SAO_CLASS; classIdx++)
    {
        int32_t&  count    = m_count[0][SAO_BO][classIdx];
        int32_t& offsetOrg = m_offsetOrg[0][SAO_BO][classIdx];
        int32_t& offsetOut = m_offset[0][SAO_BO][classIdx];

        estIterOffset(SAO_BO, lambda[0], count, offsetOrg, offsetOut, distClasses[classIdx], costClasses[classIdx]);
    }

    // Estimate Best Position
    int32_t bestClassBO  = 0;
    int64_t currentRDCost = costClasses[0];
    currentRDCost += costClasses[1];
    currentRDCost += costClasses[2];
    currentRDCost += costClasses[3];
    int64_t bestRDCostBO = currentRDCost;

    for (int i = 1; i < MAX_NUM_SAO_CLASS - SAO_NUM_OFFSET + 1; i++)
    {
        currentRDCost -= costClasses[i - 1];
        currentRDCost += costClasses[i + 3];

        if (currentRDCost < bestRDCostBO)
        {
            bestRDCostBO = currentRDCost;
            bestClassBO  = i;
        }
    }

    estDist = 0;
    for (int classIdx = bestClassBO; classIdx < bestClassBO + SAO_NUM_OFFSET; classIdx++)
        estDist += distClasses[classIdx];

    m_entropyCoder.load(m_rdContexts.temp);
    m_entropyCoder.resetBits();
    m_entropyCoder.codeSaoOffsetBO(m_offset[0][SAO_BO] + bestClassBO, bestClassBO, 0);

    int64_t cost = calcSaoRdoCost(estDist, m_entropyCoder.getNumberOfWrittenBits(), lambda[0]);

    if (cost < costPartBest)
    {
        costPartBest = cost;
        bestDist = estDist;

        lclCtuParam->mergeMode = SAO_MERGE_NONE;
        lclCtuParam->typeIdx = SAO_BO;
        lclCtuParam->bandPos = bestClassBO;
        for (int classIdx = 0; classIdx < SAO_NUM_OFFSET; classIdx++)
            lclCtuParam->offset[classIdx] = m_offset[0][SAO_BO][classIdx + bestClassBO];
    }

    rateDist = (bestDist << 8) / lambda[0];
    m_entropyCoder.load(m_rdContexts.temp);
    m_entropyCoder.codeSaoOffset(*lclCtuParam, 0);
    m_entropyCoder.store(m_rdContexts.temp);

    if (m_param->internalCsp == X265_CSP_I400)
    {
        bestCost = rateDist + m_entropyCoder.getNumberOfWrittenBits();
    }
}
void SAO::saoChromaComponentParamDist(SAOParam* saoParam, int32_t addr, int64_t& rateDist, int64_t* lambda, int64_t &bestCost)
{
    Slice* slice = m_frame->m_encData->m_slice;
    const CUData* cu = m_frame->m_encData->getPicCTU(addr);
    int64_t bestDist = 0;
    int bestTypeIdx = -1;
    SaoCtuParam* lclCtuParam[2] = { &saoParam->ctuParam[1][addr], &saoParam->ctuParam[2][addr] };

    int64_t costClasses[MAX_NUM_SAO_CLASS];
    int32_t distClasses[MAX_NUM_SAO_CLASS];
    int32_t bestClassBO[2] = { 0, 0 };

    m_entropyCoder.load(m_rdContexts.temp);
    m_entropyCoder.resetBits();
    m_entropyCoder.codeSaoType(0);

    uint32_t bits = m_entropyCoder.getNumberOfWrittenBits();
    int64_t costPartBest = calcSaoRdoCost(0, bits, lambda[1]);
    int maxSaoType;
    if (m_param->bLimitSAO && ((slice->m_sliceType == P_SLICE && cu->isSkipped(0)) ||
        (slice->m_sliceType == B_SLICE)))
    {
        maxSaoType = MAX_NUM_SAO_TYPE - 3;
    }
    else
    {
        maxSaoType = MAX_NUM_SAO_TYPE - 1;
    }

    //EO RDO
    for (int typeIdx = 0; typeIdx < maxSaoType; typeIdx++)
    {
        int64_t estDist[2] = {0, 0};
        for (int compIdx = 1; compIdx < 3; compIdx++)
        {
            for (int classIdx = 1; classIdx < SAO_NUM_OFFSET + 1; classIdx++)
            {
                int32_t& count = m_count[compIdx][typeIdx][classIdx];
                int32_t& offsetOrg = m_offsetOrg[compIdx][typeIdx][classIdx];
                int32_t& offsetOut = m_offset[compIdx][typeIdx][classIdx];

                estIterOffset(typeIdx, lambda[1], count, offsetOrg, offsetOut, distClasses[classIdx], costClasses[classIdx]);

                estDist[compIdx - 1] += distClasses[classIdx];
            }
        }

        m_entropyCoder.load(m_rdContexts.temp);
        m_entropyCoder.resetBits();

        for (int compIdx = 0; compIdx < 2; compIdx++)
            m_entropyCoder.codeSaoOffsetEO(m_offset[compIdx + 1][typeIdx] + 1, typeIdx, compIdx + 1);

        uint32_t estRate = m_entropyCoder.getNumberOfWrittenBits();
        int64_t cost = calcSaoRdoCost((estDist[0] + estDist[1]), estRate, lambda[1]);

        if (cost < costPartBest)
        {
            costPartBest = cost;
            bestDist = (estDist[0] + estDist[1]);
            bestTypeIdx = typeIdx;
        }
    }

    if (bestTypeIdx != -1)
    {
        for (int compIdx = 0; compIdx < 2; compIdx++)
        {
            lclCtuParam[compIdx]->mergeMode = SAO_MERGE_NONE;
            lclCtuParam[compIdx]->typeIdx = bestTypeIdx;
            lclCtuParam[compIdx]->bandPos = 0;
            for (int classIdx = 0; classIdx < SAO_NUM_OFFSET; classIdx++)
                lclCtuParam[compIdx]->offset[classIdx] = m_offset[compIdx + 1][bestTypeIdx][classIdx + 1];
        }
    }

    // BO RDO
    int64_t estDist[2];

    // Estimate Best Position
    for (int compIdx = 1; compIdx < 3; compIdx++)
    {
        int64_t bestRDCostBO = MAX_INT64;

        for (int classIdx = 0; classIdx < MAX_NUM_SAO_CLASS; classIdx++)
        {
            int32_t&  count = m_count[compIdx][SAO_BO][classIdx];
            int32_t& offsetOrg = m_offsetOrg[compIdx][SAO_BO][classIdx];
            int32_t& offsetOut = m_offset[compIdx][SAO_BO][classIdx];

            estIterOffset(SAO_BO, lambda[1], count, offsetOrg, offsetOut, distClasses[classIdx], costClasses[classIdx]);
        }

        for (int i = 0; i < MAX_NUM_SAO_CLASS - SAO_NUM_OFFSET + 1; i++)
        {
            int64_t currentRDCost = 0;
            for (int j = i; j < i + SAO_NUM_OFFSET; j++)
                currentRDCost += costClasses[j];

            if (currentRDCost < bestRDCostBO)
            {
                bestRDCostBO = currentRDCost;
                bestClassBO[compIdx - 1]  = i;
            }
        }

        estDist[compIdx - 1] = 0;
        for (int classIdx = bestClassBO[compIdx - 1]; classIdx < bestClassBO[compIdx - 1] + SAO_NUM_OFFSET; classIdx++)
            estDist[compIdx - 1] += distClasses[classIdx];
    }

    m_entropyCoder.load(m_rdContexts.temp);
    m_entropyCoder.resetBits();

    for (int compIdx = 0; compIdx < 2; compIdx++)
        m_entropyCoder.codeSaoOffsetBO(m_offset[compIdx + 1][SAO_BO] + bestClassBO[compIdx], bestClassBO[compIdx], compIdx + 1);

    uint32_t estRate = m_entropyCoder.getNumberOfWrittenBits();
    int64_t cost = calcSaoRdoCost((estDist[0] + estDist[1]), estRate, lambda[1]);

    if (cost < costPartBest)
    {
        costPartBest = cost;
        bestDist = (estDist[0] + estDist[1]);

        for (int compIdx = 0; compIdx < 2; compIdx++)
        {
            lclCtuParam[compIdx]->mergeMode = SAO_MERGE_NONE;
            lclCtuParam[compIdx]->typeIdx = SAO_BO;
            lclCtuParam[compIdx]->bandPos = bestClassBO[compIdx];
            for (int classIdx = 0; classIdx < SAO_NUM_OFFSET; classIdx++)
                lclCtuParam[compIdx]->offset[classIdx] = m_offset[compIdx + 1][SAO_BO][classIdx + bestClassBO[compIdx]];
        }
    }

    rateDist += (bestDist << 8) / lambda[1];
    m_entropyCoder.load(m_rdContexts.temp);

    if (saoParam->bSaoFlag[1])
    {
        m_entropyCoder.codeSaoOffset(*lclCtuParam[0], 1);
        m_entropyCoder.codeSaoOffset(*lclCtuParam[1], 2);
        m_entropyCoder.store(m_rdContexts.temp);

        uint32_t rate = m_entropyCoder.getNumberOfWrittenBits();
        bestCost = rateDist + rate;
    }
    else
    {
        uint32_t rate = m_entropyCoder.getNumberOfWrittenBits();
        bestCost = rateDist + rate;
    }
}

// NOTE: must put in namespace X265_NS since we need class SAO
void saoCuStatsBO_c(const int16_t *diff, const pixel *rec, intptr_t stride, int endX, int endY, int32_t *stats, int32_t *count)
{
    const int boShift = X265_DEPTH - SAO_BO_BITS;

    for (int y = 0; y < endY; y++)
    {
        for (int x = 0; x < endX; x++)
        {
            int classIdx = rec[x] >> boShift;
            stats[classIdx] += diff[x];
            count[classIdx]++;
        }

        diff += MAX_CU_SIZE;
        rec += stride;
    }
}

void saoCuStatsE0_c(const int16_t *diff, const pixel *rec, intptr_t stride, int endX, int endY, int32_t *stats, int32_t *count)
{
    int32_t tmp_stats[SAO::NUM_EDGETYPE];
    int32_t tmp_count[SAO::NUM_EDGETYPE];

    X265_CHECK(endX <= MAX_CU_SIZE, "endX too big\n");

    memset(tmp_stats, 0, sizeof(tmp_stats));
    memset(tmp_count, 0, sizeof(tmp_count));

    for (int y = 0; y < endY; y++)
    {
        int signLeft = signOf(rec[0] - rec[-1]);
        for (int x = 0; x < endX; x++)
        {
            int signRight = signOf2(rec[x], rec[x + 1]);
            X265_CHECK(signRight == signOf(rec[x] - rec[x + 1]), "signDown check failure\n");
            uint32_t edgeType = signRight + signLeft + 2;
            signLeft = -signRight;

            X265_CHECK(edgeType <= 4, "edgeType check failure\n");
            tmp_stats[edgeType] += diff[x];
            tmp_count[edgeType]++;
        }

        diff += MAX_CU_SIZE;
        rec += stride;
    }

    for (int x = 0; x < SAO::NUM_EDGETYPE; x++)
    {
        stats[SAO::s_eoTable[x]] += tmp_stats[x];
        count[SAO::s_eoTable[x]] += tmp_count[x];
    }
}

void saoCuStatsE1_c(const int16_t *diff, const pixel *rec, intptr_t stride, int8_t *upBuff1, int endX, int endY, int32_t *stats, int32_t *count)
{
    X265_CHECK(endX <= MAX_CU_SIZE, "endX check failure\n");
    X265_CHECK(endY <= MAX_CU_SIZE, "endY check failure\n");

    int32_t tmp_stats[SAO::NUM_EDGETYPE];
    int32_t tmp_count[SAO::NUM_EDGETYPE];

    memset(tmp_stats, 0, sizeof(tmp_stats));
    memset(tmp_count, 0, sizeof(tmp_count));

    X265_CHECK(endX * endY <= (4096 - 16), "Assembly of saoE1 may overflow with this block size\n");
    for (int y = 0; y < endY; y++)
    {
        for (int x = 0; x < endX; x++)
        {
            int signDown = signOf2(rec[x], rec[x + stride]);
            X265_CHECK(signDown == signOf(rec[x] - rec[x + stride]), "signDown check failure\n");
            uint32_t edgeType = signDown + upBuff1[x] + 2;
            upBuff1[x] = (int8_t)(-signDown);

            X265_CHECK(edgeType <= 4, "edgeType check failure\n");
            tmp_stats[edgeType] += diff[x];
            tmp_count[edgeType]++;
        }
        diff += MAX_CU_SIZE;
        rec += stride;
    }

    for (int x = 0; x < SAO::NUM_EDGETYPE; x++)
    {
        stats[SAO::s_eoTable[x]] += tmp_stats[x];
        count[SAO::s_eoTable[x]] += tmp_count[x];
    }
}

void saoCuStatsE2_c(const int16_t *diff, const pixel *rec, intptr_t stride, int8_t *upBuff1, int8_t *upBufft, int endX, int endY, int32_t *stats, int32_t *count)
{
    X265_CHECK(endX < MAX_CU_SIZE, "endX check failure\n");
    X265_CHECK(endY < MAX_CU_SIZE, "endY check failure\n");

    int32_t tmp_stats[SAO::NUM_EDGETYPE];
    int32_t tmp_count[SAO::NUM_EDGETYPE];

    memset(tmp_stats, 0, sizeof(tmp_stats));
    memset(tmp_count, 0, sizeof(tmp_count));

    for (int y = 0; y < endY; y++)
    {
        upBufft[0] = signOf(rec[stride] - rec[-1]);
        for (int x = 0; x < endX; x++)
        {
            int signDown = signOf2(rec[x], rec[x + stride + 1]);
            X265_CHECK(signDown == signOf(rec[x] - rec[x + stride + 1]), "signDown check failure\n");
            uint32_t edgeType = signDown + upBuff1[x] + 2;
            upBufft[x + 1] = (int8_t)(-signDown);
            tmp_stats[edgeType] += diff[x];
            tmp_count[edgeType]++;
        }

        std::swap(upBuff1, upBufft);

        rec += stride;
        diff += MAX_CU_SIZE;
    }

    for (int x = 0; x < SAO::NUM_EDGETYPE; x++)
    {
        stats[SAO::s_eoTable[x]] += tmp_stats[x];
        count[SAO::s_eoTable[x]] += tmp_count[x];
    }
}

void saoCuStatsE3_c(const int16_t *diff, const pixel *rec, intptr_t stride, int8_t *upBuff1, int endX, int endY, int32_t *stats, int32_t *count)
{
    X265_CHECK(endX < MAX_CU_SIZE, "endX check failure\n");
    X265_CHECK(endY < MAX_CU_SIZE, "endY check failure\n");

    int32_t tmp_stats[SAO::NUM_EDGETYPE];
    int32_t tmp_count[SAO::NUM_EDGETYPE];

    memset(tmp_stats, 0, sizeof(tmp_stats));
    memset(tmp_count, 0, sizeof(tmp_count));

    for (int y = 0; y < endY; y++)
    {
        for (int x = 0; x < endX; x++)
        {
            int signDown = signOf2(rec[x], rec[x + stride - 1]);
            X265_CHECK(signDown == signOf(rec[x] - rec[x + stride - 1]), "signDown check failure\n");
            X265_CHECK(abs(upBuff1[x]) <= 1, "upBuffer1 check failure\n");

            uint32_t edgeType = signDown + upBuff1[x] + 2;
            upBuff1[x - 1] = (int8_t)(-signDown);
            tmp_stats[edgeType] += diff[x];
            tmp_count[edgeType]++;
        }

        upBuff1[endX - 1] = signOf(rec[endX - 1 + stride] - rec[endX]);

        rec += stride;
        diff += MAX_CU_SIZE;
    }

    for (int x = 0; x < SAO::NUM_EDGETYPE; x++)
    {
        stats[SAO::s_eoTable[x]] += tmp_stats[x];
        count[SAO::s_eoTable[x]] += tmp_count[x];
    }
}

void setupSaoPrimitives_c(EncoderPrimitives &p)
{
    // TODO: move other sao functions to here
    p.saoCuStatsBO = saoCuStatsBO_c;
    p.saoCuStatsE0 = saoCuStatsE0_c;
    p.saoCuStatsE1 = saoCuStatsE1_c;
    p.saoCuStatsE2 = saoCuStatsE2_c;
    p.saoCuStatsE3 = saoCuStatsE3_c;
}
}

