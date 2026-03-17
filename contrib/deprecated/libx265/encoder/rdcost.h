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

#ifndef X265_RDCOST_H
#define X265_RDCOST_H

#include "common.h"
#include "slice.h"

namespace X265_NS {
// private namespace

class RDCost
{
public:

    /* all weights and factors stored as FIX8 */
    uint64_t  m_lambda2;
    uint64_t  m_lambda;
    uint32_t  m_chromaDistWeight[2];
    uint32_t  m_psyRdBase;
    uint32_t  m_psyRd;
    uint32_t  m_ssimRd;
    int       m_qp; /* QP used to configure lambda, may be higher than QP_MAX_SPEC but <= QP_MAX_MAX */

    void setPsyRdScale(double scale)                { m_psyRdBase = (uint32_t)floor(65536.0 * scale * 0.33); }
    void setSsimRd(int ssimRd) { m_ssimRd = ssimRd; };

    void setQP(const Slice& slice, int qp)
    {
        x265_emms(); /* TODO: if the lambda tables were ints, this would not be necessary */
        m_qp = qp;
        setLambda(x265_lambda2_tab[qp], x265_lambda_tab[qp]);

        /* Scale PSY RD factor by a slice type factor */
        static const uint32_t psyScaleFix8[3] = { 300, 256, 96 }; /* B, P, I */
        m_psyRd = (m_psyRdBase * psyScaleFix8[slice.m_sliceType]) >> 8;

        /* Scale PSY RD factor by QP, at high QP psy-rd can cause artifacts */
        if (qp >= 40)
        {
            int scale = qp >= QP_MAX_SPEC ? 0 : (QP_MAX_SPEC - qp) * 23;
            m_psyRd = (m_psyRd * scale) >> 8;
        }

        int qpCb, qpCr;
        if (slice.m_sps->chromaFormatIdc == X265_CSP_I420)
        {
            qpCb = (int)g_chromaScale[x265_clip3(QP_MIN, QP_MAX_MAX, qp + slice.m_pps->chromaQpOffset[0] + slice.m_chromaQpOffset[0])];
            qpCr = (int)g_chromaScale[x265_clip3(QP_MIN, QP_MAX_MAX, qp + slice.m_pps->chromaQpOffset[1] + slice.m_chromaQpOffset[1])];
        }
        else
        {
            qpCb = x265_clip3(QP_MIN, QP_MAX_SPEC, qp + slice.m_pps->chromaQpOffset[0] + slice.m_chromaQpOffset[0]);
            qpCr = x265_clip3(QP_MIN, QP_MAX_SPEC, qp + slice.m_pps->chromaQpOffset[1] + slice.m_chromaQpOffset[1]);
        }

        if (slice.m_sps->chromaFormatIdc == X265_CSP_I444)
        {
            int chroma_offset_idx = X265_MIN(qp - qpCb + 12, MAX_CHROMA_LAMBDA_OFFSET);
            uint16_t lambdaOffset = m_psyRd ? x265_chroma_lambda2_offset_tab[chroma_offset_idx] : 256;
            m_chromaDistWeight[0] = lambdaOffset;

            chroma_offset_idx = X265_MIN(qp - qpCr + 12, MAX_CHROMA_LAMBDA_OFFSET);
            lambdaOffset = m_psyRd ? x265_chroma_lambda2_offset_tab[chroma_offset_idx] : 256;
            m_chromaDistWeight[1] = lambdaOffset;
        }
        else
            m_chromaDistWeight[0] = m_chromaDistWeight[1] = 256;
    }

    void setLambda(double lambda2, double lambda)
    {
        m_lambda2 = (uint64_t)floor(256.0 * lambda2);
        m_lambda = (uint64_t)floor(256.0 * lambda);
    }

    inline uint64_t calcRdCost(sse_t distortion, uint32_t bits) const
    {
#if X265_DEPTH < 10
        X265_CHECK(bits <= (UINT64_MAX - 128) / m_lambda2,
                   "calcRdCost wrap detected dist: %u, bits %u, lambda: " X265_LL "\n",
                   distortion, bits, m_lambda2);
#else
        X265_CHECK(bits <= (UINT64_MAX - 128) / m_lambda2,
                   "calcRdCost wrap detected dist: " X265_LL ", bits %u, lambda: " X265_LL "\n",
                   distortion, bits, m_lambda2);
#endif
        return distortion + ((bits * m_lambda2 + 128) >> 8);
    }

    /* return the difference in energy between the source block and the recon block */
    inline int psyCost(int size, const pixel* source, intptr_t sstride, const pixel* recon, intptr_t rstride) const
    {
        return primitives.cu[size].psy_cost_pp(source, sstride, recon, rstride);
    }

    /* return the RD cost of this prediction, including the effect of psy-rd */
    inline uint64_t calcPsyRdCost(sse_t distortion, uint32_t bits, uint32_t psycost) const
    {
#if X265_DEPTH < 10
        X265_CHECK((bits <= (UINT64_MAX / m_lambda2)) && (psycost <= UINT64_MAX / (m_lambda * m_psyRd)),
                   "calcPsyRdCost wrap detected dist: %u, bits: %u, lambda: " X265_LL ", lambda2: " X265_LL "\n",
                   distortion, bits, m_lambda, m_lambda2);
#else
        X265_CHECK((bits <= (UINT64_MAX / m_lambda2)) && (psycost <= UINT64_MAX / (m_lambda * m_psyRd)),
                   "calcPsyRdCost wrap detected dist: " X265_LL ", bits: %u, lambda: " X265_LL ", lambda2: " X265_LL "\n",
                   distortion, bits, m_lambda, m_lambda2);
#endif
        return distortion + ((m_lambda * m_psyRd * psycost) >> 24) + ((bits * m_lambda2) >> 8);
    }

    inline uint64_t calcSsimRdCost(uint64_t distortion, uint32_t bits, uint32_t ssimCost) const
    {
#if X265_DEPTH < 10
        X265_CHECK((bits <= (UINT64_MAX / m_lambda2)) && (ssimCost <= UINT64_MAX / m_lambda),
                   "calcPsyRdCost wrap detected dist: " X265_LL " bits: %u, lambda: " X265_LL ", lambda2: " X265_LL "\n",
                   distortion, bits, m_lambda, m_lambda2);
#else
        X265_CHECK((bits <= (UINT64_MAX / m_lambda2)) && (ssimCost <= UINT64_MAX / m_lambda),
                   "calcPsyRdCost wrap detected dist: " X265_LL ", bits: %u, lambda: " X265_LL ", lambda2: " X265_LL "\n",
                   distortion, bits, m_lambda, m_lambda2);
#endif
        return distortion + ((m_lambda * ssimCost) >> 14) + ((bits * m_lambda2) >> 8);
    }

    inline uint64_t calcRdSADCost(uint32_t sadCost, uint32_t bits) const
    {
        X265_CHECK(bits <= (UINT64_MAX - 128) / m_lambda,
                   "calcRdSADCost wrap detected dist: %u, bits %u, lambda: " X265_LL "\n", sadCost, bits, m_lambda);
        return sadCost + ((bits * m_lambda + 128) >> 8);
    }

    inline sse_t scaleChromaDist(uint32_t plane, sse_t dist) const
    {
#if X265_DEPTH < 10
        X265_CHECK(dist <= (UINT64_MAX - 128) / m_chromaDistWeight[plane - 1],
                   "scaleChromaDist wrap detected dist: %u, lambda: %u\n",
                   dist, m_chromaDistWeight[plane - 1]);
#else
        X265_CHECK(dist <= (UINT64_MAX - 128) / m_chromaDistWeight[plane - 1],
                   "scaleChromaDist wrap detected dist: " X265_LL " lambda: %u\n",
                   dist, m_chromaDistWeight[plane - 1]);
#endif
        return (sse_t)((dist * (uint64_t)m_chromaDistWeight[plane - 1] + 128) >> 8);
    }

    inline uint32_t getCost(uint32_t bits) const
    {
        X265_CHECK(bits <= (UINT64_MAX - 128) / m_lambda,
                   "getCost wrap detected bits: %u, lambda: " X265_LL "\n", bits, m_lambda);
        return (uint32_t)((bits * m_lambda + 128) >> 8);
    }
};
}

#endif // ifndef X265_TCOMRDCOST_H
