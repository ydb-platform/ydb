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

#ifndef X265_SAO_H
#define X265_SAO_H

#include "common.h"
#include "frame.h"
#include "entropy.h"

namespace X265_NS {
// private namespace

enum SAOType
{
    SAO_EO_0 = 0,
    SAO_EO_1,
    SAO_EO_2,
    SAO_EO_3,
    SAO_BO,
    MAX_NUM_SAO_TYPE
};

class SAO
{
public:

    enum { SAO_MAX_DEPTH = 4 };
    enum { SAO_BO_BITS  = 5 };
    enum { MAX_NUM_SAO_CLASS = 32 };
    enum { SAO_BIT_INC = 0 }; /* in HM12.0, it wrote as X265_MAX(X265_DEPTH - 10, 0) */
    enum { OFFSET_THRESH = 1 << X265_MIN(X265_DEPTH - 5, 5) };
    enum { NUM_EDGETYPE = 5 };
    enum { NUM_PLANE = 3 };
    enum { SAO_DEPTHRATE_SIZE = 4 };

    static const uint32_t s_eoTable[NUM_EDGETYPE];

    typedef int32_t (PerClass[MAX_NUM_SAO_TYPE][MAX_NUM_SAO_CLASS]);
    typedef int32_t (PerPlane[NUM_PLANE][MAX_NUM_SAO_TYPE][MAX_NUM_SAO_CLASS]);

protected:

    /* allocated per part */
    PerPlane    m_count;
    PerPlane    m_offset;
    PerPlane    m_offsetOrg;

    /* allocated per CTU */
    PerPlane*   m_countPreDblk;
    PerPlane*   m_offsetOrgPreDblk;

    double*     m_depthSaoRate;
    int8_t      m_offsetBo[NUM_PLANE][MAX_NUM_SAO_CLASS];
    int8_t      m_offsetEo[NUM_PLANE][NUM_EDGETYPE];

    int         m_chromaFormat;
    int         m_numCuInWidth;
    int         m_numCuInHeight;
    int         m_hChromaShift;
    int         m_vChromaShift;

    pixel*      m_clipTable;
    pixel*      m_clipTableBase;

    pixel*      m_tmpU[3];
    pixel*      m_tmpL1[3];
    pixel*      m_tmpL2[3];

public:

    struct SAOContexts
    {
        Entropy cur;
        Entropy next;
        Entropy temp;
    };

    Frame*      m_frame;
    Entropy     m_entropyCoder;
    SAOContexts m_rdContexts;

    x265_param* m_param;
    int         m_refDepth;
    int         m_numNoSao[2];

    SAO();

    bool create(x265_param* param, int initCommon);
    void createFromRootNode(SAO *root);
    void destroy(int destoryCommon);

    void allocSaoParam(SAOParam* saoParam) const;

    void startSlice(Frame* pic, Entropy& initState);
    void resetStats();

    // CTU-based SAO process without slice granularity
    void applyPixelOffsets(int addr, int typeIdx, int plane);
    void processSaoUnitRow(SaoCtuParam* ctuParam, int idxY, int plane);
    void generateLumaOffsets(SaoCtuParam* ctuParam, int idxY, int idxX);
    void generateChromaOffsets(SaoCtuParam* ctuParam[3], int idxY, int idxX);

    void calcSaoStatsCTU(int addr, int plane);
    void calcSaoStatsCu_BeforeDblk(Frame* pic, int idxX, int idxY);

    void saoLumaComponentParamDist(SAOParam* saoParam, int addr, int64_t& rateDist, int64_t* lambda, int64_t& bestCost);
    void saoChromaComponentParamDist(SAOParam* saoParam, int addr, int64_t& rateDist, int64_t* lambda, int64_t& bestCost);

    void estIterOffset(int typeIdx, int64_t lambda, int32_t count, int32_t offsetOrg, int32_t& offset, int32_t& distClasses, int64_t& costClasses);
    void rdoSaoUnitRowEnd(const SAOParam* saoParam, int numctus);
    void rdoSaoUnitCu(SAOParam* saoParam, int rowBaseAddr, int idxX, int addr);
    int64_t calcSaoRdoCost(int64_t distortion, uint32_t bits, int64_t lambda);
    void saoStatsInitialOffset(int addr, int planes);
    friend class FrameFilter;
};

}

#endif // ifndef X265_SAO_H
