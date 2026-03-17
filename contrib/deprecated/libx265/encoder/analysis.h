/*****************************************************************************
* Copyright (C) 2013-2017 MulticoreWare, Inc
*
* Authors: Deepthi Nandakumar <deepthi@multicorewareinc.com>
*          Steve Borho <steve@borho.org>
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

#ifndef X265_ANALYSIS_H
#define X265_ANALYSIS_H

#include "common.h"
#include "predict.h"
#include "quant.h"
#include "yuv.h"
#include "shortyuv.h"
#include "cudata.h"

#include "entropy.h"
#include "search.h"

namespace X265_NS {
// private namespace

class Entropy;

struct SplitData
{
    uint32_t splitRefs;
    uint32_t mvCost[2];
    uint64_t sa8dCost;

    void initSplitCUData()
    {
        splitRefs = 0;
        mvCost[0] = 0; // L0
        mvCost[1] = 0; // L1
        sa8dCost    = 0;
    }
};

class Analysis : public Search
{
public:

    enum {
        PRED_MERGE,
        PRED_SKIP,
        PRED_INTRA,
        PRED_2Nx2N,
        PRED_BIDIR,
        PRED_Nx2N,
        PRED_2NxN,
        PRED_SPLIT,
        PRED_2NxnU,
        PRED_2NxnD,
        PRED_nLx2N,
        PRED_nRx2N,
        PRED_INTRA_NxN, /* 4x4 intra PU blocks for 8x8 CU */
        PRED_LOSSLESS,  /* lossless encode of best mode */
        MAX_PRED_TYPES
    };

    struct ModeDepth
    {
        Mode           pred[MAX_PRED_TYPES];
        Mode*          bestMode;
        Yuv            fencYuv;
        CUDataMemPool  cuMemPool;
    };

    class PMODE : public BondedTaskGroup
    {
    public:

        Analysis&     master;
        const CUGeom& cuGeom;
        int           modes[MAX_PRED_TYPES];

        PMODE(Analysis& m, const CUGeom& g) : master(m), cuGeom(g) {}

        void processTasks(int workerThreadId);

    protected:

        PMODE operator=(const PMODE&);
    };

    void processPmode(PMODE& pmode, Analysis& slave);

    ModeDepth m_modeDepth[NUM_CU_DEPTH];
    bool      m_bTryLossless;
    bool      m_bChromaSa8d;
    bool      m_bHD;

    bool      m_modeFlag[2];
    bool      m_checkMergeAndSkipOnly[2];

    Analysis();

    bool create(ThreadLocalData* tld);
    void destroy();

    Mode& compressCTU(CUData& ctu, Frame& frame, const CUGeom& cuGeom, const Entropy& initialContext);
    int32_t loadTUDepth(CUGeom cuGeom, CUData parentCTU);

protected:
    /* Analysis data for save/load mode, writes/reads data based on absPartIdx */
    analysis_inter_data* m_reuseInterDataCTU;
    int32_t*             m_reuseRef;
    uint8_t*             m_reuseDepth;
    uint8_t*             m_reuseModes;
    uint8_t*             m_reusePartSize;
    uint8_t*             m_reuseMergeFlag;

    uint32_t             m_splitRefIdx[4];
    uint64_t*            cacheCost;


    analysis2PassFrameData* m_multipassAnalysis;
    uint8_t*                m_multipassDepth;
    MV*                     m_multipassMv[2];
    int*                    m_multipassMvpIdx[2];
    int32_t*                m_multipassRef[2];
    uint8_t*                m_multipassModes;

    uint8_t                 m_evaluateInter;
    uint8_t*                m_additionalCtuInfo;
    int*                    m_prevCtuInfoChange;
    /* refine RD based on QP for rd-levels 5 and 6 */
    void qprdRefine(const CUData& parentCTU, const CUGeom& cuGeom, int32_t qp, int32_t lqp);

    /* full analysis for an I-slice CU */
    uint64_t compressIntraCU(const CUData& parentCTU, const CUGeom& cuGeom, int32_t qp);

    /* full analysis for a P or B slice CU */
    uint32_t compressInterCU_dist(const CUData& parentCTU, const CUGeom& cuGeom, int32_t qp);
    SplitData compressInterCU_rd0_4(const CUData& parentCTU, const CUGeom& cuGeom, int32_t qp);
    SplitData compressInterCU_rd5_6(const CUData& parentCTU, const CUGeom& cuGeom, int32_t qp);

    void recodeCU(const CUData& parentCTU, const CUGeom& cuGeom, int32_t qp, int32_t origqp = -1);

    /* measure merge and skip */
    void checkMerge2Nx2N_rd0_4(Mode& skip, Mode& merge, const CUGeom& cuGeom);
    void checkMerge2Nx2N_rd5_6(Mode& skip, Mode& merge, const CUGeom& cuGeom);

    /* measure inter options */
    void checkInter_rd0_4(Mode& interMode, const CUGeom& cuGeom, PartSize partSize, uint32_t refmask[2]);
    void checkInter_rd5_6(Mode& interMode, const CUGeom& cuGeom, PartSize partSize, uint32_t refmask[2]);

    void checkBidir2Nx2N(Mode& inter2Nx2N, Mode& bidir2Nx2N, const CUGeom& cuGeom);

    /* encode current bestMode losslessly, pick best RD cost */
    void tryLossless(const CUGeom& cuGeom);

    /* add the RD cost of coding a split flag (0 or 1) to the given mode */
    void addSplitFlagCost(Mode& mode, uint32_t depth);

    /* work-avoidance heuristics for RD levels < 5 */
    uint32_t topSkipMinDepth(const CUData& parentCTU, const CUGeom& cuGeom);
    bool recursionDepthCheck(const CUData& parentCTU, const CUGeom& cuGeom, const Mode& bestMode);
    bool complexityCheckCU(const Mode& bestMode);

    /* generate residual and recon pixels for an entire CTU recursively (RD0) */
    void encodeResidue(const CUData& parentCTU, const CUGeom& cuGeom);

    int calculateQpforCuSize(const CUData& ctu, const CUGeom& cuGeom, int32_t complexCheck = 0, double baseQP = -1);

    void calculateNormFactor(CUData& ctu, int qp);
    void normFactor(const pixel* src, uint32_t blockSize, CUData& ctu, int qp, TextType ttype);

    void collectPUStatistics(const CUData& ctu, const CUGeom& cuGeom);

    /* check whether current mode is the new best */
    inline void checkBestMode(Mode& mode, uint32_t depth)
    {
        ModeDepth& md = m_modeDepth[depth];
        if (md.bestMode)
        {
            if (mode.rdCost < md.bestMode->rdCost)
                md.bestMode = &mode;
        }
        else
            md.bestMode = &mode;
    }
    int findSameContentRefCount(const CUData& parentCTU, const CUGeom& cuGeom);
};

struct ThreadLocalData
{
    Analysis analysis;

    void destroy() { analysis.destroy(); }
};

}

#endif // ifndef X265_ANALYSIS_H
