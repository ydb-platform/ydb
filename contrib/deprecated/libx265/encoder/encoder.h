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

#ifndef X265_ENCODER_H
#define X265_ENCODER_H

#include "common.h"
#include "slice.h"
#include "threading.h"
#include "scalinglist.h"
#include "x265.h"
#include "nal.h"
#include "framedata.h"
#ifdef ENABLE_HDR10_PLUS
    #include "dynamicHDR10/hdr10plus.h" // Y_IGNORE
#endif
struct x265_encoder {};
namespace X265_NS {
// private namespace
extern const char g_sliceTypeToChar[3];

class Entropy;

struct EncStats
{
    double        m_psnrSumY;
    double        m_psnrSumU;
    double        m_psnrSumV;
    double        m_globalSsim;
    double        m_totalQp;
    double        m_maxFALL;
    uint64_t      m_accBits;
    uint32_t      m_numPics;
    uint16_t      m_maxCLL;

    EncStats()
    {
        m_psnrSumY = m_psnrSumU = m_psnrSumV = m_globalSsim = 0;
        m_accBits = 0;
        m_numPics = 0;
        m_totalQp = 0;
        m_maxCLL = 0;
        m_maxFALL = 0;
    }

    void addQP(double aveQp);

    void addPsnr(double psnrY, double psnrU, double psnrV);

    void addBits(uint64_t bits);

    void addSsim(double ssim);
};

#define MAX_NUM_REF_IDX 64

struct RefIdxLastGOP
{
    int numRefIdxDefault[2];
    int numRefIdxl0[MAX_NUM_REF_IDX];
    int numRefIdxl1[MAX_NUM_REF_IDX];
};

struct RPSListNode
{
    int idx;
    int count;
    RPS* rps;
    RPSListNode* next;
    RPSListNode* prior;
};

class FrameEncoder;
class DPB;
class Lookahead;
class RateControl;
class ThreadPool;
class FrameData;

class Encoder : public x265_encoder
{
public:

    uint32_t           m_residualSumEmergency[MAX_NUM_TR_CATEGORIES][MAX_NUM_TR_COEFFS];
    uint32_t           m_countEmergency[MAX_NUM_TR_CATEGORIES];
    uint16_t           (*m_offsetEmergency)[MAX_NUM_TR_CATEGORIES][MAX_NUM_TR_COEFFS];

    int64_t            m_firstPts;
    int64_t            m_bframeDelayTime;
    int64_t            m_prevReorderedPts[2];
    int64_t            m_encodeStartTime;

    int                m_pocLast;         // time index (POC)
    int                m_encodedFrameNum;
    int                m_outputCount;
    int                m_bframeDelay;
    int                m_numPools;
    int                m_curEncoder;

    // weighted prediction
    int                m_numLumaWPFrames;    // number of P frames with weighted luma reference
    int                m_numChromaWPFrames;  // number of P frames with weighted chroma reference
    int                m_numLumaWPBiFrames;  // number of B frames with weighted luma reference
    int                m_numChromaWPBiFrames; // number of B frames with weighted chroma reference
    int                m_conformanceMode;
    int                m_lastBPSEI;
    uint32_t           m_numDelayedPic;

    ThreadPool*        m_threadPool;
    FrameEncoder*      m_frameEncoder[X265_MAX_FRAME_THREADS];
    DPB*               m_dpb;
    Frame*             m_exportedPic;
    FILE*              m_analysisFile;
    FILE*              m_analysisFileIn;
    FILE*              m_analysisFileOut;
    x265_param*        m_param;
    x265_param*        m_latestParam;     // Holds latest param during a reconfigure
    RateControl*       m_rateControl;
    Lookahead*         m_lookahead;

    bool               m_externalFlush;
    /* Collect statistics globally */
    EncStats           m_analyzeAll;
    EncStats           m_analyzeI;
    EncStats           m_analyzeP;
    EncStats           m_analyzeB;
    VPS                m_vps;
    SPS                m_sps;
    PPS                m_pps;
    NALList            m_nalList;
    ScalingList        m_scalingList;      // quantization matrix information
    Window             m_conformanceWindow;

    bool               m_emitCLLSEI;
    bool               m_bZeroLatency;     // x265_encoder_encode() returns NALs for the input picture, zero lag
    bool               m_aborted;          // fatal error detected
    bool               m_reconfigure;      // Encoder reconfigure in progress
    bool               m_reconfigureRc;

    /* Begin intra refresh when one not in progress or else begin one as soon as the current 
     * one is done. Requires bIntraRefresh to be set.*/
    int                m_bQueuedIntraRefresh;

    /* For optimising slice QP */
    Lock               m_sliceQpLock;
    int                m_iFrameNum;   
    int                m_iPPSQpMinus26;
    int64_t            m_iBitsCostSum[QP_MAX_MAX + 1];
    Lock               m_sliceRefIdxLock;
    RefIdxLastGOP      m_refIdxLastGOP;

    Lock               m_rpsInSpsLock;
    int                m_rpsInSpsCount;
    /* For HDR*/
    double                m_cB;
    double                m_cR;

    int                     m_bToneMap; // Enables tone-mapping

#ifdef ENABLE_HDR10_PLUS
    const hdr10plus_api     *m_hdr10plus_api;
    uint8_t                 **m_cim;
    int                     m_numCimInfo;
#endif

    x265_sei_payload        m_prevTonemapPayload;

    Encoder();
    ~Encoder()
    {
#ifdef ENABLE_HDR10_PLUS
        if (m_prevTonemapPayload.payload != NULL)
            X265_FREE(m_prevTonemapPayload.payload);
#endif
    };

    void create();
    void stopJobs();
    void destroy();

    int encode(const x265_picture* pic, x265_picture *pic_out);

    int reconfigureParam(x265_param* encParam, x265_param* param);

    bool isReconfigureRc(x265_param* latestParam, x265_param* param_in);

    void copyCtuInfo(x265_ctu_info_t** frameCtuInfo, int poc);

    int copySlicetypePocAndSceneCut(int *slicetype, int *poc, int *sceneCut);

    int getRefFrameList(PicYuv** l0, PicYuv** l1, int sliceType, int poc);

    int setAnalysisDataAfterZScan(x265_analysis_data *analysis_data, Frame* curFrame);

    int setAnalysisData(x265_analysis_data *analysis_data, int poc, uint32_t cuBytes);

    void getStreamHeaders(NALList& list, Entropy& sbacCoder, Bitstream& bs);

    void fetchStats(x265_stats* stats, size_t statsSizeBytes);

    void printSummary();

    void printReconfigureParams();

    char* statsString(EncStats&, char*);

    void configure(x265_param *param);

    void updateVbvPlan(RateControl* rc);

    void allocAnalysis(x265_analysis_data* analysis);

    void freeAnalysis(x265_analysis_data* analysis);

    void allocAnalysis2Pass(x265_analysis_2Pass* analysis, int sliceType);

    void freeAnalysis2Pass(x265_analysis_2Pass* analysis, int sliceType);

    void readAnalysisFile(x265_analysis_data* analysis, int poc, const x265_picture* picIn);

    void writeAnalysisFile(x265_analysis_data* pic, FrameData &curEncData);
    void readAnalysis2PassFile(x265_analysis_2Pass* analysis2Pass, int poc, int sliceType);
    void writeAnalysis2PassFile(x265_analysis_2Pass* analysis2Pass, FrameData &curEncData, int slicetype);
    void finishFrameStats(Frame* pic, FrameEncoder *curEncoder, x265_frame_stats* frameStats, int inPoc);

    void calcRefreshInterval(Frame* frameEnc);

    void initRefIdx();
    void analyseRefIdx(int *numRefIdx);
    void updateRefIdx();
    bool computeSPSRPSIndex();

protected:

    void initVPS(VPS *vps);
    void initSPS(SPS *sps);
    void initPPS(PPS *pps);
};
}

#endif // ifndef X265_ENCODER_H
