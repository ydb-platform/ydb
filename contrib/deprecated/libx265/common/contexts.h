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

#ifndef X265_CONTEXTS_H
#define X265_CONTEXTS_H

#include "common.h"

#define NUM_SPLIT_FLAG_CTX          3   // number of context models for split flag
#define NUM_SKIP_FLAG_CTX           3   // number of context models for skip flag

#define NUM_MERGE_FLAG_EXT_CTX      1   // number of context models for merge flag of merge extended
#define NUM_MERGE_IDX_EXT_CTX       1   // number of context models for merge index of merge extended

#define NUM_PART_SIZE_CTX           4   // number of context models for partition size
#define NUM_PRED_MODE_CTX           1   // number of context models for prediction mode

#define NUM_ADI_CTX                 1   // number of context models for intra prediction

#define NUM_CHROMA_PRED_CTX         2   // number of context models for intra prediction (chroma)
#define NUM_INTER_DIR_CTX           5   // number of context models for inter prediction direction
#define NUM_MV_RES_CTX              2   // number of context models for motion vector difference

#define NUM_REF_NO_CTX              2   // number of context models for reference index
#define NUM_TRANS_SUBDIV_FLAG_CTX   3   // number of context models for transform subdivision flags
#define NUM_QT_CBF_CTX              7   // number of context models for QT CBF
#define NUM_QT_ROOT_CBF_CTX         1   // number of context models for QT ROOT CBF
#define NUM_DELTA_QP_CTX            3   // number of context models for dQP

#define NUM_SIG_CG_FLAG_CTX         2   // number of context models for MULTI_LEVEL_SIGNIFICANCE

#define NUM_SIG_FLAG_CTX            42  // number of context models for sig flag
#define NUM_SIG_FLAG_CTX_LUMA       27  // number of context models for luma sig flag
#define NUM_SIG_FLAG_CTX_CHROMA     15  // number of context models for chroma sig flag

#define NUM_CTX_LAST_FLAG_XY        18  // number of context models for last coefficient position
#define NUM_CTX_LAST_FLAG_XY_LUMA   15  // number of context models for last coefficient position of luma
#define NUM_CTX_LAST_FLAG_XY_CHROMA 3   // number of context models for last coefficient position of chroma

#define NUM_ONE_FLAG_CTX            24  // number of context models for greater than 1 flag
#define NUM_ONE_FLAG_CTX_LUMA       16  // number of context models for greater than 1 flag of luma
#define NUM_ONE_FLAG_CTX_CHROMA     8   // number of context models for greater than 1 flag of chroma
#define NUM_ABS_FLAG_CTX            6   // number of context models for greater than 2 flag
#define NUM_ABS_FLAG_CTX_LUMA       4   // number of context models for greater than 2 flag of luma
#define NUM_ABS_FLAG_CTX_CHROMA     2   // number of context models for greater than 2 flag of chroma

#define NUM_MVP_IDX_CTX             1   // number of context models for MVP index

#define NUM_SAO_MERGE_FLAG_CTX      1   // number of context models for SAO merge flags
#define NUM_SAO_TYPE_IDX_CTX        1   // number of context models for SAO type index

#define NUM_TRANSFORMSKIP_FLAG_CTX  1   // number of context models for transform skipping
#define NUM_TQUANT_BYPASS_FLAG_CTX  1
#define CNU                         154 // dummy initialization value for unused context models 'Context model Not Used'

// Offset for context
#define OFF_SPLIT_FLAG_CTX         (0)
#define OFF_SKIP_FLAG_CTX          (OFF_SPLIT_FLAG_CTX         +     NUM_SPLIT_FLAG_CTX)
#define OFF_MERGE_FLAG_EXT_CTX     (OFF_SKIP_FLAG_CTX          +     NUM_SKIP_FLAG_CTX)
#define OFF_MERGE_IDX_EXT_CTX      (OFF_MERGE_FLAG_EXT_CTX     +     NUM_MERGE_FLAG_EXT_CTX)
#define OFF_PART_SIZE_CTX          (OFF_MERGE_IDX_EXT_CTX      +     NUM_MERGE_IDX_EXT_CTX)
#define OFF_PRED_MODE_CTX          (OFF_PART_SIZE_CTX          +     NUM_PART_SIZE_CTX)
#define OFF_ADI_CTX                (OFF_PRED_MODE_CTX          +     NUM_PRED_MODE_CTX)
#define OFF_CHROMA_PRED_CTX        (OFF_ADI_CTX                +     NUM_ADI_CTX)
#define OFF_DELTA_QP_CTX           (OFF_CHROMA_PRED_CTX        +     NUM_CHROMA_PRED_CTX)
#define OFF_INTER_DIR_CTX          (OFF_DELTA_QP_CTX           +     NUM_DELTA_QP_CTX)
#define OFF_REF_NO_CTX             (OFF_INTER_DIR_CTX          +     NUM_INTER_DIR_CTX)
#define OFF_MV_RES_CTX             (OFF_REF_NO_CTX             +     NUM_REF_NO_CTX)
#define OFF_QT_CBF_CTX             (OFF_MV_RES_CTX             +     NUM_MV_RES_CTX)
#define OFF_TRANS_SUBDIV_FLAG_CTX  (OFF_QT_CBF_CTX             +     NUM_QT_CBF_CTX)
#define OFF_QT_ROOT_CBF_CTX        (OFF_TRANS_SUBDIV_FLAG_CTX  +     NUM_TRANS_SUBDIV_FLAG_CTX)
#define OFF_SIG_CG_FLAG_CTX        (OFF_QT_ROOT_CBF_CTX        +     NUM_QT_ROOT_CBF_CTX)
#define OFF_SIG_FLAG_CTX           (OFF_SIG_CG_FLAG_CTX        + 2 * NUM_SIG_CG_FLAG_CTX)
#define OFF_CTX_LAST_FLAG_X        (OFF_SIG_FLAG_CTX           +     NUM_SIG_FLAG_CTX)
#define OFF_CTX_LAST_FLAG_Y        (OFF_CTX_LAST_FLAG_X        +     NUM_CTX_LAST_FLAG_XY)
#define OFF_ONE_FLAG_CTX           (OFF_CTX_LAST_FLAG_Y        +     NUM_CTX_LAST_FLAG_XY)
#define OFF_ABS_FLAG_CTX           (OFF_ONE_FLAG_CTX           +     NUM_ONE_FLAG_CTX)
#define OFF_MVP_IDX_CTX            (OFF_ABS_FLAG_CTX           +     NUM_ABS_FLAG_CTX)
#define OFF_SAO_MERGE_FLAG_CTX     (OFF_MVP_IDX_CTX            +     NUM_MVP_IDX_CTX)
#define OFF_SAO_TYPE_IDX_CTX       (OFF_SAO_MERGE_FLAG_CTX     +     NUM_SAO_MERGE_FLAG_CTX)
#define OFF_TRANSFORMSKIP_FLAG_CTX (OFF_SAO_TYPE_IDX_CTX       +     NUM_SAO_TYPE_IDX_CTX)
#define OFF_TQUANT_BYPASS_FLAG_CTX (OFF_TRANSFORMSKIP_FLAG_CTX + 2 * NUM_TRANSFORMSKIP_FLAG_CTX)
#define MAX_OFF_CTX_MOD            (OFF_TQUANT_BYPASS_FLAG_CTX +     NUM_TQUANT_BYPASS_FLAG_CTX)

extern "C" const uint32_t PFX(entropyStateBits)[128];

namespace X265_NS {
// private namespace

extern const uint32_t g_entropyBits[128];
extern const uint8_t g_nextState[128][2];

#define sbacGetMps(S)            ((S) & 1)
#define sbacGetState(S)          ((S) >> 1)
#define sbacNext(S, V)           (g_nextState[(S)][(V)])
#define sbacGetEntropyBits(S, V) (g_entropyBits[(S) ^ (V)])
#define sbacGetEntropyBitsTrm(V) (g_entropyBits[126 ^ (V)])

static const uint32_t ctxCbf[3][5] = { { 1, 0, 0, 0, 0 }, { 2, 3, 4, 5, 6 }, { 2, 3, 4, 5, 6 } };

}

#endif // ifndef X265_CONTEXTS_H
