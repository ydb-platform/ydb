/*
 * H.265 video codec.
 * Copyright (c) 2013-2014 struktur AG, Dirk Farin <farin@struktur.de>
 *
 * Authors: struktur AG, Dirk Farin <farin@struktur.de>
 *          Min Chen <chenm003@163.com>
 *
 * This file is part of libde265.
 *
 * libde265 is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * libde265 is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with libde265.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef DE265_CONTEXTMODEL_H
#define DE265_CONTEXTMODEL_H

#include "libde265/cabac.h"
#include "libde265/de265.h"

#include <string.h>
#include <string>


struct context_model {
  uint8_t MPSbit : 1;
  uint8_t state  : 7;

  bool operator==(context_model b) const { return state==b.state && MPSbit==b.MPSbit; }
  bool operator!=(context_model b) const { return state!=b.state || MPSbit!=b.MPSbit; }
};


enum context_model_index {
  // SAO
  CONTEXT_MODEL_SAO_MERGE_FLAG = 0,
  CONTEXT_MODEL_SAO_TYPE_IDX   = CONTEXT_MODEL_SAO_MERGE_FLAG +1,

  // CB-tree
  CONTEXT_MODEL_SPLIT_CU_FLAG  = CONTEXT_MODEL_SAO_TYPE_IDX + 1,
  CONTEXT_MODEL_CU_SKIP_FLAG   = CONTEXT_MODEL_SPLIT_CU_FLAG + 3,

  // intra-prediction
  CONTEXT_MODEL_PART_MODE      = CONTEXT_MODEL_CU_SKIP_FLAG + 3,
  CONTEXT_MODEL_PREV_INTRA_LUMA_PRED_FLAG = CONTEXT_MODEL_PART_MODE + 4,
  CONTEXT_MODEL_INTRA_CHROMA_PRED_MODE    = CONTEXT_MODEL_PREV_INTRA_LUMA_PRED_FLAG + 1,

  // transform-tree
  CONTEXT_MODEL_CBF_LUMA                  = CONTEXT_MODEL_INTRA_CHROMA_PRED_MODE + 1,
  CONTEXT_MODEL_CBF_CHROMA                = CONTEXT_MODEL_CBF_LUMA + 2,
  CONTEXT_MODEL_SPLIT_TRANSFORM_FLAG      = CONTEXT_MODEL_CBF_CHROMA + 4,
  CONTEXT_MODEL_CU_CHROMA_QP_OFFSET_FLAG  = CONTEXT_MODEL_SPLIT_TRANSFORM_FLAG + 3,
  CONTEXT_MODEL_CU_CHROMA_QP_OFFSET_IDX   = CONTEXT_MODEL_CU_CHROMA_QP_OFFSET_FLAG + 1,

  // residual
  CONTEXT_MODEL_LAST_SIGNIFICANT_COEFFICIENT_X_PREFIX = CONTEXT_MODEL_CU_CHROMA_QP_OFFSET_IDX + 1,
  CONTEXT_MODEL_LAST_SIGNIFICANT_COEFFICIENT_Y_PREFIX = CONTEXT_MODEL_LAST_SIGNIFICANT_COEFFICIENT_X_PREFIX + 18,
  CONTEXT_MODEL_CODED_SUB_BLOCK_FLAG          = CONTEXT_MODEL_LAST_SIGNIFICANT_COEFFICIENT_Y_PREFIX + 18,
  CONTEXT_MODEL_SIGNIFICANT_COEFF_FLAG        = CONTEXT_MODEL_CODED_SUB_BLOCK_FLAG + 4,
  CONTEXT_MODEL_COEFF_ABS_LEVEL_GREATER1_FLAG = CONTEXT_MODEL_SIGNIFICANT_COEFF_FLAG + 42+2,
  CONTEXT_MODEL_COEFF_ABS_LEVEL_GREATER2_FLAG = CONTEXT_MODEL_COEFF_ABS_LEVEL_GREATER1_FLAG + 24,

  CONTEXT_MODEL_CU_QP_DELTA_ABS        = CONTEXT_MODEL_COEFF_ABS_LEVEL_GREATER2_FLAG + 6,
  CONTEXT_MODEL_TRANSFORM_SKIP_FLAG    = CONTEXT_MODEL_CU_QP_DELTA_ABS + 2,
  CONTEXT_MODEL_RDPCM_FLAG             = CONTEXT_MODEL_TRANSFORM_SKIP_FLAG + 2,
  CONTEXT_MODEL_RDPCM_DIR              = CONTEXT_MODEL_RDPCM_FLAG + 2,

  // motion
  CONTEXT_MODEL_MERGE_FLAG             = CONTEXT_MODEL_RDPCM_DIR + 2,
  CONTEXT_MODEL_MERGE_IDX              = CONTEXT_MODEL_MERGE_FLAG + 1,
  CONTEXT_MODEL_PRED_MODE_FLAG         = CONTEXT_MODEL_MERGE_IDX + 1,
  CONTEXT_MODEL_ABS_MVD_GREATER01_FLAG = CONTEXT_MODEL_PRED_MODE_FLAG + 1,
  CONTEXT_MODEL_MVP_LX_FLAG            = CONTEXT_MODEL_ABS_MVD_GREATER01_FLAG + 2,
  CONTEXT_MODEL_RQT_ROOT_CBF           = CONTEXT_MODEL_MVP_LX_FLAG + 1,
  CONTEXT_MODEL_REF_IDX_LX             = CONTEXT_MODEL_RQT_ROOT_CBF + 1,
  CONTEXT_MODEL_INTER_PRED_IDC         = CONTEXT_MODEL_REF_IDX_LX + 2,
  CONTEXT_MODEL_CU_TRANSQUANT_BYPASS_FLAG = CONTEXT_MODEL_INTER_PRED_IDC + 5,
  CONTEXT_MODEL_LOG2_RES_SCALE_ABS_PLUS1 = CONTEXT_MODEL_CU_TRANSQUANT_BYPASS_FLAG + 1,
  CONTEXT_MODEL_RES_SCALE_SIGN_FLAG      = CONTEXT_MODEL_LOG2_RES_SCALE_ABS_PLUS1 + 8,
  CONTEXT_MODEL_TABLE_LENGTH           = CONTEXT_MODEL_RES_SCALE_SIGN_FLAG + 2
};



void initialize_CABAC_models(context_model context_model_table[CONTEXT_MODEL_TABLE_LENGTH],
                             int initType,
                             int QPY);


class context_model_table
{
 public:
  context_model_table();
  context_model_table(const context_model_table&);
  ~context_model_table();

  void init(int initType, int QPY);
  void release();
  void decouple();
  context_model_table transfer();
  context_model_table copy() const { context_model_table t=*this; t.decouple(); return t; }

  bool empty() const { return refcnt != NULL; }

  context_model& operator[](int i) { return model[i]; }

  context_model_table& operator=(const context_model_table&);

  bool operator==(const context_model_table&) const;

  std::string debug_dump() const;

 private:
  void decouple_or_alloc_with_empty_data();

  context_model* model; // [CONTEXT_MODEL_TABLE_LENGTH]
  int* refcnt;
};


#endif
