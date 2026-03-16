/*
 * H.265 video codec.
 * Copyright (c) 2013-2014 struktur AG, Dirk Farin <farin@struktur.de>
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

#include "slice.h"
#include <assert.h>
#include <iomanip>
#include <sstream>

bool D = false;

context_model_table::context_model_table()
  : model(NULL), refcnt(NULL)
{
}


context_model_table::context_model_table(const context_model_table& src)
{
  if (D) printf("%p c'tor = %p\n",this,&src);

  if (src.refcnt) {
    (*(src.refcnt))++;
  }

  refcnt = src.refcnt;
  model  = src.model;
}


context_model_table::~context_model_table()
{
  if (D) printf("%p destructor\n",this);

  if (refcnt) {
    (*refcnt)--;
    if (*refcnt==0) {
      if (D) printf("mfree %p\n",model);
      delete[] model;
      delete refcnt;
    }
  }
}


void context_model_table::init(int initType, int QPY)
{
  if (D) printf("%p init\n",this);

  decouple_or_alloc_with_empty_data();

  initialize_CABAC_models(model, initType, QPY);
}


void context_model_table::release()
{
  if (D) printf("%p release %p\n",this,refcnt);

  if (!refcnt) { return; }

  // if (*refcnt == 1) { return; } <- keep memory for later, but does not work when we believe that we freed the memory and nulled all references

  (*refcnt)--;
  if (*refcnt==0) {
    delete[] model;
    delete refcnt;
  }

  model = nullptr;
  refcnt= nullptr;
}


void context_model_table::decouple()
{
  if (D) printf("%p decouple (%p)\n",this,refcnt);

  assert(refcnt); // not necessarily so, but we never use it on an uninitialized object

  if (*refcnt > 1) {
    (*refcnt)--;

    context_model* oldModel = model;

    model = new context_model[CONTEXT_MODEL_TABLE_LENGTH];
    refcnt= new int;
    *refcnt=1;

    memcpy(model,oldModel,sizeof(context_model)*CONTEXT_MODEL_TABLE_LENGTH);
  }
}


context_model_table context_model_table::transfer()
{
  context_model_table newtable;
  newtable.model = model;
  newtable.refcnt= refcnt;

  model =nullptr;
  refcnt=nullptr;

  return newtable;
}


context_model_table& context_model_table::operator=(const context_model_table& src)
{
  if (D) printf("%p assign = %p\n",this,&src);

  // assert(src.refcnt); // not necessarily so, but we never use it on an uninitialized object

  if (!src.refcnt) {
    release();
    return *this;
  }

  (*(src.refcnt))++;

  release();

  model = src.model;
  refcnt= src.refcnt;

  return *this;
}


bool context_model_table::operator==(const context_model_table& b) const
{
  if (b.model == model) return true;
  if (b.model == nullptr || model == nullptr) return false;

  for (int i=0;i<CONTEXT_MODEL_TABLE_LENGTH;i++) {
    if (!(b.model[i] == model[i])) return false;
  }

  return true;
}


std::string context_model_table::debug_dump() const
{
  int hash = 0;
  for (int i=0;i<CONTEXT_MODEL_TABLE_LENGTH;i++) {
    hash ^= ((i+7)*model[i].state) & 0xFFFF;
  }

  std::stringstream sstr;
  sstr << std::hex << hash;
  return sstr.str();
}


void context_model_table::decouple_or_alloc_with_empty_data()
{
  if (refcnt && *refcnt==1) { return; }

  if (refcnt) {
    assert(*refcnt>1);
    (*refcnt)--;
  }

  if (D) printf("%p (alloc)\n",this);

  model = new context_model[CONTEXT_MODEL_TABLE_LENGTH];
  // Without initializing the model, we got an invalid model state during decoding (issue #236)
  memset(model, 0, sizeof(context_model) * CONTEXT_MODEL_TABLE_LENGTH);
  refcnt= new int;
  *refcnt=1;
}






static void set_initValue(int SliceQPY,
                          context_model* model, int initValue, int nContexts)
{
  int slopeIdx = initValue >> 4;
  int intersecIdx = initValue & 0xF;
  int m = slopeIdx*5 - 45;
  int n = (intersecIdx<<3) - 16;
  int preCtxState = Clip3(1,126, ((m*Clip3(0,51, SliceQPY))>>4)+n);

  // logtrace(LogSlice,"QP=%d slopeIdx=%d intersecIdx=%d m=%d n=%d\n",SliceQPY,slopeIdx,intersecIdx,m,n);

  for (int i=0;i<nContexts;i++) {
    model[i].MPSbit=(preCtxState<=63) ? 0 : 1;
    model[i].state = model[i].MPSbit ? (preCtxState-64) : (63-preCtxState);

    // model state will always be between [0;62]

    assert(model[i].state <= 62);
  }
}


static const int initValue_split_cu_flag[3][3] = {
  { 139,141,157 },
  { 107,139,126 },
  { 107,139,126 },
};
static const int initValue_cu_skip_flag[2][3] = {
  { 197,185,201 },
  { 197,185,201 },
};
static const int initValue_part_mode[9] = { 184,154,139, 154,154,154, 139,154,154 };
static const int initValue_prev_intra_luma_pred_flag[3] = { 184,154,183 };
static const int initValue_intra_chroma_pred_mode[3] = { 63,152,152 };
static const int initValue_cbf_luma[4] = { 111,141,153,111 };
static const int initValue_cbf_chroma[12] = { 94,138,182,154,149,107,167,154,149,92,167,154 };
static const int initValue_split_transform_flag[9] = { 153,138,138, 124,138,94, 224,167,122 }; // FIX712
static const int initValue_last_significant_coefficient_prefix[54] = {
    110,110,124,125,140,153,125,127,140,109,111,143,127,111, 79,108,123, 63,
    125,110, 94,110, 95, 79,125,111,110, 78,110,111,111, 95, 94,108,123,108,
    125,110,124,110, 95, 94,125,111,111, 79,125,126,111,111, 79,108,123, 93
  };
static const int initValue_coded_sub_block_flag[12] = { 91,171,134,141,121,140,61,154,121,140,61,154 };
static const int initValue_significant_coeff_flag[3][42] = {
    {
      111,  111,  125,  110,  110,   94,  124,  108,  124,  107,  125,  141,  179,  153,  125,  107,
      125,  141,  179,  153,  125,  107,  125,  141,  179,  153,  125,  140,  139,  182,  182,  152,
      136,  152,  136,  153,  136,  139,  111,  136,  139,  111
    },
    {
      155,  154,  139,  153,  139,  123,  123,   63,  153,  166,  183,  140,  136,  153,  154,  166,
      183,  140,  136,  153,  154,  166,  183,  140,  136,  153,  154,  170,  153,  123,  123,  107,
      121,  107,  121,  167,  151,  183,  140,  151,  183,  140,
    },
    {
      170,  154,  139,  153,  139,  123,  123,   63,  124,  166,  183,  140,  136,  153,  154,  166,
      183,  140,  136,  153,  154,  166,  183,  140,  136,  153,  154,  170,  153,  138,  138,  122,
      121,  122,  121,  167,  151,  183,  140,  151,  183,  140
    },
  };
static const int initValue_significant_coeff_flag_skipmode[3][2] = {
  { 141,111 }, { 140,140 }, { 140,140 }
};

static const int initValue_coeff_abs_level_greater1_flag[72] = {
    140, 92,137,138,140,152,138,139,153, 74,149, 92,139,107,122,152,
    140,179,166,182,140,227,122,197,154,196,196,167,154,152,167,182,
    182,134,149,136,153,121,136,137,169,194,166,167,154,167,137,182,
    154,196,167,167,154,152,167,182,182,134,149,136,153,121,136,122,
    169,208,166,167,154,152,167,182
  };
static const int initValue_coeff_abs_level_greater2_flag[18] = {
    138,153,136,167,152,152,107,167, 91,122,107,167,
    107,167, 91,107,107,167
  };
static const int initValue_sao_merge_leftUp_flag[3] = { 153,153,153 };
static const int initValue_sao_type_idx_lumaChroma_flag[3] = { 200,185,160 };
static const int initValue_cu_qp_delta_abs[2] = { 154,154 };
static const int initValue_transform_skip_flag[2] = { 139,139 };
static const int initValue_merge_flag[2] = { 110,154 };
static const int initValue_merge_idx[2] = { 122,137 };
static const int initValue_pred_mode_flag[2] = { 149,134 };
static const int initValue_abs_mvd_greater01_flag[4] = { 140,198,169,198 };
static const int initValue_mvp_lx_flag[1] = { 168 };
static const int initValue_rqt_root_cbf[1] = { 79 };
static const int initValue_ref_idx_lX[2] = { 153,153 };
static const int initValue_inter_pred_idc[5] = { 95,79,63,31,31 };
static const int initValue_cu_transquant_bypass_flag[3] = { 154,154,154 };


static void init_context(int SliceQPY,
                         context_model* model,
                         const int* initValues, int len)
{
  for (int i=0;i<len;i++)
    {
      set_initValue(SliceQPY, &model[i], initValues[i], 1);
    }
}


static void init_context_const(int SliceQPY,
                               context_model* model,
                               int initValue, int len)
{
  set_initValue(SliceQPY, model, initValue, len);
}

void initialize_CABAC_models(context_model context_model_table[CONTEXT_MODEL_TABLE_LENGTH],
                      int initType,
                      int QPY)
{
  context_model* cm = context_model_table; // just an abbreviation

  if (initType > 0) {
    init_context(QPY, cm+CONTEXT_MODEL_CU_SKIP_FLAG,    initValue_cu_skip_flag[initType-1],  3);
    init_context(QPY, cm+CONTEXT_MODEL_PRED_MODE_FLAG, &initValue_pred_mode_flag[initType-1], 1);
    init_context(QPY, cm+CONTEXT_MODEL_MERGE_FLAG,             &initValue_merge_flag[initType-1],1);
    init_context(QPY, cm+CONTEXT_MODEL_MERGE_IDX,              &initValue_merge_idx[initType-1], 1);
    init_context(QPY, cm+CONTEXT_MODEL_INTER_PRED_IDC,         initValue_inter_pred_idc,         5);
    init_context(QPY, cm+CONTEXT_MODEL_REF_IDX_LX,             initValue_ref_idx_lX,             2);
    init_context(QPY, cm+CONTEXT_MODEL_ABS_MVD_GREATER01_FLAG, &initValue_abs_mvd_greater01_flag[initType == 1 ? 0 : 2], 2);
    init_context(QPY, cm+CONTEXT_MODEL_MVP_LX_FLAG,            initValue_mvp_lx_flag,            1);
    init_context(QPY, cm+CONTEXT_MODEL_RQT_ROOT_CBF,           initValue_rqt_root_cbf,           1);

    init_context_const(QPY, cm+CONTEXT_MODEL_RDPCM_FLAG, 139, 2);
    init_context_const(QPY, cm+CONTEXT_MODEL_RDPCM_DIR,  139, 2);
  }

  init_context(QPY, cm+CONTEXT_MODEL_SPLIT_CU_FLAG, initValue_split_cu_flag[initType], 3);
  init_context(QPY, cm+CONTEXT_MODEL_PART_MODE,     &initValue_part_mode[(initType!=2 ? initType : 5)], 4);
  init_context(QPY, cm+CONTEXT_MODEL_PREV_INTRA_LUMA_PRED_FLAG, &initValue_prev_intra_luma_pred_flag[initType], 1);
  init_context(QPY, cm+CONTEXT_MODEL_INTRA_CHROMA_PRED_MODE,    &initValue_intra_chroma_pred_mode[initType],    1);
  init_context(QPY, cm+CONTEXT_MODEL_CBF_LUMA,                  &initValue_cbf_luma[initType == 0 ? 0 : 2],     2);
  init_context(QPY, cm+CONTEXT_MODEL_CBF_CHROMA,                &initValue_cbf_chroma[initType * 4],            4);
  init_context(QPY, cm+CONTEXT_MODEL_SPLIT_TRANSFORM_FLAG,      &initValue_split_transform_flag[initType * 3],  3);
  init_context(QPY, cm+CONTEXT_MODEL_LAST_SIGNIFICANT_COEFFICIENT_X_PREFIX, &initValue_last_significant_coefficient_prefix[initType * 18], 18);
  init_context(QPY, cm+CONTEXT_MODEL_LAST_SIGNIFICANT_COEFFICIENT_Y_PREFIX, &initValue_last_significant_coefficient_prefix[initType * 18], 18);
  init_context(QPY, cm+CONTEXT_MODEL_CODED_SUB_BLOCK_FLAG,                  &initValue_coded_sub_block_flag[initType * 4],        4);
  init_context(QPY, cm+CONTEXT_MODEL_SIGNIFICANT_COEFF_FLAG,              initValue_significant_coeff_flag[initType],    42);
  init_context(QPY, cm+CONTEXT_MODEL_SIGNIFICANT_COEFF_FLAG+42, initValue_significant_coeff_flag_skipmode[initType], 2);

  init_context(QPY, cm+CONTEXT_MODEL_COEFF_ABS_LEVEL_GREATER1_FLAG,       &initValue_coeff_abs_level_greater1_flag[initType * 24], 24);
  init_context(QPY, cm+CONTEXT_MODEL_COEFF_ABS_LEVEL_GREATER2_FLAG,       &initValue_coeff_abs_level_greater2_flag[initType *  6],  6);
  init_context(QPY, cm+CONTEXT_MODEL_SAO_MERGE_FLAG,                      &initValue_sao_merge_leftUp_flag[initType],    1);
  init_context(QPY, cm+CONTEXT_MODEL_SAO_TYPE_IDX,                        &initValue_sao_type_idx_lumaChroma_flag[initType], 1);
  init_context(QPY, cm+CONTEXT_MODEL_CU_QP_DELTA_ABS,        initValue_cu_qp_delta_abs,        2);
  init_context(QPY, cm+CONTEXT_MODEL_TRANSFORM_SKIP_FLAG,    initValue_transform_skip_flag,    2);
  init_context(QPY, cm+CONTEXT_MODEL_CU_TRANSQUANT_BYPASS_FLAG, &initValue_cu_transquant_bypass_flag[initType], 1);

  init_context_const(QPY, cm+CONTEXT_MODEL_LOG2_RES_SCALE_ABS_PLUS1, 154, 8);
  init_context_const(QPY, cm+CONTEXT_MODEL_RES_SCALE_SIGN_FLAG,      154, 2);
  init_context_const(QPY, cm+CONTEXT_MODEL_CU_CHROMA_QP_OFFSET_FLAG, 154, 1);
  init_context_const(QPY, cm+CONTEXT_MODEL_CU_CHROMA_QP_OFFSET_IDX,  154, 1);
}
