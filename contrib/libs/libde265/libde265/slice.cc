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

#include "slice.h"
#include "motion.h"
#include "util.h"
#include "scan.h"
#include "intrapred.h"
#include "transform.h"
#include "threads.h"
#include "image.h"

#include <assert.h>
#include <string.h>
#include <stdlib.h>


#define LOCK de265_mutex_lock(&ctx->thread_pool.mutex)
#define UNLOCK de265_mutex_unlock(&ctx->thread_pool.mutex)

extern bool read_short_term_ref_pic_set(error_queue* errqueue,
                                        const seq_parameter_set* sps,
                                        bitreader* br,
                                        ref_pic_set* out_set,
                                        int idxRps,  // index of the set to be read
                                        const std::vector<ref_pic_set>& sets,
                                        bool sliceRefPicSet);


void read_coding_tree_unit(thread_context* tctx);
void read_coding_quadtree(thread_context* tctx,
                          int xCtb, int yCtb,
                          int Log2CtbSizeY,
                          int ctDepth);
/*
void decode_inter_block(decoder_context* ctx,thread_context* tctx,
                        int xC, int yC, int log2CbSize);
*/

void slice_segment_header::set_defaults()
{
  slice_index = 0;

  first_slice_segment_in_pic_flag = 1;
  no_output_of_prior_pics_flag = 0;
  slice_pic_parameter_set_id = 0;
  dependent_slice_segment_flag = 0;
  slice_segment_address = 0;

  slice_type = SLICE_TYPE_I;
  pic_output_flag = 1;
  colour_plane_id = 0;
  slice_pic_order_cnt_lsb = 0;
  short_term_ref_pic_set_sps_flag = 1;
  // ref_pic_set slice_ref_pic_set;

  short_term_ref_pic_set_idx = 0;
  num_long_term_sps = 0;
  num_long_term_pics = 0;

  //uint8_t lt_idx_sps[MAX_NUM_REF_PICS];
  //int     poc_lsb_lt[MAX_NUM_REF_PICS];
  //char    used_by_curr_pic_lt_flag[MAX_NUM_REF_PICS];

  //char delta_poc_msb_present_flag[MAX_NUM_REF_PICS];
  //int delta_poc_msb_cycle_lt[MAX_NUM_REF_PICS];

  slice_temporal_mvp_enabled_flag = 0;
  slice_sao_luma_flag = 0;
  slice_sao_chroma_flag = 0;

  num_ref_idx_active_override_flag = 0;
  num_ref_idx_l0_active=1; // [1;16]
  num_ref_idx_l1_active=1; // [1;16]

  ref_pic_list_modification_flag_l0 = 0;
  ref_pic_list_modification_flag_l1 = 0;
  //uint8_t list_entry_l0[16];
  //uint8_t list_entry_l1[16];

  mvd_l1_zero_flag = 0;
  cabac_init_flag = 0;
  collocated_from_l0_flag = 0;
  collocated_ref_idx = 0;

  // --- pred_weight_table ---

  luma_log2_weight_denom=0; // [0;7]
  ChromaLog2WeightDenom=0;  // [0;7]

  // first index is L0/L1
  /*
  uint8_t luma_weight_flag[2][16];   // bool
  uint8_t chroma_weight_flag[2][16]; // bool
  int16_t LumaWeight[2][16];
  int8_t  luma_offset[2][16];
  int16_t ChromaWeight[2][16][2];
  int8_t  ChromaOffset[2][16][2];
  */


  five_minus_max_num_merge_cand = 0;
  slice_qp_delta = 0;

  slice_cb_qp_offset = 0;
  slice_cr_qp_offset = 0;

  cu_chroma_qp_offset_enabled_flag = 0;

  deblocking_filter_override_flag = 0;
  slice_deblocking_filter_disabled_flag = 0;
  slice_beta_offset=0; // = pps->beta_offset if undefined
  slice_tc_offset=0;   // = pps->tc_offset if undefined

  slice_loop_filter_across_slices_enabled_flag = 0;

  num_entry_point_offsets = 0;
  //int  offset_len;
  //std::vector<int> entry_point_offset;

  slice_segment_header_extension_length = 0;

  SliceAddrRS = slice_segment_address;
}


bool read_pred_weight_table(bitreader* br, slice_segment_header* shdr, decoder_context* ctx)
{
  int vlc;

  pic_parameter_set* pps = ctx->get_pps((int)shdr->slice_pic_parameter_set_id);
  assert(pps);
  seq_parameter_set* sps = ctx->get_sps((int)pps->seq_parameter_set_id);
  assert(sps);

  shdr->luma_log2_weight_denom = vlc = get_uvlc(br);
  if (vlc<0 || vlc>7) return false;

  if (sps->chroma_format_idc != 0) {
    vlc = get_svlc(br);
    vlc += shdr->luma_log2_weight_denom;
    if (vlc<0 || vlc>7) return false;
    shdr->ChromaLog2WeightDenom = vlc;
  }

  int sumWeightFlags = 0;

  for (int l=0;l<=1;l++)
    if (l==0 || (l==1 && shdr->slice_type == SLICE_TYPE_B))
      {
        int num_ref = (l==0 ? shdr->num_ref_idx_l0_active-1 : shdr->num_ref_idx_l1_active-1);

        for (int i=0;i<=num_ref;i++) {
          shdr->luma_weight_flag[l][i] = get_bits(br,1);
          if (shdr->luma_weight_flag[l][i]) sumWeightFlags++;
        }

        if (sps->chroma_format_idc != 0) {
          for (int i=0;i<=num_ref;i++) {
            shdr->chroma_weight_flag[l][i] = get_bits(br,1);
            if (shdr->chroma_weight_flag[l][i]) sumWeightFlags+=2;
          }
        }

        for (int i=0;i<=num_ref;i++) {
          if (shdr->luma_weight_flag[l][i]) {

            // delta_luma_weight

            vlc = get_svlc(br);
            if (vlc < -128 || vlc > 127) return false;

            shdr->LumaWeight[l][i] = (1<<shdr->luma_log2_weight_denom) + vlc;

            // luma_offset

            vlc = get_svlc(br);
            if (vlc < -sps->WpOffsetHalfRangeY || vlc > sps->WpOffsetHalfRangeY-1) return false;
            shdr->luma_offset[l][i] = vlc;
          }
          else {
            shdr->LumaWeight[l][i] = 1<<shdr->luma_log2_weight_denom;
            shdr->luma_offset[l][i] = 0;
          }

          if (shdr->chroma_weight_flag[l][i])
            for (int j=0;j<2;j++) {
              // delta_chroma_weight

              vlc = get_svlc(br);
              if (vlc < -128 || vlc >  127) return false;

              shdr->ChromaWeight[l][i][j] = (1<<shdr->ChromaLog2WeightDenom) + vlc;

              // delta_chroma_offset

              vlc = get_svlc(br);
              if (vlc < -4*sps->WpOffsetHalfRangeC ||
                  vlc >  4*sps->WpOffsetHalfRangeC-1) return false;

              vlc = Clip3(-sps->WpOffsetHalfRangeC,
                          sps->WpOffsetHalfRangeC-1,
                          (sps->WpOffsetHalfRangeC
                           +vlc
                           -((sps->WpOffsetHalfRangeC*shdr->ChromaWeight[l][i][j])
                             >> shdr->ChromaLog2WeightDenom)));

              shdr->ChromaOffset[l][i][j] = vlc;
            }
          else {
            for (int j=0;j<2;j++) {
              shdr->ChromaWeight[l][i][j] = 1<<shdr->ChromaLog2WeightDenom;
              shdr->ChromaOffset[l][i][j] = 0;
            }
          }
        }
      }

  // TODO: bitstream conformance requires that 'sumWeightFlags<=24'

  return true;
}


void slice_segment_header::reset()
{
  pps = NULL;

  slice_index = 0;

  first_slice_segment_in_pic_flag = 0;
  no_output_of_prior_pics_flag = 0;
  slice_pic_parameter_set_id = 0;
  dependent_slice_segment_flag = 0;
  slice_segment_address = 0;

  slice_type = 0;
  pic_output_flag = 0;
  colour_plane_id = 0;
  slice_pic_order_cnt_lsb = 0;
  short_term_ref_pic_set_sps_flag = 0;
  slice_ref_pic_set.reset();

  short_term_ref_pic_set_idx = 0;
  num_long_term_sps = 0;
  num_long_term_pics= 0;

  for (int i=0;i<MAX_NUM_REF_PICS;i++) {
    lt_idx_sps[i] = 0;
    poc_lsb_lt[i] = 0;
    used_by_curr_pic_lt_flag[i] = 0;
    delta_poc_msb_present_flag[i] = 0;
    delta_poc_msb_cycle_lt[i] = 0;
  }

  slice_temporal_mvp_enabled_flag = 0;
  slice_sao_luma_flag = 0;
  slice_sao_chroma_flag = 0;

  num_ref_idx_active_override_flag = 0;
  num_ref_idx_l0_active = 0;
  num_ref_idx_l1_active = 0;

  ref_pic_list_modification_flag_l0 = 0;
  ref_pic_list_modification_flag_l1 = 0;
  for (int i=0;i<16;i++) {
    list_entry_l0[i] = 0;
    list_entry_l1[i] = 0;
  }

  mvd_l1_zero_flag = 0;
  cabac_init_flag  = 0;
  collocated_from_l0_flag = 0;
  collocated_ref_idx = 0;

  luma_log2_weight_denom = 0;
  ChromaLog2WeightDenom  = 0;

  for (int i=0;i<2;i++)
    for (int j=0;j<16;j++) {
      luma_weight_flag[i][j] = 0;
      chroma_weight_flag[i][j] = 0;
      LumaWeight[i][j] = 0;
      luma_offset[i][j] = 0;
      ChromaWeight[i][j][0] = ChromaWeight[i][j][1] = 0;
      ChromaOffset[i][j][0] = ChromaOffset[i][j][1] = 0;
    }

  five_minus_max_num_merge_cand = 0;
  slice_qp_delta = 0;

  slice_cb_qp_offset = 0;
  slice_cr_qp_offset = 0;

  cu_chroma_qp_offset_enabled_flag = 0;

  deblocking_filter_override_flag = 0;
  slice_deblocking_filter_disabled_flag = 0;
  slice_beta_offset = 0;
  slice_tc_offset = 0;

  slice_loop_filter_across_slices_enabled_flag = 0;

  num_entry_point_offsets = 0;
  offset_len = 0;
  entry_point_offset.clear();

  slice_segment_header_extension_length = 0;

  SliceAddrRS = 0;
  SliceQPY = 0;

  initType = 0;

  MaxNumMergeCand = 0;
  CurrRpsIdx = 0;
  CurrRps.reset();
  NumPocTotalCurr = 0;

  for (int i=0;i<2;i++)
    for (int j=0;j<MAX_NUM_REF_PICS;j++) {
      RefPicList[i][j] = 0;
      RefPicList_POC[i][j] = 0;
      RefPicList_PicState[i][j] = 0;
      LongTermRefPic[i][j] = 0;
    }

  //context_model ctx_model_storage[CONTEXT_MODEL_TABLE_LENGTH];

  RemoveReferencesList.clear();

  ctx_model_storage_defined = false;
}


de265_error slice_segment_header::read(bitreader* br, decoder_context* ctx,
                                       bool* continueDecoding)
{
  *continueDecoding = false;
  reset();

  // set defaults

  dependent_slice_segment_flag = 0;


  // read bitstream

  first_slice_segment_in_pic_flag = get_bits(br,1);

  if (ctx->get_RapPicFlag()) { // TODO: is this still correct ? Should we drop RapPicFlag ?
    no_output_of_prior_pics_flag = get_bits(br,1);
  }

  slice_pic_parameter_set_id = get_uvlc(br);
  if (slice_pic_parameter_set_id >= DE265_MAX_PPS_SETS ||
      slice_pic_parameter_set_id == UVLC_ERROR) {
    ctx->add_warning(DE265_WARNING_NONEXISTING_PPS_REFERENCED, false);
    return DE265_OK;
  }

  if (!ctx->has_pps(slice_pic_parameter_set_id)) {
    ctx->add_warning(DE265_WARNING_NONEXISTING_PPS_REFERENCED, false);
    return DE265_OK;
  }

  pps = ctx->get_shared_pps(slice_pic_parameter_set_id);

  const seq_parameter_set* sps = pps->sps.get();
  if (!sps->sps_read) {
    ctx->add_warning(DE265_WARNING_NONEXISTING_SPS_REFERENCED, false);
    *continueDecoding = false;
    return DE265_OK;
  }

  if (!first_slice_segment_in_pic_flag) {
    if (pps->dependent_slice_segments_enabled_flag) {
      dependent_slice_segment_flag = get_bits(br,1);
    } else {
      dependent_slice_segment_flag = 0;
    }

    int slice_segment_address = get_bits(br, ceil_log2(sps->PicSizeInCtbsY));

    if (dependent_slice_segment_flag) {
      if (slice_segment_address == 0) {
        *continueDecoding = false;
        ctx->add_warning(DE265_WARNING_DEPENDENT_SLICE_WITH_ADDRESS_ZERO, false);
        return DE265_OK;
      }

      if (ctx->previous_slice_header == NULL) {
        return DE265_ERROR_NO_INITIAL_SLICE_HEADER;
      }

      *this = *ctx->previous_slice_header;

      first_slice_segment_in_pic_flag = 0;
      dependent_slice_segment_flag = 1;
    }

    this->slice_segment_address = slice_segment_address;
  } else {
    dependent_slice_segment_flag = 0;
    slice_segment_address = 0;
  }

  if (slice_segment_address < 0 ||
      slice_segment_address >= sps->PicSizeInCtbsY) {
    ctx->add_warning(DE265_WARNING_SLICE_SEGMENT_ADDRESS_INVALID, false);
    return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE;
  }

  //printf("SLICE %d (%d)\n",slice_segment_address, sps->PicSizeInCtbsY);


  if (!dependent_slice_segment_flag) {
    for (int i=0; i<pps->num_extra_slice_header_bits; i++) {
      //slice_reserved_undetermined_flag[i]
      skip_bits(br,1);
    }

    slice_type = get_uvlc(br);
    if (slice_type > 2 ||
	slice_type == UVLC_ERROR) {
      ctx->add_warning(DE265_WARNING_SLICEHEADER_INVALID, false);
      *continueDecoding = false;
      return DE265_OK;
    }

    if (pps->output_flag_present_flag) {
      pic_output_flag = get_bits(br,1);
    }
    else {
      pic_output_flag = 1;
    }

    if (sps->separate_colour_plane_flag == 1) {
      colour_plane_id = get_bits(br,2);
    }


    slice_pic_order_cnt_lsb = 0;
    short_term_ref_pic_set_sps_flag = 0;

    int NumLtPics = 0;

    if (ctx->get_nal_unit_type() != NAL_UNIT_IDR_W_RADL &&
        ctx->get_nal_unit_type() != NAL_UNIT_IDR_N_LP) {
      slice_pic_order_cnt_lsb = get_bits(br, sps->log2_max_pic_order_cnt_lsb);
      short_term_ref_pic_set_sps_flag = get_bits(br,1);

      if (!short_term_ref_pic_set_sps_flag) {
        read_short_term_ref_pic_set(ctx, sps,
                                    br, &slice_ref_pic_set,
                                    sps->num_short_term_ref_pic_sets(),
                                    sps->ref_pic_sets,
                                    true);

        CurrRpsIdx = sps->num_short_term_ref_pic_sets();
        CurrRps    = slice_ref_pic_set;
      }
      else {
        int nBits = ceil_log2(sps->num_short_term_ref_pic_sets());
        if (nBits>0) short_term_ref_pic_set_idx = get_bits(br,nBits);
        else         short_term_ref_pic_set_idx = 0;

        if (short_term_ref_pic_set_idx >= sps->num_short_term_ref_pic_sets()) {
          ctx->add_warning(DE265_WARNING_SHORT_TERM_REF_PIC_SET_OUT_OF_RANGE, false);
          return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE;
        }

        CurrRpsIdx = short_term_ref_pic_set_idx;
        CurrRps    = sps->ref_pic_sets[CurrRpsIdx];
      }


      // --- long-term MC ---

      if (sps->long_term_ref_pics_present_flag) {
        if (sps->num_long_term_ref_pics_sps > 0) {
          num_long_term_sps = get_uvlc(br);
          if (num_long_term_sps == UVLC_ERROR) {
            return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE;
          }
        }
        else {
          num_long_term_sps = 0;
        }

        num_long_term_pics= get_uvlc(br);
        if (num_long_term_pics == UVLC_ERROR) {
          return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE;
        }

        // check maximum number of reference frames

        if (num_long_term_sps +
            num_long_term_pics +
            CurrRps.NumNegativePics +
            CurrRps.NumPositivePics
            > sps->sps_max_dec_pic_buffering[sps->sps_max_sub_layers-1])
          {
            ctx->add_warning(DE265_WARNING_MAX_NUM_REF_PICS_EXCEEDED, false);
            *continueDecoding = false;
            return DE265_OK;
          }

        for (int i=0; i<num_long_term_sps + num_long_term_pics; i++) {
          if (i < num_long_term_sps) {
            int nBits = ceil_log2(sps->num_long_term_ref_pics_sps);
            lt_idx_sps[i] = get_bits(br, nBits);

            // check that the referenced lt-reference really exists

            if (lt_idx_sps[i] >= sps->num_long_term_ref_pics_sps) {
              ctx->add_warning(DE265_NON_EXISTING_LT_REFERENCE_CANDIDATE_IN_SLICE_HEADER, false);
              *continueDecoding = false;
              return DE265_OK;
            }

            // delta_poc_msb_present_flag[i] = 0; // TODO ?

            ctx->PocLsbLt[i] = sps->lt_ref_pic_poc_lsb_sps[ lt_idx_sps[i] ];
            ctx->UsedByCurrPicLt[i] = sps->used_by_curr_pic_lt_sps_flag[ lt_idx_sps[i] ];
          }
          else {
            int nBits = sps->log2_max_pic_order_cnt_lsb;
            poc_lsb_lt[i] = get_bits(br, nBits);
            used_by_curr_pic_lt_flag[i] = get_bits(br,1);

            ctx->PocLsbLt[i] = poc_lsb_lt[i];
            ctx->UsedByCurrPicLt[i] = used_by_curr_pic_lt_flag[i];
          }

          if (ctx->UsedByCurrPicLt[i]) {
            NumLtPics++;
          }

          delta_poc_msb_present_flag[i] = get_bits(br,1);
          if (delta_poc_msb_present_flag[i]) {
            delta_poc_msb_cycle_lt[i] = get_uvlc(br);
            if (delta_poc_msb_cycle_lt[i]==UVLC_ERROR) {
              return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE;
            }
          }
          else {
            delta_poc_msb_cycle_lt[i] = 0;
          }

          if (i==0 || i==num_long_term_sps) {
            ctx->DeltaPocMsbCycleLt[i] = delta_poc_msb_cycle_lt[i];
          }
          else {
            ctx->DeltaPocMsbCycleLt[i] = (delta_poc_msb_cycle_lt[i] +
                                          ctx->DeltaPocMsbCycleLt[i-1]);
          }
        }
      }
      else {
        num_long_term_sps = 0;
        num_long_term_pics= 0;
      }

      if (sps->sps_temporal_mvp_enabled_flag) {
        slice_temporal_mvp_enabled_flag = get_bits(br,1);
      }
      else {
        slice_temporal_mvp_enabled_flag = 0;
      }
    }
    else {
      slice_pic_order_cnt_lsb = 0;
      num_long_term_sps = 0;
      num_long_term_pics= 0;
    }


    // --- SAO ---

    if (sps->sample_adaptive_offset_enabled_flag) {
      slice_sao_luma_flag   = get_bits(br,1);

      if (sps->ChromaArrayType != CHROMA_MONO) {
        slice_sao_chroma_flag = get_bits(br,1);
      }
      else {
        slice_sao_chroma_flag = 0;
      }
    }
    else {
      slice_sao_luma_flag   = 0;
      slice_sao_chroma_flag = 0;
    }

    num_ref_idx_l0_active = 0;
    num_ref_idx_l1_active = 0;

    if (slice_type == SLICE_TYPE_P  ||
        slice_type == SLICE_TYPE_B) {
      num_ref_idx_active_override_flag = get_bits(br,1);
      if (num_ref_idx_active_override_flag) {
        num_ref_idx_l0_active = get_uvlc(br);
        if (num_ref_idx_l0_active == UVLC_ERROR) {
	  ctx->add_warning(DE265_WARNING_SLICEHEADER_INVALID, false);
          return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE;
	}
        num_ref_idx_l0_active++;;

        if (slice_type == SLICE_TYPE_B) {
          num_ref_idx_l1_active = get_uvlc(br);
          if (num_ref_idx_l1_active == UVLC_ERROR) {
	    ctx->add_warning(DE265_WARNING_SLICEHEADER_INVALID, false);
	    return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE;
	  }
          num_ref_idx_l1_active++;
        }
      }
      else {
        num_ref_idx_l0_active = pps->num_ref_idx_l0_default_active;
        num_ref_idx_l1_active = pps->num_ref_idx_l1_default_active;
      }

      if (num_ref_idx_l0_active > 16) { return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE; }
      if (num_ref_idx_l1_active > 16) { return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE; }

      NumPocTotalCurr = CurrRps.NumPocTotalCurr_shortterm_only + NumLtPics;

      if (pps->lists_modification_present_flag && NumPocTotalCurr > 1) {

        int nBits = ceil_log2(NumPocTotalCurr);

        ref_pic_list_modification_flag_l0 = get_bits(br,1);
        if (ref_pic_list_modification_flag_l0) {
          for (int i=0;i<num_ref_idx_l0_active;i++) {
            list_entry_l0[i] = get_bits(br, nBits);
          }
        }

        if (slice_type == SLICE_TYPE_B) {
          ref_pic_list_modification_flag_l1 = get_bits(br,1);
          if (ref_pic_list_modification_flag_l1) {
            for (int i=0;i<num_ref_idx_l1_active;i++) {
              list_entry_l1[i] = get_bits(br, nBits);
            }
          }
        }
        else {
          ref_pic_list_modification_flag_l1 = 0;
        }
      }
      else {
        ref_pic_list_modification_flag_l0 = 0;
        ref_pic_list_modification_flag_l1 = 0;
      }

      if (slice_type == SLICE_TYPE_B) {
        mvd_l1_zero_flag = get_bits(br,1);
      }

      if (pps->cabac_init_present_flag) {
        cabac_init_flag = get_bits(br,1);
      }
      else {
        cabac_init_flag = 0;
      }

      if (slice_temporal_mvp_enabled_flag) {
        if (slice_type == SLICE_TYPE_B)
          collocated_from_l0_flag = get_bits(br,1);
        else
          collocated_from_l0_flag = 1;

        if (( collocated_from_l0_flag && num_ref_idx_l0_active > 1) ||
            (!collocated_from_l0_flag && num_ref_idx_l1_active > 1)) {
          collocated_ref_idx = get_uvlc(br);
          if (collocated_ref_idx == UVLC_ERROR) {
	    ctx->add_warning(DE265_WARNING_SLICEHEADER_INVALID, false);
	    return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE;
	  }
        }
        else {
          collocated_ref_idx = 0;
        }

        // check whether collocated_ref_idx points to a valid index

        if (( collocated_from_l0_flag && collocated_ref_idx >= num_ref_idx_l0_active) ||
            (!collocated_from_l0_flag && collocated_ref_idx >= num_ref_idx_l1_active)) {
          ctx->add_warning(DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE, false);
          return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE;
        }
      }


      if ((pps->weighted_pred_flag   && slice_type == SLICE_TYPE_P) ||
          (pps->weighted_bipred_flag && slice_type == SLICE_TYPE_B)) {

        if (!read_pred_weight_table(br,this,ctx))
          {
	    ctx->add_warning(DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE, false);
	    return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE;
          }
      }

      five_minus_max_num_merge_cand = get_uvlc(br);
      if (five_minus_max_num_merge_cand == UVLC_ERROR) {
	ctx->add_warning(DE265_WARNING_SLICEHEADER_INVALID, false);
	return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE;
      }
      MaxNumMergeCand = 5-five_minus_max_num_merge_cand;
    }

    slice_qp_delta = get_svlc(br);
    if (slice_qp_delta == UVLC_ERROR) {
      ctx->add_warning(DE265_WARNING_SLICEHEADER_INVALID, false);
      return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE;
    }
    //logtrace(LogSlice,"slice_qp_delta: %d\n",shdr->slice_qp_delta);

    if (pps->pps_slice_chroma_qp_offsets_present_flag) {
      slice_cb_qp_offset = get_svlc(br);
      if (slice_cb_qp_offset == UVLC_ERROR) {
	ctx->add_warning(DE265_WARNING_SLICEHEADER_INVALID, false);
	return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE;
      }

      slice_cr_qp_offset = get_svlc(br);
      if (slice_cr_qp_offset == UVLC_ERROR) {
	ctx->add_warning(DE265_WARNING_SLICEHEADER_INVALID, false);
	return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE;
      }
    }
    else {
      slice_cb_qp_offset = 0;
      slice_cr_qp_offset = 0;
    }

    if (pps->range_extension.chroma_qp_offset_list_enabled_flag) {
      cu_chroma_qp_offset_enabled_flag = get_bits(br,1);
    }

    if (pps->deblocking_filter_override_enabled_flag) {
      deblocking_filter_override_flag = get_bits(br,1);
    }
    else {
      deblocking_filter_override_flag = 0;
    }

    slice_beta_offset = pps->beta_offset;
    slice_tc_offset   = pps->tc_offset;

    if (deblocking_filter_override_flag) {
      slice_deblocking_filter_disabled_flag = get_bits(br,1);
      if (!slice_deblocking_filter_disabled_flag) {
        slice_beta_offset = get_svlc(br);
        if (slice_beta_offset == UVLC_ERROR) {
	  ctx->add_warning(DE265_WARNING_SLICEHEADER_INVALID, false);
	  return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE;
	}
        slice_beta_offset *= 2;

        slice_tc_offset   = get_svlc(br);
        if (slice_tc_offset == UVLC_ERROR) {
	  ctx->add_warning(DE265_WARNING_SLICEHEADER_INVALID, false);
	  return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE;
	}
        slice_tc_offset   *= 2;
      }
    }
    else {
      slice_deblocking_filter_disabled_flag = pps->pic_disable_deblocking_filter_flag;
    }

    if (pps->pps_loop_filter_across_slices_enabled_flag  &&
        (slice_sao_luma_flag || slice_sao_chroma_flag ||
         !slice_deblocking_filter_disabled_flag )) {
      slice_loop_filter_across_slices_enabled_flag = get_bits(br,1);
    }
    else {
      slice_loop_filter_across_slices_enabled_flag =
        pps->pps_loop_filter_across_slices_enabled_flag;
    }
  }

  if (pps->tiles_enabled_flag || pps->entropy_coding_sync_enabled_flag ) {
    num_entry_point_offsets = get_uvlc(br);
    if (num_entry_point_offsets == UVLC_ERROR) {
      ctx->add_warning(DE265_WARNING_SLICEHEADER_INVALID, false);
      return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE;
    }

    if (pps->entropy_coding_sync_enabled_flag) {
      // check num_entry_points for valid range

      int firstCTBRow = slice_segment_address / sps->PicWidthInCtbsY;
      int lastCTBRow  = firstCTBRow + num_entry_point_offsets;
      if (lastCTBRow >= sps->PicHeightInCtbsY) {
        ctx->add_warning(DE265_WARNING_SLICEHEADER_INVALID, false);
        return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE;
      }
    }

    if (pps->tiles_enabled_flag) {
      if (num_entry_point_offsets > pps->num_tile_columns * pps->num_tile_rows) {
        ctx->add_warning(DE265_WARNING_SLICEHEADER_INVALID, false);
        return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE;
      }
    }

    entry_point_offset.resize( num_entry_point_offsets );

    if (num_entry_point_offsets > 0) {
      offset_len = get_uvlc(br);
      if (offset_len == UVLC_ERROR) {
	ctx->add_warning(DE265_WARNING_SLICEHEADER_INVALID, false);
	return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE;
      }
      offset_len++;

      if (offset_len > 32) {
	return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE;
      }

      for (int i=0; i<num_entry_point_offsets; i++) {
        {
          entry_point_offset[i] = get_bits(br,offset_len)+1;
        }

        if (i>0) {
          entry_point_offset[i] += entry_point_offset[i-1];
        }
      }
    }
  }
  else {
    num_entry_point_offsets = 0;
  }

  if (pps->slice_segment_header_extension_present_flag) {
    slice_segment_header_extension_length = get_uvlc(br);
    if (slice_segment_header_extension_length == UVLC_ERROR ||
	slice_segment_header_extension_length > 1000) {  // TODO: safety check against too large values
      ctx->add_warning(DE265_WARNING_SLICEHEADER_INVALID, false);
      return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE;
    }

    for (int i=0; i<slice_segment_header_extension_length; i++) {
      //slice_segment_header_extension_data_byte[i]
      get_bits(br,8);
    }
  }


  compute_derived_values(pps.get());

  *continueDecoding = true;
  return DE265_OK;
}


de265_error slice_segment_header::write(error_queue* errqueue, CABAC_encoder& out,
                                        const seq_parameter_set* sps,
                                        const pic_parameter_set* pps,
                                        uint8_t nal_unit_type)
{
  out.write_bit(first_slice_segment_in_pic_flag);

  if (isRapPic(nal_unit_type)) { // TODO: is this still correct ? Should we drop RapPicFlag ?
    out.write_bit(no_output_of_prior_pics_flag);
  }

  if (slice_pic_parameter_set_id > DE265_MAX_PPS_SETS) {
    errqueue->add_warning(DE265_WARNING_NONEXISTING_PPS_REFERENCED, false);
    return DE265_OK;
  }
  out.write_uvlc(slice_pic_parameter_set_id);

  if (!first_slice_segment_in_pic_flag) {
    if (pps->dependent_slice_segments_enabled_flag) {
      out.write_bit(dependent_slice_segment_flag);
    }

    out.write_bits(slice_segment_address, ceil_log2(sps->PicSizeInCtbsY));

    if (dependent_slice_segment_flag) {
      if (slice_segment_address == 0) {
        errqueue->add_warning(DE265_WARNING_DEPENDENT_SLICE_WITH_ADDRESS_ZERO, false);
        return DE265_OK;
      }
    }
  }

  if (slice_segment_address < 0 ||
      slice_segment_address > sps->PicSizeInCtbsY) {
    errqueue->add_warning(DE265_WARNING_SLICE_SEGMENT_ADDRESS_INVALID, false);
    return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE;
  }



  if (!dependent_slice_segment_flag) {
    for (int i=0; i<pps->num_extra_slice_header_bits; i++) {
      //slice_reserved_undetermined_flag[i]
      out.skip_bits(1);
    }

    if (slice_type > 2) {
      errqueue->add_warning(DE265_WARNING_SLICEHEADER_INVALID, false);
      return DE265_OK;
    }
    out.write_uvlc(slice_type);

    if (pps->output_flag_present_flag) {
      out.write_bit(pic_output_flag);
    }

    if (sps->separate_colour_plane_flag == 1) {
      out.write_bits(colour_plane_id,2);
    }


    int NumLtPics = 0;

    if (nal_unit_type != NAL_UNIT_IDR_W_RADL &&
        nal_unit_type != NAL_UNIT_IDR_N_LP) {
      out.write_bits(slice_pic_order_cnt_lsb, sps->log2_max_pic_order_cnt_lsb);
      out.write_bit(short_term_ref_pic_set_sps_flag);

      if (!short_term_ref_pic_set_sps_flag) {
        /* TODO
        read_short_term_ref_pic_set(ctx, sps,
                                    br, &slice_ref_pic_set,
                                    sps->num_short_term_ref_pic_sets,
                                    sps->ref_pic_sets,
                                    true);
        */
        //CurrRpsIdx = sps->num_short_term_ref_pic_sets;
        //CurrRps    = slice_ref_pic_set;
      }
      else {
        int nBits = ceil_log2(sps->num_short_term_ref_pic_sets());
        if (nBits>0) out.write_bits(short_term_ref_pic_set_idx,nBits);
        else         { assert(short_term_ref_pic_set_idx==0); }

        if (short_term_ref_pic_set_idx > sps->num_short_term_ref_pic_sets()) {
          errqueue->add_warning(DE265_WARNING_SHORT_TERM_REF_PIC_SET_OUT_OF_RANGE, false);
          return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE;
        }

        //CurrRpsIdx = short_term_ref_pic_set_idx;
        //CurrRps    = sps->ref_pic_sets[CurrRpsIdx];
      }


      // --- long-term MC ---

      if (sps->long_term_ref_pics_present_flag) {
        if (sps->num_long_term_ref_pics_sps > 0) {
          out.write_uvlc(num_long_term_sps);
        }
        else {
          assert(num_long_term_sps == 0);
        }

        out.write_uvlc(num_long_term_pics);


        // check maximum number of reference frames

        if (num_long_term_sps +
            num_long_term_pics +
            CurrRps.NumNegativePics +
            CurrRps.NumPositivePics
            > sps->sps_max_dec_pic_buffering[sps->sps_max_sub_layers-1])
          {
            errqueue->add_warning(DE265_WARNING_MAX_NUM_REF_PICS_EXCEEDED, false);
            return DE265_OK;
          }

        for (int i=0; i<num_long_term_sps + num_long_term_pics; i++) {
          if (i < num_long_term_sps) {
            int nBits = ceil_log2(sps->num_long_term_ref_pics_sps);
            out.write_bits(lt_idx_sps[i], nBits);

            // check that the referenced lt-reference really exists

            if (lt_idx_sps[i] >= sps->num_long_term_ref_pics_sps) {
              errqueue->add_warning(DE265_NON_EXISTING_LT_REFERENCE_CANDIDATE_IN_SLICE_HEADER, false);
              return DE265_OK;
            }

            //ctx->PocLsbLt[i] = sps->lt_ref_pic_poc_lsb_sps[ lt_idx_sps[i] ];
            //ctx->UsedByCurrPicLt[i] = sps->used_by_curr_pic_lt_sps_flag[ lt_idx_sps[i] ];
          }
          else {
            int nBits = sps->log2_max_pic_order_cnt_lsb;
            out.write_bits(poc_lsb_lt[i], nBits);
            out.write_bit(used_by_curr_pic_lt_flag[i]);

            //ctx->PocLsbLt[i] = poc_lsb_lt[i];
            //ctx->UsedByCurrPicLt[i] = used_by_curr_pic_lt_flag[i];
          }

          //if (ctx->UsedByCurrPicLt[i]) {
          //NumLtPics++;
          //}

          out.write_bit(delta_poc_msb_present_flag[i]);
          if (delta_poc_msb_present_flag[i]) {
            out.write_uvlc(delta_poc_msb_cycle_lt[i]);
          }
          else {
            assert(delta_poc_msb_cycle_lt[i] == 0);
          }

          /*
          if (i==0 || i==num_long_term_sps) {
            ctx->DeltaPocMsbCycleLt[i] = delta_poc_msb_cycle_lt[i];
          }
          else {
            ctx->DeltaPocMsbCycleLt[i] = (delta_poc_msb_cycle_lt[i] +
                                          ctx->DeltaPocMsbCycleLt[i-1]);
          }
          */
        }
      }
      else {
        assert(num_long_term_sps == 0);
        assert(num_long_term_pics== 0);
      }

      if (sps->sps_temporal_mvp_enabled_flag) {
        out.write_bit(slice_temporal_mvp_enabled_flag);
      }
      else {
        assert(slice_temporal_mvp_enabled_flag == 0);
      }
    }
    else {
      assert(slice_pic_order_cnt_lsb == 0);
      assert(num_long_term_sps == 0);
      assert(num_long_term_pics== 0);
    }


    // --- SAO ---

    if (sps->sample_adaptive_offset_enabled_flag) {
      out.write_bit(slice_sao_luma_flag);
      out.write_bit(slice_sao_chroma_flag);
    }
    else {
      assert(slice_sao_luma_flag  == 0);
      assert(slice_sao_chroma_flag== 0);
    }

    if (slice_type == SLICE_TYPE_P  ||
        slice_type == SLICE_TYPE_B) {
      out.write_bit(num_ref_idx_active_override_flag);

      if (num_ref_idx_active_override_flag) {
        out.write_uvlc(num_ref_idx_l0_active);
        num_ref_idx_l0_active++;;

        if (slice_type == SLICE_TYPE_B) {
          out.write_uvlc(num_ref_idx_l1_active);
          num_ref_idx_l1_active++;
        }
      }
      else {
        assert(num_ref_idx_l0_active == pps->num_ref_idx_l0_default_active);
        assert(num_ref_idx_l1_active == pps->num_ref_idx_l1_default_active);
      }

      NumPocTotalCurr = CurrRps.NumPocTotalCurr_shortterm_only + NumLtPics;

      if (pps->lists_modification_present_flag && NumPocTotalCurr > 1) {

        int nBits = ceil_log2(NumPocTotalCurr);

        out.write_bit(ref_pic_list_modification_flag_l0);
        if (ref_pic_list_modification_flag_l0) {
          for (int i=0;i<num_ref_idx_l0_active;i++) {
            out.write_bits(list_entry_l0[i], nBits);
          }
        }

        if (slice_type == SLICE_TYPE_B) {
          out.write_bit(ref_pic_list_modification_flag_l1);
          if (ref_pic_list_modification_flag_l1) {
            for (int i=0;i<num_ref_idx_l1_active;i++) {
              out.write_bits(list_entry_l1[i], nBits);
            }
          }
        }
        else {
          assert(ref_pic_list_modification_flag_l1 == 0);
        }
      }
      else {
        assert(ref_pic_list_modification_flag_l0 == 0);
        assert(ref_pic_list_modification_flag_l1 == 0);
      }

      if (slice_type == SLICE_TYPE_B) {
        out.write_bit(mvd_l1_zero_flag);
      }

      if (pps->cabac_init_present_flag) {
        out.write_bit(cabac_init_flag);
      }
      else {
        assert(cabac_init_flag == 0);
      }

      if (slice_temporal_mvp_enabled_flag) {
        if (slice_type == SLICE_TYPE_B)
          out.write_bit(collocated_from_l0_flag);
        else
          { assert(collocated_from_l0_flag == 1); }

        if (( collocated_from_l0_flag && num_ref_idx_l0_active > 1) ||
            (!collocated_from_l0_flag && num_ref_idx_l1_active > 1)) {
          out.write_uvlc(collocated_ref_idx);
        }
        else {
          assert(collocated_ref_idx == 0);
        }
      }

      if ((pps->weighted_pred_flag   && slice_type == SLICE_TYPE_P) ||
          (pps->weighted_bipred_flag && slice_type == SLICE_TYPE_B)) {

        assert(0);
        /* TODO
        if (!read_pred_weight_table(br,this,ctx))
          {
	    ctx->add_warning(DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE, false);
	    return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE;
          }
        */
      }

      out.write_uvlc(five_minus_max_num_merge_cand);
      //MaxNumMergeCand = 5-five_minus_max_num_merge_cand;
    }

    out.write_svlc(slice_qp_delta);

    if (pps->pps_slice_chroma_qp_offsets_present_flag) {
      out.write_svlc(slice_cb_qp_offset);
      out.write_svlc(slice_cr_qp_offset);
    }
    else {
      assert(slice_cb_qp_offset == 0);
      assert(slice_cr_qp_offset == 0);
    }

    if (pps->deblocking_filter_override_enabled_flag) {
      out.write_bit(deblocking_filter_override_flag);
    }
    else {
      assert(deblocking_filter_override_flag == 0);
    }

    //slice_beta_offset = pps->beta_offset;
    //slice_tc_offset   = pps->tc_offset;

    if (deblocking_filter_override_flag) {
      out.write_bit(slice_deblocking_filter_disabled_flag);
      if (!slice_deblocking_filter_disabled_flag) {
        out.write_svlc(slice_beta_offset/2);
        out.write_svlc(slice_tc_offset  /2);
      }
    }
    else {
      assert(slice_deblocking_filter_disabled_flag == pps->pic_disable_deblocking_filter_flag);
    }

    if (pps->pps_loop_filter_across_slices_enabled_flag  &&
        (slice_sao_luma_flag || slice_sao_chroma_flag ||
         !slice_deblocking_filter_disabled_flag )) {
      out.write_bit(slice_loop_filter_across_slices_enabled_flag);
    }
    else {
      assert(slice_loop_filter_across_slices_enabled_flag ==
             pps->pps_loop_filter_across_slices_enabled_flag);
    }
  }

  if (pps->tiles_enabled_flag || pps->entropy_coding_sync_enabled_flag ) {
    out.write_uvlc(num_entry_point_offsets);

    if (num_entry_point_offsets > 0) {
      out.write_uvlc(offset_len-1);

      for (int i=0; i<num_entry_point_offsets; i++) {
        {
          int prev=0;
          if (i>0) prev = entry_point_offset[i-1];
          out.write_bits(entry_point_offset[i]-prev-1, offset_len);
        }
      }
    }
  }
  else {
    assert(num_entry_point_offsets == 0);
  }

  if (pps->slice_segment_header_extension_present_flag) {
    out.write_uvlc(slice_segment_header_extension_length);
    if (slice_segment_header_extension_length > 1000) {  // TODO: safety check against too large values
      errqueue->add_warning(DE265_WARNING_SLICEHEADER_INVALID, false);
      return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE;
    }

    for (int i=0; i<slice_segment_header_extension_length; i++) {
      //slice_segment_header_extension_data_byte[i]
      out.skip_bits(8);
    }
  }

  return DE265_OK;
}

void slice_segment_header::compute_derived_values(const pic_parameter_set* pps)
{
  // --- init variables ---

  SliceQPY = pps->pic_init_qp + slice_qp_delta;

  switch (slice_type)
    {
    case SLICE_TYPE_I: initType = 0; break;
    case SLICE_TYPE_P: initType = cabac_init_flag + 1; break;
    case SLICE_TYPE_B: initType = 2 - cabac_init_flag; break;
    }

  MaxNumMergeCand = 5-five_minus_max_num_merge_cand;
}


//-----------------------------------------------------------------------


void slice_segment_header::dump_slice_segment_header(const decoder_context* ctx, int fd) const
{
  FILE* fh;
  if (fd==1) fh=stdout;
  else if (fd==2) fh=stderr;
  else { return; }

#define LOG0(t) log2fh(fh, t)
#define LOG1(t,d) log2fh(fh, t,d)
#define LOG2(t,d1,d2) log2fh(fh, t,d1,d2)
#define LOG3(t,d1,d2,d3) log2fh(fh, t,d1,d2,d3)
#define LOG4(t,d1,d2,d3,d4) log2fh(fh, t,d1,d2,d3,d4)

  LOG0("----------------- SLICE -----------------\n");

  const pic_parameter_set* pps = ctx->get_pps(slice_pic_parameter_set_id);
  if (!pps) {
    LOG0("invalid PPS referenced\n");
    return;
  }
  assert(pps->pps_read); // TODO: error handling

  const seq_parameter_set* sps = ctx->get_sps((int)pps->seq_parameter_set_id);
  if (!sps) {
    LOG0("invalid SPS referenced\n");
    return;
  }
  assert(sps->sps_read); // TODO: error handling


  LOG1("first_slice_segment_in_pic_flag      : %d\n", first_slice_segment_in_pic_flag);
  if (ctx->get_nal_unit_type() >= NAL_UNIT_BLA_W_LP &&
      ctx->get_nal_unit_type() <= NAL_UNIT_RESERVED_IRAP_VCL23) {
    LOG1("no_output_of_prior_pics_flag         : %d\n", no_output_of_prior_pics_flag);
  }

  LOG1("slice_pic_parameter_set_id           : %d\n", slice_pic_parameter_set_id);

  if (!first_slice_segment_in_pic_flag) {
    //if (pps->dependent_slice_segments_enabled_flag) {
      LOG1("dependent_slice_segment_flag         : %d\n", dependent_slice_segment_flag);
      //}
    LOG1("slice_segment_address                : %d\n", slice_segment_address);
  }

  //if (!dependent_slice_segment_flag)
    {
    //for (int i=0; i<pps->num_extra_slice_header_bits; i++) {
    //slice_reserved_flag[i]

    LOG1("slice_type                           : %c\n",
         slice_type == 0 ? 'B' :
         slice_type == 1 ? 'P' : 'I');

    if (pps->output_flag_present_flag) {
      LOG1("pic_output_flag                      : %d\n", pic_output_flag);
    }

    if (sps->separate_colour_plane_flag == 1) {
      LOG1("colour_plane_id                      : %d\n", colour_plane_id);
    }

    LOG1("slice_pic_order_cnt_lsb              : %d\n", slice_pic_order_cnt_lsb);

    if (ctx->get_nal_unit_type() != NAL_UNIT_IDR_W_RADL &&
        ctx->get_nal_unit_type() != NAL_UNIT_IDR_N_LP) {
      LOG1("short_term_ref_pic_set_sps_flag      : %d\n", short_term_ref_pic_set_sps_flag);

      if (!short_term_ref_pic_set_sps_flag) {
        LOG1("ref_pic_set[ %2d ]: ",sps->num_short_term_ref_pic_sets());
        dump_compact_short_term_ref_pic_set(&slice_ref_pic_set, 16, fh);
      }
      else if (sps->num_short_term_ref_pic_sets() > 1) {
        LOG1("short_term_ref_pic_set_idx           : %d\n", short_term_ref_pic_set_idx);
        dump_compact_short_term_ref_pic_set(&sps->ref_pic_sets[short_term_ref_pic_set_idx], 16, fh);
      }

      if (sps->long_term_ref_pics_present_flag) {
        if (sps->num_long_term_ref_pics_sps > 0) {
          LOG1("num_long_term_sps                        : %d\n", num_long_term_sps);
        }

        LOG1("num_long_term_pics                       : %d\n", num_long_term_pics);

#if 0
        for (int i=0; i<num_long_term_sps + num_long_term_pics; i++) {
          LOG2("PocLsbLt[%d]            : %d\n", i, ctx->PocLsbLt[i]);
          LOG2("UsedByCurrPicLt[%d]     : %d\n", i, ctx->UsedByCurrPicLt[i]);
          LOG2("DeltaPocMsbCycleLt[%d]  : %d\n", i, ctx->DeltaPocMsbCycleLt[i]);
        }
#endif
      }

      if (sps->sps_temporal_mvp_enabled_flag) {
        LOG1("slice_temporal_mvp_enabled_flag : %d\n", slice_temporal_mvp_enabled_flag);
      }
    }


    if (sps->sample_adaptive_offset_enabled_flag) {
      LOG1("slice_sao_luma_flag             : %d\n", slice_sao_luma_flag);
      LOG1("slice_sao_chroma_flag           : %d\n", slice_sao_chroma_flag);
    }


    if (slice_type == SLICE_TYPE_P || slice_type == SLICE_TYPE_B) {
      LOG1("num_ref_idx_active_override_flag : %d\n", num_ref_idx_active_override_flag);

      LOG2("num_ref_idx_l0_active          : %d %s\n", num_ref_idx_l0_active,
           num_ref_idx_active_override_flag ? "" : "(from PPS)");

      if (slice_type == SLICE_TYPE_B) {
        LOG2("num_ref_idx_l1_active          : %d %s\n", num_ref_idx_l1_active,
             num_ref_idx_active_override_flag ? "" : "(from PPS)");
      }

      if (pps->lists_modification_present_flag && NumPocTotalCurr > 1)
        {
          LOG1("ref_pic_list_modification_flag_l0 : %d\n", ref_pic_list_modification_flag_l0);
          if (ref_pic_list_modification_flag_l0) {
            for (int i=0;i<num_ref_idx_l0_active;i++) {
              LOG2("  %d: %d\n",i,list_entry_l0[i]);
            }
          }

          LOG1("ref_pic_list_modification_flag_l1 : %d\n", ref_pic_list_modification_flag_l1);
          if (ref_pic_list_modification_flag_l1) {
            for (int i=0;i<num_ref_idx_l1_active;i++) {
              LOG2("  %d: %d\n",i,list_entry_l1[i]);
            }
          }
        }

      if (slice_type == SLICE_TYPE_B) {
        LOG1("mvd_l1_zero_flag               : %d\n", mvd_l1_zero_flag);
      }

      LOG1("cabac_init_flag                : %d\n", cabac_init_flag);

      if (slice_temporal_mvp_enabled_flag) {
        LOG1("collocated_from_l0_flag        : %d\n", collocated_from_l0_flag);
        LOG1("collocated_ref_idx             : %d\n", collocated_ref_idx);
      }

      if ((pps->weighted_pred_flag   && slice_type == SLICE_TYPE_P) ||
          (pps->weighted_bipred_flag && slice_type == SLICE_TYPE_B))
        {
          LOG1("luma_log2_weight_denom         : %d\n", luma_log2_weight_denom);
          if (sps->chroma_format_idc != 0) {
            LOG1("ChromaLog2WeightDenom          : %d\n", ChromaLog2WeightDenom);
          }

          for (int l=0;l<=1;l++)
            if (l==0 || (l==1 && slice_type == SLICE_TYPE_B))
              {
                int num_ref = (l==0 ?
                               num_ref_idx_l0_active-1 :
                               num_ref_idx_l1_active-1);

                if (false) { // do not show these flags
                  for (int i=0;i<=num_ref;i++) {
                    LOG3("luma_weight_flag_l%d[%d]        : %d\n",l,i,luma_weight_flag[l][i]);
                  }

                  if (sps->chroma_format_idc != 0) {
                    for (int i=0;i<=num_ref;i++) {
                      LOG3("chroma_weight_flag_l%d[%d]      : %d\n",l,i,chroma_weight_flag[l][i]);
                    }
                  }
                }

                for (int i=0;i<=num_ref;i++) {
                  LOG3("LumaWeight_L%d[%d]             : %d\n",l,i,LumaWeight[l][i]);
                  LOG3("luma_offset_l%d[%d]            : %d\n",l,i,luma_offset[l][i]);

                  for (int j=0;j<2;j++) {
                    LOG4("ChromaWeight_L%d[%d][%d]        : %d\n",l,i,j,ChromaWeight[l][i][j]);
                    LOG4("ChromaOffset_L%d[%d][%d]        : %d\n",l,i,j,ChromaOffset[l][i][j]);
                  }
                }
              }
        }

      LOG1("five_minus_max_num_merge_cand  : %d\n", five_minus_max_num_merge_cand);
    }


    LOG1("slice_qp_delta         : %d\n", slice_qp_delta);
    if (pps->pps_slice_chroma_qp_offsets_present_flag) {
      LOG1("slice_cb_qp_offset     : %d\n", slice_cb_qp_offset);
      LOG1("slice_cr_qp_offset     : %d\n", slice_cr_qp_offset);
    }

    if (pps->deblocking_filter_override_enabled_flag) {
      LOG1("deblocking_filter_override_flag : %d\n", deblocking_filter_override_flag);
    }

    LOG2("slice_deblocking_filter_disabled_flag : %d %s\n",
         slice_deblocking_filter_disabled_flag,
         (deblocking_filter_override_flag ? "(override)" : "(from pps)"));

    if (deblocking_filter_override_flag) {

      if (!slice_deblocking_filter_disabled_flag) {
        LOG1("slice_beta_offset  : %d\n", slice_beta_offset);
        LOG1("slice_tc_offset    : %d\n", slice_tc_offset);
      }
    }

    if (pps->pps_loop_filter_across_slices_enabled_flag  &&
        (slice_sao_luma_flag || slice_sao_chroma_flag ||
         !slice_deblocking_filter_disabled_flag)) {
      LOG1("slice_loop_filter_across_slices_enabled_flag : %d\n",
           slice_loop_filter_across_slices_enabled_flag);
    }
  }

  if (pps->tiles_enabled_flag || pps->entropy_coding_sync_enabled_flag) {
    LOG1("num_entry_point_offsets    : %d\n", num_entry_point_offsets);

    if (num_entry_point_offsets > 0) {
      LOG1("offset_len                 : %d\n", offset_len);

      for (int i=0; i<num_entry_point_offsets; i++) {
        LOG2("entry point [%i] : %d\n", i, entry_point_offset[i]);
      }
    }
  }

  /*
    if( slice_segment_header_extension_present_flag ) {
    slice_segment_header_extension_length
    for( i = 0; i < slice_segment_header_extension_length; i++)
    slice_segment_header_extension_data_byte[i]
    }
    byte_alignment()
    }
  */

#undef LOG0
#undef LOG1
#undef LOG2
#undef LOG3
#undef LOG4
  //#endif
}



void initialize_CABAC_models(thread_context* tctx)
{
  const int QPY = tctx->shdr->SliceQPY;
  const int initType = tctx->shdr->initType;
  assert(initType >= 0 && initType <= 2);

  tctx->ctx_model.init(initType, QPY);

  for (int i=0;i<4;i++) {
    tctx->StatCoeff[i] = 0;
  }
}



static int decode_transform_skip_flag(thread_context* tctx, int cIdx)
{
  const int context = (cIdx==0) ? 0 : 1;

  logtrace(LogSlice,"# transform_skip_flag (context=%d)\n",context);

  int bit = decode_CABAC_bit(&tctx->cabac_decoder,
                             &tctx->ctx_model[CONTEXT_MODEL_TRANSFORM_SKIP_FLAG+context]);

  logtrace(LogSymbols,"$1 transform_skip_flag=%d\n",bit);

  return bit;
}


static int decode_sao_merge_flag(thread_context* tctx)
{
  logtrace(LogSlice,"# sao_merge_left/up_flag\n");
  int bit = decode_CABAC_bit(&tctx->cabac_decoder,
                             &tctx->ctx_model[CONTEXT_MODEL_SAO_MERGE_FLAG]);

  logtrace(LogSymbols,"$1 sao_merge_flag=%d\n",bit);

  return bit;
}



static int decode_sao_type_idx(thread_context* tctx)
{
  logtrace(LogSlice,"# sao_type_idx_luma/chroma\n");

  int bit0 = decode_CABAC_bit(&tctx->cabac_decoder,
                              &tctx->ctx_model[CONTEXT_MODEL_SAO_TYPE_IDX]);

  if (bit0==0) {
    logtrace(LogSymbols,"$1 sao_type_idx=%d\n",0);
    return 0;
  }
  else {
    int bit1 = decode_CABAC_bypass(&tctx->cabac_decoder);
    if (bit1==0) {
      logtrace(LogSymbols,"$1 sao_type_idx=%d\n",1);
      return 1;
    }
    else {
      logtrace(LogSymbols,"$1 sao_type_idx=%d\n",2);
      return 2;
    }
  }
}


static int decode_sao_offset_abs(thread_context* tctx, int bitDepth)
{
  logtrace(LogSlice,"# sao_offset_abs\n");
  int cMax = (1<<(libde265_min(bitDepth,10)-5))-1;
  int value = decode_CABAC_TU_bypass(&tctx->cabac_decoder, cMax);
  logtrace(LogSymbols,"$1 sao_offset_abs=%d\n",value);
  return value;
}


static int decode_sao_class(thread_context* tctx)
{
  logtrace(LogSlice,"# sao_class\n");
  int value = decode_CABAC_FL_bypass(&tctx->cabac_decoder, 2);
  logtrace(LogSymbols,"$1 sao_class=%d\n",value);
  return value;
}


static int decode_sao_offset_sign(thread_context* tctx)
{
  logtrace(LogSlice,"# sao_offset_sign\n");
  int value = decode_CABAC_bypass(&tctx->cabac_decoder);
  logtrace(LogSymbols,"$1 sao_offset_sign=%d\n",value);
  return value;
}


static int decode_sao_band_position(thread_context* tctx)
{
  logtrace(LogSlice,"# sao_band_position\n");
  int value = decode_CABAC_FL_bypass(&tctx->cabac_decoder,5);
  logtrace(LogSymbols,"$1 sao_band_position=%d\n",value);
  return value;
}


static int decode_transquant_bypass_flag(thread_context* tctx)
{
  logtrace(LogSlice,"# cu_transquant_bypass_enable_flag\n");
  int value = decode_CABAC_bit(&tctx->cabac_decoder,
                               &tctx->ctx_model[CONTEXT_MODEL_CU_TRANSQUANT_BYPASS_FLAG]);
  logtrace(LogSymbols,"$1 transquant_bypass_flag=%d\n",value);
  return value;
}


#include <sys/types.h>
#include <signal.h>

static int decode_split_cu_flag(thread_context* tctx,
				int x0, int y0, int ctDepth)
{
  // check if neighbors are available

  int availableL = check_CTB_available(tctx->img, x0,y0, x0-1,y0);
  int availableA = check_CTB_available(tctx->img, x0,y0, x0,y0-1);

  int condL = 0;
  int condA = 0;

  if (availableL && tctx->img->get_ctDepth(x0-1,y0) > ctDepth) condL=1;
  if (availableA && tctx->img->get_ctDepth(x0,y0-1) > ctDepth) condA=1;

  int contextOffset = condL + condA;
  int context = contextOffset;

  // decode bit

  logtrace(LogSlice,"# split_cu_flag context=%d R=%x\n", context, tctx->cabac_decoder.range);

  int bit = decode_CABAC_bit(&tctx->cabac_decoder, &tctx->ctx_model[CONTEXT_MODEL_SPLIT_CU_FLAG + context]);

  logtrace(LogSlice,"> split_cu_flag R=%x, ctx=%d, bit=%d\n", tctx->cabac_decoder.range,context,bit);

  logtrace(LogSymbols,"$1 split_cu_flag=%d\n",bit);

  return bit;
}


static int decode_cu_skip_flag(thread_context* tctx,
			       int x0, int y0, int ctDepth)
{
  decoder_context* ctx = tctx->decctx;

  // check if neighbors are available

  int availableL = check_CTB_available(tctx->img, x0,y0, x0-1,y0);
  int availableA = check_CTB_available(tctx->img, x0,y0, x0,y0-1);

  int condL = 0;
  int condA = 0;

  if (availableL && tctx->img->get_cu_skip_flag(x0-1,y0)) condL=1;
  if (availableA && tctx->img->get_cu_skip_flag(x0,y0-1)) condA=1;

  int contextOffset = condL + condA;
  int context = contextOffset;

  // decode bit

  logtrace(LogSlice,"# cu_skip_flag context=%d R=%x\n", context, tctx->cabac_decoder.range);

  int bit = decode_CABAC_bit(&tctx->cabac_decoder, &tctx->ctx_model[CONTEXT_MODEL_CU_SKIP_FLAG + context]);

  logtrace(LogSlice,"> cu_skip_flag R=%x, ctx=%d, bit=%d\n", tctx->cabac_decoder.range,context,bit);

  logtrace(LogSymbols,"$1 cu_skip_flag=%d\n",bit);

  return bit;
}


static enum PartMode decode_part_mode(thread_context* tctx,
				      enum PredMode pred_mode, int cLog2CbSize)
{
  de265_image* img = tctx->img;

  if (pred_mode == MODE_INTRA) {
    logtrace(LogSlice,"# part_mode (INTRA)\n");

    int bit = decode_CABAC_bit(&tctx->cabac_decoder, &tctx->ctx_model[CONTEXT_MODEL_PART_MODE]);

    logtrace(LogSlice,"> %s\n",bit ? "2Nx2N" : "NxN");

    logtrace(LogSymbols,"$1 part_mode=%d\n",bit ? PART_2Nx2N : PART_NxN);

    return bit ? PART_2Nx2N : PART_NxN;
  }
  else {
    const seq_parameter_set& sps = img->get_sps();

    int bit0 = decode_CABAC_bit(&tctx->cabac_decoder, &tctx->ctx_model[CONTEXT_MODEL_PART_MODE+0]);
    if (bit0) { logtrace(LogSymbols,"$1 part_mode=%d\n",PART_2Nx2N); return PART_2Nx2N; }

    // CHECK_ME: I optimize code and fix bug here, need more VERIFY!
    int bit1 = decode_CABAC_bit(&tctx->cabac_decoder, &tctx->ctx_model[CONTEXT_MODEL_PART_MODE+1]);
    if (cLog2CbSize > sps.Log2MinCbSizeY) {
      if (!sps.amp_enabled_flag) {
        logtrace(LogSymbols,"$1 part_mode=%d\n",bit1 ? PART_2NxN : PART_Nx2N);
        return bit1 ? PART_2NxN : PART_Nx2N;
      }
      else {
        int bit3 = decode_CABAC_bit(&tctx->cabac_decoder, &tctx->ctx_model[CONTEXT_MODEL_PART_MODE+3]);
        if (bit3) {
          logtrace(LogSymbols,"$1 part_mode=%d\n",bit1 ? PART_2NxN : PART_Nx2N);
          return bit1 ? PART_2NxN : PART_Nx2N;
        }

        int bit4 = decode_CABAC_bypass(&tctx->cabac_decoder);
        if ( bit1 &&  bit4) {
          logtrace(LogSymbols,"$1 part_mode=%d\n",PART_2NxnD);
          return PART_2NxnD;
        }
        if ( bit1 && !bit4) {
          logtrace(LogSymbols,"$1 part_mode=%d\n",PART_2NxnU);
          return PART_2NxnU;
        }
        if (!bit1 && !bit4) {
          logtrace(LogSymbols,"$1 part_mode=%d\n",PART_nLx2N);
          return PART_nLx2N;
        }
        if (!bit1 &&  bit4) {
          logtrace(LogSymbols,"$1 part_mode=%d\n",PART_nRx2N);
          return PART_nRx2N;
        }
      }
    }
    else {
      // TODO, we could save one if here when first decoding the next bin and then
      // checkcLog2CbSize==3 when it is '0'

      if (bit1) {
        logtrace(LogSymbols,"$1 part_mode=%d\n",PART_2NxN);
        return PART_2NxN;
      }

      if (cLog2CbSize==3) {
        logtrace(LogSymbols,"$1 part_mode=%d\n",PART_Nx2N);
        return PART_Nx2N;
      }
      else {
        int bit2 = decode_CABAC_bit(&tctx->cabac_decoder, &tctx->ctx_model[CONTEXT_MODEL_PART_MODE+2]);
        logtrace(LogSymbols,"$1 part_mode=%d\n",PART_NxN-bit2);
        return (enum PartMode)((int)PART_NxN - bit2)/*bit2 ? PART_Nx2N : PART_NxN*/;
      }
    }
  }

  assert(false); // should never be reached
  return PART_2Nx2N;
}


static inline int decode_prev_intra_luma_pred_flag(thread_context* tctx)
{
  logtrace(LogSlice,"# prev_intra_luma_pred_flag\n");
  int bit = decode_CABAC_bit(&tctx->cabac_decoder, &tctx->ctx_model[CONTEXT_MODEL_PREV_INTRA_LUMA_PRED_FLAG]);
  logtrace(LogSymbols,"$1 prev_intra_luma_pred_flag=%d\n",bit);
  return bit;
}


static inline int decode_mpm_idx(thread_context* tctx)
{
  logtrace(LogSlice,"# mpm_idx (TU:2)\n");
  int mpm = decode_CABAC_TU_bypass(&tctx->cabac_decoder, 2);
  logtrace(LogSlice,"> mpm_idx = %d\n",mpm);
  logtrace(LogSymbols,"$1 mpm_idx=%d\n",mpm);
  return mpm;
}


static inline int decode_rem_intra_luma_pred_mode(thread_context* tctx)
{
  logtrace(LogSlice,"# rem_intra_luma_pred_mode (5 bits)\n");
  int value = decode_CABAC_FL_bypass(&tctx->cabac_decoder, 5);
  logtrace(LogSymbols,"$1 rem_intra_luma_pred_mode=%d\n",value);
  return value;
}


static int decode_intra_chroma_pred_mode(thread_context* tctx)
{
  logtrace(LogSlice,"# intra_chroma_pred_mode\n");

  int prefix = decode_CABAC_bit(&tctx->cabac_decoder, &tctx->ctx_model[CONTEXT_MODEL_INTRA_CHROMA_PRED_MODE]);

  int mode;
  if (prefix==0) {
    mode=4;
  }
  else {
    mode = decode_CABAC_FL_bypass(&tctx->cabac_decoder, 2);
  }

  logtrace(LogSlice,"> intra_chroma_pred_mode = %d\n",mode);
  logtrace(LogSymbols,"$1 intra_chroma_pred_mode=%d\n",mode);

  return mode;
}


static int decode_split_transform_flag(thread_context* tctx,
				       int log2TrafoSize)
{
  logtrace(LogSlice,"# split_transform_flag (log2TrafoSize=%d)\n",log2TrafoSize);

  int context = 5-log2TrafoSize;
  assert(context >= 0 && context <= 2);

  logtrace(LogSlice,"# context: %d\n",context);

  int bit = decode_CABAC_bit(&tctx->cabac_decoder, &tctx->ctx_model[CONTEXT_MODEL_SPLIT_TRANSFORM_FLAG + context]);
  logtrace(LogSymbols,"$1 split_transform_flag=%d\n",bit);
  return bit;
}


static int decode_cbf_chroma(thread_context* tctx,
			     int trafoDepth)
{
  logtrace(LogSlice,"# cbf_chroma\n");

  int bit = decode_CABAC_bit(&tctx->cabac_decoder, &tctx->ctx_model[CONTEXT_MODEL_CBF_CHROMA + trafoDepth]);

  logtrace(LogSymbols,"$1 cbf_chroma=%d\n",bit);
  return bit;
}


static int decode_cbf_luma(thread_context* tctx,
			   int trafoDepth)
{
  logtrace(LogSlice,"# cbf_luma\n");

  int bit = decode_CABAC_bit(&tctx->cabac_decoder, &tctx->ctx_model[CONTEXT_MODEL_CBF_LUMA + (trafoDepth==0)]);

  logtrace(LogSlice,"> cbf_luma = %d\n",bit);

  logtrace(LogSymbols,"$1 cbf_luma=%d\n",bit);
  return bit;
}


static inline int decode_coded_sub_block_flag(thread_context* tctx,
                                              int cIdx,
                                              uint8_t coded_sub_block_neighbors)
{
  logtrace(LogSlice,"# coded_sub_block_flag\n");

  // tricky computation of csbfCtx
  int csbfCtx = ((coded_sub_block_neighbors &  1) |  // right neighbor set  or
                 (coded_sub_block_neighbors >> 1));  // bottom neighbor set   -> csbfCtx=1

  int ctxIdxInc = csbfCtx;
  if (cIdx!=0) {
    ctxIdxInc += 2;
  }

  int bit = decode_CABAC_bit(&tctx->cabac_decoder,
                             &tctx->ctx_model[CONTEXT_MODEL_CODED_SUB_BLOCK_FLAG + ctxIdxInc]);

  logtrace(LogSymbols,"$1 coded_sub_block_flag=%d\n",bit);
  return bit;
}


static int decode_cu_qp_delta_abs(thread_context* tctx)
{
  logtrace(LogSlice,"# cu_qp_delta_abs\n");

  int bit = decode_CABAC_bit(&tctx->cabac_decoder,
                             &tctx->ctx_model[CONTEXT_MODEL_CU_QP_DELTA_ABS + 0]);
  if (bit==0) {
    logtrace(LogSymbols,"$1 cu_qp_delta_abs=%d\n",0);
    return 0;
  }

  int prefix=1;
  for (int i=0;i<4;i++) {
    bit = decode_CABAC_bit(&tctx->cabac_decoder,
                           &tctx->ctx_model[CONTEXT_MODEL_CU_QP_DELTA_ABS + 1]);
    if (bit==0) { break; }
    else { prefix++; }
  }

  if (prefix==5) {
    int value = decode_CABAC_EGk_bypass(&tctx->cabac_decoder, 0);
    logtrace(LogSymbols,"$1 cu_qp_delta_abs=%d\n",value+5);
    return value + 5;
  }
  else {
    logtrace(LogSymbols,"$1 cu_qp_delta_abs=%d\n",prefix);
    return prefix;
  }
}


static int decode_last_significant_coeff_prefix(thread_context* tctx,
						int log2TrafoSize,
						int cIdx,
						context_model* model)
{
  logtrace(LogSlice,"# last_significant_coeff_prefix log2TrafoSize:%d cIdx:%d\n",log2TrafoSize,cIdx);

  int cMax = (log2TrafoSize<<1)-1;

  int ctxOffset, ctxShift;
  if (cIdx==0) {
    ctxOffset = 3*(log2TrafoSize-2) + ((log2TrafoSize-1)>>2);
    ctxShift  = (log2TrafoSize+1)>>2;
  }
  else {
    ctxOffset = 15;
    ctxShift  = log2TrafoSize-2;
  }

  int binIdx;
  int value = cMax;
  for (binIdx=0;binIdx<cMax;binIdx++)
    {
      int ctxIdxInc = (binIdx >> ctxShift);

      logtrace(LogSlice,"context: %d+%d\n",ctxOffset,ctxIdxInc);

      int bit = decode_CABAC_bit(&tctx->cabac_decoder, &model[ctxOffset + ctxIdxInc]);
      if (bit==0) {
        value=binIdx;
        break;
      }
    }

  logtrace(LogSlice,"> last_significant_coeff_prefix: %d\n", value);

  return value;
}


static const uint8_t ctxIdxMap[16] = {
  0,1,4,5,
  2,3,4,5,
  6,6,8,8,
  7,7,8,99
};

uint8_t* ctxIdxLookup[4 /* 4-log2-32 */][2 /* !!cIdx */][2 /* !!scanIdx */][4 /* prevCsbf */];

bool alloc_and_init_significant_coeff_ctxIdx_lookupTable()
{
  int tableSize = 4*4*(2) + 8*8*(2*2*4) + 16*16*(2*4) + 32*32*(2*4);

  uint8_t* p = (uint8_t*)malloc(tableSize);
  if (p==NULL) {
    return false;
  }

  memset(p,0xFF,tableSize);  // just for debugging


  // --- Set pointers to memory areas. Note that some parameters share the same memory. ---

  // 4x4

  for (int cIdx=0;cIdx<2;cIdx++) {
    for (int scanIdx=0;scanIdx<2;scanIdx++)
      for (int prevCsbf=0;prevCsbf<4;prevCsbf++)
        ctxIdxLookup[0][cIdx][scanIdx][prevCsbf] = p;

    p += 4*4;
  }

  // 8x8

  for (int cIdx=0;cIdx<2;cIdx++)
    for (int scanIdx=0;scanIdx<2;scanIdx++)
      for (int prevCsbf=0;prevCsbf<4;prevCsbf++) {
        ctxIdxLookup[1][cIdx][scanIdx][prevCsbf] = p;
        p += 8*8;
      }

  // 16x16

  for (int cIdx=0;cIdx<2;cIdx++)
    for (int prevCsbf=0;prevCsbf<4;prevCsbf++) {
      for (int scanIdx=0;scanIdx<2;scanIdx++) {
        ctxIdxLookup[2][cIdx][scanIdx][prevCsbf] = p;
      }

      p += 16*16;
    }

  // 32x32

  for (int cIdx=0;cIdx<2;cIdx++)
    for (int prevCsbf=0;prevCsbf<4;prevCsbf++) {
      for (int scanIdx=0;scanIdx<2;scanIdx++) {
        ctxIdxLookup[3][cIdx][scanIdx][prevCsbf] = p;
      }

      p += 32*32;
    }


  // --- precompute ctxIdx tables ---

  for (int log2w=2; log2w<=5 ; log2w++)
    for (int cIdx=0;cIdx<2;cIdx++)
      for (int scanIdx=0;scanIdx<2;scanIdx++)
        for (int prevCsbf=0;prevCsbf<4;prevCsbf++)
          {
            for (int yC=0;yC<(1<<log2w);yC++)
              for (int xC=0;xC<(1<<log2w);xC++)
                {
                  int w = 1<<log2w;
                  int sbWidth = w>>2;

                  int sigCtx;

                  // if log2TrafoSize==2
                  if (sbWidth==1) {
                    sigCtx = ctxIdxMap[(yC<<2) + xC];
                  }
                  else if (xC+yC==0) {
                    sigCtx = 0;
                  }
                  else {
                    int xS = xC>>2;
                    int yS = yC>>2;
                    /*
                      int prevCsbf = 0;

                      if (xS < sbWidth-1) { prevCsbf += coded_sub_block_flag[xS+1  +yS*sbWidth];    }
                      if (yS < sbWidth-1) { prevCsbf += coded_sub_block_flag[xS+(1+yS)*sbWidth]<<1; }
                    */
                    int xP = xC & 3;
                    int yP = yC & 3;

                    //logtrace(LogSlice,"posInSubset: %d,%d\n",xP,yP);
                    //logtrace(LogSlice,"prevCsbf: %d\n",prevCsbf);

                    switch (prevCsbf) {
                    case 0:
                      sigCtx = (xP+yP>=3) ? 0 : (xP+yP>0) ? 1 : 2;
                      break;
                    case 1:
                      sigCtx = (yP==0) ? 2 : (yP==1) ? 1 : 0;
                      break;
                    case 2:
                      sigCtx = (xP==0) ? 2 : (xP==1) ? 1 : 0;
                      break;
                    default:
                      sigCtx = 2;
                      break;
                    }

                    //logtrace(LogSlice,"a) sigCtx=%d\n",sigCtx);

                    if (cIdx==0) {
                      if (xS+yS > 0) sigCtx+=3;

                      //logtrace(LogSlice,"b) sigCtx=%d\n",sigCtx);

                      // if log2TrafoSize==3
                      if (sbWidth==2) { // 8x8 block
                        sigCtx += (scanIdx==0) ? 9 : 15;
                      } else {
                        sigCtx += 21;
                      }

                      //logtrace(LogSlice,"c) sigCtx=%d\n",sigCtx);
                    }
                    else {
                      // if log2TrafoSize==3
                      if (sbWidth==2) { // 8x8 block
                        sigCtx+=9;
                      }
                      else {
                        sigCtx+=12;
                      }
                    }

                  }

                  int ctxIdxInc;
                  if (cIdx==0) { ctxIdxInc=sigCtx; }
                  else         { ctxIdxInc=27+sigCtx; }

                  if (ctxIdxLookup[log2w-2][cIdx][scanIdx][prevCsbf][xC+(yC<<log2w)] != 0xFF) {
                    assert(ctxIdxLookup[log2w-2][cIdx][scanIdx][prevCsbf][xC+(yC<<log2w)] == ctxIdxInc);
                  }

                  ctxIdxLookup[log2w-2][cIdx][scanIdx][prevCsbf][xC+(yC<<log2w)] = ctxIdxInc;

                  //NOTE: when using this option, we have to include all three scanIdx in the table
                  //ctxIdxLookup[log2w-2][cIdx][scanIdx][prevCsbf][s] = ctxIdxInc;
                }
          }

  return true;
}


bool alloc_and_init_significant_coeff_ctxIdx_lookupTable_OLD()
{
  int tableSize = 2*2*4*(4*4 + 8*8 + 16*16 + 32*32);
  uint8_t* p = (uint8_t*)malloc(tableSize);
  if (p==NULL) {
    return false;
  }

  for (int log2w=2; log2w<=5 ; log2w++)
    for (int cIdx=0;cIdx<2;cIdx++)
      for (int scanIdx=0;scanIdx<2;scanIdx++)
        for (int prevCsbf=0;prevCsbf<4;prevCsbf++)
          {
            // assign pointer into reserved memory area

            ctxIdxLookup[log2w-2][cIdx][scanIdx][prevCsbf] = p;
            p += (1<<log2w)*(1<<log2w);

            const position* ScanOrderSub = get_scan_order(log2w-2, scanIdx);
            const position* ScanOrderPos = get_scan_order(2, scanIdx);

            //for (int yC=0;yC<(1<<log2w);yC++)
            // for (int xC=0;xC<(1<<log2w);xC++)
            for (int s=0;s<(1<<log2w)*(1<<log2w);s++)
              {
                position S = ScanOrderSub[s>>4];
                int x0 = S.x<<2;
                int y0 = S.y<<2;

                int subX = ScanOrderPos[s & 0xF].x;
                int subY = ScanOrderPos[s & 0xF].y;
                int xC = x0 + subX;
                int yC = y0 + subY;


                int w = 1<<log2w;
                int sbWidth = w>>2;

                int sigCtx;

                // if log2TrafoSize==2
                if (sbWidth==1) {
                  sigCtx = ctxIdxMap[(yC<<2) + xC];
                }
                else if (xC+yC==0) {
                  sigCtx = 0;
                }
                else {
                  int xS = xC>>2;
                  int yS = yC>>2;
                  /*
                    int prevCsbf = 0;

                    if (xS < sbWidth-1) { prevCsbf += coded_sub_block_flag[xS+1  +yS*sbWidth];    }
                    if (yS < sbWidth-1) { prevCsbf += coded_sub_block_flag[xS+(1+yS)*sbWidth]<<1; }
                  */
                  int xP = xC & 3;
                  int yP = yC & 3;

                  logtrace(LogSlice,"posInSubset: %d,%d\n",xP,yP);
                  logtrace(LogSlice,"prevCsbf: %d\n",prevCsbf);

                  //printf("%d | %d %d\n",prevCsbf,xP,yP);

                  switch (prevCsbf) {
                  case 0:
                    //sigCtx = (xP+yP==0) ? 2 : (xP+yP<3) ? 1 : 0;
                    sigCtx = (xP+yP>=3) ? 0 : (xP+yP>0) ? 1 : 2;
                    break;
                  case 1:
                    sigCtx = (yP==0) ? 2 : (yP==1) ? 1 : 0;
                    break;
                  case 2:
                    sigCtx = (xP==0) ? 2 : (xP==1) ? 1 : 0;
                    break;
                  default:
                    sigCtx = 2;
                    break;
                  }

                  logtrace(LogSlice,"a) sigCtx=%d\n",sigCtx);

                  if (cIdx==0) {
                    if (xS+yS > 0) sigCtx+=3;

                    logtrace(LogSlice,"b) sigCtx=%d\n",sigCtx);

                    // if log2TrafoSize==3
                    if (sbWidth==2) { // 8x8 block
                      sigCtx += (scanIdx==0) ? 9 : 15;
                    } else {
                      sigCtx += 21;
                    }

                    logtrace(LogSlice,"c) sigCtx=%d\n",sigCtx);
                  }
                  else {
                    // if log2TrafoSize==3
                    if (sbWidth==2) { // 8x8 block
                      sigCtx+=9;
                    }
                    else {
                      sigCtx+=12;
                    }
                  }
                }

                int ctxIdxInc;
                if (cIdx==0) { ctxIdxInc=sigCtx; }
                else         { ctxIdxInc=27+sigCtx; }


                ctxIdxLookup[log2w-2][cIdx][scanIdx][prevCsbf][xC+(yC<<log2w)] = ctxIdxInc;

                //NOTE: when using this option, we have to include all three scanIdx in the table
                //ctxIdxLookup[log2w-2][cIdx][scanIdx][prevCsbf][s] = ctxIdxInc;
              }
          }

  return true;
}

void free_significant_coeff_ctxIdx_lookupTable()
{
  free(ctxIdxLookup[0][0][0][0]);
  ctxIdxLookup[0][0][0][0]=NULL;
}




#if 0
static int decode_significant_coeff_flag(thread_context* tctx,
					 int xC,int yC,
					 const uint8_t* coded_sub_block_flag,
					 int sbWidth,
					 int cIdx,
					 int scanIdx)
{
  logtrace(LogSlice,"# significant_coeff_flag (xC:%d yC:%d sbWidth:%d cIdx:%d scanIdx:%d)\n",
           xC,yC,sbWidth,cIdx,scanIdx);

  int sigCtx;

  // if log2TrafoSize==2
  if (sbWidth==1) {
    sigCtx = ctxIdxMap[(yC<<2) + xC];
  }
  else if (xC+yC==0) {
    sigCtx = 0;
  }
  else {
    int xS = xC>>2;
    int yS = yC>>2;
    int prevCsbf = 0;
    if (xS < sbWidth-1) { prevCsbf += coded_sub_block_flag[xS+1  +yS*sbWidth];    }
    if (yS < sbWidth-1) { prevCsbf += coded_sub_block_flag[xS+(1+yS)*sbWidth]<<1; }

    int xP = xC & 3;
    int yP = yC & 3;

    logtrace(LogSlice,"posInSubset: %d,%d\n",xP,yP);
    logtrace(LogSlice,"prevCsbf: %d\n",prevCsbf);

    //printf("%d | %d %d\n",prevCsbf,xP,yP);

    switch (prevCsbf) {
    case 0:
      //sigCtx = (xP+yP==0) ? 2 : (xP+yP<3) ? 1 : 0;
      sigCtx = (xP+yP>=3) ? 0 : (xP+yP>0) ? 1 : 2;
      break;
    case 1:
      sigCtx = (yP==0) ? 2 : (yP==1) ? 1 : 0;
      break;
    case 2:
      sigCtx = (xP==0) ? 2 : (xP==1) ? 1 : 0;
      break;
    default:
      sigCtx = 2;
      break;
    }

    logtrace(LogSlice,"a) sigCtx=%d\n",sigCtx);

    if (cIdx==0) {
      if (xS+yS > 0) sigCtx+=3;

      logtrace(LogSlice,"b) sigCtx=%d\n",sigCtx);

      // if log2TrafoSize==3
      if (sbWidth==2) {
        sigCtx += (scanIdx==0) ? 9 : 15;
      } else {
        sigCtx += 21;
      }

      logtrace(LogSlice,"c) sigCtx=%d\n",sigCtx);
    }
    else {
      // if log2TrafoSize==3
      if (sbWidth==2) {
        sigCtx+=9;
      }
      else {
        sigCtx+=12;
      }
    }
  }

  int ctxIdxInc;
  if (cIdx==0) { ctxIdxInc=sigCtx; }
  else         { ctxIdxInc=27+sigCtx; }

  int context = tctx->shdr->initType*42 + ctxIdxInc;
  logtrace(LogSlice,"context: %d\n",context);

  int bit = decode_CABAC_bit(&tctx->cabac_decoder,
                             &tctx->ctx_model[CONTEXT_MODEL_SIGNIFICANT_COEFF_FLAG + context]);
  return bit;
}
#endif



static inline int decode_significant_coeff_flag_lookup(thread_context* tctx,
                                                 uint8_t ctxIdxInc)
{
  logtrace(LogSlice,"# significant_coeff_flag\n");
  logtrace(LogSlice,"context: %d\n",ctxIdxInc);

  int bit = decode_CABAC_bit(&tctx->cabac_decoder,
                             &tctx->ctx_model[CONTEXT_MODEL_SIGNIFICANT_COEFF_FLAG + ctxIdxInc]);

  logtrace(LogSymbols,"$1 significant_coeff_flag=%d\n",bit);

  return bit;
}





static inline int decode_coeff_abs_level_greater1(thread_context* tctx,
                                                  int cIdx, int i,
                                                  bool firstCoeffInSubblock,
                                                  bool firstSubblock,
                                                  int  lastSubblock_greater1Ctx,
                                                  int* lastInvocation_greater1Ctx,
                                                  int* lastInvocation_coeff_abs_level_greater1_flag,
                                                  int* lastInvocation_ctxSet, int c1)
{
  logtrace(LogSlice,"# coeff_abs_level_greater1\n");

  logtrace(LogSlice,"  cIdx:%d i:%d firstCoeffInSB:%d firstSB:%d lastSB>1:%d last>1Ctx:%d lastLev>1:%d lastCtxSet:%d\n", cIdx,i,firstCoeffInSubblock,firstSubblock,lastSubblock_greater1Ctx,
	   *lastInvocation_greater1Ctx,
	   *lastInvocation_coeff_abs_level_greater1_flag,
	   *lastInvocation_ctxSet);

  int lastGreater1Ctx;
  int greater1Ctx;
  int ctxSet;

  logtrace(LogSlice,"c1: %d\n",c1);

  if (firstCoeffInSubblock) {
    // block with real DC -> ctx 0
    if (i==0 || cIdx>0) { ctxSet=0; }
    else { ctxSet=2; }

    if (firstSubblock) { lastGreater1Ctx=1; }
    else { lastGreater1Ctx = lastSubblock_greater1Ctx; }

    if (lastGreater1Ctx==0) { ctxSet++; }

    logtrace(LogSlice,"ctxSet: %d\n",ctxSet);

    greater1Ctx=1;
  }
  else { // !firstCoeffInSubblock
    ctxSet = *lastInvocation_ctxSet;
    logtrace(LogSlice,"ctxSet (old): %d\n",ctxSet);

    greater1Ctx = *lastInvocation_greater1Ctx;
    if (greater1Ctx>0) {
      int lastGreater1Flag=*lastInvocation_coeff_abs_level_greater1_flag;
      if (lastGreater1Flag==1) greater1Ctx=0;
      else { /*if (greater1Ctx>0)*/ greater1Ctx++; }
    }
  }

  ctxSet = c1; // use HM algo

  int ctxIdxInc = (ctxSet*4) + (greater1Ctx>=3 ? 3 : greater1Ctx);

  if (cIdx>0) { ctxIdxInc+=16; }

  int bit = decode_CABAC_bit(&tctx->cabac_decoder,
                             &tctx->ctx_model[CONTEXT_MODEL_COEFF_ABS_LEVEL_GREATER1_FLAG + ctxIdxInc]);

  *lastInvocation_greater1Ctx = greater1Ctx;
  *lastInvocation_coeff_abs_level_greater1_flag = bit;
  *lastInvocation_ctxSet = ctxSet;

  //logtrace(LogSymbols,"$1 coeff_abs_level_greater1=%d\n",bit);

  return bit;
}


static int decode_coeff_abs_level_greater2(thread_context* tctx,
					   int cIdx, // int i,int n,
					   int ctxSet)
{
  logtrace(LogSlice,"# coeff_abs_level_greater2\n");

  int ctxIdxInc = ctxSet;

  if (cIdx>0) ctxIdxInc+=4;

  int bit = decode_CABAC_bit(&tctx->cabac_decoder,
                             &tctx->ctx_model[CONTEXT_MODEL_COEFF_ABS_LEVEL_GREATER2_FLAG + ctxIdxInc]);

  logtrace(LogSymbols,"$1 coeff_abs_level_greater2=%d\n",bit);

  return bit;
}


#define MAX_PREFIX 64

static int decode_coeff_abs_level_remaining(thread_context* tctx,
                                            int cRiceParam)
{
  logtrace(LogSlice,"# decode_coeff_abs_level_remaining\n");

  int prefix=-1;
  int codeword=0;
  do {
    prefix++;
    codeword = decode_CABAC_bypass(&tctx->cabac_decoder);

    if (prefix>MAX_PREFIX) {
      return 0; // TODO: error
    }
  }
  while (codeword);

  // prefix = nb. 1 bits

  int value;

  if (prefix <= 3) {
    // when code only TR part (level < TRMax)

    codeword = decode_CABAC_FL_bypass(&tctx->cabac_decoder, cRiceParam);
    value = (prefix<<cRiceParam) + codeword;
  }
  else {
    // Suffix coded with EGk. Note that the unary part of EGk is already
    // included in the 'prefix' counter above.

    codeword = decode_CABAC_FL_bypass(&tctx->cabac_decoder, prefix-3+cRiceParam);
    value = (((1<<(prefix-3))+3-1)<<cRiceParam)+codeword;
  }

  logtrace(LogSymbols,"$1 coeff_abs_level_remaining=%d\n",value);

  return value;
}


static int decode_merge_flag(thread_context* tctx)
{
  logtrace(LogSlice,"# merge_flag\n");

  int bit = decode_CABAC_bit(&tctx->cabac_decoder,
                             &tctx->ctx_model[CONTEXT_MODEL_MERGE_FLAG]);

  logtrace(LogSymbols,"$1 merge_flag=%d\n",bit);

  return bit;
}


static int decode_merge_idx(thread_context* tctx)
{
  logtrace(LogSlice,"# merge_idx\n");

  if (tctx->shdr->MaxNumMergeCand <= 1) {
    logtrace(LogSymbols,"$1 merge_idx=%d\n",0);
    return 0;
  }

  // TU coding, first bin is CABAC, remaining are bypass.
  // cMax = MaxNumMergeCand-1

  int idx = decode_CABAC_bit(&tctx->cabac_decoder,
                             &tctx->ctx_model[CONTEXT_MODEL_MERGE_IDX]);

  if (idx==0) {
    // nothing
  }
  else {
    idx=1;

    while (idx<tctx->shdr->MaxNumMergeCand-1) {
      if (decode_CABAC_bypass(&tctx->cabac_decoder)) {
        idx++;
      }
      else {
        break;
      }
    }
  }

  logtrace(LogSlice,"> merge_idx = %d\n",idx);
  logtrace(LogSymbols,"$1 merge_idx=%d\n",idx);

  return idx;
}


static int decode_pred_mode_flag(thread_context* tctx)
{
  logtrace(LogSlice,"# pred_mode_flag\n");

  int bit = decode_CABAC_bit(&tctx->cabac_decoder,
                             &tctx->ctx_model[CONTEXT_MODEL_PRED_MODE_FLAG]);

  logtrace(LogSymbols,"$1 pred_mode=%d\n",bit);
  return bit;
}

static int decode_mvp_lx_flag(thread_context* tctx)
{
  logtrace(LogSlice,"# mvp_lx_flag\n");

  int bit = decode_CABAC_bit(&tctx->cabac_decoder,
                             &tctx->ctx_model[CONTEXT_MODEL_MVP_LX_FLAG]);

  logtrace(LogSymbols,"$1 mvp_lx_flag=%d\n",bit);
  return bit;
}

static int decode_rqt_root_cbf(thread_context* tctx)
{
  logtrace(LogSlice,"# rqt_root_cbf\n");

  int bit = decode_CABAC_bit(&tctx->cabac_decoder,
                             &tctx->ctx_model[CONTEXT_MODEL_RQT_ROOT_CBF]);

  logtrace(LogSymbols,"$1 rqt_root_cbf=%d\n",bit);
  return bit;
}

static int decode_ref_idx_lX(thread_context* tctx, int numRefIdxLXActive)
{
  // prevent endless loop when 'numRefIdxLXActive' is invalid
  if (numRefIdxLXActive <= 1) {
    return 0;
  }

  logtrace(LogSlice,"# ref_idx_lX\n");

  int cMax = numRefIdxLXActive-1;

  if (cMax==0) {
    logtrace(LogSlice,"> ref_idx = 0 (cMax==0)\n");
    return 0;
  } // do check for single reference frame here

  int bit = decode_CABAC_bit(&tctx->cabac_decoder,
                             &tctx->ctx_model[CONTEXT_MODEL_REF_IDX_LX + 0]);

  int idx=0;

  while (bit) {
    idx++;
    if (idx==cMax) { break; }

    if (idx==1) {
      bit = decode_CABAC_bit(&tctx->cabac_decoder,
                             &tctx->ctx_model[CONTEXT_MODEL_REF_IDX_LX + 1]);
    }
    else {
      bit = decode_CABAC_bypass(&tctx->cabac_decoder);
    }
  }

  logtrace(LogSlice,"> ref_idx = %d\n",idx);

  logtrace(LogSymbols,"$1 ref_idx_lX=%d\n",idx);
  return idx;
}


static enum InterPredIdc  decode_inter_pred_idc(thread_context* tctx,
                                               int x0, int y0,
                                               int nPbW, int nPbH,
                                               int ctDepth)
{
  logtrace(LogSlice,"# inter_pred_idc\n");

  int value;

  context_model* model = &tctx->ctx_model[CONTEXT_MODEL_INTER_PRED_IDC];

  if (nPbW+nPbH==12) {
    value = decode_CABAC_bit(&tctx->cabac_decoder,
                             &model[4]);
  }
  else {
    int bit0 = decode_CABAC_bit(&tctx->cabac_decoder,
                                &model[ctDepth]);
    if (bit0==0) {
      value = decode_CABAC_bit(&tctx->cabac_decoder,
                               &model[4]);
    }
    else {
      value = 2;
    }
  }

  logtrace(LogSlice,"> inter_pred_idc = %d (%s)\n",value,
           value==0 ? "L0" : (value==1 ? "L1" : "BI"));

  logtrace(LogSymbols,"$1 decode_inter_pred_idx=%d\n",value+1);

  return (enum InterPredIdc) (value+1);
}


static int  decode_explicit_rdpcm_flag(thread_context* tctx,int cIdx)
{
  context_model* model = &tctx->ctx_model[CONTEXT_MODEL_RDPCM_FLAG];
  int value = decode_CABAC_bit(&tctx->cabac_decoder, &model[cIdx ? 1 : 0]);
  return value;
}


static int  decode_explicit_rdpcm_dir(thread_context* tctx,int cIdx)
{
  context_model* model = &tctx->ctx_model[CONTEXT_MODEL_RDPCM_DIR];
  int value = decode_CABAC_bit(&tctx->cabac_decoder, &model[cIdx ? 1 : 0]);
  return value;
}



/* Take CtbAddrInTS and compute
   -> CtbAddrInRS, CtbX, CtbY
 */
bool setCtbAddrFromTS(thread_context* tctx)
{
  const seq_parameter_set& sps = tctx->img->get_sps();

  if (tctx->CtbAddrInTS < sps.PicSizeInCtbsY) {
    tctx->CtbAddrInRS = tctx->img->get_pps().CtbAddrTStoRS[tctx->CtbAddrInTS];

    tctx->CtbX = tctx->CtbAddrInRS % sps.PicWidthInCtbsY;
    tctx->CtbY = tctx->CtbAddrInRS / sps.PicWidthInCtbsY;
    return false;
  }
  else {
    tctx->CtbAddrInRS = sps.PicSizeInCtbsY;

    tctx->CtbX = tctx->CtbAddrInRS % sps.PicWidthInCtbsY;
    tctx->CtbY = tctx->CtbAddrInRS / sps.PicWidthInCtbsY;
    return true;
  }
}

// returns true when we reached the end of the image (ctbAddr==picSizeInCtbsY)
bool advanceCtbAddr(thread_context* tctx)
{
    tctx->CtbAddrInTS++;

    return setCtbAddrFromTS(tctx);
}


void read_sao(thread_context* tctx, int xCtb,int yCtb,
              int CtbAddrInSliceSeg)
{
  slice_segment_header* shdr = tctx->shdr;
  de265_image* img = tctx->img;
  const seq_parameter_set& sps = img->get_sps();
  const pic_parameter_set& pps = img->get_pps();

  logtrace(LogSlice,"# read_sao(%d,%d)\n",xCtb,yCtb);

  sao_info saoinfo;
  memset(&saoinfo,0,sizeof(sao_info));
  logtrace(LogSlice,"sizeof saoinfo: %d\n",sizeof(sao_info));


  char sao_merge_left_flag = 0;
  char sao_merge_up_flag = 0;

  if (xCtb>0) {
    //char leftCtbInSliceSeg = (CtbAddrInSliceSeg>0);
    char leftCtbInSliceSeg = (tctx->CtbAddrInRS > shdr->SliceAddrRS);
    char leftCtbInTile = (pps.TileIdRS[xCtb   + yCtb * sps.PicWidthInCtbsY] ==
                          pps.TileIdRS[xCtb-1 + yCtb * sps.PicWidthInCtbsY]);

    if (leftCtbInSliceSeg && leftCtbInTile) {
      sao_merge_left_flag = decode_sao_merge_flag(tctx);
      logtrace(LogSlice,"sao_merge_left_flag: %d\n",sao_merge_left_flag);
    }
  }

  if (yCtb>0 && sao_merge_left_flag==0) {
    logtrace(LogSlice,"CtbAddrInRS:%d PicWidthInCtbsY:%d slice_segment_address:%d\n",
             tctx->CtbAddrInRS,
             sps.PicWidthInCtbsY,
             shdr->slice_segment_address);
    char upCtbInSliceSeg = (tctx->CtbAddrInRS - sps.PicWidthInCtbsY) >= shdr->SliceAddrRS;
    char upCtbInTile = (pps.TileIdRS[xCtb +  yCtb    * sps.PicWidthInCtbsY] ==
                        pps.TileIdRS[xCtb + (yCtb-1) * sps.PicWidthInCtbsY]);

    if (upCtbInSliceSeg && upCtbInTile) {
      sao_merge_up_flag = decode_sao_merge_flag(tctx);
      logtrace(LogSlice,"sao_merge_up_flag: %d\n",sao_merge_up_flag);
    }
  }

  if (!sao_merge_up_flag && !sao_merge_left_flag) {
    int nChroma = 3;
    if (sps.ChromaArrayType == CHROMA_MONO) nChroma=1;

    for (int cIdx=0; cIdx<nChroma; cIdx++) {
      if ((shdr->slice_sao_luma_flag && cIdx==0) ||
          (shdr->slice_sao_chroma_flag && cIdx>0)) {

        uint8_t SaoTypeIdx = 0;

        if (cIdx==0) {
          char sao_type_idx_luma = decode_sao_type_idx(tctx);
          logtrace(LogSlice,"sao_type_idx_luma: %d\n", sao_type_idx_luma);
          saoinfo.SaoTypeIdx = SaoTypeIdx = sao_type_idx_luma;
        }
        else if (cIdx==1) {
          char sao_type_idx_chroma = decode_sao_type_idx(tctx);
          logtrace(LogSlice,"sao_type_idx_chroma: %d\n", sao_type_idx_chroma);
          SaoTypeIdx = sao_type_idx_chroma;
          saoinfo.SaoTypeIdx |= SaoTypeIdx<<(2*1);
          saoinfo.SaoTypeIdx |= SaoTypeIdx<<(2*2);  // set for both chroma components
        }
        else {
          // SaoTypeIdx = 0

          SaoTypeIdx = (saoinfo.SaoTypeIdx >> (2*cIdx)) & 0x3;
        }

        if (SaoTypeIdx != 0) {
          for (int i=0;i<4;i++) {
            saoinfo.saoOffsetVal[cIdx][i] = decode_sao_offset_abs(tctx, img->get_bit_depth(cIdx));
            logtrace(LogSlice,"saoOffsetVal[%d][%d] = %d\n",cIdx,i, saoinfo.saoOffsetVal[cIdx][i]);
          }

          int sign[4];
          if (SaoTypeIdx==1) {
            for (int i=0;i<4;i++) {
              if (saoinfo.saoOffsetVal[cIdx][i] != 0) {
                sign[i] = decode_sao_offset_sign(tctx) ? -1 : 1;
              }
              else {
                sign[i] = 0; // not really required, but compiler warns about uninitialized values
              }
            }

            saoinfo.sao_band_position[cIdx] = decode_sao_band_position(tctx);
          }
          else {
            uint8_t SaoEoClass = 0;

            sign[0] = sign[1] =  1;
            sign[2] = sign[3] = -1;

            if (cIdx==0) {
              saoinfo.SaoEoClass = SaoEoClass = decode_sao_class(tctx);
            }
            else if (cIdx==1) {
              SaoEoClass = decode_sao_class(tctx);
              saoinfo.SaoEoClass |= SaoEoClass << (2*1);
              saoinfo.SaoEoClass |= SaoEoClass << (2*2);
            }

            logtrace(LogSlice,"SaoEoClass[%d] = %d\n",cIdx,SaoEoClass);
          }

          int log2OffsetScale;

          if (cIdx==0) {
            log2OffsetScale = pps.range_extension.log2_sao_offset_scale_luma;
          }
          else {
            log2OffsetScale = pps.range_extension.log2_sao_offset_scale_chroma;
          }

          for (int i=0;i<4;i++) {
            saoinfo.saoOffsetVal[cIdx][i] = sign[i]*(saoinfo.saoOffsetVal[cIdx][i] << log2OffsetScale);
          }
        }
      }
    }

    img->set_sao_info(xCtb,yCtb,  &saoinfo);
  }


  if (sao_merge_left_flag) {
    img->set_sao_info(xCtb,yCtb,  img->get_sao_info(xCtb-1,yCtb));
  }

  if (sao_merge_up_flag) {
    img->set_sao_info(xCtb,yCtb,  img->get_sao_info(xCtb,yCtb-1));
  }
}


void read_coding_tree_unit(thread_context* tctx)
{
  slice_segment_header* shdr = tctx->shdr;
  de265_image* img = tctx->img;
  const seq_parameter_set& sps = img->get_sps();

  int xCtb = (tctx->CtbAddrInRS % sps.PicWidthInCtbsY);
  int yCtb = (tctx->CtbAddrInRS / sps.PicWidthInCtbsY);
  int xCtbPixels = xCtb << sps.Log2CtbSizeY;
  int yCtbPixels = yCtb << sps.Log2CtbSizeY;

  logtrace(LogSlice,"----- decode CTB %d;%d (%d;%d) POC=%d, SliceAddrRS=%d\n",
           xCtbPixels,yCtbPixels, xCtb,yCtb,
           tctx->img->PicOrderCntVal, tctx->shdr->SliceAddrRS);

  img->set_SliceAddrRS(xCtb, yCtb, tctx->shdr->SliceAddrRS);

  img->set_SliceHeaderIndex(xCtbPixels,yCtbPixels, shdr->slice_index);

  int CtbAddrInSliceSeg = tctx->CtbAddrInRS - shdr->slice_segment_address;

  if (shdr->slice_sao_luma_flag || shdr->slice_sao_chroma_flag)
    {
      read_sao(tctx, xCtb,yCtb, CtbAddrInSliceSeg);
    }

  read_coding_quadtree(tctx, xCtbPixels, yCtbPixels, sps.Log2CtbSizeY, 0);
}


LIBDE265_INLINE static int luma_pos_to_ctbAddrRS(const seq_parameter_set* sps, int x,int y)
{
  int ctbX = x >> sps->Log2CtbSizeY;
  int ctbY = y >> sps->Log2CtbSizeY;

  return ctbY * sps->PicWidthInCtbsY + ctbX;
}


int check_CTB_available(const de265_image* img,
                        int xC,int yC, int xN,int yN)
{
  // check whether neighbor is outside of frame

  if (xN < 0 || yN < 0) { return 0; }
  if (xN >= img->get_sps().pic_width_in_luma_samples)  { return 0; }
  if (yN >= img->get_sps().pic_height_in_luma_samples) { return 0; }


  int current_ctbAddrRS  = luma_pos_to_ctbAddrRS(&img->get_sps(), xC,yC);
  int neighbor_ctbAddrRS = luma_pos_to_ctbAddrRS(&img->get_sps(), xN,yN);

  // TODO: check if this is correct (6.4.1)

  if (img->get_SliceAddrRS_atCtbRS(current_ctbAddrRS) !=
      img->get_SliceAddrRS_atCtbRS(neighbor_ctbAddrRS)) {
    return 0;
  }

  // check if both CTBs are in the same tile.

  if (img->get_pps().TileIdRS[current_ctbAddrRS] !=
      img->get_pps().TileIdRS[neighbor_ctbAddrRS]) {
    return 0;
  }

  return 1;
}


int residual_coding(thread_context* tctx,
                    int x0, int y0,  // position of TU in frame
                    int log2TrafoSize,
                    int cIdx)
{
  logtrace(LogSlice,"- residual_coding x0:%d y0:%d log2TrafoSize:%d cIdx:%d\n",x0,y0,log2TrafoSize,cIdx);

  //slice_segment_header* shdr = tctx->shdr;

  de265_image* img = tctx->img;
  const seq_parameter_set& sps = img->get_sps();
  const pic_parameter_set& pps = img->get_pps();

  enum PredMode PredMode = img->get_pred_mode(x0,y0);

  if (cIdx==0) {
    img->set_nonzero_coefficient(x0,y0,log2TrafoSize);
  }


  if (pps.transform_skip_enabled_flag &&
      !tctx->cu_transquant_bypass_flag &&
      (log2TrafoSize <= pps.Log2MaxTransformSkipSize))
    {
      tctx->transform_skip_flag[cIdx] = decode_transform_skip_flag(tctx,cIdx);
    }
  else
    {
      tctx->transform_skip_flag[cIdx] = 0;
    }


  tctx->explicit_rdpcm_flag = false;

  if (PredMode == MODE_INTER && sps.range_extension.explicit_rdpcm_enabled_flag &&
      ( tctx->transform_skip_flag[cIdx] || tctx->cu_transquant_bypass_flag))
    {
      tctx->explicit_rdpcm_flag = decode_explicit_rdpcm_flag(tctx,cIdx);
      if (tctx->explicit_rdpcm_flag) {
        tctx->explicit_rdpcm_dir = decode_explicit_rdpcm_dir(tctx,cIdx);
      }

      //printf("EXPLICIT RDPCM %d;%d\n",x0,y0);
    }
  else
    {
      tctx->explicit_rdpcm_flag = false;
    }



  // sbType for persistent_rice_adaptation_enabled_flag

  int sbType = (cIdx==0) ? 2 : 0;
  if (tctx->transform_skip_flag[cIdx] || tctx->cu_transquant_bypass_flag) {
    sbType++;
  }


  // --- decode position of last coded coefficient ---

  int last_significant_coeff_x_prefix =
    decode_last_significant_coeff_prefix(tctx,log2TrafoSize,cIdx,
                                         &tctx->ctx_model[CONTEXT_MODEL_LAST_SIGNIFICANT_COEFFICIENT_X_PREFIX]);

  int last_significant_coeff_y_prefix =
    decode_last_significant_coeff_prefix(tctx,log2TrafoSize,cIdx,
                                         &tctx->ctx_model[CONTEXT_MODEL_LAST_SIGNIFICANT_COEFFICIENT_Y_PREFIX]);


  // TODO: we can combine both FL-bypass calls into one, but the gain may be limited...

  int LastSignificantCoeffX;
  if (last_significant_coeff_x_prefix > 3) {
    int nBits = (last_significant_coeff_x_prefix>>1)-1;
    int last_significant_coeff_x_suffix = decode_CABAC_FL_bypass(&tctx->cabac_decoder,nBits);

    LastSignificantCoeffX =
      ((2+(last_significant_coeff_x_prefix & 1)) << nBits) + last_significant_coeff_x_suffix;
  }
  else {
    LastSignificantCoeffX = last_significant_coeff_x_prefix;
  }

  int LastSignificantCoeffY;
  if (last_significant_coeff_y_prefix > 3) {
    int nBits = (last_significant_coeff_y_prefix>>1)-1;
    int last_significant_coeff_y_suffix = decode_CABAC_FL_bypass(&tctx->cabac_decoder,nBits);

    LastSignificantCoeffY =
      ((2+(last_significant_coeff_y_prefix & 1)) << nBits) + last_significant_coeff_y_suffix;
  }
  else {
    LastSignificantCoeffY = last_significant_coeff_y_prefix;
  }



  // --- determine scanIdx ---

  int scanIdx;

  if (PredMode == MODE_INTRA) {
    if (cIdx==0) {
      scanIdx = get_intra_scan_idx(log2TrafoSize, img->get_IntraPredMode(x0,y0),  cIdx, &sps);
      //printf("luma scan idx=%d <- intra mode=%d\n",scanIdx, img->get_IntraPredMode(x0,y0));
    }
    else {
      scanIdx = get_intra_scan_idx(log2TrafoSize, img->get_IntraPredModeC(x0,y0), cIdx, &sps);
      //printf("chroma scan idx=%d <- intra mode=%d chroma:%d trsize:%d\n",scanIdx,
      //       img->get_IntraPredModeC(x0,y0), sps->chroma_format_idc, 1<<log2TrafoSize);
    }
  }
  else {
    scanIdx=0;
  }

  if (scanIdx==2) {
    std::swap(LastSignificantCoeffX, LastSignificantCoeffY);
  }

  logtrace(LogSlice,"LastSignificantCoeff: x=%d;y=%d\n",LastSignificantCoeffX,LastSignificantCoeffY);

  const position* ScanOrderSub = get_scan_order(log2TrafoSize-2, scanIdx);
  const position* ScanOrderPos = get_scan_order(2, scanIdx);

  logtrace(LogSlice,"ScanOrderPos: ");
  for (int n=0;n<4*4;n++)
    logtrace(LogSlice,"*%d,%d ", ScanOrderPos[n].x, ScanOrderPos[n].y);
  logtrace(LogSlice,"*\n");


  // --- find last sub block and last scan pos ---

  int xC,yC;

  scan_position lastScanP = get_scan_position(LastSignificantCoeffX, LastSignificantCoeffY,
                                              scanIdx, log2TrafoSize);

  int lastScanPos  = lastScanP.scanPos;
  int lastSubBlock = lastScanP.subBlock;


  int sbWidth = 1<<(log2TrafoSize-2);

  uint8_t coded_sub_block_neighbors[32/4*32/4];
  memset(coded_sub_block_neighbors,0,sbWidth*sbWidth);

  int  c1 = 1;
  bool firstSubblock = true;           // for coeff_abs_level_greater1_flag context model
  int  lastSubblock_greater1Ctx=false; /* for coeff_abs_level_greater1_flag context model
                                          (initialization not strictly needed)
                                       */

#ifdef DE265_LOG_TRACE
  int16_t TransCoeffLevel[32 * 32];
  memset(TransCoeffLevel,0, sizeof(uint16_t)*32*32);
#endif

  int CoeffStride = 1<<log2TrafoSize;

  int  lastInvocation_greater1Ctx=0;
  int  lastInvocation_coeff_abs_level_greater1_flag=0;
  int  lastInvocation_ctxSet=0;



  // ----- decode coefficients -----

  tctx->nCoeff[cIdx] = 0;


  // i - subblock index
  // n - coefficient index in subblock

  for (int i=lastSubBlock;i>=0;i--) {
    position S = ScanOrderSub[i];
    int inferSbDcSigCoeffFlag=0;

    logtrace(LogSlice,"sub block scan idx: %d\n",i);


    // --- check whether this sub-block is coded ---

    int sub_block_is_coded = 0;

    if ((i<lastSubBlock) && (i>0)) {
      sub_block_is_coded = decode_coded_sub_block_flag(tctx, cIdx,
                                                       coded_sub_block_neighbors[S.x+S.y*sbWidth]);
      inferSbDcSigCoeffFlag=1;
    }
    else if (i==0 || i==lastSubBlock) {
      // first (DC) and last sub-block are always coded
      // - the first will most probably contain coefficients
      // - the last obviously contains the last coded coefficient

      sub_block_is_coded = 1;
    }

    if (sub_block_is_coded) {
      if (S.x > 0) coded_sub_block_neighbors[S.x-1 + S.y  *sbWidth] |= 1;
      if (S.y > 0) coded_sub_block_neighbors[S.x + (S.y-1)*sbWidth] |= 2;
    }


    // ----- find significant coefficients in this sub-block -----

    int16_t  coeff_value[16];
    int8_t   coeff_scan_pos[16];
    int8_t   coeff_sign[16];
    int8_t   coeff_has_max_base_level[16];
    int nCoefficients=0;


    if (sub_block_is_coded) {
      int x0 = S.x<<2;
      int y0 = S.y<<2;

      int log2w = log2TrafoSize-2;
      int prevCsbf = coded_sub_block_neighbors[S.x+S.y*sbWidth];
      uint8_t* ctxIdxMap = ctxIdxLookup[log2w][!!cIdx][!!scanIdx][prevCsbf];

      logdebug(LogSlice,"log2w:%d cIdx:%d scanIdx:%d prevCsbf:%d\n",
               log2w,cIdx,scanIdx,prevCsbf);


      // set the last coded coefficient in the last subblock

      int last_coeff =  (i==lastSubBlock) ? lastScanPos-1 : 15;

      if (i==lastSubBlock) {
        coeff_value[nCoefficients] = 1;
        coeff_has_max_base_level[nCoefficients] = 1;
        coeff_scan_pos[nCoefficients] = lastScanPos;
        nCoefficients++;
      }


      // --- decode all coefficients' significant_coeff flags except for the DC coefficient ---

      for (int n= last_coeff ; n>0 ; n--) {
        int subX = ScanOrderPos[n].x;
        int subY = ScanOrderPos[n].y;
        xC = x0 + subX;
        yC = y0 + subY;


        // for all AC coefficients in sub-block, a significant_coeff flag is coded

        int ctxInc;
        if (sps.range_extension.transform_skip_context_enabled_flag &&
            (tctx->cu_transquant_bypass_flag || tctx->transform_skip_flag[cIdx])) {
          ctxInc = ( cIdx == 0 ) ? 42 : (16+27);
        }
        else {
          ctxInc = ctxIdxMap[xC+(yC<<log2TrafoSize)];
        }

        logtrace(LogSlice,"trafoSize: %d\n",1<<log2TrafoSize);

        int significant_coeff = decode_significant_coeff_flag_lookup(tctx, ctxInc);

        if (significant_coeff) {
          coeff_value[nCoefficients] = 1;
          coeff_has_max_base_level[nCoefficients] = 1;
          coeff_scan_pos[nCoefficients] = n;
          nCoefficients++;

          // since we have a coefficient in the sub-block,
          // we cannot infer the DC coefficient anymore
          inferSbDcSigCoeffFlag = 0;
        }
      }


      // --- decode DC coefficient significance ---

      if (last_coeff>=0) // last coded coefficient (always set to 1) is not the DC coefficient
        {
          if (inferSbDcSigCoeffFlag==0) {
            // if we cannot infert the DC coefficient, it is coded

            int ctxInc;
            if (sps.range_extension.transform_skip_context_enabled_flag &&
                (tctx->cu_transquant_bypass_flag || tctx->transform_skip_flag[cIdx])) {
              ctxInc = ( cIdx == 0 ) ? 42 : (16+27);
            }
            else {
              ctxInc = ctxIdxMap[x0+(y0<<log2TrafoSize)];
            }

            int significant_coeff = decode_significant_coeff_flag_lookup(tctx, ctxInc);


            if (significant_coeff) {
              coeff_value[nCoefficients] = 1;
              coeff_has_max_base_level[nCoefficients] = 1;
              coeff_scan_pos[nCoefficients] = 0;
              nCoefficients++;
            }
          }
          else {
            // we can infer that the DC coefficient must be present
            coeff_value[nCoefficients] = 1;
            coeff_has_max_base_level[nCoefficients] = 1;
            coeff_scan_pos[nCoefficients] = 0;
            nCoefficients++;
          }
        }

    }


    /*
      logtrace(LogSlice,"significant_coeff_flags:\n");
      for (int y=0;y<4;y++) {
      logtrace(LogSlice,"  ");
      for (int x=0;x<4;x++) {
      logtrace(LogSlice,"*%d ",significant_coeff_flag[y][x]);
      }
      logtrace(LogSlice,"*\n");
      }
    */


    if (nCoefficients) {
      int ctxSet;
      if (i==0 || cIdx>0) { ctxSet=0; }
      else { ctxSet=2; }

      if (c1==0) { ctxSet++; }
      c1=1;


      // --- decode greater-1 flags ---

      int newLastGreater1ScanPos=-1;

      int lastGreater1Coefficient = libde265_min(8,nCoefficients);
      for (int c=0;c<lastGreater1Coefficient;c++) {
        int greater1_flag =
          decode_coeff_abs_level_greater1(tctx, cIdx,i,
                                          c==0,
                                          firstSubblock,
                                          lastSubblock_greater1Ctx,
                                          &lastInvocation_greater1Ctx,
                                          &lastInvocation_coeff_abs_level_greater1_flag,
                                          &lastInvocation_ctxSet, ctxSet);

        if (greater1_flag) {
          coeff_value[c]++;

          c1=0;

          if (newLastGreater1ScanPos == -1) {
            newLastGreater1ScanPos=c;
          }
        }
        else {
          coeff_has_max_base_level[c] = 0;

          if (c1<3 && c1>0) {
            c1++;
          }
        }
      }

      firstSubblock = false;
      lastSubblock_greater1Ctx = lastInvocation_greater1Ctx;


      // --- decode greater-2 flag ---

      if (newLastGreater1ScanPos != -1) {
        int flag = decode_coeff_abs_level_greater2(tctx,cIdx, lastInvocation_ctxSet);
        coeff_value[newLastGreater1ScanPos] += flag;
        coeff_has_max_base_level[newLastGreater1ScanPos] = flag;
      }


      // --- decode coefficient signs ---

      int signHidden;


      IntraPredMode predModeIntra;
      if (cIdx==0) predModeIntra = img->get_IntraPredMode(x0,y0);
      else         predModeIntra = img->get_IntraPredModeC(x0,y0);


      if (tctx->cu_transquant_bypass_flag ||
          (PredMode == MODE_INTRA &&
           sps.range_extension.implicit_rdpcm_enabled_flag &&
           tctx->transform_skip_flag[cIdx] &&
           ( predModeIntra == 10 || predModeIntra == 26 )) ||
          tctx->explicit_rdpcm_flag)
        {
          signHidden = 0;
        }
      else
        {
          signHidden = (coeff_scan_pos[0]-coeff_scan_pos[nCoefficients-1] > 3);
        }


      for (int n=0;n<nCoefficients-1;n++) {
        coeff_sign[n] = decode_CABAC_bypass(&tctx->cabac_decoder);
        logtrace(LogSlice,"sign[%d] = %d\n", n, coeff_sign[n]);
      }

      // n==nCoefficients-1
      if (!pps.sign_data_hiding_flag || !signHidden) {
        coeff_sign[nCoefficients-1] = decode_CABAC_bypass(&tctx->cabac_decoder);
        logtrace(LogSlice,"sign[%d] = %d\n", nCoefficients-1, coeff_sign[nCoefficients-1]);
      }
      else {
        coeff_sign[nCoefficients-1] = 0;
      }


      // --- decode coefficient value ---

      int sumAbsLevel=0;
      int uiGoRiceParam;

      if (sps.range_extension.persistent_rice_adaptation_enabled_flag==0) {
        uiGoRiceParam = 0;
      }
      else {
        uiGoRiceParam = tctx->StatCoeff[sbType]/4;
      }

      // printf("initial uiGoRiceParam=%d\n",uiGoRiceParam);
      bool firstCoeffWithAbsLevelRemaining = true;

      for (int n=0;n<nCoefficients;n++) {
        int baseLevel = coeff_value[n];

        int coeff_abs_level_remaining;

        // printf("coeff %d/%d, uiRiceParam: %d\n",n,nCoefficients,uiGoRiceParam);

        if (coeff_has_max_base_level[n]) {
          coeff_abs_level_remaining =
            decode_coeff_abs_level_remaining(tctx, uiGoRiceParam);

          if (sps.range_extension.persistent_rice_adaptation_enabled_flag == 0) {
            // (2014.10 / 9-20)
            if (baseLevel + coeff_abs_level_remaining > 3*(1<<uiGoRiceParam)) {
              uiGoRiceParam++;
              if (uiGoRiceParam>4) uiGoRiceParam=4;
            }
          }
          else {
            if (baseLevel + coeff_abs_level_remaining > 3*(1<<uiGoRiceParam))
              uiGoRiceParam++;
          }

          // persistent_rice_adaptation_enabled_flag
          if (sps.range_extension.persistent_rice_adaptation_enabled_flag &&
              firstCoeffWithAbsLevelRemaining) {
            if (coeff_abs_level_remaining >= (3 << (tctx->StatCoeff[sbType]/4 ))) {
              tctx->StatCoeff[sbType]++;
            }
            else if (2*coeff_abs_level_remaining < (1 << (tctx->StatCoeff[sbType]/4 )) &&
                     tctx->StatCoeff[sbType] > 0) {
              tctx->StatCoeff[sbType]--;
            }
          }

          firstCoeffWithAbsLevelRemaining=false;
        }
        else {
          coeff_abs_level_remaining = 0;
        }

        logtrace(LogSlice, "coeff_abs_level_remaining=%d\n",coeff_abs_level_remaining);


        int16_t currCoeff = baseLevel + coeff_abs_level_remaining;
        if (coeff_sign[n]) {
          currCoeff = -currCoeff;
        }

        if (pps.sign_data_hiding_flag && signHidden) {
          sumAbsLevel += currCoeff;

          if (n==nCoefficients-1 && (sumAbsLevel & 1)) {
            currCoeff = -currCoeff;
          }
        }

        logtrace(LogSlice, "quantized coefficient=%d\n",currCoeff);

#ifdef DE265_LOG_TRACE
        //TransCoeffLevel[yC*CoeffStride + xC] = currCoeff;
#endif

        // put coefficient in list
        int p = coeff_scan_pos[n];
        xC = (S.x<<2) + ScanOrderPos[p].x;
        yC = (S.y<<2) + ScanOrderPos[p].y;

        tctx->coeffList[cIdx][ tctx->nCoeff[cIdx] ] = currCoeff;
        tctx->coeffPos [cIdx][ tctx->nCoeff[cIdx] ] = xC + yC*CoeffStride;
        tctx->nCoeff[cIdx]++;

        //printf("%d ",currCoeff);
      }  // iterate through coefficients in sub-block

      //printf(" (%d;%d)\n",x0,y0);

    }  // if nonZero
  }  // next sub-block

  return DE265_OK;
}


static void decode_TU(thread_context* tctx,
                      int x0,int y0,
                      int xCUBase,int yCUBase,
                      int nT, int cIdx, enum PredMode cuPredMode, bool cbf)
{
  de265_image* img = tctx->img;
  const seq_parameter_set& sps = img->get_sps();

  int residualDpcm = 0;

  if (cuPredMode == MODE_INTRA) // if intra mode
    {
      enum IntraPredMode intraPredMode;

      if (cIdx==0) {
        intraPredMode = img->get_IntraPredMode(x0,y0);
      }
      else {
        const int SubWidthC  = sps.SubWidthC;
        const int SubHeightC = sps.SubHeightC;

        intraPredMode = img->get_IntraPredModeC(x0*SubWidthC,y0*SubHeightC);
      }

      if (intraPredMode<0 || intraPredMode>=35) {
        // TODO: ERROR
        intraPredMode = INTRA_DC;
      }

      decode_intra_prediction(img, x0,y0, intraPredMode, nT, cIdx);


      residualDpcm = sps.range_extension.implicit_rdpcm_enabled_flag &&
        (tctx->cu_transquant_bypass_flag || tctx->transform_skip_flag[cIdx]) &&
        (intraPredMode == 10 || intraPredMode == 26);

      if (residualDpcm && intraPredMode == 26)
        residualDpcm = 2;
    }
  else // INTER
    {
      if (tctx->explicit_rdpcm_flag) {
        residualDpcm = (tctx->explicit_rdpcm_dir ? 2 : 1);
      }
    }

  if (cbf) {
    scale_coefficients(tctx, x0,y0, xCUBase,yCUBase, nT, cIdx,
                       tctx->transform_skip_flag[cIdx], cuPredMode==MODE_INTRA, residualDpcm);
  }
  /*
  else if (!cbf && cIdx==0) {
    memset(tctx->residual_luma,0,32*32*sizeof(int32_t));
  }
  */
  else if (!cbf && cIdx!=0 && tctx->ResScaleVal) {
    // --- cross-component-prediction when CBF==0 ---

    tctx->nCoeff[cIdx] = 0;
    residualDpcm=0;

    scale_coefficients(tctx, x0,y0, xCUBase,yCUBase, nT, cIdx,
                       tctx->transform_skip_flag[cIdx], cuPredMode==MODE_INTRA, residualDpcm);
  }
}


static int decode_log2_res_scale_abs_plus1(thread_context* tctx, int cIdxMinus1)
{
  //const int context = (cIdx==0) ? 0 : 1;

  logtrace(LogSlice,"# log2_res_scale_abs_plus1 (c=%d)\n",cIdxMinus1);

  int value = 0;
  int cMax  = 4;
  for (int binIdx=0;binIdx<cMax;binIdx++)
    {
      int ctxIdxInc = 4*cIdxMinus1 + binIdx;

      int bit = decode_CABAC_bit(&tctx->cabac_decoder,
                                 &tctx->ctx_model[CONTEXT_MODEL_LOG2_RES_SCALE_ABS_PLUS1+ctxIdxInc]);
      if (!bit) break;
      value++;
    }

  logtrace(LogSymbols,"$1 log2_res_scale_abs_plus1=%d\n",value);

  return value;
}


static int decode_res_scale_sign_flag(thread_context* tctx, int cIdxMinus1)
{
  //const int context = (cIdx==0) ? 0 : 1;

  logtrace(LogSlice,"# res_scale_sign_flag (c=%d)\n",cIdxMinus1);

  int bit = decode_CABAC_bit(&tctx->cabac_decoder,
                             &tctx->ctx_model[CONTEXT_MODEL_RES_SCALE_SIGN_FLAG+cIdxMinus1]);

  logtrace(LogSymbols,"$1 res_scale_sign_flag=%d\n",bit);

  return bit;
}


static void read_cross_comp_pred(thread_context* tctx, int cIdxMinus1)
{
  int log2_res_scale_abs_plus1 = decode_log2_res_scale_abs_plus1(tctx,cIdxMinus1);
  int ResScaleVal;

  if (log2_res_scale_abs_plus1 != 0) {
    int res_scale_sign_flag = decode_res_scale_sign_flag(tctx,cIdxMinus1);

    ResScaleVal = 1 << (log2_res_scale_abs_plus1 - 1);
    ResScaleVal *= 1 - 2 * res_scale_sign_flag;
  }
  else {
    ResScaleVal = 0;
  }

  tctx->ResScaleVal = ResScaleVal;
}


int read_transform_unit(thread_context* tctx,
                        int x0, int y0,        // position of TU in frame
                        int xBase, int yBase,  // position of parent TU in frame
                        int xCUBase,int yCUBase,  // position of CU in frame
                        int log2TrafoSize,
                        int trafoDepth,
                        int blkIdx,
                        int cbf_luma, int cbf_cb, int cbf_cr)
{
  logtrace(LogSlice,"- read_transform_unit x0:%d y0:%d xBase:%d yBase:%d nT:%d cbf:%d:%d:%d\n",
           x0,y0,xBase,yBase, 1<<log2TrafoSize, cbf_luma, cbf_cb, cbf_cr);

  assert(cbf_cb != -1);
  assert(cbf_cr != -1);
  assert(cbf_luma != -1);

  const seq_parameter_set& sps = tctx->img->get_sps();

  const int ChromaArrayType = sps.ChromaArrayType;

  int log2TrafoSizeC = (ChromaArrayType==CHROMA_444 ? log2TrafoSize : log2TrafoSize-1);
  log2TrafoSizeC = libde265_max(2, log2TrafoSizeC);

  const int cbfLuma   = cbf_luma;
  const int cbfChroma = cbf_cb | cbf_cr;

  tctx->transform_skip_flag[0]=0;
  tctx->transform_skip_flag[1]=0;
  tctx->transform_skip_flag[2]=0;

  tctx->explicit_rdpcm_flag = false;


  enum PredMode cuPredMode = tctx->img->get_pred_mode(x0,y0);

  if (cbfLuma || cbfChroma)
    {
      bool doDecodeQuantParameters = false;

      if (tctx->img->get_pps().cu_qp_delta_enabled_flag &&
          !tctx->IsCuQpDeltaCoded) {

        int cu_qp_delta_abs = decode_cu_qp_delta_abs(tctx);
        int cu_qp_delta_sign=0;
        if (cu_qp_delta_abs) {
          cu_qp_delta_sign = decode_CABAC_bypass(&tctx->cabac_decoder);
        }

        tctx->IsCuQpDeltaCoded = 1;
        tctx->CuQpDelta = cu_qp_delta_abs*(1-2*cu_qp_delta_sign);

        //printf("read cu_qp_delta (%d;%d) = %d\n",x0,y0,tctx->CuQpDelta);

        logtrace(LogSlice,"cu_qp_delta_abs = %d\n",cu_qp_delta_abs);
        logtrace(LogSlice,"cu_qp_delta_sign = %d\n",cu_qp_delta_sign);
        logtrace(LogSlice,"CuQpDelta = %d\n",tctx->CuQpDelta);

        doDecodeQuantParameters = true;
        //decode_quantization_parameters(tctx, x0,y0, xCUBase, yCUBase);
      }

      if (tctx->shdr->cu_chroma_qp_offset_enabled_flag && cbfChroma &&
          !tctx->cu_transquant_bypass_flag && !tctx->IsCuChromaQpOffsetCoded ) {
        logtrace(LogSlice,"# cu_chroma_qp_offset_flag\n");

        int cu_chroma_qp_offset_flag = decode_CABAC_bit(&tctx->cabac_decoder,
                                                        &tctx->ctx_model[CONTEXT_MODEL_CU_CHROMA_QP_OFFSET_FLAG]);


        const pic_parameter_set& pps = tctx->img->get_pps();

        int cu_chroma_qp_offset_idx = 0;
        if (cu_chroma_qp_offset_flag && pps.range_extension.chroma_qp_offset_list_len > 1) {
          cu_chroma_qp_offset_idx = decode_CABAC_bit(&tctx->cabac_decoder,
                                                     &tctx->ctx_model[CONTEXT_MODEL_CU_CHROMA_QP_OFFSET_IDX]);
        }

        tctx->IsCuChromaQpOffsetCoded = 1;

        if (cu_chroma_qp_offset_flag) {
          tctx->CuQpOffsetCb = pps.range_extension.cb_qp_offset_list[ cu_chroma_qp_offset_idx ];
          tctx->CuQpOffsetCr = pps.range_extension.cr_qp_offset_list[ cu_chroma_qp_offset_idx ];
        }
        else {
          tctx->CuQpOffsetCb = 0;
          tctx->CuQpOffsetCr = 0;
        }

        doDecodeQuantParameters = true;
        //decode_quantization_parameters(tctx, x0,y0, xCUBase, yCUBase);
      }


      if (doDecodeQuantParameters) {
        decode_quantization_parameters(tctx, x0,y0, xCUBase, yCUBase);
      }
    }

  // position of TU in local CU
  int xL = x0 - xCUBase;
  int yL = y0 - yCUBase;
  int nT = 1<<log2TrafoSize;
  int nTC = 1<<log2TrafoSizeC;

  const int SubWidthC  = sps.SubWidthC;
  const int SubHeightC = sps.SubHeightC;

  // --- luma ---

  tctx->ResScaleVal = 0;

  int err;
  if (cbf_luma) {
    if ((err=residual_coding(tctx,x0,y0, log2TrafoSize,0)) != DE265_OK) return err;
  }

  decode_TU(tctx, x0,y0, xCUBase,yCUBase, nT, 0, cuPredMode, cbf_luma);


  // --- chroma ---

  const int yOffset422 = 1<<log2TrafoSizeC;

  if (log2TrafoSize>2 || ChromaArrayType == CHROMA_444) {
    // TODO: cross-component prediction

    const bool do_cross_component_prediction =
      (tctx->img->get_pps().range_extension.cross_component_prediction_enabled_flag &&
       cbf_luma &&
       (cuPredMode == MODE_INTER || tctx->img->is_IntraPredModeC_Mode4(x0,y0)));

    if (do_cross_component_prediction) {
      read_cross_comp_pred(tctx, 0);
    }
    else {
      tctx->ResScaleVal = 0;
    }

    {
      if (cbf_cb & 1) {
        if ((err=residual_coding(tctx,x0,y0,log2TrafoSizeC,1)) != DE265_OK) return err;
      }

      if (sps.ChromaArrayType != CHROMA_MONO) {
        decode_TU(tctx,
                  x0/SubWidthC,y0/SubHeightC,
                  xCUBase/SubWidthC,yCUBase/SubHeightC, nTC, 1, cuPredMode, cbf_cb & 1);
      }
    }

    // 4:2:2
    if (ChromaArrayType == CHROMA_422) {
      const int yOffset = 1<<log2TrafoSizeC;

      if (cbf_cb & 2) {
        if ((err=residual_coding(tctx,
                                 x0,y0+yOffset*SubHeightC,
                                 log2TrafoSizeC,1)) != DE265_OK) return err;
      }

      decode_TU(tctx,
                x0/SubWidthC,y0/SubHeightC + yOffset,
                xCUBase/SubWidthC,yCUBase/SubHeightC +yOffset,
                nTC, 1, cuPredMode, cbf_cb & 2);
    }


    if (do_cross_component_prediction) {
      read_cross_comp_pred(tctx, 1);
    }
    else {
      tctx->ResScaleVal = 0;
    }

    {
      if (cbf_cr & 1) {
        if ((err=residual_coding(tctx,x0,y0,log2TrafoSizeC,2)) != DE265_OK) return err;
      }

      if (sps.ChromaArrayType != CHROMA_MONO) {
        decode_TU(tctx,
                  x0/SubWidthC,y0/SubHeightC,
                  xCUBase/SubWidthC,yCUBase/SubHeightC,
                  nTC, 2, cuPredMode, cbf_cr & 1);
      }
    }

    // 4:2:2
    if (ChromaArrayType == CHROMA_422) {
      const int yOffset = 1<<log2TrafoSizeC;

      if (cbf_cr & 2) {
        if ((err=residual_coding(tctx,
                                 x0,y0+yOffset*SubHeightC,
                                 log2TrafoSizeC,2)) != DE265_OK) return err;
      }

      decode_TU(tctx,
                x0/SubWidthC,y0/SubHeightC+yOffset,
                xCUBase/SubWidthC,yCUBase/SubHeightC+yOffset,
                nTC, 2, cuPredMode, cbf_cr & 2);
    }
  }
  else if (blkIdx==3) {
    if (cbf_cb & 1) {
      if ((err=residual_coding(tctx,xBase,yBase,
                               log2TrafoSize,1)) != DE265_OK) return err;
    }

    if (sps.ChromaArrayType != CHROMA_MONO) {
      decode_TU(tctx,
                xBase/SubWidthC,  yBase/SubHeightC,
                xCUBase/SubWidthC,yCUBase/SubHeightC, nT, 1, cuPredMode, cbf_cb & 1);
    }

    // 4:2:2
    if (cbf_cb & 2) {
      if ((err=residual_coding(tctx,
                               xBase        ,yBase        +(1<<log2TrafoSize),
                               log2TrafoSize,1)) != DE265_OK) return err;
    }

    if (ChromaArrayType == CHROMA_422) {
      decode_TU(tctx,
                xBase/SubWidthC,  yBase/SubHeightC + (1<<log2TrafoSize),
                xCUBase/SubWidthC,yCUBase/SubHeightC, nT, 1, cuPredMode, cbf_cb & 2);
    }

    if (cbf_cr & 1) {
      if ((err=residual_coding(tctx,xBase,yBase,
                               log2TrafoSize,2)) != DE265_OK) return err;
    }

    if (sps.ChromaArrayType != CHROMA_MONO) {
      decode_TU(tctx,
                xBase/SubWidthC,  yBase/SubHeightC,
                xCUBase/SubWidthC,yCUBase/SubHeightC, nT, 2, cuPredMode, cbf_cr & 1);
    }

    // 4:2:2
    if (cbf_cr & 2) {
      if ((err=residual_coding(tctx,
                               xBase        ,yBase        +(1<<log2TrafoSizeC),
                               log2TrafoSize,2)) != DE265_OK) return err;
    }

    if (ChromaArrayType == CHROMA_422) {
      decode_TU(tctx,
                xBase/SubWidthC,  yBase/SubHeightC + (1<<log2TrafoSize),
                xCUBase/SubWidthC,yCUBase/SubHeightC, nT, 2, cuPredMode, cbf_cr & 2);
    }
  }


  return DE265_OK;
}


static void dump_cbsize(de265_image* img)
{
  int w = img->get_width(0);
  int h = img->get_height(0);

  for (int y=0;y<h;y+=8) {
    for (int x=0;x<w;x+=8) {
      printf("%d",img->get_log2CbSize(x,y));
    }
    printf("\n");
  }
}


void read_transform_tree(thread_context* tctx,
                         int x0, int y0,        // position of TU in frame
                         int xBase, int yBase,  // position of parent TU in frame
                         int xCUBase, int yCUBase, // position of CU in frame
                         int log2TrafoSize,
                         int trafoDepth,
                         int blkIdx,
                         int MaxTrafoDepth,
                         int IntraSplitFlag,
                         enum PredMode cuPredMode,
                         uint8_t parent_cbf_cb,uint8_t parent_cbf_cr)
{
  logtrace(LogSlice,"- read_transform_tree (interleaved) x0:%d y0:%d xBase:%d yBase:%d "
           "log2TrafoSize:%d trafoDepth:%d MaxTrafoDepth:%d parent-cbf-cb:%d parent-cbf-cr:%d\n",
           x0,y0,xBase,yBase,log2TrafoSize,trafoDepth,MaxTrafoDepth,parent_cbf_cb,parent_cbf_cr);

  de265_image* img = tctx->img;
  const seq_parameter_set& sps = img->get_sps();

  int split_transform_flag;

  enum PredMode PredMode = img->get_pred_mode(x0,y0);
  assert(PredMode == cuPredMode);

  /*  If TrafoSize is larger than maximum size   -> split automatically
      If TrafoSize is at minimum size            -> do not split
      If maximum transformation depth is reached -> do not split
      If intra-prediction is NxN mode            -> split automatically (only at level 0)
      Otherwise  ->  read split flag
  */
  if (log2TrafoSize <= sps.Log2MaxTrafoSize &&
      log2TrafoSize >  sps.Log2MinTrafoSize &&
      trafoDepth < MaxTrafoDepth &&
      !(IntraSplitFlag && trafoDepth==0))
    {
      split_transform_flag = decode_split_transform_flag(tctx, log2TrafoSize);
    }
  else
    {
      enum PartMode PartMode = img->get_PartMode(x0,y0);

      int interSplitFlag= (sps.max_transform_hierarchy_depth_inter==0 &&
                           trafoDepth == 0 &&
                           PredMode == MODE_INTER &&
                           PartMode != PART_2Nx2N);

      split_transform_flag = (log2TrafoSize > sps.Log2MaxTrafoSize ||
                              (IntraSplitFlag==1 && trafoDepth==0) ||
                              interSplitFlag==1) ? 1:0;
    }

  if (split_transform_flag) {
    logtrace(LogSlice,"set_split_transform_flag(%d,%d, %d)\n",x0,y0,trafoDepth);
    img->set_split_transform_flag(x0,y0,trafoDepth);
  }

  int cbf_cb=-1;
  int cbf_cr=-1;

  // CBF_CB/CR flags are encoded like this:
  // 4:2:0 and 4:4:4 modes: binary flag in bit 0
  // 4:2:2 mode: bit 0: top block, bit 1: bottom block

  if ((log2TrafoSize>2 && sps.ChromaArrayType != CHROMA_MONO) ||
      sps.ChromaArrayType == CHROMA_444) {
    // we do not have to test for trafoDepth==0, because parent_cbf_cb is 1 at depth 0
    if (/*trafoDepth==0 ||*/ parent_cbf_cb) {
      cbf_cb = decode_cbf_chroma(tctx,trafoDepth);

      if (sps.ChromaArrayType == CHROMA_422 && (!split_transform_flag || log2TrafoSize==3)) {
        cbf_cb |= (decode_cbf_chroma(tctx,trafoDepth) << 1);
      }
    }

    // we do not have to test for trafoDepth==0, because parent_cbf_cb is 1 at depth 0
    if (/*trafoDepth==0 ||*/ parent_cbf_cr) {
      cbf_cr = decode_cbf_chroma(tctx,trafoDepth);

      if (sps.ChromaArrayType == CHROMA_422 && (!split_transform_flag || log2TrafoSize==3)) {
        cbf_cr |= (decode_cbf_chroma(tctx,trafoDepth) << 1);
      }
    }
  }

  //printf("CBF: cb:%d cr:%d\n",cbf_cb,cbf_cr);

  // cbf_cr/cbf_cb not present in bitstream -> induce values

  if (cbf_cb<0) {
    assert(!(trafoDepth==0 && log2TrafoSize==2));

    /* The standard specifies to check trafoDepth>0 AND log2TrafoSize==2.
       However, I think that trafoDepth>0 is redundant as a CB is always
       at least 8x8 and hence trafoDepth>0.
    */

    if (trafoDepth>0 && log2TrafoSize==2) {
      cbf_cb = parent_cbf_cb;
    } else {
      cbf_cb=0;
    }
  }

  if (cbf_cr<0) {
    if (trafoDepth>0 && log2TrafoSize==2) {
      cbf_cr = parent_cbf_cr;
    } else {
      cbf_cr=0;
    }
  }

  if (split_transform_flag) {
    int x1 = x0 + (1<<(log2TrafoSize-1));
    int y1 = y0 + (1<<(log2TrafoSize-1));

    logtrace(LogSlice,"transform split.\n");

    read_transform_tree(tctx, x0,y0, x0,y0, xCUBase,yCUBase, log2TrafoSize-1, trafoDepth+1, 0,
                        MaxTrafoDepth,IntraSplitFlag, cuPredMode, cbf_cb,cbf_cr);
    read_transform_tree(tctx, x1,y0, x0,y0, xCUBase,yCUBase, log2TrafoSize-1, trafoDepth+1, 1,
                        MaxTrafoDepth,IntraSplitFlag, cuPredMode, cbf_cb,cbf_cr);
    read_transform_tree(tctx, x0,y1, x0,y0, xCUBase,yCUBase, log2TrafoSize-1, trafoDepth+1, 2,
                        MaxTrafoDepth,IntraSplitFlag, cuPredMode, cbf_cb,cbf_cr);
    read_transform_tree(tctx, x1,y1, x0,y0, xCUBase,yCUBase, log2TrafoSize-1, trafoDepth+1, 3,
                        MaxTrafoDepth,IntraSplitFlag, cuPredMode, cbf_cb,cbf_cr);
  }
  else {
    int cbf_luma;

    if (PredMode==MODE_INTRA || trafoDepth!=0 || cbf_cb || cbf_cr) {
      cbf_luma = decode_cbf_luma(tctx,trafoDepth);
    }
    else {
      /* There cannot be INTER blocks with no residual data.
         That case is already handled with rqt_root_cbf.
      */

      cbf_luma = 1;
    }

    logtrace(LogSlice,"call read_transform_unit %d/%d\n",x0,y0);

    read_transform_unit(tctx, x0,y0,xBase,yBase, xCUBase,yCUBase, log2TrafoSize,trafoDepth, blkIdx,
                        cbf_luma, cbf_cb, cbf_cr);
  }
}


const char* part_mode_name(enum PartMode pm)
{
  switch (pm) {
  case PART_2Nx2N: return "2Nx2N";
  case PART_2NxN:  return "2NxN";
  case PART_Nx2N:  return "Nx2N";
  case PART_NxN:   return "NxN";
  case PART_2NxnU: return "2NxnU";
  case PART_2NxnD: return "2NxnD";
  case PART_nLx2N: return "nLx2N";
  case PART_nRx2N: return "nRx2N";
  }

  return "undefined part mode";
}


void read_mvd_coding(thread_context* tctx,
                     int x0,int y0, int refList)
{
  int abs_mvd_greater0_flag[2];
  abs_mvd_greater0_flag[0] = decode_CABAC_bit(&tctx->cabac_decoder,
                                              &tctx->ctx_model[CONTEXT_MODEL_ABS_MVD_GREATER01_FLAG+0]);
  abs_mvd_greater0_flag[1] = decode_CABAC_bit(&tctx->cabac_decoder,
                                              &tctx->ctx_model[CONTEXT_MODEL_ABS_MVD_GREATER01_FLAG+0]);

  int abs_mvd_greater1_flag[2];
  if (abs_mvd_greater0_flag[0]) {
    abs_mvd_greater1_flag[0] = decode_CABAC_bit(&tctx->cabac_decoder,
                                                &tctx->ctx_model[CONTEXT_MODEL_ABS_MVD_GREATER01_FLAG+1]);
  }
  else {
    abs_mvd_greater1_flag[0]=0;
  }

  if (abs_mvd_greater0_flag[1]) {
    abs_mvd_greater1_flag[1] = decode_CABAC_bit(&tctx->cabac_decoder,
                                                &tctx->ctx_model[CONTEXT_MODEL_ABS_MVD_GREATER01_FLAG+1]);
  }
  else {
    abs_mvd_greater1_flag[1]=0;
  }


  int abs_mvd_minus2[2];
  int mvd_sign_flag[2];
  int value[2];

  for (int c=0;c<2;c++) {
    if (abs_mvd_greater0_flag[c]) {
      if (abs_mvd_greater1_flag[c]) {
        abs_mvd_minus2[c] = decode_CABAC_EGk_bypass(&tctx->cabac_decoder, 1);
      }
      else {
        abs_mvd_minus2[c] = abs_mvd_greater1_flag[c] -1;
      }

      mvd_sign_flag[c] = decode_CABAC_bypass(&tctx->cabac_decoder);

      value[c] = abs_mvd_minus2[c]+2;
      if (mvd_sign_flag[c]) { value[c] = -value[c]; }
    }
    else {
      value[c] = 0;
    }
  }

  //set_mvd(tctx->decctx, x0,y0, refList, value[0],value[1]);
  tctx->motion.mvd[refList][0] = value[0];
  tctx->motion.mvd[refList][1] = value[1];

  logtrace(LogSlice, "MVD[%d;%d|%d] = %d;%d\n",x0,y0,refList, value[0],value[1]);
}


void read_prediction_unit_SKIP(thread_context* tctx,
                               int x0, int y0,
                               int nPbW, int nPbH)
{
  int merge_idx = decode_merge_idx(tctx);

  tctx->motion.merge_idx = merge_idx;
  tctx->motion.merge_flag = true;

  logtrace(LogSlice,"prediction skip 2Nx2N, merge_idx: %d\n",merge_idx);
}


/* xC/yC : CB position
   xB/yB : position offset of the PB
   nPbW/nPbH : size of PB
   nCS   : CB size
 */
void read_prediction_unit(thread_context* tctx,
                          int xC,int yC, int xB,int yB,
                          int nPbW, int nPbH,
                          int ctDepth, int nCS,int partIdx)
{
  logtrace(LogSlice,"read_prediction_unit %d;%d %dx%d\n",xC+xB,yC+xB,nPbW,nPbH);

  int x0 = xC+xB;
  int y0 = yC+yB;

  slice_segment_header* shdr = tctx->shdr;

  int merge_flag = decode_merge_flag(tctx);
  tctx->motion.merge_flag = merge_flag;

  if (merge_flag) {
    int merge_idx = decode_merge_idx(tctx);

    logtrace(LogSlice,"prediction unit %d,%d, merge mode, index: %d\n",x0,y0,merge_idx);

    tctx->motion.merge_idx = merge_idx;
  }
  else { // no merge flag
    enum InterPredIdc inter_pred_idc;

    if (shdr->slice_type == SLICE_TYPE_B) {
      inter_pred_idc = decode_inter_pred_idc(tctx,x0,y0,nPbW,nPbH,ctDepth);
    }
    else {
      inter_pred_idc = PRED_L0;
    }

    tctx->motion.inter_pred_idc = inter_pred_idc; // set_inter_pred_idc(ctx,x0,y0, inter_pred_idc);

    if (inter_pred_idc != PRED_L1) {
      int ref_idx_l0 = decode_ref_idx_lX(tctx, shdr->num_ref_idx_l0_active);

      // NOTE: case for only one reference frame is handles in decode_ref_idx_lX()
      tctx->motion.refIdx[0] = ref_idx_l0;

      read_mvd_coding(tctx,x0,y0, 0);

      int mvp_l0_flag = decode_mvp_lx_flag(tctx); // l0
      tctx->motion.mvp_l0_flag = mvp_l0_flag;

      logtrace(LogSlice,"prediction unit %d,%d, L0, refIdx=%d mvp_l0_flag:%d\n",
               x0,y0, tctx->motion.refIdx[0], mvp_l0_flag);
    }

    if (inter_pred_idc != PRED_L0) {
      int ref_idx_l1 = decode_ref_idx_lX(tctx, shdr->num_ref_idx_l1_active);

      // NOTE: case for only one reference frame is handles in decode_ref_idx_lX()
      tctx->motion.refIdx[1] = ref_idx_l1;

      if (shdr->mvd_l1_zero_flag &&
          inter_pred_idc == PRED_BI) {
        tctx->motion.mvd[1][0] = 0;
        tctx->motion.mvd[1][1] = 0;
      }
      else {
        read_mvd_coding(tctx,x0,y0, 1);
      }

      int mvp_l1_flag = decode_mvp_lx_flag(tctx); // l1
      tctx->motion.mvp_l1_flag = mvp_l1_flag;

      logtrace(LogSlice,"prediction unit %d,%d, L1, refIdx=%d mvp_l1_flag:%d\n",
               x0,y0, tctx->motion.refIdx[1], mvp_l1_flag);
    }
  }



  decode_prediction_unit(tctx->decctx, tctx->shdr, tctx->img, tctx->motion,
                         xC,yC,xB,yB, nCS, nPbW,nPbH, partIdx);
}




template <class pixel_t>
void read_pcm_samples_internal(thread_context* tctx, int x0, int y0, int log2CbSize,
                               int cIdx, bitreader& br)
{
  const seq_parameter_set& sps = tctx->img->get_sps();

  int nPcmBits;
  int bitDepth;

  int w = 1<<log2CbSize;
  int h = 1<<log2CbSize;

  if (cIdx>0) {
    w /= sps.SubWidthC;
    h /= sps.SubHeightC;

    x0 /= sps.SubWidthC;
    y0 /= sps.SubHeightC;

    nPcmBits = sps.pcm_sample_bit_depth_chroma;
    bitDepth = sps.BitDepth_C;
  }
  else {
    nPcmBits = sps.pcm_sample_bit_depth_luma;
    bitDepth = sps.BitDepth_Y;
  }

  pixel_t* ptr;
  int stride;
  ptr    = tctx->img->get_image_plane_at_pos_NEW<pixel_t>(cIdx,x0,y0);
  stride = tctx->img->get_image_stride(cIdx);

  int shift = bitDepth - nPcmBits;

  // a shift < 0 may result when the SPS sequence header is broken
  if (shift < 0) {
    shift=0;
  }

  for (int y=0;y<h;y++)
    for (int x=0;x<w;x++)
      {
        int value = get_bits(&br, nPcmBits);
        ptr[y*stride+x] = value << shift;
      }
}

static void read_pcm_samples(thread_context* tctx, int x0, int y0, int log2CbSize)
{
  bitreader br;
  br.data            = tctx->cabac_decoder.bitstream_curr;
  br.bytes_remaining = tctx->cabac_decoder.bitstream_end - tctx->cabac_decoder.bitstream_curr;
  br.nextbits = 0;
  br.nextbits_cnt = 0;


  if (tctx->img->high_bit_depth(0)) {
    read_pcm_samples_internal<uint16_t>(tctx,x0,y0,log2CbSize,0,br);
  } else {
    read_pcm_samples_internal<uint8_t>(tctx,x0,y0,log2CbSize,0,br);
  }

  if (tctx->img->get_sps().ChromaArrayType != CHROMA_MONO) {
    if (tctx->img->high_bit_depth(1)) {
      read_pcm_samples_internal<uint16_t>(tctx,x0,y0,log2CbSize,1,br);
      read_pcm_samples_internal<uint16_t>(tctx,x0,y0,log2CbSize,2,br);
    } else {
      read_pcm_samples_internal<uint8_t>(tctx,x0,y0,log2CbSize,1,br);
      read_pcm_samples_internal<uint8_t>(tctx,x0,y0,log2CbSize,2,br);
    }
  }

  prepare_for_CABAC(&br);
  tctx->cabac_decoder.bitstream_curr = br.data;
  init_CABAC_decoder_2(&tctx->cabac_decoder);
}


int map_chroma_pred_mode(int intra_chroma_pred_mode, int IntraPredMode)
{
  if (intra_chroma_pred_mode==4) {
    return IntraPredMode;
  }
  else {
    static const enum IntraPredMode IntraPredModeCCand[4] = {
      INTRA_PLANAR,
      INTRA_ANGULAR_26, // vertical
      INTRA_ANGULAR_10, // horizontal
      INTRA_DC
    };

    int IntraPredModeC = IntraPredModeCCand[intra_chroma_pred_mode];
    if (IntraPredModeC == IntraPredMode) {
      return INTRA_ANGULAR_34;
    }
    else {
      return IntraPredModeC;
    }
  }
}

// h.265-V2 Table 8-3
static const uint8_t map_chroma_422[35] = {
  0,1,2, 2, 2, 2, 3, 5, 7, 8,10,12,13,15,17,18,19,20,
  21,22,23,23,24,24,25,25,26,27,27,28,28,29,29,30,31
};

void read_coding_unit(thread_context* tctx,
                      int x0, int y0,  // position of coding unit in frame
                      int log2CbSize,
                      int ctDepth)
{
  de265_image* img = tctx->img;
  const seq_parameter_set& sps = img->get_sps();
  const pic_parameter_set& pps = img->get_pps();
  slice_segment_header* shdr = tctx->shdr;

  logtrace(LogSlice,"- read_coding_unit %d;%d cbsize:%d\n",x0,y0,1<<log2CbSize);


  //QQprintf("- read_coding_unit %d;%d cbsize:%d\n",x0,y0,1<<log2CbSize);

  img->set_log2CbSize(x0,y0, log2CbSize, true);

  /* This is only required on corrupted input streams.
     It may happen that there are several slices in the image that overlap.
     In this case, flags would accumulate from both slices.
  */
  img->clear_split_transform_flags(x0,y0, log2CbSize);

  int nCbS = 1<<log2CbSize; // number of coding block samples

  decode_quantization_parameters(tctx, x0,y0, x0, y0);


  if (pps.transquant_bypass_enable_flag)
    {
      int transquant_bypass = decode_transquant_bypass_flag(tctx);

      tctx->cu_transquant_bypass_flag = transquant_bypass;

      if (transquant_bypass) {
        img->set_cu_transquant_bypass(x0,y0,log2CbSize);
      }
    }
  else {
    tctx->cu_transquant_bypass_flag = 0;
  }

  uint8_t cu_skip_flag = 0;
  if (shdr->slice_type != SLICE_TYPE_I) {
    cu_skip_flag = decode_cu_skip_flag(tctx,x0,y0,ctDepth);
  }

  int IntraSplitFlag = 0;

  enum PredMode cuPredMode;

  if (cu_skip_flag) {
    read_prediction_unit_SKIP(tctx,x0,y0,nCbS,nCbS);

    img->set_PartMode(x0,y0, PART_2Nx2N); // need this for deblocking filter
    img->set_pred_mode(x0,y0,log2CbSize, MODE_SKIP);
    cuPredMode = MODE_SKIP;

    logtrace(LogSlice,"CU pred mode: SKIP\n");


    // DECODE

    int nCS_L = 1<<log2CbSize;
    decode_prediction_unit(tctx->decctx,tctx->shdr,tctx->img,tctx->motion,
                           x0,y0, 0,0, nCS_L, nCS_L,nCS_L, 0);
  }
  else /* not skipped */ {
    if (shdr->slice_type != SLICE_TYPE_I) {
      int pred_mode_flag = decode_pred_mode_flag(tctx);
      cuPredMode = pred_mode_flag ? MODE_INTRA : MODE_INTER;
    }
    else {
      cuPredMode = MODE_INTRA;
    }

    img->set_pred_mode(x0,y0,log2CbSize, cuPredMode);

    logtrace(LogSlice,"CU pred mode: %s\n", cuPredMode==MODE_INTRA ? "INTRA" : "INTER");


    enum PartMode PartMode;

    if (cuPredMode != MODE_INTRA ||
        log2CbSize == sps.Log2MinCbSizeY) {
      PartMode = decode_part_mode(tctx, cuPredMode, log2CbSize);

      if (PartMode==PART_NxN && cuPredMode==MODE_INTRA) {
        IntraSplitFlag=1;
      }
    } else {
      PartMode = PART_2Nx2N;
    }

    img->set_PartMode(x0,y0, PartMode); // needed for deblocking ?

    logtrace(LogSlice, "PartMode: %s\n", part_mode_name(PartMode));


    bool pcm_flag = false;

    if (cuPredMode == MODE_INTRA) {
      if (PartMode == PART_2Nx2N && sps.pcm_enabled_flag &&
          log2CbSize >= sps.Log2MinIpcmCbSizeY &&
          log2CbSize <= sps.Log2MaxIpcmCbSizeY) {
        pcm_flag = decode_CABAC_term_bit(&tctx->cabac_decoder);
      }

      if (pcm_flag) {
        img->set_pcm_flag(x0,y0,log2CbSize);

        read_pcm_samples(tctx, x0,y0, log2CbSize);
      }
      else {
        int pbOffset = (PartMode == PART_NxN) ? (nCbS/2) : nCbS;
        int log2IntraPredSize = (PartMode == PART_NxN) ? (log2CbSize-1) : log2CbSize;

        logtrace(LogSlice,"nCbS:%d pbOffset:%d\n",nCbS,pbOffset);

        int prev_intra_luma_pred_flag[4];

        int idx=0;
        for (int j=0;j<nCbS;j+=pbOffset)
          for (int i=0;i<nCbS;i+=pbOffset)
            {
              prev_intra_luma_pred_flag[idx++] = decode_prev_intra_luma_pred_flag(tctx);
            }

        int mpm_idx[4], rem_intra_luma_pred_mode[4];
        idx=0;

        int availableA0 = check_CTB_available(img, x0,y0, x0-1,y0);
        int availableB0 = check_CTB_available(img, x0,y0, x0,y0-1);

        for (int j=0;j<nCbS;j+=pbOffset)
          for (int i=0;i<nCbS;i+=pbOffset)
            {
              if (prev_intra_luma_pred_flag[idx]) {
                mpm_idx[idx] = decode_mpm_idx(tctx);
              }
              else {
                rem_intra_luma_pred_mode[idx] = decode_rem_intra_luma_pred_mode(tctx);
              }


              int x = x0+i;
              int y = y0+j;

              // --- find intra prediction mode ---

              int IntraPredMode;

              int availableA = availableA0 || (i>0); // left candidate always available for right blk
              int availableB = availableB0 || (j>0); // top candidate always available for bottom blk



              int PUidx = (x>>sps.Log2MinPUSize) + (y>>sps.Log2MinPUSize)*sps.PicWidthInMinPUs;

              enum IntraPredMode candModeList[3];

              fillIntraPredModeCandidates(candModeList,x,y,PUidx,
                                          availableA, availableB, img);

              for (int i=0;i<3;i++)
                logtrace(LogSlice,"candModeList[%d] = %d\n", i, candModeList[i]);

              if (prev_intra_luma_pred_flag[idx]==1) {
                IntraPredMode = candModeList[ mpm_idx[idx] ];
              }
              else {
                // sort candModeList

                if (candModeList[0] > candModeList[1]) {
                  std::swap(candModeList[0],candModeList[1]);
                }
                if (candModeList[0] > candModeList[2]) {
                  std::swap(candModeList[0],candModeList[2]);
                }
                if (candModeList[1] > candModeList[2]) {
                  std::swap(candModeList[1],candModeList[2]);
                }

                // skip modes in the list
                // (we have 35 modes. skipping the 3 in the list gives us 32, which can be selected by 5 bits)
                IntraPredMode = rem_intra_luma_pred_mode[idx];
                for (int n=0;n<=2;n++) {
                  if (IntraPredMode >= candModeList[n]) { IntraPredMode++; }
                }
              }

              logtrace(LogSlice,"IntraPredMode[%d][%d] = %d (log2blk:%d)\n",x,y,IntraPredMode, log2IntraPredSize);

              img->set_IntraPredMode(PUidx, log2IntraPredSize,
                                     (enum IntraPredMode)IntraPredMode);

              idx++;
            }


        // set chroma intra prediction mode

        if (sps.ChromaArrayType == CHROMA_444) {
          // chroma 4:4:4

          idx = 0;
          for (int j=0;j<nCbS;j+=pbOffset)
            for (int i=0;i<nCbS;i+=pbOffset) {
              int x = x0+i;
              int y = y0+j;

              int intra_chroma_pred_mode = decode_intra_chroma_pred_mode(tctx);
              int IntraPredMode = img->get_IntraPredMode(x,y);

              int IntraPredModeC = map_chroma_pred_mode(intra_chroma_pred_mode, IntraPredMode);

              logtrace(LogSlice,"IntraPredModeC[%d][%d]: %d (blksize:%d)\n",x,y,IntraPredModeC,
                       1<<log2IntraPredSize);

              img->set_IntraPredModeC(x,y, log2IntraPredSize,
                                      (enum IntraPredMode)IntraPredModeC,
                                      intra_chroma_pred_mode == 4);
              idx++;
            }
        }
        else if (sps.ChromaArrayType != CHROMA_MONO) {
          // chroma 4:2:0 and 4:2:2

          int intra_chroma_pred_mode = decode_intra_chroma_pred_mode(tctx);
          int IntraPredMode = img->get_IntraPredMode(x0,y0);
          logtrace(LogSlice,"IntraPredMode: %d\n",IntraPredMode);
          int IntraPredModeC = map_chroma_pred_mode(intra_chroma_pred_mode, IntraPredMode);

          if (sps.ChromaArrayType == CHROMA_422) {
            IntraPredModeC = map_chroma_422[ IntraPredModeC ];
          }

          img->set_IntraPredModeC(x0,y0, log2CbSize,
                                  (enum IntraPredMode)IntraPredModeC,
                                  intra_chroma_pred_mode == 4);
        }
      }
    }
    else { // INTER
      int nCS = 1<<log2CbSize;

      if (PartMode == PART_2Nx2N) {
        read_prediction_unit(tctx,x0,y0,0,0,nCbS,nCbS,ctDepth,nCS,0);
      }
      else if (PartMode == PART_2NxN) {
        read_prediction_unit(tctx,x0,y0,0,0     ,nCbS,nCbS/2,ctDepth,nCS,0);
        read_prediction_unit(tctx,x0,y0,0,nCbS/2,nCbS,nCbS/2,ctDepth,nCS,1);
      }
      else if (PartMode == PART_Nx2N) {
        read_prediction_unit(tctx,x0,y0,0,0  ,   nCbS/2,nCbS,ctDepth,nCS,0);
        read_prediction_unit(tctx,x0,y0,nCbS/2,0,nCbS/2,nCbS,ctDepth,nCS,1);
      }
      else if (PartMode == PART_2NxnU) {
        read_prediction_unit(tctx,x0,y0,0,0,     nCbS,nCbS/4,ctDepth,nCS,0);
        read_prediction_unit(tctx,x0,y0,0,nCbS/4,nCbS,nCbS*3/4,ctDepth,nCS,1);
      }
      else if (PartMode == PART_2NxnD) {
        read_prediction_unit(tctx,x0,y0,0,0,       nCbS,nCbS*3/4,ctDepth,nCS,0);
        read_prediction_unit(tctx,x0,y0,0,nCbS*3/4,nCbS,nCbS/4,ctDepth,nCS,1);
      }
      else if (PartMode == PART_nLx2N) {
        read_prediction_unit(tctx,x0,y0,0,0,     nCbS/4,nCbS,ctDepth,nCS,0);
        read_prediction_unit(tctx,x0,y0,nCbS/4,0,nCbS*3/4,nCbS,ctDepth,nCS,1);
      }
      else if (PartMode == PART_nRx2N) {
        read_prediction_unit(tctx,x0,y0,0,0,       nCbS*3/4,nCbS,ctDepth,nCS,0);
        read_prediction_unit(tctx,x0,y0,nCbS*3/4,0,nCbS/4,nCbS,ctDepth,nCS,1);
      }
      else if (PartMode == PART_NxN) {
        read_prediction_unit(tctx,x0,y0,0,0,          nCbS/2,nCbS/2,ctDepth,nCS,0);
        read_prediction_unit(tctx,x0,y0,nCbS/2,0,     nCbS/2,nCbS/2,ctDepth,nCS,1);
        read_prediction_unit(tctx,x0,y0,0,nCbS/2,     nCbS/2,nCbS/2,ctDepth,nCS,2);
        read_prediction_unit(tctx,x0,y0,nCbS/2,nCbS/2,nCbS/2,nCbS/2,ctDepth,nCS,3);
      }
      else {
        assert(0); // undefined PartMode
      }
    } // INTER


    // decode residual

    if (!pcm_flag) { // !pcm
      bool rqt_root_cbf;

      uint8_t merge_flag = tctx->motion.merge_flag; // !!get_merge_flag(ctx,x0,y0);

      if (cuPredMode != MODE_INTRA &&
          !(PartMode == PART_2Nx2N && merge_flag)) {

        rqt_root_cbf = !!decode_rqt_root_cbf(tctx);
      }
      else {
        /* rqt_root_cbf=1 is inferred for Inter blocks with 2Nx2N, merge mode.
           These must be some residual data, because otherwise, the CB could
           also be coded in SKIP mode.
         */

        rqt_root_cbf = true;
      }

      //set_rqt_root_cbf(ctx,x0,y0, log2CbSize, rqt_root_cbf);

      if (rqt_root_cbf) {
        int MaxTrafoDepth;

        if (cuPredMode==MODE_INTRA) {
          MaxTrafoDepth = sps.max_transform_hierarchy_depth_intra + IntraSplitFlag;
        }
        else {
          MaxTrafoDepth = sps.max_transform_hierarchy_depth_inter;
        }

        logtrace(LogSlice,"MaxTrafoDepth: %d\n",MaxTrafoDepth);

        uint8_t initial_chroma_cbf = 1;
        if (sps.ChromaArrayType == CHROMA_MONO) {
          initial_chroma_cbf = 0;
        }

        read_transform_tree(tctx, x0,y0, x0,y0, x0,y0, log2CbSize, 0,0,
                            MaxTrafoDepth, IntraSplitFlag, cuPredMode,
                            initial_chroma_cbf, initial_chroma_cbf);
      }
    } // !pcm
  }
}


// ------------------------------------------------------------------------------------------


void read_coding_quadtree(thread_context* tctx,
                          int x0, int y0,
                          int log2CbSize,
                          int ctDepth)
{
  logtrace(LogSlice,"- read_coding_quadtree %d;%d cbsize:%d depth:%d POC:%d\n",x0,y0,1<<log2CbSize,ctDepth,tctx->img->PicOrderCntVal);

  de265_image* img = tctx->img;
  const seq_parameter_set& sps = img->get_sps();
  const pic_parameter_set& pps = img->get_pps();

  int split_flag;

  // We only send a split flag if CU is larger than minimum size and
  // completely contained within the image area.
  // If it is partly outside the image area and not at minimum size,
  // it is split. If already at minimum size, it is not split further.
  if (x0+(1<<log2CbSize) <= sps.pic_width_in_luma_samples &&
      y0+(1<<log2CbSize) <= sps.pic_height_in_luma_samples &&
      log2CbSize > sps.Log2MinCbSizeY) {
    split_flag = decode_split_cu_flag(tctx, x0,y0, ctDepth);
  } else {
    if (log2CbSize > sps.Log2MinCbSizeY) { split_flag=1; }
    else                                  { split_flag=0; }
  }


  if (pps.cu_qp_delta_enabled_flag &&
      log2CbSize >= pps.Log2MinCuQpDeltaSize)
    {
      tctx->IsCuQpDeltaCoded = 0;
      tctx->CuQpDelta = 0;
    }
  else
    {
      // shdr->CuQpDelta = 0; // TODO check: is this the right place to set to default value ?
    }


  if (tctx->shdr->cu_chroma_qp_offset_enabled_flag &&
      log2CbSize >= pps.Log2MinCuChromaQpOffsetSize) {
    tctx->IsCuChromaQpOffsetCoded = 0;
  }

  if (split_flag) {
    int x1 = x0 + (1<<(log2CbSize-1));
    int y1 = y0 + (1<<(log2CbSize-1));

    read_coding_quadtree(tctx,x0,y0, log2CbSize-1, ctDepth+1);

    if (x1<sps.pic_width_in_luma_samples)
      read_coding_quadtree(tctx,x1,y0, log2CbSize-1, ctDepth+1);

    if (y1<sps.pic_height_in_luma_samples)
      read_coding_quadtree(tctx,x0,y1, log2CbSize-1, ctDepth+1);

    if (x1<sps.pic_width_in_luma_samples &&
        y1<sps.pic_height_in_luma_samples)
      read_coding_quadtree(tctx,x1,y1, log2CbSize-1, ctDepth+1);
  }
  else {
    // set ctDepth of this CU

    img->set_ctDepth(x0,y0, log2CbSize, ctDepth);

    read_coding_unit(tctx, x0,y0, log2CbSize, ctDepth);
  }

  logtrace(LogSlice,"-\n");
}


// ---------------------------------------------------------------------------

enum DecodeResult {
  Decode_EndOfSliceSegment,
  Decode_EndOfSubstream,
  Decode_Error
};

/* Decode CTBs until the end of sub-stream, the end-of-slice, or some error occurs.
 */
enum DecodeResult decode_substream(thread_context* tctx,
                                   bool block_wpp, // block on WPP dependencies
                                   bool first_independent_substream)
{
  const pic_parameter_set& pps = tctx->img->get_pps();
  const seq_parameter_set& sps = tctx->img->get_sps();

  const int ctbW = sps.PicWidthInCtbsY;


  const int startCtbY = tctx->CtbY;

  //printf("start decoding substream at %d;%d\n",tctx->CtbX,tctx->CtbY);

  // in WPP mode: initialize CABAC model with stored model from row above

  if ((!first_independent_substream || tctx->CtbY != startCtbY) &&
      pps.entropy_coding_sync_enabled_flag &&
      tctx->CtbY>=1 && tctx->CtbX==0)
    {
      if (sps.PicWidthInCtbsY>1) {
        if ((tctx->CtbY-1) >= tctx->imgunit->ctx_models.size()) {
          return Decode_Error;
        }

        //printf("CTX wait on %d/%d\n",1,tctx->CtbY-1);

        // we have to wait until the context model data is there
        tctx->img->wait_for_progress(tctx->task, 1,tctx->CtbY-1,CTB_PROGRESS_PREFILTER);

        // copy CABAC model from previous CTB row
        tctx->ctx_model = tctx->imgunit->ctx_models[(tctx->CtbY-1)];
        tctx->imgunit->ctx_models[(tctx->CtbY-1)].release(); // not used anymore
      }
      else {
        tctx->img->wait_for_progress(tctx->task, 0,tctx->CtbY-1,CTB_PROGRESS_PREFILTER);
        initialize_CABAC_models(tctx);
      }
    }


  do {
    const int ctbx = tctx->CtbX;
    const int ctby = tctx->CtbY;

    if (ctbx+ctby*ctbW >= pps.CtbAddrRStoTS.size()) {
        return Decode_Error;
    }

    if (ctbx >= sps.PicWidthInCtbsY ||
        ctby >= sps.PicHeightInCtbsY) {
        return Decode_Error;
    }

    if (block_wpp && ctby>0 && ctbx < ctbW-1) {

      // TODO: if we are in tiles mode and at the right border, do not wait for x+1,y-1

      //printf("wait on %d/%d (%d)\n",ctbx+1,ctby-1, ctbx+1+(ctby-1)*sps->PicWidthInCtbsY);

      tctx->img->wait_for_progress(tctx->task, ctbx+1,ctby-1, CTB_PROGRESS_PREFILTER);
    }

    //printf("%p: decode %d;%d\n", tctx, tctx->CtbX,tctx->CtbY);


    // read and decode CTB

    if (tctx->ctx_model.empty() == false) {
      return Decode_Error;
    }

    read_coding_tree_unit(tctx);


    // save CABAC-model for WPP (except in last CTB row)

    if (pps.entropy_coding_sync_enabled_flag &&
        ctbx == 1 &&
        ctby < sps.PicHeightInCtbsY-1)
      {
        // no storage for context table has been allocated
        if (tctx->imgunit->ctx_models.size() <= ctby) {
          return Decode_Error;
        }

        tctx->imgunit->ctx_models[ctby] = tctx->ctx_model;
        tctx->imgunit->ctx_models[ctby].decouple(); // store an independent copy
      }


    // end of slice segment ?

    int end_of_slice_segment_flag = decode_CABAC_term_bit(&tctx->cabac_decoder);
    //printf("end-of-slice flag: %d\n", end_of_slice_segment_flag);

    if (end_of_slice_segment_flag) {
      // at the end of the slice segment, we store the CABAC model if we need it
      // because a dependent slice may follow

      if (pps.dependent_slice_segments_enabled_flag) {
        tctx->shdr->ctx_model_storage = tctx->ctx_model;
        tctx->shdr->ctx_model_storage.decouple(); // store an independent copy

        tctx->shdr->ctx_model_storage_defined = true;
      }
    }

    tctx->img->ctb_progress[ctbx+ctby*ctbW].set_progress(CTB_PROGRESS_PREFILTER);

    //printf("%p: decoded %d|%d\n",tctx, ctby,ctbx);


    logtrace(LogSlice,"read CTB %d -> end=%d\n", tctx->CtbAddrInRS, end_of_slice_segment_flag);
    //printf("read CTB %d -> end=%d\n", tctx->CtbAddrInRS, end_of_slice_segment_flag);

    const int lastCtbY = tctx->CtbY;

    bool endOfPicture = advanceCtbAddr(tctx); // true if we read past the end of the image

    if (endOfPicture &&
        end_of_slice_segment_flag == false)
      {
        tctx->decctx->add_warning(DE265_WARNING_CTB_OUTSIDE_IMAGE_AREA, false);
        tctx->img->integrity = INTEGRITY_DECODING_ERRORS;
        return Decode_Error;
      }


    if (end_of_slice_segment_flag) {
      /* corrupted inputs may send the end_of_slice_segment_flag even if not all
         CTBs in a row have been coded. Hence, we mark all of them as finished.
       */

      /*
      for (int x = ctbx+1 ; x<sps->PicWidthInCtbsY; x++) {
        printf("mark skipped %d;%d\n",ctbx,ctby);
        tctx->img->ctb_progress[ctbx+ctby*ctbW].set_progress(CTB_PROGRESS_PREFILTER);
      }
      */

      return Decode_EndOfSliceSegment;
    }


    if (!end_of_slice_segment_flag) {
      bool end_of_sub_stream = false;
      end_of_sub_stream |= (pps.tiles_enabled_flag &&
                            pps.TileId[tctx->CtbAddrInTS] != pps.TileId[tctx->CtbAddrInTS-1]);
      end_of_sub_stream |= (pps.entropy_coding_sync_enabled_flag &&
                            lastCtbY != tctx->CtbY);

      if (end_of_sub_stream) {
        int end_of_sub_stream_one_bit = decode_CABAC_term_bit(&tctx->cabac_decoder);
        if (!end_of_sub_stream_one_bit) {
          tctx->decctx->add_warning(DE265_WARNING_EOSS_BIT_NOT_SET, false);
          tctx->img->integrity = INTEGRITY_DECODING_ERRORS;
          return Decode_Error;
        }

        init_CABAC_decoder_2(&tctx->cabac_decoder); // byte alignment
        return Decode_EndOfSubstream;
      }
    }

  } while (true);
}



bool initialize_CABAC_at_slice_segment_start(thread_context* tctx)
{
  de265_image* img = tctx->img;
  const pic_parameter_set& pps = img->get_pps();
  const seq_parameter_set& sps = img->get_sps();
  slice_segment_header* shdr = tctx->shdr;

  if (shdr->dependent_slice_segment_flag) {
    int prevCtb = pps.CtbAddrTStoRS[ pps.CtbAddrRStoTS[shdr->slice_segment_address] -1 ];

    int sliceIdx = img->get_SliceHeaderIndex_atIndex(prevCtb);
    if (sliceIdx >= img->slices.size()) {
      return false;
    }
    slice_segment_header* prevCtbHdr = img->slices[ sliceIdx ];

    if (pps.is_tile_start_CTB(shdr->slice_segment_address % sps.PicWidthInCtbsY,
                              shdr->slice_segment_address / sps.PicWidthInCtbsY
                              )) {
      initialize_CABAC_models(tctx);
    }
    else {
      // wait for previous slice to finish decoding

      //printf("wait for previous slice to finish decoding\n");


      slice_unit* prevSliceSegment = tctx->imgunit->get_prev_slice_segment(tctx->sliceunit);
      //assert(prevSliceSegment);
      if (prevSliceSegment==NULL) {
        return false;
      }

      prevSliceSegment->finished_threads.wait_for_progress(prevSliceSegment->nThreads);


      /*
      printf("wait for %d,%d (init)\n",
             prevCtb / sps->PicWidthInCtbsY,
             prevCtb % sps->PicWidthInCtbsY);
      tctx->img->wait_for_progress(tctx->task, prevCtb, CTB_PROGRESS_PREFILTER);
      */

      if (!prevCtbHdr->ctx_model_storage_defined) {
        return false;
      }

      tctx->ctx_model = prevCtbHdr->ctx_model_storage;
      prevCtbHdr->ctx_model_storage.release();
    }
  }
  else {
    initialize_CABAC_models(tctx);
  }

  return true;
}


std::string thread_task_ctb_row::name() const {
  char buf[100];
  sprintf(buf,"ctb-row-%d",debug_startCtbRow);
  return buf;
}


std::string thread_task_slice_segment::name() const {
  char buf[100];
  sprintf(buf,"slice-segment-%d;%d",debug_startCtbX,debug_startCtbY);
  return buf;
}


void thread_task_slice_segment::work()
{
  thread_task_slice_segment* data = this;
  thread_context* tctx = data->tctx;
  de265_image* img = tctx->img;

  state = Running;
  img->thread_run(this);

  setCtbAddrFromTS(tctx);

  //printf("%p: A start decoding at %d/%d\n", tctx, tctx->CtbX,tctx->CtbY);

  if (data->firstSliceSubstream) {
    bool success = initialize_CABAC_at_slice_segment_start(tctx);
    if (!success) {
      state = Finished;
      tctx->sliceunit->finished_threads.increase_progress(1);
      img->thread_finishes(this);
      return;
    }
  }
  else {
    initialize_CABAC_models(tctx);
  }

  init_CABAC_decoder_2(&tctx->cabac_decoder);

  /*enum DecodeResult result =*/ decode_substream(tctx, false, data->firstSliceSubstream);

  state = Finished;
  tctx->sliceunit->finished_threads.increase_progress(1);
  img->thread_finishes(this);

  return; // DE265_OK;
}


void thread_task_ctb_row::work()
{
  thread_task_ctb_row* data = this;
  thread_context* tctx = data->tctx;
  de265_image* img = tctx->img;

  const seq_parameter_set& sps = img->get_sps();
  int ctbW = sps.PicWidthInCtbsY;

  state = Running;
  img->thread_run(this);

  setCtbAddrFromTS(tctx);

  int ctby = tctx->CtbAddrInRS / ctbW;
  int myCtbRow = ctby;

  //printf("start CTB-row decoding at row %d\n", ctby);

  if (data->firstSliceSubstream) {
    bool success = initialize_CABAC_at_slice_segment_start(tctx);
    if (!success) {
      // could not decode this row, mark whole row as finished
      for (int x=0;x<ctbW;x++) {
        img->ctb_progress[myCtbRow*ctbW + x].set_progress(CTB_PROGRESS_PREFILTER);
      }

      state = Finished;
      tctx->sliceunit->finished_threads.increase_progress(1);
      img->thread_finishes(this);
      return;
    }
    //initialize_CABAC(tctx);
  }

  init_CABAC_decoder_2(&tctx->cabac_decoder);

  bool firstIndependentSubstream =
    data->firstSliceSubstream && !tctx->shdr->dependent_slice_segment_flag;

  /*enum DecodeResult result =*/
  decode_substream(tctx, true, firstIndependentSubstream);

  // mark progress on remaining CTBs in row (in case of decoder error and early termination)

  // TODO: what about slices that end properly in the middle of a CTB row?

  if (tctx->CtbY == myCtbRow) {
    int lastCtbX = sps.PicWidthInCtbsY; // assume no tiles when WPP is on
    for (int x = tctx->CtbX; x<lastCtbX ; x++) {

      if (x        < sps.PicWidthInCtbsY &&
          myCtbRow < sps.PicHeightInCtbsY) {
        img->ctb_progress[myCtbRow*ctbW + x].set_progress(CTB_PROGRESS_PREFILTER);
      }
    }
  }

  state = Finished;
  tctx->sliceunit->finished_threads.increase_progress(1);
  img->thread_finishes(this);
}


de265_error read_slice_segment_data(thread_context* tctx)
{
  setCtbAddrFromTS(tctx);

  de265_image* img = tctx->img;
  const pic_parameter_set& pps = img->get_pps();
  const seq_parameter_set& sps = img->get_sps();
  slice_segment_header* shdr = tctx->shdr;

  bool success = initialize_CABAC_at_slice_segment_start(tctx);
  if (!success) {
    return DE265_ERROR_UNSPECIFIED_DECODING_ERROR;
  }

  init_CABAC_decoder_2(&tctx->cabac_decoder);

  //printf("-----\n");

  bool first_slice_substream = !shdr->dependent_slice_segment_flag;

  int substream=0;

  enum DecodeResult result;
  do {
    int ctby = tctx->CtbY;


    // check whether entry_points[] are correct in the bitstream

    if (substream>0) {
      if (substream-1 >= tctx->shdr->entry_point_offset.size() ||
          tctx->cabac_decoder.bitstream_curr - tctx->cabac_decoder.bitstream_start -2 /* -2 because of CABAC init */
          != tctx->shdr->entry_point_offset[substream-1]) {
        tctx->decctx->add_warning(DE265_WARNING_INCORRECT_ENTRY_POINT_OFFSET, true);
      }
    }

    substream++;


    result = decode_substream(tctx, false, first_slice_substream);


    if (result == Decode_EndOfSliceSegment ||
        result == Decode_Error) {
      break;
    }

    first_slice_substream = false;

    if (pps.tiles_enabled_flag) {
      initialize_CABAC_models(tctx);
    }
  } while (true);

  return DE265_OK;
}


/* TODO:
   When a task wants to block, but is the first in the list of pending tasks,
   do some error concealment instead of blocking, since it will never be deblocked.
   This will only happen in the case of input error.
 */
