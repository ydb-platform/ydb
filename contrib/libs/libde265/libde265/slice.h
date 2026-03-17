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

#ifndef DE265_SLICE_H
#define DE265_SLICE_H

#include "libde265/cabac.h"
#include "libde265/de265.h"
#include "libde265/util.h"
#include "libde265/refpic.h"
#include "libde265/threads.h"
#include "contextmodel.h"

#include <vector>
#include <string.h>
#include <memory>

#define MAX_NUM_REF_PICS    16

class decoder_context;
class thread_context;
class error_queue;
class seq_parameter_set;
class pic_parameter_set;

enum SliceType
  {
    SLICE_TYPE_B = 0,
    SLICE_TYPE_P = 1,
    SLICE_TYPE_I = 2
  };

/*
        2Nx2N           2NxN             Nx2N            NxN
      +-------+       +-------+       +---+---+       +---+---+
      |       |       |       |       |   |   |       |   |   |
      |       |       |_______|       |   |   |       |___|___|
      |       |       |       |       |   |   |       |   |   |
      |       |       |       |       |   |   |       |   |   |
      +-------+       +-------+       +---+---+       +---+---+

        2NxnU           2NxnD           nLx2N           nRx2N
      +-------+       +-------+       +-+-----+       +-----+-+
      |_______|       |       |       | |     |       |     | |
      |       |       |       |       | |     |       |     | |
      |       |       |_______|       | |     |       |     | |
      |       |       |       |       | |     |       |     | |
      +-------+       +-------+       +-+-----+       +-----+-+

      - AMP only if CU size > min CU size -> minimum PU size = CUsize/2
      - NxN only if size >= 16x16 (-> minimum block size = 8x8)
      - minimum block size for Bi-Pred is 8x8 (wikipedia: Coding_tree_unit)
*/
enum PartMode
  {
    PART_2Nx2N = 0,
    PART_2NxN  = 1,
    PART_Nx2N  = 2,
    PART_NxN   = 3,
    PART_2NxnU = 4,
    PART_2NxnD = 5,
    PART_nLx2N = 6,
    PART_nRx2N = 7
  };

const char* part_mode_name(enum PartMode);


enum PredMode
  {
    MODE_INTRA, MODE_INTER, MODE_SKIP
  };

enum IntraPredMode
  {
    INTRA_PLANAR = 0,
    INTRA_DC = 1,
    INTRA_ANGULAR_2 = 2,    INTRA_ANGULAR_3 = 3,    INTRA_ANGULAR_4 = 4,    INTRA_ANGULAR_5 = 5,
    INTRA_ANGULAR_6 = 6,    INTRA_ANGULAR_7 = 7,    INTRA_ANGULAR_8 = 8,    INTRA_ANGULAR_9 = 9,
    INTRA_ANGULAR_10 = 10,  INTRA_ANGULAR_11 = 11,  INTRA_ANGULAR_12 = 12,  INTRA_ANGULAR_13 = 13,
    INTRA_ANGULAR_14 = 14,  INTRA_ANGULAR_15 = 15,  INTRA_ANGULAR_16 = 16,  INTRA_ANGULAR_17 = 17,
    INTRA_ANGULAR_18 = 18,  INTRA_ANGULAR_19 = 19,  INTRA_ANGULAR_20 = 20,  INTRA_ANGULAR_21 = 21,
    INTRA_ANGULAR_22 = 22,  INTRA_ANGULAR_23 = 23,  INTRA_ANGULAR_24 = 24,  INTRA_ANGULAR_25 = 25,
    INTRA_ANGULAR_26 = 26,  INTRA_ANGULAR_27 = 27,  INTRA_ANGULAR_28 = 28,  INTRA_ANGULAR_29 = 29,
    INTRA_ANGULAR_30 = 30,  INTRA_ANGULAR_31 = 31,  INTRA_ANGULAR_32 = 32,  INTRA_ANGULAR_33 = 33,
    INTRA_ANGULAR_34 = 34
  };


enum IntraChromaPredMode
  {
    INTRA_CHROMA_PLANAR_OR_34     = 0,
    INTRA_CHROMA_ANGULAR_26_OR_34 = 1,
    INTRA_CHROMA_ANGULAR_10_OR_34 = 2,
    INTRA_CHROMA_DC_OR_34         = 3,
    INTRA_CHROMA_LIKE_LUMA  = 4
  };


enum InterPredIdc
  {
    // note: values have to match the decoding function decode_inter_pred_idc()
    PRED_L0=1,
    PRED_L1=2,
    PRED_BI=3
  };



class slice_segment_header {
public:
  slice_segment_header() {
    reset();
  }

  de265_error read(bitreader* br, decoder_context*, bool* continueDecoding);
  de265_error write(error_queue*, CABAC_encoder&,
                    const seq_parameter_set* sps,
                    const pic_parameter_set* pps,
                    uint8_t nal_unit_type);

  void dump_slice_segment_header(const decoder_context*, int fd) const;

  void set_defaults();
  void reset();


  int  slice_index; // index through all slices in a picture  (internal only)
  std::shared_ptr<const pic_parameter_set> pps;


  char first_slice_segment_in_pic_flag;
  char no_output_of_prior_pics_flag;
  int  slice_pic_parameter_set_id;
  char dependent_slice_segment_flag;
  int  slice_segment_address;

  int  slice_type;
  char pic_output_flag;
  char colour_plane_id;
  int  slice_pic_order_cnt_lsb;
  char short_term_ref_pic_set_sps_flag;
  ref_pic_set slice_ref_pic_set;

  int  short_term_ref_pic_set_idx;
  int  num_long_term_sps;
  int  num_long_term_pics;

  uint8_t lt_idx_sps[MAX_NUM_REF_PICS];
  int     poc_lsb_lt[MAX_NUM_REF_PICS];
  char    used_by_curr_pic_lt_flag[MAX_NUM_REF_PICS];

  char delta_poc_msb_present_flag[MAX_NUM_REF_PICS];
  int delta_poc_msb_cycle_lt[MAX_NUM_REF_PICS];

  char slice_temporal_mvp_enabled_flag;
  char slice_sao_luma_flag;
  char slice_sao_chroma_flag;

  char num_ref_idx_active_override_flag;
  int  num_ref_idx_l0_active; // [1;16]
  int  num_ref_idx_l1_active; // [1;16]

  char ref_pic_list_modification_flag_l0;
  char ref_pic_list_modification_flag_l1;
  uint8_t list_entry_l0[16];
  uint8_t list_entry_l1[16];

  char mvd_l1_zero_flag;
  char cabac_init_flag;
  char collocated_from_l0_flag;
  int  collocated_ref_idx;

  // --- pred_weight_table ---

  uint8_t luma_log2_weight_denom; // [0;7]
  uint8_t ChromaLog2WeightDenom;  // [0;7]

  // first index is L0/L1
  uint8_t luma_weight_flag[2][16];   // bool
  uint8_t chroma_weight_flag[2][16]; // bool
  int16_t LumaWeight[2][16];
  int8_t  luma_offset[2][16];
  int16_t ChromaWeight[2][16][2];
  int8_t  ChromaOffset[2][16][2];


  int  five_minus_max_num_merge_cand;
  int  slice_qp_delta;

  int  slice_cb_qp_offset;
  int  slice_cr_qp_offset;

  char cu_chroma_qp_offset_enabled_flag;

  char deblocking_filter_override_flag;
  char slice_deblocking_filter_disabled_flag;
  int  slice_beta_offset; // = pps->beta_offset if undefined
  int  slice_tc_offset;   // = pps->tc_offset if undefined

  char slice_loop_filter_across_slices_enabled_flag;

  int  num_entry_point_offsets;
  int  offset_len;
  std::vector<int> entry_point_offset;

  int  slice_segment_header_extension_length;


  // --- derived data ---

  int SliceQPY;
  int initType;

  void compute_derived_values(const pic_parameter_set* pps);


  // --- data for external modules ---

  int SliceAddrRS;  // slice_segment_address of last independent slice

  int MaxNumMergeCand;  // directly derived from 'five_minus_max_num_merge_cand'
  int CurrRpsIdx;
  ref_pic_set CurrRps;  // the active reference-picture set
  int NumPocTotalCurr;

  // number of entries: num_ref_idx_l0_active / num_ref_idx_l1_active
  int RefPicList[2][MAX_NUM_REF_PICS]; // contains buffer IDs (D:indices into DPB/E:frame number)
  int RefPicList_POC[2][MAX_NUM_REF_PICS];
  int RefPicList_PicState[2][MAX_NUM_REF_PICS]; /* We have to save the PicState because the decoding
                                                   of an image may be delayed and the PicState can
                                                   change in the mean-time (e.g. from ShortTerm to
                                                   LongTerm). PicState is used in motion.cc */

  char LongTermRefPic[2][MAX_NUM_REF_PICS]; /* Flag whether the picture at this ref-pic-list
                                               is a long-term picture. */

  // context storage for dependent slices (stores CABAC model at end of slice segment)
  context_model_table ctx_model_storage;
  bool ctx_model_storage_defined; // whether there is valid data in ctx_model_storage

  std::vector<int> RemoveReferencesList; // images that can be removed from the DPB before decoding this slice

};



typedef struct {
  // TODO: we could combine SaoTypeIdx and SaoEoClass into one byte to make the struct 16 bytes only

  unsigned char SaoTypeIdx; // use with (SaoTypeIdx>>(2*cIdx)) & 0x3
  unsigned char SaoEoClass; // use with (SaoTypeIdx>>(2*cIdx)) & 0x3

  uint8_t sao_band_position[3];
  int8_t  saoOffsetVal[3][4]; // index with [][idx-1] as saoOffsetVal[][0]==0 always
} sao_info;




de265_error read_slice_segment_data(thread_context* tctx);

bool alloc_and_init_significant_coeff_ctxIdx_lookupTable();
void free_significant_coeff_ctxIdx_lookupTable();


class thread_task_ctb_row : public thread_task
{
public:
  bool   firstSliceSubstream;
  int    debug_startCtbRow;
  thread_context* tctx;

  virtual void work();
  virtual std::string name() const;
};

class thread_task_slice_segment : public thread_task
{
public:
  bool   firstSliceSubstream;
  int    debug_startCtbX, debug_startCtbY;
  thread_context* tctx;

  virtual void work();
  virtual std::string name() const;
};


int check_CTB_available(const de265_image* img,
                        int xC,int yC, int xN,int yN);

#endif
