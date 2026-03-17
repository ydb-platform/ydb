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

#ifndef DE265_SPS_H
#define DE265_SPS_H

#include "libde265/vps.h"
#include "libde265/vui.h"
#include "libde265/bitstream.h"
#include "libde265/refpic.h"
#include "libde265/de265.h"
#include "libde265/cabac.h"

#include <vector>

class error_queue;

// #define MAX_REF_PIC_SETS 64  // maximum according to standard
#define MAX_NUM_LT_REF_PICS_SPS 32

// This is just a safety range. It is chosen such that width/height fits into 16bit integers and the total number of pixels in 32bit integers.
#define MAX_PICTURE_WIDTH  65535
#define MAX_PICTURE_HEIGHT 65535

enum {
  CHROMA_MONO = 0,
  CHROMA_420 = 1,
  CHROMA_422 = 2,
  CHROMA_444 = 3,
  CHROMA_444_SEPARATE
};


typedef struct scaling_list_data {
  // structure size: approx. 4 kB

  uint8_t ScalingFactor_Size0[6][4][4];
  uint8_t ScalingFactor_Size1[6][8][8];
  uint8_t ScalingFactor_Size2[6][16][16];
  uint8_t ScalingFactor_Size3[6][32][32];
} scaling_list_data;


enum PresetSet {
  Preset_Default
};


class sps_range_extension
{
 public:
  sps_range_extension();

  uint8_t transform_skip_rotation_enabled_flag;
  uint8_t transform_skip_context_enabled_flag;
  uint8_t implicit_rdpcm_enabled_flag;
  uint8_t explicit_rdpcm_enabled_flag;
  uint8_t extended_precision_processing_flag;
  uint8_t intra_smoothing_disabled_flag;
  uint8_t high_precision_offsets_enabled_flag;
  uint8_t persistent_rice_adaptation_enabled_flag;
  uint8_t cabac_bypass_alignment_enabled_flag;

  de265_error read(error_queue*, bitreader*);
  void dump(int fd) const;
};


class seq_parameter_set {
public:
  seq_parameter_set();
  ~seq_parameter_set();

  de265_error read(error_queue*, bitreader*);
  de265_error write(error_queue*, CABAC_encoder&);

  void dump(int fd) const;

  void set_defaults(enum PresetSet = Preset_Default);
  void set_CB_log2size_range(int mini,int maxi);
  void set_TB_log2size_range(int mini,int maxi);
  void set_resolution(int w,int h);

  bool sps_read; // whether the sps has been read from the bitstream


  char video_parameter_set_id;
  char sps_max_sub_layers;            // [1;7]
  char sps_temporal_id_nesting_flag;

  profile_tier_level profile_tier_level_;

  int seq_parameter_set_id;
  int chroma_format_idc;

  char separate_colour_plane_flag;
  int  pic_width_in_luma_samples;
  int  pic_height_in_luma_samples;
  char conformance_window_flag;

  int conf_win_left_offset;
  int conf_win_right_offset;
  int conf_win_top_offset;
  int conf_win_bottom_offset;

  int bit_depth_luma;
  int bit_depth_chroma;

  int  log2_max_pic_order_cnt_lsb;
  char sps_sub_layer_ordering_info_present_flag;

  int sps_max_dec_pic_buffering[7]; // for each temporal layer
  int sps_max_num_reorder_pics[7];
  int sps_max_latency_increase_plus1[7];

  int  log2_min_luma_coding_block_size;             // smallest CB size [3;6]
  int  log2_diff_max_min_luma_coding_block_size;    // largest  CB size
  int  log2_min_transform_block_size;               // smallest TB size [2;5]
  int  log2_diff_max_min_transform_block_size;      // largest  TB size
  int  max_transform_hierarchy_depth_inter;
  int  max_transform_hierarchy_depth_intra;

  char scaling_list_enable_flag;
  char sps_scaling_list_data_present_flag; /* if not set, the default scaling lists will be set
                                              in scaling_list */

  struct scaling_list_data scaling_list;

  char amp_enabled_flag;
  char sample_adaptive_offset_enabled_flag;
  char pcm_enabled_flag;

  char pcm_sample_bit_depth_luma;
  char pcm_sample_bit_depth_chroma;
  int  log2_min_pcm_luma_coding_block_size;
  int  log2_diff_max_min_pcm_luma_coding_block_size;
  char pcm_loop_filter_disable_flag;

  int num_short_term_ref_pic_sets() const { return ref_pic_sets.size(); }
  std::vector<ref_pic_set> ref_pic_sets; // [0 ; num_short_term_ref_pic_set (<=MAX_REF_PIC_SETS) )

  char long_term_ref_pics_present_flag;

  int num_long_term_ref_pics_sps;

  int  lt_ref_pic_poc_lsb_sps[MAX_NUM_LT_REF_PICS_SPS];
  char used_by_curr_pic_lt_sps_flag[MAX_NUM_LT_REF_PICS_SPS];

  char sps_temporal_mvp_enabled_flag;
  char strong_intra_smoothing_enable_flag;

  char vui_parameters_present_flag;
  video_usability_information vui;

  char sps_extension_present_flag;
  char sps_range_extension_flag;
  char sps_multilayer_extension_flag;
  char sps_extension_6bits;

  sps_range_extension range_extension;

  /*
    if( sps_extension_flag )
    while( more_rbsp_data() )
    sps_extension_data_flag
    u(1)
    rbsp_trailing_bits()
  */


  // --- derived values ---

  de265_error compute_derived_values(bool sanitize_values = false);

  int BitDepth_Y;
  int QpBdOffset_Y;
  int BitDepth_C;
  int QpBdOffset_C;

  int ChromaArrayType;
  int SubWidthC, SubHeightC;
  int WinUnitX, WinUnitY;

  int MaxPicOrderCntLsb;

  int Log2MinCbSizeY;
  int Log2CtbSizeY;
  int MinCbSizeY;
  int CtbSizeY;
  int PicWidthInMinCbsY;
  int PicWidthInCtbsY;
  int PicHeightInMinCbsY;
  int PicHeightInCtbsY;
  int PicSizeInMinCbsY;
  int PicSizeInCtbsY;
  uint32_t PicSizeInSamplesY;

  int CtbWidthC, CtbHeightC;

  int PicWidthInTbsY; // not in standard
  int PicHeightInTbsY; // not in standard
  int PicSizeInTbsY; // not in standard

  int Log2MinTrafoSize;
  int Log2MaxTrafoSize;

  int Log2MinPUSize;
  int PicWidthInMinPUs;  // might be rounded up
  int PicHeightInMinPUs; // might be rounded up

  int Log2MinIpcmCbSizeY;
  int Log2MaxIpcmCbSizeY;

  int SpsMaxLatencyPictures[7]; // [temporal layer]

  uint8_t WpOffsetBdShiftY;
  uint8_t WpOffsetBdShiftC;
  int32_t WpOffsetHalfRangeY;
  int32_t WpOffsetHalfRangeC;


  int getPUIndexRS(int pixelX,int pixelY) const {
    return (pixelX>>Log2MinPUSize) + (pixelY>>Log2MinPUSize)*PicWidthInMinPUs;
  }

  int get_bit_depth(int cIdx) const {
    if (cIdx==0) return BitDepth_Y;
    else         return BitDepth_C;
  }

  int get_chroma_shift_W(int cIdx) const { return cIdx ? SubWidthC -1 : 0; }
  int get_chroma_shift_H(int cIdx) const { return cIdx ? SubHeightC-1 : 0; }
};

de265_error read_scaling_list(bitreader*, const seq_parameter_set*, scaling_list_data*, bool inPPS);
de265_error write_scaling_list(CABAC_encoder& out, const seq_parameter_set* sps,
                               scaling_list_data* sclist, bool inPPS);
void set_default_scaling_lists(scaling_list_data*);

#endif
