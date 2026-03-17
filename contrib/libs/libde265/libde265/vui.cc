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

#include "vui.h"
#include "decctx.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#define READ_VLC_OFFSET(variable, vlctype, offset)   \
  if ((vlc = get_ ## vlctype(br)) == UVLC_ERROR) {   \
    errqueue->add_warning(DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE, false);  \
    return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE; \
  } \
  variable = vlc + offset;

#define READ_VLC(variable, vlctype)  READ_VLC_OFFSET(variable,vlctype,0)


#define NUM_SAR_PRESETS 17

static uint16_t sar_presets[NUM_SAR_PRESETS+1][2] = {
  { 0,0 },
  { 1,1 },
  { 12,11 },
  { 10,11 },
  { 16,11 },
  { 40,33 },
  { 24,11 },
  { 20,11 },
  { 32,11 },
  { 80,33 },
  { 18,11 },
  { 15,11 },
  { 64,33 },
  { 160,99 },
  { 4,3 },
  { 3,2 },
  { 2,1 }
};

#define EXTENDED_SAR 255


const char* get_video_format_name(enum VideoFormat format)
{
  switch (format) {
  case VideoFormat_Component: return "component";
  case VideoFormat_PAL:       return "PAL";
  case VideoFormat_NTSC:      return "NTSC";
  case VideoFormat_SECAM:     return "SECAM";
  case VideoFormat_MAC:       return "MAC";
  default:                    return "unspecified";
  }
}


video_usability_information::video_usability_information()
{
  aspect_ratio_info_present_flag = false;
  sar_width  = 0;
  sar_height = 0;


  // --- overscan ---

  overscan_info_present_flag = false;
  overscan_appropriate_flag  = false;


  // --- video signal type ---

  video_signal_type_present_flag = false;
  video_format = VideoFormat_Unspecified;
  video_full_range_flag = false;
  colour_description_present_flag = false;
  colour_primaries = 2;
  transfer_characteristics = 2;
  matrix_coeffs = 2;

  // --- chroma / interlaced ---

  chroma_loc_info_present_flag = false;
  chroma_sample_loc_type_top_field    = 0;
  chroma_sample_loc_type_bottom_field = 0;

  neutral_chroma_indication_flag = false;
  field_seq_flag = false;
  frame_field_info_present_flag = false;

  // --- default display window ---

  default_display_window_flag = false;
  def_disp_win_left_offset   = 0;
  def_disp_win_right_offset  = 0;
  def_disp_win_top_offset    = 0;
  def_disp_win_bottom_offset = 0;


  // --- timing ---

  vui_timing_info_present_flag = false;
  vui_num_units_in_tick = 0;
  vui_time_scale = 0;

  vui_poc_proportional_to_timing_flag = false;
  vui_num_ticks_poc_diff_one = 1;


  // --- hrd parameters ---

  vui_hrd_parameters_present_flag = false;
 

  // --- bitstream restriction ---

  bitstream_restriction_flag = false;
  tiles_fixed_structure_flag = false;
  motion_vectors_over_pic_boundaries_flag = true;
  restricted_ref_pic_lists_flag = false;
  min_spatial_segmentation_idc = 0;
  max_bytes_per_pic_denom   = 2;
  max_bits_per_min_cu_denom = 1;
  log2_max_mv_length_horizontal = 15;
  log2_max_mv_length_vertical   = 15;
}


de265_error video_usability_information::hrd_parameters(error_queue* errqueue, bitreader* br, const seq_parameter_set* sps)
{
  int vlc;

  nal_hrd_parameters_present_flag = get_bits(br, 1);
  vcl_hrd_parameters_present_flag = get_bits(br, 1);

  if (nal_hrd_parameters_present_flag || vcl_hrd_parameters_present_flag)
  {
    sub_pic_hrd_params_present_flag = get_bits(br, 1);
    if (sub_pic_hrd_params_present_flag)
    {
      tick_divisor_minus2 = get_bits(br, 8);
      du_cpb_removal_delay_increment_length_minus1 = get_bits(br, 5);
      sub_pic_cpb_params_in_pic_timing_sei_flag = get_bits(br, 1);
      dpb_output_delay_du_length_minus1 = get_bits(br, 5);
    }
    bit_rate_scale = get_bits(br, 4);
    cpb_size_scale = get_bits(br, 4);


    if (sub_pic_hrd_params_present_flag)
    {
      cpb_size_du_scale = get_bits(br, 4);
    }
    initial_cpb_removal_delay_length_minus1 = get_bits(br, 5);
    au_cpb_removal_delay_length_minus1 = get_bits(br, 5);
    dpb_output_delay_length_minus1 = get_bits(br, 5);
  }
  int  i, j, nalOrVcl;

  for (i = 0; i < sps->sps_max_sub_layers; i++)
  {
    fixed_pic_rate_general_flag[i] = get_bits(br, 1);
    if (!fixed_pic_rate_general_flag[i])
    {
      fixed_pic_rate_within_cvs_flag[i] = get_bits(br, 1);
    }
    else
    {
      fixed_pic_rate_within_cvs_flag[i] = true;
    }

    low_delay_hrd_flag[i] = 0;// Inferred to be 0 when not present
    cpb_cnt_minus1[i] = 0;    // Inferred to be 0 when not present

    if (fixed_pic_rate_within_cvs_flag[i])
    {
      READ_VLC_OFFSET(elemental_duration_in_tc_minus1[i], uvlc, 0);
    }
    else
    {
      low_delay_hrd_flag[i] = get_bits(br, 1);
    }
    if (!low_delay_hrd_flag[i])
    {
      READ_VLC_OFFSET(cpb_cnt_minus1[i], uvlc, 0);
      if (cpb_cnt_minus1[i] > 31) {
	return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE;
      }
    }

    for (nalOrVcl = 0; nalOrVcl < 2; nalOrVcl++)
    {
      if (((nalOrVcl == 0) && nal_hrd_parameters_present_flag) ||
        ((nalOrVcl == 1) && vcl_hrd_parameters_present_flag))
      {
        for (j = 0; j <= cpb_cnt_minus1[i]; j++)
        {
          READ_VLC_OFFSET(bit_rate_value_minus1[i][j][nalOrVcl], uvlc, 0);
          READ_VLC_OFFSET(cpb_size_value_minus1[i][j][nalOrVcl], uvlc, 0);

          if (sub_pic_hrd_params_present_flag)
          {
            READ_VLC_OFFSET(cpb_size_du_value_minus1[i][j][nalOrVcl], uvlc, 0);
            READ_VLC_OFFSET(bit_rate_du_value_minus1[i][j][nalOrVcl], uvlc, 0);
          }
          cbr_flag[i][j][nalOrVcl] = get_bits(br, 1);
        }
      }
    }
  }
  return DE265_OK;
}

de265_error video_usability_information::read(error_queue* errqueue, bitreader* br,
                                              const seq_parameter_set* sps)
{
  int vlc;


  // --- sample aspect ratio (SAR) ---

  aspect_ratio_info_present_flag = get_bits(br, 1);
  if (aspect_ratio_info_present_flag) {
    int aspect_ratio_idc = get_bits(br, 8);
    if (aspect_ratio_idc <= NUM_SAR_PRESETS) {
      sar_width = sar_presets[aspect_ratio_idc][0];
      sar_height = sar_presets[aspect_ratio_idc][1];
    }
    else if (aspect_ratio_idc == EXTENDED_SAR) {
      sar_width = get_bits(br, 16);
      sar_height = get_bits(br, 16);
    }
    else {
      sar_width = 0;
      sar_height = 0;
    }
  }
  else {
    sar_width = 0;
    sar_height = 0;
  }


  // --- overscan ---

  overscan_info_present_flag = get_bits(br, 1);
  if (overscan_info_present_flag) {
    overscan_appropriate_flag = get_bits(br, 1);
  }


  // --- video signal type ---

  { // defaults
    video_format = VideoFormat_Unspecified;
    video_full_range_flag = false;
    colour_primaries = 2;
    transfer_characteristics = 2;
    matrix_coeffs = 2;
  }

  video_signal_type_present_flag = get_bits(br, 1);
  if (video_signal_type_present_flag) {
    int video_format_idc = get_bits(br, 3);
    if (video_format_idc > 5) {
      video_format_idc = VideoFormat_Unspecified;
    }
    video_format = (VideoFormat)video_format_idc;

    video_full_range_flag = get_bits(br, 1);

    colour_description_present_flag = get_bits(br, 1);
    if (colour_description_present_flag) {
      colour_primaries = get_bits(br, 8);
      if (colour_primaries == 0 ||
        colour_primaries == 3 ||
        colour_primaries >= 11) {
        colour_primaries = 2;
      }

      transfer_characteristics = get_bits(br, 8);
      if (transfer_characteristics == 0 ||
        transfer_characteristics == 3 ||
        transfer_characteristics >= 18) {
        transfer_characteristics = 2;
      }

      matrix_coeffs = get_bits(br, 8);
      
      if (matrix_coeffs >= 11) {
        matrix_coeffs = 2;
      }
    }
  }


  // --- chroma / interlaced ---

  chroma_loc_info_present_flag = get_bits(br, 1);
  if (chroma_loc_info_present_flag) {
    READ_VLC(chroma_sample_loc_type_top_field, uvlc);
    READ_VLC(chroma_sample_loc_type_bottom_field, uvlc);
  }
  else {
    chroma_sample_loc_type_top_field = 0;
    chroma_sample_loc_type_bottom_field = 0;
  }

  neutral_chroma_indication_flag = get_bits(br, 1);
  field_seq_flag = get_bits(br, 1);
  frame_field_info_present_flag = get_bits(br, 1);


  // --- default display window ---

  default_display_window_flag = get_bits(br, 1);
  if (default_display_window_flag) {
    READ_VLC(def_disp_win_left_offset, uvlc);
    READ_VLC(def_disp_win_right_offset, uvlc);
    READ_VLC(def_disp_win_top_offset, uvlc);
    READ_VLC(def_disp_win_bottom_offset, uvlc);
  }
  else {
    def_disp_win_left_offset = 0;
    def_disp_win_right_offset = 0;
    def_disp_win_top_offset = 0;
    def_disp_win_bottom_offset = 0;
  }


  // --- timing ---

  vui_timing_info_present_flag = get_bits(br, 1);
  if (vui_timing_info_present_flag) {
    vui_num_units_in_tick = get_bits(br, 32);
    vui_time_scale = get_bits(br, 32);

    vui_poc_proportional_to_timing_flag = get_bits(br, 1);
    if (vui_poc_proportional_to_timing_flag) {
      READ_VLC_OFFSET(vui_num_ticks_poc_diff_one, uvlc, 1);
    }

    // --- hrd parameters ---

    vui_hrd_parameters_present_flag = get_bits(br, 1);
    if (vui_hrd_parameters_present_flag) {
      de265_error err;
      err = hrd_parameters(errqueue, br, sps);
      if (err) {
	return err;
      }
    }
  }

  // --- bitstream restriction ---

  bitstream_restriction_flag = get_bits(br,1);
  if (bitstream_restriction_flag) {
    tiles_fixed_structure_flag = get_bits(br,1);
    motion_vectors_over_pic_boundaries_flag = get_bits(br,1);
    restricted_ref_pic_lists_flag = get_bits(br,1);

    READ_VLC(min_spatial_segmentation_idc, uvlc);
    if (min_spatial_segmentation_idc > 4095) {
      errqueue->add_warning(DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE, false);
      min_spatial_segmentation_idc = 0;
    }

    READ_VLC(max_bytes_per_pic_denom, uvlc);
    if (max_bytes_per_pic_denom > 16) {
      errqueue->add_warning(DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE, false);
      max_bytes_per_pic_denom = 2;
    }

    READ_VLC(max_bits_per_min_cu_denom, uvlc);
    if (max_bits_per_min_cu_denom > 16) {
      errqueue->add_warning(DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE, false);
      max_bits_per_min_cu_denom = 1;
    }

    READ_VLC(log2_max_mv_length_horizontal, uvlc);
    if (log2_max_mv_length_horizontal > 15) {
      errqueue->add_warning(DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE, false);
      log2_max_mv_length_horizontal = 15;
    }

    READ_VLC(log2_max_mv_length_vertical, uvlc);
    if (log2_max_mv_length_vertical > 15) {
      errqueue->add_warning(DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE, false);
      log2_max_mv_length_vertical = 15;
    }
  }
  else {
    tiles_fixed_structure_flag = false;
    motion_vectors_over_pic_boundaries_flag = true;
    restricted_ref_pic_lists_flag = false; // NOTE: default not specified in standard 2014/10

    min_spatial_segmentation_idc = 0;
    max_bytes_per_pic_denom   = 2;
    max_bits_per_min_cu_denom = 1;
    log2_max_mv_length_horizontal = 15;
    log2_max_mv_length_vertical   = 15;
  }

  //vui_read = true;

  return DE265_OK;
}


void video_usability_information::dump(int fd) const
{
  //#if (_MSC_VER >= 1500)
  //#define LOG0(t) loginfo(LogHeaders, t)
  //#define LOG1(t,d) loginfo(LogHeaders, t,d)
  //#define LOG2(t,d1,d2) loginfo(LogHeaders, t,d1,d2)
  //#define LOG3(t,d1,d2,d3) loginfo(LogHeaders, t,d1,d2,d3)

  FILE* fh;
  if (fd==1) fh=stdout;
  else if (fd==2) fh=stderr;
  else { return; }

#define LOG0(t) log2fh(fh, t)
#define LOG1(t,d) log2fh(fh, t,d)
#define LOG2(t,d1,d2) log2fh(fh, t,d1,d2)
#define LOG3(t,d1,d2,d3) log2fh(fh, t,d1,d2,d3)

  LOG0("----------------- VUI -----------------\n");
  LOG2("sample aspect ratio        : %d:%d\n", sar_width,sar_height);
  LOG1("overscan_info_present_flag : %d\n", overscan_info_present_flag);
  LOG1("overscan_appropriate_flag  : %d\n", overscan_appropriate_flag);

  LOG1("video_signal_type_present_flag: %d\n", video_signal_type_present_flag);
  if (video_signal_type_present_flag) {
    LOG1("  video_format                : %s\n", get_video_format_name(video_format));
    LOG1("  video_full_range_flag       : %d\n", video_full_range_flag);
    LOG1("  colour_description_present_flag : %d\n", colour_description_present_flag);
    LOG1("  colour_primaries            : %d\n", colour_primaries);
    LOG1("  transfer_characteristics    : %d\n", transfer_characteristics);
    LOG1("  matrix_coeffs               : %d\n", matrix_coeffs);
  }

  LOG1("chroma_loc_info_present_flag: %d\n", chroma_loc_info_present_flag);
  if (chroma_loc_info_present_flag) {
    LOG1("  chroma_sample_loc_type_top_field   : %d\n", chroma_sample_loc_type_top_field);
    LOG1("  chroma_sample_loc_type_bottom_field: %d\n", chroma_sample_loc_type_bottom_field);
  }

  LOG1("neutral_chroma_indication_flag: %d\n", neutral_chroma_indication_flag);
  LOG1("field_seq_flag                : %d\n", field_seq_flag);
  LOG1("frame_field_info_present_flag : %d\n", frame_field_info_present_flag);

  LOG1("default_display_window_flag   : %d\n", default_display_window_flag);
  LOG1("  def_disp_win_left_offset    : %d\n", def_disp_win_left_offset);
  LOG1("  def_disp_win_right_offset   : %d\n", def_disp_win_right_offset);
  LOG1("  def_disp_win_top_offset     : %d\n", def_disp_win_top_offset);
  LOG1("  def_disp_win_bottom_offset  : %d\n", def_disp_win_bottom_offset);

  LOG1("vui_timing_info_present_flag  : %d\n", vui_timing_info_present_flag);
  if (vui_timing_info_present_flag) {
    LOG1("  vui_num_units_in_tick       : %d\n", vui_num_units_in_tick);
    LOG1("  vui_time_scale              : %d\n", vui_time_scale);
  }

  LOG1("vui_poc_proportional_to_timing_flag : %d\n", vui_poc_proportional_to_timing_flag);
  LOG1("vui_num_ticks_poc_diff_one          : %d\n", vui_num_ticks_poc_diff_one);

  LOG1("vui_hrd_parameters_present_flag : %d\n", vui_hrd_parameters_present_flag);
  if (vui_hrd_parameters_present_flag) {
    //hrd_parameters vui_hrd_parameters;
  }


  // --- bitstream restriction ---

  LOG1("bitstream_restriction_flag         : %d\n", bitstream_restriction_flag);
  if (bitstream_restriction_flag) {
    LOG1("  tiles_fixed_structure_flag       : %d\n", tiles_fixed_structure_flag);
    LOG1("  motion_vectors_over_pic_boundaries_flag : %d\n", motion_vectors_over_pic_boundaries_flag);
    LOG1("  restricted_ref_pic_lists_flag    : %d\n", restricted_ref_pic_lists_flag);
    LOG1("  min_spatial_segmentation_idc     : %d\n", min_spatial_segmentation_idc);
    LOG1("  max_bytes_per_pic_denom          : %d\n", max_bytes_per_pic_denom);
    LOG1("  max_bits_per_min_cu_denom        : %d\n", max_bits_per_min_cu_denom);
    LOG1("  log2_max_mv_length_horizontal    : %d\n", log2_max_mv_length_horizontal);
    LOG1("  log2_max_mv_length_vertical      : %d\n", log2_max_mv_length_vertical);
  }

#undef LOG0
#undef LOG1
#undef LOG2
#undef LOG3
  //#endif
}
