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

#ifndef DE265_VPS_H
#define DE265_VPS_H

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#include "libde265/bitstream.h"
#include "libde265/de265.h"
#include "libde265/cabac.h"

#include <vector>

class error_queue;

#define MAX_TEMPORAL_SUBLAYERS 8


enum profile_idc {
  Profile_Main   = 1,
  Profile_Main10 = 2,
  Profile_MainStillPicture = 3,
  Profile_FormatRangeExtensions = 4
};


class profile_data {
public:
  void read(bitreader* reader);
  void write(CABAC_encoder& writer) const;
  void dump(bool general, FILE* fh) const;

  void set_defaults(enum profile_idc, int level_major, int level_minor);

  // --- profile ---

  char profile_present_flag;  // always true for general profile

  char profile_space;  // currently always 0
  char tier_flag;      // main tier or low tier (see Table A-66/A-67)
  enum profile_idc profile_idc; // profile

  char profile_compatibility_flag[32]; // to which profile we are compatible

  char progressive_source_flag;
  char interlaced_source_flag;
  char non_packed_constraint_flag;
  char frame_only_constraint_flag;


  // --- level ---

  char level_present_flag; // always true for general level
  int  level_idc;          // level * 30
};


class profile_tier_level
{
public:
  void read(bitreader* reader, int max_sub_layers);
  void write(CABAC_encoder& writer, int max_sub_layers) const;
  void dump(int max_sub_layers, FILE* fh) const;

  profile_data general;

  //bool sub_layer_profile_present[MAX_TEMPORAL_SUBLAYERS];
  //bool sub_layer_level_present[MAX_TEMPORAL_SUBLAYERS];

  profile_data sub_layer[MAX_TEMPORAL_SUBLAYERS];
};


/*
struct bit_rate_pic_rate_info {
  char bit_rate_info_present_flag[8];
  char pic_rate_info_present_flag[8];

  int avg_bit_rate[8];
  int max_bit_rate[8];

  char constant_pic_rate_idc[8];
  int  avg_pic_rate[8];

};

void read_bit_rate_pic_rate_info(bitreader* reader,
                                 struct bit_rate_pic_rate_info* hdr,
                                 int TempLevelLow,
                                 int TempLevelHigh);

void dump_bit_rate_pic_rate_info(struct bit_rate_pic_rate_info* hdr,
                                 int TempLevelLow,
                                 int TempLevelHigh);
*/


typedef struct {
  int vps_max_dec_pic_buffering; // [1 ; ]
  int vps_max_num_reorder_pics;  // [0 ; ]
  int vps_max_latency_increase;  // 0 -> no limit, otherwise value is (x-1)
} layer_data;


class video_parameter_set
{
public:
  de265_error read(error_queue* errqueue, bitreader* reader);
  de265_error write(error_queue* errqueue, CABAC_encoder& out) const;
  void dump(int fd) const;

  void set_defaults(enum profile_idc profile, int level_major, int level_minor);

  int video_parameter_set_id;
  int vps_max_layers;            // [1;?]  currently always 1
  int vps_max_sub_layers;        // [1;7]  number of temporal sub-layers
  int vps_temporal_id_nesting_flag; // indicate temporal up-switching always possible
  profile_tier_level profile_tier_level_;

  int vps_sub_layer_ordering_info_present_flag;
  layer_data layer[MAX_TEMPORAL_SUBLAYERS];

  uint8_t vps_max_layer_id;   // max value for nuh_layer_id in NALs
  int     vps_num_layer_sets; // [1;1024], currently always 1

  std::vector<std::vector<char> > layer_id_included_flag; // max size = [1024][64]


  // --- timing info ---

  char     vps_timing_info_present_flag;
  uint32_t vps_num_units_in_tick;
  uint32_t vps_time_scale;
  char     vps_poc_proportional_to_timing_flag;
  uint32_t vps_num_ticks_poc_diff_one;

  int vps_num_hrd_parameters;     // currently [0;1]

  std::vector<uint16_t> hrd_layer_set_idx;  // max size = 1024
  std::vector<char>     cprms_present_flag; // max size = 1024


  // --- vps extension ---

  char vps_extension_flag;
};


#endif
