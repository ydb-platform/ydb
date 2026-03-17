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

#include "vps.h"
#include "util.h"
#include "decctx.h"

#include <assert.h>


void profile_data::set_defaults(enum profile_idc profile, int level_major, int level_minor)
{
  profile_present_flag = 1;

  profile_space = 0;
  tier_flag = 0;
  profile_idc = profile;

  for (int i=0;i<32;i++) {
    profile_compatibility_flag[i]=0;
  }

  switch (profile) {
  case Profile_Main:
    profile_compatibility_flag[Profile_Main]=1;
    profile_compatibility_flag[Profile_Main10]=1;
    break;
  case Profile_Main10:
    profile_compatibility_flag[Profile_Main10]=1;
    break;
  default:
    assert(0);
  }

  progressive_source_flag = 0;
  interlaced_source_flag  = 0;
  non_packed_constraint_flag = 0;
  frame_only_constraint_flag = 0;


  // --- level ---

  level_present_flag = 1;
  level_idc = level_major*30 + level_minor*3;
}


void video_parameter_set::set_defaults(enum profile_idc profile, int level_major, int level_minor)
{
  video_parameter_set_id = 0;
  vps_max_layers = 1; // always =1 in current version of standard
  vps_max_sub_layers = 1; // temporal sub-layers
  vps_temporal_id_nesting_flag = 1;

  profile_tier_level_.general.set_defaults(profile,level_major,level_minor);

  vps_sub_layer_ordering_info_present_flag = 0;
  layer[0].vps_max_dec_pic_buffering = 1;
  layer[0].vps_max_num_reorder_pics  = 0;
  layer[0].vps_max_latency_increase  = 0;

  vps_max_layer_id = 0;
  vps_num_layer_sets = 1;

  layer_id_included_flag.resize(vps_num_layer_sets);


  // --- timing info ---

  vps_timing_info_present_flag = 0;
  vps_num_units_in_tick = 0;
  vps_time_scale = 0;
  vps_poc_proportional_to_timing_flag = 0;

  vps_num_ticks_poc_diff_one = 0;
  vps_num_hrd_parameters = 0;


  // --- vps extension ---

  vps_extension_flag = 0;
}


de265_error video_parameter_set::read(error_queue* errqueue, bitreader* reader)
{
  int vlc;

  video_parameter_set_id = vlc = get_bits(reader, 4);
  if (vlc >= DE265_MAX_VPS_SETS) return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE;

  skip_bits(reader, 2);
  vps_max_layers = vlc = get_bits(reader,6) +1;
  if (vlc > 63) return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE; // vps_max_layers_minus1 (range 0...63)

  vps_max_sub_layers = vlc = get_bits(reader,3) +1;
  if (vlc >= MAX_TEMPORAL_SUBLAYERS) return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE;

  vps_temporal_id_nesting_flag = get_bits(reader,1);
  skip_bits(reader, 16);

  profile_tier_level_.read(reader, vps_max_sub_layers);

  /*
    read_bit_rate_pic_rate_info(reader, &bit_rate_pic_rate_info,
    0, vps_max_sub_layers-1);
  */

  vps_sub_layer_ordering_info_present_flag = get_bits(reader,1);
  //assert(vps_max_sub_layers-1 < MAX_TEMPORAL_SUBLAYERS);

  int firstLayerRead = vps_sub_layer_ordering_info_present_flag ? 0 : (vps_max_sub_layers-1);

  for (int i=firstLayerRead;i<vps_max_sub_layers;i++) {
    layer[i].vps_max_dec_pic_buffering = get_uvlc(reader);
    layer[i].vps_max_num_reorder_pics  = get_uvlc(reader);
    layer[i].vps_max_latency_increase  = get_uvlc(reader);

if (layer[i].vps_max_dec_pic_buffering == UVLC_ERROR ||
    layer[i].vps_max_num_reorder_pics  == UVLC_ERROR ||
    layer[i].vps_max_latency_increase  == UVLC_ERROR) {
      return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE;
    }
  }

  if (!vps_sub_layer_ordering_info_present_flag) {
    assert(firstLayerRead < MAX_TEMPORAL_SUBLAYERS);

    for (int i=0;i<firstLayerRead;i++) {
      layer[i].vps_max_dec_pic_buffering = layer[firstLayerRead].vps_max_dec_pic_buffering;
      layer[i].vps_max_num_reorder_pics  = layer[firstLayerRead].vps_max_num_reorder_pics;
      layer[i].vps_max_latency_increase  = layer[firstLayerRead].vps_max_latency_increase;
    }
  }


  vps_max_layer_id = get_bits(reader,6);
  vps_num_layer_sets = get_uvlc(reader);

  if (vps_num_layer_sets+1<0 ||
      vps_num_layer_sets+1>=1024 ||
      vps_num_layer_sets == UVLC_ERROR) {
    errqueue->add_warning(DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE, false);
    return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE;
  }
  vps_num_layer_sets += 1;

  layer_id_included_flag.resize(vps_num_layer_sets);

  for (int i=1; i <= vps_num_layer_sets-1; i++)
    {
      layer_id_included_flag[i].resize(vps_max_layer_id+1);

      for (int j=0; j <= vps_max_layer_id; j++)
        {
          layer_id_included_flag[i][j] = get_bits(reader,1);
        }
    }

  vps_timing_info_present_flag = get_bits(reader,1);

  if (vps_timing_info_present_flag) {
    vps_num_units_in_tick = get_bits(reader,32);
    vps_time_scale        = get_bits(reader,32);
    vps_poc_proportional_to_timing_flag = get_bits(reader,1);

    if (vps_poc_proportional_to_timing_flag) {
      vps_num_ticks_poc_diff_one = get_uvlc(reader)+1;
      vps_num_hrd_parameters     = get_uvlc(reader);

      if (vps_num_hrd_parameters >= 1024 || vps_num_hrd_parameters < 0) {
        errqueue->add_warning(DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE, false);
        return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE;
      }

      hrd_layer_set_idx .resize(vps_num_hrd_parameters);
      cprms_present_flag.resize(vps_num_hrd_parameters);

      for (int i=0; i<vps_num_hrd_parameters; i++) {
        hrd_layer_set_idx[i] = get_uvlc(reader);

        if (i > 0) {
          cprms_present_flag[i] = get_bits(reader,1);
        }

        //hrd_parameters(cprms_present_flag[i], vps_max_sub_layers_minus1)

        return DE265_OK; // TODO: decode hrd_parameters()
      }
    }
  }

  vps_extension_flag = get_bits(reader,1);

  if (vps_extension_flag) {
    /*
      while( more_rbsp_data() )
      vps_extension_data_flag u(1)
      rbsp_trailing_bits()
    */
  }

  return DE265_OK;
}


de265_error video_parameter_set::write(error_queue* errqueue, CABAC_encoder& out) const
{
  if (video_parameter_set_id >= DE265_MAX_VPS_SETS) return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE;
  out.write_bits(video_parameter_set_id,4);

  out.write_bits(0x3,2);
  out.write_bits(vps_max_layers-1,6);

  if (vps_max_sub_layers >= MAX_TEMPORAL_SUBLAYERS) return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE;
  out.write_bits(vps_max_sub_layers-1,3);

  out.write_bit(vps_temporal_id_nesting_flag);
  out.write_bits(0xFFFF, 16);

  profile_tier_level_.write(out, vps_max_sub_layers);

  /*
  read_bit_rate_pic_rate_info(reader, &bit_rate_pic_rate_info,
                              0, vps_max_sub_layers-1);
  */

  out.write_bit(vps_sub_layer_ordering_info_present_flag);
  //assert(vps_max_sub_layers-1 < MAX_TEMPORAL_SUBLAYERS);

  int firstLayerRead = vps_sub_layer_ordering_info_present_flag ? 0 : (vps_max_sub_layers-1);

  for (int i=firstLayerRead;i<vps_max_sub_layers;i++) {
    out.write_uvlc(layer[i].vps_max_dec_pic_buffering);
    out.write_uvlc(layer[i].vps_max_num_reorder_pics);
    out.write_uvlc(layer[i].vps_max_latency_increase);
  }

  if (vps_num_layer_sets<0 ||
      vps_num_layer_sets>=1024) {
    errqueue->add_warning(DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE, false);
    return DE265_ERROR_CODED_PARAMETER_OUT_OF_RANGE;
  }

  out.write_bits(vps_max_layer_id,6);
  out.write_uvlc(vps_num_layer_sets-1);

  for (int i=1; i <= vps_num_layer_sets-1; i++)
    for (int j=0; j <= vps_max_layer_id; j++)
      {
        out.write_bit(layer_id_included_flag[i][j]);
      }

  out.write_bit(vps_timing_info_present_flag);

  if (vps_timing_info_present_flag) {
    out.write_bits(vps_num_units_in_tick,32);
    out.write_bits(vps_time_scale       ,32);
    out.write_bit (vps_poc_proportional_to_timing_flag);

    if (vps_poc_proportional_to_timing_flag) {
      out.write_uvlc(vps_num_ticks_poc_diff_one-1);
      out.write_uvlc(vps_num_hrd_parameters);

      for (int i=0; i<vps_num_hrd_parameters; i++) {
        out.write_uvlc(hrd_layer_set_idx[i]);

        if (i > 0) {
          out.write_bit(cprms_present_flag[i]);
        }

        //hrd_parameters(cprms_present_flag[i], vps_max_sub_layers_minus1)

        return DE265_OK; // TODO: decode hrd_parameters()
      }
    }
  }

  out.write_bit(vps_extension_flag);

  if (vps_extension_flag) {
    /*
    while( more_rbsp_data() )
    vps_extension_data_flag u(1)
    rbsp_trailing_bits()
    */
  }

  return DE265_OK;
}


void profile_data::read(bitreader* reader)
{
  if (profile_present_flag) {
    profile_space = get_bits(reader,2);
    tier_flag = get_bits(reader,1);
    profile_idc = (enum profile_idc)get_bits(reader,5);

    for (int i=0; i<32; i++) {
      profile_compatibility_flag[i] = get_bits(reader,1);
    }

    progressive_source_flag = get_bits(reader,1);
    interlaced_source_flag  = get_bits(reader,1);
    non_packed_constraint_flag = get_bits(reader,1);
    frame_only_constraint_flag = get_bits(reader,1);
    skip_bits(reader,44);
  }

  if (level_present_flag) {
    level_idc = get_bits(reader,8);
  }
}


void profile_tier_level::read(bitreader* reader,
                              int max_sub_layers)
{
  // --- read the general profile ---

  general.profile_present_flag = true;
  general.level_present_flag = true;
  general.read(reader);


  // --- read the profile/levels of the sub-layers ---

  for (int i=0; i<max_sub_layers-1; i++)
    {
      sub_layer[i].profile_present_flag = get_bits(reader,1);
      sub_layer[i].level_present_flag   = get_bits(reader,1);
    }

  if (max_sub_layers > 1)
    {
      for (int i=max_sub_layers-1; i<8; i++)
        {
          skip_bits(reader,2);
        }
    }

  for (int i=0; i<max_sub_layers-1; i++)
    {
      sub_layer[i].read(reader);
    }
}


void profile_data::write(CABAC_encoder& out) const
{
  if (profile_present_flag)
    {
      out.write_bits(profile_space,2);
      out.write_bit (tier_flag);
      out.write_bits(profile_idc,5);

      for (int j=0; j<32; j++)
        {
          out.write_bit(profile_compatibility_flag[j]);
        }

      out.write_bit(progressive_source_flag);
      out.write_bit(interlaced_source_flag);
      out.write_bit(non_packed_constraint_flag);
      out.write_bit(frame_only_constraint_flag);
      out.skip_bits(44);
    }

  if (level_present_flag)
    {
      out.write_bits(level_idc,8);
    }
}

void profile_tier_level::write(CABAC_encoder& out, int max_sub_layers) const
{
  assert(general.profile_present_flag==true);
  assert(general.level_present_flag==true);

  general.write(out);

  for (int i=0; i<max_sub_layers-1; i++)
    {
      out.write_bit(sub_layer[i].profile_present_flag);
      out.write_bit(sub_layer[i].level_present_flag);
    }

  if (max_sub_layers > 1)
    {
      for (int i=max_sub_layers-1; i<8; i++)
        {
          out.skip_bits(2);
        }
    }

  for (int i=0; i<max_sub_layers-1; i++)
    {
      sub_layer[i].write(out);
    }
}


/*
void read_bit_rate_pic_rate_info(bitreader* reader,
                                 struct bit_rate_pic_rate_info* hdr,
                                 int TempLevelLow,
                                 int TempLevelHigh)
{
  for (int i=TempLevelLow; i<=TempLevelHigh; i++) {

    hdr->bit_rate_info_present_flag[i] = get_bits(reader,1);
    hdr->pic_rate_info_present_flag[i] = get_bits(reader,1);

    if (hdr->bit_rate_info_present_flag[i]) {
      hdr->avg_bit_rate[i] = get_bits(reader,16);
      hdr->max_bit_rate[i] = get_bits(reader,16);
    }

    if (hdr->pic_rate_info_present_flag[i]) {
      hdr->constant_pic_rate_idc[i] = get_bits(reader,2);
      hdr->avg_pic_rate[i] = get_bits(reader,16);
    }
  }
}
*/



#define LOG0(t) log2fh(fh, t)
#define LOG1(t,d) log2fh(fh, t,d)
#define LOG2(t,d1,d2) log2fh(fh, t,d1,d2)
#define LOG3(t,d1,d2,d3) log2fh(fh, t,d1,d2,d3)

void video_parameter_set::dump(int fd) const
{
  FILE* fh;
  if (fd==1) fh=stdout;
  else if (fd==2) fh=stderr;
  else { return; }

  LOG0("----------------- VPS -----------------\n");
  LOG1("video_parameter_set_id                : %d\n", video_parameter_set_id);
  LOG1("vps_max_layers                        : %d\n", vps_max_layers);
  LOG1("vps_max_sub_layers                    : %d\n", vps_max_sub_layers);
  LOG1("vps_temporal_id_nesting_flag          : %d\n", vps_temporal_id_nesting_flag);

  profile_tier_level_.dump(vps_max_sub_layers, fh);
  //dump_bit_rate_pic_rate_info(&bit_rate_pic_rate_info, 0, vps_max_sub_layers-1);

  LOG1("vps_sub_layer_ordering_info_present_flag : %d\n",
       vps_sub_layer_ordering_info_present_flag);

  if (vps_sub_layer_ordering_info_present_flag) {
    for (int i=0;i<vps_max_sub_layers;i++) {
      LOG2("layer %d: vps_max_dec_pic_buffering = %d\n",i,layer[i].vps_max_dec_pic_buffering);
      LOG1("         vps_max_num_reorder_pics  = %d\n",layer[i].vps_max_num_reorder_pics);
      LOG1("         vps_max_latency_increase  = %d\n",layer[i].vps_max_latency_increase);
    }
  }
  else {
    LOG1("layer (all): vps_max_dec_pic_buffering = %d\n",layer[0].vps_max_dec_pic_buffering);
    LOG1("             vps_max_num_reorder_pics  = %d\n",layer[0].vps_max_num_reorder_pics);
    LOG1("             vps_max_latency_increase  = %d\n",layer[0].vps_max_latency_increase);
  }


  LOG1("vps_max_layer_id   = %d\n", vps_max_layer_id);
  LOG1("vps_num_layer_sets = %d\n", vps_num_layer_sets);

  for (int i=1; i <= vps_num_layer_sets-1; i++)
    for (int j=0; j <= vps_max_layer_id; j++)
      {
        LOG3("layer_id_included_flag[%d][%d] = %d\n",i,j,
             int(layer_id_included_flag[i][j]));
      }

  LOG1("vps_timing_info_present_flag = %d\n",
       vps_timing_info_present_flag);

  if (vps_timing_info_present_flag) {
    LOG1("vps_num_units_in_tick = %d\n", vps_num_units_in_tick);
    LOG1("vps_time_scale        = %d\n", vps_time_scale);
    LOG1("vps_poc_proportional_to_timing_flag = %d\n", vps_poc_proportional_to_timing_flag);

    if (vps_poc_proportional_to_timing_flag) {
      LOG1("vps_num_ticks_poc_diff_one = %d\n", vps_num_ticks_poc_diff_one);
      LOG1("vps_num_hrd_parameters     = %d\n", vps_num_hrd_parameters);

      for (int i=0; i<vps_num_hrd_parameters; i++) {
        LOG2("hrd_layer_set_idx[%d] = %d\n", i, hrd_layer_set_idx[i]);

        if (i > 0) {
          LOG2("cprms_present_flag[%d] = %d\n", i, cprms_present_flag[i]);
        }

        //hrd_parameters(cprms_present_flag[i], vps_max_sub_layers_minus1)

        return; // TODO: decode hrd_parameters()
      }
    }
  }

  LOG1("vps_extension_flag = %d\n", vps_extension_flag);
}


static const char* profile_name(profile_idc p)
{
  switch (p) {
  case Profile_Main: return "Main";
  case Profile_Main10: return "Main10";
  case Profile_MainStillPicture: return "MainStillPicture";
  case Profile_FormatRangeExtensions: return "FormatRangeExtensions";
  default:
    return "(unknown)";
  }
}


void profile_data::dump(bool general, FILE* fh) const
{
  const char* prefix = (general ? "general" : "sub_layer");

  if (profile_present_flag) {
    LOG2("  %s_profile_space     : %d\n", prefix,profile_space);
    LOG2("  %s_tier_flag         : %d\n", prefix,tier_flag);
    LOG2("  %s_profile_idc       : %s\n", prefix, profile_name(profile_idc));

    LOG1("  %s_profile_compatibility_flags: ", prefix);
    for (int i=0; i<32; i++) {
      if (i) LOG0("*,");
      LOG1("*%d",profile_compatibility_flag[i]);
    }
    LOG0("*\n");
    LOG2("    %s_progressive_source_flag : %d\n",prefix,progressive_source_flag);
    LOG2("    %s_interlaced_source_flag : %d\n",prefix,interlaced_source_flag);
    LOG2("    %s_non_packed_constraint_flag : %d\n",prefix,non_packed_constraint_flag);
    LOG2("    %s_frame_only_constraint_flag : %d\n",prefix,frame_only_constraint_flag);
  }

  if (level_present_flag) {
    LOG3("  %s_level_idc         : %d (%4.2f)\n", prefix,level_idc, level_idc/30.0f);
  }
}


void profile_tier_level::dump(int max_sub_layers, FILE* fh) const
{
  general.dump(true, fh);

  for (int i=0; i<max_sub_layers-1; i++)
    {
      LOG1("  Profile/Tier/Level [Layer %d]\n",i);
      sub_layer[i].dump(false, fh);
    }
}

#undef LOG0
#undef LOG1
#undef LOG2
#undef LOG3


/*
void dump_bit_rate_pic_rate_info(struct bit_rate_pic_rate_info* hdr,
                                 int TempLevelLow,
                                 int TempLevelHigh)
{
  for (int i=TempLevelLow; i<=TempLevelHigh; i++) {

    LOG("  Bitrate [Layer %d]\n", i);

    if (hdr->bit_rate_info_present_flag[i]) {
      LOG("    avg_bit_rate : %d\n", hdr->avg_bit_rate[i]);
      LOG("    max_bit_rate : %d\n", hdr->max_bit_rate[i]);
    }

    if (hdr->pic_rate_info_present_flag[i]) {
      LOG("    constant_pic_rate_idc : %d\n", hdr->constant_pic_rate_idc[i]);
      LOG("    avg_pic_rate[i]       : %d\n", hdr->avg_pic_rate[i]);
    }
  }
}
*/
