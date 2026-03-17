/*
 * H.265 video codec.
 * Copyright (c) 2013-2014 struktur AG, Dirk Farin <farin@struktur.de>
 *
 * Authors: struktur AG, Dirk Farin <farin@struktur.de>
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

#include "encoder-params.h"



static std::vector<int> power2range(int low,int high)
{
  std::vector<int> vals;
  for (int i=low; i<=high; i*=2)
    vals.push_back(i);
  return vals;
}

encoder_params::encoder_params()
{
  //rateControlMethod = RateControlMethod_ConstantQP;

  min_cb_size.set_ID("min-cb-size"); min_cb_size.set_valid_values(power2range(8,64)); min_cb_size.set_default(8);
  max_cb_size.set_ID("max-cb-size"); max_cb_size.set_valid_values(power2range(8,64)); max_cb_size.set_default(32);
  min_tb_size.set_ID("min-tb-size"); min_tb_size.set_valid_values(power2range(4,32)); min_tb_size.set_default(4);
  max_tb_size.set_ID("max-tb-size"); max_tb_size.set_valid_values(power2range(8,32)); max_tb_size.set_default(32);

  max_transform_hierarchy_depth_intra.set_ID("max-transform-hierarchy-depth-intra");
  max_transform_hierarchy_depth_intra.set_range(0,4);
  max_transform_hierarchy_depth_intra.set_default(3);

  max_transform_hierarchy_depth_inter.set_ID("max-transform-hierarchy-depth-inter");
  max_transform_hierarchy_depth_inter.set_range(0,4);
  max_transform_hierarchy_depth_inter.set_default(3);

  sop_structure.set_ID("sop-structure");

  mAlgo_TB_IntraPredMode.set_ID("TB-IntraPredMode");
  mAlgo_TB_IntraPredMode_Subset.set_ID("TB-IntraPredMode-subset");
  mAlgo_CB_IntraPartMode.set_ID("CB-IntraPartMode");

  mAlgo_TB_RateEstimation.set_ID("TB-RateEstimation");

  mAlgo_MEMode.set_ID("MEMode");
}


void encoder_params::registerParams(config_parameters& config)
{
  config.add_option(&min_cb_size);
  config.add_option(&max_cb_size);
  config.add_option(&min_tb_size);
  config.add_option(&max_tb_size);
  config.add_option(&max_transform_hierarchy_depth_intra);
  config.add_option(&max_transform_hierarchy_depth_inter);

  config.add_option(&sop_structure);

  config.add_option(&mAlgo_TB_IntraPredMode);
  config.add_option(&mAlgo_TB_IntraPredMode_Subset);
  config.add_option(&mAlgo_CB_IntraPartMode);

  config.add_option(&mAlgo_MEMode);
  config.add_option(&mAlgo_TB_RateEstimation);

  mSOP_LowDelay.registerParams(config);
}
