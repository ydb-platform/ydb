/*
 * H.265 video codec.
 * Copyright (c) 2013-2014 struktur AG, Dirk Farin <farin@struktur.de>
 *
 * Authors: Dirk Farin <farin@struktur.de>
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

#ifndef ENCODER_PARAMS_H
#define ENCODER_PARAMS_H

#include "libde265/encoder/encoder-types.h"
#include "libde265/encoder/encoder-core.h"
#include "libde265/encoder/sop.h"


enum RateControlMethod
  {
    RateControlMethod_ConstantQP,
    RateControlMethod_ConstantLambda
  };

enum IntraPredSearch
  {
    IntraPredSearch_Complete
  };


enum SOP_Structure
  {
    SOP_Intra,
    SOP_LowDelay
  };

class option_SOP_Structure : public choice_option<enum SOP_Structure>
{
 public:
  option_SOP_Structure() {
    add_choice("intra",     SOP_Intra);
    add_choice("low-delay", SOP_LowDelay, true);
  }
};


enum MEMode
  {
    MEMode_Test,
    MEMode_Search
  };

class option_MEMode : public choice_option<enum MEMode>
{
 public:
  option_MEMode() {
    add_choice("test",   MEMode_Test, true);
    add_choice("search", MEMode_Search);
  }
};


struct encoder_params
{
  encoder_params();

  void registerParams(config_parameters& config);


  // CB quad-tree

  option_int min_cb_size;
  option_int max_cb_size;

  option_int min_tb_size;
  option_int max_tb_size;

  option_int max_transform_hierarchy_depth_intra;
  option_int max_transform_hierarchy_depth_inter;


  option_SOP_Structure sop_structure;

  sop_creator_trivial_low_delay::params mSOP_LowDelay;


  // --- Algo_TB_IntraPredMode

  option_ALGO_TB_IntraPredMode        mAlgo_TB_IntraPredMode;
  option_ALGO_TB_IntraPredMode_Subset mAlgo_TB_IntraPredMode_Subset;

  //Algo_TB_IntraPredMode_FastBrute::params TB_IntraPredMode_FastBrute;
  //Algo_TB_IntraPredMode_MinResidual::params TB_IntraPredMode_MinResidual;


  // --- Algo_TB_Split_BruteForce

  //Algo_TB_Split_BruteForce::params  TB_Split_BruteForce;


  // --- Algo_CB_IntraPartMode

  option_ALGO_CB_IntraPartMode mAlgo_CB_IntraPartMode;

  //Algo_CB_IntraPartMode_Fixed::params CB_IntraPartMode_Fixed;

  // --- Algo_CB_Split

  // --- Algo_CTB_QScale

  //Algo_CTB_QScale_Constant::params    CTB_QScale_Constant;

  option_MEMode mAlgo_MEMode;


  // intra-prediction

  enum IntraPredSearch intraPredSearch;


  // rate-control

  enum RateControlMethod rateControlMethod;
  option_ALGO_TB_RateEstimation mAlgo_TB_RateEstimation;

  //int constant_QP;
  //int lambda;
};


#endif
