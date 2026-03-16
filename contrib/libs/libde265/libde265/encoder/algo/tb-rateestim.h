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

#ifndef TB_RATEESTIM_H
#define TB_RATEESTIM_H

#include "libde265/nal-parser.h"
#include "libde265/decctx.h"
#include "libde265/encoder/encoder-types.h"
#include "libde265/encoder/algo/algo.h"
#include "libde265/slice.h"
#include "libde265/scan.h"
#include "libde265/intrapred.h"
#include "libde265/transform.h"
#include "libde265/fallback-dct.h"
#include "libde265/quality.h"
#include "libde265/fallback.h"
#include "libde265/configparam.h"


enum ALGO_TB_RateEstimation {
  ALGO_TB_RateEstimation_None,
  ALGO_TB_RateEstimation_Exact
};

class option_ALGO_TB_RateEstimation : public choice_option<enum ALGO_TB_RateEstimation>
{
 public:
  option_ALGO_TB_RateEstimation() {
    add_choice("none" ,ALGO_TB_RateEstimation_None);
    add_choice("exact",ALGO_TB_RateEstimation_Exact, true);
  }
};



class Algo_TB_RateEstimation : public Algo
{
 public:
  virtual ~Algo_TB_RateEstimation() { }

  virtual float encode_transform_unit(encoder_context* ectx,
                                      context_model_table& ctxModel,
                                      const enc_tb* tb, const enc_cb* cb,
                                      int x0,int y0, int xBase,int yBase,
                                      int log2TrafoSize, int trafoDepth, int blkIdx) = 0;

  virtual const char* name() const { return "tb-rateestimation"; }
};


class Algo_TB_RateEstimation_None : public Algo_TB_RateEstimation
{
 public:
  virtual float encode_transform_unit(encoder_context* ectx,
                                      context_model_table& ctxModel,
                                      const enc_tb* tb, const enc_cb* cb,
                                      int x0,int y0, int xBase,int yBase,
                                      int log2TrafoSize, int trafoDepth, int blkIdx)
  {
    leaf(cb, NULL);
    return 0.0f;
  }

  virtual const char* name() const { return "tb-rateestimation-none"; }
};


class Algo_TB_RateEstimation_Exact : public Algo_TB_RateEstimation
{
 public:
  virtual float encode_transform_unit(encoder_context* ectx,
                                      context_model_table& ctxModel,
                                      const enc_tb* tb, const enc_cb* cb,
                                      int x0,int y0, int xBase,int yBase,
                                      int log2TrafoSize, int trafoDepth, int blkIdx);

  virtual const char* name() const { return "tb-rateestimation-exact"; }
};


#endif
