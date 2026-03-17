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

#ifndef PB_MV_H
#define PB_MV_H

#include "libde265/nal-parser.h"
#include "libde265/decctx.h"
#include "libde265/slice.h"
#include "libde265/scan.h"
#include "libde265/intrapred.h"
#include "libde265/transform.h"
#include "libde265/fallback-dct.h"
#include "libde265/quality.h"
#include "libde265/fallback.h"
#include "libde265/configparam.h"

#include "libde265/encoder/algo/algo.h"


// ========== CB Intra/Inter decision ==========

class Algo_TB_Split;


class Algo_PB_MV : public Algo_PB
{
 public:
 Algo_PB_MV() : mTBSplitAlgo(NULL) { }
  virtual ~Algo_PB_MV() { }

  void setChildAlgo(Algo_TB_Split* algo) { mTBSplitAlgo = algo; }

 protected:
  Algo_TB_Split* mTBSplitAlgo;
};




enum MVTestMode
  {
    MVTestMode_Zero,
    MVTestMode_Random,
    MVTestMode_Horizontal,
    MVTestMode_Vertical
  };

class option_MVTestMode : public choice_option<enum MVTestMode>
{
 public:
  option_MVTestMode() {
    add_choice("zero",   MVTestMode_Zero);
    add_choice("random", MVTestMode_Random);
    add_choice("horiz",  MVTestMode_Horizontal, true);
    add_choice("verti",  MVTestMode_Vertical);
  }
};


class Algo_PB_MV_Test : public Algo_PB_MV
{
 public:
 Algo_PB_MV_Test() : mCodeResidual(false) { }

  struct params
  {
    params() {
      testMode.set_ID("PB-MV-TestMode");
      range.set_ID   ("PB-MV-Range");
      range.set_default(4);
    }

    option_MVTestMode testMode;
    option_int        range;
  };

  void registerParams(config_parameters& config) {
    config.add_option(&mParams.testMode);
    config.add_option(&mParams.range);
  }

  void setParams(const params& p) { mParams=p; }

  virtual enc_cb* analyze(encoder_context*,
                          context_model_table&,
                          enc_cb* cb,
                          int PBidx, int x,int y,int w,int h);

 private:
  params mParams;

  bool mCodeResidual;
};




enum MVSearchAlgo
  {
    MVSearchAlgo_Zero,
    MVSearchAlgo_Full,
    MVSearchAlgo_Diamond,
    MVSearchAlgo_PMVFast
  };

class option_MVSearchAlgo : public choice_option<enum MVSearchAlgo>
{
 public:
  option_MVSearchAlgo() {
    add_choice("zero",   MVSearchAlgo_Zero);
    add_choice("full",   MVSearchAlgo_Full, true);
    add_choice("diamond",MVSearchAlgo_Diamond);
    add_choice("pmvfast",MVSearchAlgo_PMVFast);
  }
};


class Algo_PB_MV_Search : public Algo_PB_MV
{
 public:
 Algo_PB_MV_Search() : mCodeResidual(false) { }

  struct params
  {
    params() {
      mvSearchAlgo.set_ID("PB-MV-Search-Algo");
      hrange.set_ID      ("PB-MV-Search-HRange");
      vrange.set_ID      ("PB-MV-Search-VRange");
      hrange.set_default(8);
      vrange.set_default(8);
    }

    option_MVSearchAlgo mvSearchAlgo;
    option_int        hrange;
    option_int        vrange;
  };

  void registerParams(config_parameters& config) {
    config.add_option(&mParams.mvSearchAlgo);
    config.add_option(&mParams.hrange);
    config.add_option(&mParams.vrange);
  }

  void setParams(const params& p) { mParams=p; }

  virtual enc_cb* analyze(encoder_context*,
                          context_model_table&,
                          enc_cb* cb,
                          int PBidx, int x,int y,int w,int h);

 private:
  params mParams;

  bool mCodeResidual;
};

#endif
