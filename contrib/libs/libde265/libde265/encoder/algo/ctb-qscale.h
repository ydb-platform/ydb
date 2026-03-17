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

#ifndef CTB_QSCALE_H
#define CTB_QSCALE_H

#include "libde265/nal-parser.h"
#include "libde265/decctx.h"
#include "libde265/encoder/algo/algo.h"
#include "libde265/slice.h"
#include "libde265/scan.h"
#include "libde265/intrapred.h"
#include "libde265/transform.h"
#include "libde265/fallback-dct.h"
#include "libde265/quality.h"
#include "libde265/fallback.h"
#include "libde265/configparam.h"

#include "libde265/encoder/algo/cb-split.h"


/*  Encoder search tree, bottom up:

    - Algo_TB_Split - whether TB is split or not

    - Algo_TB_IntraPredMode - choose the intra prediction mode (or NOP, if at the wrong tree level)

    - Algo_CB_IntraPartMode - choose between NxN and 2Nx2N intra parts

    - Algo_CB_Split - whether CB is split or not

    - Algo_CTB_QScale - select QScale on CTB granularity
 */


// ========== choose a qscale at CTB level ==========

class Algo_CTB_QScale : public Algo
{
 public:
 Algo_CTB_QScale() : mChildAlgo(NULL) { }
  virtual ~Algo_CTB_QScale() { }

  virtual enc_cb* analyze(encoder_context*,
                          context_model_table&,
                          int ctb_x,int ctb_y) = 0;

  void setChildAlgo(Algo_CB_Split* algo) { mChildAlgo = algo; }

 protected:
  Algo_CB_Split* mChildAlgo;
};



class Algo_CTB_QScale_Constant : public Algo_CTB_QScale
{
 public:
  struct params
  {
    params() {
      mQP.set_range(1,51);
      mQP.set_default(27);
      mQP.set_ID("CTB-QScale-Constant");
      mQP.set_cmd_line_options("qp",'q');
    }

    option_int mQP;
  };

  void setParams(const params& p) { mParams=p; }

  void registerParams(config_parameters& config) {
    config.add_option(&mParams.mQP);
  }

  virtual enc_cb* analyze(encoder_context*,
                          context_model_table&,
                          int ctb_x,int ctb_y);

  int getQP() const { return mParams.mQP; }

  const char* name() const { return "ctb-qscale-constant"; }

 private:
  params mParams;
};


#endif
