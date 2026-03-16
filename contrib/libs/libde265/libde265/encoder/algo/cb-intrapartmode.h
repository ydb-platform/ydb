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

#ifndef CB_INTRAPARTMODE_H
#define CB_INTRAPARTMODE_H

#include "libde265/nal-parser.h"
#include "libde265/decctx.h"
#include "libde265/encoder/encoder-types.h"
#include "libde265/slice.h"
#include "libde265/scan.h"
#include "libde265/intrapred.h"
#include "libde265/transform.h"
#include "libde265/fallback-dct.h"
#include "libde265/quality.h"
#include "libde265/fallback.h"
#include "libde265/configparam.h"

#include "libde265/encoder/algo/algo.h"
#include "libde265/encoder/algo/tb-intrapredmode.h"
#include "libde265/encoder/algo/tb-split.h"


/*  Encoder search tree, bottom up:

    - Algo_TB_Split - whether TB is split or not

    - Algo_TB_IntraPredMode - choose the intra prediction mode (or NOP, if at the wrong tree level)

    - Algo_CB_IntraPartMode - choose between NxN and 2Nx2N intra parts

    - Algo_CB_Split - whether CB is split or not

    - Algo_CTB_QScale - select QScale on CTB granularity
 */


// ========== CB intra NxN vs. 2Nx2N decision ==========

enum ALGO_CB_IntraPartMode {
  ALGO_CB_IntraPartMode_BruteForce,
  ALGO_CB_IntraPartMode_Fixed
};

class option_ALGO_CB_IntraPartMode : public choice_option<enum ALGO_CB_IntraPartMode>
{
 public:
  option_ALGO_CB_IntraPartMode() {
    add_choice("fixed",      ALGO_CB_IntraPartMode_Fixed);
    add_choice("brute-force",ALGO_CB_IntraPartMode_BruteForce, true);
  }
};


class Algo_CB_IntraPartMode : public Algo_CB
{
 public:
  Algo_CB_IntraPartMode() : mTBIntraPredModeAlgo(NULL) { }
  virtual ~Algo_CB_IntraPartMode() { }

  virtual enc_cb* analyze(encoder_context*,
                          context_model_table&,
                          enc_cb* cb) = 0;

  void setChildAlgo(Algo_TB_IntraPredMode* algo) { mTBIntraPredModeAlgo = algo; }

  virtual const char* name() const { return "cb-intrapartmode"; }

 protected:
  Algo_TB_IntraPredMode* mTBIntraPredModeAlgo;
};

/* Try both NxN, 2Nx2N and choose better one.
 */
class Algo_CB_IntraPartMode_BruteForce : public Algo_CB_IntraPartMode
{
 public:
  virtual enc_cb* analyze(encoder_context*,
                          context_model_table&,
                          enc_cb* cb);

  virtual const char* name() const { return "cb-intrapartmode-bruteforce"; }
};


class option_PartMode : public choice_option<enum PartMode> // choice_option
{
 public:
  option_PartMode() {
    add_choice("NxN",   PART_NxN);
    add_choice("2Nx2N", PART_2Nx2N, true);
  }
};


/* Always use choose selected part mode.
   If NxN is chosen but cannot be applied (CB tree not at maximum depth), 2Nx2N is used instead.
 */
class Algo_CB_IntraPartMode_Fixed : public Algo_CB_IntraPartMode
{
 public:
 Algo_CB_IntraPartMode_Fixed() { }

  struct params
  {
    params() {
      partMode.set_ID("CB-IntraPartMode-Fixed-partMode");
    }

    option_PartMode partMode;
  };

  void registerParams(config_parameters& config) {
    config.add_option(&mParams.partMode);
  }

  void setParams(const params& p) { mParams=p; }

  virtual enc_cb* analyze(encoder_context* ectx,
                          context_model_table& ctxModel,
                          enc_cb* cb);

  virtual const char* name() const { return "cb-intrapartmode-fixed"; }

 private:
  params mParams;
};


#endif
