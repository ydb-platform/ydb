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

#ifndef CB_INTERPARTMODE_H
#define CB_INTERPARTMODE_H

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
#include "libde265/encoder/algo/tb-intrapredmode.h"
#include "libde265/encoder/algo/tb-split.h"
#include "libde265/encoder/algo/cb-intrapartmode.h"


// ========== CB Intra/Inter decision ==========

class Algo_CB_InterPartMode : public Algo_CB
{
 public:
  virtual ~Algo_CB_InterPartMode() { }

  void setChildAlgo(Algo_PB* algo) { mChildAlgo = algo; }

  virtual const char* name() const { return "cb-interpartmode"; }

 protected:
  Algo_PB* mChildAlgo;

  enc_cb* codeAllPBs(encoder_context*,
                     context_model_table&,
                     enc_cb* cb);
};




class option_InterPartMode : public choice_option<enum PartMode> // choice_option
{
 public:
  option_InterPartMode() {
    add_choice("2Nx2N", PART_2Nx2N, true);
    add_choice("NxN",   PART_NxN);
    add_choice("Nx2N",  PART_Nx2N);
    add_choice("2NxN",  PART_2NxN);
    add_choice("2NxnU", PART_2NxnU);
    add_choice("2NxnD", PART_2NxnD);
    add_choice("nLx2N", PART_nLx2N);
    add_choice("nRx2N", PART_nRx2N);
  }
};

class Algo_CB_InterPartMode_Fixed : public Algo_CB_InterPartMode
{
 public:
  struct params
  {
    params() {
      partMode.set_ID("CB-InterPartMode-Fixed-partMode");
    }

    option_InterPartMode partMode;
  };

  void registerParams(config_parameters& config) {
    config.add_option(&mParams.partMode);
  }

  void setParams(const params& p) { mParams=p; }

  virtual enc_cb* analyze(encoder_context*,
                          context_model_table&,
                          enc_cb* cb);

  virtual const char* name() const { return "cb-interpartmode-fixed"; }

 private:
  params mParams;
};

#endif
