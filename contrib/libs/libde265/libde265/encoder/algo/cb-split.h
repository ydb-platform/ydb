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

#ifndef CB_SPLIT_H
#define CB_SPLIT_H

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


/*  Encoder search tree, bottom up:

    - Algo_TB_Split - whether TB is split or not

    - Algo_TB_IntraPredMode - choose the intra prediction mode (or NOP, if at the wrong tree level)

    - Algo_CB_IntraPartMode - choose between NxN and 2Nx2N intra parts

    - Algo_CB_Split - whether CB is split or not

    - Algo_CTB_QScale - select QScale on CTB granularity
 */


// ========== CB split decision ==========

class Algo_CB_Split : public Algo_CB
{
 public:
  virtual ~Algo_CB_Split() { }

  // TODO: probably, this will later be a intra/inter decision which again
  // has two child algorithms, depending on the coding mode.
  void setChildAlgo(Algo_CB* algo) { mChildAlgo = algo; }

  const char* name() const { return "cb-split"; }

 protected:
  Algo_CB* mChildAlgo;

  enc_cb* encode_cb_split(encoder_context* ectx,
                          context_model_table& ctxModel,
                          enc_cb* cb);
};


class Algo_CB_Split_BruteForce : public Algo_CB_Split
{
 public:
  virtual enc_cb* analyze(encoder_context*,
                          context_model_table&,
                          enc_cb* cb);

  const char* name() const { return "cb-split-bruteforce"; }
};

#endif
