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

#ifndef CB_INTRA_INTER_H
#define CB_INTRA_INTER_H

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

class Algo_CB_IntraInter : public Algo_CB
{
 public:
  virtual ~Algo_CB_IntraInter() { }

  void setIntraChildAlgo(Algo_CB* algo) { mIntraAlgo = algo; }
  void setInterChildAlgo(Algo_CB* algo) { mInterAlgo = algo; }

  virtual const char* name() const { return "cb-intra-inter"; }

 protected:
  Algo_CB* mIntraAlgo;
  Algo_CB* mInterAlgo;
};

class Algo_CB_IntraInter_BruteForce : public Algo_CB_IntraInter
{
 public:
  virtual enc_cb* analyze(encoder_context*,
                          context_model_table&,
                          enc_cb* cb);
};

#endif
