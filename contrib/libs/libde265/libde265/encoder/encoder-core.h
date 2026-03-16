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

#ifndef ANALYZE_H
#define ANALYZE_H

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

#include "libde265/encoder/algo/tb-intrapredmode.h"
#include "libde265/encoder/algo/tb-transform.h"
#include "libde265/encoder/algo/tb-split.h"
#include "libde265/encoder/algo/cb-intrapartmode.h"
#include "libde265/encoder/algo/cb-interpartmode.h"
#include "libde265/encoder/algo/cb-split.h"
#include "libde265/encoder/algo/ctb-qscale.h"
#include "libde265/encoder/algo/cb-mergeindex.h"
//#include "libde265/encoder/algo/cb-skip-or-inter.h"
#include "libde265/encoder/algo/pb-mv.h"
#include "libde265/encoder/algo/cb-skip.h"
#include "libde265/encoder/algo/cb-intra-inter.h"


/*  Encoder search tree, bottom up:

    - Algo_TB_Split - whether TB is split or not

    - Algo_TB_IntraPredMode - choose the intra prediction mode (or NOP, if at the wrong tree level)

    - Algo_CB_IntraPartMode - choose between NxN and 2Nx2N intra parts

    - Algo_CB_PredMode - intra / inter

    - Algo_CB_Split - whether CB is split or not

    - Algo_CTB_QScale - select QScale on CTB granularity
 */


// ========== an encoding algorithm combines a set of algorithm modules ==========

class EncoderCore
{
 public:
  virtual ~EncoderCore() { }

  virtual Algo_CTB_QScale* getAlgoCTBQScale() = 0;

  virtual int getPPS_QP() const = 0;
  virtual int getSlice_QPDelta() const { return 0; }
};


class EncoderCore_Custom : public EncoderCore
{
 public:

  void setParams(struct encoder_params& params);

  void registerParams(config_parameters& config) {
    mAlgo_CTB_QScale_Constant.registerParams(config);
    mAlgo_CB_IntraPartMode_Fixed.registerParams(config);
    mAlgo_CB_InterPartMode_Fixed.registerParams(config);
    mAlgo_PB_MV_Test.registerParams(config);
    mAlgo_PB_MV_Search.registerParams(config);
    mAlgo_TB_IntraPredMode_FastBrute.registerParams(config);
    mAlgo_TB_IntraPredMode_MinResidual.registerParams(config);
    mAlgo_TB_Split_BruteForce.registerParams(config);
  }

  virtual Algo_CTB_QScale* getAlgoCTBQScale() { return &mAlgo_CTB_QScale_Constant; }

  virtual int getPPS_QP() const { return mAlgo_CTB_QScale_Constant.getQP(); }

 private:
  Algo_CTB_QScale_Constant         mAlgo_CTB_QScale_Constant;

  Algo_CB_Split_BruteForce         mAlgo_CB_Split_BruteForce;
  Algo_CB_Skip_BruteForce          mAlgo_CB_Skip_BruteForce;
  Algo_CB_IntraInter_BruteForce    mAlgo_CB_IntraInter_BruteForce;

  Algo_CB_IntraPartMode_BruteForce mAlgo_CB_IntraPartMode_BruteForce;
  Algo_CB_IntraPartMode_Fixed      mAlgo_CB_IntraPartMode_Fixed;

  Algo_CB_InterPartMode_Fixed      mAlgo_CB_InterPartMode_Fixed;
  Algo_CB_MergeIndex_Fixed         mAlgo_CB_MergeIndex_Fixed;

  Algo_PB_MV_Test                  mAlgo_PB_MV_Test;
  Algo_PB_MV_Search                mAlgo_PB_MV_Search;

  Algo_TB_Split_BruteForce          mAlgo_TB_Split_BruteForce;

  Algo_TB_IntraPredMode_BruteForce  mAlgo_TB_IntraPredMode_BruteForce;
  Algo_TB_IntraPredMode_FastBrute   mAlgo_TB_IntraPredMode_FastBrute;
  Algo_TB_IntraPredMode_MinResidual mAlgo_TB_IntraPredMode_MinResidual;

  Algo_TB_Transform                 mAlgo_TB_Transform;
  Algo_TB_RateEstimation_None       mAlgo_TB_RateEstimation_None;
  Algo_TB_RateEstimation_Exact      mAlgo_TB_RateEstimation_Exact;
};



double encode_image(encoder_context*, const de265_image* input, EncoderCore&);

void encode_sequence(encoder_context*);


class Logging
{
public:
  virtual ~Logging() { }

  static void print_logging(const encoder_context* ectx, const char* id, const char* filename);

  virtual const char* name() const = 0;
  virtual void print(const encoder_context* ectx, const char* filename) = 0;
};


LIBDE265_API void en265_print_logging(const encoder_context* ectx, const char* id, const char* filename);

#endif
