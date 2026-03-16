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

#ifndef TB_INTRAPREDMODE_H
#define TB_INTRAPREDMODE_H

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


// ========== TB intra prediction mode ==========

enum ALGO_TB_IntraPredMode {
  ALGO_TB_IntraPredMode_BruteForce,
  ALGO_TB_IntraPredMode_FastBrute,
  ALGO_TB_IntraPredMode_MinResidual
};

class option_ALGO_TB_IntraPredMode : public choice_option<enum ALGO_TB_IntraPredMode>
{
 public:
  option_ALGO_TB_IntraPredMode() {
    add_choice("min-residual",ALGO_TB_IntraPredMode_MinResidual);
    add_choice("brute-force" ,ALGO_TB_IntraPredMode_BruteForce);
    add_choice("fast-brute"  ,ALGO_TB_IntraPredMode_FastBrute, true);
  }
};


enum TBBitrateEstimMethod {
  //TBBitrateEstim_AccurateBits,
  TBBitrateEstim_SSD,
  TBBitrateEstim_SAD,
  TBBitrateEstim_SATD_DCT,
  TBBitrateEstim_SATD_Hadamard
};

class option_TBBitrateEstimMethod : public choice_option<enum TBBitrateEstimMethod>
{
 public:
  option_TBBitrateEstimMethod() {
    add_choice("ssd",TBBitrateEstim_SSD);
    add_choice("sad",TBBitrateEstim_SAD);
    add_choice("satd-dct",TBBitrateEstim_SATD_DCT);
    add_choice("satd",TBBitrateEstim_SATD_Hadamard, true);
  }
};

class Algo_TB_Split;


/** Base class for intra prediction-mode algorithms.
    Selects one of the 35 prediction modes.
 */
class Algo_TB_IntraPredMode : public Algo
{
 public:
  Algo_TB_IntraPredMode() : mTBSplitAlgo(NULL) { }
  virtual ~Algo_TB_IntraPredMode() { }

  virtual enc_tb* analyze(encoder_context*,
                          context_model_table&,
                          const de265_image* input,
                          enc_tb* tb,
                          int TrafoDepth, int MaxTrafoDepth, int IntraSplitFlag) = 0;

  void setChildAlgo(Algo_TB_Split* algo) { mTBSplitAlgo = algo; }

  const char* name() const { return "tb-intrapredmode"; }

 protected:
  Algo_TB_Split* mTBSplitAlgo;
};


enum ALGO_TB_IntraPredMode_Subset {
  ALGO_TB_IntraPredMode_Subset_All,
  ALGO_TB_IntraPredMode_Subset_HVPlus,
  ALGO_TB_IntraPredMode_Subset_DC,
  ALGO_TB_IntraPredMode_Subset_Planar
};

class option_ALGO_TB_IntraPredMode_Subset : public choice_option<enum ALGO_TB_IntraPredMode_Subset>
{
 public:
  option_ALGO_TB_IntraPredMode_Subset() {
    add_choice("all"   ,ALGO_TB_IntraPredMode_Subset_All, true);
    add_choice("HV+"   ,ALGO_TB_IntraPredMode_Subset_HVPlus);
    add_choice("DC"    ,ALGO_TB_IntraPredMode_Subset_DC);
    add_choice("planar",ALGO_TB_IntraPredMode_Subset_Planar);
  }
};


/** Utility class for intra prediction-mode algorithm that uses a subset of modes.
 */
class Algo_TB_IntraPredMode_ModeSubset : public Algo_TB_IntraPredMode
{
 public:
  Algo_TB_IntraPredMode_ModeSubset() {
    enableAllIntraPredModes();
  }

  void enableAllIntraPredModes() {
    for (int i=0;i<35;i++) {
      mPredMode_enabled[i] = true;
      mPredMode[i] = (enum IntraPredMode)i;
    }

    mNumPredModesEnabled = 35;
  }

  void disableAllIntraPredModes() {
    for (int i=0;i<35;i++) {
      mPredMode_enabled[i] = false;
    }

    mNumPredModesEnabled = 0;
  }

  void enableIntraPredMode(enum IntraPredMode mode) {
    if (!mPredMode_enabled[mode]) {
      mPredMode[mNumPredModesEnabled] = mode;
      mPredMode_enabled[mode] = true;
      mNumPredModesEnabled++;
    }
  }

  // TODO: method to disable modes

  void enableIntraPredModeSubset(enum ALGO_TB_IntraPredMode_Subset subset) {
    switch (subset)
      {
      case ALGO_TB_IntraPredMode_Subset_All: // activate all is the default
        for (int i=0;i<35;i++) { enableIntraPredMode((enum IntraPredMode)i); }
        break;
      case ALGO_TB_IntraPredMode_Subset_DC:
        disableAllIntraPredModes();
        enableIntraPredMode(INTRA_DC);
        break;
      case ALGO_TB_IntraPredMode_Subset_Planar:
        disableAllIntraPredModes();
        enableIntraPredMode(INTRA_PLANAR);
        break;
      case ALGO_TB_IntraPredMode_Subset_HVPlus:
        disableAllIntraPredModes();
        enableIntraPredMode(INTRA_DC);
        enableIntraPredMode(INTRA_PLANAR);
        enableIntraPredMode(INTRA_ANGULAR_10);
        enableIntraPredMode(INTRA_ANGULAR_26);
        break;
      }
  }


  enum IntraPredMode getPredMode(int idx) const {
    assert(idx<mNumPredModesEnabled);
    return mPredMode[idx];
  }

  int nPredModesEnabled() const {
    return mNumPredModesEnabled;
  }

  bool isPredModeEnabled(enum IntraPredMode mode) {
    return mPredMode_enabled[mode];
  }

 private:
  IntraPredMode mPredMode[35];
  bool mPredMode_enabled[35];
  int  mNumPredModesEnabled;
};


/** Algorithm that brute-forces through all intra prediction mode.
 */
class Algo_TB_IntraPredMode_BruteForce : public Algo_TB_IntraPredMode_ModeSubset
{
 public:

  virtual enc_tb* analyze(encoder_context*,
                          context_model_table&,
                          const de265_image* input,
                          enc_tb* tb,
                          int TrafoDepth, int MaxTrafoDepth, int IntraSplitFlag);


  const char* name() const { return "tb-intrapredmode_BruteForce"; }
};


/** Algorithm that makes a quick pre-selection of modes and then brute-forces through them.
 */
class Algo_TB_IntraPredMode_FastBrute : public Algo_TB_IntraPredMode_ModeSubset
{
 public:

  struct params
  {
    params() {
      keepNBest.set_ID("IntraPredMode-FastBrute-keepNBest");
      keepNBest.set_range(0,32);
      keepNBest.set_default(5);

      bitrateEstimMethod.set_ID("IntraPredMode-FastBrute-estimator");
    }

    option_TBBitrateEstimMethod bitrateEstimMethod;
    option_int keepNBest;
  };

  void registerParams(config_parameters& config) {
    config.add_option(&mParams.keepNBest);
    config.add_option(&mParams.bitrateEstimMethod);
  }

  void setParams(const params& p) { mParams=p; }


  virtual enc_tb* analyze(encoder_context*,
                          context_model_table&,
                          const de265_image* input,
                          enc_tb* tb,
                          int TrafoDepth, int MaxTrafoDepth, int IntraSplitFlag);


  const char* name() const { return "tb-intrapredmode_FastBrute"; }

 private:
  params mParams;
};


/** Algorithm that selects the intra prediction mode on minimum residual only.
 */
class Algo_TB_IntraPredMode_MinResidual : public Algo_TB_IntraPredMode_ModeSubset
{
 public:

  struct params
  {
    params() {
      bitrateEstimMethod.set_ID("IntraPredMode-MinResidual-estimator");
    }

    option_TBBitrateEstimMethod bitrateEstimMethod;
  };

  void setParams(const params& p) { mParams=p; }

  void registerParams(config_parameters& config) {
    config.add_option(&mParams.bitrateEstimMethod);
  }

  enc_tb* analyze(encoder_context*,
                  context_model_table&,
                  const de265_image* input,
                  enc_tb* tb,
                  int TrafoDepth, int MaxTrafoDepth, int IntraSplitFlag);

  const char* name() const { return "tb-intrapredmode_MinResidual"; }

 private:
  params mParams;
};

#endif
