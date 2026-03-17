/*
 * H.265 video codec.
 * Copyright (c) 2013-2014 struktur AG, Dirk Farin <farin@struktur.de>
 *
 * Authors: struktur AG, Dirk Farin <farin@struktur.de>
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


#include "libde265/encoder/encoder-context.h"
#include "libde265/encoder/algo/tb-split.h"
#include "libde265/encoder/algo/coding-options.h"
#include "libde265/encoder/encoder-intrapred.h"
#include <assert.h>
#include <limits>
#include <math.h>
#include <algorithm>
#include <iostream>


float get_intra_pred_mode_bits(const enum IntraPredMode candidates[3],
                               enum IntraPredMode intraMode,
                               enum IntraPredMode intraModeC,
                               context_model_table& context_models,
                               bool includeChroma)
{
  float rate;
  int enc_bin;

  /**/ if (candidates[0]==intraMode) { rate = 1; enc_bin=1; }
  else if (candidates[1]==intraMode) { rate = 2; enc_bin=1; }
  else if (candidates[2]==intraMode) { rate = 2; enc_bin=1; }
  else { rate = 5; enc_bin=0; }

  CABAC_encoder_estim estim;
  estim.set_context_models(&context_models);
  logtrace(LogSymbols,"$1 prev_intra_luma_pred_flag=%d\n",enc_bin);
  estim.write_CABAC_bit(CONTEXT_MODEL_PREV_INTRA_LUMA_PRED_FLAG, enc_bin);

  // TODO: currently we make the chroma-pred-mode decision for each part even
  // in NxN part mode. Since we always set this to the same value, it does not
  // matter. However, we should only add the rate for it once (for blkIdx=0).

  if (includeChroma) {
    assert(intraMode == intraModeC);

    logtrace(LogSymbols,"$1 intra_chroma_pred_mode=%d\n",0);
    estim.write_CABAC_bit(CONTEXT_MODEL_INTRA_CHROMA_PRED_MODE,0);
  }
  rate += estim.getRDBits();

  return rate;
}




float estim_TB_bitrate(const encoder_context* ectx,
                       const de265_image* input,
                       const enc_tb* tb,
                       enum TBBitrateEstimMethod method)
{
  int x0 = tb->x;
  int y0 = tb->y;
  int blkSize = 1 << tb->log2Size;

  float distortion;

  switch (method)
    {
    case TBBitrateEstim_SSD:
      return SSD(input->get_image_plane_at_pos(0, x0,y0),
                 input->get_image_stride(0),
                 tb->intra_prediction[0]->get_buffer_u8(),
                 tb->intra_prediction[0]->getStride(),
                 blkSize, blkSize);
      break;

    case TBBitrateEstim_SAD:
      return SAD(input->get_image_plane_at_pos(0, x0,y0),
                 input->get_image_stride(0),
                 tb->intra_prediction[0]->get_buffer_u8(),
                 tb->intra_prediction[0]->getStride(),
                 blkSize, blkSize);
      break;

    case TBBitrateEstim_SATD_DCT:
    case TBBitrateEstim_SATD_Hadamard:
      {
        int16_t coeffs[64*64];
        int16_t diff[64*64];

        // Usually, TBs are max. 32x32 big. However, it may be that this is still called
        // for 64x64 blocks, because we are sometimes computing an intra pred mode for a
        // whole CTB at once.
        assert(blkSize <= 64);

        diff_blk(diff,blkSize,
                 input->get_image_plane_at_pos(0, x0,y0), input->get_image_stride(0),
                 tb->intra_prediction[0]->get_buffer_u8(),
                 tb->intra_prediction[0]->getStride(),
                 blkSize);

        void (*transform)(int16_t *coeffs, const int16_t *src, ptrdiff_t stride);


        if (tb->log2Size == 6) {
          // hack for 64x64 blocks: compute 4 times 32x32 blocks

          if (method == TBBitrateEstim_SATD_Hadamard) {
            transform = ectx->acceleration.hadamard_transform_8[6-1-2];

            transform(coeffs,         &diff[0       ], 64);
            transform(coeffs+1*32*32, &diff[32      ], 64);
            transform(coeffs+2*32*32, &diff[32*64   ], 64);
            transform(coeffs+3*32*32, &diff[32*64+32], 64);
          }
          else {
            transform = ectx->acceleration.fwd_transform_8[6-1-2];

            transform(coeffs,         &diff[0       ], 64);
            transform(coeffs+1*32*32, &diff[32      ], 64);
            transform(coeffs+2*32*32, &diff[32*64   ], 64);
            transform(coeffs+3*32*32, &diff[32*64+32], 64);
          }
        }
        else {
          assert(tb->log2Size-2 <= 3);

          if (method == TBBitrateEstim_SATD_Hadamard) {
            ectx->acceleration.hadamard_transform_8[tb->log2Size-2](coeffs, diff, &diff[blkSize] - &diff[0]);
          }
          else {
            ectx->acceleration.fwd_transform_8[tb->log2Size-2](coeffs, diff, &diff[blkSize] - &diff[0]);
          }
        }

        float distortion=0;
        for (int i=0;i<blkSize*blkSize;i++) {
          distortion += abs_value((int)coeffs[i]);
        }

        return distortion;
      }
      break;

      /*
    case TBBitrateEstim_AccurateBits:
      assert(false);
      return 0;
      */
    }

  assert(false);
  return 0;
}



enc_tb*
Algo_TB_IntraPredMode_BruteForce::analyze(encoder_context* ectx,
                                          context_model_table& ctxModel,
                                          const de265_image* input,
                                          enc_tb* tb,
                                          int TrafoDepth, int MaxTrafoDepth,
                                          int IntraSplitFlag)
{
  enter();

  enc_cb* cb = tb->cb;

  bool selectIntraPredMode = false;
  selectIntraPredMode |= (cb->PredMode==MODE_INTRA && cb->PartMode==PART_2Nx2N && TrafoDepth==0);
  selectIntraPredMode |= (cb->PredMode==MODE_INTRA && cb->PartMode==PART_NxN   && TrafoDepth==1);

  if (selectIntraPredMode) {

    CodingOptions<enc_tb> options(ectx, tb, ctxModel);
    CodingOption<enc_tb>  option[35];

    for (int i=0;i<35;i++) {
      bool computeIntraMode = isPredModeEnabled((enum IntraPredMode)i);
      option[i] = options.new_option(computeIntraMode);
    }

    options.start();


    const seq_parameter_set* sps = &ectx->get_sps();
    enum IntraPredMode candidates[3];
    fillIntraPredModeCandidates(candidates, tb->x,tb->y,
                                tb->x > 0, tb->y > 0, ectx->ctbs, &ectx->get_sps());


    for (int i = 0; i<35; i++) {
      if (!option[i]) {
        continue;
      }


      enum IntraPredMode intraMode = (IntraPredMode)i;


      option[i].begin();

      enc_tb* tb_option = option[i].get_node();

      *tb_option->downPtr = tb_option;

      tb_option->intra_mode        = intraMode;

      // set chroma mode to same mode is its luma mode
      enum IntraPredMode intraModeC;

      if (cb->PartMode==PART_2Nx2N || ectx->get_sps().ChromaArrayType==CHROMA_444) {
        intraModeC = intraMode;
      }
      else {
        intraModeC = tb_option->parent->children[0]->intra_mode;
      }

      tb_option->intra_mode_chroma = intraModeC;


      descend(tb_option,"%d",intraMode);
      tb_option = mTBSplitAlgo->analyze(ectx,option[i].get_context(),input,tb_option,
                                        TrafoDepth, MaxTrafoDepth, IntraSplitFlag);
      option[i].set_node(tb_option);
      ascend();

      float intraPredModeBits = get_intra_pred_mode_bits(candidates,
                                                         intraMode,
                                                         intraModeC,
                                                         option[i].get_context(),
                                                         tb_option->blkIdx == 0);

      tb_option->rate_withoutCbfChroma += intraPredModeBits;
      tb_option->rate += intraPredModeBits;

      option[i].end();
    }


    options.compute_rdo_costs();

    enc_tb* bestTB = options.return_best_rdo_node();

    return bestTB;
  }
  else {
    descend(tb,"NOP"); // TODO: not parent
    enc_tb* new_tb = mTBSplitAlgo->analyze(ectx, ctxModel, input, tb,
                                           TrafoDepth, MaxTrafoDepth, IntraSplitFlag);
    ascend();

    return new_tb;
  }

  assert(false);
  return nullptr;
}



enc_tb*
Algo_TB_IntraPredMode_MinResidual::analyze(encoder_context* ectx,
                                           context_model_table& ctxModel,
                                           const de265_image* input,
                                           enc_tb* tb,
                                           int TrafoDepth, int MaxTrafoDepth, int IntraSplitFlag)
{
  enter();

  enc_cb* cb = tb->cb;

  int x0 = tb->x;
  int y0 = tb->y;
  int xBase = cb->x;
  int yBase = cb->y;
  int log2TbSize = tb->log2Size;

  bool selectIntraPredMode = false;
  selectIntraPredMode |= (cb->PredMode==MODE_INTRA && cb->PartMode==PART_2Nx2N && TrafoDepth==0);
  selectIntraPredMode |= (cb->PredMode==MODE_INTRA && cb->PartMode==PART_NxN   && TrafoDepth==1);

  //printf("tb-intrapredmode: %d %d %d\n", cb->PredMode, cb->PartMode, TrafoDepth);

  if (selectIntraPredMode) {

    *tb->downPtr = tb;

    enum IntraPredMode intraMode;
    float minDistortion = std::numeric_limits<float>::max();

    assert(nPredModesEnabled()>=1);

    if (nPredModesEnabled()==1) {
      intraMode = getPredMode(0);
    }
    else {
      tb->intra_prediction[0] = std::make_shared<small_image_buffer>(log2TbSize, sizeof(uint8_t));

      for (int idx=0;idx<nPredModesEnabled();idx++) {
        enum IntraPredMode mode = getPredMode(idx);

        tb->intra_mode = mode;
        decode_intra_prediction_from_tree(ectx->img, tb, ectx->ctbs, ectx->get_sps(), 0);

        float distortion;
        distortion = estim_TB_bitrate(ectx, input, tb,
                                      mParams.bitrateEstimMethod());

        if (distortion<minDistortion) {
          minDistortion = distortion;
          intraMode = mode;
        }
      }
    }

    //cb->intra.pred_mode[blkIdx] = intraMode;
    //if (blkIdx==0) { cb->intra.chroma_mode = intraMode; }

    //intraMode=(IntraPredMode)17; // HACK

    //printf("INTRA MODE (%d;%d) = %d\n",x0,y0,intraMode);

    tb->intra_mode        = intraMode;

    // set chroma mode to same mode is its luma mode
    enum IntraPredMode intraModeC;

    if (cb->PartMode==PART_2Nx2N || ectx->get_sps().ChromaArrayType==CHROMA_444) {
      intraModeC = intraMode;
    }
    else {
      intraModeC = tb->parent->children[0]->intra_mode;
    }

    tb->intra_mode_chroma = intraModeC;


    // Note: cannot prepare intra prediction pixels here, because this has to
    // be done at the lowest TB split level.


    descend(tb,"%d",intraMode);
    tb = mTBSplitAlgo->analyze(ectx,ctxModel,input,tb,
                               TrafoDepth, MaxTrafoDepth, IntraSplitFlag);
    ascend();

    debug_show_image(ectx->img, 0);


    enum IntraPredMode candidates[3];
    fillIntraPredModeCandidates(candidates, x0,y0,
                                x0>0, y0>0, ectx->ctbs, &ectx->get_sps());

    float intraPredModeBits = get_intra_pred_mode_bits(candidates,
                                                       intraMode,
                                                       intraModeC,
                                                       ctxModel,
                                                       tb->blkIdx == 0);

    tb->rate_withoutCbfChroma += intraPredModeBits;
    tb->rate += intraPredModeBits;

    return tb;
  }
  else {
    descend(tb,"NOP");
    enc_tb* nop_tb = mTBSplitAlgo->analyze(ectx, ctxModel, input, tb,
                                           TrafoDepth, MaxTrafoDepth,
                                           IntraSplitFlag);
    ascend();
    return nop_tb;
  }

  assert(false);
  return nullptr;
}

static bool sortDistortions(std::pair<enum IntraPredMode,float> i,
                            std::pair<enum IntraPredMode,float> j)
{
  return i.second < j.second;
}


enc_tb*
Algo_TB_IntraPredMode_FastBrute::analyze(encoder_context* ectx,
                                         context_model_table& ctxModel,
                                         const de265_image* input,
                                         enc_tb* tb,
                                         int TrafoDepth, int MaxTrafoDepth, int IntraSplitFlag)
{
  enc_cb* cb = tb->cb;

  bool selectIntraPredMode = false;
  selectIntraPredMode |= (cb->PredMode==MODE_INTRA && cb->PartMode==PART_2Nx2N && TrafoDepth==0);
  selectIntraPredMode |= (cb->PredMode==MODE_INTRA && cb->PartMode==PART_NxN   && TrafoDepth==1);

  if (selectIntraPredMode) {
    float minCost = std::numeric_limits<float>::max();
    int   minCostIdx=0;
    float minCandCost;

    const seq_parameter_set* sps = &ectx->get_sps();
    enum IntraPredMode candidates[3];
    fillIntraPredModeCandidates(candidates, tb->x,tb->y,
                                tb->x>0, tb->y>0, ectx->ctbs, &ectx->get_sps());



    std::vector< std::pair<enum IntraPredMode,float> > distortions;

    int log2TbSize = tb->log2Size;
    tb->intra_prediction[0] = std::make_shared<small_image_buffer>(log2TbSize, sizeof(uint8_t));

    for (int idx=0;idx<35;idx++)
      if (idx!=candidates[0] && idx!=candidates[1] && idx!=candidates[2] &&
          isPredModeEnabled((enum IntraPredMode)idx))
        {
          enum IntraPredMode mode = (enum IntraPredMode)idx;

          tb->intra_mode = mode;
          decode_intra_prediction_from_tree(ectx->img, tb, ectx->ctbs, ectx->get_sps(), 0);

          float distortion;
          distortion = estim_TB_bitrate(ectx, input, tb,
                                        mParams.bitrateEstimMethod());

          distortions.push_back( std::make_pair((enum IntraPredMode)idx, distortion) );
        }

    std::sort( distortions.begin(), distortions.end(), sortDistortions );


    for (int i=0;i<distortions.size();i++)
      {
        //printf("%d -> %f\n",i,distortions[i].second);
      }

    int keepNBest=std::min((int)mParams.keepNBest, (int)distortions.size());
    distortions.resize(keepNBest);
    distortions.push_back(std::make_pair((enum IntraPredMode)candidates[0],0));
    distortions.push_back(std::make_pair((enum IntraPredMode)candidates[1],0));
    distortions.push_back(std::make_pair((enum IntraPredMode)candidates[2],0));


    CodingOptions<enc_tb> options(ectx, tb, ctxModel);
    std::vector<CodingOption<enc_tb> >  option;

    for (size_t i=0;i<distortions.size();i++) {
      enum IntraPredMode intraMode  = (IntraPredMode)distortions[i].first;
      if (!isPredModeEnabled(intraMode)) { continue; }

      CodingOption<enc_tb> opt = options.new_option(isPredModeEnabled(intraMode));
      opt.get_node()->intra_mode = intraMode;
      option.push_back(opt);
    }

    options.start();


    for (int i=0;i<option.size();i++) {

      enc_tb* opt_tb = option[i].get_node();

      *opt_tb->downPtr = opt_tb;

      // set chroma mode to same mode is its luma mode
      enum IntraPredMode intraModeC;
      if (cb->PartMode==PART_2Nx2N || ectx->get_sps().ChromaArrayType==CHROMA_444) {
        intraModeC = opt_tb->intra_mode;
      }
      else {
        intraModeC = opt_tb->parent->children[0]->intra_mode;
      }

      opt_tb->intra_mode_chroma = intraModeC;

      option[i].begin();

      descend(opt_tb,"%d",opt_tb->intra_mode);
      opt_tb = mTBSplitAlgo->analyze(ectx,option[i].get_context(),input,opt_tb,
                                     TrafoDepth, MaxTrafoDepth, IntraSplitFlag);
      option[i].set_node(opt_tb);
      ascend();


      float intraPredModeBits = get_intra_pred_mode_bits(candidates,
                                                         opt_tb->intra_mode,
                                                         intraModeC,
                                                         option[i].get_context(),
                                                         tb->blkIdx == 0);

      opt_tb->rate_withoutCbfChroma += intraPredModeBits;
      opt_tb->rate += intraPredModeBits;

      option[i].end();
    }


    options.compute_rdo_costs();

    return options.return_best_rdo_node();
  }
  else {
    descend(tb,"NOP");
    enc_tb* new_tb = mTBSplitAlgo->analyze(ectx, ctxModel, input, tb,
                                           TrafoDepth, MaxTrafoDepth, IntraSplitFlag);
    ascend();
    return new_tb;
  }

  assert(false);
  return nullptr;
}
