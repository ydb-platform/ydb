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


#include "libde265/encoder/algo/cb-intra-inter.h"
#include "libde265/encoder/algo/coding-options.h"
#include "libde265/encoder/encoder-context.h"
#include <assert.h>
#include <limits>
#include <math.h>



enc_cb* Algo_CB_IntraInter_BruteForce::analyze(encoder_context* ectx,
                                               context_model_table& ctxModel,
                                               enc_cb* cb)
{
  assert(cb->pcm_flag==0);

  bool try_intra = true;
  bool try_inter = (ectx->shdr->slice_type != SLICE_TYPE_I);

  bool debug_halt = try_inter;
  try_inter = false;
  //try_intra = !try_inter; // TODO HACK: no intra in inter frames

  if (ectx->imgdata->frame_number > 0) {
    //printf("%d\n",ectx->imgdata->frame_number);
  }

  // 0: intra
  // 1: inter

  CodingOptions<enc_cb> options(ectx,cb,ctxModel);

  CodingOption<enc_cb> option_intra = options.new_option(try_intra);
  CodingOption<enc_cb> option_inter = options.new_option(try_inter);

  options.start();

  enc_cb* cb_inter = NULL;
  enc_cb* cb_intra = NULL;

  const int log2CbSize = cb->log2Size;
  const int x = cb->x;
  const int y = cb->y;


  // try encoding with inter

  if (option_inter) {
    option_inter.begin();
    cb_inter = option_inter.get_node();

    cb_inter->PredMode = MODE_INTER;
    ectx->img->set_pred_mode(x,y, log2CbSize, MODE_INTER);

    enc_cb* cb_result;
    descend(cb,"inter");
    cb_result=mInterAlgo->analyze(ectx, option_inter.get_context(), cb_inter);
    ascend();

    if (cb_result->PredMode != MODE_SKIP) {
      CABAC_encoder_estim* cabac = option_inter.get_cabac();
      cabac->reset();

      cabac->write_CABAC_bit(CONTEXT_MODEL_PRED_MODE_FLAG, 0); // 0 - inter
      float rate_pred_mode_flag = cabac->getRDBits();
      //printf("inter bits: %f\n", rate_pred_mode_flag);

      cb_result->rate += rate_pred_mode_flag;
    }

    option_inter.set_node(cb_result);

    option_inter.end();
  }


  // try intra

  if (option_intra) {
    option_intra.begin();
    cb_intra = option_intra.get_node();

    cb_intra->PredMode = MODE_INTRA;
    ectx->img->set_pred_mode(x,y, log2CbSize, MODE_INTRA);

    enc_cb* cb_result;
    descend(cb,"intra");
    cb_result=mIntraAlgo->analyze(ectx, option_intra.get_context(), cb_intra);
    ascend();

    if (ectx->shdr->slice_type != SLICE_TYPE_I) {
      CABAC_encoder_estim* cabac = option_intra.get_cabac();
      cabac->reset();

      cabac->write_CABAC_bit(CONTEXT_MODEL_PRED_MODE_FLAG, 1); // 1 - intra
      float rate_pred_mode_flag = cabac->getRDBits();
      //printf("intra bits: %f\n", rate_pred_mode_flag);

      cb_result->rate += rate_pred_mode_flag;
    }

    option_intra.set_node(cb_result);

    option_intra.end();
  }


  options.compute_rdo_costs();
  return options.return_best_rdo_node();
}
