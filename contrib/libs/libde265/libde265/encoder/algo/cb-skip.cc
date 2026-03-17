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


#include "libde265/encoder/algo/cb-skip.h"
#include "libde265/encoder/algo/coding-options.h"
#include "libde265/encoder/encoder-syntax.h"
#include "libde265/encoder/encoder-context.h"
#include <assert.h>
#include <limits>
#include <math.h>




enc_cb* Algo_CB_Skip_BruteForce::analyze(encoder_context* ectx,
                                         context_model_table& ctxModel,
                                         enc_cb* cb)
{
  bool try_skip  = (ectx->shdr->slice_type != SLICE_TYPE_I);
  bool try_nonskip = true;

  //try_nonskip = !try_skip;

  CodingOptions<enc_cb> options(ectx,cb,ctxModel);
  CodingOption<enc_cb> option_skip    = options.new_option(try_skip);
  CodingOption<enc_cb> option_nonskip = options.new_option(try_nonskip);
  options.start();

  for (int i=0;i<CONTEXT_MODEL_TABLE_LENGTH;i++) {
    //printf("%i: %d/%d\n",i, ctxModel[i].state, ctxModel[i].MPSbit);
  }

  if (option_skip) {
    CodingOption<enc_cb>& opt = option_skip;
    opt.begin();

    enc_cb* cb = opt.get_node();

    // calc rate for skip flag (=true)

    CABAC_encoder_estim* cabac = opt.get_cabac();
    encode_cu_skip_flag(ectx, cabac, cb, true);
    float rate_pred_mode = cabac->getRDBits();
    cabac->reset();

    // set skip flag

    cb->PredMode = MODE_SKIP;
    ectx->img->set_pred_mode(cb->x,cb->y, cb->log2Size, cb->PredMode);

    // encode CB

    descend(cb,"yes");
    cb = mSkipAlgo->analyze(ectx, opt.get_context(), cb);
    ascend();

    // add rate for PredMode

    cb->rate += rate_pred_mode;
    opt.set_node(cb);
    opt.end();
  }

  if (option_nonskip) {
    CodingOption<enc_cb>& opt = option_nonskip;
    enc_cb* cb = opt.get_node();

    opt.begin();

    // calc rate for skip flag (=true)

    float rate_pred_mode = 0;

    if (try_skip) {
      CABAC_encoder_estim* cabac = opt.get_cabac();
      encode_cu_skip_flag(ectx, cabac, cb, false);
      rate_pred_mode = cabac->getRDBits();
      cabac->reset();
    }

    descend(cb,"no");
    cb = mNonSkipAlgo->analyze(ectx, opt.get_context(), cb);
    ascend();

    // add rate for PredMode

    cb->rate += rate_pred_mode;
    opt.set_node(cb);
    opt.end();
  }

  options.compute_rdo_costs();
  return options.return_best_rdo_node();
}
