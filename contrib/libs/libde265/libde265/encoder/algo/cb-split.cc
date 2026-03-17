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


#include "libde265/encoder/algo/cb-split.h"
#include "libde265/encoder/algo/coding-options.h"
#include "libde265/encoder/encoder-context.h"
#include "libde265/encoder/encoder-syntax.h"
#include <assert.h>
#include <limits>
#include <math.h>
#include <iostream>


// Utility function to encode all four children in a split CB.
// Children are coded with the specified algo_cb_split.
enc_cb* Algo_CB_Split::encode_cb_split(encoder_context* ectx,
                                       context_model_table& ctxModel,
                                       enc_cb* cb)
{
  int w = ectx->imgdata->input->get_width();
  int h = ectx->imgdata->input->get_height();


  cb->split_cu_flag = true;


  // encode all 4 children and sum their distortions and rates

  for (int i=0;i<4;i++) {
    cb->children[i] = NULL;
  }

  for (int i=0;i<4;i++) {
    int child_x = cb->x + ((i&1)  << (cb->log2Size-1));
    int child_y = cb->y + ((i>>1) << (cb->log2Size-1));

    if (child_x>=w || child_y>=h) {
      // NOP
    }
    else {
      enc_cb* childCB = new enc_cb;
      childCB->log2Size = cb->log2Size-1;
      childCB->ctDepth  = cb->ctDepth+1;

      childCB->x = child_x;
      childCB->y = child_y;
      childCB->parent  = cb;
      childCB->downPtr = &cb->children[i];

      descend(cb,"yes %d/4",i+1);
      cb->children[i] = analyze(ectx, ctxModel, childCB);
      ascend();

      cb->distortion += cb->children[i]->distortion;
      cb->rate       += cb->children[i]->rate;
    }
  }

  return cb;
}




enc_cb* Algo_CB_Split_BruteForce::analyze(encoder_context* ectx,
                                          context_model_table& ctxModel,
                                          enc_cb* cb_input)
{
  assert(cb_input->pcm_flag==0);

  // --- prepare coding options ---

  const SplitType split_type = get_split_type(&ectx->get_sps(),
                                              cb_input->x, cb_input->y,
                                              cb_input->log2Size);


  bool can_split_CB   = (split_type != ForcedNonSplit);
  bool can_nosplit_CB = (split_type != ForcedSplit);

  //if (can_split_CB) { can_nosplit_CB=false; } // TODO TMP
  //if (can_nosplit_CB) { can_split_CB=false; } // TODO TMP

  CodingOptions<enc_cb> options(ectx, cb_input, ctxModel);

  CodingOption<enc_cb> option_no_split = options.new_option(can_nosplit_CB);
  CodingOption<enc_cb> option_split    = options.new_option(can_split_CB);

  options.start();

  /*
  cb_input->writeSurroundingMetadata(ectx, ectx->img,
                                     enc_node::METADATA_CT_DEPTH, // for estimation cb-split bits
                                     cb_input->get_rectangle());
  */

  // --- encode without splitting ---

  if (option_no_split) {
    CodingOption<enc_cb>& opt = option_no_split; // abbrev.

    opt.begin();

    enc_cb* cb = opt.get_node();
    *cb_input->downPtr = cb;

    // set CB size in image data-structure
    //ectx->img->set_ctDepth(cb->x,cb->y,cb->log2Size, cb->ctDepth);
    //ectx->img->set_log2CbSize(cb->x,cb->y,cb->log2Size, true);

    /* We set QP here, because this is required at in non-split CBs only.
     */
    cb->qp = ectx->active_qp;

    // analyze subtree
    assert(mChildAlgo);

    descend(cb,"no");
    cb = mChildAlgo->analyze(ectx, opt.get_context(), cb);
    ascend();

    // add rate for split flag
    if (split_type == OptionalSplit) {
      encode_split_cu_flag(ectx,opt.get_cabac(), cb->x,cb->y, cb->ctDepth, 0);
      cb->rate += opt.get_cabac_rate();
    }

    opt.set_node(cb);
    opt.end();
  }

  // --- encode with splitting ---

  if (option_split) {
    option_split.begin();

    enc_cb* cb = option_split.get_node();
    *cb_input->downPtr = cb;

    cb = encode_cb_split(ectx, option_split.get_context(), cb);

    // add rate for split flag
    if (split_type == OptionalSplit) {
      encode_split_cu_flag(ectx,option_split.get_cabac(), cb->x,cb->y, cb->ctDepth, 1);
      cb->rate += option_split.get_cabac_rate();
    }

    option_split.set_node(cb);
    option_split.end();
  }

  options.compute_rdo_costs();
  enc_cb* bestCB = options.return_best_rdo_node();

  //bestCB->debug_assertTreeConsistency(ectx->img);

  return bestCB;
}
