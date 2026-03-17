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


#include "libde265/encoder/algo/cb-mergeindex.h"
#include "libde265/encoder/encoder-context.h"
#include "libde265/encoder/encoder-syntax.h"
#include "libde265/encoder/encoder-motion.h"
#include <assert.h>
#include <limits>
#include <math.h>




enc_cb* Algo_CB_MergeIndex_Fixed::analyze(encoder_context* ectx,
                                          context_model_table& ctxModel,
                                          enc_cb* cb)
{
  assert(cb->split_cu_flag==false);
  assert(cb->PredMode==MODE_SKIP); // TODO: || (cb->PredMode==MODE_INTER && cb->inter.skip_flag));


  PBMotion mergeCandList[5];

  int partIdx = 0;

  int cbSize = 1 << cb->log2Size;

  get_merge_candidate_list_from_tree(ectx, ectx->shdr,
                                     cb->x, cb->y, // xC/yC
                                     cb->x, cb->y, // xP/yP
                                     cbSize, // nCS
                                     cbSize,cbSize, // nPbW/nPbH
                                     partIdx, // partIdx
                                     mergeCandList);

  PBMotionCoding&   spec = cb->inter.pb[partIdx].spec;

  spec.merge_flag = 1; // we use merge mode
  spec.merge_idx  = 0;


  // build prediction

  // previous frame (TODO)
  const de265_image* refimg = ectx->get_image(ectx->imgdata->frame_number -1);

  //printf("prev frame: %p %d\n",refimg,ectx->imgdata->frame_number);

  /*
    printf("#l0: %d\n",ectx->imgdata->shdr.num_ref_idx_l0_active);
    printf("#l1: %d\n",ectx->imgdata->shdr.num_ref_idx_l1_active);

    for (int i=0;i<ectx->imgdata->shdr.num_ref_idx_l0_active;i++)
    printf("RefPixList[0][%d] = %d\n", i, ectx->imgdata->shdr.RefPicList[0][i]);
  */

  // TODO: fake motion data

  const PBMotion& vec = mergeCandList[spec.merge_idx];
  cb->inter.pb[partIdx].motion = vec;

  //ectx->img->set_mv_info(cb->x,cb->y, 1<<cb->log2Size,1<<cb->log2Size, vec);

  /*
  generate_inter_prediction_samples(ectx, ectx->shdr, ectx->prediction,
                                    cb->x,cb->y, // int xC,int yC,
                                    0,0,         // int xB,int yB,
                                    1<<cb->log2Size, // int nCS,
                                    1<<cb->log2Size,
                                    1<<cb->log2Size, // int nPbW,int nPbH,
                                    &vec);
  */

  generate_inter_prediction_samples(ectx, ectx->shdr, ectx->img,
                                    cb->x,cb->y, // int xC,int yC,
                                    0,0,         // int xB,int yB,
                                    1<<cb->log2Size, // int nCS,
                                    1<<cb->log2Size,
                                    1<<cb->log2Size, // int nPbW,int nPbH,
                                    &vec);

  /*
  printBlk("merge prediction:",
           ectx->img->get_image_plane_at_pos(0, cb->x,cb->y), 1<<cb->log2Size,
           ectx->img->get_image_stride(0),
           "merge ");
  */

  // estimate rate for sending merge index

  //CABAC_encoder_estim cabac;
  //cabac.write_bits();

  int IntraSplitFlag = 0;
  int MaxTrafoDepth = ectx->get_sps().max_transform_hierarchy_depth_inter;

  if (mCodeResidual) {
    assert(false);
    descend(cb,"with residual");
    assert(false);
    /* TODO
    cb->transform_tree = mTBSplit->analyze(ectx,ctxModel, ectx->imgdata->input, NULL, cb,
                                           cb->x,cb->y,cb->x,cb->y, cb->log2Size,0,
                                           0, MaxTrafoDepth, IntraSplitFlag);
    */
    ascend();

    cb->inter.rqt_root_cbf = ! cb->transform_tree->isZeroBlock();

    cb->distortion = cb->transform_tree->distortion;
    cb->rate       = cb->transform_tree->rate;
  }
  else {
    const de265_image* input = ectx->imgdata->input;
    //de265_image* img   = ectx->prediction;
    int x0 = cb->x;
    int y0 = cb->y;
    int tbSize = 1<<cb->log2Size;

    CABAC_encoder_estim cabac;
    cabac.set_context_models(&ctxModel);
    encode_merge_idx(ectx, &cabac, spec.merge_idx);

    leaf(cb,"no residual");

    cb->rate = cabac.getRDBits();

    cb->inter.rqt_root_cbf = 0;


    enc_tb* tb = new enc_tb(x0,y0,cb->log2Size,cb);
    tb->downPtr = &cb->transform_tree;
    cb->transform_tree = tb;

    tb->reconstruct(ectx, ectx->img); // reconstruct luma

    /*
    printBlk("distortion input:",
             input->get_image_plane_at_pos(0,x0,y0), 1<<cb->log2Size,
             input->get_image_stride(0),
             "input ");

    printBlk("distortion prediction:",
             ectx->img->get_image_plane_at_pos(0,x0,y0), 1<<cb->log2Size,
             ectx->img->get_image_stride(0),
             "pred ");
    */

    cb->distortion = compute_distortion_ssd(input, ectx->img, x0,y0, cb->log2Size, 0);
  }

  //printf("%d;%d rqt_root_cbf=%d\n",cb->x,cb->y,cb->inter.rqt_root_cbf);

  return cb;
}
