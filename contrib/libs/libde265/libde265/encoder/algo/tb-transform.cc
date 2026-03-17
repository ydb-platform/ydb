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


#include "libde265/encoder/algo/tb-transform.h"
#include "libde265/encoder/encoder-core.h"
#include "libde265/encoder/encoder-context.h"
#include "libde265/encoder/encoder-syntax.h"
#include <assert.h>
#include <limits>
#include <math.h>
#include <iostream>


// DEPRECATED IN THIS FILE
void diff_blk(int16_t* out,int out_stride,
              const uint8_t* a_ptr, int a_stride,
              const uint8_t* b_ptr, int b_stride,
              int blkSize)
{
  for (int by=0;by<blkSize;by++)
    for (int bx=0;bx<blkSize;bx++)
      {
        out[by*out_stride+bx] = a_ptr[by*a_stride+bx] - b_ptr[by*b_stride+bx];
      }
}


static bool has_nonzero_value(const int16_t* data, int n)
{
  for (int i=0;i<n;i++)
    if (data[i]) return true;

  return false;
}


void compute_transform_coeffs(encoder_context* ectx,
                              enc_tb* tb,
                              const de265_image* input, // TODO: probably pass pixels/stride directly
                              //int16_t* residual, int stride,
                              int x0,int y0, // luma position
                              int log2TbSize, // chroma adapted
                              const enc_cb* cb,
                              int cIdx)
{
  int xC = x0;
  int yC = y0;
  int tbSize = 1<<log2TbSize;
  if (cIdx>0) { xC>>=1; yC>>=1; }

  enum PredMode predMode = cb->PredMode;

  int16_t blk[32*32]; // residual
  int16_t* residual;


  //printf("transform-coeffs %d;%d size:%d cIdx:%d\n", tb->x,tb->y,1<<tb->log2Size,cIdx);

  // --- do intra prediction ---

  if (predMode==MODE_INTRA) {
    residual = tb->residual[cIdx]->get_buffer_s16();

    //printf("intra residual: %p stride: %d\n",residual, tb->residual[cIdx]->get_stride());
  }
  else {
    // --- subtract prediction from input image ---

    /* TMP REMOVE: ectx->prediction does not exist anymore

    uint8_t* pred = ectx->prediction->get_image_plane(cIdx);
    int stride = ectx->prediction->get_image_stride(cIdx);

    //printBlk("input",input->get_image_plane_at_pos(cIdx,xC,yC), tbSize, input->get_image_stride(cIdx));
    //printBlk("prediction", pred,tbSize, stride);

    diff_blk(blk,tbSize,
             input->get_image_plane_at_pos(cIdx,xC,yC), input->get_image_stride(cIdx),
             &pred[yC*stride+xC],stride, tbSize);

    residual=blk;
    */

    //printBlk("residual", blk,tbSize,tbSize);
  }





  // --- forward transform ---

  tb->alloc_coeff_memory(cIdx, tbSize);


  // transformation mode (DST or DCT)

  int trType;
  if (cIdx==0 && log2TbSize==2 && predMode==MODE_INTRA) trType=1; // TODO: inter mode
  else trType=0;


  // do forward transform

  fwd_transform(&ectx->acceleration, tb->coeff[cIdx], tbSize, log2TbSize, trType,  residual, tbSize);


  // --- quantization ---

  quant_coefficients(tb->coeff[cIdx], tb->coeff[cIdx], log2TbSize,  cb->qp, true);


  // set CBF to 0 if there are no non-zero coefficients

  tb->cbf[cIdx] = has_nonzero_value(tb->coeff[cIdx], 1<<(log2TbSize<<1));
}


enc_tb* Algo_TB_Transform::analyze(encoder_context* ectx,
                                   context_model_table& ctxModel,
                                   const de265_image* input,
                                   enc_tb* tb,
                                   int trafoDepth, int MaxTrafoDepth,
                                   int IntraSplitFlag)
{
  enter();

  const enc_cb* cb = tb->cb;
  *tb->downPtr = tb; // TODO: should be obsolet

  de265_image* img = ectx->img;

  int stride = ectx->img->get_image_stride(0);

  uint8_t* luma_plane = ectx->img->get_image_plane(0);
  uint8_t* cb_plane = ectx->img->get_image_plane(1);
  uint8_t* cr_plane = ectx->img->get_image_plane(2);

  // --- compute transform coefficients ---

  int x0 = tb->x;
  int y0 = tb->y;
  int xBase = cb->x;
  int yBase = cb->y;
  int log2TbSize = tb->log2Size;

  // luma block

  compute_transform_coeffs(ectx, tb, input, x0,y0, log2TbSize, cb, 0 /* Y */);


  // chroma blocks

  if (ectx->get_sps().chroma_format_idc == CHROMA_444) {
    compute_transform_coeffs(ectx, tb, input, x0,y0, log2TbSize, cb, 1 /* Cb */);
    compute_transform_coeffs(ectx, tb, input, x0,y0, log2TbSize, cb, 2 /* Cr */);
  }
  else if (log2TbSize > 2) {
    // if TB is > 4x4, do chroma transform of half size
    compute_transform_coeffs(ectx, tb, input, x0,y0, log2TbSize-1, cb, 1 /* Cb */);
    compute_transform_coeffs(ectx, tb, input, x0,y0, log2TbSize-1, cb, 2 /* Cr */);
  }
  else if (tb->blkIdx==3) {
    // if TB size is 4x4, do chroma transform for last sub-block
    compute_transform_coeffs(ectx, tb, input, xBase,yBase, log2TbSize, cb, 1 /* Cb */);
    compute_transform_coeffs(ectx, tb, input, xBase,yBase, log2TbSize, cb, 2 /* Cr */);
  }


  // --- reconstruction ---

  /* We could compute the reconstruction lazy on first access. However, we currently
     use it right away for computing the distortion.
  */
  tb->reconstruct(ectx, ectx->img);


  // measure rate

  CABAC_encoder_estim estim;
  estim.set_context_models(&ctxModel);


  tb->rate_withoutCbfChroma = 0;

  const seq_parameter_set* sps = &ectx->img->get_sps();


  if (log2TbSize <= sps->Log2MaxTrafoSize &&
      log2TbSize >  sps->Log2MinTrafoSize &&
      trafoDepth < MaxTrafoDepth &&
      !(IntraSplitFlag && trafoDepth==0))
    {
      encode_split_transform_flag(ectx, &estim, log2TbSize, 0);
      tb->rate_withoutCbfChroma += estim.getRDBits();
      estim.reset();
    }

  // --- CBF CB/CR ---

  float luma_cbf_bits = 0;
  if (cb->PredMode == MODE_INTRA || trafoDepth != 0 ||
      tb->cbf[1] || tb->cbf[2]) {
    encode_cbf_luma(&estim, trafoDepth==0, tb->cbf[0]);
    luma_cbf_bits = estim.getRDBits();
  }

  descend(tb,"DCT");
  float bits = mAlgo_TB_RateEstimation->encode_transform_unit(ectx,ctxModel,
                                                              tb,cb, x0,y0, xBase,yBase,
                                                              log2TbSize, trafoDepth, tb->blkIdx);
  ascend();

  tb->rate_withoutCbfChroma += bits + luma_cbf_bits;

  estim.reset(); // TODO: not needed ?

  tb->rate = (tb->rate_withoutCbfChroma +
              recursive_cbfChroma_rate(&estim,tb,log2TbSize,trafoDepth));

  //float rate_cbfChroma = estim.getRDBits();
  //tb->rate = tb->rate_withoutCbfChroma + rate_cbfChroma;


  // measure distortion

  int tbSize = 1<<log2TbSize;
  tb->distortion = SSD(input->get_image_plane_at_pos(0, x0,y0), input->get_image_stride(0),
                       tb->reconstruction[0]->get_buffer_u8(),
                       tb->reconstruction[0]->getStride(),
                       tbSize, tbSize);

  return tb;
}
