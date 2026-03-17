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


#include "libde265/encoder/encoder-core.h"
#include "libde265/encoder/encoder-context.h"
#include "libde265/encoder/encoder-syntax.h"
#include "libde265/encoder/encoder-intrapred.h"
#include "libde265/encoder/algo/coding-options.h"
#include <assert.h>
#include <limits>
#include <math.h>
#include <iostream>


struct Logging_TB_Split : public Logging
{
  int skipTBSplit, noskipTBSplit;
  int zeroBlockCorrelation[6][2][5];

  const char* name() const { return "tb-split"; }

  void print(const encoder_context* ectx, const char* filename)
  {
    for (int tb=3;tb<=5;tb++) {
      for (int z=0;z<=1;z++) {
        float total = 0;

        for (int c=0;c<5;c++)
          total += zeroBlockCorrelation[tb][z][c];

        for (int c=0;c<5;c++) {
          printf("%d %d %d : %d %5.2f\n", tb,z,c,
                 zeroBlockCorrelation[tb][z][c],
                 total==0 ? 0 : zeroBlockCorrelation[tb][z][c]/total*100);
        }
      }
    }


    for (int z=0;z<2;z++) {
      printf("\n");
      for (int tb=3;tb<=5;tb++) {
        float total = 0;

        for (int c=0;c<5;c++)
          total += zeroBlockCorrelation[tb][z][c];

        printf("%dx%d ",1<<tb,1<<tb);

        for (int c=0;c<5;c++) {
          printf("%5.2f ", total==0 ? 0 : zeroBlockCorrelation[tb][z][c]/total*100);
        }
        printf("\n");
      }
    }
  }
} logging_tb_split;


template <class pixel_t>
void diff_blk(int16_t* out,int out_stride,
              const pixel_t* a_ptr, int a_stride,
              const pixel_t* b_ptr, int b_stride,
              int blkSize)
{
  for (int by=0;by<blkSize;by++)
    for (int bx=0;bx<blkSize;bx++)
      {
        out[by*out_stride+bx] = a_ptr[by*a_stride+bx] - b_ptr[by*b_stride+bx];
      }
}


template <class pixel_t>
void compute_residual_channel(encoder_context* ectx, enc_tb* tb, const de265_image* input,
                              int cIdx, int x,int y,int log2Size)
{
  int blkSize = (1<<log2Size);

  enum IntraPredMode mode;

  if (cIdx==0) {
    mode = tb->intra_mode;
  }
  else {
    mode = tb->intra_mode_chroma;
  }

  // decode intra prediction

  tb->intra_prediction[cIdx] = std::make_shared<small_image_buffer>(log2Size, sizeof(pixel_t));

  decode_intra_prediction_from_tree(ectx->img, tb, ectx->ctbs, ectx->get_sps(), cIdx);

  // create residual buffer and compute differences

  tb->residual[cIdx] = std::make_shared<small_image_buffer>(log2Size, sizeof(int16_t));

  diff_blk<pixel_t>(tb->residual[cIdx]->get_buffer_s16(), blkSize,
                    input->get_image_plane_at_pos(cIdx,x,y),
                    input->get_image_stride(cIdx),
                    tb->intra_prediction[cIdx]->get_buffer<pixel_t>(), blkSize,
                    blkSize);
}


template <class pixel_t>
void compute_residual(encoder_context* ectx, enc_tb* tb, const de265_image* input, int blkIdx)
{
  int tbSize = 1<<tb->log2Size;

  /*
  tb->writeSurroundingMetadata(ectx, ectx->img,
                               enc_node::METADATA_RECONSTRUCTION_BORDERS,
                               tb->get_rectangle_with_width(1<<(tb->log2Size+1)));
  */

  compute_residual_channel<pixel_t>(ectx,tb,input, 0,tb->x,tb->y,tb->log2Size);

  if (ectx->get_sps().chroma_format_idc == CHROMA_444) {
    compute_residual_channel<pixel_t>(ectx,tb,input, 1,tb->x,tb->y,tb->log2Size);
    compute_residual_channel<pixel_t>(ectx,tb,input, 2,tb->x,tb->y,tb->log2Size);
  }
  else if (tb->log2Size > 2) {
    int x = tb->x / input->SubWidthC;
    int y = tb->y / input->SubHeightC;
    int log2BlkSize = tb->log2Size -1;  // TODO chroma 422/444

    compute_residual_channel<pixel_t>(ectx,tb,input, 1,x,y,log2BlkSize);
    compute_residual_channel<pixel_t>(ectx,tb,input, 2,x,y,log2BlkSize);
  }
  else if (blkIdx==3) {
    int x = tb->parent->x / input->SubWidthC;
    int y = tb->parent->y / input->SubHeightC;
    int log2BlkSize = tb->log2Size;

    compute_residual_channel<pixel_t>(ectx,tb,input, 1,x,y,log2BlkSize);
    compute_residual_channel<pixel_t>(ectx,tb,input, 2,x,y,log2BlkSize);
  }
}


enc_tb*
Algo_TB_Split_BruteForce::analyze(encoder_context* ectx,
                                  context_model_table& ctxModel,
                                  const de265_image* input,
                                  enc_tb* tb,
                                  int TrafoDepth, int MaxTrafoDepth, int IntraSplitFlag)
{
  enter();

  enc_cb* cb = tb->cb;

  int log2TbSize = tb->log2Size;

  bool test_split = (log2TbSize > 2 &&
                     TrafoDepth < MaxTrafoDepth &&
                     log2TbSize > ectx->get_sps().Log2MinTrafoSize);

  bool test_no_split = true;
  if (IntraSplitFlag && TrafoDepth==0) test_no_split=false; // we have to split
  if (log2TbSize > ectx->get_sps().Log2MaxTrafoSize) test_no_split=false;

  assert(test_no_split || test_split);

  CodingOptions<enc_tb> options(ectx, tb, ctxModel);

  CodingOption<enc_tb> option_no_split = options.new_option(test_no_split);
  CodingOption<enc_tb> option_split    = options.new_option(test_split);

  //if (test_no_split) test_split = false;
  //if (test_split) test_no_split = false;   // HACK for debugging

  options.start();


  enc_tb* tb_no_split = NULL;
  enc_tb* tb_split    = NULL;

  if (test_no_split) {
    descend(tb,"no split");
    option_no_split.begin();
    tb_no_split = option_no_split.get_node();
    //tb_no_split = new enc_tb(*tb);
    *tb->downPtr = tb_no_split;

    if (cb->PredMode == MODE_INTRA) {
      compute_residual<uint8_t>(ectx, tb_no_split, input, tb->blkIdx);
    }

    tb_no_split = mAlgo_TB_Residual->analyze(ectx, option_no_split.get_context(),
                                             input, tb_no_split, TrafoDepth,MaxTrafoDepth,IntraSplitFlag);
    ascend(tb_no_split,"bits:%f/%f",tb_no_split->rate,tb_no_split->rate_withoutCbfChroma);


    option_no_split.set_node(tb_no_split);
    option_no_split.end();


    // --- some statistics ---

    if (log2TbSize <= mParams.zeroBlockPrune()) {
      bool zeroBlock = tb_no_split->isZeroBlock();

      if (zeroBlock) {
        test_split = false;
        logging_tb_split.skipTBSplit++;
      }
      else
        logging_tb_split.noskipTBSplit++;
    }
  }


  if (test_split) {
    option_split.begin();

    tb_split = option_split.get_node();
    *tb->downPtr = tb_split;

    //descend(tb,"split");
    tb_split = encode_transform_tree_split(ectx, option_split.get_context(), input, tb_split, cb,
                                           TrafoDepth, MaxTrafoDepth, IntraSplitFlag);
    option_split.set_node(tb_split);
    //ascend("bits:%f/%f",tb_split->rate,tb_split->rate_withoutCbfChroma);

    option_split.end();
  }


  // --- do some statistics that will help us develop a fast algorithm ---

  if (test_split && test_no_split) {
    bool zero_block = tb_no_split->isZeroBlock();

    int nChildZero = 0;
    for (int i=0;i<4;i++) {
      if (tb_split->children[i]->isZeroBlock()) nChildZero++;
    }

    logging_tb_split.zeroBlockCorrelation[log2TbSize][zero_block ? 0 : 1][nChildZero]++;
  }


  //bool split = (rd_cost_split < rd_cost_no_split);

  //if (test_split) split=true;  /// DEBUGGING HACK

  options.compute_rdo_costs();

  enc_tb* bestTB = options.return_best_rdo_node();
  return bestTB;
}



enc_tb* Algo_TB_Split::encode_transform_tree_split(encoder_context* ectx,
                                                   context_model_table& ctxModel,
                                                   const de265_image* input,
                                                   enc_tb* tb,
                                                   enc_cb* cb,
                                                   int TrafoDepth, int MaxTrafoDepth,
                                                   int IntraSplitFlag)
{
  const de265_image* img = ectx->img;

  int log2TbSize = tb->log2Size;
  int x0 = tb->x;
  int y0 = tb->y;

  context_model ctxModelCbfChroma[4];
  for (int i=0;i<4;i++) {
    ctxModelCbfChroma[i] = ctxModel[CONTEXT_MODEL_CBF_CHROMA+i];
  }

  tb->split_transform_flag = true;
  tb->rate_withoutCbfChroma = 0;
  tb->distortion = 0;

  // --- encode all child nodes ---

  for (int i=0;i<4;i++) {
    tb->children[i] = NULL;
  }

  for (int i=0;i<4;i++) {

    // generate child node and propagate values down

    int dx = (i&1)  << (log2TbSize-1);
    int dy = (i>>1) << (log2TbSize-1);

    enc_tb* child_tb = new enc_tb(x0+dx,y0+dy, log2TbSize-1,cb);

    child_tb->intra_mode        = tb->intra_mode;
    child_tb->intra_mode_chroma = tb->intra_mode_chroma;
    child_tb->TrafoDepth = tb->TrafoDepth + 1;
    child_tb->parent = tb;
    child_tb->blkIdx = i;
    child_tb->downPtr = &tb->children[i];

    descend(tb,"split %d/4",i+1);

    if (cb->PredMode == MODE_INTRA) {
      //descend(tb,"intra");
      tb->children[i] = mAlgo_TB_IntraPredMode->analyze(ectx, ctxModel, input,
                                                        child_tb,
                                                        TrafoDepth+1, MaxTrafoDepth,
                                                        IntraSplitFlag);
      //ascend("bits:%f",tb->rate);
    }
    else {
      //descend(tb,"inter");
      tb->children[i] = this->analyze(ectx, ctxModel, input,
                                      child_tb, TrafoDepth+1, MaxTrafoDepth, IntraSplitFlag);
      //ascend();
    }

    ascend();

    tb->distortion            += tb->children[i]->distortion;
    tb->rate_withoutCbfChroma += tb->children[i]->rate_withoutCbfChroma;
  }

  tb->set_cbf_flags_from_children();


  // --- add rate for this TB level ---

  CABAC_encoder_estim estim;
  estim.set_context_models(&ctxModel);




  const seq_parameter_set* sps = &ectx->img->get_sps();

  if (log2TbSize <= sps->Log2MaxTrafoSize &&
      log2TbSize >  sps->Log2MinTrafoSize &&
      TrafoDepth < MaxTrafoDepth &&
      !(IntraSplitFlag && TrafoDepth==0))
    {
      encode_split_transform_flag(ectx, &estim, log2TbSize, 1);
      tb->rate_withoutCbfChroma += estim.getRDBits();
      estim.reset();
    }

  // restore chroma CBF context models

  for (int i=0;i<4;i++) {
    ctxModel[CONTEXT_MODEL_CBF_CHROMA+i] = ctxModelCbfChroma[i];
  }

  tb->rate = (tb->rate_withoutCbfChroma +
              recursive_cbfChroma_rate(&estim,tb, log2TbSize, TrafoDepth));

  return tb;
}
