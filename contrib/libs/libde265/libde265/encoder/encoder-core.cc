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
#include <assert.h>
#include <limits>
#include <math.h>
#include <iostream>
#include <fstream>


#define ENCODER_DEVELOPMENT 0
#define COMPARE_ESTIMATED_RATE_TO_REAL_RATE 0


static int IntraPredModeCnt[7][35];
static int MPM_used[7][35];

static int IntraPredModeCnt_total[35];
static int MPM_used_total[35];

/*
void statistics_IntraPredMode(const encoder_context* ectx, int x,int y, const enc_cb* cb)
{
  if (cb->split_cu_flag) {
    for (int i=0;i<4;i++)
      if (cb->children[i]) {
        statistics_IntraPredMode(ectx, childX(x,i,cb->log2Size), childY(y,i,cb->log2Size), cb->children[i]);
      }
  }
  else {
    int cnt;
    int size = cb->log2Size;

    if (cb->PartMode == PART_NxN) { cnt=4; size--; } else cnt=1;

    for (int i=0;i<cnt;i++) {
      IntraPredModeCnt[size][ cb->intra.pred_mode[i] ]++;
      IntraPredModeCnt_total[ cb->intra.pred_mode[i] ]++;

      int xi = childX(x,i,cb->log2Size);
      int yi = childY(y,i,cb->log2Size);

      enum IntraPredMode candModeList[3];
      fillIntraPredModeCandidates(candModeList,xi,yi, xi>0, yi>0, ectx->img);

      int predmode = cb->intra.pred_mode[i];
      if (candModeList[0]==predmode ||
          candModeList[1]==predmode ||
          candModeList[2]==predmode) {
        MPM_used[size][predmode]++;
        MPM_used_total[predmode]++;
      }
    }
  }
}
*/

void statistics_print()
{
  for (int i=0;i<35;i++) {
    printf("%d",i);
    printf("  %d %d",IntraPredModeCnt_total[i], MPM_used_total[i]);

    for (int k=2;k<=6;k++) {
      printf("  %d %d",IntraPredModeCnt[k][i], MPM_used[k][i]);
    }

    printf("\n");
  }
}


void print_tb_tree_rates(const enc_tb* tb, int level)
{
  for (int i=0;i<level;i++)
    std::cout << "  ";

  std::cout << "TB rate=" << tb->rate << " (" << tb->rate_withoutCbfChroma << ")\n";
  if (tb->split_transform_flag) {
    for (int i=0;i<4;i++)
      print_tb_tree_rates(tb->children[i], level+1);
  }
}


void print_cb_tree_rates(const enc_cb* cb, int level)
{
  for (int i=0;i<level;i++)
    std::cout << "  ";

  std::cout << "CB rate=" << cb->rate << "\n";
  if (cb->split_cu_flag) {
    for (int i=0;i<4;i++)
      print_cb_tree_rates(cb->children[i], level+1);
  }
  else {
    print_tb_tree_rates(cb->transform_tree, level+1);
  }
}


// /*LIBDE265_API*/ ImageSink_YUV reconstruction_sink;

double encode_image(encoder_context* ectx,
                    const de265_image* input,
                    EncoderCore& algo)
{
  int stride=input->get_image_stride(0);

  int w = ectx->get_sps().pic_width_in_luma_samples;
  int h = ectx->get_sps().pic_height_in_luma_samples;

  // --- create reconstruction image ---
  ectx->img = new de265_image;
  ectx->img->set_headers(ectx->get_shared_vps(), ectx->get_shared_sps(), ectx->get_shared_pps());
  ectx->img->PicOrderCntVal = input->PicOrderCntVal;

  ectx->img->alloc_image(w,h, input->get_chroma_format(), ectx->get_shared_sps(), true,
                         NULL /* no decctx */, /*ectx,*/ 0,NULL,false);
  //ectx->img->alloc_encoder_data(&ectx->sps);
  ectx->img->clear_metadata();

#if 0
  if (1) {
    ectx->prediction = new de265_image;
    ectx->prediction->alloc_image(w,h, input->get_chroma_format(), &ectx->sps, false /* no metadata */,
                                  NULL /* no decctx */, NULL /* no encctx */, 0,NULL,false);
    ectx->prediction->vps = ectx->vps;
    ectx->prediction->sps = ectx->sps;
    ectx->prediction->pps = ectx->pps;
  }
#endif

  ectx->active_qp = ectx->get_pps().pic_init_qp; // TODO take current qp from slice


  ectx->cabac_ctx_models.init(ectx->shdr->initType, ectx->shdr->SliceQPY);
  ectx->cabac_encoder.set_context_models(&ectx->cabac_ctx_models);


  context_model_table modelEstim;
  CABAC_encoder_estim cabacEstim;

  modelEstim.init(ectx->shdr->initType, ectx->shdr->SliceQPY);
  cabacEstim.set_context_models(&modelEstim);


  int Log2CtbSize = ectx->get_sps().Log2CtbSizeY;

  uint8_t* luma_plane = ectx->img->get_image_plane(0);
  uint8_t* cb_plane   = ectx->img->get_image_plane(1);
  uint8_t* cr_plane   = ectx->img->get_image_plane(2);

  double mse=0;


  // encode CTB by CTB

  ectx->ctbs.clear();

  for (int y=0;y<ectx->get_sps().PicHeightInCtbsY;y++)
    for (int x=0;x<ectx->get_sps().PicWidthInCtbsY;x++)
      {
        ectx->img->set_SliceAddrRS(x, y, ectx->shdr->SliceAddrRS);

        int x0 = x<<Log2CtbSize;
        int y0 = y<<Log2CtbSize;

        logtrace(LogSlice,"encode CTB at %d %d\n",x0,y0);

        // make a copy of the context model that we can modify for testing alternatives

        context_model_table ctxModel;
        //copy_context_model_table(ctxModel, ectx->ctx_model_bitstream);
        ctxModel = ectx->cabac_ctx_models.copy();
        ctxModel = modelEstim.copy(); // TODO TMP

        disable_logging(LogSymbols);
        enable_logging(LogSymbols);  // TODO TMP

        //printf("================================================== ANALYZE\n");

#if 1
        /*
          enc_cb* cb = encode_cb_may_split(ectx, ctxModel,
          input, x0,y0, Log2CtbSize, 0, qp);
        */

        enc_cb* cb = algo.getAlgoCTBQScale()->analyze(ectx,ctxModel, x0,y0);
#else
        float minCost = std::numeric_limits<float>::max();
        int bestQ = 0;
        int qp = ectx->params.constant_QP;

        enc_cb* cb;
        for (int q=1;q<51;q++) {
          copy_context_model_table(ctxModel, ectx->ctx_model_bitstream);

          enc_cb* cbq = encode_cb_may_split(ectx, ctxModel,
                                            input, x0,y0, Log2CtbSize, 0, q);

          float cost = cbq->distortion + ectx->lambda * cbq->rate;
          if (cost<minCost) { minCost=cost; bestQ=q; }

          if (q==qp) { cb=cbq; }
        }

        printf("Q %d\n",bestQ);
        fflush(stdout);
#endif

        //print_cb_tree_rates(cb,0);

        //statistics_IntraPredMode(ectx, x0,y0, cb);


        // --- write bitstream ---

        //ectx->switch_CABAC_to_bitstream();

        enable_logging(LogSymbols);

        logdebug(LogEncoder,"write CTB %d;%d\n",x,y);

        if (logdebug_enabled(LogEncoder)) {
          cb->debug_dumpTree(enc_tb::DUMPTREE_ALL);
        }

        /*
        cb->debug_assertTreeConsistency(ectx->img);

        //cb->invalidateMetadataInSubTree(ectx->img);
        cb->writeMetadata(ectx, ectx->img,
                          enc_node::METADATA_INTRA_MODES |
                          enc_node::METADATA_RECONSTRUCTION |
                          enc_node::METADATA_CT_DEPTH);

        cb->debug_assertTreeConsistency(ectx->img);
        */

        encode_ctb(ectx, &ectx->cabac_encoder, cb, x,y);

        //printf("================================================== WRITE\n");


        if (COMPARE_ESTIMATED_RATE_TO_REAL_RATE) {
          float realPre = cabacEstim.getRDBits();
          encode_ctb(ectx, &cabacEstim, cb, x,y);
          float realPost = cabacEstim.getRDBits();

          printf("estim: %f  real: %f  diff: %f\n",
                 cb->rate,
                 realPost-realPre,
                 cb->rate - (realPost-realPre));
        }


        int last = (y==ectx->get_sps().PicHeightInCtbsY-1 &&
                    x==ectx->get_sps().PicWidthInCtbsY-1);
        ectx->cabac_encoder.write_CABAC_term_bit(last);

        //delete cb;

        //ectx->free_all_pools();

        mse += cb->distortion;
      }

  mse /= ectx->img->get_width() * ectx->img->get_height();


  //reconstruction_sink.send_image(ectx->img);


  //statistics_print();


  //delete ectx->prediction;


  // frame PSNR

  ectx->ctbs.writeReconstructionToImage(ectx->img, &ectx->get_sps());

#if 0
  std::ofstream ostr("out.pgm");
  ostr << "P5\n" << ectx->img->get_width() << " " << ectx->img->get_height() << "\n255\n";
  for (int y=0;y<ectx->img->get_height();y++) {
    ostr.write( (char*)ectx->img->get_image_plane_at_pos(0,0,y), ectx->img->get_width() );
  }
#endif

  double psnr = 10*log10(255.0*255.0 / mse);

#if 0
  double psnr2 = PSNR(MSE(input->get_image_plane(0), input->get_image_stride(0),
                          luma_plane, ectx->img->get_image_stride(0),
                          input->get_width(), input->get_height()));

  printf("rate-estim PSNR: %f vs %f\n",psnr,psnr2);
#endif

  return psnr;
}



void EncoderCore_Custom::setParams(encoder_params& params)
{
  // build algorithm tree

  mAlgo_CTB_QScale_Constant.setChildAlgo(&mAlgo_CB_Split_BruteForce);
  mAlgo_CB_Split_BruteForce.setChildAlgo(&mAlgo_CB_Skip_BruteForce);

  mAlgo_CB_Skip_BruteForce.setSkipAlgo(&mAlgo_CB_MergeIndex_Fixed);
  mAlgo_CB_Skip_BruteForce.setNonSkipAlgo(&mAlgo_CB_IntraInter_BruteForce);
  //&mAlgo_CB_InterPartMode_Fixed);

  Algo_CB_IntraPartMode* algo_CB_IntraPartMode = NULL;
  switch (params.mAlgo_CB_IntraPartMode()) {
  case ALGO_CB_IntraPartMode_BruteForce:
    algo_CB_IntraPartMode = &mAlgo_CB_IntraPartMode_BruteForce;
    break;
  case ALGO_CB_IntraPartMode_Fixed:
    algo_CB_IntraPartMode = &mAlgo_CB_IntraPartMode_Fixed;
    break;
  }

  mAlgo_CB_IntraInter_BruteForce.setIntraChildAlgo(algo_CB_IntraPartMode);
  mAlgo_CB_IntraInter_BruteForce.setInterChildAlgo(&mAlgo_CB_InterPartMode_Fixed);

  mAlgo_CB_MergeIndex_Fixed.setChildAlgo(&mAlgo_TB_Split_BruteForce);

  Algo_PB_MV* pbAlgo = NULL;
  switch (params.mAlgo_MEMode()) {
  case MEMode_Test:
    pbAlgo = &mAlgo_PB_MV_Test;
    break;
  case MEMode_Search:
    pbAlgo = &mAlgo_PB_MV_Search;
    break;
  }

  mAlgo_CB_InterPartMode_Fixed.setChildAlgo(pbAlgo);
  pbAlgo->setChildAlgo(&mAlgo_TB_Split_BruteForce);


  Algo_TB_IntraPredMode_ModeSubset* algo_TB_IntraPredMode = NULL;
  switch (params.mAlgo_TB_IntraPredMode()) {
  case ALGO_TB_IntraPredMode_BruteForce:
    algo_TB_IntraPredMode = &mAlgo_TB_IntraPredMode_BruteForce;
    break;
  case ALGO_TB_IntraPredMode_FastBrute:
    algo_TB_IntraPredMode = &mAlgo_TB_IntraPredMode_FastBrute;
    break;
  case ALGO_TB_IntraPredMode_MinResidual:
    algo_TB_IntraPredMode = &mAlgo_TB_IntraPredMode_MinResidual;
    break;
  }

  algo_CB_IntraPartMode->setChildAlgo(algo_TB_IntraPredMode);

  mAlgo_TB_Split_BruteForce.setAlgo_TB_IntraPredMode(algo_TB_IntraPredMode);
  mAlgo_TB_Split_BruteForce.setAlgo_TB_Residual(&mAlgo_TB_Transform);

  Algo_TB_RateEstimation* algo_TB_RateEstimation = NULL;
  switch (params.mAlgo_TB_RateEstimation()) {
  case ALGO_TB_RateEstimation_None:  algo_TB_RateEstimation = &mAlgo_TB_RateEstimation_None;  break;
  case ALGO_TB_RateEstimation_Exact: algo_TB_RateEstimation = &mAlgo_TB_RateEstimation_Exact; break;
  }
  mAlgo_TB_Transform.setAlgo_TB_RateEstimation(algo_TB_RateEstimation);
  //mAlgo_TB_Split_BruteForce.setParams(params.TB_Split_BruteForce);

  algo_TB_IntraPredMode->setChildAlgo(&mAlgo_TB_Split_BruteForce);


  // ===== set algorithm parameters ======

  //mAlgo_CB_IntraPartMode_Fixed.setParams(params.CB_IntraPartMode_Fixed);

  //mAlgo_TB_IntraPredMode_FastBrute.setParams(params.TB_IntraPredMode_FastBrute);
  //mAlgo_TB_IntraPredMode_MinResidual.setParams(params.TB_IntraPredMode_MinResidual);


  //mAlgo_CTB_QScale_Constant.setParams(params.CTB_QScale_Constant);


  algo_TB_IntraPredMode->enableIntraPredModeSubset( params.mAlgo_TB_IntraPredMode_Subset() );
}


void Logging::print_logging(const encoder_context* ectx, const char* id, const char* filename)
{
#if 000
  if (strcmp(id,logging_tb_split.name())==0) {
    logging_tb_split.print(ectx,filename);
  }
#endif
}


void en265_print_logging(const encoder_context* ectx, const char* id, const char* filename)
{
  Logging::print_logging(ectx,id,filename);
}
