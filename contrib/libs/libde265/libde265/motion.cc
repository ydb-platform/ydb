/*
 * H.265 video codec.
 * Copyright (c) 2013-2014 struktur AG, Dirk Farin <farin@struktur.de>
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

#include "motion.h"
#include "decctx.h"
#include "util.h"
#include "dpb.h"

#include <assert.h>


#include <sys/types.h>
#include <signal.h>
#include <string.h>

#if defined(_MSC_VER) || defined(__MINGW32__)
# include <malloc.h>
#elif defined(HAVE_ALLOCA_H)
# include <alloca.h>
#endif


#define MAX_CU_SIZE 64


static int extra_before[4] = { 0,3,3,2 };
static int extra_after [4] = { 0,3,4,4 };



template <class pixel_t>
void mc_luma(const base_context* ctx,
             const seq_parameter_set* sps, int mv_x, int mv_y,
             int xP,int yP,
             int16_t* out, int out_stride,
             const pixel_t* ref, int ref_stride,
             int nPbW, int nPbH, int bitDepth_L)
{
  int xFracL = mv_x & 3;
  int yFracL = mv_y & 3;

  int xIntOffsL = xP + (mv_x>>2);
  int yIntOffsL = yP + (mv_y>>2);

  // luma sample interpolation process (8.5.3.2.2.1)

  //const int shift1 = sps->BitDepth_Y-8;
  //const int shift2 = 6;
  const int shift3 = 14 - sps->BitDepth_Y;

  int w = sps->pic_width_in_luma_samples;
  int h = sps->pic_height_in_luma_samples;

  ALIGNED_16(int16_t) mcbuffer[MAX_CU_SIZE * (MAX_CU_SIZE+7)];

  if (xFracL==0 && yFracL==0) {

    if (xIntOffsL >= 0 && yIntOffsL >= 0 &&
        nPbW+xIntOffsL <= w && nPbH+yIntOffsL <= h) {

      ctx->acceleration.put_hevc_qpel(out, out_stride,
                                      &ref[yIntOffsL*ref_stride + xIntOffsL],
                                      ref_stride /* sizeof(pixel_t)*/,
                                      nPbW,nPbH, mcbuffer, 0,0, bitDepth_L);
    }
    else {
      for (int y=0;y<nPbH;y++)
        for (int x=0;x<nPbW;x++) {

          int xA = Clip3(0,w-1,x + xIntOffsL);
          int yA = Clip3(0,h-1,y + yIntOffsL);

          out[y*out_stride+x] = ref[ xA + yA*ref_stride ] << shift3;
        }
    }

#ifdef DE265_LOG_TRACE
    logtrace(LogMotion,"---MC luma %d %d = direct---\n",xFracL,yFracL);

    for (int y=0;y<nPbH;y++) {
      for (int x=0;x<nPbW;x++) {

        int xA = Clip3(0,w-1,x + xIntOffsL);
        int yA = Clip3(0,h-1,y + yIntOffsL);

        logtrace(LogMotion,"%02x ", ref[ xA + yA*ref_stride ]);
      }
      logtrace(LogMotion,"\n");
    }

    logtrace(LogMotion," -> \n");

    for (int y=0;y<nPbH;y++) {
      for (int x=0;x<nPbW;x++) {

        logtrace(LogMotion,"%02x ",out[y*out_stride+x] >> 6); // 6 will be used when summing predictions
      }
      logtrace(LogMotion,"\n");
    }
#endif
  }
  else {
    int extra_left   = extra_before[xFracL];
    int extra_right  = extra_after [xFracL];
    int extra_top    = extra_before[yFracL];
    int extra_bottom = extra_after [yFracL];

    //int nPbW_extra = extra_left + nPbW + extra_right;
    //int nPbH_extra = extra_top  + nPbH + extra_bottom;


    pixel_t padbuf[(MAX_CU_SIZE+16)*(MAX_CU_SIZE+7)];

    const pixel_t* src_ptr;
    int src_stride;

    if (-extra_left + xIntOffsL >= 0 &&
        -extra_top  + yIntOffsL >= 0 &&
        nPbW+extra_right  + xIntOffsL < w &&
        nPbH+extra_bottom + yIntOffsL < h) {
      src_ptr = &ref[xIntOffsL + yIntOffsL*ref_stride];
      src_stride = ref_stride;
    }
    else {
      for (int y=-extra_top;y<nPbH+extra_bottom;y++) {
        for (int x=-extra_left;x<nPbW+extra_right;x++) {

          int xA = Clip3(0,w-1,x + xIntOffsL);
          int yA = Clip3(0,h-1,y + yIntOffsL);

          padbuf[x+extra_left + (y+extra_top)*(MAX_CU_SIZE+16)] = ref[ xA + yA*ref_stride ];
        }
      }

      src_ptr = &padbuf[extra_top*(MAX_CU_SIZE+16) + extra_left];
      src_stride = MAX_CU_SIZE+16;
    }

    ctx->acceleration.put_hevc_qpel(out, out_stride,
                                    src_ptr, src_stride /* sizeof(pixel_t) */,
                                    nPbW,nPbH, mcbuffer, xFracL,yFracL, bitDepth_L);


    logtrace(LogMotion,"---V---\n");
    for (int y=0;y<nPbH;y++) {
      for (int x=0;x<nPbW;x++) {
        logtrace(LogMotion,"%04x ",out[x+y*out_stride]);
      }
      logtrace(LogMotion,"\n");
    }
  }
}



template <class pixel_t>
void mc_chroma(const base_context* ctx,
               const seq_parameter_set* sps,
               int mv_x, int mv_y,
               int xP,int yP,
               int16_t* out, int out_stride,
               const pixel_t* ref, int ref_stride,
               int nPbWC, int nPbHC, int bit_depth_C)
{
  // chroma sample interpolation process (8.5.3.2.2.2)

  //const int shift1 = sps->BitDepth_C-8;
  //const int shift2 = 6;
  const int shift3 = 14 - sps->BitDepth_C;

  int wC = sps->pic_width_in_luma_samples /sps->SubWidthC;
  int hC = sps->pic_height_in_luma_samples/sps->SubHeightC;

  mv_x *= 2 / sps->SubWidthC;
  mv_y *= 2 / sps->SubHeightC;

  int xFracC = mv_x & 7;
  int yFracC = mv_y & 7;

  int xIntOffsC = xP/sps->SubWidthC  + (mv_x>>3);
  int yIntOffsC = yP/sps->SubHeightC + (mv_y>>3);

  ALIGNED_32(int16_t mcbuffer[MAX_CU_SIZE*(MAX_CU_SIZE+7)]);

  if (xFracC == 0 && yFracC == 0) {
    if (xIntOffsC>=0 && nPbWC+xIntOffsC<=wC &&
        yIntOffsC>=0 && nPbHC+yIntOffsC<=hC) {
      ctx->acceleration.put_hevc_epel(out, out_stride,
                                      &ref[xIntOffsC + yIntOffsC*ref_stride], ref_stride,
                                      nPbWC,nPbHC, 0,0, NULL, bit_depth_C);
    }
    else
      {
        for (int y=0;y<nPbHC;y++)
          for (int x=0;x<nPbWC;x++) {

            int xB = Clip3(0,wC-1,x + xIntOffsC);
            int yB = Clip3(0,hC-1,y + yIntOffsC);

            out[y*out_stride+x] = ref[ xB + yB*ref_stride ] << shift3;
          }
      }
  }
  else {
    pixel_t padbuf[(MAX_CU_SIZE+16)*(MAX_CU_SIZE+3)];

    const pixel_t* src_ptr;
    int src_stride;

    int extra_top  = 1;
    int extra_left = 1;
    int extra_right  = 2;
    int extra_bottom = 2;

    if (xIntOffsC>=1 && nPbWC+xIntOffsC<=wC-2 &&
        yIntOffsC>=1 && nPbHC+yIntOffsC<=hC-2) {
      src_ptr = &ref[xIntOffsC + yIntOffsC*ref_stride];
      src_stride = ref_stride;
    }
    else {
      for (int y=-extra_top;y<nPbHC+extra_bottom;y++) {
        for (int x=-extra_left;x<nPbWC+extra_right;x++) {

          int xA = Clip3(0,wC-1,x + xIntOffsC);
          int yA = Clip3(0,hC-1,y + yIntOffsC);

          padbuf[x+extra_left + (y+extra_top)*(MAX_CU_SIZE+16)] = ref[ xA + yA*ref_stride ];
        }
      }

      src_ptr = &padbuf[extra_left + extra_top*(MAX_CU_SIZE+16)];
      src_stride = MAX_CU_SIZE+16;
    }


    if (xFracC && yFracC) {
      ctx->acceleration.put_hevc_epel_hv(out, out_stride,
                                         src_ptr, src_stride,
                                         nPbWC,nPbHC, xFracC,yFracC, mcbuffer, bit_depth_C);
    }
    else if (xFracC) {
      ctx->acceleration.put_hevc_epel_h(out, out_stride,
                                        src_ptr, src_stride,
                                        nPbWC,nPbHC, xFracC,yFracC, mcbuffer, bit_depth_C);
    }
    else if (yFracC) {
      ctx->acceleration.put_hevc_epel_v(out, out_stride,
                                        src_ptr, src_stride,
                                        nPbWC,nPbHC, xFracC,yFracC, mcbuffer, bit_depth_C);
    }
    else {
      assert(false); // full-pel shifts are handled above
    }
  }
}



// 8.5.3.2
// NOTE: for full-pel shifts, we can introduce a fast path, simply copying without shifts
void generate_inter_prediction_samples(base_context* ctx,
                                       const slice_segment_header* shdr,
                                       de265_image* img,
                                       int xC,int yC,
                                       int xB,int yB,
                                       int nCS, int nPbW,int nPbH,
                                       const PBMotion* vi)
{
  int xP = xC+xB;
  int yP = yC+yB;

  void*  pixels[3];
  int    stride[3];

  const pic_parameter_set* pps = shdr->pps.get();
  const seq_parameter_set* sps = pps->sps.get();

  if (sps->BitDepth_Y != img->get_bit_depth(0) ||
      sps->BitDepth_C != img->get_bit_depth(1)) {
    img->integrity = INTEGRITY_DECODING_ERRORS;
    ctx->add_warning(DE265_WARNING_BIT_DEPTH_OF_CURRENT_IMAGE_DOES_NOT_MATCH_SPS, false);
    return;
  }

  if (sps->chroma_format_idc != img->get_chroma_format()) {
    img->integrity = INTEGRITY_DECODING_ERRORS;
    ctx->add_warning(DE265_WARNING_CHROMA_OF_CURRENT_IMAGE_DOES_NOT_MATCH_SPS, false);
    return;
  }

  const int SubWidthC  = sps->SubWidthC;
  const int SubHeightC = sps->SubHeightC;

  pixels[0] = img->get_image_plane_at_pos_any_depth(0,xP,yP);
  stride[0] = img->get_image_stride(0);

  pixels[1] = img->get_image_plane_at_pos_any_depth(1,xP/SubWidthC,yP/SubHeightC);
  stride[1] = img->get_image_stride(1);

  pixels[2] = img->get_image_plane_at_pos_any_depth(2,xP/SubWidthC,yP/SubHeightC);
  stride[2] = img->get_image_stride(2);


  ALIGNED_16(int16_t) predSamplesL                 [2 /* LX */][MAX_CU_SIZE* MAX_CU_SIZE];
  ALIGNED_16(int16_t) predSamplesC[2 /* chroma */ ][2 /* LX */][MAX_CU_SIZE* MAX_CU_SIZE];

  //int xP = xC+xB;
  //int yP = yC+yB;

  int predFlag[2];
  predFlag[0] = vi->predFlag[0];
  predFlag[1] = vi->predFlag[1];

  const int bit_depth_L = sps->BitDepth_Y;
  const int bit_depth_C = sps->BitDepth_C;

  // Some encoders use bi-prediction with two similar MVs.
  // Identify this case and use only one MV.

  // do this only without weighted prediction, because the weights/offsets may be different
  if (pps->weighted_pred_flag==0) {
    if (predFlag[0] && predFlag[1]) {
      if (vi->mv[0].x == vi->mv[1].x &&
          vi->mv[0].y == vi->mv[1].y &&
          shdr->RefPicList[0][vi->refIdx[0]] ==
          shdr->RefPicList[1][vi->refIdx[1]]) {
        predFlag[1] = 0;
      }
    }
  }


  for (int l=0;l<2;l++) {
    if (predFlag[l]) {
      // 8.5.3.2.1

      if (vi->refIdx[l] >= MAX_NUM_REF_PICS) {
        img->integrity = INTEGRITY_DECODING_ERRORS;
        ctx->add_warning(DE265_WARNING_NONEXISTING_REFERENCE_PICTURE_ACCESSED, false);
        return;
      }

      const de265_image* refPic = ctx->get_image(shdr->RefPicList[l][vi->refIdx[l]]);

      logtrace(LogMotion, "refIdx: %d -> dpb[%d]\n", vi->refIdx[l], shdr->RefPicList[l][vi->refIdx[l]]);

      if (!refPic || refPic->PicState == UnusedForReference) {
        img->integrity = INTEGRITY_DECODING_ERRORS;
        ctx->add_warning(DE265_WARNING_NONEXISTING_REFERENCE_PICTURE_ACCESSED, false);

        // TODO: fill predSamplesC with black or grey
      }
      else if (refPic->get_width(0) != sps->pic_width_in_luma_samples ||
               refPic->get_height(0) != sps->pic_height_in_luma_samples ||
               img->get_chroma_format() != refPic->get_chroma_format()) {
        img->integrity = INTEGRITY_DECODING_ERRORS;
        ctx->add_warning(DE265_WARNING_REFERENCE_IMAGE_SIZE_DOES_NOT_MATCH_SPS, false);
      }
      else if (img->get_bit_depth(0) != refPic->get_bit_depth(0) ||
               img->get_bit_depth(1) != refPic->get_bit_depth(1)) {
        img->integrity = INTEGRITY_DECODING_ERRORS;
        ctx->add_warning(DE265_WARNING_REFERENCE_IMAGE_BIT_DEPTH_DOES_NOT_MATCH, false);
      }
      else if (img->get_chroma_format() != refPic->get_chroma_format()) {
        img->integrity = INTEGRITY_DECODING_ERRORS;
        ctx->add_warning(DE265_WARNING_REFERENCE_IMAGE_CHROMA_FORMAT_DOES_NOT_MATCH, false);
      }
      else {
        // 8.5.3.2.2

        logtrace(LogMotion,"do MC: L%d,MV=%d;%d RefPOC=%d\n",
                 l,vi->mv[l].x,vi->mv[l].y,refPic->PicOrderCntVal);


        // TODO: must predSamples stride really be nCS or can it be something smaller like nPbW?

        if (img->high_bit_depth(0)) {
          mc_luma(ctx, sps, vi->mv[l].x, vi->mv[l].y, xP,yP,
                  predSamplesL[l],nCS,
                  (const uint16_t*)refPic->get_image_plane(0),
                  refPic->get_luma_stride(), nPbW,nPbH, bit_depth_L);
        }
        else {
          mc_luma(ctx, sps, vi->mv[l].x, vi->mv[l].y, xP,yP,
                  predSamplesL[l],nCS,
                  (const uint8_t*)refPic->get_image_plane(0),
                  refPic->get_luma_stride(), nPbW,nPbH, bit_depth_L);
        }

        if (img->get_chroma_format() != de265_chroma_mono) {
          if (img->high_bit_depth(1)) {
            mc_chroma(ctx, sps, vi->mv[l].x, vi->mv[l].y, xP, yP,
                      predSamplesC[0][l], nCS, (const uint16_t*) refPic->get_image_plane(1),
                      refPic->get_chroma_stride(), nPbW / SubWidthC, nPbH / SubHeightC, bit_depth_C);
            mc_chroma(ctx, sps, vi->mv[l].x, vi->mv[l].y, xP, yP,
                      predSamplesC[1][l], nCS, (const uint16_t*) refPic->get_image_plane(2),
                      refPic->get_chroma_stride(), nPbW / SubWidthC, nPbH / SubHeightC, bit_depth_C);
          }
          else {
            mc_chroma(ctx, sps, vi->mv[l].x, vi->mv[l].y, xP, yP,
                      predSamplesC[0][l], nCS, (const uint8_t*) refPic->get_image_plane(1),
                      refPic->get_chroma_stride(), nPbW / SubWidthC, nPbH / SubHeightC, bit_depth_C);
            mc_chroma(ctx, sps, vi->mv[l].x, vi->mv[l].y, xP, yP,
                      predSamplesC[1][l], nCS, (const uint8_t*) refPic->get_image_plane(2),
                      refPic->get_chroma_stride(), nPbW / SubWidthC, nPbH / SubHeightC, bit_depth_C);
          }
        }
      }
    }
  }


  // weighted sample prediction  (8.5.3.2.3)

  const int shift1_L = libde265_max(2,14-sps->BitDepth_Y);
  const int offset_shift1_L = img->get_sps().WpOffsetBdShiftY;
  const int shift1_C = libde265_max(2,14-sps->BitDepth_C);
  const int offset_shift1_C = img->get_sps().WpOffsetBdShiftC;

  /*
  const int shift1_L = 14-img->sps.BitDepth_Y;
  const int offset_shift1_L = img->sps.BitDepth_Y-8;
  const int shift1_C = 14-img->sps.BitDepth_C;
  const int offset_shift1_C = img->sps.BitDepth_C-8;
  */

  /*
  if (0)
  printf("%d/%d %d/%d %d/%d %d/%d\n",
         shift1_L,
         Nshift1_L,
         offset_shift1_L,
         Noffset_shift1_L,
         shift1_C,
         Nshift1_C,
         offset_shift1_C,
         Noffset_shift1_C);

  assert(shift1_L==
         Nshift1_L);
  assert(offset_shift1_L==
         Noffset_shift1_L);
  assert(shift1_C==
         Nshift1_C);
  assert(offset_shift1_C==
         Noffset_shift1_C);
  */


  logtrace(LogMotion,"predFlags (modified): %d %d\n", predFlag[0], predFlag[1]);

  if (shdr->slice_type == SLICE_TYPE_P) {
    if (pps->weighted_pred_flag==0) {
      if (predFlag[0]==1 && predFlag[1]==0) {
        ctx->acceleration.put_unweighted_pred(pixels[0], stride[0],
                                              predSamplesL[0],nCS, nPbW,nPbH, bit_depth_L);

        if (img->get_chroma_format() != de265_chroma_mono) {
          ctx->acceleration.put_unweighted_pred(pixels[1], stride[1],
                                                predSamplesC[0][0], nCS,
                                                nPbW / SubWidthC, nPbH / SubHeightC, bit_depth_C);
          ctx->acceleration.put_unweighted_pred(pixels[2], stride[2],
                                                predSamplesC[1][0], nCS,
                                                nPbW / SubWidthC, nPbH / SubHeightC, bit_depth_C);
        }
      }
      else {
        ctx->add_warning(DE265_WARNING_BOTH_PREDFLAGS_ZERO, false);
        img->integrity = INTEGRITY_DECODING_ERRORS;
      }
    }
    else {
      // weighted prediction

      if (predFlag[0]==1 && predFlag[1]==0) {

        int refIdx0 = vi->refIdx[0];

        int luma_log2WD   = shdr->luma_log2_weight_denom + shift1_L;
        int chroma_log2WD = shdr->ChromaLog2WeightDenom  + shift1_C;

        int luma_w0 = shdr->LumaWeight[0][refIdx0];
        int luma_o0 = shdr->luma_offset[0][refIdx0] * (1<<(offset_shift1_L));

        int chroma0_w0 = shdr->ChromaWeight[0][refIdx0][0];
        int chroma0_o0 = shdr->ChromaOffset[0][refIdx0][0] * (1<<(offset_shift1_C));
        int chroma1_w0 = shdr->ChromaWeight[0][refIdx0][1];
        int chroma1_o0 = shdr->ChromaOffset[0][refIdx0][1] * (1<<(offset_shift1_C));

        logtrace(LogMotion,"weighted-0 [%d] %d %d %d  %dx%d\n", refIdx0, luma_log2WD-6,luma_w0,luma_o0,nPbW,nPbH);

        ctx->acceleration.put_weighted_pred(pixels[0], stride[0],
                                            predSamplesL[0],nCS, nPbW,nPbH,
                                            luma_w0, luma_o0, luma_log2WD, bit_depth_L);
        if (img->get_chroma_format() != de265_chroma_mono) {
          ctx->acceleration.put_weighted_pred(pixels[1], stride[1],
                                              predSamplesC[0][0], nCS, nPbW / SubWidthC, nPbH / SubHeightC,
                                              chroma0_w0, chroma0_o0, chroma_log2WD, bit_depth_C);
          ctx->acceleration.put_weighted_pred(pixels[2], stride[2],
                                              predSamplesC[1][0], nCS, nPbW / SubWidthC, nPbH / SubHeightC,
                                              chroma1_w0, chroma1_o0, chroma_log2WD, bit_depth_C);
        }
      }
      else {
        ctx->add_warning(DE265_WARNING_BOTH_PREDFLAGS_ZERO, false);
        img->integrity = INTEGRITY_DECODING_ERRORS;
      }
    }
  }
  else {
    assert(shdr->slice_type == SLICE_TYPE_B);

    if (predFlag[0]==1 && predFlag[1]==1) {
      if (pps->weighted_bipred_flag==0) {
        //const int shift2  = 15-8; // TODO: real bit depth
        //const int offset2 = 1<<(shift2-1);

        int16_t* in0 = predSamplesL[0];
        int16_t* in1 = predSamplesL[1];

        ctx->acceleration.put_weighted_pred_avg(pixels[0], stride[0],
                                                in0,in1, nCS, nPbW, nPbH, bit_depth_L);

        int16_t* in00 = predSamplesC[0][0];
        int16_t* in01 = predSamplesC[0][1];
        int16_t* in10 = predSamplesC[1][0];
        int16_t* in11 = predSamplesC[1][1];

        if (img->get_chroma_format() != de265_chroma_mono) {
          ctx->acceleration.put_weighted_pred_avg(pixels[1], stride[1],
                                                  in00, in01, nCS,
                                                  nPbW / SubWidthC, nPbH / SubHeightC, bit_depth_C);
          ctx->acceleration.put_weighted_pred_avg(pixels[2], stride[2],
                                                  in10, in11, nCS,
                                                  nPbW / SubWidthC, nPbH / SubHeightC, bit_depth_C);
        }
      }
      else {
        // weighted prediction

        int refIdx0 = vi->refIdx[0];
        int refIdx1 = vi->refIdx[1];

        int luma_log2WD   = shdr->luma_log2_weight_denom + shift1_L;
        int chroma_log2WD = shdr->ChromaLog2WeightDenom + shift1_C;

        int luma_w0 = shdr->LumaWeight[0][refIdx0];
        int luma_o0 = shdr->luma_offset[0][refIdx0] * (1<<(offset_shift1_L));
        int luma_w1 = shdr->LumaWeight[1][refIdx1];
        int luma_o1 = shdr->luma_offset[1][refIdx1] * (1<<(offset_shift1_L));

        int chroma0_w0 = shdr->ChromaWeight[0][refIdx0][0];
        int chroma0_o0 = shdr->ChromaOffset[0][refIdx0][0] * (1<<(offset_shift1_C));
        int chroma1_w0 = shdr->ChromaWeight[0][refIdx0][1];
        int chroma1_o0 = shdr->ChromaOffset[0][refIdx0][1] * (1<<(offset_shift1_C));
        int chroma0_w1 = shdr->ChromaWeight[1][refIdx1][0];
        int chroma0_o1 = shdr->ChromaOffset[1][refIdx1][0] * (1<<(offset_shift1_C));
        int chroma1_w1 = shdr->ChromaWeight[1][refIdx1][1];
        int chroma1_o1 = shdr->ChromaOffset[1][refIdx1][1] * (1<<(offset_shift1_C));

        logtrace(LogMotion,"weighted-BI-0 [%d] %d %d %d  %dx%d\n", refIdx0, luma_log2WD-6,luma_w0,luma_o0,nPbW,nPbH);
        logtrace(LogMotion,"weighted-BI-1 [%d] %d %d %d  %dx%d\n", refIdx1, luma_log2WD-6,luma_w1,luma_o1,nPbW,nPbH);

        int16_t* in0 = predSamplesL[0];
        int16_t* in1 = predSamplesL[1];

        ctx->acceleration.put_weighted_bipred(pixels[0], stride[0],
                                              in0,in1, nCS, nPbW, nPbH,
                                              luma_w0,luma_o0,
                                              luma_w1,luma_o1,
                                              luma_log2WD, bit_depth_L);

        int16_t* in00 = predSamplesC[0][0];
        int16_t* in01 = predSamplesC[0][1];
        int16_t* in10 = predSamplesC[1][0];
        int16_t* in11 = predSamplesC[1][1];

        if (img->get_chroma_format() != de265_chroma_mono) {
          ctx->acceleration.put_weighted_bipred(pixels[1], stride[1],
                                                in00, in01, nCS, nPbW / SubWidthC, nPbH / SubHeightC,
                                                chroma0_w0, chroma0_o0,
                                                chroma0_w1, chroma0_o1,
                                                chroma_log2WD, bit_depth_C);
          ctx->acceleration.put_weighted_bipred(pixels[2], stride[2],
                                                in10, in11, nCS, nPbW / SubWidthC, nPbH / SubHeightC,
                                                chroma1_w0, chroma1_o0,
                                                chroma1_w1, chroma1_o1,
                                                chroma_log2WD, bit_depth_C);
        }
      }
    }
    else if (predFlag[0]==1 || predFlag[1]==1) {
      int l = predFlag[0] ? 0 : 1;

      if (pps->weighted_bipred_flag==0) {
        ctx->acceleration.put_unweighted_pred(pixels[0], stride[0],
                                              predSamplesL[l],nCS, nPbW,nPbH, bit_depth_L);

        if (img->get_chroma_format() != de265_chroma_mono) {
          ctx->acceleration.put_unweighted_pred(pixels[1], stride[1],
                                                predSamplesC[0][l], nCS,
                                                nPbW / SubWidthC, nPbH / SubHeightC, bit_depth_C);
          ctx->acceleration.put_unweighted_pred(pixels[2], stride[2],
                                                predSamplesC[1][l], nCS,
                                                nPbW / SubWidthC, nPbH / SubHeightC, bit_depth_C);
        }
      }
      else {
        int refIdx = vi->refIdx[l];

        int luma_log2WD   = shdr->luma_log2_weight_denom + shift1_L;
        int chroma_log2WD = shdr->ChromaLog2WeightDenom  + shift1_C;

        int luma_w = shdr->LumaWeight[l][refIdx];
        int luma_o = shdr->luma_offset[l][refIdx] * (1<<(offset_shift1_L));

        int chroma0_w = shdr->ChromaWeight[l][refIdx][0];
        int chroma0_o = shdr->ChromaOffset[l][refIdx][0] * (1<<(offset_shift1_C));
        int chroma1_w = shdr->ChromaWeight[l][refIdx][1];
        int chroma1_o = shdr->ChromaOffset[l][refIdx][1] * (1<<(offset_shift1_C));

        logtrace(LogMotion,"weighted-B-L%d [%d] %d %d %d  %dx%d\n", l, refIdx, luma_log2WD-6,luma_w,luma_o,nPbW,nPbH);

        ctx->acceleration.put_weighted_pred(pixels[0], stride[0],
                                            predSamplesL[l],nCS, nPbW,nPbH,
                                            luma_w, luma_o, luma_log2WD, bit_depth_L);

        if (img->get_chroma_format() != de265_chroma_mono) {
          ctx->acceleration.put_weighted_pred(pixels[1], stride[1],
                                              predSamplesC[0][l], nCS,
                                              nPbW / SubWidthC, nPbH / SubHeightC,
                                              chroma0_w, chroma0_o, chroma_log2WD, bit_depth_C);
          ctx->acceleration.put_weighted_pred(pixels[2], stride[2],
                                              predSamplesC[1][l], nCS,
                                              nPbW / SubWidthC, nPbH / SubHeightC,
                                              chroma1_w, chroma1_o, chroma_log2WD, bit_depth_C);
        }
      }
    }
    else {
      // TODO: check why it can actually happen that both predFlags[] are false.
      // For now, we ignore this and continue decoding.

      ctx->add_warning(DE265_WARNING_BOTH_PREDFLAGS_ZERO, false);
      img->integrity = INTEGRITY_DECODING_ERRORS;
    }
  }

#if defined(DE265_LOG_TRACE) && 0
  logtrace(LogTransform,"MC pixels (luma), position %d %d:\n", xP,yP);

  for (int y=0;y<nPbH;y++) {
    logtrace(LogTransform,"MC-y-%d-%d ",xP,yP+y);

    for (int x=0;x<nPbW;x++) {
      logtrace(LogTransform,"*%02x ", pixels[0][x+y*stride[0]]);
    }

    logtrace(LogTransform,"*\n");
  }


  logtrace(LogTransform,"MC pixels (chroma cb), position %d %d:\n", xP/2,yP/2);

  for (int y=0;y<nPbH/2;y++) {
    logtrace(LogTransform,"MC-cb-%d-%d ",xP/2,yP/2+y);

    for (int x=0;x<nPbW/2;x++) {
      logtrace(LogTransform,"*%02x ", pixels[1][x+y*stride[1]]);
    }

    logtrace(LogTransform,"*\n");
  }


  logtrace(LogTransform,"MC pixels (chroma cr), position %d %d:\n", xP/2,yP/2);

  for (int y=0;y<nPbH/2;y++) {
    logtrace(LogTransform,"MC-cr-%d-%d ",xP/2,yP/2+y);

    for (int x=0;x<nPbW/2;x++) {
      logtrace(LogTransform,"*%02x ", pixels[2][x+y*stride[2]]);
    }

    logtrace(LogTransform,"*\n");
  }
#endif
}


#ifdef DE265_LOG_TRACE
void logmvcand(const PBMotion& p)
{
  for (int v=0;v<2;v++) {
    if (p.predFlag[v]) {
      logtrace(LogMotion,"  %d: %s  %d;%d ref=%d\n", v, p.predFlag[v] ? "yes":"no ",
               p.mv[v].x,p.mv[v].y, p.refIdx[v]);
    } else {
      logtrace(LogMotion,"  %d: %s  --;-- ref=--\n", v, p.predFlag[v] ? "yes":"no ");
    }
  }
}
#else
#define logmvcand(p)
#endif


bool PBMotion::operator==(const PBMotion& b) const
{
  const PBMotion& a = *this;

  // TODO: is this really correct? no check for predFlag? Standard says so... (p.127)

  for (int i=0;i<2;i++) {
    if (a.predFlag[i] != b.predFlag[i]) return false;

    if (a.predFlag[i]) {
      if (a.mv[i].x != b.mv[i].x) return false;
      if (a.mv[i].y != b.mv[i].y) return false;
      if (a.refIdx[i] != b.refIdx[i]) return false;
    }
  }

  return true;
}


class MotionVectorAccess_de265_image : public MotionVectorAccess
{
public:
  MotionVectorAccess_de265_image(const de265_image* i) : img(i) { }

  enum PartMode get_PartMode(int x,int y) const override { return img->get_PartMode(x,y); }
  const PBMotion& get_mv_info(int x,int y) const override { return img->get_mv_info(x,y); }

private:
  const de265_image* img;
};



/*
  +--+                +--+--+
  |B2|                |B1|B0|
  +--+----------------+--+--+
     |                   |
     |                   |
     |                   |
     |                   |
     |        PB         |
     |                   |
     |                   |
  +--+                   |
  |A1|                   |
  +--+-------------------+
  |A0|
  +--+
*/


// 8.5.3.1.2
// TODO: check: can we fill the candidate list directly in this function and omit to copy later
/*
  xC/yC:  CB position
  nCS:    CB size                 (probably modified because of singleMCLFlag)
  xP/yP:  PB position (absolute)  (probably modified because of singleMCLFlag)
  singleMCLFlag
  nPbW/nPbH: PB size
  partIdx
  out_cand: merging candidate vectors

  Add these candidates:
  - A1
  - B1  (if != A1)
  - B0  (if != B1)
  - A0  (if != A1)
  - B2  (if != A1 and != B1)

  A maximum of 4 candidates are generated.

  Note 1: For a CB split into two PBs, it does not make sense to merge the
  second part to the parameters of the first part, since then, we could use 2Nx2N
  right away. -> Exclude this candidate.
*/
int derive_spatial_merging_candidates(//const de265_image* img,
                                      const MotionVectorAccess& mvaccess,
                                      const de265_image* img,
                                      int xC, int yC, int nCS, int xP, int yP,
                                      uint8_t singleMCLFlag,
                                      int nPbW, int nPbH,
                                      int partIdx,
                                      PBMotion* out_cand,
                                      int maxCandidates)
{
  const pic_parameter_set* pps = &img->get_pps();
  const int log2_parallel_merge_level = pps->log2_parallel_merge_level;

  enum PartMode PartMode = mvaccess.get_PartMode(xC,yC);

  /*
  const int A0 = SpatialMergingCandidates::PRED_A0;
  const int A1 = SpatialMergingCandidates::PRED_A1;
  const int B0 = SpatialMergingCandidates::PRED_B0;
  const int B1 = SpatialMergingCandidates::PRED_B1;
  const int B2 = SpatialMergingCandidates::PRED_B2;
  */

  // --- A1 ---

  // a pixel within A1 (bottom right of A1)
  int xA1 = xP-1;
  int yA1 = yP+nPbH-1;

  bool availableA1;
  int idxA1;

  int computed_candidates = 0;

  // check if candidate is in same motion-estimation region (MER) -> discard
  if ((xP>>log2_parallel_merge_level) == (xA1>>log2_parallel_merge_level) &&
      (yP>>log2_parallel_merge_level) == (yA1>>log2_parallel_merge_level)) {
    availableA1 = false;
    logtrace(LogMotion,"spatial merging candidate A1: below parallel merge level\n");
  }
  // redundant candidate? (Note 1) -> discard
  else if (// !singleMCLFlag &&    automatically true when partIdx==1
           partIdx==1 &&
           (PartMode==PART_Nx2N ||
            PartMode==PART_nLx2N ||
            PartMode==PART_nRx2N)) {
    availableA1 = false;
    logtrace(LogMotion,"spatial merging candidate A1: second part ignore\n");
  }
  // MV available in A1
  else {
    availableA1 = img->available_pred_blk(xC,yC, nCS, xP,yP, nPbW,nPbH,partIdx, xA1,yA1);
    if (!availableA1) logtrace(LogMotion,"spatial merging candidate A1: unavailable\n");
  }

  if (availableA1) {
    idxA1 = computed_candidates++;
    out_cand[idxA1] = mvaccess.get_mv_info(xA1,yA1);

    logtrace(LogMotion,"spatial merging candidate A1:\n");
    logmvcand(out_cand[idxA1]);
  }

  if (computed_candidates>=maxCandidates) return computed_candidates;


  // --- B1 ---

  int xB1 = xP+nPbW-1;
  int yB1 = yP-1;

  bool availableB1;
  int idxB1;

  // same MER -> discard
  if ((xP>>log2_parallel_merge_level) == (xB1>>log2_parallel_merge_level) &&
      (yP>>log2_parallel_merge_level) == (yB1>>log2_parallel_merge_level)) {
    availableB1 = false;
    logtrace(LogMotion,"spatial merging candidate B1: below parallel merge level\n");
  }
  // redundant candidate (Note 1) -> discard
  else if (// !singleMCLFlag &&    automatically true when partIdx==1
           partIdx==1 &&
           (PartMode==PART_2NxN ||
            PartMode==PART_2NxnU ||
            PartMode==PART_2NxnD)) {
    availableB1 = false;
    logtrace(LogMotion,"spatial merging candidate B1: second part ignore\n");
  }
  // MV available in B1
  else {
    availableB1 = img->available_pred_blk(xC,yC, nCS, xP,yP, nPbW,nPbH,partIdx, xB1,yB1);
    if (!availableB1) logtrace(LogMotion,"spatial merging candidate B1: unavailable\n");
  }

  if (availableB1) {
    const PBMotion& b1 = img->get_mv_info(xB1,yB1);

    // B1 == A1 -> discard B1
    if (availableA1 && out_cand[idxA1] == b1) {
      idxB1 = idxA1;
      logtrace(LogMotion,"spatial merging candidate B1: redundant to A1\n");
    }
    else {
      idxB1 = computed_candidates++;
      out_cand[idxB1] = b1;

      logtrace(LogMotion,"spatial merging candidate B1:\n");
      logmvcand(out_cand[idxB1]);
    }
  }

  if (computed_candidates>=maxCandidates) return computed_candidates;


  // --- B0 ---

  int xB0 = xP+nPbW;
  int yB0 = yP-1;

  bool availableB0;
  int idxB0;

  if ((xP>>log2_parallel_merge_level) == (xB0>>log2_parallel_merge_level) &&
      (yP>>log2_parallel_merge_level) == (yB0>>log2_parallel_merge_level)) {
    availableB0 = false;
    logtrace(LogMotion,"spatial merging candidate B0: below parallel merge level\n");
  }
  else {
    availableB0 = img->available_pred_blk(xC,yC, nCS, xP,yP, nPbW,nPbH,partIdx, xB0,yB0);
    if (!availableB0) logtrace(LogMotion,"spatial merging candidate B0: unavailable\n");
  }

  if (availableB0) {
    const PBMotion& b0 = img->get_mv_info(xB0,yB0);

    // B0 == B1 -> discard B0
    if (availableB1 && out_cand[idxB1]==b0) {
      idxB0 = idxB1;
      logtrace(LogMotion,"spatial merging candidate B0: redundant to B1\n");
    }
    else {
      idxB0 = computed_candidates++;
      out_cand[idxB0] = b0;
      logtrace(LogMotion,"spatial merging candidate B0:\n");
      logmvcand(out_cand[idxB0]);
    }
  }

  if (computed_candidates>=maxCandidates) return computed_candidates;


  // --- A0 ---

  int xA0 = xP-1;
  int yA0 = yP+nPbH;

  bool availableA0;
  int idxA0;

  if ((xP>>log2_parallel_merge_level) == (xA0>>log2_parallel_merge_level) &&
      (yP>>log2_parallel_merge_level) == (yA0>>log2_parallel_merge_level)) {
    availableA0 = false;
    logtrace(LogMotion,"spatial merging candidate A0: below parallel merge level\n");
  }
  else {
    availableA0 = img->available_pred_blk(xC,yC, nCS, xP,yP, nPbW,nPbH,partIdx, xA0,yA0);
    if (!availableA0) logtrace(LogMotion,"spatial merging candidate A0: unavailable\n");
  }

  if (availableA0) {
    const PBMotion& a0 = img->get_mv_info(xA0,yA0);

    // A0 == A1 -> discard A0
    if (availableA1 && out_cand[idxA1]==a0) {
      idxA0 = idxA1;
      logtrace(LogMotion,"spatial merging candidate A0: redundant to A1\n");
    }
    else {
      idxA0 = computed_candidates++;
      out_cand[idxA0] = a0;
      logtrace(LogMotion,"spatial merging candidate A0:\n");
      logmvcand(out_cand[idxA0]);
    }
  }

  if (computed_candidates>=maxCandidates) return computed_candidates;


  // --- B2 ---

  int xB2 = xP-1;
  int yB2 = yP-1;

  bool availableB2;
  int idxB2;

  // if we already have four candidates, do not consider B2 anymore
  if (computed_candidates==4) {
    availableB2 = false;
    logtrace(LogMotion,"spatial merging candidate B2: ignore\n");
  }
  else if ((xP>>log2_parallel_merge_level) == (xB2>>log2_parallel_merge_level) &&
           (yP>>log2_parallel_merge_level) == (yB2>>log2_parallel_merge_level)) {
    availableB2 = false;
    logtrace(LogMotion,"spatial merging candidate B2: below parallel merge level\n");
  }
  else {
    availableB2 = img->available_pred_blk(xC,yC, nCS, xP,yP, nPbW,nPbH,partIdx, xB2,yB2);
    if (!availableB2) logtrace(LogMotion,"spatial merging candidate B2: unavailable\n");
  }

  if (availableB2) {
    const PBMotion& b2 = img->get_mv_info(xB2,yB2);

    // B2 == B1 -> discard B2
    if (availableB1 && out_cand[idxB1]==b2) {
      idxB2 = idxB1;
      logtrace(LogMotion,"spatial merging candidate B2: redundant to B1\n");
    }
    // B2 == A1 -> discard B2
    else if (availableA1 && out_cand[idxA1]==b2) {
      idxB2 = idxA1;
      logtrace(LogMotion,"spatial merging candidate B2: redundant to A1\n");
    }
    else {
      idxB2 = computed_candidates++;
      out_cand[idxB2] = b2;
      logtrace(LogMotion,"spatial merging candidate B2:\n");
      logmvcand(out_cand[idxB2]);
    }
  }

  return computed_candidates;
}


// 8.5.3.1.4
void derive_zero_motion_vector_candidates(const slice_segment_header* shdr,
                                          PBMotion* out_mergeCandList,
                                          int* inout_numCurrMergeCand,
                                          int maxCandidates)
{
  logtrace(LogMotion,"derive_zero_motion_vector_candidates\n");

  int numRefIdx;

  if (shdr->slice_type==SLICE_TYPE_P) {
    numRefIdx = shdr->num_ref_idx_l0_active;
  }
  else {
    numRefIdx = libde265_min(shdr->num_ref_idx_l0_active,
                             shdr->num_ref_idx_l1_active);
  }


  //int numInputMergeCand = *inout_numMergeCand;
  int zeroIdx = 0;

  while (*inout_numCurrMergeCand < maxCandidates) {
    // 1.

    logtrace(LogMotion,"zeroIdx:%d numRefIdx:%d\n", zeroIdx, numRefIdx);

    PBMotion* newCand = &out_mergeCandList[*inout_numCurrMergeCand];

    const int refIdx = (zeroIdx < numRefIdx) ? zeroIdx : 0;

    if (shdr->slice_type==SLICE_TYPE_P) {
      newCand->refIdx[0] = refIdx;
      newCand->refIdx[1] = -1;
      newCand->predFlag[0] = 1;
      newCand->predFlag[1] = 0;
    }
    else {
      newCand->refIdx[0] = refIdx;
      newCand->refIdx[1] = refIdx;
      newCand->predFlag[0] = 1;
      newCand->predFlag[1] = 1;
    }

    newCand->mv[0].x = 0;
    newCand->mv[0].y = 0;
    newCand->mv[1].x = 0;
    newCand->mv[1].y = 0;

    (*inout_numCurrMergeCand)++;

    // 2.

    zeroIdx++;
  }
}


bool scale_mv(MotionVector* out_mv, MotionVector mv, int colDist, int currDist)
{
  int td = Clip3(-128,127, colDist);
  int tb = Clip3(-128,127, currDist);

  if (td==0) {
    *out_mv = mv;
    return false;
  }
  else {
    int tx = (16384 + (abs_value(td)>>1)) / td;
    int distScaleFactor = Clip3(-4096,4095, (tb*tx+32)>>6);
    out_mv->x = Clip3(-32768,32767,
                      Sign(distScaleFactor*mv.x)*((abs_value(distScaleFactor*mv.x)+127)>>8));
    out_mv->y = Clip3(-32768,32767,
                      Sign(distScaleFactor*mv.y)*((abs_value(distScaleFactor*mv.y)+127)>>8));
    return true;
  }
}


// (L1003) 8.5.3.2.8

void derive_collocated_motion_vectors(base_context* ctx,
                                      de265_image* img,
                                      const slice_segment_header* shdr,
                                      int xP,int yP,
                                      int colPic,
                                      int xColPb,int yColPb,
                                      int refIdxLX,  // (always 0 for merge mode)
                                      int X,
                                      MotionVector* out_mvLXCol,
                                      uint8_t* out_availableFlagLXCol)
{
  logtrace(LogMotion,"derive_collocated_motion_vectors %d;%d\n",xP,yP);


  // get collocated image and the prediction mode at the collocated position

  assert(ctx->has_image(colPic));
  const de265_image* colImg = ctx->get_image(colPic);

  // check for access outside image area

  if (xColPb >= colImg->get_width() ||
      yColPb >= colImg->get_height()) {
    ctx->add_warning(DE265_WARNING_COLLOCATED_MOTION_VECTOR_OUTSIDE_IMAGE_AREA, false);
    *out_availableFlagLXCol = 0;
    return;
  }

  enum PredMode predMode = colImg->get_pred_mode(xColPb,yColPb);


  // collocated block is Intra -> no collocated MV

  if (predMode == MODE_INTRA) {
    out_mvLXCol->x = 0;
    out_mvLXCol->y = 0;
    *out_availableFlagLXCol = 0;
    return;
  }


  logtrace(LogMotion,"colPic:%d (POC=%d) X:%d refIdxLX:%d refpiclist:%d\n",
           colPic,
           colImg->PicOrderCntVal,
           X,refIdxLX,shdr->RefPicList[X][refIdxLX]);


  // collocated reference image is unavailable -> no collocated MV

  if (colImg->integrity == INTEGRITY_UNAVAILABLE_REFERENCE) {
    out_mvLXCol->x = 0;
    out_mvLXCol->y = 0;
    *out_availableFlagLXCol = 0;
    return;
  }


  // get the collocated MV

  const PBMotion& mvi = colImg->get_mv_info(xColPb,yColPb);
  int listCol;
  int refIdxCol;
  MotionVector mvCol;

  logtrace(LogMotion,"read MVI %d;%d:\n",xColPb,yColPb);
  logmvcand(mvi);


  // collocated MV uses only L1 -> use L1
  if (mvi.predFlag[0]==0) {
    mvCol = mvi.mv[1];
    refIdxCol = mvi.refIdx[1];
    listCol = 1;
  }
  // collocated MV uses only L0 -> use L0
  else if (mvi.predFlag[1]==0) {
    mvCol = mvi.mv[0];
    refIdxCol = mvi.refIdx[0];
    listCol = 0;
  }
  // collocated MV uses L0 and L1
  else {
    bool allRefFramesBeforeCurrentFrame = true;

    const int currentPOC = img->PicOrderCntVal;

    // all reference POCs earlier than current POC (list 1)
    // Test L1 first, because there is a higher change to find a future reference frame.

    for (int rIdx=0; rIdx<shdr->num_ref_idx_l1_active && allRefFramesBeforeCurrentFrame; rIdx++)
      {
        const de265_image* refimg = ctx->get_image(shdr->RefPicList[1][rIdx]);
        int refPOC = refimg->PicOrderCntVal;

        if (refPOC > currentPOC) {
          allRefFramesBeforeCurrentFrame = false;
        }
      }

    // all reference POCs earlier than current POC (list 0)

    for (int rIdx=0; rIdx<shdr->num_ref_idx_l0_active && allRefFramesBeforeCurrentFrame; rIdx++)
      {
        const de265_image* refimg = ctx->get_image(shdr->RefPicList[0][rIdx]);
        int refPOC = refimg->PicOrderCntVal;

        if (refPOC > currentPOC) {
          allRefFramesBeforeCurrentFrame = false;
        }
      }


    /* TODO: What is the rationale behind this ???

       My guess:
       when there are images before the current frame (most probably in L0) and images after
       the current frame (most probably in L1), we take the reference in the opposite
       direction than where the collocated frame is positioned in the hope that the distance
       to the current frame will be smaller and thus give a better prediction.

       If all references point into the past, we cannot say much about the temporal order or
       L0,L1 and thus take over both parts.
     */

    if (allRefFramesBeforeCurrentFrame) {
      mvCol = mvi.mv[X];
      refIdxCol = mvi.refIdx[X];
      listCol = X;
    }
    else {
      int N = shdr->collocated_from_l0_flag;
      mvCol = mvi.mv[N];
      refIdxCol = mvi.refIdx[N];
      listCol = N;
    }
  }



  int slice_hdr_idx = colImg->get_SliceHeaderIndex(xColPb,yColPb);
  if (slice_hdr_idx >= colImg->slices.size()) {
    ctx->add_warning(DE265_WARNING_INVALID_SLICE_HEADER_INDEX_ACCESS, false);

    *out_availableFlagLXCol = 0;
    out_mvLXCol->x = 0;
    out_mvLXCol->y = 0;
    return;
  }

  const slice_segment_header* colShdr = colImg->slices[ colImg->get_SliceHeaderIndex(xColPb,yColPb) ];

  if (shdr->LongTermRefPic[X][refIdxLX] !=
      colShdr->LongTermRefPic[listCol][refIdxCol]) {
    *out_availableFlagLXCol = 0;
    out_mvLXCol->x = 0;
    out_mvLXCol->y = 0;
  }
  else {
    *out_availableFlagLXCol = 1;

    const bool isLongTerm = shdr->LongTermRefPic[X][refIdxLX];

    int colDist  = colImg->PicOrderCntVal - colShdr->RefPicList_POC[listCol][refIdxCol];
    int currDist = img->PicOrderCntVal - shdr->RefPicList_POC[X][refIdxLX];

    logtrace(LogMotion,"COLPOCDIFF %d %d [%d %d / %d %d]\n",colDist, currDist,
             colImg->PicOrderCntVal, colShdr->RefPicList_POC[listCol][refIdxCol],
             img->PicOrderCntVal, shdr->RefPicList_POC[X][refIdxLX]
             );

    if (isLongTerm || colDist == currDist) {
      *out_mvLXCol = mvCol;
    }
    else {
      if (!scale_mv(out_mvLXCol, mvCol, colDist, currDist)) {
        ctx->add_warning(DE265_WARNING_INCORRECT_MOTION_VECTOR_SCALING, false);
        img->integrity = INTEGRITY_DECODING_ERRORS;
      }

      logtrace(LogMotion,"scale: %d;%d to %d;%d\n",
               mvCol.x,mvCol.y, out_mvLXCol->x,out_mvLXCol->y);
    }
  }
}


// 8.5.3.1.7
void derive_temporal_luma_vector_prediction(base_context* ctx,
                                            de265_image* img,
                                            const slice_segment_header* shdr,
                                            int xP,int yP,
                                            int nPbW,int nPbH,
                                            int refIdxL,
                                            int X, // which MV (L0/L1) to get
                                            MotionVector* out_mvLXCol,
                                            uint8_t*      out_availableFlagLXCol)
{
  // --- no temporal MVP -> exit ---

  if (shdr->slice_temporal_mvp_enabled_flag == 0) {
    out_mvLXCol->x = 0;
    out_mvLXCol->y = 0;
    *out_availableFlagLXCol = 0;
    return;
  }


  // --- find collocated reference image ---

  int Log2CtbSizeY = img->get_sps().Log2CtbSizeY;

  int colPic; // TODO: this is the same for the whole slice. We can precompute it.

  if (shdr->slice_type == SLICE_TYPE_B &&
      shdr->collocated_from_l0_flag == 0)
    {
      logtrace(LogMotion,"collocated L1 ref_idx=%d\n",shdr->collocated_ref_idx);

      colPic = shdr->RefPicList[1][ shdr->collocated_ref_idx ];
    }
  else
    {
      logtrace(LogMotion,"collocated L0 ref_idx=%d\n",shdr->collocated_ref_idx);

      colPic = shdr->RefPicList[0][ shdr->collocated_ref_idx ];
    }


  // check whether collocated reference picture exists

  if (!ctx->has_image(colPic)) {
    out_mvLXCol->x = 0;
    out_mvLXCol->y = 0;
    *out_availableFlagLXCol = 0;

    ctx->add_warning(DE265_WARNING_NONEXISTING_REFERENCE_PICTURE_ACCESSED, false);
    return;
  }


  // --- get collocated MV either at bottom-right corner or from center of PB ---

  int xColPb,yColPb;
  int yColBr = yP + nPbH; // bottom right collocated motion vector position
  int xColBr = xP + nPbW;

  /* If neighboring pixel at bottom-right corner is in the same CTB-row and inside the image,
     use this (reduced down to 16 pixels resolution) as collocated MV position.

     Note: see 2014, Sze, Sect. 5.2.1.2 why candidate C0 is excluded when on another CTB-row.
     This is to reduce the memory bandwidth requirements.
   */
  if ((yP>>Log2CtbSizeY) == (yColBr>>Log2CtbSizeY) &&
      xColBr < img->get_sps().pic_width_in_luma_samples &&
      yColBr < img->get_sps().pic_height_in_luma_samples)
    {
      xColPb = xColBr & ~0x0F; // reduce resolution of collocated motion-vectors to 16 pixels grid
      yColPb = yColBr & ~0x0F;

      derive_collocated_motion_vectors(ctx,img,shdr, xP,yP, colPic, xColPb,yColPb, refIdxL, X,
                                       out_mvLXCol, out_availableFlagLXCol);
    }
  else
    {
      out_mvLXCol->x = 0;
      out_mvLXCol->y = 0;
      *out_availableFlagLXCol = 0;
    }


  if (*out_availableFlagLXCol==0) {

    int xColCtr = xP+(nPbW>>1);
    int yColCtr = yP+(nPbH>>1);

    xColPb = xColCtr & ~0x0F; // reduce resolution of collocated motion-vectors to 16 pixels grid
    yColPb = yColCtr & ~0x0F;

    derive_collocated_motion_vectors(ctx,img,shdr, xP,yP, colPic, xColPb,yColPb, refIdxL, X,
                                     out_mvLXCol, out_availableFlagLXCol);
  }
}


static int table_8_19[2][12] = {
  { 0,1,0,2,1,2,0,3,1,3,2,3 },
  { 1,0,2,0,2,1,3,0,3,1,3,2 }
  };

// 8.5.3.1.3
/* Note (TODO): during decoding, we know which of the candidates we will select.
+   Hence, we do not really have to generate the other ones...
+ */
void derive_combined_bipredictive_merging_candidates(const base_context* ctx,
                                                     const slice_segment_header* shdr,
                                                     PBMotion* inout_mergeCandList,
                                                     int* inout_numMergeCand,
                                                     int maxCandidates)
{
  if (*inout_numMergeCand>1 && *inout_numMergeCand < maxCandidates) {
    int numOrigMergeCand = *inout_numMergeCand;

    int numInputMergeCand = *inout_numMergeCand;
    int combIdx = 0;
    uint8_t combStop = false;

    while (!combStop) {
      int l0CandIdx = table_8_19[0][combIdx];
      int l1CandIdx = table_8_19[1][combIdx];

      if (l0CandIdx >= numInputMergeCand ||
          l1CandIdx >= numInputMergeCand) {
        assert(false); // bitstream error -> TODO: conceal error
      }

      PBMotion& l0Cand = inout_mergeCandList[l0CandIdx];
      PBMotion& l1Cand = inout_mergeCandList[l1CandIdx];

      logtrace(LogMotion,"add bipredictive merging candidate (combIdx:%d)\n",combIdx);
      logtrace(LogMotion,"l0Cand:\n"); logmvcand(l0Cand);
      logtrace(LogMotion,"l1Cand:\n"); logmvcand(l1Cand);

      const de265_image* img0 = l0Cand.predFlag[0] ? ctx->get_image(shdr->RefPicList[0][l0Cand.refIdx[0]]) : NULL;
      const de265_image* img1 = l1Cand.predFlag[1] ? ctx->get_image(shdr->RefPicList[1][l1Cand.refIdx[1]]) : NULL;

      if (l0Cand.predFlag[0] && !img0) {
        return; // TODO error
      }

      if (l1Cand.predFlag[1] && !img1) {
        return; // TODO error
      }

      if (l0Cand.predFlag[0] && l1Cand.predFlag[1] &&
          (img0->PicOrderCntVal != img1->PicOrderCntVal     ||
           l0Cand.mv[0].x != l1Cand.mv[1].x ||
           l0Cand.mv[0].y != l1Cand.mv[1].y)) {
        PBMotion& p = inout_mergeCandList[ *inout_numMergeCand ];
        p.refIdx[0] = l0Cand.refIdx[0];
        p.refIdx[1] = l1Cand.refIdx[1];
        p.predFlag[0] = l0Cand.predFlag[0];
        p.predFlag[1] = l1Cand.predFlag[1];
        p.mv[0] = l0Cand.mv[0];
        p.mv[1] = l1Cand.mv[1];
        (*inout_numMergeCand)++;

        logtrace(LogMotion,"result:\n");
        logmvcand(p);
      }

      combIdx++;
      if (combIdx == numOrigMergeCand*(numOrigMergeCand-1) ||
          *inout_numMergeCand == maxCandidates) {
        combStop = true;
      }
    }
  }
}


// 8.5.3.1.1

void get_merge_candidate_list_without_step_9(base_context* ctx,
                                             const slice_segment_header* shdr,
                                             const MotionVectorAccess& mvaccess,
                                             de265_image* img,
                                             int xC,int yC, int xP,int yP,
                                             int nCS, int nPbW,int nPbH, int partIdx,
                                             int max_merge_idx,
                                             PBMotion* mergeCandList)
{

  //int xOrigP = xP;
  //int yOrigP = yP;
  int nOrigPbW = nPbW;
  int nOrigPbH = nPbH;

  int singleMCLFlag; // single merge-candidate-list (MCL) flag

  /* Use single MCL for CBs of size 8x8, except when parallel-merge-level is at 4x4.
     Without this flag, PBs smaller than 8x8 would not receive as much merging candidates.
     Having additional candidates might have these advantages:
     - coding MVs for these small PBs is expensive, and
     - since the PBs are not far away from a proper (neighboring) merging candidate,
     the quality of the candidates will still be good.
  */
  singleMCLFlag = (img->get_pps().log2_parallel_merge_level > 2 && nCS==8);

  if (singleMCLFlag) {
    xP=xC;
    yP=yC;
    nPbW=nCS;
    nPbH=nCS;
    partIdx=0;
  }

  int maxCandidates = max_merge_idx+1;
  //MotionVectorSpec mergeCandList[5];
  int numMergeCand=0;

  // --- spatial merge candidates

  numMergeCand = derive_spatial_merging_candidates(mvaccess,
                                                   img, xC,yC, nCS, xP,yP, singleMCLFlag,
                                                   nPbW,nPbH,partIdx, mergeCandList,
                                                   maxCandidates);

  // --- collocated merge candidate
  if (numMergeCand < maxCandidates) {
    int refIdxCol[2] = { 0,0 };

    MotionVector mvCol[2];
    uint8_t predFlagLCol[2];
    derive_temporal_luma_vector_prediction(ctx,img,shdr, xP,yP,nPbW,nPbH,
                                           refIdxCol[0],0, &mvCol[0],
                                           &predFlagLCol[0]);

    uint8_t availableFlagCol = predFlagLCol[0];
    predFlagLCol[1] = 0;

    if (shdr->slice_type == SLICE_TYPE_B) {
      derive_temporal_luma_vector_prediction(ctx,img,shdr,
                                             xP,yP,nPbW,nPbH, refIdxCol[1],1, &mvCol[1],
                                             &predFlagLCol[1]);
      availableFlagCol |= predFlagLCol[1];
    }


    if (availableFlagCol) {
      PBMotion* colVec = &mergeCandList[numMergeCand++];

      colVec->mv[0] = mvCol[0];
      colVec->mv[1] = mvCol[1];
      colVec->predFlag[0] = predFlagLCol[0];
      colVec->predFlag[1] = predFlagLCol[1];
      colVec->refIdx[0] = refIdxCol[0];
      colVec->refIdx[1] = refIdxCol[1];
    }
  }


  // --- bipredictive merge candidates ---

  if (shdr->slice_type == SLICE_TYPE_B) {
    derive_combined_bipredictive_merging_candidates(ctx, shdr,
                                                    mergeCandList, &numMergeCand, maxCandidates);
  }


  // --- zero-vector merge candidates ---

  derive_zero_motion_vector_candidates(shdr, mergeCandList, &numMergeCand, maxCandidates);


  logtrace(LogMotion,"mergeCandList:\n");
  for (int i=0;i<shdr->MaxNumMergeCand;i++)
    {
      //logtrace(LogMotion, " %d:%s\n", i, i==merge_idx ? " SELECTED":"");
      logmvcand(mergeCandList[i]);
    }
}



void get_merge_candidate_list(base_context* ctx,
                              const slice_segment_header* shdr,
                              de265_image* img,
                              int xC,int yC, int xP,int yP,
                              int nCS, int nPbW,int nPbH, int partIdx,
                              PBMotion* mergeCandList)
{
  int max_merge_idx = 5-shdr->five_minus_max_num_merge_cand -1;

  get_merge_candidate_list_without_step_9(ctx, shdr,
                                          MotionVectorAccess_de265_image(img), img,
                                          xC,yC,xP,yP,nCS,nPbW,nPbH, partIdx,
                                          max_merge_idx, mergeCandList);

  // 9. for encoder: modify all merge candidates

  for (int i=0;i<=max_merge_idx;i++) {
    if (mergeCandList[i].predFlag[0] &&
        mergeCandList[i].predFlag[1] &&
        nPbW+nPbH==12)
      {
        mergeCandList[i].refIdx[1]   = -1;
        mergeCandList[i].predFlag[1] = 0;
      }
  }
}


void derive_luma_motion_merge_mode(base_context* ctx,
                                   const slice_segment_header* shdr,
                                   de265_image* img,
                                   int xC,int yC, int xP,int yP,
                                   int nCS, int nPbW,int nPbH, int partIdx,
                                   int merge_idx,
                                   PBMotion* out_vi)
{
  PBMotion mergeCandList[5];

  get_merge_candidate_list_without_step_9(ctx, shdr,
                                          MotionVectorAccess_de265_image(img), img,
                                          xC,yC,xP,yP,nCS,nPbW,nPbH, partIdx,
                                          merge_idx, mergeCandList);


  *out_vi = mergeCandList[merge_idx];

  // 8.5.3.1.1 / 9.

  if (out_vi->predFlag[0] && out_vi->predFlag[1] && nPbW+nPbH==12) {
    out_vi->refIdx[1] = -1;
    out_vi->predFlag[1] = 0;
  }
}


// 8.5.3.1.6
void derive_spatial_luma_vector_prediction(base_context* ctx,
                                           de265_image* img,
                                           const slice_segment_header* shdr,
                                           int xC,int yC,int nCS,int xP,int yP,
                                           int nPbW,int nPbH, int X,
                                           int refIdxLX, int partIdx,
                                           uint8_t out_availableFlagLXN[2],
                                           MotionVector out_mvLXN[2])
{
  if (refIdxLX >= MAX_NUM_REF_PICS) {
    ctx->add_warning(DE265_WARNING_INCORRECT_MOTION_VECTOR_SCALING, false);
    img->integrity = INTEGRITY_DECODING_ERRORS;

    out_availableFlagLXN[0] = false;
    out_availableFlagLXN[1] = false;
    out_mvLXN[0] = MotionVector();
    out_mvLXN[1] = MotionVector();
    return;
  }

  int isScaledFlagLX = 0;

  const int A=0;
  const int B=1;

  out_availableFlagLXN[A] = 0;
  out_availableFlagLXN[B] = 0;


  // --- A ---

  // 1.

  int xA[2], yA[2];
  xA[0] = xP-1;
  yA[0] = yP + nPbH;
  xA[1] = xA[0];
  yA[1] = yA[0]-1;

  // 2.

  out_availableFlagLXN[A] = 0;
  out_mvLXN[A].x = 0;
  out_mvLXN[A].y = 0;

  // 3. / 4.

  bool availableA[2];
  availableA[0] = img->available_pred_blk(xC,yC, nCS, xP,yP, nPbW,nPbH,partIdx, xA[0],yA[0]);
  availableA[1] = img->available_pred_blk(xC,yC, nCS, xP,yP, nPbW,nPbH,partIdx, xA[1],yA[1]);

  // 5.

  if (availableA[0] || availableA[1]) {
    isScaledFlagLX = 1;
  }

  // 6.  test A0 and A1  (Ak)

  int refIdxA=-1;

  // the POC we want to reference in this PB
  const de265_image* tmpimg = ctx->get_image(shdr->RefPicList[X][ refIdxLX ]);
  if (tmpimg==NULL) { return; }
  const int referenced_POC = tmpimg->PicOrderCntVal;

  for (int k=0;k<=1;k++) {

    if (availableA[k] &&
        out_availableFlagLXN[A]==0 && // no A?-predictor so far
        img->get_pred_mode(xA[k],yA[k]) != MODE_INTRA) {

      int Y=1-X;

      const PBMotion& vi = img->get_mv_info(xA[k],yA[k]);
      logtrace(LogMotion,"MVP A%d=\n",k);
      logmvcand(vi);

      const de265_image* imgX = NULL;
      if (vi.predFlag[X]) {
        // check for input data validity
        if (vi.refIdx[X]<0 || vi.refIdx[X] >= MAX_NUM_REF_PICS) {
          return;
        }

        imgX = ctx->get_image(shdr->RefPicList[X][ vi.refIdx[X] ]);
      }

      const de265_image* imgY = NULL;
      if (vi.predFlag[Y]) {
        // check for input data validity
        if (vi.refIdx[Y]<0 || vi.refIdx[Y] >= MAX_NUM_REF_PICS) {
          return;
        }

        imgY = ctx->get_image(shdr->RefPicList[Y][ vi.refIdx[Y] ]);
      }

      // check whether the predictor X is available and references the same POC
      if (vi.predFlag[X] && imgX && imgX->PicOrderCntVal == referenced_POC) {

        logtrace(LogMotion,"take A%d/L%d as A candidate with same POC\n",k,X);

        out_availableFlagLXN[A]=1;
        out_mvLXN[A] = vi.mv[X];
        refIdxA = vi.refIdx[X];
      }
      // check whether the other predictor (Y) is available and references the same POC
      else if (vi.predFlag[Y] && imgY && imgY->PicOrderCntVal == referenced_POC) {

        logtrace(LogMotion,"take A%d/L%d as A candidate with same POC\n",k,Y);

        out_availableFlagLXN[A]=1;
        out_mvLXN[A] = vi.mv[Y];
        refIdxA = vi.refIdx[Y];
      }
    }
  }

  // 7. If there is no predictor referencing the same POC, we take any other reference as
  //    long as it is the same type of reference (long-term / short-term)

  for (int k=0 ; k<=1 && out_availableFlagLXN[A]==0 ; k++) {
    int refPicList=-1;

    if (availableA[k] &&
        // TODO: we could remove this call by storing the result of the similar computation above
        img->get_pred_mode(xA[k],yA[k]) != MODE_INTRA) {

      int Y=1-X;

      const PBMotion& vi = img->get_mv_info(xA[k],yA[k]);
      if (vi.predFlag[X]==1 &&
          shdr->LongTermRefPic[X][refIdxLX] == shdr->LongTermRefPic[X][ vi.refIdx[X] ]) {

        logtrace(LogMotion,"take A%D/L%d as A candidate with different POCs\n",k,X);

        out_availableFlagLXN[A]=1;
        out_mvLXN[A] = vi.mv[X];
        refIdxA = vi.refIdx[X];
        refPicList = X;
      }
      else if (vi.predFlag[Y]==1 &&
               shdr->LongTermRefPic[X][refIdxLX] == shdr->LongTermRefPic[Y][ vi.refIdx[Y] ]) {

        logtrace(LogMotion,"take A%d/L%d as A candidate with different POCs\n",k,Y);

        out_availableFlagLXN[A]=1;
        out_mvLXN[A] = vi.mv[Y];
        refIdxA = vi.refIdx[Y];
        refPicList = Y;
      }
    }

    if (out_availableFlagLXN[A]==1) {
      if (refIdxA<0) {
        out_availableFlagLXN[0] = out_availableFlagLXN[1] = false;
        return; // error
      }

      assert(refIdxA>=0);
      assert(refPicList>=0);

      const de265_image* refPicA = ctx->get_image(shdr->RefPicList[refPicList][refIdxA ]);
      const de265_image* refPicX = ctx->get_image(shdr->RefPicList[X         ][refIdxLX]);

      //int picStateA = shdr->RefPicList_PicState[refPicList][refIdxA ];
      //int picStateX = shdr->RefPicList_PicState[X         ][refIdxLX];

      int isLongTermA = shdr->LongTermRefPic[refPicList][refIdxA ];
      int isLongTermX = shdr->LongTermRefPic[X         ][refIdxLX];

      logtrace(LogMotion,"scale MVP A: A-POC:%d X-POC:%d\n",
               refPicA->PicOrderCntVal,refPicX->PicOrderCntVal);

      if (!isLongTermA && !isLongTermX)
      /*
      if (picStateA == UsedForShortTermReference &&
          picStateX == UsedForShortTermReference)
      */
        {
          int distA = img->PicOrderCntVal - refPicA->PicOrderCntVal;
          int distX = img->PicOrderCntVal - referenced_POC;

          if (!scale_mv(&out_mvLXN[A], out_mvLXN[A], distA, distX)) {
            ctx->add_warning(DE265_WARNING_INCORRECT_MOTION_VECTOR_SCALING, false);
            img->integrity = INTEGRITY_DECODING_ERRORS;
          }
        }
    }
  }

  // --- B ---

  // 1.

  int xB[3], yB[3];
  xB[0] = xP+nPbW;
  yB[0] = yP-1;
  xB[1] = xB[0]-1;
  yB[1] = yP-1;
  xB[2] = xP-1;
  yB[2] = yP-1;

  // 2.

  out_availableFlagLXN[B] = 0;
  out_mvLXN[B].x = 0;
  out_mvLXN[B].y = 0;

  // 3. test B0,B1,B2 (Bk)

  int refIdxB=-1;

  bool availableB[3];
  for (int k=0;k<3;k++) {
    availableB[k] = img->available_pred_blk(xC,yC, nCS, xP,yP, nPbW,nPbH,partIdx, xB[k],yB[k]);

    if (availableB[k] && out_availableFlagLXN[B]==0) {

      int Y=1-X;

      const PBMotion& vi = img->get_mv_info(xB[k],yB[k]);
      logtrace(LogMotion,"MVP B%d=\n",k);
      logmvcand(vi);

      const de265_image* imgX = NULL;
      if (vi.predFlag[X]) {
        if (vi.refIdx[X] < 0 || vi.refIdx[X] >= MAX_NUM_REF_PICS) {
          return;
        }

        imgX = ctx->get_image(shdr->RefPicList[X][ vi.refIdx[X] ]);
      }

      const de265_image* imgY = NULL;
      if (vi.predFlag[Y]) {
        if (vi.refIdx[Y] < 0 || vi.refIdx[Y] >= MAX_NUM_REF_PICS) {
          return;
        }

        imgY = ctx->get_image(shdr->RefPicList[Y][ vi.refIdx[Y] ]);
      }

      if (vi.predFlag[X] && imgX && imgX->PicOrderCntVal == referenced_POC) {
        logtrace(LogMotion,"a) take B%d/L%d as B candidate with same POC\n",k,X);

        out_availableFlagLXN[B]=1;
        out_mvLXN[B] = vi.mv[X];
        refIdxB = vi.refIdx[X];
      }
      else if (vi.predFlag[Y] && imgY && imgY->PicOrderCntVal == referenced_POC) {
        logtrace(LogMotion,"b) take B%d/L%d as B candidate with same POC\n",k,Y);

        out_availableFlagLXN[B]=1;
        out_mvLXN[B] = vi.mv[Y];
        refIdxB = vi.refIdx[Y];
      }
    }
  }

  // 4.

  if (isScaledFlagLX==0 &&      // no A predictor,
      out_availableFlagLXN[B])  // but an unscaled B predictor
    {
      // use unscaled B predictor as A predictor

      logtrace(LogMotion,"copy the same-POC B candidate as additional A candidate\n");

      out_availableFlagLXN[A]=1;
      out_mvLXN[A] = out_mvLXN[B];
      refIdxA = refIdxB;
    }

  // 5.

  // If no A predictor, we output the unscaled B as the A predictor (above)
  // and also add a scaled B predictor here.
  // If there is (probably) an A predictor, no differing-POC B predictor is generated.
  if (isScaledFlagLX==0) {
    out_availableFlagLXN[B]=0;

    for (int k=0 ; k<=2 && out_availableFlagLXN[B]==0 ; k++) {
      int refPicList=-1;

      if (availableB[k]) {
        int Y=1-X;

        const PBMotion& vi = img->get_mv_info(xB[k],yB[k]);

        if (vi.refIdx[X] >= MAX_NUM_REF_PICS) {
          img->integrity = INTEGRITY_DECODING_ERRORS;
          ctx->add_warning(DE265_WARNING_NONEXISTING_REFERENCE_PICTURE_ACCESSED, false);
          return; // error   // TODO: we actually should make sure that this is never set to an out-of-range value
        }

        if (vi.predFlag[X]==1 &&
            shdr->LongTermRefPic[X][refIdxLX] == shdr->LongTermRefPic[X][ vi.refIdx[X] ]) {
          out_availableFlagLXN[B]=1;
          out_mvLXN[B] = vi.mv[X];
          refIdxB = vi.refIdx[X];
          refPicList = X;
        }
        else if (vi.predFlag[Y]==1 &&
                 shdr->LongTermRefPic[X][refIdxLX] == shdr->LongTermRefPic[Y][ vi.refIdx[Y] ]) {
          out_availableFlagLXN[B]=1;
          out_mvLXN[B] = vi.mv[Y];
          refIdxB = vi.refIdx[Y];
          refPicList = Y;
        }
      }

      if (out_availableFlagLXN[B]==1) {
        if (refIdxB<0) {
          out_availableFlagLXN[0] = out_availableFlagLXN[1] = false;
          return; // error
        }

        assert(refPicList>=0);
        assert(refIdxB>=0);

        const de265_image* refPicB=ctx->get_image(shdr->RefPicList[refPicList][refIdxB ]);
        const de265_image* refPicX=ctx->get_image(shdr->RefPicList[X         ][refIdxLX]);

        int isLongTermB = shdr->LongTermRefPic[refPicList][refIdxB ];
        int isLongTermX = shdr->LongTermRefPic[X         ][refIdxLX];

        if (refPicB==NULL || refPicX==NULL) {
          img->decctx->add_warning(DE265_WARNING_NONEXISTING_REFERENCE_PICTURE_ACCESSED,false);
          img->integrity = INTEGRITY_DECODING_ERRORS;
        }
        else if (refPicB->PicOrderCntVal != refPicX->PicOrderCntVal &&
                 !isLongTermB && !isLongTermX) {
          int distB = img->PicOrderCntVal - refPicB->PicOrderCntVal;
          int distX = img->PicOrderCntVal - referenced_POC;

          logtrace(LogMotion,"scale MVP B: B-POC:%d X-POC:%d\n",refPicB->PicOrderCntVal,refPicX->PicOrderCntVal);

          if (!scale_mv(&out_mvLXN[B], out_mvLXN[B], distB, distX)) {
            ctx->add_warning(DE265_WARNING_INCORRECT_MOTION_VECTOR_SCALING, false);
            img->integrity = INTEGRITY_DECODING_ERRORS;
          }
        }
      }
    }
  }
}


// 8.5.3.1.5
void fill_luma_motion_vector_predictors(base_context* ctx,
                                        const slice_segment_header* shdr,
                                        de265_image* img,
                                        int xC,int yC,int nCS,int xP,int yP,
                                        int nPbW,int nPbH, int l,
                                        int refIdx, int partIdx,
                                        MotionVector out_mvpList[2])
{
  // 8.5.3.1.6: derive two spatial vector predictors A (0) and B (1)

  uint8_t availableFlagLXN[2];
  MotionVector mvLXN[2];

  derive_spatial_luma_vector_prediction(ctx, img, shdr, xC,yC, nCS, xP,yP,
                                        nPbW,nPbH, l, refIdx, partIdx,
                                        availableFlagLXN, mvLXN);

  // 8.5.3.1.7: if we only have one spatial vector or both spatial vectors are the same,
  // derive a temporal predictor

  uint8_t availableFlagLXCol;
  MotionVector mvLXCol;


  if (availableFlagLXN[0] &&
      availableFlagLXN[1] &&
      (mvLXN[0].x != mvLXN[1].x || mvLXN[0].y != mvLXN[1].y)) {
    availableFlagLXCol = 0;
  }
  else {
    derive_temporal_luma_vector_prediction(ctx, img, shdr,
                                           xP,yP, nPbW,nPbH, refIdx,l,
                                           &mvLXCol, &availableFlagLXCol);
  }


  // --- build candidate vector list with exactly two entries ---

  int numMVPCandLX=0;

  // spatial predictor A

  if (availableFlagLXN[0])
    {
      out_mvpList[numMVPCandLX++] = mvLXN[0];
    }

  // spatial predictor B (if not same as A)

  if (availableFlagLXN[1] &&
      (!availableFlagLXN[0] || // in case A in not available, but mvLXA initialized to same as mvLXB
       (mvLXN[0].x != mvLXN[1].x || mvLXN[0].y != mvLXN[1].y)))
    {
      out_mvpList[numMVPCandLX++] = mvLXN[1];
    }

  // temporal predictor

  if (availableFlagLXCol)
    {
      out_mvpList[numMVPCandLX++] = mvLXCol;
    }

  // fill with zero predictors

  while (numMVPCandLX<2) {
    out_mvpList[numMVPCandLX].x = 0;
    out_mvpList[numMVPCandLX].y = 0;
    numMVPCandLX++;
  }


  assert(numMVPCandLX==2);
}


MotionVector luma_motion_vector_prediction(base_context* ctx,
                                           const slice_segment_header* shdr,
                                           de265_image* img,
                                           const PBMotionCoding& motion,
                                           int xC,int yC,int nCS,int xP,int yP,
                                           int nPbW,int nPbH, int l,
                                           int refIdx, int partIdx)
{
  MotionVector mvpList[2];

  fill_luma_motion_vector_predictors(ctx, shdr, img,
                                     xC,yC,nCS,xP,yP,
                                     nPbW, nPbH, l, refIdx, partIdx,
                                     mvpList);

  // select predictor according to mvp_lX_flag

  return mvpList[ l ? motion.mvp_l1_flag : motion.mvp_l0_flag ];
}


#if DE265_LOG_TRACE
void logMV(int x0,int y0,int nPbW,int nPbH, const char* mode,const PBMotion* mv)
{
  int pred0 = mv->predFlag[0];
  int pred1 = mv->predFlag[1];

  logtrace(LogMotion,
           "*MV %d;%d [%d;%d] %s: (%d) %d;%d @%d   (%d) %d;%d @%d\n", x0,y0,nPbW,nPbH,mode,
           pred0,
           pred0 ? mv->mv[0].x : 0,pred0 ? mv->mv[0].y : 0, pred0 ? mv->refIdx[0] : 0,
           pred1,
           pred1 ? mv->mv[1].x : 0,pred1 ? mv->mv[1].y : 0, pred1 ? mv->refIdx[1] : 0);
}
#else
#define logMV(x0,y0,nPbW,nPbH,mode,mv)
#endif



// 8.5.3.1
void motion_vectors_and_ref_indices(base_context* ctx,
                                    const slice_segment_header* shdr,
                                    de265_image* img,
                                    const PBMotionCoding& motion,
                                    int xC,int yC, int xB,int yB, int nCS, int nPbW,int nPbH,
                                    int partIdx,
                                    PBMotion* out_vi)
{
  //slice_segment_header* shdr = tctx->shdr;

  int xP = xC+xB;
  int yP = yC+yB;

  enum PredMode predMode = img->get_pred_mode(xC,yC);

  if (predMode == MODE_SKIP ||
      (predMode == MODE_INTER && motion.merge_flag))
    {
      derive_luma_motion_merge_mode(ctx,shdr,img,
                                    xC,yC, xP,yP, nCS,nPbW,nPbH, partIdx,
                                    motion.merge_idx, out_vi);

      logMV(xP,yP,nPbW,nPbH, "merge_mode", out_vi);
    }
  else {
    int mvdL[2][2];
    MotionVector mvpL[2];

    for (int l=0;l<2;l++) {
      // 1.

      enum InterPredIdc inter_pred_idc = (enum InterPredIdc)motion.inter_pred_idc;

      if (inter_pred_idc == PRED_BI ||
          (inter_pred_idc == PRED_L0 && l==0) ||
          (inter_pred_idc == PRED_L1 && l==1)) {
        out_vi->refIdx[l] = motion.refIdx[l];
        out_vi->predFlag[l] = 1;

        if (motion.refIdx[l] >= MAX_NUM_REF_PICS) {
          out_vi->refIdx[l] = 0;

          img->integrity = INTEGRITY_DECODING_ERRORS;
          ctx->add_warning(DE265_WARNING_NONEXISTING_REFERENCE_PICTURE_ACCESSED, false);
          return;
        }
      }
      else {
        out_vi->refIdx[l] = -1;
        out_vi->predFlag[l] = 0;
      }

      // 2.

      mvdL[l][0] = motion.mvd[l][0];
      mvdL[l][1] = motion.mvd[l][1];


      if (out_vi->predFlag[l]) {
        // 3.

        mvpL[l] = luma_motion_vector_prediction(ctx,shdr,img,motion,
                                                xC,yC,nCS,xP,yP, nPbW,nPbH, l,
                                                out_vi->refIdx[l], partIdx);

        // 4.

        int32_t x = (mvpL[l].x + mvdL[l][0] + 0x10000) & 0xFFFF;
        int32_t y = (mvpL[l].y + mvdL[l][1] + 0x10000) & 0xFFFF;

        out_vi->mv[l].x = (x>=0x8000) ? x-0x10000 : x;
        out_vi->mv[l].y = (y>=0x8000) ? y-0x10000 : y;
      }
    }

    logMV(xP,yP,nPbW,nPbH, "mvp", out_vi);
  }
}


// 8.5.3

/* xC/yC : CB position
   xB/yB : position offset of the PB
   nPbW/nPbH : size of PB
   nCS   : CB size
 */
void decode_prediction_unit(base_context* ctx,
                            const slice_segment_header* shdr,
                            de265_image* img,
                            const PBMotionCoding& motion,
                            int xC,int yC, int xB,int yB, int nCS, int nPbW,int nPbH, int partIdx)
{
  logtrace(LogMotion,"decode_prediction_unit POC=%d %d;%d %dx%d\n",
           img->PicOrderCntVal, xC+xB,yC+yB, nPbW,nPbH);

  //slice_segment_header* shdr = tctx->shdr;

  // 1.

  PBMotion vi;
  motion_vectors_and_ref_indices(ctx, shdr, img, motion,
                                 xC,yC, xB,yB, nCS, nPbW,nPbH, partIdx, &vi);

  // 2.

  generate_inter_prediction_samples(ctx,shdr, img, xC,yC, xB,yB, nCS, nPbW,nPbH, &vi);


  img->set_mv_info(xC+xB,yC+yB,nPbW,nPbH, vi);
}
