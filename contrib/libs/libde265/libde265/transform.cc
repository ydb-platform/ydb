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

#include "transform.h"
#include "util.h"

#include <assert.h>


const int tab8_22[] = { 29,30,31,32,33,33,34,34,35,35,36,36,37 /*,37*/ };


// (8.6.1)
void decode_quantization_parameters(thread_context* tctx, int xC,int yC,
                                    int xCUBase, int yCUBase)
{
  logtrace(LogTransform,">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> decode_quantization_parameters(int xC,int yC)=(%d,%d)\n", xC,yC);

  const pic_parameter_set& pps = tctx->img->get_pps();
  const seq_parameter_set& sps = tctx->img->get_sps();
  slice_segment_header* shdr = tctx->shdr;

  // top left pixel position of current quantization group
  int xQG = xCUBase - (xCUBase & ((1<<pps.Log2MinCuQpDeltaSize)-1));
  int yQG = yCUBase - (yCUBase & ((1<<pps.Log2MinCuQpDeltaSize)-1));

  logtrace(LogTransform,"QG: %d,%d\n",xQG,yQG);


  // we only have to set QP in the first call in a quantization-group

  /* TODO: check why this does not work with HoneyBee stream

  if (xQG == tctx->currentQG_x &&
      yQG == tctx->currentQG_y)
    {
      return;
    }
  */

  // if first QG in CU, remember last QPY of last CU previous QG

  if (xQG != tctx->currentQG_x ||
      yQG != tctx->currentQG_y)
    {
      tctx->lastQPYinPreviousQG = tctx->currentQPY;
      tctx->currentQG_x = xQG;
      tctx->currentQG_y = yQG;
    }

  int qPY_PRED;

  // first QG in CTB row ?

  int ctbLSBMask = ((1<<sps.Log2CtbSizeY)-1);
  bool firstInCTBRow = (xQG == 0 && ((yQG & ctbLSBMask)==0));

  // first QG in slice ?    TODO: a "firstQG" flag in the thread context would be faster

  int first_ctb_in_slice_RS = tctx->shdr->SliceAddrRS;

  int SliceStartX = (first_ctb_in_slice_RS % sps.PicWidthInCtbsY) * sps.CtbSizeY;
  int SliceStartY = (first_ctb_in_slice_RS / sps.PicWidthInCtbsY) * sps.CtbSizeY;

  bool firstQGInSlice = (SliceStartX == xQG && SliceStartY == yQG);

  // first QG in tile ?

  bool firstQGInTile = false;
  if (pps.tiles_enabled_flag) {
    if ((xQG & ((1 << sps.Log2CtbSizeY)-1)) == 0 &&
        (yQG & ((1 << sps.Log2CtbSizeY)-1)) == 0)
      {
        int ctbX = xQG >> sps.Log2CtbSizeY;
        int ctbY = yQG >> sps.Log2CtbSizeY;

        firstQGInTile = pps.is_tile_start_CTB(ctbX,ctbY); // TODO: this is slow
      }
  }


  if (firstQGInSlice || firstQGInTile ||
      (firstInCTBRow && pps.entropy_coding_sync_enabled_flag)) {
    qPY_PRED = tctx->shdr->SliceQPY;
  }
  else {
    qPY_PRED = tctx->lastQPYinPreviousQG;
  }


  int qPYA,qPYB;

  if (tctx->img->available_zscan(xQG,yQG, xQG-1,yQG)) {
    int xTmp = (xQG-1) >> sps.Log2MinTrafoSize;
    int yTmp = (yQG  ) >> sps.Log2MinTrafoSize;
    int minTbAddrA = pps.MinTbAddrZS[xTmp + yTmp*sps.PicWidthInTbsY];
    int ctbAddrA = minTbAddrA >> (2 * (sps.Log2CtbSizeY-sps.Log2MinTrafoSize));
    if (ctbAddrA == tctx->CtbAddrInTS) {
      qPYA = tctx->img->get_QPY(xQG-1,yQG);
    }
    else {
      qPYA = qPY_PRED;
    }
  }
  else {
    qPYA = qPY_PRED;
  }

  if (tctx->img->available_zscan(xQG,yQG, xQG,yQG-1)) {
    int xTmp = (xQG  ) >> sps.Log2MinTrafoSize;
    int yTmp = (yQG-1) >> sps.Log2MinTrafoSize;
    int minTbAddrB = pps.MinTbAddrZS[xTmp + yTmp*sps.PicWidthInTbsY];
    int ctbAddrB = minTbAddrB >> (2 * (sps.Log2CtbSizeY-sps.Log2MinTrafoSize));
    if (ctbAddrB == tctx->CtbAddrInTS) {
      qPYB = tctx->img->get_QPY(xQG,yQG-1);
    }
    else {
      qPYB = qPY_PRED;
    }
  }
  else {
    qPYB = qPY_PRED;
  }

  qPY_PRED = (qPYA + qPYB + 1)>>1;

  logtrace(LogTransform,"qPY_PRED = %d  (%d, %d)\n",qPY_PRED, qPYA, qPYB);

  int QPY = ((qPY_PRED + tctx->CuQpDelta + 52+2*sps.QpBdOffset_Y) %
             (52 + sps.QpBdOffset_Y)) - sps.QpBdOffset_Y;

  tctx->qPYPrime = QPY + sps.QpBdOffset_Y;
  if (tctx->qPYPrime<0) {
    tctx->qPYPrime=0;
  }

  int qPiCb = Clip3(-sps.QpBdOffset_C,57, QPY+pps.pic_cb_qp_offset + shdr->slice_cb_qp_offset + tctx->CuQpOffsetCb);
  int qPiCr = Clip3(-sps.QpBdOffset_C,57, QPY+pps.pic_cr_qp_offset + shdr->slice_cr_qp_offset + tctx->CuQpOffsetCr);

  logtrace(LogTransform,"qPiCb:%d (%d %d), qPiCr:%d (%d %d)\n",
           qPiCb, pps.pic_cb_qp_offset, shdr->slice_cb_qp_offset,
           qPiCr, pps.pic_cr_qp_offset, shdr->slice_cr_qp_offset);

  int qPCb,qPCr;

  if (sps.ChromaArrayType == CHROMA_420) {
    qPCb = table8_22(qPiCb);
    qPCr = table8_22(qPiCr);
  }
  else {
    qPCb = qPiCb;
    qPCr = qPiCr;
  }

  //printf("q: %d %d\n",qPiCb, qPCb);

  tctx->qPCbPrime = qPCb + sps.QpBdOffset_C;
  if (tctx->qPCbPrime<0) {
    tctx->qPCbPrime = 0;
  }

  tctx->qPCrPrime = qPCr + sps.QpBdOffset_C;
  if (tctx->qPCrPrime<0) {
    tctx->qPCrPrime = 0;
  }

  /*
  printf("Q: %d (%d %d %d / %d %d) %d %d %d\n",QPY,
         sps->QpBdOffset_Y,
         pps->pic_cb_qp_offset + shdr->slice_cb_qp_offset,
         pps->pic_cr_qp_offset + shdr->slice_cr_qp_offset,
         sps->QpBdOffset_C, sps->QpBdOffset_C,
         tctx->qPYPrime, tctx->qPCbPrime, tctx->qPCrPrime);
  */

  int log2CbSize = tctx->img->get_log2CbSize(xCUBase, yCUBase);

  // TODO: On broken input, log2CbSize may be zero (multithreaded only). Not sure yet why.
  // Maybe another decoding thread is overwriting the value set in slice.cc:read_coding_unit.
  // id:000163,sig:06,src:002041,op:havoc,rep:16.bin
  if (log2CbSize<3) { log2CbSize=3; }

  tctx->img->set_QPY(xCUBase, yCUBase, log2CbSize, QPY);
  tctx->currentQPY = QPY;

  /*
  printf("SET QPY POC=%d %d;%d-%d;%d = %d\n",ctx->img->PicOrderCntVal,xCUBase,yCUBase,
         xCUBase+(1<<log2CbSize),yCUBase+(1<<log2CbSize), QPY);
  */

  logtrace(LogTransform,"qPY(%d,%d,%d)= %d, qPYPrime=%d\n",
           xCUBase,yCUBase,1<<log2CbSize,QPY,tctx->qPYPrime);
}



template <class pixel_t>
void transform_coefficients(acceleration_functions* acceleration,
                            int16_t* coeff, int coeffStride, int nT, int trType,
                            pixel_t* dst, int dstStride, int bit_depth)
{
  logtrace(LogTransform,"transform --- trType: %d nT: %d\n",trType,nT);


  if (trType==1) {

    acceleration->transform_4x4_dst_add<pixel_t>(dst, coeff, dstStride, bit_depth);

  } else {

    /**/ if (nT==4)  { acceleration->transform_add<pixel_t>(0,dst,coeff,dstStride, bit_depth); }
    else if (nT==8)  { acceleration->transform_add<pixel_t>(1,dst,coeff,dstStride, bit_depth); }
    else if (nT==16) { acceleration->transform_add<pixel_t>(2,dst,coeff,dstStride, bit_depth); }
    else             { acceleration->transform_add<pixel_t>(3,dst,coeff,dstStride, bit_depth); }
  }

#if 0
  printf("decoded pixels:\n");
  for (int y=0;y<nT;y++,printf("\n"))
    for (int x=0;x<nT;x++) {
      printf("%02x ",dst[y*dstStride+x]);
    }
#endif
}


// TODO: make this an accelerated function
void cross_comp_pred(const thread_context* tctx, int32_t* residual, int nT)
{
  const int BitDepthC = tctx->img->get_sps().BitDepth_C;
  const int BitDepthY = tctx->img->get_sps().BitDepth_Y;

  for (int y=0;y<nT;y++)
    for (int x=0;x<nT;x++) {
      /* TODO: the most usual case is definitely BitDepthY == BitDepthC, in which case
         we could just omit two shifts. The second most common case is probably
         BitDepthY>BitDepthC, for which we could also eliminate one shift. The remaining
         case is also one shift only.
      */

      residual[y*nT+x] += (tctx->ResScaleVal *
                           ((tctx->residual_luma[y*nT+x] << BitDepthC ) >> BitDepthY ) ) >> 3;
    }
}


template <class pixel_t>
void transform_coefficients_explicit(thread_context* tctx,
                                     int16_t* coeff, int coeffStride, int nT, int trType,
                                     pixel_t* dst, int dstStride, int bit_depth, int cIdx)
{
  logtrace(LogTransform,"transform --- trType: %d nT: %d\n",trType,nT);

  const acceleration_functions* acceleration = &tctx->decctx->acceleration;

  int32_t residual_buffer[32*32];
  int32_t* residual;
  if (cIdx==0) {
    residual = tctx->residual_luma;
  }
  else {
    residual = residual_buffer;
  }


  // TODO
  int bdShift = 20 - bit_depth;
  int max_coeff_bits = 15;

  if (trType==1) {

    acceleration->transform_idst_4x4(residual, coeff, bdShift, max_coeff_bits);

  } else {

    /**/ if (nT==4)  { acceleration->transform_idct_4x4(residual,coeff,bdShift,max_coeff_bits); }
    else if (nT==8)  { acceleration->transform_idct_8x8(residual,coeff,bdShift,max_coeff_bits); }
    else if (nT==16) { acceleration->transform_idct_16x16(residual,coeff,bdShift,max_coeff_bits); }
    else             { acceleration->transform_idct_32x32(residual,coeff,bdShift,max_coeff_bits); }
  }


  //printBlk("prediction",(uint8_t*)dst,nT,dstStride);
  //printBlk("residual",residual,nT,nT);

  if (cIdx != 0) {
    if (tctx->ResScaleVal != 0) {
      cross_comp_pred(tctx, residual, nT);
    }

    //printBlk("cross-comp-pred modified residual",residual,nT,nT);
  }

  acceleration->add_residual(dst,dstStride, residual,nT, bit_depth);
}


void inv_transform(acceleration_functions* acceleration,
                   uint8_t* dst, int dstStride, int16_t* coeff,
                   int log2TbSize, int trType)
{
  if (trType==1) {
    assert(log2TbSize==2);

    acceleration->transform_4x4_dst_add_8(dst, coeff, dstStride);

  } else {
    acceleration->transform_add_8[log2TbSize-2](dst,coeff,dstStride);
  }


#if 0
  int nT = 1<<log2TbSize;
  printf("decoded pixels:\n");
  for (int y=0;y<nT;y++,printf("\n"))
    for (int x=0;x<nT;x++) {
  printf("%02x ",dst[y*dstStride+x]);
}
#endif
}


void fwd_transform(acceleration_functions* acceleration,
                   int16_t* coeff, int coeffStride, int log2TbSize, int trType,
                   const int16_t* src, int srcStride)
{
  logtrace(LogTransform,"transform --- trType: %d nT: %d\n",trType,1<<log2TbSize);

  if (trType==1) {
    // DST 4x4

    acceleration->fwd_transform_4x4_dst_8(coeff, src, srcStride);
  } else {
    // DCT 4x4, 8x8, 16x16, 32x32

    acceleration->fwd_transform_8[log2TbSize-2](coeff,src,srcStride);
  }
}



static const int levelScale[] = { 40,45,51,57,64,72 };

// (8.6.2) and (8.6.3)
template <class pixel_t>
void scale_coefficients_internal(thread_context* tctx,
                                 int xT,int yT, // position of TU in frame (chroma adapted)
                                 int x0,int y0, // position of CU in frame (chroma adapted)
                                 int nT, int cIdx,
                                 bool transform_skip_flag, bool intra, int rdpcmMode)
{
  const seq_parameter_set& sps = tctx->img->get_sps();
  const pic_parameter_set& pps = tctx->img->get_pps();

  int qP;
  switch (cIdx) {
  case 0: qP = tctx->qPYPrime;  break;
  case 1: qP = tctx->qPCbPrime; break;
  case 2: qP = tctx->qPCrPrime; break;
  default: qP = 0; assert(0); break; // should never happen
  }

  logtrace(LogTransform,"qP: %d\n",qP);


  int16_t* coeff;
  int      coeffStride;

  coeff = tctx->coeffBuf;
  coeffStride = nT;





  pixel_t* pred;
  int      stride;
  pred = tctx->img->get_image_plane_at_pos_NEW<pixel_t>(cIdx, xT,yT);
  stride = tctx->img->get_image_stride(cIdx);

  // We explicitly include the case for sizeof(pixel_t)==1 so that the compiler
  // can optimize away a lot of code for 8-bit pixels.
  const int bit_depth = ((sizeof(pixel_t)==1) ? 8 : sps.get_bit_depth(cIdx));

  //assert(intra == (tctx->img->get_pred_mode(xT,yT)==MODE_INTRA));
  int cuPredModeIntra = (tctx->img->get_pred_mode(xT,yT)==MODE_INTRA);

  bool rotateCoeffs = (sps.range_extension.transform_skip_rotation_enabled_flag &&
                       nT == 4 &&
                       cuPredModeIntra);

  if (tctx->cu_transquant_bypass_flag) {

    int32_t residual_buffer[32*32];

    int32_t* residual;
    if (cIdx==0) residual = tctx->residual_luma;
    else         residual = residual_buffer;


    // TODO: we could fold the coefficient rotation into the coefficient expansion here:
    for (int i=0;i<tctx->nCoeff[cIdx];i++) {
      int32_t currCoeff = tctx->coeffList[cIdx][i];
      tctx->coeffBuf[ tctx->coeffPos[cIdx][i] ] = currCoeff;
    }

    if (rotateCoeffs) {
      tctx->decctx->acceleration.rotate_coefficients(coeff, nT);
    }

    if (rdpcmMode) {
      if (rdpcmMode==2)
        tctx->decctx->acceleration.transform_bypass_rdpcm_v(residual, coeff, nT);
      else
        tctx->decctx->acceleration.transform_bypass_rdpcm_h(residual, coeff, nT);
    }
    else {
      tctx->decctx->acceleration.transform_bypass(residual, coeff, nT);
    }

    if (cIdx != 0) {
      if (tctx->ResScaleVal != 0) {
        cross_comp_pred(tctx, residual, nT);
      }
    }

    tctx->decctx->acceleration.add_residual(pred,stride, residual,nT, bit_depth);

    if (rotateCoeffs) {
      memset(coeff, 0, nT*nT*sizeof(int16_t)); // delete all, because we moved the coeffs around
    }
  }
  else {
    // (8.6.3)

    int bdShift = (cIdx==0 ? sps.BitDepth_Y : sps.BitDepth_C) + Log2(nT) - 5;

    logtrace(LogTransform,"bdShift=%d\n",bdShift);

    logtrace(LogTransform,"dequant %d;%d cIdx=%d qp=%d\n",xT*(cIdx?2:1),yT*(cIdx?2:1),cIdx,qP);


    // --- inverse quantization ---

    if (sps.scaling_list_enable_flag==0) {

      //const int m_x_y = 16;
      const int m_x_y = 1;
      bdShift -= 4;  // this is equivalent to having a m_x_y of 16 and we can use 32bit integers

      const int offset = (1<<(bdShift-1));
      const int fact = m_x_y * levelScale[qP%6] << (qP/6);

      for (int i=0;i<tctx->nCoeff[cIdx];i++) {

        // usually, this needs to be 64bit, but because we modify the shift above, we can use 16 bit
        int32_t currCoeff  = tctx->coeffList[cIdx][i];

        //logtrace(LogTransform,"coefficient[%d] = %d\n",tctx->coeffPos[cIdx][i],
        //tctx->coeffList[cIdx][i]);

        currCoeff = Clip3(-32768,32767,
                          ( (currCoeff * fact + offset ) >> bdShift));

        //logtrace(LogTransform," -> %d\n",currCoeff);

        tctx->coeffBuf[ tctx->coeffPos[cIdx][i] ] = currCoeff;
      }
    }
    else {
      const int offset = (1<<(bdShift-1));

      const uint8_t* sclist;
      int matrixID = cIdx;

      if (nT==32) {
        matrixID=0;
      }

      if (!intra) {
        if (nT<32) { matrixID += 3; }
        else { matrixID++; }
      }

      switch (nT) {
      case  4: sclist = &pps.scaling_list.ScalingFactor_Size0[matrixID][0][0]; break;
      case  8: sclist = &pps.scaling_list.ScalingFactor_Size1[matrixID][0][0]; break;
      case 16: sclist = &pps.scaling_list.ScalingFactor_Size2[matrixID][0][0]; break;
      case 32: sclist = &pps.scaling_list.ScalingFactor_Size3[matrixID][0][0]; break;
      default: assert(0);
      }

      for (int i=0;i<tctx->nCoeff[cIdx];i++) {
        int pos = tctx->coeffPos[cIdx][i];

        const int m_x_y = sclist[pos];
        const int fact = m_x_y * levelScale[qP%6] << (qP/6);

        int64_t currCoeff  = tctx->coeffList[cIdx][i];

        currCoeff = Clip3(-32768,32767,
                          ( (currCoeff * fact + offset ) >> bdShift));

        tctx->coeffBuf[ tctx->coeffPos[cIdx][i] ] = currCoeff;
      }
    }


    // --- do transform or skip ---

    logtrace(LogTransform,"coefficients OUT:\n");
    for (int y=0;y<nT;y++) {
      logtrace(LogTransform,"  ");
      for (int x=0;x<nT;x++) {
        logtrace(LogTransform,"*%3d ", coeff[x+y*coeffStride]);
      }
      logtrace(LogTransform,"*\n");
    }

    int bdShift2 = (cIdx==0) ? 20-sps.BitDepth_Y : 20-sps.BitDepth_C;

    logtrace(LogTransform,"bdShift2=%d\n",bdShift2);

    logtrace(LogSlice,"get_transform_skip_flag(%d,%d, cIdx=%d)=%d\n",xT,yT,cIdx,
             transform_skip_flag);

    if (transform_skip_flag) {

      int extended_precision_processing_flag = 0;
      int Log2nTbS = Log2(nT);
      int bdShift = libde265_max( 20 - bit_depth, extended_precision_processing_flag ? 11 : 0 );
      int tsShift = (extended_precision_processing_flag ? libde265_min( 5, bdShift - 2 ) : 5 )
        + Log2nTbS;

      if (rotateCoeffs) {
        tctx->decctx->acceleration.rotate_coefficients(coeff, nT);
      }

      int32_t residual_buffer[32*32];

      int32_t* residual;
      if (cIdx==0) residual = tctx->residual_luma;
      else         residual = residual_buffer;

      if (rdpcmMode) {
        /*
        if (rdpcmMode==2)
          tctx->decctx->acceleration.transform_skip_rdpcm_v(pred,coeff, Log2(nT), stride, bit_depth);
        else
          tctx->decctx->acceleration.transform_skip_rdpcm_h(pred,coeff, Log2(nT), stride, bit_depth);
        */

        if (rdpcmMode==2)
          tctx->decctx->acceleration.rdpcm_v(residual, coeff,nT, tsShift,bdShift);
        else
          tctx->decctx->acceleration.rdpcm_h(residual, coeff,nT, tsShift,bdShift);
      }
      else {
        //tctx->decctx->acceleration.transform_skip(pred, coeff, stride, bit_depth);

        tctx->decctx->acceleration.transform_skip_residual(residual, coeff, nT, tsShift, bdShift);
      }

      if (cIdx != 0) {
        if (tctx->ResScaleVal != 0) {
          cross_comp_pred(tctx, residual, nT);
        }
      }

      tctx->decctx->acceleration.add_residual(pred,stride, residual,nT, bit_depth);

      if (rotateCoeffs) {
        memset(coeff, 0, nT*nT*sizeof(int16_t)); // delete all, because we moved the coeffs around
      }
    }
    else {
      int trType;

      //if (nT==4 && cIdx==0 && tctx->img->get_pred_mode(xT,yT)==MODE_INTRA) {
      if (nT==4 && cIdx==0 && cuPredModeIntra) {
        trType=1;
      }
      else {
        trType=0;
      }

      assert(rdpcmMode==0);


      if (tctx->img->get_pps().range_extension.cross_component_prediction_enabled_flag) {
        // cross-component-prediction: transform to residual buffer and add in a separate step

        transform_coefficients_explicit(tctx, coeff, coeffStride, nT, trType,
                                        pred, stride, bit_depth, cIdx);
      }
      else {
        transform_coefficients(&tctx->decctx->acceleration, coeff, coeffStride, nT, trType,
                               pred, stride, bit_depth);
      }
    }
  }


  logtrace(LogTransform,"pixels (cIdx:%d), position %d %d:\n",cIdx, xT,yT);

  for (int y=0;y<nT;y++) {
    logtrace(LogTransform,"RECO-%3d-%3d-%d ",xT,yT+y,cIdx);

    for (int x=0;x<nT;x++) {
      logtrace(LogTransform,"*%03x ", pred[x+y*stride]);
    }

    logtrace(LogTransform,"*\n");
  }

  // zero out scrap coefficient buffer again

  for (int i=0;i<tctx->nCoeff[cIdx];i++) {
    tctx->coeffBuf[ tctx->coeffPos[cIdx][i] ] = 0;
  }
}


void scale_coefficients(thread_context* tctx,
                        int xT,int yT, // position of TU in frame (chroma adapted)
                        int x0,int y0, // position of CU in frame (chroma adapted)
                        int nT, int cIdx,
                        bool transform_skip_flag, bool intra,
                        int rdpcmMode // 0 - off, 1 - Horizontal, 2 - Vertical
                        )
{
  if (tctx->img->high_bit_depth(cIdx)) {
    scale_coefficients_internal<uint16_t>(tctx, xT,yT, x0,y0, nT,cIdx, transform_skip_flag, intra,
                                          rdpcmMode);
  } else {
    scale_coefficients_internal<uint8_t> (tctx, xT,yT, x0,y0, nT,cIdx, transform_skip_flag, intra,
                                          rdpcmMode);
  }
}


//#define QUANT_IQUANT_SHIFT    20 // Q(QP%6) * IQ(QP%6) = 2^20
#define QUANT_SHIFT           14 // Q(4) = 2^14
//#define SCALE_BITS            15 // Inherited from TMuC, presumably for fractional bit estimates in RDOQ
#define MAX_TR_DYNAMIC_RANGE  15 // Maximum transform dynamic range (excluding sign bit)


const static uint16_t g_quantScales[6] = {
  26214,23302,20560,18396,16384,14564
};

void quant_coefficients(//encoder_context* ectx,
                        int16_t* out_coeff,
                        const int16_t* in_coeff,
                        int log2TrSize, int qp,
                        bool intra)
{
  const int qpDiv6 = qp / 6;
  const int qpMod6 = qp % 6;

  //int uiLog2TrSize = xLog2( iWidth - 1);

  int uiQ = g_quantScales[qpMod6];
  int bitDepth = 8;
  int transformShift = MAX_TR_DYNAMIC_RANGE - bitDepth - log2TrSize;  // Represents scaling through forward transform
  int qBits = QUANT_SHIFT + qpDiv6 + transformShift;

  /* TODO: originally, this was checking for intra slices, why not for intra mode ?
   */
  int rnd = (intra ? 171 : 85) << (qBits-9);

  int x, y;
  int uiAcSum = 0;

  int nStride = (1<<log2TrSize);

  for (y=0; y < (1<<log2TrSize) ; y++) {
    for (x=0; x < (1<<log2TrSize) ; x++) {
      int level;
      int sign;
      int blockPos = y * nStride + x;
      level  = in_coeff[blockPos];
      //logtrace(LogTransform,"(%d,%d) %d -> ", x,y,level);
      sign   = (level < 0 ? -1: 1);

      level = (abs_value(level) * uiQ + rnd ) >> qBits;
      uiAcSum += level;
      level *= sign;
      out_coeff[blockPos] = Clip3(-32768, 32767, level);
      //logtrace(LogTransform,"%d\n", out_coeff[blockPos]);
    }
  }
}


void dequant_coefficients(int16_t* out_coeff,
                          const int16_t* in_coeff,
                          int log2TrSize, int qP)
{
  const int m_x_y = 1;
  int bitDepth = 8;
  int bdShift = bitDepth + log2TrSize - 5;
  bdShift -= 4;  // this is equivalent to having a m_x_y of 16 and we can use 32bit integers

  const int offset = (1<<(bdShift-1));
  const int fact = m_x_y * levelScale[qP%6] << (qP/6);

  int blkSize = (1<<log2TrSize);
  int nCoeff  = (1<<(log2TrSize<<1));

  for (int i=0;i<nCoeff;i++) {

    // usually, this needs to be 64bit, but because we modify the shift above, we can use 16 bit
    int32_t currCoeff  = in_coeff[i];

    //logtrace(LogTransform,"coefficient[%d] = %d\n",i,currCoeff);

    currCoeff = Clip3(-32768,32767,
                      ( (currCoeff * fact + offset ) >> bdShift));

    //logtrace(LogTransform," -> %d\n",currCoeff);

    out_coeff[i] = currCoeff;
  }
}
