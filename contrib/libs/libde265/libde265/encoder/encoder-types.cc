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

#include "encoder-types.h"
#include "encoder-context.h"
#include "slice.h"
#include "scan.h"
#include "intrapred.h"
#include "libde265/transform.h"
#include "libde265/fallback-dct.h"
#include <iostream>


int allocTB = 0;
int allocCB = 0;

#define DEBUG_ALLOCS 0


small_image_buffer::small_image_buffer(int log2Size,int bytes_per_pixel)
{
  mWidth  = 1<<log2Size;
  mHeight = 1<<log2Size;
  mStride = 1<<log2Size;
  mBytesPerRow = bytes_per_pixel * (1<<log2Size);

  int nBytes = mWidth*mHeight*bytes_per_pixel;
  mBuf = new uint8_t[nBytes];
}


small_image_buffer::~small_image_buffer()
{
  delete[] mBuf;
}


void enc_cb::set_rqt_root_bf_from_children_cbf()
{
  assert(transform_tree);
  inter.rqt_root_cbf = (transform_tree->cbf[0] |
                        transform_tree->cbf[1] |
                        transform_tree->cbf[2]);
}




alloc_pool enc_tb::mMemPool(sizeof(enc_tb));

enc_tb::enc_tb(int x,int y,int log2TbSize, enc_cb* _cb)
  : enc_node(x,y,log2TbSize)
{
  parent = NULL;
  cb = _cb;
  downPtr = NULL;
  blkIdx = 0;

  split_transform_flag = false;
  coeff[0]=coeff[1]=coeff[2]=NULL;

  TrafoDepth = 0;
  cbf[0] = cbf[1] = cbf[2] = 0;

  distortion = 0;
  rate = 0;
  rate_withoutCbfChroma = 0;

  intra_mode = INTRA_PLANAR;
  intra_mode_chroma = INTRA_PLANAR;

  if (DEBUG_ALLOCS) { allocTB++; printf("TB  : %d\n",allocTB); }
}


enc_tb::~enc_tb()
{
  if (split_transform_flag) {
    for (int i=0;i<4;i++) {
      delete children[i];
    }
  }
  else {
    for (int i=0;i<3;i++) {
      delete[] coeff[i];
    }
  }

  if (DEBUG_ALLOCS) { allocTB--; printf("TB ~: %d\n",allocTB); }
}


void enc_tb::alloc_coeff_memory(int cIdx, int tbSize)
{
  assert(coeff[cIdx]==NULL);
  coeff[cIdx] = new int16_t[tbSize*tbSize];
}


void enc_tb::reconstruct_tb(encoder_context* ectx,
                            de265_image* img,
                            int x0,int y0,  // luma
                            int log2TbSize, // chroma adapted
                            int cIdx) const
{
  // chroma adapted position
  int xC=x0;
  int yC=y0;

  if (cIdx>0 && ectx->get_sps().chroma_format_idc == CHROMA_420) {
    xC>>=1;
    yC>>=1;
  }


  if (!reconstruction[cIdx]) {

    reconstruction[cIdx] = std::make_shared<small_image_buffer>(log2TbSize, sizeof(uint8_t));

    if (cb->PredMode == MODE_SKIP) {
      PixelAccessor dstPixels(*reconstruction[cIdx], xC,yC);
      dstPixels.copyFromImage(img, cIdx);
    }
    else { // not SKIP mode
      if (cb->PredMode == MODE_INTRA) {

        //enum IntraPredMode intraPredMode  = img->get_IntraPredMode(x0,y0);
        enum IntraPredMode intraPredMode  = intra_mode;

        if (cIdx>0) {
          intraPredMode = intra_mode_chroma;
        }

        //printf("reconstruct TB (%d;%d): intra mode (cIdx=%d) = %d\n",xC,yC,cIdx,intraPredMode);

        //decode_intra_prediction(img, xC,yC,  intraPredMode, 1<< log2TbSize   , cIdx);

        //printf("access intra-prediction of TB %p\n",this);

        intra_prediction[cIdx]->copy_to(*reconstruction[cIdx]);
        /*
          copy_subimage(img->get_image_plane_at_pos(cIdx,xC,yC),
          img->get_image_stride(cIdx),
          intra_prediction[cIdx]->get_buffer<uint8_t>(), 1<<log2TbSize,
          1<<log2TbSize, 1<<log2TbSize);
        */
      }
      else {
        assert(0); // -> TODO: now only store in tb_enc

        int size = 1<<log2TbSize;

        uint8_t* dst_ptr  = img->get_image_plane_at_pos(cIdx, xC,  yC  );
        int dst_stride  = img->get_image_stride(cIdx);

        /* TODO TMP HACK: prediction does not exist anymore
        uint8_t* src_ptr  = ectx->prediction->get_image_plane_at_pos(cIdx, xC,  yC  );
        int src_stride  = ectx->prediction->get_image_stride(cIdx);

        for (int y=0;y<size;y++) {
          for (int x=0;x<size;x++) {
            dst_ptr[y*dst_stride+x] = src_ptr[y*src_stride+x];
          }
        }
        */
      }

      ALIGNED_16(int16_t) dequant_coeff[32*32];

      if (cbf[cIdx]) dequant_coefficients(dequant_coeff, coeff[cIdx], log2TbSize, cb->qp);

      if (0 && cbf[cIdx]) {
        printf("--- quantized coeffs ---\n");
        printBlk("qcoeffs",coeff[0],1<<log2TbSize,1<<log2TbSize);

        printf("--- dequantized coeffs ---\n");
        printBlk("dequant",dequant_coeff,1<<log2TbSize,1<<log2TbSize);
      }

#if 0
      uint8_t* ptr  = img->get_image_plane_at_pos(cIdx, xC,  yC  );
      int stride  = img->get_image_stride(cIdx);
#endif

      int trType = (cIdx==0 && log2TbSize==2); // TODO: inter

      //printf("--- prediction %d %d / %d ---\n",x0,y0,cIdx);
      //printBlk("prediction",ptr,1<<log2TbSize,stride);

      if (cbf[cIdx]) inv_transform(&ectx->acceleration,
                                   reconstruction[cIdx]->get_buffer<uint8_t>(), 1<<log2TbSize,
                                   dequant_coeff, log2TbSize,   trType);

      //printBlk("RECO",reconstruction[cIdx]->get_buffer_u8(),1<<log2TbSize,
      //         reconstruction[cIdx]->getStride());
    }
  }


  // copy reconstruction into image

#if 0
  copy_subimage(img->get_image_plane_at_pos(cIdx,xC,yC),
                img->get_image_stride(cIdx),
                reconstruction[cIdx]->get_buffer<uint8_t>(), 1<<log2TbSize,
                1<<log2TbSize, 1<<log2TbSize);
#endif

  //printf("--- RECO intra prediction %d %d ---\n",x0,y0);
  //printBlk("RECO",ptr,1<<log2TbSize,stride);
}


void enc_tb::debug_writeBlack(encoder_context* ectx, de265_image* img) const
{
  if (split_transform_flag) {
    for (int i=0;i<4;i++) {
      children[i]->debug_writeBlack(ectx,img);
    }
  }
  else {
    //reconstruct_tb(ectx, img, x,y, log2Size, 0);

    int size = 1<<(log2Size<<1);
    std::vector<uint8_t> buf(size);
    memset(&buf[0],0x12,size);

    int cIdx=0;
    int xC=x,yC=y;

    copy_subimage(img->get_image_plane_at_pos(cIdx,xC,yC),
                  img->get_image_stride(cIdx),
                  &buf[0], 1<<log2Size,
                  1<<log2Size, 1<<log2Size);
  }
}


void enc_tb::reconstruct(encoder_context* ectx, de265_image* img) const
{
  if (split_transform_flag) {
    for (int i=0;i<4;i++) {
      children[i]->reconstruct(ectx,img);
    }
  }
  else {
    reconstruct_tb(ectx, img, x,y, log2Size, 0);

    if (ectx->get_sps().chroma_format_idc == CHROMA_444) {
      reconstruct_tb(ectx, img, x,y, log2Size, 1);
      reconstruct_tb(ectx, img, x,y, log2Size, 2);
    }
    else if (log2Size>2) {
      reconstruct_tb(ectx, img, x,y, log2Size-1, 1);
      reconstruct_tb(ectx, img, x,y, log2Size-1, 2);
    }
    else if (blkIdx==3) {
      int xBase = x - (1<<log2Size);
      int yBase = y - (1<<log2Size);

      reconstruct_tb(ectx, img, xBase,yBase, log2Size, 1);
      reconstruct_tb(ectx, img, xBase,yBase, log2Size, 2);
    }
  }
}


void enc_tb::set_cbf_flags_from_children()
{
  assert(split_transform_flag);

  cbf[0] = 0;
  cbf[1] = 0;
  cbf[2] = 0;

  for (int i=0;i<4;i++) {
    cbf[0] |= children[i]->cbf[0];
    cbf[1] |= children[i]->cbf[1];
    cbf[2] |= children[i]->cbf[2];
  }
}



alloc_pool enc_cb::mMemPool(sizeof(enc_cb), 200);


enc_cb::enc_cb()
  : split_cu_flag(false),
    cu_transquant_bypass_flag(false),
    pcm_flag(false),
    transform_tree(NULL),
    distortion(0),
    rate(0)
{
  parent = NULL;
  downPtr = NULL;

  if (DEBUG_ALLOCS) { allocCB++; printf("CB  : %d\n",allocCB); }
}

enc_cb::~enc_cb()
{
  if (split_cu_flag) {
    for (int i=0;i<4;i++) {
      delete children[i];
    }
  }
  else {
    delete transform_tree;
  }

  if (DEBUG_ALLOCS) { allocCB--; printf("CB ~: %d\n",allocCB); }
}


/*
void enc_cb::write_to_image(de265_image* img) const
{
  //printf("write_to_image %d %d size:%d\n",x,y,1<<log2Size);


  if (!split_cu_flag) {
    img->set_log2CbSize(x,y,log2Size, true);
    img->set_ctDepth(x,y,log2Size, ctDepth);
    assert(pcm_flag==0);
    img->set_pcm_flag(x,y,log2Size, pcm_flag);
    img->set_cu_transquant_bypass(x,y,log2Size, cu_transquant_bypass_flag);
    img->set_QPY(x,y,log2Size, qp);
    img->set_pred_mode(x,y, log2Size, PredMode);
    img->set_PartMode(x,y, PartMode);

    if (PredMode == MODE_INTRA) {

      if (PartMode == PART_NxN) {
        int h = 1<<(log2Size-1);
        img->set_IntraPredMode(x  ,y  ,log2Size-1, transform_tree->children[0]->intra_mode);
        img->set_IntraPredMode(x+h,y  ,log2Size-1, transform_tree->children[1]->intra_mode);
        img->set_IntraPredMode(x  ,y+h,log2Size-1, transform_tree->children[2]->intra_mode);
        img->set_IntraPredMode(x+h,y+h,log2Size-1, transform_tree->children[3]->intra_mode);
      }
      else {
        img->set_IntraPredMode(x,y,log2Size, transform_tree->intra_mode);
      }
    }
    else {
      int nC = 1<<log2Size;
      int nC2 = nC>>1;
      int nC4 = nC>>2;
      int nC3 = nC-nC4;
      switch (PartMode) {
      case PART_2Nx2N:
        img->set_mv_info(x,y,nC,nC, inter.pb[0].motion);
        break;
      case PART_NxN:
        img->set_mv_info(x    ,y    ,nC2,nC2, inter.pb[0].motion);
        img->set_mv_info(x+nC2,y    ,nC2,nC2, inter.pb[1].motion);
        img->set_mv_info(x    ,y+nC2,nC2,nC2, inter.pb[2].motion);
        img->set_mv_info(x+nC2,y+nC2,nC2,nC2, inter.pb[3].motion);
        break;
      case PART_2NxN:
        img->set_mv_info(x,y    ,nC,nC2, inter.pb[0].motion);
        img->set_mv_info(x,y+nC2,nC,nC2, inter.pb[1].motion);
        break;
      case PART_Nx2N:
        img->set_mv_info(x    ,y,nC2,nC, inter.pb[0].motion);
        img->set_mv_info(x+nC2,y,nC2,nC, inter.pb[1].motion);
        break;
      case PART_2NxnU:
        img->set_mv_info(x,y    ,nC,nC4, inter.pb[0].motion);
        img->set_mv_info(x,y+nC4,nC,nC3, inter.pb[1].motion);
        break;
      case PART_2NxnD:
        img->set_mv_info(x,y    ,nC,nC3, inter.pb[0].motion);
        img->set_mv_info(x,y+nC3,nC,nC4, inter.pb[1].motion);
        break;
      case PART_nLx2N:
        img->set_mv_info(x    ,y,nC4,nC, inter.pb[0].motion);
        img->set_mv_info(x+nC4,y,nC3,nC, inter.pb[1].motion);
        break;
      case PART_nRx2N:
        img->set_mv_info(x    ,y,nC3,nC, inter.pb[0].motion);
        img->set_mv_info(x+nC3,y,nC4,nC, inter.pb[1].motion);
        break;
      }
    }
  }
  else {
    for (int i=0;i<4;i++) {
      if (children[i]) {
        children[i]->write_to_image(img);
      }
    }
  }
}
*/

void enc_cb::reconstruct(encoder_context* ectx, de265_image* img) const
{
  assert(0);
  if (split_cu_flag) {
    for (int i=0;i<4;i++) {
      children[i]->reconstruct(ectx, img);
    }
  }
  else {
    //write_to_image(img);
    transform_tree->reconstruct(ectx,img);
  }
}


void enc_cb::debug_dumpTree(int flags, int indent) const
{
  std::string indentStr;
  indentStr.insert(indentStr.begin(),indent,' ');

  std::cout << indentStr << "CB " << x << ";" << y << " "
            << (1<<log2Size) << "x" << (1<<log2Size) << " [" << this << "]\n";

  std::cout << indentStr << "| split_cu_flag: " << int(split_cu_flag) << "\n";
  std::cout << indentStr << "| ctDepth:       " << int(ctDepth) << "\n";

  if (split_cu_flag) {
    for (int i=0;i<4;i++)
      if (children[i]) {
        std::cout << indentStr << "| child CB " << i << ":\n";
        children[i]->debug_dumpTree(flags, indent+2);
      }
  }
  else {
    std::cout << indentStr << "| qp: " << int(qp) << "\n";
    std::cout << indentStr << "| PredMode: " << PredMode << "\n";
    std::cout << indentStr << "| PartMode: " << part_mode_name(PartMode) << "\n";
    std::cout << indentStr << "| transform_tree:\n";

    transform_tree->debug_dumpTree(flags, indent+2);
  }
}


void enc_tb::debug_dumpTree(int flags, int indent) const
{
  std::string indentStr;
  indentStr.insert(indentStr.begin(),indent,' ');

  std::cout << indentStr << "TB " << x << ";" << y << " "
            << (1<<log2Size) << "x" << (1<<log2Size) << " [" << this << "]\n";

  std::cout << indentStr << "| split_transform_flag: " << int(split_transform_flag) << "\n";
  std::cout << indentStr << "| TrafoDepth:           " << int(TrafoDepth) << "\n";
  std::cout << indentStr << "| blkIdx:               " << int(blkIdx) << "\n";

  std::cout << indentStr << "| intra_mode:           " << int(intra_mode) << "\n";
  std::cout << indentStr << "| intra_mode_chroma:    " << int(intra_mode_chroma) << "\n";

  std::cout << indentStr << "| cbf:                  "
            << int(cbf[0]) << ":"
            << int(cbf[1]) << ":"
            << int(cbf[2]) << "\n";


  if (flags & DUMPTREE_RECONSTRUCTION) {
    for (int i=0;i<3;i++)
      if (reconstruction[i]) {
        std::cout << indentStr << "| Reconstruction, channel " << i << ":\n";
        printBlk(NULL,
                 reconstruction[i]->get_buffer_u8(),
                 reconstruction[i]->getWidth(),
                 reconstruction[i]->getStride(),
                 indentStr + "| ");
      }
  }

  if (flags & DUMPTREE_INTRA_PREDICTION) {
    for (int i=0;i<3;i++)
      if (intra_prediction[i]) {
        //if (i==0) print_border(debug_intra_border+64, NULL, 1<<log2Size);

        std::cout << indentStr << "| Intra prediction, channel " << i << ":\n";
        printBlk(NULL,
                 intra_prediction[i]->get_buffer_u8(),
                 intra_prediction[i]->getWidth(),
                 intra_prediction[i]->getStride(),
                 indentStr + "| ");
      }
  }

  if (split_transform_flag) {
    for (int i=0;i<4;i++)
      if (children[i]) {
        std::cout << indentStr << "| child TB " << i << ":\n";
        children[i]->debug_dumpTree(flags, indent+2);
      }
  }
}



const enc_tb* enc_cb::getTB(int x,int y) const
{
  assert(!split_cu_flag);
  assert(transform_tree);

  return transform_tree->getTB(x,y);
}


const enc_tb* enc_tb::getTB(int px,int py) const
{
  if (split_transform_flag) {
    int xHalf = x + (1<<(log2Size-1));
    int yHalf = y + (1<<(log2Size-1));

    enc_tb* child;

    if (px<xHalf) {
      if (py<yHalf) {
        child = children[0];
      }
      else {
        child = children[2];
      }
    }
    else {
      if (py<yHalf) {
        child = children[1];
      }
      else {
        child = children[3];
      }
    }

    if (!child) { return NULL; }

    return child->getTB(px,py);
  }

  return this;
}



void CTBTreeMatrix::alloc(int w,int h, int log2CtbSize)
{
  free();

  int ctbSize = 1<<log2CtbSize;

  mWidthCtbs  = (w+ctbSize-1) >> log2CtbSize;
  mHeightCtbs = (h+ctbSize-1) >> log2CtbSize;
  mLog2CtbSize = log2CtbSize;

  mCTBs.resize(mWidthCtbs * mHeightCtbs, NULL);
}


const enc_cb* CTBTreeMatrix::getCB(int px,int py) const
{
  int xCTB = px>>mLog2CtbSize;
  int yCTB = py>>mLog2CtbSize;

  int idx = xCTB + yCTB*mWidthCtbs;
  assert(idx < mCTBs.size());

  enc_cb* cb = mCTBs[idx];
  if (!cb) { return NULL; }

  while (cb->split_cu_flag) {
    int xHalf = cb->x + (1<<(cb->log2Size-1));
    int yHalf = cb->y + (1<<(cb->log2Size-1));

    if (px<xHalf) {
      if (py<yHalf) {
        cb = cb->children[0];
      }
      else {
        cb = cb->children[2];
      }
    }
    else {
      if (py<yHalf) {
        cb = cb->children[1];
      }
      else {
        cb = cb->children[3];
      }
    }

    if (!cb) { return NULL; }
  }

  return cb;
}


const enc_tb* CTBTreeMatrix::getTB(int x,int y) const
{
  const enc_cb* cb = getCB(x,y);
  if (!cb) { return NULL; }

  const enc_tb* tb = cb->transform_tree;
  if (!tb) { return NULL; }

  return tb->getTB(x,y);
}


const enc_pb_inter* CTBTreeMatrix::getPB(int x,int y) const
{
  const enc_cb* cb = getCB(x,y);

  // TODO: get PB block based on partitioning

  return &cb->inter.pb[0];
}


void CTBTreeMatrix::writeReconstructionToImage(de265_image* img,
                                               const seq_parameter_set* sps) const
{
  for (size_t i=0;i<mCTBs.size();i++) {
    const enc_cb* cb = mCTBs[i];
    cb->writeReconstructionToImage(img, sps);
  }
}

void enc_cb::writeReconstructionToImage(de265_image* img,
                                        const seq_parameter_set* sps) const
{
  if (split_cu_flag) {
    for (int i=0;i<4;i++) {
      if (children[i]) {
        children[i]->writeReconstructionToImage(img,sps);
      }
    }
  }
  else {
    transform_tree->writeReconstructionToImage(img,sps);
  }
}

void enc_tb::writeReconstructionToImage(de265_image* img,
                                        const seq_parameter_set* sps) const
{
  if (split_transform_flag) {
    for (int i=0;i<4;i++) {
      if (children[i]) {
        children[i]->writeReconstructionToImage(img,sps);
      }
    }
  }
  else {
    // luma pixels

    PixelAccessor lumaPixels(*reconstruction[0], x,y);
    lumaPixels.copyToImage(img, 0);

    // chroma pixels

    if (sps->chroma_format_idc == CHROMA_444) {
      PixelAccessor chroma1Pixels(*reconstruction[1], x,y);
      chroma1Pixels.copyToImage(img, 1);
      PixelAccessor chroma2Pixels(*reconstruction[2], x,y);
      chroma2Pixels.copyToImage(img, 2);
    }
    else if (log2Size>2) {
      PixelAccessor chroma1Pixels(*reconstruction[1], x>>1,y>>1);
      chroma1Pixels.copyToImage(img, 1);
      PixelAccessor chroma2Pixels(*reconstruction[2], x>>1,y>>1);
      chroma2Pixels.copyToImage(img, 2);

      //reconstruct_tb(ectx, img, x,y, log2Size-1, 1);
      //reconstruct_tb(ectx, img, x,y, log2Size-1, 2);
    }
    else if (blkIdx==3) {
      int xBase = x - (1<<log2Size);
      int yBase = y - (1<<log2Size);

      PixelAccessor chroma1Pixels(*reconstruction[1], xBase>>1,yBase>>1);
      chroma1Pixels.copyToImage(img, 1);
      PixelAccessor chroma2Pixels(*reconstruction[2], xBase>>1,yBase>>1);
      chroma2Pixels.copyToImage(img, 2);

      //reconstruct_tb(ectx, img, xBase,yBase, log2Size, 1);
      //reconstruct_tb(ectx, img, xBase,yBase, log2Size, 2);
    }
  }
}


PixelAccessor enc_tb::getPixels(int x,int y, int cIdx, const seq_parameter_set& sps)
{
  int xL = x <<  sps.get_chroma_shift_W(cIdx);
  int yL = y <<  sps.get_chroma_shift_H(cIdx);

  const enc_tb* tb = getTB(xL,yL);

  if (cIdx==0 || sps.chroma_format_idc == CHROMA_444) {
    return PixelAccessor(*tb->reconstruction[cIdx], tb->x, tb->y);
  }
  else if (sps.chroma_format_idc == CHROMA_420) {
    if (tb->log2Size > 2) {
      return PixelAccessor(*tb->reconstruction[cIdx],
                           tb->x >> 1,
                           tb->y >> 1);
    }
    else {
      enc_tb* parent = tb->parent;
      tb = parent->children[3];

      return PixelAccessor(*tb->reconstruction[cIdx],
                           parent->x >> 1,
                           parent->y >> 1);
    }
  }
  else {
    assert(sps.chroma_format_idc == CHROMA_422);

    assert(false); // not supported yet

    return PixelAccessor::invalid();
  }
}


void PixelAccessor::copyToImage(de265_image* img, int cIdx) const
{
  uint8_t* p = img->get_image_plane_at_pos(cIdx, mXMin, mYMin);
  int stride = img->get_image_stride(cIdx);

  for (int y=0;y<mHeight;y++) {
    memcpy(p, mBase+mXMin+(y+mYMin)*mStride, mWidth);
    p += stride;
  }
}

void PixelAccessor::copyFromImage(const de265_image* img, int cIdx)
{
  const uint8_t* p = img->get_image_plane_at_pos(cIdx, mXMin, mYMin);
  int stride = img->get_image_stride(cIdx);

  for (int y=0;y<mHeight;y++) {
    memcpy(mBase+mXMin+(y+mYMin)*mStride, p, mWidth);
    p += stride;
  }
}
