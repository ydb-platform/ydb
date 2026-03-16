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

#include "intrapred.h"
#include "encoder/encoder-intrapred.h"
#include "encoder/encoder-types.h"
#include "transform.h"
#include "util.h"
#include <assert.h>


#include <sys/types.h>
#include <string.h>


void fillIntraPredModeCandidates(enum IntraPredMode candModeList[3],
                                 int x,int y,
                                 bool availableA, // left
                                 bool availableB, // top
                                 const CTBTreeMatrix& ctbs,
                                 const seq_parameter_set* sps)
{

  // block on left side

  enum IntraPredMode candIntraPredModeA, candIntraPredModeB;

  if (availableA==false) {
    candIntraPredModeA=INTRA_DC;
  }
  else {
    const enc_cb* cbL = ctbs.getCB(x-1,y);
    assert(cbL != NULL);

    if (cbL->PredMode != MODE_INTRA ||
        cbL->pcm_flag) {
      candIntraPredModeA=INTRA_DC;
    }
    else {
      const enc_tb* tbL = cbL->getTB(x-1,y);
      assert(tbL);
      candIntraPredModeA = tbL->intra_mode;
    }
  }

  // block above

  if (availableB==false) {
    candIntraPredModeB=INTRA_DC;
  }
  else {
    const enc_cb* cbA = ctbs.getCB(x,y-1);
    assert(cbA != NULL);

    if (cbA->PredMode != MODE_INTRA ||
        cbA->pcm_flag) {
      candIntraPredModeB=INTRA_DC;
    }
    else if (y-1 < ((y >> sps->Log2CtbSizeY) << sps->Log2CtbSizeY)) {
      candIntraPredModeB=INTRA_DC;
    }
    else {
      const enc_tb* tbA = cbA->getTB(x,y-1);
      assert(tbA);

      candIntraPredModeB = tbA->intra_mode;
    }
  }


  logtrace(LogSlice,"%d;%d candA:%d / candB:%d\n", x,y,
           availableA ? candIntraPredModeA : -999,
           availableB ? candIntraPredModeB : -999);


  fillIntraPredModeCandidates(candModeList,
                              candIntraPredModeA,
                              candIntraPredModeB);
}


template <class pixel_t>
class intra_border_computer_ctbtree : public intra_border_computer<pixel_t>
{
public:
  void fill_from_ctbtree(const enc_tb* tb,
                         const CTBTreeMatrix& ctbs);
};


template <class pixel_t>
void intra_border_computer_ctbtree<pixel_t>::fill_from_ctbtree(const enc_tb* blkTb,
                                                               const CTBTreeMatrix& ctbs)
{
  int xBLuma = this->xB * this->SubWidth;
  int yBLuma = this->yB * this->SubHeight;

  int currBlockAddr = this->pps->MinTbAddrZS[ (xBLuma >> this->sps->Log2MinTrafoSize) +
                                              (yBLuma >> this->sps->Log2MinTrafoSize) * this->sps->PicWidthInTbsY ];


  // copy pixels at left column

  for (int y=this->nBottom-1 ; y>=0 ; y-=4)
    if (this->availableLeft)
      {
        int NBlockAddr = this->pps->MinTbAddrZS[ (((this->xB-1)*this->SubWidth )>>this->sps->Log2MinTrafoSize) +
                                                 (((this->yB+y)*this->SubHeight)>>this->sps->Log2MinTrafoSize)
                                                 * this->sps->PicWidthInTbsY ];

        bool availableN = NBlockAddr <= currBlockAddr;

        int xN = this->xB-1;
        int yN = this->yB+y;

        const enc_cb* cb = ctbs.getCB(xN*this->SubWidth, yN*this->SubHeight);

        if (this->pps->constrained_intra_pred_flag) {
          if (cb->PredMode != MODE_INTRA)
            availableN = false;
        }

        if (availableN) {
          PixelAccessor pa = cb->transform_tree->getPixels(xN,yN, this->cIdx, *this->sps);

          if (!this->nAvail) this->firstValue = pa[this->yB+y][this->xB-1];

          for (int i=0;i<4;i++) {
            this->available[-y+i-1] = availableN;
            this->out_border[-y+i-1] = pa[this->yB+y-i][this->xB-1];
          }

          this->nAvail+=4;
        }
      }

  // copy pixel at top-left position

  if (this->availableTopLeft)
    {
      int NBlockAddr = this->pps->MinTbAddrZS[ (((this->xB-1)*this->SubWidth )>>this->sps->Log2MinTrafoSize) +
                                               (((this->yB-1)*this->SubHeight)>>this->sps->Log2MinTrafoSize)
                                               * this->sps->PicWidthInTbsY ];

      bool availableN = NBlockAddr <= currBlockAddr;

      int xN = this->xB-1;
      int yN = this->yB-1;

      const enc_cb* cb = ctbs.getCB(xN*this->SubWidth, yN*this->SubHeight);

      if (this->pps->constrained_intra_pred_flag) {
        if (cb->PredMode!=MODE_INTRA) {
          availableN = false;
        }
      }

      if (availableN) {
        PixelAccessor pa = cb->transform_tree->getPixels(xN,yN, this->cIdx, *this->sps);

        this->out_border[0] = pa[this->yB-1][this->xB-1];
        this->available[0] = availableN;

        if (!this->nAvail) this->firstValue = this->out_border[0];
        this->nAvail++;
      }
    }


  // copy pixels at top row

  for (int x=0 ; x<this->nRight ; x+=4) {
    bool borderAvailable;
    if (x<this->nT) borderAvailable = this->availableTop;
    else            borderAvailable = this->availableTopRight;

    if (borderAvailable)
      {
        int NBlockAddr = this->pps->MinTbAddrZS[ (((this->xB+x)*this->SubWidth )>>this->sps->Log2MinTrafoSize) +
                                                 (((this->yB-1)*this->SubHeight)>>this->sps->Log2MinTrafoSize)
                                                 * this->sps->PicWidthInTbsY ];

        bool availableN = NBlockAddr <= currBlockAddr;

        int xN = this->xB+x;
        int yN = this->yB-1;

        const enc_cb* cb = ctbs.getCB(xN*this->SubWidth, yN*this->SubHeight);

        if (this->pps->constrained_intra_pred_flag) {
          if (cb->PredMode!=MODE_INTRA) {
            availableN = false;
          }
        }


        if (availableN) {
          PixelAccessor pa = cb->transform_tree->getPixels(xN,yN, this->cIdx, *this->sps);

          if (!this->nAvail) this->firstValue = pa[this->yB-1][this->xB+x];

          for (int i=0;i<4;i++) {
            this->out_border[x+i+1] = pa[this->yB-1][this->xB+x+i];
            this->available[x+i+1] = availableN;
          }

          this->nAvail+=4;
        }
      }
  }
}


// (8.4.4.2.2)
template <class pixel_t>
void fill_border_samples_from_tree(const de265_image* img,
                                   const enc_tb* tb,
                                   const CTBTreeMatrix& ctbs,
                                   int cIdx,
                                   pixel_t* out_border)
{
  intra_border_computer_ctbtree<pixel_t> c;

  // xB,yB in component specific resolution
  int xB,yB;
  int nT = 1<<tb->log2Size;

  xB = tb->x;
  yB = tb->y;

  if (img->get_sps().chroma_format_idc == CHROMA_444) {
  }
  else if (cIdx > 0) {
    // TODO: proper chroma handling
    xB >>= 1;
    yB >>= 1;
    nT >>= 1;

    if (tb->log2Size==2) {
      xB = tb->parent->x >> 1;
      yB = tb->parent->y >> 1;
      nT = 4;
    }
  }

  c.init(out_border, img, nT, cIdx, xB, yB);
  c.preproc();
  c.fill_from_ctbtree(tb, ctbs);
  c.reference_sample_substitution();
}



template <class pixel_t>
void decode_intra_prediction_from_tree_internal(const de265_image* img,
                                                const enc_tb* tb,
                                                const CTBTreeMatrix& ctbs,
                                                const seq_parameter_set& sps,
                                                int cIdx)
{
  enum IntraPredMode intraPredMode;
  if (cIdx==0) intraPredMode = tb->intra_mode;
  else         intraPredMode = tb->intra_mode_chroma;

  pixel_t* dst = tb->intra_prediction[cIdx]->get_buffer<pixel_t>();
  int dstStride = tb->intra_prediction[cIdx]->getStride();

  pixel_t  border_pixels_mem[4*MAX_INTRA_PRED_BLOCK_SIZE+1];
  pixel_t* border_pixels = &border_pixels_mem[2*MAX_INTRA_PRED_BLOCK_SIZE];

  fill_border_samples_from_tree(img, tb, ctbs, cIdx, border_pixels);

  if (cIdx==0) {
    // memcpy(tb->debug_intra_border, border_pixels_mem, 2*64+1);
  }

  int nT = 1<<tb->log2Size;
  if (cIdx>0 && tb->log2Size>2 && sps.chroma_format_idc == CHROMA_420) {
    nT >>= 1; // TODO: 4:2:2
  }


  if (sps.range_extension.intra_smoothing_disabled_flag == 0 &&
      (cIdx==0 || sps.ChromaArrayType==CHROMA_444))
    {
      intra_prediction_sample_filtering(sps, border_pixels, nT, cIdx, intraPredMode);
    }


  switch (intraPredMode) {
  case INTRA_PLANAR:
    intra_prediction_planar(dst,dstStride, nT, cIdx, border_pixels);
    break;
  case INTRA_DC:
    intra_prediction_DC(dst,dstStride, nT, cIdx, border_pixels);
    break;
  default:
    {
      //int bit_depth = img->get_bit_depth(cIdx);
      int bit_depth = 8; // TODO

      bool disableIntraBoundaryFilter =
        (sps.range_extension.implicit_rdpcm_enabled_flag &&
         tb->cb->cu_transquant_bypass_flag);

      intra_prediction_angular(dst,dstStride, bit_depth,disableIntraBoundaryFilter,
                               tb->x,tb->y,intraPredMode,nT,cIdx, border_pixels);
    }
    break;
  }
}


void decode_intra_prediction_from_tree(const de265_image* img,
                                       const enc_tb* tb,
                                       const CTBTreeMatrix& ctbs,
                                       const seq_parameter_set& sps,
                                       int cIdx)
{
  // TODO: high bit depths

  decode_intra_prediction_from_tree_internal<uint8_t>(img ,tb, ctbs, sps, cIdx);
}
