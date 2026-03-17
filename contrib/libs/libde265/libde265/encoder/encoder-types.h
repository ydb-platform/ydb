/*
 * H.265 video codec.
 * Copyright (c) 2013-2014 struktur AG, Dirk Farin <farin@struktur.de>
 *
 * Authors: Dirk Farin <farin@struktur.de>
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

#ifndef ENCODE_H
#define ENCODE_H

#include "libde265/image.h"
#include "libde265/decctx.h"
#include "libde265/image-io.h"
#include "libde265/alloc_pool.h"

#include <memory>

class encoder_context;
class enc_cb;


class small_image_buffer
{
 public:
  explicit small_image_buffer(int log2Size,int bytes_per_pixel=1);
  ~small_image_buffer();

  uint8_t*  get_buffer_u8() const { return mBuf; }
  int16_t*  get_buffer_s16() const { return (int16_t*)mBuf; }
  uint16_t* get_buffer_u16() const { return (uint16_t*)mBuf; }
  template <class pixel_t> pixel_t* get_buffer() const { return (pixel_t*)mBuf; }

  void copy_to(small_image_buffer& b) const {
    assert(b.mHeight==mHeight);
    assert(b.mBytesPerRow==mBytesPerRow);
    memcpy(b.mBuf, mBuf, mBytesPerRow*mHeight);
  }

  int getWidth() const { return mWidth; }
  int getHeight() const { return mHeight; }

  int getStride() const { return mStride; } // pixels per row

 private:
  uint8_t*  mBuf;
  uint16_t  mStride;
  uint16_t  mBytesPerRow;

  uint8_t   mWidth, mHeight;

  // small_image_buffer cannot be copied

  small_image_buffer(const small_image_buffer&) { assert(false); } // = delete;
  small_image_buffer& operator=(const small_image_buffer&) { assert(false); return *this; } // = delete;
};


class enc_node
{
public:
  enc_node() { }
  enc_node(int _x,int _y, int _log2Size) : x(_x), y(_y), log2Size(_log2Size) { }
  virtual ~enc_node() { }

  uint16_t x,y;
  uint8_t  log2Size : 3;


  static const int DUMPTREE_INTRA_PREDICTION = (1<<0);
  static const int DUMPTREE_RESIDUAL         = (1<<1);
  static const int DUMPTREE_RECONSTRUCTION   = (1<<2);
  static const int DUMPTREE_ALL              = 0xFFFF;

  virtual void debug_dumpTree(int flags, int indent=0) const = 0;
};


class PixelAccessor
{
 public:
  PixelAccessor(small_image_buffer& buf, int x0,int y0) {
    mBase = buf.get_buffer_u8();
    mStride = buf.getStride();
    mXMin = x0;
    mYMin = y0;
    mWidth = buf.getWidth();
    mHeight= buf.getHeight();

    mBase -= x0 + y0*mStride;
  }

  const uint8_t* operator[](int y) const { return mBase+y*mStride; }

  int getLeft() const { return mXMin; }
  int getWidth() const { return mWidth; }
  int getTop() const { return mYMin; }
  int getHeight() const { return mHeight; }

  void copyToImage(de265_image* img, int cIdx) const;
  void copyFromImage(const de265_image* img, int cIdx);

  static PixelAccessor invalid() {
    return PixelAccessor();
  }

 private:
  uint8_t* mBase;
  short   mStride;
  short   mXMin,  mYMin;
  uint8_t mWidth, mHeight;

  PixelAccessor() {
    mBase = NULL;
    mStride = mXMin = mYMin = mWidth = mHeight = 0;
  }
};


class enc_tb : public enc_node
{
 public:
  enc_tb(int x,int y,int log2TbSize, enc_cb* _cb);
  ~enc_tb();

  enc_tb* parent;
  enc_cb* cb;
  enc_tb** downPtr;

  uint8_t split_transform_flag : 1;
  uint8_t TrafoDepth : 2;  // 2 bits enough ? (TODO)
  uint8_t blkIdx : 2;

  enum IntraPredMode intra_mode;

  // Note: in NxN partition mode, the chroma mode is always derived from
  // the top-left child's intra mode (for chroma 4:2:0).
  enum IntraPredMode intra_mode_chroma;

  uint8_t cbf[3];


  /* intra_prediction and residual is filled in tb-split, because this is where we decide
     on the final block-size the TB is coded with.
   */
  //mutable uint8_t debug_intra_border[2*64+1];
  std::shared_ptr<small_image_buffer> intra_prediction[3];
  std::shared_ptr<small_image_buffer> residual[3];

  /* Reconstruction is computed on-demand in writeMetadata().
   */
  mutable std::shared_ptr<small_image_buffer> reconstruction[3];

  union {
    // split
    struct {
      enc_tb* children[4];
    };

    // leaf node
    struct {
      int16_t* coeff[3];

      bool    skip_transform[3][2];
      uint8_t explicit_rdpcm[3][2];
    };
  };

  float distortion;  // total distortion for this level of the TB tree (including all children)
  float rate;        // total rate for coding this TB level and all children
  float rate_withoutCbfChroma;

  void set_cbf_flags_from_children();

  void reconstruct(encoder_context* ectx, de265_image* img) const;
  void debug_writeBlack(encoder_context* ectx, de265_image* img) const;

  bool isZeroBlock() const { return cbf[0]==false && cbf[1]==false && cbf[2]==false; }

  void alloc_coeff_memory(int cIdx, int tbSize);

  const enc_tb* getTB(int x,int y) const;

  PixelAccessor getPixels(int x,int y, int cIdx, const seq_parameter_set& sps);

  void writeReconstructionToImage(de265_image* img,
                                  const seq_parameter_set* sps) const;

  /*
  static void* operator new(const size_t size) { return mMemPool.new_obj(size); }
  static void operator delete(void* obj) { mMemPool.delete_obj(obj); }
  */


  virtual void debug_dumpTree(int flags, int indent=0) const;

private:
  static alloc_pool mMemPool;

  void reconstruct_tb(encoder_context* ectx,
                      de265_image* img, int x0,int y0, int log2TbSize,
                      int cIdx) const;
};


struct enc_pb_inter
{
  /* absolute motion information (for MV-prediction candidates)
   */
  PBMotion         motion;

  /* specification how to code the motion vector in the bitstream
   */
  PBMotionCoding   spec;


  // NOT TRUE: refIdx in 'spec' is not used. It is taken from 'motion'
  // Currently, information is duplicated. Same as with inter_pred_idc/predFlag[].

  /* SPEC:
  int8_t  refIdx[2]; // not used
  int16_t mvd[2][2];

  uint8_t inter_pred_idc : 2; // enum InterPredIdc
  uint8_t mvp_l0_flag : 1;
  uint8_t mvp_l1_flag : 1;
  uint8_t merge_flag : 1;
  uint8_t merge_idx  : 3;
  */
};


class enc_cb : public enc_node
{
public:
  enc_cb();
  ~enc_cb();

  enc_cb* parent;
  enc_cb** downPtr;

  uint8_t split_cu_flag : 1;
  uint8_t ctDepth : 2;


  union {
    // split
    struct {
      enc_cb* children[4];   // undefined when split_cu_flag==false
    };

    // non-split
    struct {
      uint8_t qp : 6;
      uint8_t cu_transquant_bypass_flag : 1; // currently unused
      uint8_t pcm_flag : 1;

      enum PredMode PredMode; // : 6;
      enum PartMode PartMode; // : 3;

      union {
        struct {
          //enum IntraPredMode pred_mode[4];
          //enum IntraPredMode chroma_mode;
        } intra;

        struct {
          enc_pb_inter pb[4];

          uint8_t rqt_root_cbf : 1;
        } inter;
      };

      enc_tb* transform_tree;
    };
  };


  float distortion;
  float rate;


  void set_rqt_root_bf_from_children_cbf();

  /* Save CB reconstruction in the node and restore it again to the image.
     Pixel data and metadata.
   */
  //virtual void save(const de265_image*);
  //virtual void restore(de265_image*);


  /* Decode this CB: pixel data and write metadata to image.
   */
  void reconstruct(encoder_context* ectx,de265_image* img) const;

  // can only be called on the lowest-level CB (with TB-tree as its direct child)
  const enc_tb* getTB(int x,int y) const;

  void writeReconstructionToImage(de265_image* img,
                                  const seq_parameter_set* sps) const;


  virtual void debug_dumpTree(int flags, int indent=0) const;


  // memory management

  static void* operator new(const size_t size) {
    void* p = mMemPool.new_obj(size);
    //printf("ALLOC %p\n",p);
    return p;
  }
  static void operator delete(void* obj) {
    //printf("DELETE %p\n",obj);
    mMemPool.delete_obj(obj);
  }

 private:
  //void write_to_image(de265_image*) const;

  static alloc_pool mMemPool;
};



class CTBTreeMatrix
{
 public:
 CTBTreeMatrix() : mWidthCtbs(0), mHeightCtbs(0), mLog2CtbSize(0) { }
  ~CTBTreeMatrix() { free(); }

  void alloc(int w,int h, int log2CtbSize);
  void clear() { free(); }

  void setCTB(int xCTB, int yCTB, enc_cb* ctb) {
    int idx = xCTB + yCTB*mWidthCtbs;
    assert(idx < mCTBs.size());
    if (mCTBs[idx]) { delete mCTBs[idx]; }
    mCTBs[idx] = ctb;
  }

  const enc_cb* getCTB(int xCTB, int yCTB) const {
    int idx = xCTB + yCTB*mWidthCtbs;
    assert(idx < mCTBs.size());
    return mCTBs[idx];
  }

  enc_cb** getCTBRootPointer(int x, int y) {
    x >>= mLog2CtbSize;
    y >>= mLog2CtbSize;

    int idx = x + y*mWidthCtbs;
    assert(idx < mCTBs.size());
    return &mCTBs[idx];
  }

  const enc_cb* getCB(int x,int y) const;
  const enc_tb* getTB(int x,int y) const;
  const enc_pb_inter* getPB(int x,int y) const;

  void writeReconstructionToImage(de265_image* img,
                                  const seq_parameter_set*) const;

 private:
  std::vector<enc_cb*> mCTBs;
  int mWidthCtbs;
  int mHeightCtbs;
  int mLog2CtbSize;

  void free() {
    for (int i=0 ; i<mWidthCtbs*mHeightCtbs ; i++) {
      if (mCTBs[i]) {
        delete mCTBs[i];
        mCTBs[i]=NULL;
      }
    }
  }
};




inline int childX(int x0, int idx, int log2CbSize)
{
  return x0 + ((idx&1) << (log2CbSize-1));
}

inline int childY(int y0, int idx, int log2CbSize)
{
  return y0 + ((idx>>1) << (log2CbSize-1));
}



#endif
