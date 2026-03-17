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

#ifndef DE265_IMAGE_H
#define DE265_IMAGE_H

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <memory>

#include "libde265/de265.h"
#include "libde265/sps.h"
#include "libde265/pps.h"
#include "libde265/motion.h"
#include "libde265/threads.h"
#include "libde265/slice.h"
#include "libde265/nal.h"

struct en265_encoder_context;

enum PictureState {
  UnusedForReference,
  UsedForShortTermReference,
  UsedForLongTermReference
};


/* TODO:
   At INTEGRITY_DERIVED_FROM_FAULTY_REFERENCE images, we can check the SEI hash, whether
   the output image is correct despite the faulty reference, and set the state back to correct.
*/
#define INTEGRITY_CORRECT 0
#define INTEGRITY_UNAVAILABLE_REFERENCE 1
#define INTEGRITY_NOT_DECODED 2
#define INTEGRITY_DECODING_ERRORS 3
#define INTEGRITY_DERIVED_FROM_FAULTY_REFERENCE 4

#define SEI_HASH_UNCHECKED 0
#define SEI_HASH_CORRECT   1
#define SEI_HASH_INCORRECT 2

#define TU_FLAG_NONZERO_COEFF  (1<<7)
#define TU_FLAG_SPLIT_TRANSFORM_MASK  0x1F

#define DEBLOCK_FLAG_VERTI (1<<4)
#define DEBLOCK_FLAG_HORIZ (1<<5)
#define DEBLOCK_PB_EDGE_VERTI (1<<6)
#define DEBLOCK_PB_EDGE_HORIZ (1<<7)
#define DEBLOCK_BS_MASK     0x03


#define CTB_PROGRESS_NONE      0
#define CTB_PROGRESS_PREFILTER 1
#define CTB_PROGRESS_DEBLK_V   2
#define CTB_PROGRESS_DEBLK_H   3
#define CTB_PROGRESS_SAO       4

class decoder_context;

template <class DataUnit> class MetaDataArray
{
 public:
  MetaDataArray() { data=NULL; data_size=0; log2unitSize=0; width_in_units=0; height_in_units=0; }
  ~MetaDataArray() { free(data); }

  LIBDE265_CHECK_RESULT bool alloc(int w,int h, int _log2unitSize) {
    int size = w*h;

    if (size != data_size) {
      free(data);
      data = (DataUnit*)malloc(size * sizeof(DataUnit));
      if (data == NULL) {
        data_size = 0;
        return false;
      }
      data_size = size;
    }

    width_in_units = w;
    height_in_units = h;

    log2unitSize = _log2unitSize;

    return data != NULL;
  }

  void clear() {
    if (data) memset(data, 0, sizeof(DataUnit) * data_size);
  }

  const DataUnit& get(int x,int y) const {
    int unitX = x>>log2unitSize;
    int unitY = y>>log2unitSize;

    assert(unitX >= 0 && unitX < width_in_units);
    assert(unitY >= 0 && unitY < height_in_units);

    return data[ unitX + unitY*width_in_units ];
  }

  DataUnit& get(int x,int y) {
    int unitX = x>>log2unitSize;
    int unitY = y>>log2unitSize;

    assert(unitX >= 0 && unitX < width_in_units);
    assert(unitY >= 0 && unitY < height_in_units);

    return data[ unitX + unitY*width_in_units ];
  }

  void set(int x,int y, const DataUnit& d) {
    int unitX = x>>log2unitSize;
    int unitY = y>>log2unitSize;

    assert(unitX >= 0 && unitX < width_in_units);
    assert(unitY >= 0 && unitY < height_in_units);

    data[ unitX + unitY*width_in_units ] = d;
  }

  DataUnit& operator[](int idx) { return data[idx]; }
  const DataUnit& operator[](int idx) const { return data[idx]; }

  int size() const { return data_size; }

  // private:
  DataUnit* data;
  int data_size;
  int log2unitSize;
  int width_in_units;
  int height_in_units;
};

#define SET_CB_BLK(x,y,log2BlkWidth,  Field,value)              \
  int cbX = x >> cb_info.log2unitSize; \
  int cbY = y >> cb_info.log2unitSize; \
  int width = 1 << (log2BlkWidth - cb_info.log2unitSize);           \
  for (int cby=cbY;cby<cbY+width;cby++)                             \
    for (int cbx=cbX;cbx<cbX+width;cbx++)                           \
      {                                                             \
        cb_info[ cbx + cby*cb_info.width_in_units ].Field = value;  \
      }

#define CLEAR_TB_BLK(x,y,log2BlkWidth)              \
  int tuX = x >> tu_info.log2unitSize; \
  int tuY = y >> tu_info.log2unitSize; \
  int width = 1 << (log2BlkWidth - tu_info.log2unitSize);           \
  for (int tuy=tuY;tuy<tuY+width;tuy++)                             \
    for (int tux=tuX;tux<tuX+width;tux++)                           \
      {                                                             \
        tu_info[ tux + tuy*tu_info.width_in_units ] = 0;  \
      }


typedef struct {
  uint16_t SliceAddrRS;
  uint16_t SliceHeaderIndex; // index into array to slice header for this CTB

  sao_info saoInfo;
  bool     deblock;         // this CTB has to be deblocked

  // The following flag helps to quickly check whether we have to
  // check all conditions in the SAO filter or whether we can skip them.
  bool     has_pcm_or_cu_transquant_bypass; // pcm or transquant_bypass is used in this CTB
} CTB_info;


typedef struct {
  uint8_t log2CbSize : 3;   /* [0;6] (1<<log2CbSize) = 64
                               This is only set in the top-left corner of the CB.
                               The other values should be zero.
                               TODO: in the encoder, we have to clear to zero.
                               Used in deblocking and QP-scale decoding */
  uint8_t PartMode : 3;     // (enum PartMode)  [0;7] set only in top-left of CB
                            // Used for spatial merging candidates in current frame
                            // and for deriving interSplitFlag in decoding.

  uint8_t ctDepth : 2;      // [0:3]? (for CTB size 64: 0:64, 1:32, 2:16, 3:8)
                            // Used for decoding/encoding split_cu flag.

  // --- byte boundary ---
  uint8_t PredMode : 2;     // (enum PredMode)  [0;2] must be saved for past images
                            // Used in motion decoding.
  uint8_t pcm_flag : 1;     // Stored for intra-prediction / SAO
  uint8_t cu_transquant_bypass : 1; // Stored for SAO
  // note: 4 bits left

  // --- byte boundary ---
  int8_t  QP_Y;  // Stored for QP prediction

} CB_ref_info;




struct de265_image {
  de265_image();
  ~de265_image();


  de265_error alloc_image(int w,int h, enum de265_chroma c,
                          std::shared_ptr<const seq_parameter_set> sps,
                          bool allocMetadata,
                          decoder_context* dctx,
                          //class encoder_context* ectx,
                          de265_PTS pts, void* user_data,
                          bool useCustomAllocFunctions);

  //de265_error alloc_encoder_data(const seq_parameter_set* sps);

  bool is_allocated() const { return pixels[0] != NULL; }

  void release();

  void set_headers(std::shared_ptr<video_parameter_set> _vps,
                   std::shared_ptr<seq_parameter_set>   _sps,
                   std::shared_ptr<pic_parameter_set>   _pps) {
    vps = _vps;
    sps = _sps;
    pps = _pps;
  }

  void fill_image(int y,int u,int v);
  void fill_plane(int channel, int value);
  de265_error copy_image(const de265_image* src);
  void copy_lines_from(const de265_image* src, int first, int end);
  void exchange_pixel_data_with(de265_image&);

  uint32_t get_ID() const { return ID; }


  /* */ uint8_t* get_image_plane(int cIdx)       { return pixels[cIdx]; }
  const uint8_t* get_image_plane(int cIdx) const { return pixels[cIdx]; }

  void set_image_plane(int cIdx, uint8_t* mem, int stride, void *userdata);

  uint8_t* get_image_plane_at_pos(int cIdx, int xpos,int ypos)
  {
    int stride = get_image_stride(cIdx);
    return pixels[cIdx] + xpos + ypos*stride;
  }


  /// xpos;ypos in actual plane resolution
  template <class pixel_t>
  pixel_t* get_image_plane_at_pos_NEW(int cIdx, int xpos,int ypos)
  {
    int stride = get_image_stride(cIdx);
    return (pixel_t*)(pixels[cIdx] + (xpos + ypos*stride)*sizeof(pixel_t));
  }

  const uint8_t* get_image_plane_at_pos(int cIdx, int xpos,int ypos) const
  {
    int stride = get_image_stride(cIdx);
    return pixels[cIdx] + xpos + ypos*stride;
  }

  void* get_image_plane_at_pos_any_depth(int cIdx, int xpos,int ypos)
  {
    int stride = get_image_stride(cIdx);
    return pixels[cIdx] + ((xpos + ypos*stride) << bpp_shift[cIdx]);
  }

  const void* get_image_plane_at_pos_any_depth(int cIdx, int xpos,int ypos) const
  {
    int stride = get_image_stride(cIdx);
    return pixels[cIdx] + ((xpos + ypos*stride) << bpp_shift[cIdx]);
  }

  /* Number of pixels in one row (not number of bytes).
   */
  int get_image_stride(int cIdx) const
  {
    if (cIdx==0) return stride;
    else         return chroma_stride;
  }

  int get_luma_stride() const { return stride; }
  int get_chroma_stride() const { return chroma_stride; }

  int get_width (int cIdx=0) const { return cIdx==0 ? width  : chroma_width;  }
  int get_height(int cIdx=0) const { return cIdx==0 ? height : chroma_height; }

  enum de265_chroma get_chroma_format() const { return chroma_format; }

  int get_bit_depth(int cIdx) const {
    if (cIdx==0) return sps->BitDepth_Y;
    else         return sps->BitDepth_C;
  }

  int get_bytes_per_pixel(int cIdx) const {
    return (get_bit_depth(cIdx)+7)/8;
  }

  bool high_bit_depth(int cIdx) const {
    return get_bit_depth(cIdx)>8;
  }

  bool can_be_released() const { return PicOutputFlag==false && PicState==UnusedForReference; }


  void add_slice_segment_header(slice_segment_header* shdr) {
    shdr->slice_index = slices.size();
    slices.push_back(shdr);
  }


  bool available_zscan(int xCurr,int yCurr, int xN,int yN) const;

  bool available_pred_blk(int xC,int yC, int nCbS,
                          int xP, int yP, int nPbW, int nPbH, int partIdx,
                          int xN,int yN) const;


  static de265_image_allocation default_image_allocation;

  void printBlk(const char* title, int x0,int y0,int blkSize,int cIdx) const {
    ::printBlk(title, get_image_plane_at_pos(cIdx,x0,y0),
               blkSize, get_image_stride(cIdx));
  }

private:
  uint32_t ID;
  static uint32_t s_next_image_ID;

  uint8_t* pixels[3];
  uint8_t  bpp_shift[3];  // 0 for 8 bit, 1 for 16 bit

  enum de265_chroma chroma_format;

  int width, height;  // size in luma pixels

  int chroma_width, chroma_height;
  int stride, chroma_stride;

public:
  uint8_t BitDepth_Y, BitDepth_C;
  uint8_t SubWidthC, SubHeightC;
  std::vector<slice_segment_header*> slices;

public:

  // --- conformance cropping window ---

  uint8_t* pixels_confwin[3];   // pointer to pixels in the conformance window

  int width_confwin, height_confwin;
  int chroma_width_confwin, chroma_height_confwin;

  // --- decoding info ---

  // If PicOutputFlag==false && PicState==UnusedForReference, image buffer is free.

  int  picture_order_cnt_lsb;
  int  PicOrderCntVal;
  enum PictureState PicState;
  bool PicOutputFlag;

  int32_t removed_at_picture_id;

  const video_parameter_set& get_vps() const { return *vps; }
  const seq_parameter_set& get_sps() const { return *sps; }
  const pic_parameter_set& get_pps() const { return *pps; }

  bool has_vps() const { return (bool)vps; }
  bool has_sps() const { return (bool)sps; }
  bool has_pps() const { return (bool)pps; }

  std::shared_ptr<const seq_parameter_set> get_shared_sps() { return sps; }

  //std::shared_ptr<const seq_parameter_set> get_shared_sps() const { return sps; }
  //std::shared_ptr<const pic_parameter_set> get_shared_pps() const { return pps; }

  decoder_context*    decctx;
  //class encoder_context*    encctx;

  int number_of_ctbs() const { return ctb_info.size(); }

private:
  // The image also keeps a reference to VPS/SPS/PPS, because when decoding is delayed,
  // the currently active parameter sets in the decctx might already have been replaced
  // with new parameters.
  std::shared_ptr<const video_parameter_set> vps;
  std::shared_ptr<const seq_parameter_set>   sps;  // the SPS used for decoding this image
  std::shared_ptr<const pic_parameter_set>   pps;  // the PPS used for decoding this image

  MetaDataArray<CTB_info>    ctb_info;
  MetaDataArray<CB_ref_info> cb_info;
  MetaDataArray<PBMotion>    pb_info;
  MetaDataArray<uint8_t>     intraPredMode;
  MetaDataArray<uint8_t>     intraPredModeC;
  MetaDataArray<uint8_t>     tu_info;
  MetaDataArray<uint8_t>     deblk_info;

public:
  // --- meta information ---

  de265_PTS pts;
  void*     user_data;
  void*     plane_user_data[3];  // this is logically attached to the pixel data pointers
  de265_image_allocation image_allocation_functions; // the functions used for memory allocation

  /*
  void (*encoder_image_release_func)(en265_encoder_context*,
                                     de265_image*,
                                     void* userdata);
  */

  uint8_t integrity; /* Whether an error occurred while the image was decoded.
                        When generated, this is initialized to INTEGRITY_CORRECT,
                        and changed on decoding errors.
                      */
  bool sei_hash_check_result;

  nal_header nal_hdr;

  // --- multi core ---

  de265_progress_lock* ctb_progress; // ctb_info_size

  void mark_all_CTB_progress(int progress) {
    for (int i=0;i<ctb_info.data_size;i++) {
      ctb_progress[i].set_progress(progress);
    }
  }


  void thread_start(int nThreads);
  void thread_run(const thread_task*);
  void thread_blocks();
  void thread_unblocks();
  /* NOTE: you should not access any data in the thread_task after
     calling this, as this function may unlock other threads that
     will push this image to the output queue and free all decoder data. */
  void thread_finishes(const thread_task*);

  void wait_for_progress(thread_task* task, int ctbx,int ctby, int progress);
  void wait_for_progress(thread_task* task, int ctbAddrRS, int progress);

  void wait_for_completion();  // block until image is decoded by background threads
  bool debug_is_completed() const;
  int  num_threads_active() const { return nThreadsRunning + nThreadsBlocked; } // for debug only

  //private:
  int   nThreadsQueued;
  int   nThreadsRunning;
  int   nThreadsBlocked;
  int   nThreadsFinished;
  int   nThreadsTotal;

  // ALIGNED_8(de265_sync_int tasks_pending); // number of tasks pending to complete decoding
  de265_mutex mutex;
  de265_cond  finished_cond;

public:

  /* Clear all CTB/CB/PB decoding data of this image.
     All CTB's processing states are set to 'unprocessed'.
  */
  void clear_metadata();


  // --- CB metadata access ---

  void set_pred_mode(int x,int y, int log2BlkWidth, enum PredMode mode)
  {
    SET_CB_BLK(x,y,log2BlkWidth, PredMode, mode);
  }

  void fill_pred_mode(enum PredMode mode)
  {
    for (int i=0;i<cb_info.data_size;i++)
      { cb_info[i].PredMode = MODE_INTRA; }
  }

  enum PredMode get_pred_mode(int x,int y) const
  {
    return (enum PredMode)cb_info.get(x,y).PredMode;
  }

  uint8_t get_cu_skip_flag(int x,int y) const
  {
    return get_pred_mode(x,y)==MODE_SKIP;
  }

  void set_pcm_flag(int x,int y, int log2BlkWidth, uint8_t value=1)
  {
    SET_CB_BLK(x,y,log2BlkWidth, pcm_flag, value);

    // TODO: in the encoder, we somewhere have to clear this
    ctb_info.get(x,y).has_pcm_or_cu_transquant_bypass = true;
  }

  int  get_pcm_flag(int x,int y) const
  {
    return cb_info.get(x,y).pcm_flag;
  }

  void set_cu_transquant_bypass(int x,int y, int log2BlkWidth, uint8_t value=1)
  {
    SET_CB_BLK(x,y,log2BlkWidth, cu_transquant_bypass, value);

    // TODO: in the encoder, we somewhere have to clear this
    ctb_info.get(x,y).has_pcm_or_cu_transquant_bypass = true;
  }

  int  get_cu_transquant_bypass(int x,int y) const
  {
    return cb_info.get(x,y).cu_transquant_bypass;
  }

  void set_log2CbSize(int x0, int y0, int log2CbSize, bool fill)
  {
    // In theory, we could assume that remaining cb_info blocks are initialized to zero.
    // But in corrupted streams, slices may overlap and set contradicting log2CbSizes.
    // We also need this for encoding.
    if (fill) {
      SET_CB_BLK(x0,y0,log2CbSize, log2CbSize, 0);
    }

    cb_info.get(x0,y0).log2CbSize = log2CbSize;
  }

  int  get_log2CbSize(int x0, int y0) const
  {
    return (enum PredMode)cb_info.get(x0,y0).log2CbSize;
  }

  // coordinates in CB units
  int  get_log2CbSize_cbUnits(int xCb, int yCb) const
  {
    return (enum PredMode)cb_info[ xCb + yCb*cb_info.width_in_units ].log2CbSize;
  }

  void set_PartMode(int x,int y, enum PartMode mode)
  {
    cb_info.get(x,y).PartMode = mode;
  }

  enum PartMode get_PartMode(int x,int y) const
  {
    return (enum PartMode)cb_info.get(x,y).PartMode;
  }

  void set_ctDepth(int x,int y, int log2BlkWidth, int depth)
  {
    SET_CB_BLK(x,y,log2BlkWidth, ctDepth, depth);
  }

  int get_ctDepth(int x,int y) const
  {
    return cb_info.get(x,y).ctDepth;
  }

  void set_QPY(int x,int y, int log2BlkWidth, int QP_Y)
  {
    SET_CB_BLK (x, y, log2BlkWidth, QP_Y, QP_Y);
  }

  int  get_QPY(int x0,int y0) const
  {
    return cb_info.get(x0,y0).QP_Y;
  }

  // --- TU metadata access ---

  void set_split_transform_flag(int x0,int y0,int trafoDepth)
  {
    tu_info.get(x0,y0) |= (1<<trafoDepth);
  }

  void clear_split_transform_flags(int x0,int y0,int log2CbSize)
  {
    CLEAR_TB_BLK (x0,y0, log2CbSize);
  }

  int  get_split_transform_flag(int x0,int y0,int trafoDepth) const
  {
    return (tu_info.get(x0,y0) & (1<<trafoDepth));
  }

  void set_nonzero_coefficient(int x,int y, int log2TrafoSize)
  {
    const int tuX = x >> tu_info.log2unitSize;
    const int tuY = y >> tu_info.log2unitSize;
    const int width = 1 << (log2TrafoSize - tu_info.log2unitSize);

    for (int tuy=tuY;tuy<tuY+width;tuy++)
      for (int tux=tuX;tux<tuX+width;tux++)
        {
          tu_info[ tux + tuy*tu_info.width_in_units ] |= TU_FLAG_NONZERO_COEFF;
        }
  }

  int  get_nonzero_coefficient(int x,int y) const
  {
    return tu_info.get(x,y) & TU_FLAG_NONZERO_COEFF;
  }


  // --- intraPredMode metadata access ---

  enum IntraPredMode get_IntraPredMode(int x,int y) const
  {
    uint8_t ipm = intraPredMode.get(x,y);

    // sanitize values if IPM is uninitialized (because of earlier read error)
    if (ipm > 34) {
      ipm = 0;
    }

    return static_cast<enum IntraPredMode>(ipm);
  }

  enum IntraPredMode get_IntraPredMode_atIndex(int idx) const
  {
    return (enum IntraPredMode)intraPredMode[idx];
  }

  void set_IntraPredMode(int PUidx,int log2blkSize, enum IntraPredMode mode)
  {
    int pbSize = 1<<(log2blkSize - intraPredMode.log2unitSize);

    for (int y=0;y<pbSize;y++)
      for (int x=0;x<pbSize;x++)
        intraPredMode[PUidx + x + y*intraPredMode.width_in_units] = mode;
  }

  void set_IntraPredMode(int x0,int y0,int log2blkSize,
                         enum IntraPredMode mode)
  {
    int pbSize = 1<<(log2blkSize - intraPredMode.log2unitSize);
    int PUidx  = (x0>>sps->Log2MinPUSize) + (y0>>sps->Log2MinPUSize)*sps->PicWidthInMinPUs;

    for (int y=0;y<pbSize;y++)
      for (int x=0;x<pbSize;x++) {
        assert(x < sps->PicWidthInMinPUs);
        assert(y < sps->PicHeightInMinPUs);

        int idx = PUidx + x + y*intraPredMode.width_in_units;
        assert(idx<intraPredMode.data_size);
        intraPredMode[idx] = mode;
      }
  }


  enum IntraPredMode get_IntraPredModeC(int x,int y) const
  {
    return (enum IntraPredMode)(intraPredModeC.get(x,y) & 0x3f);
  }

  bool is_IntraPredModeC_Mode4(int x,int y) const
  {
    return intraPredModeC.get(x,y) & 0x80;
  }

  void set_IntraPredModeC(int x0,int y0,int log2blkSize, enum IntraPredMode mode,
                          bool is_mode4)
  {
    uint8_t combinedValue = mode;
    if (is_mode4) combinedValue |= 0x80;

    int pbSize = 1<<(log2blkSize - intraPredMode.log2unitSize);
    int PUidx  = (x0>>sps->Log2MinPUSize) + (y0>>sps->Log2MinPUSize)*sps->PicWidthInMinPUs;

    for (int y=0;y<pbSize;y++)
      for (int x=0;x<pbSize;x++) {
        assert(x<sps->PicWidthInMinPUs);
        assert(y<sps->PicHeightInMinPUs);

        int idx = PUidx + x + y*intraPredModeC.width_in_units;
        assert(idx<intraPredModeC.data_size);
        intraPredModeC[idx] = combinedValue;
      }
  }


  /*
  // NOTE: encoder only
  void set_ChromaIntraPredMode(int x,int y,int log2BlkWidth, enum IntraChromaPredMode mode)
  {
    SET_CB_BLK (x, y, log2BlkWidth, intra_chroma_pred_mode, mode);
  }

  // NOTE: encoder only
  enum IntraChromaPredMode get_ChromaIntraPredMode(int x,int y) const
  {
    return (enum IntraChromaPredMode)(cb_info.get(x,y).intra_chroma_pred_mode);
  }
  */

  // --- CTB metadata access ---

  // address of first CTB in slice
  void set_SliceAddrRS(int ctbX, int ctbY, int SliceAddrRS)
  {
    if (ctbX >= ctb_info.width_in_units || ctbY >= ctb_info.height_in_units) {
      return;
    }

    int idx = ctbX + ctbY*ctb_info.width_in_units;
    ctb_info[idx].SliceAddrRS = SliceAddrRS;
  }

  int  get_SliceAddrRS(int ctbX, int ctbY) const
  {
    return ctb_info[ctbX + ctbY*ctb_info.width_in_units].SliceAddrRS;
  }

  int  get_SliceAddrRS_atCtbRS(int ctbRS) const
  {
    return ctb_info[ctbRS].SliceAddrRS;
  }


  void set_SliceHeaderIndex(int x, int y, int SliceHeaderIndex)
  {
    ctb_info.get(x,y).SliceHeaderIndex = SliceHeaderIndex;
  }

  int  get_SliceHeaderIndex(int x, int y) const
  {
    return ctb_info.get(x,y).SliceHeaderIndex;
  }

  int  get_SliceHeaderIndexCtb(int ctbX, int ctbY) const
  {
    return ctb_info[ctbX + ctbY*ctb_info.width_in_units].SliceHeaderIndex;
  }

  int  get_SliceHeaderIndex_atIndex(int ctb) const
  {
    return ctb_info[ctb].SliceHeaderIndex;
  }

  bool is_SliceHeader_available(int x,int y) const
  {
    int idx = ctb_info.get(x,y).SliceHeaderIndex;
    return idx >= 0 && idx < slices.size();
  }

  slice_segment_header* get_SliceHeader(int x, int y)
  {
    int idx = get_SliceHeaderIndex(x,y);
    if (idx >= slices.size()) { return NULL; }
    return slices[idx];
  }

  slice_segment_header* get_SliceHeaderCtb(int ctbX, int ctbY)
  {
    int idx = get_SliceHeaderIndexCtb(ctbX,ctbY);
    if (idx >= slices.size()) { return NULL; }
    return slices[idx];
  }

  const slice_segment_header* get_SliceHeaderCtb(int ctbX, int ctbY) const
  {
    int idx = get_SliceHeaderIndexCtb(ctbX,ctbY);
    if (idx >= slices.size()) { return NULL; }
    return slices[idx];
  }

  void set_sao_info(int ctbX,int ctbY,const sao_info* saoinfo)
  {
    sao_info* sao = &ctb_info[ctbX + ctbY*ctb_info.width_in_units].saoInfo;

    memcpy(sao,
           saoinfo,
           sizeof(sao_info));
  }

  const sao_info* get_sao_info(int ctbX,int ctbY) const
  {
    return &ctb_info[ctbX + ctbY*ctb_info.width_in_units].saoInfo;
  }


  void set_CtbDeblockFlag(int ctbX, int ctbY, bool flag)
  {
    int idx = ctbX + ctbY*ctb_info.width_in_units;
    ctb_info[idx].deblock = flag;
  }

  bool get_CtbDeblockFlag(int ctbX, int ctbY) const
  {
    return ctb_info[ctbX + ctbY*ctb_info.width_in_units].deblock;
  }


  bool get_CTB_has_pcm_or_cu_transquant_bypass(int ctbX,int ctbY) const
  {
    int idx = ctbX + ctbY*ctb_info.width_in_units;
    return ctb_info[idx].has_pcm_or_cu_transquant_bypass;
  }



  // --- DEBLK metadata access ---

  int  get_deblk_width()  const { return deblk_info.width_in_units; }
  int  get_deblk_height() const { return deblk_info.height_in_units; }

  void    set_deblk_flags(int x0,int y0, uint8_t flags)
  {
    const int xd = x0/4;
    const int yd = y0/4;

    if (xd<deblk_info.width_in_units &&
        yd<deblk_info.height_in_units) {
      deblk_info[xd + yd*deblk_info.width_in_units] |= flags;
    }
  }

  uint8_t get_deblk_flags(int x0,int y0) const
  {
    const int xd = x0/4;
    const int yd = y0/4;

    return deblk_info[xd + yd*deblk_info.width_in_units];
  }

  void    set_deblk_bS(int x0,int y0, uint8_t bS)
  {
    uint8_t* data = &deblk_info[x0/4 + y0/4*deblk_info.width_in_units];
    *data &= ~DEBLOCK_BS_MASK;
    *data |= bS;
  }

  uint8_t get_deblk_bS(int x0,int y0) const
  {
    return deblk_info[x0/4 + y0/4*deblk_info.width_in_units] & DEBLOCK_BS_MASK;
  }


  // --- PB metadata access ---

  const PBMotion& get_mv_info(int x,int y) const
  {
    return pb_info.get(x,y);
  }

  void set_mv_info(int x,int y, int nPbW,int nPbH, const PBMotion& mv);

  // --- value logging ---

  void printBlk(int x0,int y0, int cIdx, int log2BlkSize);
};


#endif
