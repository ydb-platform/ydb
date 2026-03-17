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

#ifndef DE265_SOP_H
#define DE265_SOP_H

#include "libde265/image.h"
#include "libde265/sps.h"
#include "libde265/configparam.h"
//#include "libde265/encoder/encoder-context.h"
#include "libde265/encoder/encpicbuf.h"

#include <deque>
#include <vector>

/*
struct refpic_set
{
  std::vector<int> l0;
  std::vector<int> l1;
};
*/

class encoder_context;


class pic_order_counter
{
 public:
  pic_order_counter() { mFrameNumber=0; mPOC=0; mNumLsbBits=6; }

  void reset_poc() { mPOC=0; }

  int get_frame_number() const { return mFrameNumber; }

  int get_pic_order_count() const { return mPOC; }
  int get_pic_order_count_lsb() const {
    return mPOC & ((1<<mNumLsbBits)-1);
  }

  void advance_frame(int n=1) { mFrameNumber+=n; mPOC+=n; }

  void set_num_poc_lsb_bits(int n) { mNumLsbBits=n; }
  int  get_num_poc_lsb_bits() const { return mNumLsbBits; }

 private:
  int mFrameNumber;
  int mPOC;
  int mNumLsbBits;
};


class sop_creator : public pic_order_counter
{
 public:
  sop_creator() { mEncCtx=NULL; mEncPicBuf=NULL; }
  virtual ~sop_creator() { }

  void set_encoder_context(encoder_context* encctx) { mEncCtx=encctx; }
  void set_encoder_picture_buffer(encoder_picture_buffer* encbuf) { mEncPicBuf=encbuf; }

  /* Fills in the following fields:
     - SPS.ref_pic_sets
     - SPS.log2_max_pic_order_cnt_lsb
   */
  virtual void set_SPS_header_values() = 0;

  /* Fills in the following fields:
     - NAL.nal_type
     - SHDR.slice_type
     - SHDR.slice_pic_order_cnt_lsb
     - IMGDATA.references
   */
  virtual void insert_new_input_image(de265_image*) = 0;
  virtual void insert_end_of_stream() { mEncPicBuf->insert_end_of_stream(); }

  virtual int  get_number_of_temporal_layers() const { return 1; }

  //virtual std::vector<refpic_set> get_sps_refpic_sets() const = 0;

 protected:
  encoder_context*        mEncCtx;
  encoder_picture_buffer* mEncPicBuf;
};



class sop_creator_intra_only : public sop_creator
{
 public:
  sop_creator_intra_only();

  virtual void set_SPS_header_values();
  virtual void insert_new_input_image(de265_image* img);
};



class sop_creator_trivial_low_delay : public sop_creator
{
 public:
  struct params {
    params() {
      intraPeriod.set_ID("sop-lowDelay-intraPeriod");
      intraPeriod.set_minimum(1);
      intraPeriod.set_default(250);
    }

    void registerParams(config_parameters& config) {
      config.add_option(&intraPeriod);
    }

    option_int intraPeriod;
  };

  sop_creator_trivial_low_delay();

  void setParams(const params& p) { mParams=p; }

  virtual void set_SPS_header_values();
  virtual void insert_new_input_image(de265_image* img);

 private:
  params mParams;

  bool isIntra(int frame) const { return (frame % mParams.intraPeriod)==0; }
};


#endif
