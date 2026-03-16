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

#include "libde265/encoder/sop.h"
#include "libde265/encoder/encoder-context.h"


sop_creator_intra_only::sop_creator_intra_only()
{
}


void sop_creator_intra_only::set_SPS_header_values()
{
  mEncCtx->get_sps().log2_max_pic_order_cnt_lsb = get_num_poc_lsb_bits();
}


void sop_creator_intra_only::insert_new_input_image(de265_image* img)
{
  img->PicOrderCntVal = get_pic_order_count();

  reset_poc();
  int poc = get_pic_order_count();

  assert(mEncPicBuf);
  image_data* imgdata = mEncPicBuf->insert_next_image_in_encoding_order(img, get_frame_number());

  imgdata->set_intra();
  imgdata->set_NAL_type(NAL_UNIT_IDR_N_LP);
  imgdata->shdr.slice_type = SLICE_TYPE_I;
  imgdata->shdr.slice_pic_order_cnt_lsb = get_pic_order_count_lsb();

  mEncPicBuf->sop_metadata_commit(get_frame_number());

  advance_frame();
}


// ---------------------------------------------------------------------------


sop_creator_trivial_low_delay::sop_creator_trivial_low_delay()
{
}


void sop_creator_trivial_low_delay::set_SPS_header_values()
{
  ref_pic_set rps;
  rps.DeltaPocS0[0] = -1;
  rps.UsedByCurrPicS0[0] = true;
  rps.NumNegativePics = 1;
  rps.NumPositivePics = 0;
  rps.compute_derived_values();
  mEncCtx->get_sps().ref_pic_sets.push_back(rps);
  mEncCtx->get_sps().log2_max_pic_order_cnt_lsb = get_num_poc_lsb_bits();
}


void sop_creator_trivial_low_delay::insert_new_input_image(de265_image* img)
{
  img->PicOrderCntVal = get_pic_order_count();

  int frame = get_frame_number();

  std::vector<int> l0, l1, empty;
  if (!isIntra(frame)) {
    l0.push_back(frame-1);
  }

  assert(mEncPicBuf);
  image_data* imgdata = mEncPicBuf->insert_next_image_in_encoding_order(img, get_frame_number());

  if (isIntra(frame)) {
    reset_poc();
    imgdata->set_intra();
    imgdata->set_NAL_type(NAL_UNIT_IDR_N_LP);
    imgdata->shdr.slice_type = SLICE_TYPE_I;
  } else {
    imgdata->set_references(0, l0,l1, empty,empty);
    imgdata->set_NAL_type(NAL_UNIT_TRAIL_R);
    imgdata->shdr.slice_type = SLICE_TYPE_P;
  }
  imgdata->shdr.slice_pic_order_cnt_lsb = get_pic_order_count_lsb();
  mEncPicBuf->sop_metadata_commit(get_frame_number());

  advance_frame();
}
