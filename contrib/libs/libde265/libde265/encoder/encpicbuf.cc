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

#include "libde265/encoder/encpicbuf.h"
#include "libde265/util.h"


encoder_picture_buffer::encoder_picture_buffer()
{
}

encoder_picture_buffer::~encoder_picture_buffer()
{
  flush_images();
}


image_data::image_data()
{
  //printf("new %p\n",this);

  frame_number = 0;

  input = NULL;
  prediction = NULL;
  reconstruction = NULL;

  // SOP metadata

  sps_index = -1;
  skip_priority = 0;
  is_intra = true;

  state = state_unprocessed;

  is_in_output_queue = true;
}

image_data::~image_data()
{
  //printf("delete %p\n",this);

  delete input;
  // TODO: this could still be referenced in the packet output queue, so the
  //       images should really be refcounted. release for now to prevent leaks
  delete reconstruction;
  delete prediction;
}


// --- input pushed by the input process ---

void encoder_picture_buffer::reset()
{
  flush_images();

  mEndOfStream = false;
}


void encoder_picture_buffer::flush_images()
{
  while (!mImages.empty()) {
    delete mImages.front();
    mImages.pop_front();
  }
}


image_data* encoder_picture_buffer::insert_next_image_in_encoding_order(const de265_image* img,
                                                                        int frame_number)
{
  image_data* data = new image_data();
  data->frame_number = frame_number;
  data->input = img;
  data->shdr.set_defaults();

  mImages.push_back(data);

  return data;
}

void encoder_picture_buffer::insert_end_of_stream()
{
  mEndOfStream = true;
}


// --- SOP structure ---

void image_data::set_intra()
{
  is_intra = true;
}

void image_data::set_NAL_type(uint8_t nalType)
{
  nal.nal_unit_type = nalType;
}

void image_data::set_references(int sps_index, // -1 -> custom
                                const std::vector<int>& l0,
                                const std::vector<int>& l1,
                                const std::vector<int>& lt,
                                const std::vector<int>& keepMoreReferences)
{
  this->sps_index = sps_index;
  ref0 = l0;
  ref1 = l1;
  longterm = lt;
  keep = keepMoreReferences;


  // TODO: pps.num_ref_idx_l0_default_active

  shdr.num_ref_idx_l0_active = l0.size();
  //shdr.num_ref_idx_l1_active = l1.size();

  assert(l0.size() < MAX_NUM_REF_PICS);
  for (size_t i=0;i<l0.size();i++) {
    shdr.RefPicList[0][i] = l0[i];
  }

  /*
  assert(l1.size() < MAX_NUM_REF_PICS);
  for (int i=0;i<l1.size();i++) {
    shdr.RefPicList[1][i] = l1[i];
  }
  */
}

void image_data::set_NAL_temporal_id(int temporal_id)
{
  this->nal.nuh_temporal_id = temporal_id;
}

void image_data::set_skip_priority(int skip_priority)
{
  this->skip_priority = skip_priority;
}

void encoder_picture_buffer::sop_metadata_commit(int frame_number)
{
  image_data* data = mImages.back();
  assert(data->frame_number == frame_number);

  data->state = image_data::state_sop_metadata_available;
}



// --- infos pushed by encoder ---

void encoder_picture_buffer::mark_encoding_started(int frame_number)
{
  image_data* data = get_picture(frame_number);

  data->state = image_data::state_encoding;
}

void encoder_picture_buffer::set_prediction_image(int frame_number, de265_image* pred)
{
  image_data* data = get_picture(frame_number);

  data->prediction = pred;
}

void encoder_picture_buffer::set_reconstruction_image(int frame_number, de265_image* reco)
{
  image_data* data = get_picture(frame_number);

  data->reconstruction = reco;
}

void encoder_picture_buffer::mark_encoding_finished(int frame_number)
{
  image_data* data = get_picture(frame_number);

  data->state = image_data::state_keep_for_reference;


  // --- delete images that are not required anymore ---

  // first, mark all images unused

#ifdef FOR_LOOP_AUTO_SUPPORT
  FOR_LOOP(auto, imgdata, mImages) {
#else
  FOR_LOOP(image_data *, imgdata, mImages) {
#endif
    imgdata->mark_used = false;
  }

  // mark all images that will be used later

  FOR_LOOP(int, f, data->ref0)     { get_picture(f)->mark_used=true; }
  FOR_LOOP(int, f, data->ref1)     { get_picture(f)->mark_used=true; }
  FOR_LOOP(int, f, data->longterm) { get_picture(f)->mark_used=true; }
  FOR_LOOP(int, f, data->keep)     { get_picture(f)->mark_used=true; }
  data->mark_used=true;

  // copy over all images that we still keep

  std::deque<image_data*> newImageSet;
#ifdef FOR_LOOP_AUTO_SUPPORT
  FOR_LOOP(auto, imgdata, mImages) {
#else
  FOR_LOOP(image_data *, imgdata, mImages) {
#endif
    if (imgdata->mark_used || imgdata->is_in_output_queue) {
      imgdata->reconstruction->PicState = UsedForShortTermReference; // TODO: this is only a hack

      newImageSet.push_back(imgdata);
    }
    else {
      // image is not needed anymore for reference, remove it from EncPicBuf

      delete imgdata;
    }
  }

  mImages = newImageSet;
}



// --- data access ---

bool encoder_picture_buffer::have_more_frames_to_encode() const
{
  for (size_t i=0;i<mImages.size();i++) {
    if (mImages[i]->state < image_data::state_encoding) {
      return true;
    }
  }

  return false;
}


image_data* encoder_picture_buffer::get_next_picture_to_encode()
{
  for (size_t i=0;i<mImages.size();i++) {
    if (mImages[i]->state < image_data::state_encoding) {
      return mImages[i];
    }
  }

  return NULL;
}


const image_data* encoder_picture_buffer::get_picture(int frame_number) const
{
  for (size_t i=0;i<mImages.size();i++) {
    if (mImages[i]->frame_number == frame_number)
      return mImages[i];
  }

  assert(false);
  return NULL;
}


image_data* encoder_picture_buffer::get_picture(int frame_number)
{
  for (size_t i=0;i<mImages.size();i++) {
    if (mImages[i]->frame_number == frame_number)
      return mImages[i];
  }

  assert(false);
  return NULL;
}


bool encoder_picture_buffer::has_picture(int frame_number) const
{
  for (size_t i=0;i<mImages.size();i++) {
    if (mImages[i]->frame_number == frame_number)
      return true;
  }

  return false;
}


void encoder_picture_buffer::mark_image_is_outputted(int frame_number)
{
  image_data* idata = get_picture(frame_number);
  assert(idata);

  idata->is_in_output_queue = false;
}


void encoder_picture_buffer::release_input_image(int frame_number)
{
  image_data* idata = get_picture(frame_number);
  assert(idata);

  delete idata->input;
  idata->input = NULL;
}
