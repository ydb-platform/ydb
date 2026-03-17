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

#include "dpb.h"
#include "decctx.h"
#include <string.h>
#include <assert.h>


#define DPB_DEFAULT_MAX_IMAGES  30


decoded_picture_buffer::decoded_picture_buffer()
{
  max_images_in_DPB  = DPB_DEFAULT_MAX_IMAGES;
  norm_images_in_DPB = DPB_DEFAULT_MAX_IMAGES;
}


decoded_picture_buffer::~decoded_picture_buffer()
{
  for (size_t i=0;i<dpb.size();i++)
    delete dpb[i];
}


void decoded_picture_buffer::log_dpb_content() const
{
  for (size_t i=0;i<dpb.size();i++) {
    loginfo(LogHighlevel, " DPB %d: POC=%d, ID=%d %s %s\n", i,
            dpb[i]->PicOrderCntVal,
            dpb[i]->get_ID(),
            dpb[i]->PicState == UnusedForReference ? "unused" :
            dpb[i]->PicState == UsedForShortTermReference ? "short-term" : "long-term",
            dpb[i]->PicOutputFlag ? "output" : "---");
  }
}


bool decoded_picture_buffer::has_free_dpb_picture(bool high_priority) const
{
  // we will always adapt the buffer to insert high-priority images
  if (high_priority) return true;

  // quick test to check for free slots
  if (dpb.size() < max_images_in_DPB) return true;

  // scan for empty slots
  for (size_t i=0;i<dpb.size();i++) {
    if (dpb[i]->PicOutputFlag==false && dpb[i]->PicState == UnusedForReference) {
      return true;
    }
  }

  return false;
}


int decoded_picture_buffer::DPB_index_of_picture_with_POC(int poc, int currentID, bool preferLongTerm) const
{
  logdebug(LogHeaders,"DPB_index_of_picture_with_POC POC=%d\n",poc);

  //log_dpb_content(ctx);
  //loginfo(LogDPB,"searching for short-term reference POC=%d\n",poc);

  if (preferLongTerm) {
    for (size_t k=0;k<dpb.size();k++) {
      if (dpb[k]->PicOrderCntVal == poc &&
          dpb[k]->removed_at_picture_id > currentID &&
          dpb[k]->PicState == UsedForLongTermReference) {
        return k;
      }
    }
  }

  for (size_t k=0;k<dpb.size();k++) {
    if (dpb[k]->PicOrderCntVal == poc &&
        dpb[k]->removed_at_picture_id > currentID &&
        dpb[k]->PicState != UnusedForReference) {
      return k;
    }
  }

  return -1;
}


int decoded_picture_buffer::DPB_index_of_picture_with_LSB(int lsb, int currentID, bool preferLongTerm) const
{
  logdebug(LogHeaders,"get access to picture with LSB %d from DPB\n",lsb);

  if (preferLongTerm) {
    for (size_t k=0;k<dpb.size();k++) {
      if (dpb[k]->picture_order_cnt_lsb == lsb &&
          dpb[k]->removed_at_picture_id > currentID &&
          dpb[k]->PicState == UsedForLongTermReference) {
        return k;
      }
    }
  }

  for (size_t k=0;k<dpb.size();k++) {
    if (dpb[k]->picture_order_cnt_lsb == lsb &&
        dpb[k]->removed_at_picture_id > currentID &&
        dpb[k]->PicState != UnusedForReference) {
      return k;
    }
  }

  return -1;
}


int decoded_picture_buffer::DPB_index_of_picture_with_ID(int id) const
{
  logdebug(LogHeaders,"get access to picture with ID %d from DPB\n",id);

  for (size_t k=0;k<dpb.size();k++) {
    if (dpb[k]->get_ID() == id) {
      return k;
    }
  }

  return -1;
}


void decoded_picture_buffer::output_next_picture_in_reorder_buffer()
{
  assert(!reorder_output_queue.empty());

  // search for picture in reorder buffer with minimum POC

  int minPOC = reorder_output_queue[0]->PicOrderCntVal;
  int minIdx = 0;
  for (size_t i=1;i<reorder_output_queue.size();i++)
    {
      if (reorder_output_queue[i]->PicOrderCntVal < minPOC) {
        minPOC = reorder_output_queue[i]->PicOrderCntVal;
        minIdx = i;
      }
    }


  // put image into output queue

  image_output_queue.push_back(reorder_output_queue[minIdx]);


  // remove image from reorder buffer

  reorder_output_queue[minIdx] = reorder_output_queue.back();
  reorder_output_queue.pop_back();
}


bool decoded_picture_buffer::flush_reorder_buffer()
{
  // return 'false' when there are no pictures in reorder buffer
  if (reorder_output_queue.empty()) return false;

  while (!reorder_output_queue.empty()) {
    output_next_picture_in_reorder_buffer();
  }

  return true;
}


void decoded_picture_buffer::clear()
{
  for (size_t i=0;i<dpb.size();i++) {
    if (dpb[i]->PicOutputFlag ||
        dpb[i]->PicState != UnusedForReference)
      {
        dpb[i]->PicOutputFlag = false;
        dpb[i]->PicState = UnusedForReference;
        dpb[i]->release();
      }
  }

  reorder_output_queue.clear();
  image_output_queue.clear();
}


int decoded_picture_buffer::new_image(std::shared_ptr<const seq_parameter_set> sps,
                                      decoder_context* decctx,
                                      de265_PTS pts, void* user_data, bool isOutputImage)
{
  loginfo(LogHeaders,"DPB::new_image\n");
  log_dpb_content();

  // --- search for a free slot in the DPB ---

  int free_image_buffer_idx = -DE265_ERROR_IMAGE_BUFFER_FULL;
  for (size_t i=0;i<dpb.size();i++) {
    if (dpb[i]->can_be_released()) {
      dpb[i]->release(); /* TODO: this is surely not the best place to free the image, but
                            we have to do it here because releasing it in de265_release_image()
                            would break the API compatibility. */

      free_image_buffer_idx = i;
      break;
    }
  }


  // Try to free a buffer at the end if the DPB got too large.
  /* This should also probably move to a better place as soon as the API allows for this. */

  if (dpb.size() > norm_images_in_DPB &&           // buffer too large
      free_image_buffer_idx != dpb.size()-1 &&     // last slot not reused in this alloc
      dpb.back()->can_be_released())               // last slot is free
    {
      delete dpb.back();
      dpb.pop_back();
    }


  // create a new image slot if no empty slot remaining

  if (free_image_buffer_idx == -DE265_ERROR_IMAGE_BUFFER_FULL) {
    free_image_buffer_idx = dpb.size();
    dpb.push_back(new de265_image);
  }


  // --- allocate new image ---

  if (free_image_buffer_idx<0) {
    return free_image_buffer_idx;
  }

  de265_image* img = dpb[free_image_buffer_idx];

  int w = sps->pic_width_in_luma_samples;
  int h = sps->pic_height_in_luma_samples;

  enum de265_chroma chroma;
  switch (sps->chroma_format_idc) {
  case 0: chroma = de265_chroma_mono; break;
  case 1: chroma = de265_chroma_420;  break;
  case 2: chroma = de265_chroma_422;  break;
  case 3: chroma = de265_chroma_444;  break;
  default: chroma = de265_chroma_420; assert(0); break; // should never happen
  }

  de265_error error = img->alloc_image(w,h, chroma, sps, true, decctx, /*NULL,*/ pts, user_data, isOutputImage);
  if (error) {
    return -error;
  }

  img->integrity = INTEGRITY_CORRECT;

  return free_image_buffer_idx;
}


void decoded_picture_buffer::pop_next_picture_in_output_queue()
{
  image_output_queue.pop_front();


  loginfo(LogDPB, "DPB output queue: ");
  for (int i=0;i<image_output_queue.size();i++) {
    loginfo(LogDPB, "*%d ", image_output_queue[i]->PicOrderCntVal);
  }
  loginfo(LogDPB,"*\n");
}


void decoded_picture_buffer::log_dpb_queues() const
{
    loginfo(LogDPB, "DPB reorder queue (after push): ");
    for (int i=0;i<num_pictures_in_reorder_buffer();i++) {
      loginfo(LogDPB, "*%d ", reorder_output_queue[i]->PicOrderCntVal);
    }
    loginfo(LogDPB,"*\n");

    loginfo(LogDPB, "DPB output queue (after push): ");
    for (int i=0;i<num_pictures_in_output_queue();i++) {
      loginfo(LogDPB, "*%d ", image_output_queue[i]->PicOrderCntVal);
    }
    loginfo(LogDPB,"*\n");
}
