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

#ifndef DE265_ENCPICBUF_H
#define DE265_ENCPICBUF_H

#include "libde265/image.h"
#include "libde265/sps.h"

#include <deque>
#include <vector>


/* TODO: we need a way to quickly access pictures with a stable ID, like in the DPB.
 */

struct image_data
{
  image_data();
  ~image_data();

  int frame_number;

  const de265_image* input; // owner
  de265_image* prediction;  // owner
  de265_image* reconstruction; // owner

  // SOP metadata

  nal_header nal; // TODO: image split into several NALs (always same NAL header?)

  slice_segment_header shdr; // TODO: multi-slice pictures

  std::vector<int> ref0;
  std::vector<int> ref1;
  std::vector<int> longterm;
  std::vector<int> keep;
  int sps_index;
  int skip_priority;
  bool is_intra;  // TODO: remove, use shdr.slice_type instead

  /* unprocessed              only input image has been inserted, no metadata
     sop_metadata_available   sop-creator has filled in references and skipping metadata
     a) encoding              encoding started for this frame, reconstruction image was created
     .  keep_for_reference    encoding finished, picture is kept in the buffer for reference
     b) skipped               image was skipped, no encoding was done, no reconstruction image
  */
  enum state {
    state_unprocessed,
    state_sop_metadata_available,
    state_encoding,
    state_keep_for_reference,
    state_skipped
  } state;

  bool is_in_output_queue;

  bool mark_used;


  // --- SOP structure ---

  void set_intra();
  void set_NAL_type(uint8_t nalType);
  void set_NAL_temporal_id(int temporal_id);
  void set_references(int sps_index, // -1 -> custom
                      const std::vector<int>& l0, const std::vector<int>& l1,
                      const std::vector<int>& lt,
                      const std::vector<int>& keepMoreReferences);
  void set_skip_priority(int skip_priority);
};


class encoder_picture_buffer
{
 public:
  encoder_picture_buffer();
  ~encoder_picture_buffer();


  // --- input pushed by the input process ---

  void reset();

  image_data* insert_next_image_in_encoding_order(const de265_image*, int frame_number);
  void insert_end_of_stream();


  // --- SOP structure ---

  void sop_metadata_commit(int frame_number); // note: frame_number is only for consistency checking


  // --- infos pushed by encoder ---

  void mark_encoding_started(int frame_number);
  void set_prediction_image(int frame_number, de265_image*); // store it just for debugging fun
  void set_reconstruction_image(int frame_number, de265_image*);
  void mark_encoding_finished(int frame_number);



  // --- data access ---

  bool have_more_frames_to_encode() const;
  image_data* get_next_picture_to_encode(); // or return NULL if no picture is available
  const image_data* get_picture(int frame_number) const;
  bool has_picture(int frame_number) const;

  const image_data* peek_next_picture_to_encode() const {
    assert(!mImages.empty());
    return mImages.front();
  }

  void mark_image_is_outputted(int frame_number);
  void release_input_image(int frame_number);

 private:
  bool mEndOfStream;
  std::deque<image_data*> mImages;

  void flush_images();
  image_data* get_picture(int frame_number);
};


#endif
