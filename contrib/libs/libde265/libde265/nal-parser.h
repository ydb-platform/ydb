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

#ifndef DE265_NAL_PARSER_H
#define DE265_NAL_PARSER_H

#include "libde265/sps.h"
#include "libde265/pps.h"
#include "libde265/nal.h"
#include "libde265/util.h"

#include <vector>
#include <queue>

#define DE265_NAL_FREE_LIST_SIZE 16
#define DE265_SKIPPED_BYTES_INITIAL_SIZE 16


class NAL_unit {
 public:
  NAL_unit();
  ~NAL_unit();

  nal_header header;

  de265_PTS  pts;
  void*      user_data;


  void clear();

  // --- rbsp data ---

  LIBDE265_CHECK_RESULT bool resize(int new_size);
  LIBDE265_CHECK_RESULT bool append(const unsigned char* data, int n);
  LIBDE265_CHECK_RESULT bool set_data(const unsigned char* data, int n);

  int size() const { return data_size; }
  void set_size(int s) { data_size=s; }
  unsigned char* data() { return nal_data; }
  const unsigned char* data() const { return nal_data; }


  // --- skipped stuffing bytes ---

  int num_skipped_bytes_before(int byte_position, int headerLength) const;
  int  num_skipped_bytes() const { return skipped_bytes.size(); }

  //void clear_skipped_bytes() { skipped_bytes.clear(); }

  /* Mark a byte as skipped. It is assumed that the byte is already removed
     from the input data. The NAL data is not modified.
  */
  void insert_skipped_byte(int pos);

  /* Remove all stuffing bytes from NAL data. The NAL data is modified and
     the removed bytes are marked as skipped bytes.
   */
  void remove_stuffing_bytes();

 private:
  unsigned char* nal_data;
  int data_size;
  int capacity;

  std::vector<int> skipped_bytes; // up to position[x], there were 'x' skipped bytes
};


class NAL_Parser
{
 public:
  NAL_Parser();
  ~NAL_Parser();

  de265_error push_data(const unsigned char* data, int len,
                        de265_PTS pts, void* user_data = NULL);

  de265_error push_NAL(const unsigned char* data, int len,
                       de265_PTS pts, void* user_data = NULL);

  NAL_unit*   pop_from_NAL_queue();
  de265_error flush_data();
  void        mark_end_of_stream() { end_of_stream=true; }
  void        mark_end_of_frame() { end_of_frame=true; }
  void  remove_pending_input_data();

  int bytes_in_input_queue() const {
    int size = nBytes_in_NAL_queue;
    if (pending_input_NAL) { size += pending_input_NAL->size(); }
    return size;
  }

  int number_of_NAL_units_pending() const {
    int size = NAL_queue.size();
    if (pending_input_NAL) { size++; }
    return size;
  }

  int number_of_complete_NAL_units_pending() const {
    return NAL_queue.size();
  }

  void free_NAL_unit(NAL_unit*);


  int get_NAL_queue_length() const { return NAL_queue.size(); }
  bool is_end_of_stream() const { return end_of_stream; }
  bool is_end_of_frame() const { return end_of_frame; }

 private:
  // byte-stream level

  bool end_of_stream; // data in pending_input_data is end of stream
  bool end_of_frame;  // data in pending_input_data is end of frame
  int  input_push_state;

  NAL_unit* pending_input_NAL;


  // NAL level

  std::queue<NAL_unit*> NAL_queue;  // enqueued NALs have suffing bytes removed
  int nBytes_in_NAL_queue; // data bytes currently in NAL_queue

  void push_to_NAL_queue(NAL_unit*);


  // pool of unused NAL memory

  std::vector<NAL_unit*> NAL_free_list;  // maximum size: DE265_NAL_FREE_LIST_SIZE

  LIBDE265_CHECK_RESULT NAL_unit* alloc_NAL_unit(int size);
};


#endif
