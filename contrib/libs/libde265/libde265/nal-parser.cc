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

#include "nal-parser.h"

#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include <stdio.h>

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif


NAL_unit::NAL_unit()
  : skipped_bytes(DE265_SKIPPED_BYTES_INITIAL_SIZE)
{
  pts=0;
  user_data = NULL;

  nal_data = NULL;
  data_size = 0;
  capacity = 0;
}

NAL_unit::~NAL_unit()
{
  free(nal_data);
}

void NAL_unit::clear()
{
  header = nal_header();
  pts = 0;
  user_data = NULL;

  // set size to zero but keep memory
  data_size = 0;

  skipped_bytes.clear();
}

LIBDE265_CHECK_RESULT bool NAL_unit::resize(int new_size)
{
  if (capacity < new_size) {
    unsigned char* newbuffer = (unsigned char*)malloc(new_size);
    if (newbuffer == NULL) {
      return false;
    }

    if (nal_data != NULL) {
      memcpy(newbuffer, nal_data, data_size);
      free(nal_data);
    }

    nal_data = newbuffer;
    capacity = new_size;
  }
  return true;
}

LIBDE265_CHECK_RESULT bool NAL_unit::append(const unsigned char* in_data, int n)
{
  if (!resize(data_size + n)) {
    return false;
  }
  memcpy(nal_data + data_size, in_data, n);
  data_size += n;
  return true;
}

bool LIBDE265_CHECK_RESULT NAL_unit::set_data(const unsigned char* in_data, int n)
{
  if (!resize(n)) {
    return false;
  }
  memcpy(nal_data, in_data, n);
  data_size = n;
  return true;
}

void NAL_unit::insert_skipped_byte(int pos)
{
  skipped_bytes.push_back(pos);
}

int NAL_unit::num_skipped_bytes_before(int byte_position, int headerLength) const
{
  for (int k=skipped_bytes.size()-1;k>=0;k--)
    if (skipped_bytes[k] >= headerLength &&
        skipped_bytes[k]-headerLength <= byte_position) {
      return k+1;
    }

  return 0;
}

void NAL_unit::remove_stuffing_bytes()
{
  uint8_t* p = data();

  for (int i=0;i<size()-2;i++)
    {
#if 0
        for (int k=i;k<i+64;k++) 
          if (i*0+k<size()) {
            printf("%c%02x", (k==i) ? '[':' ', data()[k]);
          }
        printf("\n");
#endif

      if (p[2]!=3 && p[2]!=0) {
        // fast forward 3 bytes (2+1)
        p+=2;
        i+=2;
      }
      else {
        if (p[0]==0 && p[1]==0 && p[2]==3) {
          //printf("SKIP NAL @ %d\n",i+2+num_skipped_bytes);
          insert_skipped_byte(i+2 + num_skipped_bytes());

          memmove(p+2, p+3, size()-i-3);
          set_size(size()-1);

          p++;
          i++;
        }
      }

      p++;
    }
}





NAL_Parser::NAL_Parser()
{
  end_of_stream = false;
  end_of_frame = false;
  input_push_state = 0;
  pending_input_NAL = NULL;
  nBytes_in_NAL_queue = 0;
}


NAL_Parser::~NAL_Parser()
{
  // --- free NAL queues ---

  // empty NAL queue

  NAL_unit* nal;
  while ( (nal = pop_from_NAL_queue()) ) {
    free_NAL_unit(nal);
  }

  // free the pending input NAL

  if (pending_input_NAL != NULL) {
    free_NAL_unit(pending_input_NAL);
  }

  // free all NALs in free-list

  for (size_t i=0;i<NAL_free_list.size();i++) {
    delete NAL_free_list[i];
  }
}


LIBDE265_CHECK_RESULT NAL_unit* NAL_Parser::alloc_NAL_unit(int size)
{
  NAL_unit* nal;

  // --- get NAL-unit object ---

  if (NAL_free_list.size() > 0) {
    nal = NAL_free_list.back();
    NAL_free_list.pop_back();
  }
  else {
    nal = new NAL_unit;
  }

  nal->clear();
  if (!nal->resize(size)) {
    free_NAL_unit(nal);
    return NULL;
  }

  return nal;
}

void NAL_Parser::free_NAL_unit(NAL_unit* nal)
{
  if (nal == NULL) {
    // Allow calling with NULL just like regular "free()"
    return;
  }
  if (NAL_free_list.size() < DE265_NAL_FREE_LIST_SIZE) {
    NAL_free_list.push_back(nal);
  }
  else {
    delete nal;
  }
}

NAL_unit* NAL_Parser::pop_from_NAL_queue()
{
  if (NAL_queue.empty()) {
    return NULL;
  }
  else {
    NAL_unit* nal = NAL_queue.front();
    NAL_queue.pop();

    nBytes_in_NAL_queue -= nal->size();

    return nal;
  }
}

void NAL_Parser::push_to_NAL_queue(NAL_unit* nal)
{
  NAL_queue.push(nal);
  nBytes_in_NAL_queue += nal->size();
}

de265_error NAL_Parser::push_data(const unsigned char* data, int len,
                                  de265_PTS pts, void* user_data)
{
  end_of_frame = false;

  if (pending_input_NAL == NULL) {
    pending_input_NAL = alloc_NAL_unit(len+3);
    if (pending_input_NAL == NULL) {
      return DE265_ERROR_OUT_OF_MEMORY;
    }
    pending_input_NAL->pts = pts;
    pending_input_NAL->user_data = user_data;
  }

  NAL_unit* nal = pending_input_NAL; // shortcut

  // Resize output buffer so that complete input would fit.
  // We add 3, because in the worst case 3 extra bytes are created for an input byte.
  if (!nal->resize(nal->size() + len + 3)) {
    return DE265_ERROR_OUT_OF_MEMORY;
  }

  unsigned char* out = nal->data() + nal->size();

  for (int i=0;i<len;i++) {
    /*
    printf("state=%d input=%02x (%p) (output size: %d)\n",ctx->input_push_state, *data, data,
           out - ctx->nal_data.data);
    */

    switch (input_push_state) {
    case 0:
    case 1:
      if (*data == 0) { input_push_state++; }
      else { input_push_state=0; }
      break;
    case 2:
      if      (*data == 1) { input_push_state=3; } // nal->clear_skipped_bytes(); }
      else if (*data == 0) { } // *out++ = 0; }
      else { input_push_state=0; }
      break;
    case 3:
      *out++ = *data;
      input_push_state = 4;
      break;
    case 4:
      *out++ = *data;
      input_push_state = 5;
      break;

    case 5:
      if (*data==0) { input_push_state=6; }
      else { *out++ = *data; }
      break;

    case 6:
      if (*data==0) { input_push_state=7; }
      else {
        *out++ = 0;
        *out++ = *data;
        input_push_state=5;
      }
      break;

    case 7:
      if      (*data==0) { *out++ = 0; }
      else if (*data==3) {
        *out++ = 0; *out++ = 0; input_push_state=5;

        // remember which byte we removed
        nal->insert_skipped_byte((out - nal->data()) + nal->num_skipped_bytes());
      }
      else if (*data==1) {

#if DEBUG_INSERT_STREAM_ERRORS
        if ((rand()%100)<90 && nal_data.size>0) {
          int pos = rand()%nal_data.size;
          int bit = rand()%8;
          nal->nal_data.data[pos] ^= 1<<bit;

          //printf("inserted error...\n");
        }
#endif

        nal->set_size(out - nal->data());;

        // push this NAL decoder queue
        push_to_NAL_queue(nal);


        // initialize new, empty NAL unit

        pending_input_NAL = alloc_NAL_unit(len+3);
        if (pending_input_NAL == NULL) {
          return DE265_ERROR_OUT_OF_MEMORY;
        }
        pending_input_NAL->pts = pts;
        pending_input_NAL->user_data = user_data;
        nal = pending_input_NAL;
        out = nal->data();

        input_push_state=3;
        //nal->clear_skipped_bytes();
      }
      else {
        *out++ = 0;
        *out++ = 0;
        *out++ = *data;

        input_push_state=5;
      }
      break;
    }

    data++;
  }

  nal->set_size(out - nal->data());
  return DE265_OK;
}


de265_error NAL_Parser::push_NAL(const unsigned char* data, int len,
                                 de265_PTS pts, void* user_data)
{

  // Cannot use byte-stream input and NAL input at the same time.
  assert(pending_input_NAL == NULL);

  end_of_frame = false;

  NAL_unit* nal = alloc_NAL_unit(len);
  if (nal == NULL || !nal->set_data(data, len)) {
    free_NAL_unit(nal);
    return DE265_ERROR_OUT_OF_MEMORY;
  }
  nal->pts = pts;
  nal->user_data = user_data;

  nal->remove_stuffing_bytes();

  push_to_NAL_queue(nal);

  return DE265_OK;
}


de265_error NAL_Parser::flush_data()
{
  if (pending_input_NAL) {
    NAL_unit* nal = pending_input_NAL;
    uint8_t null[2] = { 0,0 };

    // append bytes that are implied by the push state

    if (input_push_state==6) {
      if (!nal->append(null,1)) {
        return DE265_ERROR_OUT_OF_MEMORY;
      }
    }
    if (input_push_state==7) {
      if (!nal->append(null,2)) {
        return DE265_ERROR_OUT_OF_MEMORY;
      }
    }


    // only push the NAL if it contains at least the NAL header

    if (input_push_state>=5) {
      push_to_NAL_queue(nal);
      pending_input_NAL = NULL;
    }

    input_push_state = 0;
  }

  return DE265_OK;
}


void NAL_Parser::remove_pending_input_data()
{
  // --- remove pending input data ---

  if (pending_input_NAL) {
    free_NAL_unit(pending_input_NAL);
    pending_input_NAL = NULL;
  }

  for (;;) {
    NAL_unit* nal = pop_from_NAL_queue();
    if (nal) { free_NAL_unit(nal); }
    else break;
  }

  input_push_state = 0;
  nBytes_in_NAL_queue = 0;
}
