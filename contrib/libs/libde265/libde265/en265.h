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

#ifndef EN265_H
#define EN265_H

#ifdef __cplusplus
extern "C" {
#endif

#include <libde265/de265.h>


// ========== encoder context ==========

struct en265_encoder_context; // private structure

/* Get a new encoder context. Must be freed with en265_free_encoder(). */
LIBDE265_API en265_encoder_context* en265_new_encoder(void);

/* Free encoder context. May only be called once on a context. */
LIBDE265_API de265_error en265_free_encoder(en265_encoder_context*);

/* The alloc_userdata pointer will be given to the release_func(). */
/*
LIBDE265_API void en265_set_image_release_function(en265_encoder_context*,
                                                   void (*release_func)(en265_encoder_context*,
                                                                        struct de265_image*,
                                                                        void* userdata),
                                                   void* alloc_userdata);
*/

// ========== encoder parameters ==========

LIBDE265_API de265_error en265_set_parameter_bool(en265_encoder_context*,
                                                  const char* parametername,int value);
LIBDE265_API de265_error en265_set_parameter_int(en265_encoder_context*,
                                                 const char* parametername,int value);
LIBDE265_API de265_error en265_set_parameter_string(en265_encoder_context*,
                                                    const char* parametername,const char* value);
LIBDE265_API de265_error en265_set_parameter_choice(en265_encoder_context*,
                                                    const char* parametername,const char* value);


LIBDE265_API const char** en265_list_parameters(en265_encoder_context*);

enum en265_parameter_type {
  en265_parameter_bool,
  en265_parameter_int,
  en265_parameter_string,
  en265_parameter_choice
};

LIBDE265_API enum en265_parameter_type en265_get_parameter_type(en265_encoder_context*,
                                                                const char* parametername);

LIBDE265_API const char** en265_list_parameter_choices(en265_encoder_context*,
                                                       const char* parametername);


// --- convenience functions for command-line parameters ---

LIBDE265_API de265_error en265_parse_command_line_parameters(en265_encoder_context*,
                                                             int* argc, char** argv);
LIBDE265_API void en265_show_parameters(en265_encoder_context*);



// ========== encoding loop ==========

LIBDE265_API de265_error en265_start_encoder(en265_encoder_context*, int number_of_threads);

// If we have provided our own memory release function, no image memory will be allocated.
LIBDE265_API struct de265_image* en265_allocate_image(en265_encoder_context*,
                                                      int width, int height,
                                                      enum de265_chroma chroma,
                                                      de265_PTS pts, void* image_userdata);

LIBDE265_API void* de265_alloc_image_plane(struct de265_image* img, int cIdx,
                                           void* inputdata, int inputstride, void *userdata);
LIBDE265_API void de265_free_image_plane(struct de265_image* img, int cIdx);


// Request a specification of the image memory layout for an image of the specified dimensions.
LIBDE265_API void en265_get_image_spec(en265_encoder_context*,
                                       int width, int height, enum de265_chroma chroma,
                                       struct de265_image_spec* out_spec);

// Image memory layout specification for an image returned by en265_allocate_image().
/* TODO: do we need this?
LIBDE265_API void de265_get_image_spec_from_image(de265_image* img, struct de265_image_spec* spec);
*/


LIBDE265_API de265_error en265_push_image(en265_encoder_context*,
                                          struct de265_image*); // non-blocking

LIBDE265_API de265_error en265_push_eof(en265_encoder_context*);

// block when there are more than max_input_images in the input queue
LIBDE265_API de265_error en265_block_on_input_queue_length(en265_encoder_context*,
                                                           int max_pending_images,
                                                           int timeout_ms);

LIBDE265_API de265_error en265_trim_input_queue(en265_encoder_context*, int max_pending_images);

LIBDE265_API int  en265_current_input_queue_length(en265_encoder_context*);

// Run encoder in main thread. Only use this when not using background threads.
LIBDE265_API de265_error en265_encode(en265_encoder_context*);

enum en265_encoder_state
{
  EN265_STATE_IDLE,
  EN265_STATE_WAITING_FOR_INPUT,
  EN265_STATE_WORKING,
  EN265_STATE_OUTPUT_QUEUE_FULL,
  EN265_STATE_EOS
};


LIBDE265_API enum en265_encoder_state en265_get_encoder_state(en265_encoder_context*);


enum en265_packet_content_type {
  EN265_PACKET_VPS,
  EN265_PACKET_SPS,
  EN265_PACKET_PPS,
  EN265_PACKET_SEI,
  EN265_PACKET_SLICE,
  EN265_PACKET_SKIPPED_IMAGE
};


enum en265_nal_unit_type {
  EN265_NUT_TRAIL_N = 0,
  EN265_NUT_TRAIL_R = 1,
  EN265_NUT_TSA_N   = 2,
  EN265_NUT_TSA_R   = 3,
  EN265_NUT_STSA_N  = 4,
  EN265_NUT_STSA_R  = 5,
  EN265_NUT_RADL_N  = 6,
  EN265_NUT_RADL_R  = 7,
  EN265_NUT_RASL_N  = 8,
  EN265_NUT_RASL_R  = 9,
  EN265_NUT_BLA_W_LP  = 16,
  EN265_NUT_BLA_W_RADL= 17,
  EN265_NUT_BLA_N_LP  = 18,
  EN265_NUT_IDR_W_RADL= 19,
  EN265_NUT_IDR_N_LP  = 20,
  EN265_NUT_CRA       = 21,
  EN265_NUT_VPS   =    32,
  EN265_NUT_SPS   =    33,
  EN265_NUT_PPS   =    34,
  EN265_NUT_AUD   =    35,
  EN265_NUT_EOS   =    36,
  EN265_NUT_EOB   =    37,
  EN265_NUT_FD    =    38,
  EN265_NUT_PREFIX_SEI = 39,
  EN265_NUT_SUFFIX_SEI = 40
};


struct en265_packet
{
  int version; // currently: 1

  const uint8_t* data;
  int   length;

  int  frame_number;

  enum en265_packet_content_type content_type;
  char complete_picture : 1;
  char final_slice      : 1;
  char dependent_slice  : 1;

  enum en265_nal_unit_type nal_unit_type;
  unsigned char nuh_layer_id;
  unsigned char nuh_temporal_id;

  en265_encoder_context* encoder_context;

  const struct de265_image* input_image;
  const struct de265_image* reconstruction;
};

// timeout_ms - timeout in milliseconds. 0 - no timeout, -1 - block forever
LIBDE265_API struct en265_packet* en265_get_packet(en265_encoder_context*, int timeout_ms);
LIBDE265_API void en265_free_packet(en265_encoder_context*, struct en265_packet*);

LIBDE265_API int en265_number_of_queued_packets(en265_encoder_context*);

#ifdef __cplusplus
}
#endif


#endif
