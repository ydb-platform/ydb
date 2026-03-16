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

#ifndef ENCODER_CONTEXT_H
#define ENCODER_CONTEXT_H

#include "libde265/image.h"
#include "libde265/decctx.h"
#include "libde265/image-io.h"
#include "libde265/encoder/encoder-params.h"
#include "libde265/encoder/encpicbuf.h"
#include "libde265/encoder/sop.h"
#include "libde265/en265.h"
#include "libde265/util.h"

#include <memory>


class encoder_context : public base_context
{
 public:
  encoder_context();
  ~encoder_context();

  virtual const de265_image* get_image(int frame_id) const {
    return picbuf.get_picture(frame_id)->reconstruction;
  }

  virtual bool has_image(int frame_id) const {
    return picbuf.has_picture(frame_id);
  }

  bool encoder_started;

  encoder_params params;
  config_parameters params_config;

  EncoderCore_Custom algo;

  int image_width, image_height;
  bool image_spec_is_defined;  // whether we know the input image size

  void* param_image_allocation_userdata;
  /*
  void (*release_func)(en265_encoder_context*,
                       de265_image*,
                       void* userdata);
  */

  //error_queue errqueue;
  //acceleration_functions accel;

  // quick links
  de265_image* img; // reconstruction
  //de265_image* prediction;
  image_data* imgdata; // input image
  slice_segment_header* shdr;

  CTBTreeMatrix ctbs;

  // temporary memory for motion compensated pixels (when CB-algo passes this down to TB-algo)
  //uint8_t prediction[3][64*64]; // stride: 1<<(cb->log2Size)
  //int prediction_x0,prediction_y0;


  int active_qp; // currently active QP
  /*int target_qp;*/ /* QP we want to code at.
                     (Not actually the real QP. Check image.get_QPY() for that.) */

  const seq_parameter_set& get_sps() const { return *sps; }
  const pic_parameter_set& get_pps() const { return *pps; }

  seq_parameter_set& get_sps() { return *sps; }
  pic_parameter_set& get_pps() { return *pps; }

  std::shared_ptr<video_parameter_set>& get_shared_vps() { return vps; }
  std::shared_ptr<seq_parameter_set>& get_shared_sps() { return sps; }
  std::shared_ptr<pic_parameter_set>& get_shared_pps() { return pps; }

 private:
  std::shared_ptr<video_parameter_set>  vps;
  std::shared_ptr<seq_parameter_set>    sps;
  std::shared_ptr<pic_parameter_set>    pps;
  //slice_segment_header shdr;

 public:
  bool parameters_have_been_set;
  bool headers_have_been_sent;

  encoder_picture_buffer picbuf;
  std::shared_ptr<sop_creator> sop;

  std::deque<en265_packet*> output_packets;


  // --- rate-control ---

  float lambda;


  // --- CABAC output and rate estimation ---

  //CABAC_encoder*  cabac;      // currently active CABAC output (estim or bitstream)
  //context_model_table2* ctx_model;  // currently active ctx models (estim or bitstream)

  // CABAC bitstream writer
  CABAC_encoder_bitstream cabac_encoder;
  context_model_table     cabac_ctx_models;

  //std::shared_ptr<CABAC_encoder> cabac_estim;

  bool use_adaptive_context;


  /*** TODO: CABAC_encoder direkt an encode-Funktion übergeben, anstatt hier
       aussenrum zwischenzuspeichern (mit undefinierter Lifetime).
       Das Context-Model kann dann gleich mit in den Encoder rein cabac_encoder(ctxtable).
       write_bits() wird dann mit dem context-index aufgerufen, nicht mit dem model direkt.
  ***/


  /*
  void switch_CABAC(context_model_table2* model) {
    cabac      = cabac_estim.get();
    ctx_model  = model;
  }

  void switch_CABAC_to_bitstream() {
    cabac     = &cabac_bitstream;
    ctx_model = &ctx_model_bitstream;
  }
  */

  en265_packet* create_packet(en265_packet_content_type t);


  // --- encoding control ---

  void start_encoder();
  de265_error encode_headers();
  de265_error encode_picture_from_input_buffer();


  // Input images can be released after encoding and when the output packet is released.
  // This is important to do as soon as possible, as the image might actually wrap
  // scarce resources like camera picture buffers.
  // This function does release (only) the raw input data.
  void release_input_image(int frame_number) { picbuf.release_input_image(frame_number); }

  void mark_image_is_outputted(int frame_number) { picbuf.mark_image_is_outputted(frame_number); }
};


#endif
