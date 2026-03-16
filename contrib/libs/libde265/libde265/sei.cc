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

#include "sei.h"
#include "util.h"
#include "md5.h"

#include "libde265/sps.h"
#include "libde265/image.h"
#include "libde265/decctx.h"

#include <assert.h>


static de265_error read_sei_decoded_picture_hash(bitreader* reader, sei_message* sei,
                                                 const seq_parameter_set* sps)
{
  sei_decoded_picture_hash* seihash = &sei->data.decoded_picture_hash;

  seihash->hash_type = (enum sei_decoded_picture_hash_type)get_bits(reader,8);

  if (sps==NULL) {
    return DE265_WARNING_SPS_MISSING_CANNOT_DECODE_SEI;
  }

  int nHashes = sps->chroma_format_idc==0 ? 1 : 3;
  for (int i=0;i<nHashes;i++) {
    switch (seihash->hash_type) {
    case sei_decoded_picture_hash_type_MD5:
      for (int b=0;b<16;b++) { seihash->md5[i][b] = get_bits(reader,8); }
      break;

    case sei_decoded_picture_hash_type_CRC:
      seihash->crc[i] = get_bits(reader,16);
      break;

    case sei_decoded_picture_hash_type_checksum:
      seihash->checksum[i]  = get_bits(reader,32);
      break;
    }
  }

  return DE265_OK;
}


static void dump_sei_decoded_picture_hash(const sei_message* sei,
                                          const seq_parameter_set* sps)
{
  const sei_decoded_picture_hash* seihash = &sei->data.decoded_picture_hash;

  loginfo(LogSEI,"  hash_type: ");
  switch (seihash->hash_type) {
  case sei_decoded_picture_hash_type_MD5: loginfo(LogSEI,"MD5\n"); break;
  case sei_decoded_picture_hash_type_CRC: loginfo(LogSEI,"CRC\n"); break;
  case sei_decoded_picture_hash_type_checksum: loginfo(LogSEI,"checksum\n"); break;
  }

  int nHashes = sps->chroma_format_idc==0 ? 1 : 3;
  for (int i=0;i<nHashes;i++) {
    switch (seihash->hash_type) {
    case sei_decoded_picture_hash_type_MD5:
      loginfo(LogSEI,"  MD5[%d]: %02x", i,seihash->md5[i][0]);
      for (int b=1;b<16;b++) {
        loginfo(LogSEI,"*:%02x", seihash->md5[i][b]);
      }
      loginfo(LogSEI,"*\n");
      break;

    case sei_decoded_picture_hash_type_CRC:
      loginfo(LogSEI,"  CRC[%d]: %02x\n", i,seihash->crc[i]);
      break;

    case sei_decoded_picture_hash_type_checksum:
      loginfo(LogSEI,"  checksum[%d]: %04x\n", i,seihash->checksum[i]);
      break;
    }
  }
}


class raw_hash_data
{
public:
  raw_hash_data(int w, int stride);
  ~raw_hash_data();

  struct data_chunk {
    const uint8_t* data;
    int            len;
  };

  data_chunk prepare_8bit(const uint8_t* data,int y);
  data_chunk prepare_16bit(const uint8_t* data,int y);

private:
  int mWidth, mStride;

  uint8_t* mMem;
};


raw_hash_data::raw_hash_data(int w, int stride)
{
  mWidth=w;
  mStride=stride;
  mMem = NULL;
}

raw_hash_data::~raw_hash_data()
{
  delete[] mMem;
}

raw_hash_data::data_chunk raw_hash_data::prepare_8bit(const uint8_t* data,int y)
{
  data_chunk chunk;
  chunk.data = data+y*mStride;
  chunk.len  = mWidth;
  return chunk;
}

raw_hash_data::data_chunk raw_hash_data::prepare_16bit(const uint8_t* data,int y)
{
  if (mMem == NULL) {
    mMem = new uint8_t[2*mWidth];
  }

  const uint16_t* data16 = (uint16_t*)data;

  for (int x=0; x<mWidth; x++) {
    mMem[2*x+0] = data16[y*mStride+x] & 0xFF;
    mMem[2*x+1] = data16[y*mStride+x] >> 8;
  }

  data_chunk chunk;
  chunk.data = mMem;
  chunk.len  = 2*mWidth;
  return chunk;
}


static uint32_t compute_checksum_8bit(uint8_t* data,int w,int h,int stride, int bit_depth)
{
  uint32_t sum = 0;

  if (bit_depth<=8) {
    for (int y=0; y<h; y++)
      for(int x=0; x<w; x++) {
        uint8_t xorMask = ( x & 0xFF ) ^ ( y & 0xFF ) ^ ( x  >>  8 ) ^ ( y  >>  8 );
        sum += data[y*stride + x] ^ xorMask;
      }
  }
  else {
    for (int y=0; y<h; y++)
      for(int x=0; x<w; x++) {
        uint8_t xorMask = ( x & 0xFF ) ^ ( y & 0xFF ) ^ ( x  >>  8 ) ^ ( y  >>  8 );
        sum += (data[y*stride + x] & 0xFF) ^ xorMask;
        sum += (data[y*stride + x] >> 8)   ^ xorMask;
      }
  }

  return sum & 0xFFFFFFFF;
}

static inline uint16_t crc_process_byte(uint16_t crc, uint8_t byte)
{
  for (int bit=0;bit<8;bit++) {
    int bitVal = (byte >> (7-bit)) & 1;

    int crcMsb = (crc>>15) & 1;
    crc = (((crc<<1) + bitVal) & 0xFFFF);

    if (crcMsb) { crc ^=  0x1021; }
  }

  return crc;
}

/*
static uint16_t compute_CRC_8bit_old(const uint8_t* data,int w,int h,int stride)
{
  uint16_t crc = 0xFFFF;

  for (int y=0; y<h; y++)
    for(int x=0; x<w; x++) {
      crc = crc_process_byte(crc, data[y*stride+x]);
    }

  crc = crc_process_byte(crc, 0);
  crc = crc_process_byte(crc, 0);

  return crc;
}
*/

static inline uint16_t crc_process_byte_parallel(uint16_t crc, uint8_t byte)
{
  uint16_t s = byte ^ (crc >> 8);
  uint16_t t = s ^ (s >> 4);

  return  ((crc << 8) ^
	   t ^
	   (t <<  5) ^
	   (t << 12)) & 0xFFFF;
}

static uint32_t compute_CRC_8bit_fast(const uint8_t* data,int w,int h,int stride, int bit_depth)
{
  raw_hash_data raw_data(w,stride);

  uint16_t crc = 0xFFFF;

  crc = crc_process_byte_parallel(crc, 0);
  crc = crc_process_byte_parallel(crc, 0);

  for (int y=0; y<h; y++) {
    raw_hash_data::data_chunk chunk;

    if (bit_depth>8)
      chunk = raw_data.prepare_16bit(data, y);
    else
      chunk = raw_data.prepare_8bit(data, y);

    for(int x=0; x<chunk.len; x++) {
      crc = crc_process_byte_parallel(crc, chunk.data[x]);
    }
  }

  return crc;
}


static void compute_MD5(uint8_t* data,int w,int h,int stride, uint8_t* result, int bit_depth)
{
  MD5_CTX md5;
  MD5_Init(&md5);

  raw_hash_data raw_data(w,stride);

  for (int y=0; y<h; y++) {
    raw_hash_data::data_chunk chunk;

    if (bit_depth>8)
      chunk = raw_data.prepare_16bit(data, y);
    else
      chunk = raw_data.prepare_8bit(data, y);

    MD5_Update(&md5, (void*)chunk.data, chunk.len);
  }

  MD5_Final(result, &md5);
}


static de265_error process_sei_decoded_picture_hash(const sei_message* sei, de265_image* img)
{
  const sei_decoded_picture_hash* seihash = &sei->data.decoded_picture_hash;

  /* Do not check SEI on pictures that are not output.
     Hash may be wrong, because of a broken link (BLA).
     This happens, for example in conformance stream RAP_B, where a EOS-NAL
     appears before a CRA (POC=32). */
  if (img->PicOutputFlag == false) {
    return DE265_OK;
  }

  //write_picture(img);

  int nHashes = img->get_sps().chroma_format_idc==0 ? 1 : 3;
  for (int i=0;i<nHashes;i++) {
    uint8_t* data;
    int w,h,stride;

    w = img->get_width(i);
    h = img->get_height(i);

    data = img->get_image_plane(i);
    stride = img->get_image_stride(i);

    switch (seihash->hash_type) {
    case sei_decoded_picture_hash_type_MD5:
      {
        uint8_t md5[16];
        compute_MD5(data,w,h,stride,md5, img->get_bit_depth(i));

/*
        fprintf(stderr,"computed MD5: ");
        for (int b=0;b<16;b++) {
          fprintf(stderr,"%02x", md5[b]);
        }
        fprintf(stderr,"\n");
*/

        for (int b=0;b<16;b++) {
          if (md5[b] != seihash->md5[i][b]) {
/*
            fprintf(stderr,"SEI decoded picture MD5 mismatch (POC=%d)\n", img->PicOrderCntVal);
*/
            return DE265_ERROR_CHECKSUM_MISMATCH;
          }
        }
      }
      break;

    case sei_decoded_picture_hash_type_CRC:
      {
        uint16_t crc = compute_CRC_8bit_fast(data,w,h,stride, img->get_bit_depth(i));

        logtrace(LogSEI,"SEI decoded picture hash: %04x <-[%d]-> decoded picture: %04x\n",
                 seihash->crc[i], i, crc);

        if (crc != seihash->crc[i]) {
/*
          fprintf(stderr,"SEI decoded picture hash: %04x, decoded picture: %04x (POC=%d)\n",
                  seihash->crc[i], crc, img->PicOrderCntVal);
*/
          return DE265_ERROR_CHECKSUM_MISMATCH;
        }
      }
      break;

    case sei_decoded_picture_hash_type_checksum:
      {
        uint32_t chksum = compute_checksum_8bit(data,w,h,stride, img->get_bit_depth(i));

        if (chksum != seihash->checksum[i]) {
/*
          fprintf(stderr,"SEI decoded picture hash: %04x, decoded picture: %04x (POC=%d)\n",
                  seihash->checksum[i], chksum, img->PicOrderCntVal);
*/
          return DE265_ERROR_CHECKSUM_MISMATCH;
        }
      }
      break;
    }
  }

  loginfo(LogSEI,"decoded picture hash checked: OK\n");
  //printf("checked picture %d SEI: OK\n", img->PicOrderCntVal);

  return DE265_OK;
}


de265_error read_sei(bitreader* reader, sei_message* sei, bool suffix, const seq_parameter_set* sps)
{
  int payload_type = 0;
  for (;;)
    {
      int byte = get_bits(reader,8);
      payload_type += byte;
      if (byte != 0xFF) { break; }
    }

  //printf("SEI payload: %d\n",payload_type);

  int payload_size = 0;
  for (;;)
    {
      int byte = get_bits(reader,8);
      payload_size += byte;
      if (byte != 0xFF) { break; }
    }

  sei->payload_type = (enum sei_payload_type)payload_type;
  sei->payload_size = payload_size;


  // --- sei message dispatch

  de265_error err = DE265_OK;

  switch (sei->payload_type) {
  case sei_payload_type_decoded_picture_hash:
    err = read_sei_decoded_picture_hash(reader,sei,sps);
    break;

  default:
    // TODO: unknown SEI messages are ignored
    break;
  }

  return err;
}

void dump_sei(const sei_message* sei, const seq_parameter_set* sps)
{
  loginfo(LogHeaders,"SEI message: %s\n", sei_type_name(sei->payload_type));

  switch (sei->payload_type) {
  case sei_payload_type_decoded_picture_hash:
    dump_sei_decoded_picture_hash(sei, sps);
    break;

  default:
    // TODO: unknown SEI messages are ignored
    break;
  }
}


de265_error process_sei(const sei_message* sei, de265_image* img)
{
  de265_error err = DE265_OK;

  switch (sei->payload_type) {
  case sei_payload_type_decoded_picture_hash:
    if (img->decctx->param_sei_check_hash) {
      err = process_sei_decoded_picture_hash(sei, img);
      if (err==DE265_OK) {
        //printf("SEI check ok\n");
      }
    }

    break;

  default:
    // TODO: unknown SEI messages are ignored
    break;
  }

  return err;
}


const char* sei_type_name(enum sei_payload_type type)
{
  switch (type) {
  case sei_payload_type_buffering_period:
    return "buffering_period";
  case sei_payload_type_pic_timing:
    return "pic_timing";
  case sei_payload_type_pan_scan_rect:
    return "pan_scan_rect";
  case sei_payload_type_filler_payload:
    return "filler_payload";
  case sei_payload_type_user_data_registered_itu_t_t35:
    return "user_data_registered_itu_t_t35";
  case sei_payload_type_user_data_unregistered:
    return "user_data_unregistered";
  case sei_payload_type_recovery_point:
    return "recovery_point";
  case sei_payload_type_scene_info:
    return "scene_info";
  case sei_payload_type_picture_snapshot:
    return "picture_snapshot";
  case sei_payload_type_progressive_refinement_segment_start:
    return "progressive_refinement_segment_start";
  case sei_payload_type_progressive_refinement_segment_end:
    return "progressive_refinement_segment_end";
  case sei_payload_type_film_grain_characteristics:
    return "film_grain_characteristics";
  case sei_payload_type_post_filter_hint:
    return "post_filter_hint";
  case sei_payload_type_tone_mapping_info:
    return "tone_mapping_info";
  case sei_payload_type_frame_packing_arrangement:
    return "frame_packing_arrangement";
  case sei_payload_type_display_orientation:
    return "display_orientation";
  case sei_payload_type_structure_of_pictures_info:
    return "structure_of_pictures_info";
  case sei_payload_type_active_parameter_sets:
    return "active_parameter_sets";
  case sei_payload_type_decoding_unit_info:
    return "decoding_unit_info";
  case sei_payload_type_temporal_sub_layer_zero_index:
    return "temporal_sub_layer_zero_index";
  case sei_payload_type_decoded_picture_hash:
    return "decoded_picture_hash";
  case sei_payload_type_scalable_nesting:
    return "scalable_nesting";
  case sei_payload_type_region_refresh_info:
    return "region_refresh_info";
  case sei_payload_type_no_display:
    return "no_display";
  case sei_payload_type_motion_constrained_tile_sets:
    return "motion_constrained_tile_sets";

  default:
    return "unknown SEI message";
  }
}
