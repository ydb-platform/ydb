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

#ifndef DE265_NAL_H
#define DE265_NAL_H

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#include <stdint.h>

#include "libde265/bitstream.h"
#include "libde265/cabac.h"

struct nal_header {
  nal_header() {
    nal_unit_type = 0;
    nuh_layer_id = 0;
    nuh_temporal_id = 0;
  }

  void read(bitreader* reader);
  void write(CABAC_encoder& writer) const;

  void set(int unit_type, int layer_id=0, int temporal_id=0) {
    nal_unit_type  =unit_type;
    nuh_layer_id   =layer_id;
    nuh_temporal_id=temporal_id;
  }

  uint8_t nal_unit_type;
  uint8_t nuh_layer_id;
  uint8_t nuh_temporal_id;
};

#define NAL_UNIT_TRAIL_N  0
#define NAL_UNIT_TRAIL_R  1
#define NAL_UNIT_TSA_N    2
#define NAL_UNIT_TSA_R    3
#define NAL_UNIT_STSA_N   4
#define NAL_UNIT_STSA_R   5
#define NAL_UNIT_RADL_N   6
#define NAL_UNIT_RADL_R   7
#define NAL_UNIT_RASL_N   8
#define NAL_UNIT_RASL_R   9
#define NAL_UNIT_RESERVED_VCL_N10  10
#define NAL_UNIT_RESERVED_VCL_N12  12
#define NAL_UNIT_RESERVED_VCL_N14  14
#define NAL_UNIT_RESERVED_VCL_R11  11
#define NAL_UNIT_RESERVED_VCL_R13  13
#define NAL_UNIT_RESERVED_VCL_R15  15
#define NAL_UNIT_BLA_W_LP   16     // BLA = broken link access
#define NAL_UNIT_BLA_W_RADL 17
#define NAL_UNIT_BLA_N_LP   18
#define NAL_UNIT_IDR_W_RADL 19
#define NAL_UNIT_IDR_N_LP   20
#define NAL_UNIT_CRA_NUT    21     // CRA = clean random access
#define NAL_UNIT_RESERVED_IRAP_VCL22 22
#define NAL_UNIT_RESERVED_IRAP_VCL23 23
#define NAL_UNIT_RESERVED_VCL24     24
#define NAL_UNIT_RESERVED_VCL25     25
#define NAL_UNIT_RESERVED_VCL26     26
#define NAL_UNIT_RESERVED_VCL27     27
#define NAL_UNIT_RESERVED_VCL28     28
#define NAL_UNIT_RESERVED_VCL29     29
#define NAL_UNIT_RESERVED_VCL30     30
#define NAL_UNIT_RESERVED_VCL31     31
#define NAL_UNIT_VPS_NUT       32
#define NAL_UNIT_SPS_NUT       33
#define NAL_UNIT_PPS_NUT       34
#define NAL_UNIT_AUD_NUT       35
#define NAL_UNIT_EOS_NUT       36
#define NAL_UNIT_EOB_NUT       37
#define NAL_UNIT_FD_NUT        38
#define NAL_UNIT_PREFIX_SEI_NUT 39
#define NAL_UNIT_SUFFIX_SEI_NUT 40
#define NAL_UNIT_RESERVED_NVCL41     41
#define NAL_UNIT_RESERVED_NVCL42     42
#define NAL_UNIT_RESERVED_NVCL43     43
#define NAL_UNIT_RESERVED_NVCL44     44
#define NAL_UNIT_RESERVED_NVCL45     45
#define NAL_UNIT_RESERVED_NVCL46     46
#define NAL_UNIT_RESERVED_NVCL47     47

#define NAL_UNIT_UNDEFINED    255

bool isIDR(uint8_t unit_type);
bool isBLA(uint8_t unit_type);
bool isCRA(uint8_t unit_type);
bool isRAP(uint8_t unit_type);
bool isRASL(uint8_t unit_type);
bool isIRAP(uint8_t unit_type);
bool isRADL(uint8_t unit_type);
bool isReferenceNALU(uint8_t unit_type);
bool isSublayerNonReference(uint8_t unit_type);

const char* get_NAL_name(uint8_t unit_type);

inline bool isIdrPic(uint8_t nal_unit_type) {
  return (nal_unit_type == NAL_UNIT_IDR_W_RADL ||
          nal_unit_type == NAL_UNIT_IDR_N_LP);
}

inline bool isRapPic(uint8_t nal_unit_type) {
  return nal_unit_type >= 16 && nal_unit_type <= 23;
}

#endif
