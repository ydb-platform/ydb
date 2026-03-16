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

#include "nal.h"
#include "cabac.h"
#include <assert.h>


void nal_header::read(bitreader* reader)
{
  skip_bits(reader,1);
  nal_unit_type = get_bits(reader,6);
  nuh_layer_id  = get_bits(reader,6);
  nuh_temporal_id = get_bits(reader,3) -1;
}


void nal_header::write(CABAC_encoder& out) const
{
  out.skip_bits(1);
  out.write_bits(nal_unit_type,6);
  out.write_bits(nuh_layer_id ,6);
  out.write_bits(nuh_temporal_id+1,3);
}


bool isIDR(uint8_t unit_type)
{
  return (unit_type == NAL_UNIT_IDR_W_RADL ||
          unit_type == NAL_UNIT_IDR_N_LP);
}

bool isBLA(uint8_t unit_type)
{
  return (unit_type == NAL_UNIT_BLA_W_LP ||
          unit_type == NAL_UNIT_BLA_W_RADL ||
          unit_type == NAL_UNIT_BLA_N_LP);
}

bool isCRA(uint8_t unit_type)
{
  return unit_type == NAL_UNIT_CRA_NUT;
}

bool isRAP(uint8_t unit_type)
{
  return isIDR(unit_type) || isBLA(unit_type) || isCRA(unit_type);
}

bool isRASL(uint8_t unit_type)
{
  return (unit_type == NAL_UNIT_RASL_N ||
          unit_type == NAL_UNIT_RASL_R);
}

bool isIRAP(uint8_t unit_type)
{
  return (unit_type >= NAL_UNIT_BLA_W_LP &&
          unit_type <= NAL_UNIT_RESERVED_IRAP_VCL23);
}

bool isRADL(uint8_t unit_type)
{
  return (unit_type == NAL_UNIT_RADL_N ||
          unit_type == NAL_UNIT_RADL_R);
}


bool isReferenceNALU(uint8_t unit_type)
{
  return ( ((unit_type <= NAL_UNIT_RESERVED_VCL_R15) && (unit_type%2 != 0)) ||
           ((unit_type >= NAL_UNIT_BLA_W_LP) &&
            (unit_type <= NAL_UNIT_RESERVED_IRAP_VCL23)) );
}

bool isSublayerNonReference(uint8_t unit_type)
{
  switch (unit_type) {
  case NAL_UNIT_TRAIL_N:
  case NAL_UNIT_TSA_N:
  case NAL_UNIT_STSA_N:
  case NAL_UNIT_RADL_N:
  case NAL_UNIT_RASL_N:
  case NAL_UNIT_RESERVED_VCL_N10:
  case NAL_UNIT_RESERVED_VCL_N12:
  case NAL_UNIT_RESERVED_VCL_N14:
    return true;

  default:
    return false;
  }
}

static const char* NAL_unit_name[] = {
  "TRAIL_N", // 0
  "TRAIL_R",
  "TSA_N",
  "TSA_R",
  "STSA_N",
  "STSA_R",  // 5
  "RADL_N",
  "RADL_R",
  "RASL_N",
  "RASL_R",
  "RESERVED_VCL_N10", // 10
  "RESERVED_VCL_R11",
  "RESERVED_VCL_N12",
  "RESERVED_VCL_R13",
  "RESERVED_VCL_N14",
  "RESERVED_VCL_R15", // 15
  "BLA_W_LP",
  "BLA_W_RADL",
  "BLA_N_LP",
  "IDR_W_RADL",
  "IDR_N_LP",     // 20
  "CRA_NUT",
  "RESERVED_IRAP_VCL22",
  "RESERVED_IRAP_VCL23",
  "RESERVED_VCL24",
  "RESERVED_VCL25", // 25
  "RESERVED_VCL26",
  "RESERVED_VCL27",
  "RESERVED_VCL28",
  "RESERVED_VCL29",
  "RESERVED_VCL30", // 30
  "RESERVED_VCL31",
  "VPS",
  "SPS",
  "PPS",
  "AUD", // 35
  "EOS",
  "EOB",
  "FD",
  "PREFIX_SEI",
  "SUFFIX_SEI", // 40
  "RESERVED_NVCL41",
  "RESERVED_NVCL42",
  "RESERVED_NVCL43",
  "RESERVED_NVCL44",
  "RESERVED_NVCL45", // 45
  "RESERVED_NVCL46",
  "RESERVED_NVCL47"
};

const char* get_NAL_name(uint8_t unit_type)
{
  if (unit_type >= 48) { return "INVALID NAL >= 48"; }
  return NAL_unit_name[unit_type];
}
