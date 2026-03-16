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

#include "bitstream.h"
#include "de265.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>



void bitreader_init(bitreader* br, unsigned char* buffer, int len)
{
  br->data = buffer;
  br->bytes_remaining = len;

  br->nextbits=0;
  br->nextbits_cnt=0;

  bitreader_refill(br);
}

void bitreader_refill(bitreader* br)
{
  int shift = 64-br->nextbits_cnt;

  while (shift >= 8 && br->bytes_remaining) {
    uint64_t newval = *br->data++;
    br->bytes_remaining--;

    shift -= 8;
    newval <<= shift;
    br->nextbits |= newval;
  }

  br->nextbits_cnt = 64-shift;
}

int  get_bits(bitreader* br, int n)
{
  if (br->nextbits_cnt < n) {
    bitreader_refill(br);
  }

  uint64_t val = br->nextbits;
  val >>= 64-n;

  br->nextbits <<= n;
  br->nextbits_cnt -= n;

  return val;
}

int  get_bits_fast(bitreader* br, int n)
{
  assert(br->nextbits_cnt >= n);

  uint64_t val = br->nextbits;
  val >>= 64-n;

  br->nextbits <<= n;
  br->nextbits_cnt -= n;

  return val;
}

int  peek_bits(bitreader* br, int n)
{
  if (br->nextbits_cnt < n) {
    bitreader_refill(br);
  }

  uint64_t val = br->nextbits;
  val >>= 64-n;

  return val;
}

void skip_bits(bitreader* br, int n)
{
  if (br->nextbits_cnt < n) {
    bitreader_refill(br);
  }

  br->nextbits <<= n;
  br->nextbits_cnt -= n;
}

void skip_bits_fast(bitreader* br, int n)
{
  br->nextbits <<= n;
  br->nextbits_cnt -= n;
}

void skip_to_byte_boundary(bitreader* br)
{
  int nskip = (br->nextbits_cnt & 7);

  br->nextbits <<= nskip;
  br->nextbits_cnt -= nskip;
}

void prepare_for_CABAC(bitreader* br)
{
  skip_to_byte_boundary(br);

  int rewind = br->nextbits_cnt/8;
  br->data -= rewind;
  br->bytes_remaining += rewind;
  br->nextbits = 0;
  br->nextbits_cnt = 0;
}

int  get_uvlc(bitreader* br)
{
  int num_zeros=0;

  while (get_bits(br,1)==0) {
    num_zeros++;

    if (num_zeros > MAX_UVLC_LEADING_ZEROS) { return UVLC_ERROR; }
  }

  int offset = 0;
  if (num_zeros != 0) {
    offset = get_bits(br, num_zeros);
    int value = offset + (1<<num_zeros)-1;
    assert(value>0);
    return value;
  } else {
    return 0;
  }
}

int  get_svlc(bitreader* br)
{
  int v = get_uvlc(br);
  if (v==0) return v;
  if (v==UVLC_ERROR) return UVLC_ERROR;

  bool negative = ((v&1)==0);
  return negative ? -v/2 : (v+1)/2;
}

bool check_rbsp_trailing_bits(bitreader* br)
{
  int stop_bit = get_bits(br,1);
  assert(stop_bit==1);

  while (br->nextbits_cnt>0 || br->bytes_remaining>0) {
    int filler = get_bits(br,1);
    if (filler!=0) {
      return false;
    }
  }

  return true;
}
