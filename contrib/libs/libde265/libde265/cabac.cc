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

#include "cabac.h"
#include "util.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#define INITIAL_CABAC_BUFFER_CAPACITY 4096


static const uint8_t LPS_table[64][4] =
  {
    { 128, 176, 208, 240},
    { 128, 167, 197, 227},
    { 128, 158, 187, 216},
    { 123, 150, 178, 205},
    { 116, 142, 169, 195},
    { 111, 135, 160, 185},
    { 105, 128, 152, 175},
    { 100, 122, 144, 166},
    {  95, 116, 137, 158},
    {  90, 110, 130, 150},
    {  85, 104, 123, 142},
    {  81,  99, 117, 135},
    {  77,  94, 111, 128},
    {  73,  89, 105, 122},
    {  69,  85, 100, 116},
    {  66,  80,  95, 110},
    {  62,  76,  90, 104},
    {  59,  72,  86,  99},
    {  56,  69,  81,  94},
    {  53,  65,  77,  89},
    {  51,  62,  73,  85},
    {  48,  59,  69,  80},
    {  46,  56,  66,  76},
    {  43,  53,  63,  72},
    {  41,  50,  59,  69},
    {  39,  48,  56,  65},
    {  37,  45,  54,  62},
    {  35,  43,  51,  59},
    {  33,  41,  48,  56},
    {  32,  39,  46,  53},
    {  30,  37,  43,  50},
    {  29,  35,  41,  48},
    {  27,  33,  39,  45},
    {  26,  31,  37,  43},
    {  24,  30,  35,  41},
    {  23,  28,  33,  39},
    {  22,  27,  32,  37},
    {  21,  26,  30,  35},
    {  20,  24,  29,  33},
    {  19,  23,  27,  31},
    {  18,  22,  26,  30},
    {  17,  21,  25,  28},
    {  16,  20,  23,  27},
    {  15,  19,  22,  25},
    {  14,  18,  21,  24},
    {  14,  17,  20,  23},
    {  13,  16,  19,  22},
    {  12,  15,  18,  21},
    {  12,  14,  17,  20},
    {  11,  14,  16,  19},
    {  11,  13,  15,  18},
    {  10,  12,  15,  17},
    {  10,  12,  14,  16},
    {   9,  11,  13,  15},
    {   9,  11,  12,  14},
    {   8,  10,  12,  14},
    {   8,   9,  11,  13},
    {   7,   9,  11,  12},
    {   7,   9,  10,  12},
    {   7,   8,  10,  11},
    {   6,   8,   9,  11},
    {   6,   7,   9,  10},
    {   6,   7,   8,   9},
    {   2,   2,   2,   2}
  };

static const uint8_t renorm_table[32] =
  {
    6,  5,  4,  4,
    3,  3,  3,  3,
    2,  2,  2,  2,
    2,  2,  2,  2,
    1,  1,  1,  1,
    1,  1,  1,  1,
    1,  1,  1,  1,
    1,  1,  1,  1
  };

static const uint8_t next_state_MPS[64] =
  {
    1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,
    17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,
    33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,
    49,50,51,52,53,54,55,56,57,58,59,60,61,62,62,63
  };

static const uint8_t next_state_LPS[64] =
  {
    0,0,1,2,2,4,4,5,6,7,8,9,9,11,11,12,
    13,13,15,15,16,16,18,18,19,19,21,21,22,22,23,24,
    24,25,26,26,27,27,28,29,29,30,30,30,31,32,32,33,
    33,33,34,34,35,35,35,36,36,36,37,37,37,38,38,63
  };





#ifdef DE265_LOG_TRACE
int logcnt=1;
#endif

void init_CABAC_decoder(CABAC_decoder* decoder, uint8_t* bitstream, int length)
{
  assert(length >= 0);

  decoder->bitstream_start = bitstream;
  decoder->bitstream_curr  = bitstream;
  decoder->bitstream_end   = bitstream+length;
}

void init_CABAC_decoder_2(CABAC_decoder* decoder)
{
  int length = decoder->bitstream_end - decoder->bitstream_curr;

  decoder->range = 510;
  decoder->bits_needed = 8;

  decoder->value = 0;

  if (length>0) { decoder->value  = (*decoder->bitstream_curr++) << 8;  decoder->bits_needed-=8; }
  if (length>1) { decoder->value |= (*decoder->bitstream_curr++);       decoder->bits_needed-=8; }

  logtrace(LogCABAC,"[%3d] init_CABAC_decode_2 r:%x v:%x\n", logcnt, decoder->range, decoder->value);
}


int  decode_CABAC_bit(CABAC_decoder* decoder, context_model* model)
{
  logtrace(LogCABAC,"[%3d] decodeBin r:%x v:%x state:%d\n",logcnt,decoder->range, decoder->value, model->state);

  int decoded_bit;
  int LPS = LPS_table[model->state][ ( decoder->range >> 6 ) - 4 ];
  decoder->range -= LPS;

  uint32_t scaled_range = decoder->range << 7;

  logtrace(LogCABAC,"[%3d] sr:%x v:%x\n",logcnt,scaled_range, decoder->value);

  if (decoder->value < scaled_range)
    {
      logtrace(LogCABAC,"[%3d] MPS\n",logcnt);

      // MPS path

      decoded_bit = model->MPSbit;
      model->state = next_state_MPS[model->state];

      if (scaled_range < ( 256 << 7 ) )
        {
          // scaled range, highest bit (15) not set

          decoder->range = scaled_range >> 6; // shift range by one bit
          decoder->value <<= 1;               // shift value by one bit
          decoder->bits_needed++;

          if (decoder->bits_needed == 0)
            {
              decoder->bits_needed = -8;
              if (decoder->bitstream_curr < decoder->bitstream_end)
                { decoder->value |= *decoder->bitstream_curr++; }
            }
        }
    }
  else
    {
      logtrace(LogCABAC,"[%3d] LPS\n",logcnt);
      //printf("%d %d\n", model->state, 0);

      // LPS path

      decoder->value = (decoder->value - scaled_range);

      int num_bits = renorm_table[ LPS >> 3 ];
      decoder->value <<= num_bits;
      decoder->range   = LPS << num_bits;  /* this is always >= 0x100 except for state 63,
                                              but state 63 is never used */

      int num_bitsTab = renorm_table[ LPS >> 3 ];

      assert(num_bits == num_bitsTab);

      decoded_bit      = 1 - model->MPSbit;

      if (model->state==0) { model->MPSbit = 1-model->MPSbit; }
      model->state = next_state_LPS[model->state];

      decoder->bits_needed += num_bits;

      if (decoder->bits_needed >= 0)
        {
          logtrace(LogCABAC,"bits_needed: %d\n", decoder->bits_needed);
          if (decoder->bitstream_curr < decoder->bitstream_end)
            { decoder->value |= (*decoder->bitstream_curr++) << decoder->bits_needed; }

          decoder->bits_needed -= 8;
        }
    }

  logtrace(LogCABAC,"[%3d] -> bit %d  r:%x v:%x\n", logcnt, decoded_bit, decoder->range, decoder->value);
#ifdef DE265_LOG_TRACE
  logcnt++;
#endif

  return decoded_bit;
}

int  decode_CABAC_term_bit(CABAC_decoder* decoder)
{
  logtrace(LogCABAC,"CABAC term: range=%x\n", decoder->range);

  decoder->range -= 2;
  uint32_t scaledRange = decoder->range << 7;

  if (decoder->value >= scaledRange)
    {
      return 1;
    }
  else
    {
      // there is a while loop in the standard, but it will always be executed only once

      if (scaledRange < (256<<7))
        {
          decoder->range = scaledRange >> 6;
          decoder->value *= 2;

          decoder->bits_needed++;
          if (decoder->bits_needed==0)
            {
              decoder->bits_needed = -8;

              if (decoder->bitstream_curr < decoder->bitstream_end) {
                decoder->value += (*decoder->bitstream_curr++);
              }
            }
        }

      return 0;
    }
}


// When we read past the end of the bitstream (which should only happen on faulty bitstreams),
// we will eventually only return zeros.
int  decode_CABAC_bypass(CABAC_decoder* decoder)
{
  logtrace(LogCABAC,"[%3d] bypass r:%x v:%x\n",logcnt,decoder->range, decoder->value);

  decoder->value <<= 1;
  decoder->bits_needed++;

  if (decoder->bits_needed >= 0)
    {
      if (decoder->bitstream_end > decoder->bitstream_curr) {
        decoder->bits_needed = -8;
        decoder->value |= *decoder->bitstream_curr++;
      }
      else {
        // we read past the end of the bitstream, fill with 0
        decoder->bits_needed = -8;
      }
    }

  int bit;
  uint32_t scaled_range = decoder->range << 7;
  if (decoder->value >= scaled_range)
    {
      decoder->value -= scaled_range;
      bit=1;
    }
  else
    {
      bit=0;
    }

  logtrace(LogCABAC,"[%3d] -> bit %d  r:%x v:%x\n", logcnt, bit, decoder->range, decoder->value);
#ifdef DE265_LOG_TRACE
  logcnt++;
#endif

  return bit;
}


int  decode_CABAC_TU_bypass(CABAC_decoder* decoder, int cMax)
{
  for (int i=0;i<cMax;i++)
    {
      int bit = decode_CABAC_bypass(decoder);
      if (bit==0)
        return i;
    }

  return cMax;
}

int  decode_CABAC_TU(CABAC_decoder* decoder, int cMax, context_model* model)
{
  for (int i=0;i<cMax;i++)
    {
      int bit = decode_CABAC_bit(decoder,model);
      if (bit==0)
        return i;
    }

  return cMax;
}


int  decode_CABAC_FL_bypass_parallel(CABAC_decoder* decoder, int nBits)
{
  logtrace(LogCABAC,"[%3d] bypass group r:%x v:%x (nBits=%d)\n",logcnt,
           decoder->range, decoder->value, nBits);

  decoder->value <<= nBits;
  decoder->bits_needed+=nBits;

  if (decoder->bits_needed >= 0)
    {
      if (decoder->bitstream_end > decoder->bitstream_curr) {
        int input = *decoder->bitstream_curr++;
        input <<= decoder->bits_needed;

        decoder->bits_needed -= 8;
        decoder->value |= input;
      }
    }

  uint32_t scaled_range = decoder->range << 7;
  int value = decoder->value / scaled_range;
  if (unlikely(value>=(1<<nBits))) { value=(1<<nBits)-1; } // may happen with broken bitstreams
  decoder->value -= value * scaled_range;

  logtrace(LogCABAC,"[%3d] -> value %d  r:%x v:%x\n", logcnt+nBits-1,
           value, decoder->range, decoder->value);

#ifdef DE265_LOG_TRACE
  logcnt+=nBits;
#endif

  return value;
}


uint32_t  decode_CABAC_FL_bypass(CABAC_decoder* decoder, int nBits)
{
  uint32_t value=0;

  if (likely(nBits<=8)) {
    if (nBits==0) {
      return 0;
    }
    // we could use decode_CABAC_bypass() for a single bit, but this seems to be slower
#if 0
    else if (nBits==1) {
      value = decode_CABAC_bypass(decoder);
    }
#endif
    else {
      value = decode_CABAC_FL_bypass_parallel(decoder,nBits);
    }
  }
  else {
    value = decode_CABAC_FL_bypass_parallel(decoder,8);
    nBits-=8;

    while (nBits--) {
      value <<= 1;
      value |= decode_CABAC_bypass(decoder);
    }
  }
  logtrace(LogCABAC,"      -> FL: %d\n", value);

  return value;
}

int  decode_CABAC_TR_bypass(CABAC_decoder* decoder, int cRiceParam, int cTRMax)
{
  int prefix = decode_CABAC_TU_bypass(decoder, cTRMax>>cRiceParam);
  if (prefix==4) { // TODO check: constant 4 only works for coefficient decoding
    return cTRMax;
  }

  int suffix = decode_CABAC_FL_bypass(decoder, cRiceParam);

  return (prefix << cRiceParam) | suffix;
}


#define MAX_PREFIX 32

int  decode_CABAC_EGk_bypass(CABAC_decoder* decoder, int k)
{
  int base=0;
  int n=k;

  for (;;)
    {
      int bit = decode_CABAC_bypass(decoder);
      if (bit==0)
        break;
      else {
        base += 1<<n;
        n++;
      }

      if (n == k+MAX_PREFIX) {
        return 0; // TODO: error
      }
    }

  int suffix = decode_CABAC_FL_bypass(decoder, n);
  return base + suffix;
}


// ---------------------------------------------------------------------------

void CABAC_encoder::add_trailing_bits()
{
  write_bit(1);
  int nZeros = number_free_bits_in_byte();
  write_bits(0, nZeros);
}



CABAC_encoder_bitstream::CABAC_encoder_bitstream()
{
  data_mem = NULL;
  data_capacity = 0;
  data_size = 0;
  state = 0;

  vlc_buffer_len = 0;

  init_CABAC();
}

CABAC_encoder_bitstream::~CABAC_encoder_bitstream()
{
  free(data_mem);
}

void CABAC_encoder_bitstream::reset()
{
  data_size = 0;
  state = 0;

  vlc_buffer_len = 0;

  init_CABAC();
}

void CABAC_encoder_bitstream::write_bits(uint32_t bits,int n)
{
  vlc_buffer <<= n;
  vlc_buffer |= bits;
  vlc_buffer_len += n;

  // TODO: errors returned by append_byte() are ignored, resulting in a broken output.

  while (vlc_buffer_len>=8) {
    append_byte((vlc_buffer >> (vlc_buffer_len-8)) & 0xFF);
    vlc_buffer_len -= 8;
  }
}

void CABAC_encoder::write_uvlc(int value)
{
  assert(value>=0);

  int nLeadingZeros=0;
  int base=0;
  int range=1;

  while (value>=base+range) {
    base += range;
    range <<= 1;
    nLeadingZeros++;
  }

  write_bits((1<<nLeadingZeros) | (value-base),2*nLeadingZeros+1);
}

void CABAC_encoder::write_svlc(int value)
{
  if      (value==0) write_bits(1,1);
  else if (value>0)  write_uvlc(2*value-1);
  else               write_uvlc(-2*value);
}

void CABAC_encoder_bitstream::flush_VLC()
{
  // TODO: errors returned by append_byte() are ignored, resulting in a broken output.

  while (vlc_buffer_len>=8) {
    append_byte((vlc_buffer >> (vlc_buffer_len-8)) & 0xFF);
    vlc_buffer_len -= 8;
  }

  if (vlc_buffer_len>0) {
    append_byte(vlc_buffer << (8-vlc_buffer_len));
    vlc_buffer_len = 0;
  }

  vlc_buffer = 0;
}

void CABAC_encoder_bitstream::skip_bits(int nBits)
{
  while (nBits>=8) {
    write_bits(0,8);
    nBits-=8;
  }

  if (nBits>0) {
    write_bits(0,nBits);
  }
}


int  CABAC_encoder_bitstream::number_free_bits_in_byte() const
{
  if ((vlc_buffer_len % 8)==0) return 0;
  return 8- (vlc_buffer_len % 8);
}


bool CABAC_encoder_bitstream::check_size_and_resize(int nBytes)
{
  if (data_size+nBytes > data_capacity) { // 1 extra byte for stuffing
    if (data_capacity==0) {
      data_capacity = INITIAL_CABAC_BUFFER_CAPACITY;
    } else {
      data_capacity *= 2;
    }

    uint8_t* new_data_mem = (uint8_t*)realloc(data_mem,data_capacity);
    if (new_data_mem) {
      data_mem = new_data_mem;
    }
    else {
      return false;
    }
  }

  return true;
}


bool CABAC_encoder_bitstream::append_byte(int byte)
{
  if (!check_size_and_resize(2)) {
    return false;
  }

  // --- emulation prevention ---

  /* These byte sequences may never occur in the bitstream:
     0x000000 / 0x000001 / 0x000002

     Hence, we have to add a 0x03 before the third byte.
     We also have to add a 0x03 for this sequence: 0x000003, because
     the escape byte itself also has to be escaped.
  */

  // S0 --(0)--> S1 --(0)--> S2 --(0,1,2,3)--> add stuffing

  if (byte<=3) {
    /**/ if (state< 2 && byte==0) { state++; }
    else if (state==2 && byte<=3) {
      data_mem[ data_size++ ] = 3;

      if (byte==0) state=1;
      else         state=0;
    }
    else { state=0; }
  }
  else { state=0; }


  // write actual data byte

  data_mem[ data_size++ ] = byte;

  return true;
}


bool CABAC_encoder_bitstream::write_startcode()
{
  if (!check_size_and_resize(3)) {
    return false;
  }

  data_mem[ data_size+0 ] = 0;
  data_mem[ data_size+1 ] = 0;
  data_mem[ data_size+2 ] = 1;
  data_size+=3;

  return true;
}

void CABAC_encoder_bitstream::init_CABAC()
{
  range = 510;
  low = 0;

  bits_left = 23;
  buffered_byte = 0xFF;
  num_buffered_bytes = 0;
}

void CABAC_encoder_bitstream::flush_CABAC()
{
  // TODO: errors returned by append_byte() are ignored, resulting in a broken output.

  if (low >> (32 - bits_left))
    {
      append_byte(buffered_byte + 1);
      while (num_buffered_bytes > 1)
        {
          append_byte(0x00);
          num_buffered_bytes--;
        }

      low -= 1 << (32 - bits_left);
    }
  else
    {
      if (num_buffered_bytes > 0)
        {
          append_byte(buffered_byte);
        }

      while (num_buffered_bytes > 1)
        {
          append_byte(0xff);
          num_buffered_bytes--;
        }
    }

  // printf("low: %08x  nbits left:%d  filled:%d\n",low,bits_left,32-bits_left);

  write_bits(low >> 8, 24-bits_left);
}


void CABAC_encoder_bitstream::write_out()
{
  // TODO: errors returned by append_byte() are ignored, resulting in a broken output.
  
  //logtrace(LogCABAC,"low = %08x (bits_left=%d)\n",low,bits_left);
  int leadByte = low >> (24 - bits_left);
  bits_left += 8;
  low &= 0xffffffffu >> bits_left;

  //logtrace(LogCABAC,"write byte %02x\n",leadByte);
  //logtrace(LogCABAC,"-> low = %08x\n",low);

  if (leadByte == 0xff)
    {
      num_buffered_bytes++;
    }
  else
    {
      if (num_buffered_bytes > 0)
        {
          int carry = leadByte >> 8;
          int byte = buffered_byte + carry;
          buffered_byte = leadByte & 0xff;
          append_byte(byte);

          byte = ( 0xff + carry ) & 0xff;
          while ( num_buffered_bytes > 1 )
            {
              append_byte(byte);
              num_buffered_bytes--;
            }
        }
      else
        {
          num_buffered_bytes = 1;
          buffered_byte = leadByte;
        }
    }
}

void CABAC_encoder_bitstream::testAndWriteOut()
{
  // logtrace(LogCABAC,"bits_left = %d\n",bits_left);

  if (bits_left < 12)
    {
      write_out();
    }
}


#ifdef DE265_LOG_TRACE
int encBinCnt=1;
#endif

void CABAC_encoder_bitstream::write_CABAC_bit(int modelIdx, int bin)
{
  context_model* model = &(*mCtxModels)[modelIdx];
  //m_uiBinsCoded += m_binCountIncrement;
  //rcCtxModel.setBinsCoded( 1 );

  logtrace(LogCABAC,"[%d] range=%x low=%x state=%d, bin=%d\n",
           encBinCnt, range,low, model->state,bin);

  /*
  printf("[%d] range=%x low=%x state=%d, bin=%d\n",
         encBinCnt, range,low, model->state,bin);

  printf("%d %d X\n",model->state,bin != model->MPSbit);
  */

#ifdef DE265_LOG_TRACE
  encBinCnt++;
#endif

  uint32_t LPS = LPS_table[model->state][ ( range >> 6 ) - 4 ];
  range -= LPS;

  if (bin != model->MPSbit)
    {
      //logtrace(LogCABAC,"LPS\n");

      int num_bits = renorm_table[ LPS >> 3 ];
      low = (low + range) << num_bits;
      range   = LPS << num_bits;

      if (model->state==0) { model->MPSbit = 1-model->MPSbit; }

      model->state = next_state_LPS[model->state];

      bits_left -= num_bits;
    }
  else
    {
      //logtrace(LogCABAC,"MPS\n");

      model->state = next_state_MPS[model->state];


      // renorm

      if (range >= 256) { return; }

      low <<= 1;
      range <<= 1;
      bits_left--;
    }

  testAndWriteOut();
}

void CABAC_encoder_bitstream::write_CABAC_bypass(int bin)
{
  logtrace(LogCABAC,"[%d] bypass = %d, range=%x\n",encBinCnt,bin,range);
  /*
  printf("[%d] bypass = %d, range=%x\n",encBinCnt,bin,range);
  printf("%d %d X\n",64, -1);
  */

#ifdef DE265_LOG_TRACE
  encBinCnt++;
#endif

  // BinsCoded += m_binCountIncrement;
  low <<= 1;

  if (bin)
    {
      low += range;
    }
  bits_left--;

  testAndWriteOut();
}

void CABAC_encoder::write_CABAC_TU_bypass(int value, int cMax)
{
  for (int i=0;i<value;i++) {
    write_CABAC_bypass(1);
  }

  if (value<cMax) {
    write_CABAC_bypass(0);
  }
}

void CABAC_encoder::write_CABAC_FL_bypass(int value, int n)
{
  while (n>0) {
    n--;
    write_CABAC_bypass(value & (1<<n));
  }
}

void CABAC_encoder_bitstream::write_CABAC_term_bit(int bit)
{
  logtrace(LogCABAC,"CABAC term: range=%x\n", range);

  range -= 2;

  if (bit) {
    low += range;

    low <<= 7;
    range = 2 << 7;
    bits_left -= 7;
  }
  else if (range >= 256)
    {
      return;
    }
  else
    {
      low   <<= 1;
      range <<= 1;
      bits_left--;
    }

  testAndWriteOut();
}




static const uint32_t entropy_table[128] = {
  // -------------------- 200 --------------------
  /* state= 0 */  0x07d13 /* 0.977164 */,  0x08255 /* 1.018237 */,
  /* state= 1 */  0x07738 /* 0.931417 */,  0x086ef /* 1.054179 */,
  /* state= 2 */  0x0702b /* 0.876323 */,  0x0935a /* 1.151195 */,
  /* state= 3 */  0x069e6 /* 0.827333 */,  0x09c7f /* 1.222650 */,
  /* state= 4 */  0x062e8 /* 0.772716 */,  0x0a2c7 /* 1.271708 */,
  /* state= 5 */  0x05c18 /* 0.719488 */,  0x0ae25 /* 1.360532 */,
  /* state= 6 */  0x05632 /* 0.673414 */,  0x0b724 /* 1.430793 */,
  /* state= 7 */  0x05144 /* 0.634904 */,  0x0c05d /* 1.502850 */,
  /* state= 8 */  0x04bdf /* 0.592754 */,  0x0ccf2 /* 1.601145 */,
  /* state= 9 */  0x0478d /* 0.559012 */,  0x0d57b /* 1.667843 */,
  /* state=10 */  0x042ad /* 0.520924 */,  0x0de81 /* 1.738336 */,
  /* state=11 */  0x03f4d /* 0.494564 */,  0x0e4b8 /* 1.786871 */,
  /* state=12 */  0x03a9d /* 0.457945 */,  0x0f471 /* 1.909721 */,
  /* state=13 */  0x037d5 /* 0.436201 */,  0x0fc56 /* 1.971385 */,
  /* state=14 */  0x034c2 /* 0.412177 */,  0x10236 /* 2.017284 */,
  /* state=15 */  0x031a6 /* 0.387895 */,  0x10d5c /* 2.104394 */,
  /* state=16 */  0x02e62 /* 0.362383 */,  0x11b34 /* 2.212552 */,
  /* state=17 */  0x02c20 /* 0.344752 */,  0x120b4 /* 2.255512 */,
  /* state=18 */  0x029b8 /* 0.325943 */,  0x1294d /* 2.322672 */,
  /* state=19 */  0x02791 /* 0.309143 */,  0x135e1 /* 2.420959 */,
  /* state=20 */  0x02562 /* 0.292057 */,  0x13e37 /* 2.486077 */,
  /* state=21 */  0x0230d /* 0.273846 */,  0x144fd /* 2.539000 */,
  /* state=22 */  0x02193 /* 0.262308 */,  0x150c9 /* 2.631150 */,
  /* state=23 */  0x01f5d /* 0.245026 */,  0x15ca0 /* 2.723641 */,
  /* state=24 */  0x01de7 /* 0.233617 */,  0x162f9 /* 2.773246 */,
  /* state=25 */  0x01c2f /* 0.220208 */,  0x16d99 /* 2.856259 */,
  /* state=26 */  0x01a8e /* 0.207459 */,  0x17a93 /* 2.957634 */,
  /* state=27 */  0x0195a /* 0.198065 */,  0x18051 /* 3.002477 */,
  /* state=28 */  0x01809 /* 0.187778 */,  0x18764 /* 3.057759 */,
  /* state=29 */  0x0164a /* 0.174144 */,  0x19460 /* 3.159206 */,
  /* state=30 */  0x01539 /* 0.165824 */,  0x19f20 /* 3.243181 */,
  /* state=31 */  0x01452 /* 0.158756 */,  0x1a465 /* 3.284334 */,
  /* state=32 */  0x0133b /* 0.150261 */,  0x1b422 /* 3.407303 */,
  /* state=33 */  0x0120c /* 0.140995 */,  0x1bce5 /* 3.475767 */,
  /* state=34 */  0x01110 /* 0.133315 */,  0x1c394 /* 3.527962 */,
  /* state=35 */  0x0104d /* 0.127371 */,  0x1d059 /* 3.627736 */,
  /* state=36 */  0x00f8b /* 0.121451 */,  0x1d74b /* 3.681983 */,
  /* state=37 */  0x00ef4 /* 0.116829 */,  0x1dfd0 /* 3.748540 */,
  /* state=38 */  0x00e10 /* 0.109864 */,  0x1e6d3 /* 3.803335 */,
  /* state=39 */  0x00d3f /* 0.103507 */,  0x1f925 /* 3.946462 */,
  /* state=40 */  0x00cc4 /* 0.099758 */,  0x1fda7 /* 3.981667 */,
  /* state=41 */  0x00c42 /* 0.095792 */,  0x203f8 /* 4.031012 */,
  /* state=42 */  0x00b78 /* 0.089610 */,  0x20f7d /* 4.121014 */,
  /* state=43 */  0x00afc /* 0.085830 */,  0x21dd6 /* 4.233102 */,
  /* state=44 */  0x00a5e /* 0.081009 */,  0x22419 /* 4.282016 */,
  /* state=45 */  0x00a1b /* 0.078950 */,  0x22a5e /* 4.331015 */,
  /* state=46 */  0x00989 /* 0.074514 */,  0x23756 /* 4.432323 */,
  /* state=47 */  0x0091b /* 0.071166 */,  0x24225 /* 4.516775 */,
  /* state=48 */  0x008cf /* 0.068837 */,  0x2471a /* 4.555487 */,
  /* state=49 */  0x00859 /* 0.065234 */,  0x25313 /* 4.649048 */,
  /* state=50 */  0x00814 /* 0.063140 */,  0x25d67 /* 4.729721 */,
  /* state=51 */  0x007b6 /* 0.060272 */,  0x2651f /* 4.790028 */,
  /* state=52 */  0x0076e /* 0.058057 */,  0x2687c /* 4.816294 */,
  /* state=53 */  0x00707 /* 0.054924 */,  0x27da7 /* 4.981661 */,
  /* state=54 */  0x006d5 /* 0.053378 */,  0x28172 /* 5.011294 */,
  /* state=55 */  0x00659 /* 0.049617 */,  0x28948 /* 5.072512 */,
  /* state=56 */  0x00617 /* 0.047598 */,  0x297c5 /* 5.185722 */,
  /* state=57 */  0x005dd /* 0.045814 */,  0x2a2df /* 5.272434 */,
  /* state=58 */  0x005c1 /* 0.044965 */,  0x2a581 /* 5.293019 */,
  /* state=59 */  0x00574 /* 0.042619 */,  0x2ad59 /* 5.354304 */,
  /* state=60 */  0x0053b /* 0.040882 */,  0x2bba5 /* 5.465973 */,
  /* state=61 */  0x0050c /* 0.039448 */,  0x2c596 /* 5.543651 */,
  /* state=62 */  0x004e9 /* 0.038377 */,  0x2cd88 /* 5.605741 */,
  0x00400 ,  0x2d000 /* dummy, should never be used */
};


static const uint32_t entropy_table_orig[128] = {
  0x07b23, 0x085f9, 0x074a0, 0x08cbc, 0x06ee4, 0x09354, 0x067f4, 0x09c1b,
  0x060b0, 0x0a62a, 0x05a9c, 0x0af5b, 0x0548d, 0x0b955, 0x04f56, 0x0c2a9,
  0x04a87, 0x0cbf7, 0x045d6, 0x0d5c3, 0x04144, 0x0e01b, 0x03d88, 0x0e937,
  0x039e0, 0x0f2cd, 0x03663, 0x0fc9e, 0x03347, 0x10600, 0x03050, 0x10f95,
  0x02d4d, 0x11a02, 0x02ad3, 0x12333, 0x0286e, 0x12cad, 0x02604, 0x136df,
  0x02425, 0x13f48, 0x021f4, 0x149c4, 0x0203e, 0x1527b, 0x01e4d, 0x15d00,
  0x01c99, 0x166de, 0x01b18, 0x17017, 0x019a5, 0x17988, 0x01841, 0x18327,
  0x016df, 0x18d50, 0x015d9, 0x19547, 0x0147c, 0x1a083, 0x0138e, 0x1a8a3,
  0x01251, 0x1b418, 0x01166, 0x1bd27, 0x01068, 0x1c77b, 0x00f7f, 0x1d18e,
  0x00eda, 0x1d91a, 0x00e19, 0x1e254, 0x00d4f, 0x1ec9a, 0x00c90, 0x1f6e0,
  0x00c01, 0x1fef8, 0x00b5f, 0x208b1, 0x00ab6, 0x21362, 0x00a15, 0x21e46,
  0x00988, 0x2285d, 0x00934, 0x22ea8, 0x008a8, 0x239b2, 0x0081d, 0x24577,
  0x007c9, 0x24ce6, 0x00763, 0x25663, 0x00710, 0x25e8f, 0x006a0, 0x26a26,
  0x00672, 0x26f23, 0x005e8, 0x27ef8, 0x005ba, 0x284b5, 0x0055e, 0x29057,
  0x0050c, 0x29bab, 0x004c1, 0x2a674, 0x004a7, 0x2aa5e, 0x0046f, 0x2b32f,
  0x0041f, 0x2c0ad, 0x003e7, 0x2ca8d, 0x003ba, 0x2d323, 0x0010c, 0x3bfbb
};


const uint32_t entropy_table_theory[128] =
  {
    0x08000, 0x08000, 0x076da, 0x089a0, 0x06e92, 0x09340, 0x0670a, 0x09cdf, 0x06029, 0x0a67f, 0x059dd, 0x0b01f, 0x05413, 0x0b9bf, 0x04ebf, 0x0c35f,
    0x049d3, 0x0ccff, 0x04546, 0x0d69e, 0x0410d, 0x0e03e, 0x03d22, 0x0e9de, 0x0397d, 0x0f37e, 0x03619, 0x0fd1e, 0x032ee, 0x106be, 0x02ffa, 0x1105d,
    0x02d37, 0x119fd, 0x02aa2, 0x1239d, 0x02836, 0x12d3d, 0x025f2, 0x136dd, 0x023d1, 0x1407c, 0x021d2, 0x14a1c, 0x01ff2, 0x153bc, 0x01e2f, 0x15d5c,
    0x01c87, 0x166fc, 0x01af7, 0x1709b, 0x0197f, 0x17a3b, 0x0181d, 0x183db, 0x016d0, 0x18d7b, 0x01595, 0x1971b, 0x0146c, 0x1a0bb, 0x01354, 0x1aa5a,
    0x0124c, 0x1b3fa, 0x01153, 0x1bd9a, 0x01067, 0x1c73a, 0x00f89, 0x1d0da, 0x00eb7, 0x1da79, 0x00df0, 0x1e419, 0x00d34, 0x1edb9, 0x00c82, 0x1f759,
    0x00bda, 0x200f9, 0x00b3c, 0x20a99, 0x00aa5, 0x21438, 0x00a17, 0x21dd8, 0x00990, 0x22778, 0x00911, 0x23118, 0x00898, 0x23ab8, 0x00826, 0x24458,
    0x007ba, 0x24df7, 0x00753, 0x25797, 0x006f2, 0x26137, 0x00696, 0x26ad7, 0x0063f, 0x27477, 0x005ed, 0x27e17, 0x0059f, 0x287b6, 0x00554, 0x29156,
    0x0050e, 0x29af6, 0x004cc, 0x2a497, 0x0048d, 0x2ae35, 0x00451, 0x2b7d6, 0x00418, 0x2c176, 0x003e2, 0x2cb15, 0x003af, 0x2d4b5, 0x0037f, 0x2de55
  };


void CABAC_encoder_estim::write_CABAC_bit(int modelIdx, int bit)
{
  context_model* model = &(*mCtxModels)[modelIdx];
  //printf("[%d] state=%d, bin=%d\n", encBinCnt, model->state,bit);
  //encBinCnt++;

  int idx = model->state<<1;

  if (bit==model->MPSbit) {
    model->state = next_state_MPS[model->state];
  }
  else {
    idx++;
    if (model->state==0) { model->MPSbit = 1-model->MPSbit; }
    model->state = next_state_LPS[model->state];
  }

  mFracBits += entropy_table[idx];

  //printf("-> %08lx %f\n",entropy_table[idx], entropy_table[idx] / float(1<<15));
}


float CABAC_encoder::RDBits_for_CABAC_bin(int modelIdx, int bit)
{
  context_model* model = &(*mCtxModels)[modelIdx];
  int idx = model->state<<1;

  if (bit!=model->MPSbit) {
    idx++;
  }

  return entropy_table[idx] / float(1<<15);
}


void CABAC_encoder::write_CABAC_EGk(int val, int k)
{
  while (val  >=  ( 1 << k ) ) {
    write_CABAC_bypass(1);
    val = val - ( 1 << k );
    k++;
  }

  write_CABAC_bypass(0);

  while (k)  {
    k--;
    write_CABAC_bypass((val >> k) & 1);
  }
}



void CABAC_encoder_estim_constant::write_CABAC_bit(int modelIdx, int bit)
{
  context_model* model = &(*mCtxModels)[modelIdx];
  int idx = model->state<<1;

  if (bit!=model->MPSbit) {
    idx++;
  }

  mFracBits += entropy_table[idx];
}



#if 0
void printtab(int idx,int s)
{
  printf("%d %f %f %f\n", s,
         double(entropy_table[idx])/0x8000,
         double(entropy_table_orig[idx])/0x8000,
         double(entropy_table_f265[idx])/0x8000);
}


void plot_tables()
{
  for (int i=-62;i<=0;i++) {
    int idx = -i *2;
    int s = i;
    printtab(idx,s);
  }

  for (int i=0;i<=62;i++) {
    int idx = 2*i +1;
    int s = i;
    printtab(idx,s);
  }
}
#endif
