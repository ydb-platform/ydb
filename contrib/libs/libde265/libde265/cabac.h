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

#ifndef DE265_CABAC_H
#define DE265_CABAC_H

#include <stdint.h>
#include "contextmodel.h"


typedef struct {
  uint8_t* bitstream_start;
  uint8_t* bitstream_curr;
  uint8_t* bitstream_end;

  uint32_t range;
  uint32_t value;
  int16_t  bits_needed;
} CABAC_decoder;


void init_CABAC_decoder(CABAC_decoder* decoder, uint8_t* bitstream, int length);
void init_CABAC_decoder_2(CABAC_decoder* decoder);
int  decode_CABAC_bit(CABAC_decoder* decoder, context_model* model);
int  decode_CABAC_TU(CABAC_decoder* decoder, int cMax, context_model* model);
int  decode_CABAC_term_bit(CABAC_decoder* decoder);

int  decode_CABAC_bypass(CABAC_decoder* decoder);
int  decode_CABAC_TU_bypass(CABAC_decoder* decoder, int cMax);
uint32_t  decode_CABAC_FL_bypass(CABAC_decoder* decoder, int nBits);
int  decode_CABAC_TR_bypass(CABAC_decoder* decoder, int cRiceParam, int cTRMax);
int  decode_CABAC_EGk_bypass(CABAC_decoder* decoder, int k);


// ---------------------------------------------------------------------------

class CABAC_encoder
{
public:
 CABAC_encoder() : mCtxModels(NULL) { }
  virtual ~CABAC_encoder() { }

  virtual int size() const = 0;
  virtual void reset() = 0;

  // --- VLC ---

  virtual void write_bits(uint32_t bits,int n) = 0;
  virtual void write_bit(int bit) { write_bits(bit,1); }
  virtual void write_uvlc(int value);
  virtual void write_svlc(int value);
  virtual bool write_startcode() = 0;
  virtual void skip_bits(int nBits) = 0;

  virtual void add_trailing_bits();
  virtual int  number_free_bits_in_byte() const = 0;

  // output all remaining bits and fill with zeros to next byte boundary
  virtual void flush_VLC() { }


  // --- CABAC ---

  void set_context_models(context_model_table* models) { mCtxModels=models; }

  virtual void init_CABAC() { }
  virtual void write_CABAC_bit(int modelIdx, int bit) = 0;
  virtual void write_CABAC_bypass(int bit) = 0;
  virtual void write_CABAC_TU_bypass(int value, int cMax);
  virtual void write_CABAC_FL_bypass(int value, int nBits);
  virtual void write_CABAC_term_bit(int bit) = 0;
  virtual void flush_CABAC()  { }

  void write_CABAC_EGk(int absolute_symbol, int k); // absolute_symbol >= 0

  virtual bool modifies_context() const = 0;

  float RDBits_for_CABAC_bin(int modelIdx, int bit);

 protected:
  context_model_table* mCtxModels;
};


class CABAC_encoder_bitstream : public CABAC_encoder
{
public:
  CABAC_encoder_bitstream();
  ~CABAC_encoder_bitstream();

  virtual void reset();

  virtual int size() const { return data_size; }
  uint8_t* data() const { return data_mem; }

  // --- VLC ---

  virtual void write_bits(uint32_t bits,int n);
  virtual bool write_startcode();
  virtual void skip_bits(int nBits);

  virtual int  number_free_bits_in_byte() const;

  // output all remaining bits and fill with zeros to next byte boundary
  virtual void flush_VLC();


  // --- CABAC ---

  virtual void init_CABAC();
  virtual void write_CABAC_bit(int modelIdx, int bit);
  virtual void write_CABAC_bypass(int bit);
  virtual void write_CABAC_term_bit(int bit);
  virtual void flush_CABAC();

  virtual bool modifies_context() const { return true; }

private:
  // data buffer

  uint8_t* data_mem;
  uint32_t data_capacity;
  uint32_t data_size;
  char     state; // for inserting emulation-prevention bytes

  // VLC

  uint32_t vlc_buffer;
  uint32_t vlc_buffer_len;


  // CABAC

  uint32_t range;
  uint32_t low;
  int8_t   bits_left;
  uint8_t  buffered_byte;
  uint16_t num_buffered_bytes;


  bool check_size_and_resize(int nBytes);
  void testAndWriteOut();
  void write_out();
  bool append_byte(int byte);
};


class CABAC_encoder_estim : public CABAC_encoder
{
public:
  CABAC_encoder_estim() : mFracBits(0) { }

  virtual void reset() { mFracBits=0; }

  virtual int size() const { return mFracBits>>(15+3); }

  uint64_t getFracBits() const { return mFracBits; }
  float    getRDBits() const { return mFracBits / float(1<<15); }

  // --- VLC ---

  virtual void write_bits(uint32_t bits,int n) { mFracBits += n<<15; }
  virtual void write_bit(int bit) { mFracBits+=1<<15; }
  virtual bool write_startcode() { mFracBits += (1<<15)*8*3; return true; }
  virtual void skip_bits(int nBits) { mFracBits += nBits<<15; }
  virtual int  number_free_bits_in_byte() const { return 0; } // TODO, good enough for now

  // --- CABAC ---

  virtual void write_CABAC_bit(int modelIdx, int bit);
  virtual void write_CABAC_bypass(int bit) {
    mFracBits += 0x8000;
  }
  virtual void write_CABAC_FL_bypass(int value, int nBits) {
    mFracBits += nBits<<15;
  }
  virtual void write_CABAC_term_bit(int bit) { /* not implemented (not needed) */ }

  virtual bool modifies_context() const { return true; }

 protected:
  uint64_t mFracBits;
};


class CABAC_encoder_estim_constant : public CABAC_encoder_estim
{
 public:
  void write_CABAC_bit(int modelIdx, int bit);

  virtual bool modifies_context() const { return false; }
};

#endif
