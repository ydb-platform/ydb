/* -*- C++ -*-
 * File: pana8.cpp
 * Copyright (C) 2024-2025 Alex Tutubalin, LibRaw LLC
 *
   Olympus/OM System 12/14-bit file decoder

LibRaw is free software; you can redistribute it and/or modify
it under the terms of the one of two licenses as you choose:

1. GNU LESSER GENERAL PUBLIC LICENSE version 2.1
   (See file LICENSE.LGPL provided in LibRaw distribution archive for details).

2. COMMON DEVELOPMENT AND DISTRIBUTION LICENSE (CDDL) Version 1.0
   (See file LICENSE.CDDL provided in LibRaw distribution archive for details).

 */

#include "../../internal/libraw_cxx_defs.h"
#include <vector>


 struct buffered_bitpump_t
{
  uint32_t bitcount;
  uint32_t bitstorage;
  LibRaw_abstract_datastream *input;
  std::vector<uint8_t> buffer;
  uint8_t *bufp;
  int pos, datasz;
  bool is_buf;
  buffered_bitpump_t(LibRaw_abstract_datastream *in, int bufsz) : bitcount(0), bitstorage(0), input(in),
	  buffer(bufsz),pos(0),datasz(0)
	{
	  is_buf = input->is_buffered();
	  input->buffering_off();
	  bufp = buffer.data();
    }
  ~buffered_bitpump_t()
  {
	  if (is_buf)
		  input->buffering_on();
  }
void skip_bit(uint32_t &vbits)
  {
    if (!vbits)
    {
      replace_data();
      vbits = --bitcount;
    }
    else
      bitcount = --vbits;
  }
  void replace_data() 
  {
     bitstorage = get4();
     bitcount = 32;
  }

  uint32_t add_data()
  {
	uint32_t bb = get2();
	bitstorage = bitstorage << 16 | bb;
    bitcount += 16;
	return bitcount;
  }
  uint32_t getbits(unsigned wbits)
  {
    while (bitcount < wbits)
      add_data();
    uint32_t ret = bitstorage << (32 - (bitcount & 0x1f)) >> (32 - wbits);
    bitcount -= wbits;
    return ret;
  }
  uint32_t get2()
  {
    if (pos < datasz - 1)
    {
      uint32_t ret = bufp[pos] << 8 | bufp[pos + 1];
      pos += 2;
      return ret;
    }
    uint8_t b[2];
    int i = 0;
    while (pos < datasz)
      b[i++] = bufp[pos++];
    refill(2);
    for (; i < 2; i++)
      b[i] = bufp[pos++];
    return b[0] << 8 | b[1];
  }

  uint32_t get4()
  {
    if (pos < datasz - 3)
    {
      uint32_t ret = bufp[pos] << 24 | bufp[pos + 1] << 16 | bufp[pos + 2] << 8 | bufp[pos + 3];
      pos += 4;
      return ret;
    }
    uint8_t b[4];
    int i = 0;
    while (pos < datasz)
      b[i++] = bufp[pos++];
    refill(4);
    for (; i < 4; i++)
      b[i] = bufp[pos++];
    return b[0] << 24 | b[1] << 16 | b[2] << 8 | b[3];
  }
  void refill(int b)
  {
	  int r = input->read(bufp, 1, buffer.size());
	  pos = 0;
	  datasz = r > b ? r : b; 
  }
};


static bool checkhdr(LibRaw_abstract_datastream *ifp, int bytes)
{
	if (bytes <= 0) return false;
	uint64_t b64 = 0ULL;
	for(int i = 0; i < bytes; i++)
    {
      uint8_t databyte1 = (uint8_t)ifp->get_char();
      uint64_t q = (b64 << 8) | databyte1;
      b64 = q;
    } 
	if (b64 != 1ULL)
		return false;
	if (ifp->get_char() | ifp->get_char())
		return false;
	return true;
}

static
#ifdef _MSC_VER
    __forceinline
#else
    inline
#endif
int32_t	_local_iabs(int32_t x){	return (x ^ (x >> 31)) - (x >> 31);}

static
#ifdef _MSC_VER
__forceinline
#else
inline
#endif
int32_t oly_code(buffered_bitpump_t *th, unsigned int wbits, unsigned int tag0x640,
                           unsigned int tag0x643, int tag0x652, 
                           int *glc, int *t640bits, int *t643bits)
{
  int highdatabit;

  if (!th->bitcount)
    th->replace_data();

  int v = --th->bitcount;
  highdatabit = (th->bitstorage >> v) & 1;
  *t643bits = tag0x643 ? th->getbits(tag0x643) : 0;
  *t640bits = tag0x640 ? th->getbits(tag0x640) : 0;

  uint32_t vbits = th->bitcount;
  uint32_t high = 0;
  uint32_t low = 0;

  while (1)
  {
    th->skip_bit(vbits);
    if ((th->bitstorage >> vbits) & 1)
      break;
    if ((uint32_t)tag0x652 == ++high)
    {
      high = 0;
      if (wbits != 15)
      {
        uint32_t i;
        for (i = 15 - wbits; vbits < i;)
          vbits = th->add_data();

        high = th->bitstorage << (32 - (vbits & 0x1f)) >> (wbits + 17);
        vbits -= i;
        th->bitcount = vbits;
      }
      th->skip_bit(vbits);
      break;
    }
  } 
  
  if (wbits < 1)
    low = 0;
  else
  {
	  while (vbits < wbits)
		  vbits = th->add_data();
	  low = th->bitstorage << (32 - (vbits & 0x1f)) >> (32 - (wbits & 0x1f));
	  th->bitcount = vbits - wbits;
  }
  *glc = low | (high << wbits);
  int32_t result = *glc ^ (unsigned int)(-highdatabit);
  return result;
}

void LibRaw::olympus_load_raw()
{ 
	if (!checkhdr(libraw_internal_data.internal_data.input, imgdata.makernotes.olympus.tagX653))
		throw LIBRAW_EXCEPTION_IO_CORRUPT;

	const int32_t tag0x640 = imgdata.makernotes.olympus.tagX640; // usually 0
    const uint32_t tag0x643 = imgdata.makernotes.olympus.tagX643; // usually 2

	uint32_t context[4] = { 0,0,0,0 };
	buffered_bitpump_t pump(libraw_internal_data.internal_data.input, 65536);

	const int32_t one_shl_tag0x641 = 1 << imgdata.makernotes.olympus.tagX641;
	const int32_t tag0x642 = imgdata.makernotes.olympus.tagX642;
	const int32_t tag0x644 = imgdata.makernotes.olympus.tagX644;
	const int32_t one_shl_tag645 = 1 << (imgdata.makernotes.olympus.tagX645 & 0x1f);
	const int32_t tag0x646 = imgdata.makernotes.olympus.tagX646;
	const int32_t tag0x647 = imgdata.makernotes.olympus.tagX647;
	const int32_t tag0x648 = imgdata.makernotes.olympus.tagX648;
	const int32_t tag0x649 = imgdata.makernotes.olympus.tagX649;
	const int32_t tag0x650 = imgdata.makernotes.olympus.tagX650;
	const int32_t tag0x651 = imgdata.makernotes.olympus.tagX651;
	const int32_t tag0x652 = MAX(1, imgdata.makernotes.olympus.tagX652);
	const uint16_t datamax = 1<<imgdata.makernotes.olympus.ValidBits;

    const int32_t raw_width = imgdata.sizes.raw_width;
    ushort * const raw_image = imgdata.rawdata.raw_image;

	std::vector<uint32_t> bitcounts(65536);
	bitcounts[0] = 0;
	for (int i = 0, n = 1; i < 16; i++)
		for (int j = 0; j < 1 << i; j++)
			bitcounts[n++] = i+1;

	int32_t lpred = 0, pred1 = 0, glc = 0, gcode=0;

	for (uint32_t row = 0; row < imgdata.sizes.raw_height; row++)
	{
		checkCancel();
		for (uint32_t col = 0; col < raw_width; col++)
		{
          int32_t wbits, vbits, t640bits, t643bits;
          int32_t p = gcode;
		  gcode = glc;
		  int32_t psel = ((col>1) && one_shl_tag645 >= p) ? context[3] + 1 : 0;
		  context[3] = context[2];
		  context[2] = psel;
          if (col>1)
          {
            int highbitcount = bitcounts[p & 0xffff];
            int32_t T1 = ((col > 1) && (psel >= tag0x646)) ? tag0x648 : tag0x650;
            int32_t T2 = ((col > 1) && (psel >= tag0x646)) ? tag0x647 : tag0x649;
            int32_t TT = MAX((highbitcount - T1),-1);
            wbits = T2 + TT + 1;
          }
		  else
			  wbits = tag0x649;

		  if (wbits > tag0x651) 
            wbits = tag0x651;

          int32_t gcode = oly_code(&pump, wbits, tag0x640, tag0x643, tag0x652, &glc, &t640bits, &t643bits);
          if (tag0x643) 
          {
            if (tag0x643 == 1) 
            {
              int32_t v = col<2 ? 0 : pred1;
              gcode = t643bits + 2 * (v + gcode);
              vbits = v + (gcode >> 1);
            }
            else
            {
              int32_t v = col<2 ? 0 : pred1;
              gcode = t643bits + 4 * (v + gcode);
              vbits = ((gcode >> 1) & 0xFFFFFFFE) + v + (gcode >> 2);
            }
          }
          else
            vbits = 0;

		  pred1 = context[0];
		  context[0] = (tag0x644 == 15) ? 0 : vbits >> tag0x644;
          int32_t W = col < 2 ? tag0x642 : context[1];
          int32_t N = row < 2 ? tag0x642 : raw_image[(row - 2) * raw_width + col] >> tag0x640;
          int32_t NW = (row < 2 || col < 2)? tag0x642 : NW = raw_image[(row - 2) * raw_width + col - 2] >> tag0x640;

		  context[1] = lpred;
          if ((W < N) || (NW < W))
		  {
			  if (NW <= N && W >= N)
				  N = W;
			  else if (NW < N || W >= N)
			  {
                if (NW > W || W >= N)
                {
                  if (_local_iabs(N - NW) > one_shl_tag0x641)
                    N += W - NW;
                  else if (_local_iabs(W - NW) <= one_shl_tag0x641)
                    N = (N + W) >> 1;
                  else
                    N = N + W - NW;
                }
              }
              else
                N = W;
		  }
          lpred = gcode + N;
          uint32_t pixel_final_value = t640bits + ((gcode + N) << tag0x640);
          if(((raw_image[row * raw_width + col] = (ushort)pixel_final_value) > datamax)
			  && col >= imgdata.sizes.width)
            derror();
		}
	}
}
