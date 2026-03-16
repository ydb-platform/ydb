/* -*- C++ -*-
 * File: huffmandec.cpp
 * Copyright (C) 2024-2025 Alex Tutubalin, LibRaw LLC
 *
   Lossless JPEG decoder

   Code partially backported from DNGLab's rust code
   DNGLab was originally created in 2021 by Daniel Vogelbacher.

LibRaw is free software; you can redistribute it and/or modify
it under the terms of the one of two licenses as you choose:

1. GNU LESSER GENERAL PUBLIC LICENSE version 2.1
   (See file LICENSE.LGPL provided in LibRaw distribution archive for details).

2. COMMON DEVELOPMENT AND DISTRIBUTION LICENSE (CDDL) Version 1.0
   (See file LICENSE.CDDL provided in LibRaw distribution archive for details).

 */

#include "../../internal/losslessjpeg.h"
#include <string.h>

#define ZERO(a) do { memset(a,0,sizeof(a));} while(0)

bool ByteStreamBE::skip_to_marker() // true: success, false - no marker
{
	if (pos + 2 > size) return false;
	while (!(buffer[pos] == 0xff && buffer[pos + 1] != 0 && buffer[pos + 1] != 0xff))
	{
		pos++;
		if (pos + 2 > size)
			return false;
	}
	pos++;
	return true;
}

LibRaw_LjpegDecompressor::LibRaw_LjpegDecompressor(uint8_t *b, unsigned bs, bool dngbug, bool csfix): buffer(b,bs),
	state(State::NotInited)
{
	initialize(dngbug,csfix);
}

LibRaw_LjpegDecompressor::LibRaw_LjpegDecompressor(uint8_t *b, unsigned bs): buffer(b,bs),
	state(State::NotInited)
{
	initialize(false,false);
}


void LibRaw_LjpegDecompressor::initialize(bool dngbug, bool csfix)
{
	sof.csfix = csfix;
	bool dht_init[4] = { false,false,false,false };
	uint32_t dht_bits[4][17];
	uint32_t dht_huffval[4][256];
	ZERO(dht_bits);
    ZERO(dht_huffval);
    if (next_marker(false) != Marker::SOI)
    {
      state = State::NoSOI;
      return;
    }
	while (1)
	{
		uint8_t marker = next_marker(true);
		if (marker == Marker::SOF3)
		{
			if (!sof.parse_sof(buffer))
			{
              state = State::InvalidSOF;
              return;
			}
			if (sof.precision > 16 || sof.precision < 12)
			{
				state = State::IncorrectPrecision;
				return;
			}
		}
        else if (marker == Marker::DHT)
        {
          bool hres = parse_dht(dht_init, dht_bits, dht_huffval);
		  if(!hres)
		  {
			  state = State::InvalidDHT;
			  return;
		  }
        }
        else if ( marker == Marker::SOS)
        {
          uint32_t val = sof.parse_sos(buffer);
          if (val < 0x10000)
          {
            predictor = (val >> 8) & 0xff;
            point_transform = val & 0xff;
          }
          else
          {
            state = State::InvalidSOS;
            return;
          }
		  break;
        }
		else if (marker == Marker::EOI)
		{
			state = State::EOIReached;
			return;
		}
		else if (marker == Marker::DQT)
		{
          state = State::DQTPresent;
          return;
		}
		else if(marker == Marker::Fill)
		{
          state = State::EOIReached;
          return;
		}
	}

	dhts.resize(4);
	for (int i = 0; i < 4; i++)
		if (dht_init[i])
			dhts[i].initval(dht_bits[i], dht_huffval[i], dngbug);
	datastart = buffer.pos;
    state = State::OK;
}

uint8_t LibRaw_LjpegDecompressor::next_marker(bool allowskip)
{
	if (!allowskip)
	{
		if (buffer.get_u8() != 0xff)
			return Marker::Fill; // Error;
		uint8_t mark = buffer.get_u8();
		return mark;
	}
	if (buffer.skip_to_marker())
		return buffer.get_u8();
	else
		return Marker::Fill;
}

bool LibRaw_LjpegDecompressor::parse_dht(bool init[4], uint32_t bits[4][17], uint32_t huffval[4][256])
{
  uint16_t length = buffer.get_u16() - 2u;

  while (length > 0)
  {
    uint8_t b = buffer.get_u8();
    uint8_t tc = b >> 4;
    uint8_t th = b & 0xf;

    if (tc != 0)
      return false;

    if (th > 3)
      return false;

    uint32_t acc = 0;
    for (int i = 0; i < 16; i++)
    {
      bits[th][i + 1] = buffer.get_u8();
      acc += bits[th][i + 1];
    }
    bits[th][0] = 0;

    if (acc > 256)
      return false;

    if (length < 1 + 16 + acc)
      return false;
	for (uint32_t i = 0; i < acc; i++)
		huffval[th][i] = buffer.get_u8();

    init[th] = true;
    length -= 1 + 16 + acc;
  }
  return true;
}

static
#ifdef _MSC_VER
__forceinline
#else
inline
#endif
void copy_yuv_422(uint16_t *out, uint32_t row, uint32_t col, uint32_t width, int32_t y1, int32_t y2,
	 int32_t cb, int32_t cr)
{
  uint32_t pix1 = row * width + col;
  uint32_t pix2 = pix1 + 3;
  out[pix1 + 0] = uint16_t(y1);
  out[pix1 + 1] = uint16_t(cb);
  out[pix1 + 2] = uint16_t(cr);
  out[pix2 + 0] = uint16_t(y2);
  out[pix2 + 1] = uint16_t(cb);
  out[pix2 + 2] = uint16_t(cr);
}


bool LibRaw_LjpegDecompressor::decode_ljpeg_422(std::vector<uint16_t> &_dest, int width, int height)
{
  if (sof.width * 3u != unsigned(width) || sof.height != unsigned(height))
    return false;
  if (width % 2 || width % 6 || height % 2)
    return false;

  if (_dest.size() < size_t(width * height))
    return false;

  if(width < 1 || height < 1)
    return false;

  uint16_t *dest = _dest.data();

  HuffTable &h1 = dhts[sof.components[0].dc_tbl];
  HuffTable &h2 = dhts[sof.components[1].dc_tbl];
  HuffTable &h3 = dhts[sof.components[2].dc_tbl];

  if (!h1.initialized || !h2.initialized || !h3.initialized)
    return false;

  BitPumpJpeg pump(buffer);

  int32_t base = 1 << (sof.precision - point_transform - 1);
  int32_t y1 = base + h1.decode(pump);
  int32_t y2 = y1 + h1.decode(pump);
  int32_t cb = base + h2.decode(pump);
  int32_t cr = base + h3.decode(pump);
  copy_yuv_422(dest, 0, 0, width, y1, y2, cb, cr);

  for (uint32_t row = 0; row < uint32_t(height); row++)
  {
	  uint32_t startcol = row == 0 ? 6 : 0;
	  for (uint32_t col = startcol; col < uint32_t(width); col += 6)
	  {
        uint32_t pos = (col == 0) ? (row - 1) * width : row * width + col - 3;
        int32_t py = dest[pos],
			pcb = dest[pos + 1],
			pcr = dest[pos + 2];
        int32_t _y1 = py + h1.decode(pump);
        int32_t _y2 = _y1 + h1.decode(pump);
        int32_t _cb = pcb + h2.decode(pump);
        int32_t _cr = pcr + h3.decode(pump);
        copy_yuv_422(dest, row, col, width, _y1, _y2, _cb, _cr);
	  }
  }
  return true;
}

bool LibRaw_SOFInfo::parse_sof(ByteStreamBE& input)
{
	uint32_t header_length = input.get_u16();
	precision = input.get_u8();
    height = input.get_u16();
	width = input.get_u16();
	cps = input.get_u8();

	if (precision > 16)
		return false;
	if (cps > 4 || cps < 1)
		return false;
	if (header_length != 8 + cps * 3)
		return false;

	components.clear();
	for (unsigned i = 0; i < cps; i++)
	{
      unsigned id = input.get_u8();
	  unsigned subs = input.get_u8();
	  components.push_back(LibRaw_JpegComponentInfo(id, i, 0, (subs >> 4), (subs & 0xf) ));
      input.get_u8(); 
	}
	return true;
}

uint32_t LibRaw_SOFInfo::parse_sos(ByteStreamBE& input)
{
  if (width == 0)
    return 0x10000;

  input.get_u16();

  uint32_t soscps = input.get_u8();
  if (cps != soscps)
	  return 0x10000;
  for (uint32_t csi = 0; csi < cps; csi++)
  {
    uint32_t readcs = input.get_u8();
	uint32_t cs = csfix ? csi : readcs; // csfix might be used in MOS decoder
	int cid = -1;
	for(unsigned c = 0; c < components.size(); c++)
		if (components[c].id == cs)
		{
			cid = c;
			break;
		}
	if (cid < 0)
		return 0x10000;

    uint8_t td = input.get_u8() >> 4;
	if (td > 3)
		return 0x10000;
	components[cid].dc_tbl = td;
  }
  uint8_t pred = input.get_u8();
  input.get_u8();
  uint8_t pt = input.get_u8() & 0xf;
  return (pred << 8 | pt);
}

HuffTable::HuffTable()
{
	ZERO(bits);
	ZERO(huffval);
	ZERO(shiftval);
	dng_bug = false;
	disable_cache = false;
	nbits = 0;
	initialized = false;
}

struct PseudoPump : public BitPump
{
	uint64_t bits;
	int32_t nbits;
	PseudoPump() : bits(0), nbits(0) {}

	void set(uint32_t _bits, uint32_t nb)
	{
		bits = uint64_t(_bits) << 32;
		nbits = nb + 32;
	}
	int32_t valid() { return nbits - 32; }

	uint32_t peek(uint32_t num)
	{
		return uint32_t((bits >> (nbits - num)) & 0xffffffffUL);
	}

    void consume(uint32_t num)
    {
      nbits -= num;
      bits &= (uint64_t(1) << nbits) - 1UL;
    }
};

void HuffTable::initval(uint32_t _bits[17], uint32_t _huffval[256], bool _dng_bug)
{
	memmove(bits, _bits, sizeof(bits));
	memmove(huffval, _huffval, sizeof(huffval));
	dng_bug = _dng_bug;

	nbits = 16;
    for(int i = 0; i < 16; i++)
      {
        if(bits[16 - i] != 0)
			break;
		nbits--;
      }
	hufftable.resize( size_t(1ULL << nbits));
	for (unsigned i = 0; i < hufftable.size(); i++) hufftable[i] = 0;

	int h = 0;
    int pos = 0;
    for (uint8_t len = 0; len < nbits; len++)
    {
      for (uint32_t i = 0; i < bits[len + 1]; i++)
      {
        for (int j = 0; j < (1 << (nbits - len - 1)); j++)
        {
          hufftable[h] = ((len+1) << 16) | (uint8_t(huffval[pos] & 0xff) << 8) | uint8_t(shiftval[pos] & 0xff);
          h++;
        }
        pos++;
      }
    }
	if (!disable_cache)
	{
		PseudoPump pump;
		decodecache = std::vector<uint64_t>(1 << LIBRAW_DECODE_CACHE_BITS,0);
		for(uint32_t i = 0; i < 1 << LIBRAW_DECODE_CACHE_BITS; i++)
		{
          pump.set(i, LIBRAW_DECODE_CACHE_BITS);
		  uint32_t len;
		  int16_t val16 = int16_t(decode_slow2(pump,len));
		  if (pump.valid() >= 0)
			  decodecache[i] = LIBRAW_CACHE_PRESENT_FLAG | uint64_t(((len & 0xff) << 16) | uint16_t(val16));
		}
	}
    initialized = true;
}
