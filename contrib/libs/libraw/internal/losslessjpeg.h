/* -*- C++ -*-
 * File: huffmandec.h
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
#pragma  once
#include <stdint.h>
#include <vector>

struct BitPump // generic bit source
{
  virtual uint32_t peek(uint32_t num) = 0;
  virtual void consume(uint32_t num) = 0;

  uint32_t get(uint32_t num)
  {
    if(num == 0) { return 0u; }
    uint32_t val = peek(num);
    consume(num);
	return val;
  }
};

struct ByteStreamBE // Jpeg is always big endian
{
  enum Exceptions
  {
    OK = 0,
    EndOfBuffer = 1
  };

  uint8_t *buffer;
  unsigned size, pos;
  ByteStreamBE(uint8_t *b, unsigned s) : buffer(b), size(s), pos(0) {}
  ByteStreamBE() : buffer(0),size(0),pos(0){}
  bool skip_to_marker();

  uint8_t get_u8()
  {
    if (pos >= size)
      throw EndOfBuffer;
    uint8_t ret = buffer[pos];
    pos++;
    return ret;
  }
  uint16_t get_u16()
  {
    if (pos + 2 > size)
      throw EndOfBuffer;
    uint8_t r1 = buffer[pos];
    uint8_t r2 = buffer[pos + 1];
    pos += 2;
    return (r1 << 8) | r2;
  }
};

struct BitPumpJpeg : public BitPump
{
	uint8_t *buffer;
	unsigned size, pos;
	uint64_t bits;
	uint32_t nbits;
	bool finished;

	void consume(uint32_t num)
	{
		if (num <= nbits)
		{
			nbits -= num;
			bits &= (uint64_t(1) << nbits) - 1UL;
		}
	}
    uint32_t peek(uint32_t num)
    {
		if (num > nbits && !finished)
		{
			if ((size >= 4) && pos < size && buffer[pos] != 0xff && buffer[pos + 1] != 0xff && buffer[pos + 2] != 0xff &&
				buffer[pos + 3] != 0xff)
			{
				uint64_t inbits = (uint32_t(buffer[pos]) << 24) | (uint32_t(buffer[pos + 1]) << 16) |
					(uint32_t(buffer[pos + 2]) << 8) | (uint32_t(buffer[pos + 3]));
				bits = (bits << 32) | inbits;
				pos += 4;
				nbits += 32;
			}
			else
			{
				int read_bytes = 0;
				while (read_bytes < 4 && !finished)
				{
					uint8_t byte = 0;
					if (pos >= size)
						finished = true;
					else
					{
						uint8_t nextbyte = buffer[pos];
						if (nextbyte != 0xff)
							byte = nextbyte;
						else if (buffer[pos + 1] == 0x00)
						{
							pos += 1;
							byte = nextbyte;
						}
						else
							finished = true;
					};
					bits = (bits << 8) | uint64_t(byte);
					pos += 1;
					nbits += 8;
					read_bytes += 1;
				}
			}
		}
	  if(num > nbits && finished)
        {
          bits <<= 32;
          nbits += 32;
        }
	  return uint32_t(bits >> (nbits - num));
    }

	BitPumpJpeg(ByteStreamBE& s): buffer(s.buffer+s.pos),size(s.size-s.pos),pos(0),bits(0),nbits(0),finished(false){}
};


const uint32_t LIBRAW_DECODE_CACHE_BITS = 13;
const uint64_t LIBRAW_CACHE_PRESENT_FLAG = 0x100000000L;

struct HuffTable
{
	uint32_t bits[17];
	uint32_t huffval[256];
	uint32_t shiftval[256];
	bool dng_bug;
	bool disable_cache;
	uint32_t nbits;
    std::vector<uint32_t> hufftable; // len:8 << 16| huffval:8  << 8 | shift:8
	std::vector<uint64_t> decodecache;
	bool initialized;
	HuffTable();
	void initval(uint32_t bits[17], uint32_t huffval[256], bool dng_bug);

	int32_t decode(BitPump& pump)
    {
      uint64_t cached = disable_cache ? 0 : decodecache[pump.peek(LIBRAW_DECODE_CACHE_BITS)];
      if (cached & LIBRAW_CACHE_PRESENT_FLAG)
      {
        uint32_t _bits = (cached >> 16) & 0xff;
        int16_t val = int16_t(cached & 0xffff);
        if (val == -32768 && dng_bug)
        {
          if (_bits > 16)
            pump.consume(_bits - 16);
        }
        else
          pump.consume(_bits);
        return val;
      }
      else
        return decode_slow1(pump);
    }

	int32_t decode_slow1(BitPump &pump)
    {
      int32_t _diff = diff(pump, len(pump));
      return _diff;
    }

    int32_t decode_slow2(BitPump & pump, uint32_t& outlen) // output:  (len+shift):8, code:16
    {
      uint32_t _len = len(pump);
      int32_t _diff = diff(pump, _len);
      uint8_t bits8 = (_len >> 16) & 0xff;
      uint8_t len8 = (_len >> 8) & 0xff;
      outlen = bits8 + len8;
      return _diff;
    }

    uint32_t len(BitPump & pump) //bits:8, len:8, shift:8
    {
      uint32_t code = pump.peek(nbits);
      uint32_t huffdata = hufftable[code];
      uint32_t _bits = (huffdata >> 16) & 0xff;
      pump.consume(_bits);
      return huffdata;
    }

    int32_t diff(BitPump & pump, uint32_t hentry) // input: bits:8, len:8, shift:8; output:  diff:i32
    {
      uint32_t len = (hentry >> 8) & 0xff;
      if (len == 0)
        return 0;
      if (len == 16)
      {
        if (dng_bug)
          pump.get(16);
        return -32768;
      }
      uint32_t shift = hentry & 0xff;
	  uint32_t fulllen = len + shift;
      uint32_t _bits = pump.get(len);
      int32_t diff = ((_bits << 1) + 1) << shift >> 1;
      if ((diff & (1 << (fulllen - 1))) == 0)
      {
        diff -= int32_t((1 << fulllen) - ((shift == 0)));
      }
      return diff;
    }
};

struct LibRaw_JpegComponentInfo
{
	unsigned id;
	unsigned index;
	unsigned dc_tbl;
	unsigned subsample_h, subsample_v;
	LibRaw_JpegComponentInfo() : id(0), index(0), dc_tbl(0), subsample_h(0), subsample_v(0) {};
	LibRaw_JpegComponentInfo(unsigned _id, unsigned _index, unsigned _dcn, unsigned _sh, unsigned _sv)
		: id(_id), index(_index), dc_tbl(_dcn), subsample_h(_sh), subsample_v(_sv){};
	LibRaw_JpegComponentInfo(const LibRaw_JpegComponentInfo& q): id(q.id), index(q.index), dc_tbl(q.dc_tbl),
			subsample_h(q.subsample_h),subsample_v(q.subsample_v){}
};

struct LibRaw_SOFInfo
{
	unsigned width, height;
	unsigned cps, precision;
	std::vector<LibRaw_JpegComponentInfo> components;
	bool csfix;
	LibRaw_SOFInfo(): width(0),height(0),cps(0),precision(0),csfix(false){}
	bool parse_sof(ByteStreamBE&); // true => OK, false => not OK
	uint32_t parse_sos(ByteStreamBE&); // returns (predictor << 8 | point transform); if above 0xffff => error
};

struct LibRaw_LjpegDecompressor 
{
	ByteStreamBE buffer;
	LibRaw_SOFInfo sof;
	uint32_t predictor, point_transform;
	uint32_t datastart;
	std::vector<HuffTable> dhts;
	LibRaw_LjpegDecompressor(uint8_t *b, unsigned s);
	LibRaw_LjpegDecompressor(uint8_t *b, unsigned bs, bool dngbug, bool csfix);
	void  initialize(bool dngbug, bool csfix);
    uint8_t next_marker(bool allowskip);
	bool  parse_dht(bool init[4], uint32_t dht_bits[4][17], uint32_t dht_huffval[4][256]); // return true on OK;
	bool decode_ljpeg_422(std::vector<uint16_t> &dest, int width, int height);

	struct State {
      enum States
      {
        OK = 0,
		NotInited = 1,
		NoSOI = 2,
		IncorrectPrecision = 3,
		EOIReached = 4,
		InvalidDHT = 5,
		InvalidSOF = 6,
        InvalidSOS = 7,
		DQTPresent = 8
      };

	};
	struct Marker {
		enum Markers {
			Stuff = 0x00,
			SOF3 = 0xc3, // lossless
			DHT = 0xc4,  // huffman tables
			SOI = 0xd8,  // start of image
			EOI = 0xd9,  // end of image
			SOS = 0xda,  // start of scan
			DQT = 0xdb,  // quantization tables
			Fill = 0xff,
		};
	};
	enum State::States state;
};

