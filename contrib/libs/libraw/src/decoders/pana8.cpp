/* -*- C++ -*-
 * File: pana8.cpp
 * Copyright (C) 2022 Alex Tutubalin, LibRaw LLC
 *
   Panasonic RawFormat=8 file decoder

LibRaw is free software; you can redistribute it and/or modify
it under the terms of the one of two licenses as you choose:

1. GNU LESSER GENERAL PUBLIC LICENSE version 2.1
   (See file LICENSE.LGPL provided in LibRaw distribution archive for details).

2. COMMON DEVELOPMENT AND DISTRIBUTION LICENSE (CDDL) Version 1.0
   (See file LICENSE.CDDL provided in LibRaw distribution archive for details).

 */

#include "../../internal/libraw_cxx_defs.h"
#include <vector>

 // in 8-byte words, 800kb
#define PANA8_BUFSIZE 102400

class pana8_bufio_t
{
public:
  pana8_bufio_t(LibRaw_abstract_datastream *stream, INT64 start, uint32_t len)
      : data(PANA8_BUFSIZE), input(stream), baseoffset(start), begin(0), end(0), _size(len)
  {
  }
  uint32_t size() { return ((_size+7)/8)*8; }
  uint64_t getQWord(uint32_t offset)
  {
    if (offset >= begin && offset < end)
      return data[offset - begin];
	if (!input) return 0;
    refill(offset);
    if (offset >= begin && offset < end)
      return data[offset - begin];
    return 0;
  }
  void refill(uint32_t newoffset);

  std::vector<uint64_t> data;
  LibRaw_abstract_datastream *input;
  INT64 baseoffset;
  INT64 begin, end;
  uint32_t _size;
};

struct pana8_param_t
{
	uint32_t range_shift, gamma_base;
	uint32_t tag3A[6];
	uint32_t tag39[6];
	uint32_t tag3B;
	uint32_t initial[4];
	uint32_t huff_coeff[17];
	uint32_t tag3B_2;
	uint32_t noGammaFlag;
	uint64_t hufftable1[17];
    uint64_t hufftable2[17];
    std::vector<uint16_t> gammaTable;
    std::vector<uint8_t> extrahuff;
	pana8_param_t(const pana8_tags_t& init);
	int32_t gammaCurve(uint32_t i);
	bool DecodeC8(
		pana8_bufio_t& bufio,
		unsigned int width, unsigned int height,
		LibRaw *libraw, uint16_t left_margin);
	uint32_t GetDBit(uint64_t a2);
};

void invertBits(void *buf, size_t size);

void pana8_bufio_t::refill(uint32_t newoffset)
{
	if (newoffset >= begin && newoffset < end)
		return; 
	uint32_t readwords, remainwords,toread;
#ifdef LIBRAW_USE_OPENMP
#pragma omp critical
    {
#endif
      input->lock();
      input->seek(baseoffset + newoffset*sizeof(int64_t), SEEK_SET);
      remainwords = (_size - newoffset*sizeof(int64_t) + 7) >> 3;
      toread = MIN(PANA8_BUFSIZE, remainwords);
      uint32_t readbytes = input->read(data.data(), 1, toread*sizeof(uint64_t));
	  readwords = (readbytes + 7) >> 3;
      input->unlock();
#ifdef LIBRAW_USE_OPENMP
    }
#endif

  if (INT64(readwords) < INT64(toread) - 1LL)
    throw LIBRAW_EXCEPTION_IO_EOF;

  if(readwords>0)
      invertBits(data.data(), readwords * sizeof(uint64_t));
  begin = newoffset;
  end = newoffset + readwords;
}

void LibRaw::panasonicC8_load_raw()
{
	int errs = 0;
	unsigned totalw = 0;
	INT64 fsz = libraw_internal_data.internal_data.input->size();
	if (libraw_internal_data.unpacker_data.pana8.stripe_count > 5) errs++;
	for (int i = 0; i < libraw_internal_data.unpacker_data.pana8.stripe_count && i < 5; i++)
	{
		if (libraw_internal_data.unpacker_data.pana8.stripe_height[i] != imgdata.sizes.raw_height)
			errs++;
		if (libraw_internal_data.unpacker_data.pana8.stripe_offsets[i] < 0
			|| (libraw_internal_data.unpacker_data.pana8.stripe_offsets[i] + INT64((libraw_internal_data.unpacker_data.pana8.stripe_compressed_size[i] + 7u) / 8u)) > fsz)
			errs++;
		totalw += libraw_internal_data.unpacker_data.pana8.stripe_width[i];
	}
	if (totalw != imgdata.sizes.raw_width) errs++;
	if (errs)
		throw LIBRAW_EXCEPTION_IO_CORRUPT;

	pana8_param_t pana8_param(libraw_internal_data.unpacker_data.pana8);
	pana8_decode_loop(&pana8_param);
}

void LibRaw::pana8_decode_loop(void *data)
{
#ifdef LIBRAW_USE_OPENMP
	int errs = 0, scount = MIN(5,libraw_internal_data.unpacker_data.pana8.stripe_count);
#pragma omp parallel for 
  for (int stream = 0; stream < scount; stream++)
  {
		if (pana8_decode_strip(data, stream))
			errs++;
  }
	if(errs)
      throw LIBRAW_EXCEPTION_IO_CORRUPT;
#else
  for (int stream = 0; stream < libraw_internal_data.unpacker_data.pana8.stripe_count && stream < 5; stream++)
    if (pana8_decode_strip(data, stream))
      throw LIBRAW_EXCEPTION_IO_CORRUPT;
#endif
}

int LibRaw::pana8_decode_strip(void* data, int stream)
{
	pana8_param_t *pana8_param = (pana8_param_t*)data;
	if (!data || stream < 0 || stream > 4 || stream > libraw_internal_data.unpacker_data.pana8.stripe_count) return 1; // error

	unsigned exactbytes = (libraw_internal_data.unpacker_data.pana8.stripe_compressed_size[stream] + 7u) / 8u;
    pana8_bufio_t bufio(libraw_internal_data.internal_data.input,
                        libraw_internal_data.unpacker_data.pana8.stripe_offsets[stream], exactbytes);
    return !pana8_param->DecodeC8(bufio, libraw_internal_data.unpacker_data.pana8.stripe_width[stream],
                                  libraw_internal_data.unpacker_data.pana8.stripe_height[stream], this,
                                  libraw_internal_data.unpacker_data.pana8.stripe_left[stream]);
}

struct pana8_base_t
{
  pana8_base_t() { coeff[0] = coeff[1] = coeff[2] = coeff[3] = 0; }
  pana8_base_t(const pana8_base_t &s) { clone(s.coeff); }
  void clone(const uint32_t *scoeff)
  { // TODO: implement SSE load for SSE-enabled code
    coeff[0] = scoeff[0];
    coeff[1] = scoeff[1];
    coeff[2] = scoeff[2];
    coeff[3] = scoeff[3];
  }
  uint32_t coeff[4];
};

bool pana8_param_t::DecodeC8(pana8_bufio_t &bufio, unsigned int width, unsigned int height, LibRaw *libraw,
                             uint16_t left_margin)
{
  unsigned halfwidth = width >> 1;
  unsigned halfheight = height >> 1;
  if (!halfwidth || !halfheight || bufio.size() < 9)
    return false; // invalid input

  uint32_t datamax = tag3B_2 >> range_shift;

  pana8_base_t start_coeff;
  for(int i = 0; i < 4; i++)
	start_coeff.coeff[i] = initial[i] & 0xffffu;

  bool _extrahuff = (extrahuff.size() >= 0x10000); 

  uint8_t *big_huff_table = 0;
  if (_extrahuff)
    big_huff_table = extrahuff.data();

  uint16_t *gammatable = (gammaTable.size() >= 0x10000) && !noGammaFlag ? (uint16_t *)gammaTable.data() : 0;

#ifdef PANA8_FULLY_BUFFERED
  const uint8_t	*inputbyteptr = source.data();
  uint32_t	jobsz_in_qwords = source.size() >> 3;
#else
  uint32_t jobsz_in_qwords = bufio.size() >> 3;
#endif
  int32_t	doublewidth = 4 * halfwidth;
  std::vector<uint8_t> outline(4 * doublewidth);
  pana8_base_t line_base(start_coeff);
  int64_t	bittail = 0LL;
  int32_t	bitportion = 0;
  uint32_t inqword = 0u;

  try
  {
	  for (uint32_t current_row = 0; current_row < halfheight; current_row++)
	  {
		  uint8_t *outrowp = outline.data();
		  pana8_base_t current_base(line_base);

		  for (int32_t col = 0; col < doublewidth; col++)
		  {
			  uint64_t pixbits;
			  if (bitportion < 0)
			  {
				  uint32_t inqword_next = inqword + 1;
				  if ((int)inqword + 1 >= int(jobsz_in_qwords))
					  return false;
				  bitportion += 64;
				  uint64_t inputqword = bufio.getQWord(inqword);
				  uint64_t inputqword_next = bufio.getQWord(inqword_next);
				  pixbits = (inputqword_next >> bitportion) | (inputqword << (64 - (uint8_t)(bitportion & 0xffu)));
				  if ((unsigned int)inqword < jobsz_in_qwords)
					  inqword = inqword_next;
			  }
			  else
			  {
				  if ((unsigned int)inqword >= jobsz_in_qwords)
					  return false;
                  uint64_t inputqword = bufio.getQWord(inqword);
				  pixbits = (inputqword >> bitportion) | bittail;
				  uint32_t step = (bitportion == 0);
				  if (!bitportion)
					  bitportion = 64;
				  inqword += step;
			  }
			  int huff_index = 0;
			  if (_extrahuff)
				  huff_index = *(uint8_t *)(big_huff_table + ((pixbits >> 48) & 0xffffu));
			  else
			  {
				  huff_index = int(GetDBit(pixbits));
				  datamax = tag3B_2;
			  }
			  int32_t v37 = (huff_coeff[huff_index] >> 24) & 0x1F;
			  uint32_t hc = huff_coeff[huff_index];
			  int64_t v38 = pixbits << (((hc >> 16) & 0xffffu) & 0x1F);
			  uint64_t v90 = (uint32_t)(huff_index - v37);
			  int32_t v39 = (uint16_t)((uint64_t)v38 >> ((uint8_t)v37 - (uint8_t)huff_index)) << ((huff_coeff[huff_index] >> 24) & 0xffu);

			  if (huff_index - v37 <= 0)
				  v39 &= 0xffff0000u;

			  int32_t delta1;
			  if (v38 < 0)
				  delta1 = (uint16_t)v39;
			  else if (huff_index)
			  {
				  int32_t v40 = -1 << huff_index;
				  if ((uint8_t)v37)
					  delta1 = (uint16_t)v39 + v40;
				  else
					  delta1 = (uint16_t)v39 + v40 + 1;
			  }
			  else
				  delta1 = 0;

			  uint32_t v42 = bitportion - ((huff_coeff[huff_index] >> 16) & 0x1F);
			  int32_t delta2 = uint8_t(v37) ? 1 << (v37 - 1) : 0;
			  uint32_t *destpixel = (uint32_t *)(outrowp + 16LL * (col >> 2));

			  int32_t delta = delta1 + delta2;
			  int32_t col_amp_3 = col & 3;
#define _LIM(a, _max_) (a < 0) ? 0 : ((a > _max_) ? _max_ : a)
			  if (col_amp_3 == 2)
			  {
				  int32_t val = current_base.coeff[1] + delta;
				  destpixel[1] = uint32_t(_LIM(val, int(datamax)));
			  }
			  else if (col_amp_3 == 1)
			  {
				  int32_t val = current_base.coeff[2] + delta;
				  destpixel[2] = uint32_t(_LIM(val, int(datamax)));
			  }
			  else if ((col & 3) != 0) // == 3
			  {
				  int32_t val = current_base.coeff[3] + delta;
				  destpixel[3] = uint32_t(_LIM(val, int(datamax)));
			  }
			  else // 0
			  {
				  int32_t val = current_base.coeff[0] + delta;
				  destpixel[0] = uint32_t(_LIM(val, int(datamax)));
			  }
#undef _LIM
			  if (huff_index <= v37)
				  v90 = 0LL;
			  bittail = v38 << v90;
			  bitportion = int32_t(v42 - v90);

			  if (col_amp_3 == 3)
				  current_base.clone((uint32_t *)(outrowp + 16LL * (col >> 2)));
			  if (col == 3)
				  line_base.clone((uint32_t *)(outrowp));
		  }

		  int destrow = current_row * 2;
		  uint16_t *destrow0 = libraw->imgdata.rawdata.raw_image + (destrow * libraw->imgdata.sizes.raw_width) + left_margin;
		  uint16_t *destrow1 =
			  libraw->imgdata.rawdata.raw_image + (destrow + 1) * libraw->imgdata.sizes.raw_width + left_margin;
		  uint16_t *srcrow = (uint16_t *)(outrowp);
		  if (gammatable)
		  {
			  for (unsigned col = 0; col < width - 1; col += 2)
			  {
				  const int c6 = col * 4;
				  destrow0[col] = gammatable[srcrow[c6]];
				  destrow0[col + 1] = gammatable[srcrow[c6 + 2]];
				  destrow1[col] = gammatable[srcrow[c6 + 4]];
				  destrow1[col + 1] = gammatable[srcrow[c6 + 6]];
			  }
		  }
		  else
		  {
			  for (unsigned col = 0; col < width - 1; col += 2)
			  {
				  const int c6 = col * 4;
				  destrow0[col] = srcrow[c6];
				  destrow0[col + 1] = srcrow[c6 + 2];
				  destrow1[col] = srcrow[c6 + 4];
				  destrow1[col + 1] = srcrow[c6 + 6];
			  }
		  }
	  }
  }
  catch (...) // buffer read may throw an exception
  {
	  return false;
  }
  return true;
}


uint32_t pana8_param_t::GetDBit(uint64_t a2)
{
	for (int i = 0; i < 16; i++)
	{
		if ((a2 & hufftable2[i]) == hufftable1[i])
			return i;
	}
	return uint32_t((hufftable2[16] & a2) == hufftable1[16]) ^ 0x11u;
}

pana8_param_t::pana8_param_t(const pana8_tags_t &meta) : gammaTable(0)
{
  range_shift = gamma_base = tag3B = 0;
#ifndef ZERO
#define ZERO(a) memset(a, 0, sizeof(a))
#endif
  ZERO(tag3A);
  ZERO(tag39);
  ZERO(tag3A);
  ZERO(initial);
  ZERO(huff_coeff);
  ZERO(hufftable1);
  ZERO(hufftable2);
#undef ZERO
  noGammaFlag = 1;

  for (int i = 0; i < 6; i++)
  {
    tag3A[i] = meta.tag3A[i];
    tag39[i] = meta.tag39[i];
  }
  tag3B_2 = tag3B = meta.tag3B;
  for(int i = 0; i < 4; i++)
	initial[i] = meta.initial[i];

  for (int i = 0; i < 17; i++)
    huff_coeff[i] = (uint32_t(meta.tag41[i]) << 24) | (uint32_t(meta.tag40a[i]) << 16) | meta.tag40b[i];

  std::vector<uint16_t> tempGamma(0x10000);
  for (unsigned i = 0; i < 0x10000; i++)
  {
    uint64_t val = gammaCurve(i);
    tempGamma[i] = uint16_t(val & 0xffffu);
    if (i != val)
      noGammaFlag = 0;
  }
  if (!noGammaFlag)
	  gammaTable = tempGamma;

  int v7 = 0;

  for (unsigned hindex = 0; hindex < 17; hindex++)
  {
    uint32_t hc = huff_coeff[hindex];
    uint32_t hlow = (hc >> 16) & 0x1F;
    int16_t v8 = 0;
    if ((hc & 0x1F0000) != 0)
    {
      int h7 = ((hc >> 16) & 0xffffu) & 7;
      if (hlow - 1 >= 7)
      {
        uint32_t hdiff = h7 - hlow;
        v8 = 0;
        do
        {
          v8 = (v8 << 8) | 0xFFu;
          hdiff += 8;
        } while (hdiff);
      }
      else
        v8 = 0;
      for (; h7; --h7)
        v8 = 2 * v8 + 1;
    }

    uint16_t v9 = hc & v8;
    if (uint32_t(v7) < hlow)
      v7 = ((huff_coeff[hindex] >> 16) & 0xFFFFu) & 0x1F;
    hufftable2[hindex] = 0xFFFFULL << (64-hlow);
    hufftable1[hindex] = (uint64_t)v9 << (64-hlow);
  }

  if (v7 < 17)
  {
    if (extrahuff.size() < 0x10000)
      extrahuff.resize(0x10000);
    uint64_t v17 = 0LL;

	for (int j = 0LL; j != 0x10000; ++j)
	{
		extrahuff[j] = uint8_t(GetDBit(v17) & 0xffu);
		v17 += 0x1000000000000ULL;
	}
  }
}

int32_t pana8_param_t::gammaCurve(uint32_t idx)
{
  unsigned int v2 = idx | 0xFFFF0000;
  if ((idx & 0x10000) == 0)
    v2 = idx & 0x1FFFF;

  int v3 = gamma_base + v2;
  unsigned int v4 = MIN(v3, 0xFFFF);

  int v5 = 0;
  if ((v4 & 0x80000000) != 0)
    v4 = 0;

  if (v4 >= (0xFFFF & tag3A[1]))
  {
    v5 = 1;
    if (v4 >= (0xFFFF & tag3A[2]))
    {
      v5 = 2;
      if (v4 >= (0xFFFF & tag3A[3]))
      {
        v5 = 3;
        if (v4 >= (0xFFFF & tag3A[4]))
          v5 = ((v4 | 0x500000000LL) - (uint64_t)(0xFFFF & tag3A[5])) >> 32;
      }
    }
  }
  unsigned int v6 = tag3A[v5];
  int v7 = tag39[v5];
  unsigned int v8 = v4 - (uint16_t)v6;
  char v9 = v7 & 0x1F;
  int64_t result = 0;

  if (v9 == 31)
  {
    result = v5 == 5 ? 0xFFFFLL : ((tag3A[v5 + 1] >> 16) & 0xFFFF);
    return MIN( uint32_t(result), tag3B);
  }
  if ((v7 & 0x10) == 0)
  {
    if (v9 == 15)
    {
      result = ((v6 >> 16) & 0xFFFF);
      return MIN( uint32_t(result), tag3B);
    }
	else if(v9!=0)
		v8 = (v8 + (1 << (v9 - 1))) >> v9;
  }
  else
	v8 <<= v7 & 0xF;

  result = v8 + ((v6 >> 16) & 0xFFFF);
  return MIN( uint32_t(result), tag3B);
}

const static uint8_t _bitRevTable[256] = {
    0x00, 0x80, 0x40, 0xC0, 0x20, 0xA0, 0x60, 0xE0, 0x10, 0x90, 0x50, 0xD0, 0x30, 0xB0, 0x70, 0xF0, 0x08, 0x88, 0x48,
    0xC8, 0x28, 0xA8, 0x68, 0xE8, 0x18, 0x98, 0x58, 0xD8, 0x38, 0xB8, 0x78, 0xF8, 0x04, 0x84, 0x44, 0xC4, 0x24, 0xA4,
    0x64, 0xE4, 0x14, 0x94, 0x54, 0xD4, 0x34, 0xB4, 0x74, 0xF4, 0x0C, 0x8C, 0x4C, 0xCC, 0x2C, 0xAC, 0x6C, 0xEC, 0x1C,
    0x9C, 0x5C, 0xDC, 0x3C, 0xBC, 0x7C, 0xFC, 0x02, 0x82, 0x42, 0xC2, 0x22, 0xA2, 0x62, 0xE2, 0x12, 0x92, 0x52, 0xD2,
    0x32, 0xB2, 0x72, 0xF2, 0x0A, 0x8A, 0x4A, 0xCA, 0x2A, 0xAA, 0x6A, 0xEA, 0x1A, 0x9A, 0x5A, 0xDA, 0x3A, 0xBA, 0x7A,
    0xFA, 0x06, 0x86, 0x46, 0xC6, 0x26, 0xA6, 0x66, 0xE6, 0x16, 0x96, 0x56, 0xD6, 0x36, 0xB6, 0x76, 0xF6, 0x0E, 0x8E,
    0x4E, 0xCE, 0x2E, 0xAE, 0x6E, 0xEE, 0x1E, 0x9E, 0x5E, 0xDE, 0x3E, 0xBE, 0x7E, 0xFE, 0x01, 0x81, 0x41, 0xC1, 0x21,
    0xA1, 0x61, 0xE1, 0x11, 0x91, 0x51, 0xD1, 0x31, 0xB1, 0x71, 0xF1, 0x09, 0x89, 0x49, 0xC9, 0x29, 0xA9, 0x69, 0xE9,
    0x19, 0x99, 0x59, 0xD9, 0x39, 0xB9, 0x79, 0xF9, 0x05, 0x85, 0x45, 0xC5, 0x25, 0xA5, 0x65, 0xE5, 0x15, 0x95, 0x55,
    0xD5, 0x35, 0xB5, 0x75, 0xF5, 0x0D, 0x8D, 0x4D, 0xCD, 0x2D, 0xAD, 0x6D, 0xED, 0x1D, 0x9D, 0x5D, 0xDD, 0x3D, 0xBD,
    0x7D, 0xFD, 0x03, 0x83, 0x43, 0xC3, 0x23, 0xA3, 0x63, 0xE3, 0x13, 0x93, 0x53, 0xD3, 0x33, 0xB3, 0x73, 0xF3, 0x0B,
    0x8B, 0x4B, 0xCB, 0x2B, 0xAB, 0x6B, 0xEB, 0x1B, 0x9B, 0x5B, 0xDB, 0x3B, 0xBB, 0x7B, 0xFB, 0x07, 0x87, 0x47, 0xC7,
    0x27, 0xA7, 0x67, 0xE7, 0x17, 0x97, 0x57, 0xD7, 0x37, 0xB7, 0x77, 0xF7, 0x0F, 0x8F, 0x4F, 0xCF, 0x2F, 0xAF, 0x6F,
    0xEF, 0x1F, 0x9F, 0x5F, 0xDF, 0x3F, 0xBF, 0x7F, 0xFF};

void invertBits(void *buf, size_t size)
{
  unsigned sz = unsigned(size / 8);
  uint64_t *ptr = (uint64_t *)buf;
  for (unsigned i = 0; i < sz; i++)
  {
    uint8_t *b = (uint8_t *)&ptr[i];
    uint64_t r = ((uint64_t)_bitRevTable[b[0]] << 56) | ((uint64_t)_bitRevTable[b[1]] << 48) |
                 ((uint64_t)_bitRevTable[b[2]] << 40) | ((uint64_t)_bitRevTable[b[3]] << 32) |
                 ((uint64_t)_bitRevTable[b[4]] << 24) | ((uint64_t)_bitRevTable[b[5]] << 16) |
                 ((uint64_t)_bitRevTable[b[6]] << 8) | _bitRevTable[b[7]];
    ptr[i] = r;
  }
}
