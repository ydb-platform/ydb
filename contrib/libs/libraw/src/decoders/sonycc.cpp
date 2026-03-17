/* -*- C++ -*-
 * File: sonycc.cpp
 * Copyright (C) 2024-2025 Alex Tutubalin, LibRaw LLC
 *
   Sony YCC (small/medium lossy compressed) decoder

   Code partially backported from DNGLab's rust code
   DNGLab was originally created in 2021 by Daniel Vogelbacher.

LibRaw is free software; you can redistribute it and/or modify
it under the terms of the one of two licenses as you choose:

1. GNU LESSER GENERAL PUBLIC LICENSE version 2.1
   (See file LICENSE.LGPL provided in LibRaw distribution archive for details).

2. COMMON DEVELOPMENT AND DISTRIBUTION LICENSE (CDDL) Version 1.0
   (See file LICENSE.CDDL provided in LibRaw distribution archive for details).

 */

#include "../../internal/libraw_cxx_defs.h"
#include "../../internal/losslessjpeg.h"
#include <vector>
#include <algorithm>

#define ifp libraw_internal_data.internal_data.input
#define UD libraw_internal_data.unpacker_data
#define S imgdata.sizes

struct LibRaw_SonyYCC_Decompressor : public LibRaw_LjpegDecompressor
{
	LibRaw_SonyYCC_Decompressor(uint8_t *b, unsigned s): LibRaw_LjpegDecompressor(b,s){}
	bool decode_sony(std::vector<uint16_t> &dest, int width, int height);
	bool decode_sony_ljpeg_420(std::vector<uint16_t> &dest, int width, int height);

};

static
#ifdef _MSC_VER
    __forceinline
#else
    inline
#endif
void copy_yuv_420(uint16_t *out, uint32_t row, uint32_t col, uint32_t width,
	int32_t y1, int32_t y2, int32_t y3, int32_t y4, int32_t cb, int32_t cr)
{
  uint32_t pix1 = row * width + col;
  uint32_t pix2 = pix1 + 3;
  uint32_t pix3 = (row + 1) * width + col;
  uint32_t pix4 = pix3 + 3;

  out[pix1 + 0] = uint16_t(y1);
  out[pix1 + 1] = uint16_t(cb);
  out[pix1 + 2] = uint16_t(cr);
  out[pix2 + 0] = uint16_t(y2);
  out[pix2 + 1] = uint16_t(cb);
  out[pix2 + 2] = uint16_t(cr);
  out[pix3 + 0] = uint16_t(y3);
  out[pix3 + 1] = uint16_t(cb);
  out[pix3 + 2] = uint16_t(cr);
  out[pix4 + 0] = uint16_t(y4);
  out[pix4 + 1] = uint16_t(cb);
  out[pix4 + 2] = uint16_t(cr);
}


bool LibRaw_SonyYCC_Decompressor::decode_sony_ljpeg_420(std::vector<uint16_t> &_dest, int width, int height)
{
  if (sof.width * 3 != unsigned(width) || sof.height != unsigned(height))
    return false;
  if (width % 2 || width % 6 || height % 2)
    return false;
  if (_dest.size() < size_t(width*height))
	  return false;

  HuffTable &huff1 = dhts[sof.components[0].dc_tbl];
  HuffTable &huff2 = dhts[sof.components[1].dc_tbl];
  HuffTable &huff3 = dhts[sof.components[2].dc_tbl];

  if (!huff1.initialized || !huff2.initialized || !huff3.initialized)
	  return false;

  BitPumpJpeg pump(buffer);

  int32_t base = 1 << (sof.precision - point_transform - 1);
  int32_t y1 = base + huff1.decode(pump);
  int32_t y2 = y1 + huff1.decode(pump);
  int32_t y3 = y1 + huff1.decode(pump); 
  int32_t y4 = y3 + huff1.decode(pump);

  int32_t cb = base + huff2.decode(pump);
  int32_t cr = base + huff3.decode(pump);

  uint16_t *dest = _dest.data();
  copy_yuv_420(dest, 0, 0, width, y1, y2, y3, y4, cb, cr);

  for (int32_t row = 0; row < height; row += 2)
  {
    int32_t startcol = row == 0 ? 6 : 0;
    for (int32_t col = startcol; col < width; col += 6)
    {
      int32_t py1, py3, pcb, pcr;
      if (col == 0)
      {
        uint32_t pos = (row - 2) * width; 
        py1 = dest[pos];
        py3 = 0;
        pcb = dest[pos + 1];
        pcr = dest[pos + 2];
      }
      else
      {
        uint32_t pos1 = row * width + col - 3;       
        uint32_t pos3 = (row + 1) * width + col - 3; 
        py1 = dest[pos1];
        py3 = dest[pos3];
        pcb = dest[pos1 + 1];
        pcr = dest[pos1 + 2];
      };
      y1 = py1 + huff1.decode(pump);
      y2 = y1 + huff1.decode(pump);
      y3 = ((col == 0) ? y1 : py3) + huff1.decode(pump);
      y4 = y3 + huff1.decode(pump);
      cb = pcb + huff2.decode(pump);
      cr = pcr + huff3.decode(pump);
      copy_yuv_420(dest, row, col, width, y1, y2, y3, y4, cb, cr);
    }
  }
  return true;
}

bool LibRaw_SonyYCC_Decompressor::decode_sony(std::vector<uint16_t> &dest, int width, int height)
{
  if (sof.components[0].subsample_h == 2 && sof.components[0].subsample_v == 2)
  {
    return decode_sony_ljpeg_420(dest, width, height);
  }
  else if (sof.components[0].subsample_h == 2 && sof.components[0].subsample_v == 1)
    return decode_ljpeg_422(dest, width, height);
  else
    return false;
}

static
#ifdef _MSC_VER
    __forceinline
#else
    inline
#endif
void copy_ycc(ushort dst[][4], int rawwidth, int rawheight, int destrow0, int destcol0, ushort *src,
	int srcwidth, // full array width is 3x
	int srcheight, int steph, int stepv)
{
	if (steph < 2 && stepv < 2)
	{
		for (int tilerow = 0; tilerow < srcheight && destrow0 + tilerow < rawheight; tilerow++)
		{
			ushort(*destrow)[4] = &dst[(destrow0 + tilerow) * rawwidth + destcol0];
			for (int tilecol = 0; tilecol < srcwidth && tilecol + destcol0 < rawwidth; tilecol++)
			{
				int pix = (tilerow * srcwidth + tilecol) * 3;
				destrow[tilecol][0] = src[pix];
				destrow[tilecol][1] = src[pix + 1] > 8192 ? src[pix + 1] - 8192 : 0;
				destrow[tilecol][2] = src[pix + 2] > 8192 ? src[pix + 2] - 8192 : 0;
			}
		}
	}
	else
	{
      for (int tilerow = 0; tilerow < srcheight && destrow0 + tilerow < rawheight; tilerow++)
      {
        int destrow = destrow0 + tilerow;
        ushort(*dest)[4] = &dst[destrow * rawwidth + destcol0];
        for (int tilecol = 0; tilecol < srcwidth && tilecol + destcol0 < rawwidth; tilecol++)
        {
          int pix = (tilerow * srcwidth + tilecol) * 3;
		  int destcol = destcol0 + tilecol;
          dest[tilecol][0] = src[pix];
		  if((destrow%stepv) == 0 && (destcol%steph)==0)
		  {
            dest[tilecol][1] = src[pix + 1] > 8192 ? src[pix + 1] - 8192 : 0;
            dest[tilecol][2] = src[pix + 2] > 8192 ? src[pix + 2] - 8192 : 0;
		  }
        }
      }

	}
}

static
#ifdef _MSC_VER
    __forceinline
#else
    inline
#endif
uint16_t _lim16bit(float q)
{
	if (q < 0.f)
		q = 0.f;
	else if (q > 65535.f)
		q = 65535.f;
	return uint16_t(uint32_t(q));
}

static
#ifdef _MSC_VER
    __forceinline
#else
    inline
#endif
void ycc2rgb(uint16_t dst[][4], int rawwidth, int rawheight, int destrow0, int destcol0, ushort *src,
                     int srcwidth, // full array width is 3x
                     int srcheight)
{
  const ushort cdelta = 16383;
  for (int tilerow = 0; tilerow < srcheight && destrow0 + tilerow < rawheight; tilerow++)
  {
    ushort(*destrow)[4] = &dst[(destrow0 + tilerow) * rawwidth + destcol0];
    for (int tilecol = 0; tilecol < srcwidth && tilecol + destcol0 < rawwidth; tilecol++)
    {
      int pix = (tilerow * srcwidth + tilecol) * 3;
      float Y = float(src[pix]);
	  float Cb = float(src[pix + 1] - cdelta);
      float Cr = float(src[pix + 2] - cdelta);
	  float R = Y + 1.40200f * Cr;
	  float G = Y - 0.34414f * Cb - 0.71414f * Cr;
	  float B = Y + 1.77200f * Cb;
	  destrow[tilecol][0] = _lim16bit(R);
	  destrow[tilecol][1] = _lim16bit(G);
	  destrow[tilecol][2] = _lim16bit(B);
    }
  }
}

void LibRaw::sony_ycbcr_load_raw()
{
  if (!imgdata.image)
    throw LIBRAW_EXCEPTION_IO_CORRUPT;
  // Sony YCC RAWs are always tiled
  if (UD.tile_width < 1 || UD.tile_width > S.raw_width)
    throw LIBRAW_EXCEPTION_IO_CORRUPT;
  if (UD.tile_length < 1 || UD.tile_length > S.raw_height)
    throw LIBRAW_EXCEPTION_IO_CORRUPT;

  // calculate tile count
  int tile_w = (S.raw_width + UD.tile_width - 1) / UD.tile_width;
  int tile_h = (S.raw_height + UD.tile_length - 1) / UD.tile_length;
  int tiles = tile_w * tile_h;
  if (tiles < 1 || tiles > 1024)
    throw LIBRAW_EXCEPTION_IO_CORRUPT;

  INT64 fsize = ifp->size();
  std::vector<INT64> toffsets(tiles);
  ifp->seek(UD.data_offset, SEEK_SET); // We're already here, but to make sure
  for (int i = 0; i < tiles; i++)
	  toffsets[i] = get4();

  std::vector<unsigned> tlengths(tiles);
  ifp->seek(UD.data_size, SEEK_SET);
  for (int i = 0; i < tiles; i++)
  {
	  tlengths[i] = get4();
	  if(toffsets[i]+tlengths[i] > fsize)
        throw LIBRAW_EXCEPTION_IO_CORRUPT;
  }
  unsigned maxcomprlen = *std::max_element(tlengths.begin(), tlengths.end());

  std::vector<uint8_t> iobuffer(maxcomprlen+1); // Extra byte to ensure LJPEG byte stream marker search is ok
  std::vector<uint16_t> tilebuffer;
  for (int tile = 0; tile < tiles; tile++)
  {
	  ifp->seek(toffsets[tile],SEEK_SET);
	  int readed =  ifp->read(iobuffer.data(), 1, tlengths[tile]);
	  if(unsigned(readed) != tlengths[tile])
        throw LIBRAW_EXCEPTION_IO_EOF;
	  LibRaw_SonyYCC_Decompressor dec(iobuffer.data(), readed);
	  if(dec.sof.cps != 3) // !YUV
        throw LIBRAW_EXCEPTION_IO_CORRUPT;
	  if(dec.state != LibRaw_LjpegDecompressor::State::OK)
        throw LIBRAW_EXCEPTION_IO_CORRUPT;

	  unsigned tiledatatsize = UD.tile_width * UD.tile_length * 3;
	  if (tilebuffer.size() < tiledatatsize)
		  tilebuffer.resize(tiledatatsize);

	  if(!dec.decode_sony(tilebuffer, UD.tile_width * 3, UD.tile_length))
        throw LIBRAW_EXCEPTION_IO_CORRUPT;

	  int tilerow = tile / tile_w;
	  int tilecol = tile % tile_w;
	  if (imgdata.rawparams.specials & LIBRAW_RAWSPECIAL_SRAW_NO_RGB)
	  {
		  if(imgdata.rawparams.specials & LIBRAW_RAWSPECIAL_SRAW_NO_INTERPOLATE)
	          copy_ycc(imgdata.image, S.raw_width, S.raw_height, tilerow * UD.tile_length, tilecol * UD.tile_width,
		               tilebuffer.data(), UD.tile_width, UD.tile_length, dec.sof.components[0].subsample_h, dec.sof.components[0].subsample_v);
		  else
            copy_ycc(imgdata.image, S.raw_width, S.raw_height, tilerow * UD.tile_length, tilecol * UD.tile_width,
                     tilebuffer.data(), UD.tile_width, UD.tile_length, 1, 1);
	  }
	  else
		ycc2rgb(imgdata.image, S.raw_width, S.raw_height, tilerow * UD.tile_length, tilecol * UD.tile_width,
			  tilebuffer.data(), UD.tile_width, UD.tile_length);
  }

  for (int i = 0; i < 6; i++)
    imgdata.color.cblack[i] = 0;

  if (imgdata.rawparams.specials & LIBRAW_RAWSPECIAL_SRAW_NO_RGB)
  {
	  imgdata.color.maximum = 18091; // estimated on over-exposed ISO100 frame
	  imgdata.color.black = 0;
  }
  else
  {
	  imgdata.color.maximum = 17536; // estimated on over-exposed ISO100 frame
	  imgdata.color.black = 1024;
  }
}
