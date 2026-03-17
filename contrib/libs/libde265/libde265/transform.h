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

#ifndef DE265_TRANSFORM_H
#define DE265_TRANSFORM_H

#include "libde265/de265.h"
#include "libde265/decctx.h"

extern const int tab8_22[];

LIBDE265_INLINE static int table8_22(int qPi)
{
  if (qPi<30) return qPi;
  if (qPi>=43) return qPi-6;
  return tab8_22[qPi-30];
}

// (8.6.1)
void decode_quantization_parameters(thread_context* tctx, int xC,int yC,
                                    int xCUBase, int yCUBase);

// (8.6.2)
void scale_coefficients(thread_context* tctx,
                        int xT,int yT, // position of TU in frame (chroma adapted)
                        int x0,int y0, // position of CU in frame (chroma adapted)
                        int nT, int cIdx,
                        bool transform_skip_flag, bool intra, int rdpcmMode);


void inv_transform(acceleration_functions* acceleration,
                   uint8_t* dst, int dstStride, int16_t* coeff,
                   int log2TbSize, int trType);

void fwd_transform(acceleration_functions* acceleration,
                   int16_t* coeff, int coeffStride, int log2TbSize, int trType,
                   const int16_t* src, int srcStride);

void quant_coefficients(int16_t* out_coeff,
                        const int16_t* in_coeff,
                        int log2TrSize, int qp,
                        bool intra);

void dequant_coefficients(int16_t* out_coeff,
                          const int16_t* in_coeff,
                          int log2TrSize, int qP);

#endif
