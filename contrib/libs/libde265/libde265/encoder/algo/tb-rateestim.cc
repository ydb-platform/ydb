/*
 * H.265 video codec.
 * Copyright (c) 2013-2014 struktur AG, Dirk Farin <farin@struktur.de>
 *
 * Authors: struktur AG, Dirk Farin <farin@struktur.de>
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


#include "libde265/encoder/algo/tb-rateestim.h"
#include "libde265/encoder/encoder-syntax.h"
#include <assert.h>
#include <iostream>


float Algo_TB_RateEstimation_Exact::encode_transform_unit(encoder_context* ectx,
                                                          context_model_table& ctxModel,
                                                          const enc_tb* tb, const enc_cb* cb,
                                                          int x0,int y0, int xBase,int yBase,
                                                          int log2TrafoSize, int trafoDepth,
                                                          int blkIdx)
{
  CABAC_encoder_estim estim;
  estim.set_context_models(&ctxModel);

  leaf(cb, NULL);

  ::encode_transform_unit(ectx, &estim, tb,cb, x0,y0, xBase,yBase,
                          log2TrafoSize, trafoDepth, blkIdx);

  return estim.getRDBits();
}
