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

#ifndef DE265_ENCODER_INTRAPRED_H
#define DE265_ENCODER_INTRAPRED_H

#include "libde265/decctx.h"


void fillIntraPredModeCandidates(enum IntraPredMode candModeList[3],
                                 int x,int y,
                                 bool availableA, // left
                                 bool availableB, // top
                                 const class CTBTreeMatrix& ctbs,
                                 const seq_parameter_set* sps);

void decode_intra_prediction_from_tree(const de265_image* img,
                                       const class enc_tb* tb,
                                       const class CTBTreeMatrix& ctbs,
                                       const class seq_parameter_set& sps,
                                       int cIdx);

#endif
