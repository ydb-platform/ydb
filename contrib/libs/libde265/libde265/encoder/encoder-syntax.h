/*
 * H.265 video codec.
 * Copyright (c) 2013-2014 struktur AG, Dirk Farin <farin@struktur.de>
 *
 * Authors: Dirk Farin <farin@struktur.de>
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

#ifndef ENCODER_SYNTAX_H
#define ENCODER_SYNTAX_H

#include "libde265/image.h"
#include "libde265/encoder/encoder-types.h"


void encode_split_cu_flag(encoder_context* ectx,
                          CABAC_encoder* cabac,
                          int x0, int y0, int ctDepth, int split_flag);

void encode_transform_tree(encoder_context* ectx,
                           CABAC_encoder* cabac,
                           const enc_tb* tb, const enc_cb* cb,
                           int x0,int y0, int xBase,int yBase,
                           int log2TrafoSize, int trafoDepth, int blkIdx,
                           int MaxTrafoDepth, int IntraSplitFlag, bool recurse);

void encode_coding_unit(encoder_context* ectx,
                        CABAC_encoder* cabac,
                        const enc_cb* cb, int x0,int y0, int log2CbSize, bool recurse);

/* returns
   1  - forced split
   0  - forced non-split
   -1 - optional split
*/
enum SplitType {
  ForcedNonSplit = 0,
  ForcedSplit    = 1,
  OptionalSplit  = 2
};

SplitType get_split_type(const seq_parameter_set* sps,
                         int x0,int y0, int log2CbSize);


/* Compute how much rate is required for sending the chroma CBF flags
   in the whole TB tree.
 */
float recursive_cbfChroma_rate(CABAC_encoder_estim* cabac,
                               enc_tb* tb, int log2TrafoSize, int trafoDepth);


void encode_split_transform_flag(encoder_context* ectx,
                                 CABAC_encoder* cabac,
                                 int log2TrafoSize, int split_flag);

void encode_merge_idx(encoder_context* ectx,
                      CABAC_encoder* cabac,
                      int mergeIdx);

void encode_cu_skip_flag(encoder_context* ectx,
                         CABAC_encoder* cabac,
                         const enc_cb* cb,
                         bool skip);

void encode_cbf_luma(CABAC_encoder* cabac,
                     bool zeroTrafoDepth, int cbf_luma);

void encode_cbf_chroma(CABAC_encoder* cabac,
                       int trafoDepth, int cbf_chroma);

void encode_transform_unit(encoder_context* ectx,
                           CABAC_encoder* cabac,
                           const enc_tb* tb, const enc_cb* cb,
                           int x0,int y0, int xBase,int yBase,
                           int log2TrafoSize, int trafoDepth, int blkIdx);


void encode_quadtree(encoder_context* ectx,
                     CABAC_encoder* cabac,
                     const enc_cb* cb, int x0,int y0, int log2CbSize, int ctDepth,
                     bool recurse);

void encode_ctb(encoder_context* ectx,
                CABAC_encoder* cabac,
                enc_cb* cb, int ctbX,int ctbY);

#endif
