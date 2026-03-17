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

#ifndef DE265_REFPIC_H
#define DE265_REFPIC_H

#include "libde265/bitstream.h"

#define MAX_NUM_REF_PICS 16  // maximum defined by standard, may be lower for some Levels


class ref_pic_set
{
 public:
  // Lists of pictures that have to be kept in the decoded picture buffer for future
  // reference and that may optionally be used for prediction in the current frame.
  // Lists contain the relative POC positions.
  int16_t DeltaPocS0[MAX_NUM_REF_PICS]; // sorted in decreasing order (e.g. -1, -2, -4, -7, ...)
  int16_t DeltaPocS1[MAX_NUM_REF_PICS]; // sorted in ascending order (e.g. 1, 2, 4, 7)

  // flag for each reference whether this is actually used for prediction in the current frame
  uint8_t UsedByCurrPicS0[MAX_NUM_REF_PICS];
  uint8_t UsedByCurrPicS1[MAX_NUM_REF_PICS];

  uint8_t NumNegativePics;  // number of past reference pictures
  uint8_t NumPositivePics;  // number of future reference pictures

  // --- derived values ---

  void compute_derived_values();

  uint8_t NumDeltaPocs;     // total number of reference pictures (past + future)

  uint8_t NumPocTotalCurr_shortterm_only; /* Total number of reference pictures that may actually
                                             be used for prediction in the current frame. */

  void reset();
};


void dump_short_term_ref_pic_set(const ref_pic_set*, FILE* fh);
void dump_compact_short_term_ref_pic_set(const ref_pic_set* set, int range, FILE* fh);

#endif
