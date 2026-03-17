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

#ifndef DE265_ENCODER_MOTION_H
#define DE265_ENCODER_MOTION_H

#include "libde265/motion.h"

void get_merge_candidate_list_from_tree(class encoder_context* ectx,
                                        const slice_segment_header* shdr,
                                        int xC,int yC, int xP,int yP,
                                        int nCS, int nPbW,int nPbH, int partIdx,
                                        PBMotion* mergeCandList);

#endif
