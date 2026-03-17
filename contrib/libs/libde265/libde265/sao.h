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

#ifndef DE265_SAO_H
#define DE265_SAO_H

#include "libde265/decctx.h"

void apply_sample_adaptive_offset(de265_image* img);

/* requires less memory than the function above */
void apply_sample_adaptive_offset_sequential(de265_image* img);

/* saoInputProgress - the CTB progress that SAO will wait for before beginning processing.
   Returns 'true' if any tasks have been added.
 */
bool add_sao_tasks(image_unit* imgunit, int saoInputProgress);

#endif
