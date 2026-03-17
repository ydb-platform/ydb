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

#ifndef DE265_VISUALIZE_H
#define DE265_VISUALIZE_H

#include "libde265/de265.h"
#include "libde265/image.h"


void write_picture_to_file(const de265_image* img, const char* filename);

#ifdef __cplusplus
extern "C" {
#endif

// TODO: these should either move to "sherlock265", or be part of the
// "official" public API
LIBDE265_API void draw_CB_grid(const de265_image* img, uint8_t* dst, int stride, uint32_t value, int pixelSize);
LIBDE265_API void draw_TB_grid(const de265_image* img, uint8_t* dst, int stride, uint32_t value, int pixelSize);
LIBDE265_API void draw_PB_grid(const de265_image* img, uint8_t* dst, int stride, uint32_t value, int pixelSize);
LIBDE265_API void draw_PB_pred_modes(const de265_image* img, uint8_t* dst, int stride, int pixelSize);
LIBDE265_API void draw_intra_pred_modes(const de265_image* img, uint8_t* dst, int stride, uint32_t value, int pixelSize);
LIBDE265_API void draw_QuantPY(const de265_image* img, uint8_t* dst, int stride, int pixelSize);
LIBDE265_API void draw_Motion(const de265_image* img, uint8_t* dst, int stride, int pixelSize);
LIBDE265_API void draw_Slices(const de265_image* img, uint8_t* dst, int stride, int pixelSize);
LIBDE265_API void draw_Tiles(const de265_image* img, uint8_t* dst, int stride, int pixelSize);

#ifdef __cplusplus
}
#endif

#endif
