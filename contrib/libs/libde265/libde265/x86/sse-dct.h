/*
 * H.265 video codec.
 * Copyright (c) 2013 openHEVC contributors
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

#ifndef SSE_DCT_H
#define SSE_DCT_H

#include <stddef.h>
#include <stdint.h>

void ff_hevc_transform_skip_8_sse(uint8_t *_dst, const int16_t *coeffs, ptrdiff_t _stride);
void ff_hevc_transform_4x4_luma_add_8_sse4(uint8_t *dst, const int16_t *coeffs, ptrdiff_t stride);
void ff_hevc_transform_4x4_add_8_sse4(uint8_t *dst, const int16_t *coeffs, ptrdiff_t stride);
void ff_hevc_transform_8x8_add_8_sse4(uint8_t *dst, const int16_t *coeffs, ptrdiff_t stride);
void ff_hevc_transform_16x16_add_8_sse4(uint8_t *dst, const int16_t *coeffs, ptrdiff_t stride);
void ff_hevc_transform_32x32_add_8_sse4(uint8_t *dst, const int16_t *coeffs, ptrdiff_t stride);

#endif
