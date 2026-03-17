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

#ifndef DE265_QUALITY_H
#define DE265_QUALITY_H

#include <stdint.h>
#include <libde265/de265.h>
#include <libde265/image.h>


LIBDE265_API uint32_t SSD(const uint8_t* img, int imgStride,
                          const uint8_t* ref, int refStride,
                          int width, int height);

LIBDE265_API uint32_t SAD(const uint8_t* img, int imgStride,
                          const uint8_t* ref, int refStride,
                          int width, int height);

LIBDE265_API double MSE(const uint8_t* img, int imgStride,
                        const uint8_t* ref, int refStride,
                        int width, int height);

LIBDE265_API double PSNR(double mse);


LIBDE265_API uint32_t compute_distortion_ssd(const de265_image* img1, const de265_image* img2,
                                             int x0, int y0, int log2size, int cIdx);

#endif
