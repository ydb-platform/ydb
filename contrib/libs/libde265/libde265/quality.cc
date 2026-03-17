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

#include "quality.h"
#include <math.h>


uint32_t SSD(const uint8_t* img, int imgStride,
             const uint8_t* ref, int refStride,
             int width, int height)
{
  uint32_t sum=0;

  const uint8_t* iPtr = img;
  const uint8_t* rPtr = ref;

  for (int y=0;y<height;y++) {
    for (int x=0;x<width;x++) {
      int diff = iPtr[x] - rPtr[x];
      sum += diff*diff;
    }

    iPtr += imgStride;
    rPtr += refStride;
  }

  return sum;
}


uint32_t SAD(const uint8_t* img, int imgStride,
             const uint8_t* ref, int refStride,
             int width, int height)
{
  uint32_t sum=0;

  const uint8_t* iPtr = img;
  const uint8_t* rPtr = ref;

  for (int y=0;y<height;y++) {
    for (int x=0;x<width;x++) {
      int diff = iPtr[x] - rPtr[x];
      sum += abs_value(diff);
    }

    iPtr += imgStride;
    rPtr += refStride;
  }

  return sum;
}


double MSE(const uint8_t* img, int imgStride,
           const uint8_t* ref, int refStride,
           int width, int height)
{
  double sum=0.0;

  const uint8_t* iPtr = img;
  const uint8_t* rPtr = ref;

  for (int y=0;y<height;y++) {
    uint32_t lineSum=0;

    for (int x=0;x<width;x++) {
      int diff = iPtr[x] - rPtr[x];
      lineSum += diff*diff;
    }

    sum += ((double)lineSum)/width;

    iPtr += imgStride;
    rPtr += refStride;
  }

  return sum/height;
}


double PSNR(double mse)
{
  if (mse==0) { return 99.99999; }

  return 10*log10(255.0*255.0/mse);
}

uint32_t compute_distortion_ssd(const de265_image* img1, const de265_image* img2,
                                int x0, int y0, int log2size, int cIdx)
{
  return SSD(img1->get_image_plane_at_pos(cIdx,x0,y0), img1->get_image_stride(cIdx),
             img2->get_image_plane_at_pos(cIdx,x0,y0), img2->get_image_stride(cIdx),
             1<<log2size, 1<<log2size);
}
