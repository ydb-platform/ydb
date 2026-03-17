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

#include "libde265/image-io.h"
#include <assert.h>



ImageSource::ImageSource()
{
}


ImageSource::~ImageSource() { }


ImageSource_YUV::ImageSource_YUV()
  : mFH(NULL)
{
}


ImageSource_YUV::~ImageSource_YUV()
{
  if (mFH) {
    fclose(mFH);
  }
}


bool ImageSource_YUV::set_input_file(const char* filename, int w,int h)
{
  assert(mFH==NULL);

  mFH = fopen(filename,"rb");
  if (mFH==NULL) {
    return false;
  }

  width =w;
  height=h;
  mReachedEndOfFile = false;

  return true;
}


de265_image* ImageSource_YUV::read_next_image()
{
  if (mReachedEndOfFile) return NULL;

  de265_image* img = new de265_image;
  img->alloc_image(width,height,de265_chroma_420, NULL, false,
                   NULL, /*NULL,*/ 0, NULL, false);
  assert(img); // TODO: error handling

  // --- load image ---

  uint8_t* p;
  int stride;

  p = img->get_image_plane(0);  stride = img->get_image_stride(0);
  for (int y=0;y<height;y++) {
    if (fread(p+y*stride,1,width,mFH) != width) {
      goto check_eof;
    }
  }

  p = img->get_image_plane(1);  stride = img->get_image_stride(1);
  for (int y=0;y<height/2;y++) {
    if (fread(p+y*stride,1,width/2,mFH) != width/2) {
      goto check_eof;
    }
  }

  p = img->get_image_plane(2);  stride = img->get_image_stride(2);
  for (int y=0;y<height/2;y++) {
    if (fread(p+y*stride,1,width/2,mFH) != width/2) {
      goto check_eof;
    }
  }

  // --- check for EOF ---

check_eof:
  if (feof(mFH)) {
    mReachedEndOfFile = true;
    delete img;
    return NULL;
  }
  else {
    return img;
  }
}


/*
ImageSource::ImageStatus  ImageSource_YUV::get_status()
{
  return Available;
}
*/

de265_image* ImageSource_YUV::get_image(bool block)
{
  de265_image* img = read_next_image();
  return img;
}


void ImageSource_YUV::skip_frames(int n)
{
  int imageSize = width*height*3/2;
  fseek(mFH,n * imageSize, SEEK_CUR);
}


int ImageSource_YUV::get_width() const
{
  return width;
}


int ImageSource_YUV::get_height() const
{
  return height;
}




ImageSink::~ImageSink() { }

ImageSink_YUV::ImageSink_YUV() : mFH(NULL) { }


ImageSink_YUV::~ImageSink_YUV()
{
  if (mFH) {
    fclose(mFH);
  }
}

bool ImageSink_YUV::set_filename(const char* filename)
{
  assert(mFH==NULL);

  mFH = fopen(filename,"wb");

  return true;
}

void ImageSink_YUV::send_image(const de265_image* img)
{
  // --- write image ---

  const uint8_t* p;
  int stride;

  int width = img->get_width();
  int height= img->get_height();

  p = img->get_image_plane(0);  stride = img->get_image_stride(0);
  for (int y=0;y<height;y++) {
    size_t n = fwrite(p+y*stride,1,width,mFH);
    (void)n;
  }

  p = img->get_image_plane(1);  stride = img->get_image_stride(1);
  for (int y=0;y<height/2;y++) {
    size_t n = fwrite(p+y*stride,1,width/2,mFH);
    (void)n;
  }

  p = img->get_image_plane(2);  stride = img->get_image_stride(2);
  for (int y=0;y<height/2;y++) {
    size_t n = fwrite(p+y*stride,1,width/2,mFH);
    (void)n;
  }
}



PacketSink::~PacketSink() { }

PacketSink_File::PacketSink_File()
  : mFH(NULL)
{
}


LIBDE265_API PacketSink_File::~PacketSink_File()
{
  if (mFH) {
    fclose(mFH);
  }
}


LIBDE265_API void PacketSink_File::set_filename(const char* filename)
{
  assert(mFH==NULL);

  mFH = fopen(filename,"wb");
}


LIBDE265_API void PacketSink_File::send_packet(const uint8_t* data, int n)
{
  uint8_t startCode[3];
  startCode[0] = 0;
  startCode[1] = 0;
  startCode[2] = 1;

  size_t dummy;
  dummy = fwrite(startCode,1,3,mFH);
  (void)dummy;

  dummy = fwrite(data,1,n,mFH);
  (void)dummy;

  fflush(mFH);
}
