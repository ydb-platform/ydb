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

#ifndef IMAGE_IO_H
#define IMAGE_IO_H

#include "libde265/image.h"
#include <deque>


class ImageSource
{
 public:
  LIBDE265_API ImageSource();
  virtual LIBDE265_API ~ImageSource();

  //enum ImageStatus { Available, Waiting, EndOfVideo };

  //virtual ImageStatus  get_status() = 0;
  virtual LIBDE265_API de265_image* get_image(bool block=true) = 0;
  virtual LIBDE265_API void skip_frames(int n) = 0;

  virtual LIBDE265_API int get_width() const = 0;
  virtual LIBDE265_API int get_height() const = 0;
};



class ImageSource_YUV : public ImageSource
{
 public:
  LIBDE265_API ImageSource_YUV();
  virtual LIBDE265_API ~ImageSource_YUV();

  bool LIBDE265_API set_input_file(const char* filename, int w,int h);

  //virtual ImageStatus  get_status();
  virtual LIBDE265_API de265_image* get_image(bool block=true);
  virtual LIBDE265_API void skip_frames(int n);

  virtual LIBDE265_API int get_width() const;
  virtual LIBDE265_API int get_height() const;

 private:
  FILE* mFH;
  bool mReachedEndOfFile;

  int width,height;

  de265_image* read_next_image();
};



class ImageSink
{
 public:
  virtual LIBDE265_API ~ImageSink();

  virtual LIBDE265_API void send_image(const de265_image* img) = 0;
};

class ImageSink_YUV : public ImageSink
{
 public:
  LIBDE265_API ImageSink_YUV();
  LIBDE265_API ~ImageSink_YUV();

  bool LIBDE265_API set_filename(const char* filename);

  virtual LIBDE265_API void send_image(const de265_image* img);

 private:
  FILE* mFH;
};



class PacketSink
{
 public:
  virtual LIBDE265_API ~PacketSink();

  virtual LIBDE265_API void send_packet(const uint8_t* data, int n) = 0;
};


class PacketSink_File : public PacketSink
{
 public:
  LIBDE265_API PacketSink_File();
  virtual LIBDE265_API ~PacketSink_File();

  LIBDE265_API void set_filename(const char* filename);

  virtual LIBDE265_API void send_packet(const uint8_t* data, int n);

 private:
  FILE* mFH;
};

#endif
