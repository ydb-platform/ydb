/*
  Copyright 1999 ImageMagick Studio LLC, a non-profit organization
  dedicated to making software imaging solutions freely available.

  You may not use this file except in compliance with the License.  You may
  obtain a copy of the License at

    https://imagemagick.org/license/

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

  MagickCore Binary Large OBjects methods.
*/
#ifndef MAGICKCORE_BLOB_H
#define MAGICKCORE_BLOB_H

#include "magick/image.h"
#include "magick/stream.h"

#if defined(__cplusplus) || defined(c_plusplus)
extern "C" {
#endif

#define MagickMaxBufferExtent  524288
#define MagickMinBufferExtent  16384

typedef enum
{
  ReadMode,
  WriteMode,
  IOMode,
  PersistMode
} MapMode;

extern MagickExport FILE
  *GetBlobFileHandle(const Image *) magick_attribute((__pure__));

extern MagickExport Image
  *BlobToImage(const ImageInfo *,const void *,const size_t,ExceptionInfo *),
  *PingBlob(const ImageInfo *,const void *,const size_t,ExceptionInfo *);

extern MagickExport MagickBooleanType
  BlobToFile(char *,const void *,const size_t,ExceptionInfo *),
  FileToImage(Image *,const char *),
  GetBlobError(const Image *) magick_attribute((__pure__)),
  ImageToFile(Image *,char *,ExceptionInfo *),
  InjectImageBlob(const ImageInfo *,Image *,Image *,const char *,
    ExceptionInfo *),
  IsBlobExempt(const Image *) magick_attribute((__pure__)),
  IsBlobSeekable(const Image *) magick_attribute((__pure__)),
  IsBlobTemporary(const Image *) magick_attribute((__pure__));

extern MagickExport MagickSizeType
  GetBlobSize(const Image *);

extern MagickExport StreamHandler
  GetBlobStreamHandler(const Image *) magick_attribute((__pure__));

extern MagickExport unsigned char
  *FileToBlob(const char *,const size_t,size_t *,ExceptionInfo *),
  *GetBlobStreamData(const Image *) magick_attribute((__pure__)),
  *ImageToBlob(const ImageInfo *,Image *,size_t *,ExceptionInfo *),
  *ImagesToBlob(const ImageInfo *,Image *,size_t *,ExceptionInfo *);

extern MagickExport void
  DestroyBlob(Image *),
  DuplicateBlob(Image *,const Image *),
  SetBlobExempt(Image *,const MagickBooleanType);

#if defined(__cplusplus) || defined(c_plusplus)
}
#endif

#endif
