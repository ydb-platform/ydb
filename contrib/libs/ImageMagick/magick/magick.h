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

  MagickCore magick methods.
*/
#ifndef MAGICKCORE_MAGICK_H
#define MAGICKCORE_MAGICK_H

#if defined(__cplusplus) || defined(c_plusplus)
extern "C" {
#endif

#include <stdarg.h>
#include "magick/semaphore.h"

#if defined(__cplusplus) || defined(c_plusplus)
# define magick_module  _module   /* reserved word in C++(20) */
#else
# define magick_module  module
#endif

typedef enum
{
  UndefinedFormatType,
  ImplicitFormatType,
  ExplicitFormatType
} MagickFormatType;

typedef enum
{
  NoThreadSupport = 0x0000,
  DecoderThreadSupport = 0x0001,
  EncoderThreadSupport = 0x0002
} MagickThreadSupport;

typedef Image
  *DecodeImageHandler(const ImageInfo *,ExceptionInfo *);

typedef MagickBooleanType
  EncodeImageHandler(const ImageInfo *,Image *);

typedef MagickBooleanType
  IsImageFormatHandler(const unsigned char *,const size_t);

typedef struct _MagickInfo
{
  char
    *name,
    *description,
    *version,
    *note,
    *magick_module;

  ImageInfo
    *image_info;

  DecodeImageHandler
    *decoder;

  EncodeImageHandler
    *encoder;

  IsImageFormatHandler
    *magick;

  void
    *client_data;

  MagickBooleanType
    adjoin,
    raw,
    endian_support,
    blob_support,
    seekable_stream;

  MagickFormatType
    format_type;

  MagickStatusType
    thread_support;

  MagickBooleanType
    stealth;

  struct _MagickInfo
    *previous,
    *next;  /* deprecated, use GetMagickInfoList() */

  size_t
    signature;

  char
    *mime_type;

  SemaphoreInfo
    *semaphore;
} MagickInfo;

extern MagickExport char
  **GetMagickList(const char *,size_t *,ExceptionInfo *);

extern MagickExport const char
  *GetMagickDescription(const MagickInfo *),
  *GetMagickMimeType(const MagickInfo *);

extern MagickExport DecodeImageHandler
  *GetImageDecoder(const MagickInfo *) magick_attribute((__pure__));

extern MagickExport EncodeImageHandler
  *GetImageEncoder(const MagickInfo *) magick_attribute((__pure__));

extern MagickExport int
  GetMagickPrecision(void),
  SetMagickPrecision(const int);

extern MagickExport MagickBooleanType
  GetImageMagick(const unsigned char *,const size_t,char *),
  GetMagickAdjoin(const MagickInfo *) magick_attribute((__pure__)),
  GetMagickBlobSupport(const MagickInfo *) magick_attribute((__pure__)),
  GetMagickEndianSupport(const MagickInfo *) magick_attribute((__pure__)),
  GetMagickRawSupport(const MagickInfo *) magick_attribute((__pure__)),
  GetMagickSeekableStream(const MagickInfo *) magick_attribute((__pure__)),
  IsMagickCoreInstantiated(void) magick_attribute((__pure__)),
  MagickComponentGenesis(void),
  UnregisterMagickInfo(const char *);

extern const MagickExport MagickInfo
  *GetMagickInfo(const char *,ExceptionInfo *),
  **GetMagickInfoList(const char *,size_t *,ExceptionInfo *);

extern MagickExport MagickInfo
  *RegisterMagickInfo(MagickInfo *),
  *SetMagickInfo(const char *);

extern MagickExport MagickStatusType
  GetMagickThreadSupport(const MagickInfo *);

extern MagickExport void
  MagickComponentTerminus(void),
  MagickCoreGenesis(const char *,const MagickBooleanType),
  MagickCoreTerminus(void);

#if defined(__cplusplus) || defined(c_plusplus)
}
#endif

#endif
