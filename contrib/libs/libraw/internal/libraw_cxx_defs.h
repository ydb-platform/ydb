/* -*- C++ -*-
 * File: internal/libraw_cxx_defs.h
 * Copyright 2008-2025 LibRaw LLC (info@libraw.org)
 * Created: Sat Aug  17, 2020

LibRaw is free software; you can redistribute it and/or modify
it under the terms of the one of two licenses as you choose:

1. GNU LESSER GENERAL PUBLIC LICENSE version 2.1
   (See file LICENSE.LGPL provided in LibRaw distribution archive for details).

2. COMMON DEVELOPMENT AND DISTRIBUTION LICENSE (CDDL) Version 1.0
   (See file LICENSE.CDDL provided in LibRaw distribution archive for details).

 */

#ifndef _LIBRAW_CXX_DEFS_H
#define _LIBRAW_CXX_DEFS_H

#include <math.h>
#include <errno.h>
#include <float.h>
#include <new>
#include <exception>
#include <sys/types.h>
#include <sys/stat.h>
#define LIBRAW_LIBRARY_BUILD
#include "libraw/libraw.h"
#include "internal/defines.h"
#ifdef USE_ZLIB
#include <zlib.h>
#endif

#ifndef LIBRAW_WIN32_CALLS
#include <netinet/in.h>
#else
#ifndef LIBRAW_NO_WINSOCK2
#include <winsock2.h>
#endif
#include <io.h>
#endif
#ifdef USE_RAWSPEED
void *make_camera_metadata();
void clear_camera_metadata(void*);
void clear_rawspeed_decoder(void*);
#endif
#ifdef USE_DNGSDK
void clear_dng_negative(void *);
void clear_dng_image(void*);
#endif


#define P1 imgdata.idata
#define S  imgdata.sizes
#ifndef LIBRAW_DNGSDK_CONFLICT
#define O  imgdata.params
#define C  imgdata.color
#define T  imgdata.thumbnail
#define MN imgdata.makernotes
#ifndef LIBRAW_EXPAT_CONFLICT
#define IO libraw_internal_data.internal_output_params
#define ID libraw_internal_data.internal_data
#endif
#endif

#define makeIs(idx) (imgdata.idata.maker_index == idx)
#define mnCamID imgdata.lens.makernotes.CamID

#define EXCEPTION_HANDLER(e)                                                   \
  do                                                                           \
  {                                                                            \
    switch (e)                                                                 \
    {                                                                          \
    case LIBRAW_EXCEPTION_MEMPOOL:                                             \
      recycle();                                                               \
      return LIBRAW_MEMPOOL_OVERFLOW;                                          \
    case LIBRAW_EXCEPTION_ALLOC:                                               \
      recycle();                                                               \
      return LIBRAW_UNSUFFICIENT_MEMORY;                                       \
    case LIBRAW_EXCEPTION_TOOBIG:                                              \
      recycle();                                                               \
      return LIBRAW_TOO_BIG;                                                   \
    case LIBRAW_EXCEPTION_DECODE_RAW:                                          \
    case LIBRAW_EXCEPTION_DECODE_JPEG:                                         \
      recycle();                                                               \
      return LIBRAW_DATA_ERROR;                                                \
    case LIBRAW_EXCEPTION_DECODE_JPEG2000:                                     \
      recycle();                                                               \
      return LIBRAW_DATA_ERROR;                                                \
    case LIBRAW_EXCEPTION_IO_EOF:                                              \
    case LIBRAW_EXCEPTION_IO_CORRUPT:                                          \
      recycle();                                                               \
      return LIBRAW_IO_ERROR;                                                  \
    case LIBRAW_EXCEPTION_CANCELLED_BY_CALLBACK:                               \
      recycle();                                                               \
      return LIBRAW_CANCELLED_BY_CALLBACK;                                     \
    case LIBRAW_EXCEPTION_BAD_CROP:                                            \
      recycle();                                                               \
      return LIBRAW_BAD_CROP;                                                  \
    case LIBRAW_EXCEPTION_UNSUPPORTED_FORMAT:                                  \
      recycle();                                                               \
      return LIBRAW_FILE_UNSUPPORTED;                                          \
    default:                                                                   \
      return LIBRAW_UNSPECIFIED_ERROR;                                         \
    }                                                                          \
  } while (0)

// copy-n-paste from image pipe
#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))
#define LIM(x, min, max) MAX(min, MIN(x, max))
#ifndef CLIP
#define CLIP(x) LIM(x, 0, 65535)
#endif
#define THUMB_READ_BEYOND 16384

#define ZERO(a) memset(&a, 0, sizeof(a))

#endif
