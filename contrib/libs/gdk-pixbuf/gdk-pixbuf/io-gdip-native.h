/* -*- Mode: C; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 8 -*- */
/* GdkPixbuf library - Win32 GDI+ Pixbuf Loader
 *
 * Copyright (C) 2007 Google (Evan Stade)
 * Copyright (C) 2008 Alberto Ruiz <aruiz@gnome.org>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301, USA
 */

#ifndef _HAVE_IO_GDIP_NATIVE_H
#define _HAVE_IO_GDIP_NATIVE_H

#include <windows.h>

#include <glib.h>

/* //////////// Native API ///////////// */

#define WINGDIPAPI __stdcall

typedef gulong ARGB;
typedef gint PixelFormat;

typedef enum {
    EncoderParameterValueTypeByte = 1,
    EncoderParameterValueTypeASCII = 2,
    EncoderParameterValueTypeShort = 3,
    EncoderParameterValueTypeLong = 4,
    EncoderParameterValueTypeRational = 5,
    EncoderParameterValueTypeLongRange = 6,
    EncoderParameterValueTypeUndefined = 7,
    EncoderParameterValueTypeRationalRange = 8,
    EncoderParameterValueTypePointer = 9
} EncoderParameterValueType;

#define    PixelFormatIndexed   0x00010000
#define    PixelFormatGDI       0x00020000
#define    PixelFormatAlpha     0x00040000
#define    PixelFormatPAlpha    0x00080000
#define    PixelFormatExtended  0x00100000
#define    PixelFormatCanonical 0x00200000

#define    PixelFormatUndefined 0
#define    PixelFormatDontCare  0

#define    PixelFormat1bppIndexed       (1 | ( 1 << 8) | PixelFormatIndexed | PixelFormatGDI)
#define    PixelFormat4bppIndexed       (2 | ( 4 << 8) | PixelFormatIndexed | PixelFormatGDI)
#define    PixelFormat8bppIndexed       (3 | ( 8 << 8) | PixelFormatIndexed | PixelFormatGDI)
#define    PixelFormat16bppGrayScale    (4 | (16 << 8) | PixelFormatExtended)
#define    PixelFormat16bppRGB555       (5 | (16 << 8) | PixelFormatGDI)
#define    PixelFormat16bppRGB565       (6 | (16 << 8) | PixelFormatGDI)
#define    PixelFormat16bppARGB1555     (7 | (16 << 8) | PixelFormatAlpha | PixelFormatGDI)
#define    PixelFormat24bppRGB          (8 | (24 << 8) | PixelFormatGDI)
#define    PixelFormat32bppRGB          (9 | (32 << 8) | PixelFormatGDI)
#define    PixelFormat32bppARGB         (10 | (32 << 8) | PixelFormatAlpha | PixelFormatGDI | PixelFormatCanonical)
#define    PixelFormat32bppPARGB        (11 | (32 << 8) | PixelFormatAlpha | PixelFormatPAlpha | PixelFormatGDI)
#define    PixelFormat48bppRGB          (12 | (48 << 8) | PixelFormatExtended)
#define    PixelFormat64bppARGB         (13 | (64 << 8) | PixelFormatAlpha  | PixelFormatCanonical | PixelFormatExtended)
#define    PixelFormat64bppPARGB        (14 | (64 << 8) | PixelFormatAlpha  | PixelFormatPAlpha | PixelFormatExtended)
#define    PixelFormatMax               15

enum _Status
{
    Ok                          = 0,
    GenericError                = 1,
    InvalidParameter            = 2,
    OutOfMemory                 = 3,
    ObjectBusy                  = 4,
    InsufficientBuffer          = 5,
    NotImplemented              = 6,
    Win32Error                  = 7,
    WrongState                  = 8,
    Aborted                     = 9,
    FileNotFound                = 10,
    ValueOverflow               = 11,
    AccessDenied                = 12,
    UnknownImageFormat          = 13,
    FontFamilyNotFound          = 14,
    FontStyleNotFound           = 15,
    NotTrueTypeFont             = 16,
    UnsupportedGdiplusVersion   = 17,
    GdiplusNotInitialized       = 18,
    PropertyNotFound            = 19,
    PropertyNotSupported        = 20,
    ProfileNotFound             = 21
};
typedef enum _Status Status;
typedef enum _Status GpStatus;

typedef enum {
    ImageFlagsNone = 0,
    ImageFlagsScalable = 0x0001,
    ImageFlagsHasAlpha = 0x0002,
    ImageFlagsHasTranslucent = 0x0004,
    ImageFlagsPartiallyScalable = 0x0008,
    ImageFlagsColorSpaceRGB = 0x0010,
    ImageFlagsColorSpaceCMYK = 0x0020,
    ImageFlagsColorSpaceGRAY = 0x0040,
    ImageFlagsColorSpaceYCBCR = 0x0080,
    ImageFlagsColorSpaceYCCK = 0x0100,
    ImageFlagsHasRealDPI = 0x1000,
    ImageFlagsHasRealPixelSize = 0x2000,
    ImageFlagsReadOnly = 0x00010000,
    ImageFlagsCaching = 0x00020000
} ImageFlags;

enum _ImageLockMode
{
    ImageLockModeRead           = 1,
    ImageLockModeWrite          = 2,
    ImageLockModeUserInputBuf   = 4
};
typedef enum _ImageLockMode ImageLockMode;

enum _ImageType
{
    ImageTypeUnknown,
    ImageTypeBitmap,
    ImageTypeMetafile
};
typedef enum _ImageType ImageType;

typedef struct _GpImage GpImage;
typedef struct _GpBitmap GpBitmap;
typedef struct _GpGraphics GpGraphics;

struct _GdiplusStartupInput
{
    UINT32 GdiplusVersion;
    gpointer DebugEventCallback;
    BOOL SuppressBackgroundThread;
    BOOL SuppressExternalCodecs;
};
typedef struct _GdiplusStartupInput GdiplusStartupInput;

struct _PropItem
{
  ULONG id;
  ULONG length;
  WORD type;
  VOID *value;
};
typedef struct _PropItem PropertyItem;

struct _EncoderParameter
{
    GUID    Guid;
    ULONG   NumberOfValues;
    ULONG   Type;
    VOID*   Value;
};
typedef struct _EncoderParameter EncoderParameter;

struct _EncoderParameters
{
    UINT Count;                      /* Number of parameters in this structure */
    EncoderParameter Parameter[1];   /* Parameter values */
};
typedef struct _EncoderParameters EncoderParameters;

struct _ImageCodecInfo
{
    CLSID Clsid;
    GUID  FormatID;
    const WCHAR* CodecName;
    const WCHAR* DllName;
    const WCHAR* FormatDescription;
    const WCHAR* FilenameExtension;
    const WCHAR* MimeType;
    DWORD Flags;
    DWORD Version;
    DWORD SigCount;
    DWORD SigSize;
    const BYTE* SigPattern;
    const BYTE* SigMask;
};
typedef struct _ImageCodecInfo ImageCodecInfo;

struct _BitmapData
{
    UINT Width;
    UINT Height;
    INT Stride;
    PixelFormat PixelFormat;
    VOID* Scan0;
    UINT_PTR Reserved;
};
typedef struct _BitmapData BitmapData;

struct _GpRect
{
    INT X;
    INT Y;
    INT Width;
    INT Height;
};
typedef struct _GpRect GpRect;

#ifndef IStream_Release
#define IStream_Release(This) (This)->lpVtbl->Release(This)
#endif

#ifndef IStream_Seek
#define IStream_Seek(This,dlibMove,dwOrigin,plibNewPosition) (This)->lpVtbl->Seek(This,dlibMove,dwOrigin,plibNewPosition)
#endif

#ifndef IStream_Read
#define IStream_Read(This,pv,cb,pcbRead) (This)->lpVtbl->Read(This,pv,cb,pcbRead)
#endif

#ifndef IStream_SetSize
#define IStream_SetSize(This,size) (This)->lpVtbl->SetSize(This,size)
#endif

GpStatus WINGDIPAPI GdiplusStartup (gpointer, const gpointer, gpointer);
GpStatus WINGDIPAPI GdipCreateBitmapFromStream (gpointer, GpBitmap**);
GpStatus WINGDIPAPI GdipBitmapGetPixel (GpBitmap*, gint x, gint y, ARGB*);
GpStatus WINGDIPAPI GdipGetImageWidth (GpImage*, guint*);
GpStatus WINGDIPAPI GdipGetImageHeight (GpImage*, guint*);
GpStatus WINGDIPAPI GdipDisposeImage (GpImage*);
GpStatus WINGDIPAPI GdipGetImageFlags (GpImage *, guint*);
GpStatus WINGDIPAPI GdipImageGetFrameCount (GpImage *image, const GUID* dimensionID, UINT* count);
GpStatus WINGDIPAPI GdipImageSelectActiveFrame (GpImage *image, const GUID* dimensionID, UINT frameIndex);
GpStatus WINGDIPAPI GdipGetPropertyItemSize (GpImage *image, int propId, guint* size);
GpStatus WINGDIPAPI GdipGetPropertyItem (GpImage *image, int propId, guint propSize, PropertyItem* buffer);
GpStatus WINGDIPAPI GdipCreateBitmapFromScan0 (INT width, INT height, INT stride, PixelFormat format, BYTE* scan0, 
                                               GpBitmap** bitmap);
GpStatus WINGDIPAPI GdipSaveImageToStream (GpImage *image, IStream* stream, const CLSID* clsidEncoder, 
                                           const EncoderParameters* encoderParams);
GpStatus WINGDIPAPI GdipGetImageEncoders (UINT numEncoders, UINT size, ImageCodecInfo *encoders);
GpStatus WINGDIPAPI GdipGetImageEncodersSize (UINT *numEncoders, UINT *size);
GpStatus WINGDIPAPI GdipBitmapSetPixel (GpBitmap* bitmap, INT x, INT y, ARGB color);
GpStatus WINGDIPAPI GdipDrawImageI (GpGraphics *graphics, GpImage *image, INT x, INT y);
GpStatus WINGDIPAPI GdipGetImageGraphicsContext (GpImage *image, GpGraphics **graphics);
GpStatus WINGDIPAPI GdipFlush (GpGraphics *graphics, INT intention);
GpStatus WINGDIPAPI GdipGraphicsClear (GpGraphics *graphics, ARGB color);
GpStatus WINGDIPAPI GdipBitmapSetResolution (GpBitmap* bitmap, float xdpi, float ydpi);
GpStatus WINGDIPAPI GdipGetImageHorizontalResolution (GpImage *image, float *resolution);
GpStatus WINGDIPAPI GdipGetImageVerticalResolution (GpImage *image, float *resolution);
GpStatus WINGDIPAPI GdipLoadImageFromStream (IStream* stream, GpImage **image);
GpStatus WINGDIPAPI GdipDeleteGraphics (GpGraphics *graphics);

#endif
