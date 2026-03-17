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

  MagickCore image private methods.
*/
#ifndef MAGICKCORE_IMAGE_PRIVATE_H
#define MAGICKCORE_IMAGE_PRIVATE_H

#define MagickMax(x,y)  (((x) > (y)) ? (x) : (y))
#define MagickMin(x,y)  (((x) < (y)) ? (x) : (y))

#include "magick/quantum-private.h"

#define BackgroundColor  "#ffffff"  /* white */
#if defined(__cplusplus) || defined(c_plusplus)
extern "C" {
#endif

#define BackgroundColorRGBA  QuantumRange,QuantumRange,QuantumRange,OpaqueOpacity
#define BorderColor  "#dfdfdf"  /* gray */
#define BorderColorRGBA  ScaleShortToQuantum(0xdfdf),\
  ScaleShortToQuantum(0xdfdf),ScaleShortToQuantum(0xdfdf),OpaqueOpacity
#define DefaultResolution  72.0
#define DefaultTileFrame  "15x15+3+3"
#define DefaultTileGeometry  "120x120+4+3>"
#define DefaultTileLabel  "%f\n%G\n%b"
#define ForegroundColor  "#000"  /* black */
#define ForegroundColorRGBA  0,0,0,OpaqueOpacity
#define LoadImagesTag  "Load/Images"
#define LoadImageTag  "Load/Image"
#define Magick2PI    6.28318530717958647692528676655900576839433879875020
#define MagickAbsoluteValue(x)  ((x) < 0 ? -(x) : (x))
#define MAGICK_INT_MAX  (INT_MAX)
#define MagickPHI    1.61803398874989484820458683436563811772030917980576
#define MagickPI2    1.57079632679489661923132169163975144209858469968755
#define MagickPI  3.14159265358979323846264338327950288419716939937510
#define MAGICK_PTRDIFF_MAX  (PTRDIFF_MAX)
#define MAGICK_PTRDIFF_MIN  (-PTRDIFF_MAX-1)
#define MagickSQ1_2  0.70710678118654752440084436210484903928483593768847
#define MagickSQ2    1.41421356237309504880168872420969807856967187537695
#define MagickSQ2PI  2.50662827463100024161235523934010416269302368164062
#define MAGICK_SIZE_MAX  (SIZE_MAX)
#define MAGICK_SSIZE_MAX  (SSIZE_MAX)
#define MAGICK_SSIZE_MIN  (-SSIZE_MAX-1)
#define MatteColor  "#bdbdbd"  /* gray */
#define MatteColorRGBA  ScaleShortToQuantum(0xbdbd),\
  ScaleShortToQuantum(0xbdbd),ScaleShortToQuantum(0xbdbd),OpaqueOpacity
#define PSDensityGeometry  "72.0x72.0"
#define PSPageGeometry  "612x792"
#define SaveImagesTag  "Save/Images"
#define SaveImageTag  "Save/Image"
#define TransparentColor  "#00000000"  /* transparent black */
#define TransparentColorRGBA  0,0,0,TransparentOpacity
#define UndefinedCompressionQuality  0UL
#define UndefinedTicksPerSecond  100L

static inline int CastDoubleToInt(const double x)
{
  double
    value;

  if (IsNaN(x) != 0)
    {
      errno=ERANGE;
      return(0);
    }
  value=(x < 0.0) ? ceil(x) : floor(x);
  if (value < 0.0)
    {
      errno=ERANGE;
      return(0);
    }
  if (value >= ((double) MAGICK_INT_MAX))
    {
      errno=ERANGE;
      return(MAGICK_INT_MAX);
    }
  return((int) value);
}

static inline ssize_t CastDoubleToLong(const double x)
{
  double
    value;

  if (IsNaN(x) != 0)
    {
      errno=ERANGE;
      return(0);
    }
  value=(x < 0.0) ? ceil(x) : floor(x);
  if (value < ((double) MAGICK_SSIZE_MIN))
    {
      errno=ERANGE;
      return(MAGICK_SSIZE_MIN);
    }
  if (value >= ((double) MAGICK_SSIZE_MAX))
    {
      errno=ERANGE;
      return(MAGICK_SSIZE_MAX);
    }
  return((ssize_t) value);
}

static inline QuantumAny CastDoubleToQuantumAny(const double x)
{
  double
    value;

  if (IsNaN(x) != 0)
    {
      errno=ERANGE;
      return(0);
    }
  value=(x < 0.0) ? ceil(x) : floor(x);
  if (value < 0.0)
    {
      errno=ERANGE;
      return(0);
    }
  if (value >= ((double) ((QuantumAny) ~0)))
    {
      errno=ERANGE;
      return((QuantumAny) ~0);
    }
  return((QuantumAny) value);
}

static inline size_t CastDoubleToUnsigned(const double x)
{
  double
    value;

  if (IsNaN(x) != 0)
    {
      errno=ERANGE;
      return(0);
    }
  value=(x < 0.0) ? ceil(x) : floor(x);
  if (value < 0.0)
    {
      errno=ERANGE;
      return(0);
    }
  if (value >= ((double) MAGICK_SIZE_MAX))
    {
      errno=ERANGE;
      return(MAGICK_SIZE_MAX);
    }
  return((size_t) value);
}

static inline double DegreesToRadians(const double degrees)
{
  return((double) (MagickPI*degrees/180.0));
}

static inline MagickRealType RadiansToDegrees(const MagickRealType radians)
{
  return((MagickRealType) (180.0*radians/MagickPI));
}

static inline unsigned char ScaleColor5to8(const unsigned int color)
{
  return((unsigned char) (((color) << 3) | ((color) >> 2)));
}

static inline unsigned char ScaleColor6to8(const unsigned int color)
{
  return((unsigned char) (((color) << 2) | ((color) >> 4)));
}

static inline unsigned int ScaleColor8to5(const unsigned char color)
{
  return((unsigned int) (((color) & ~0x07) >> 3));
}

static inline unsigned int ScaleColor8to6(const unsigned char color)
{
  return((unsigned int) (((color) & ~0x03) >> 2));
}

#if defined(__cplusplus) || defined(c_plusplus)
}
#endif

#endif
