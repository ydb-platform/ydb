// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXGE_DIB_FX_DIB_H_
#define CORE_FXGE_DIB_FX_DIB_H_

#include <stdint.h>

#include <utility>

#include "core/fxcrt/compiler_specific.h"

// Encoding:
// - Bits-per-pixel: value & 0xFF
// - Is mask: value & 0x100
// - Has alpha: value & 0x200
// - Is premultiplied-alpha: value & 0x400
enum class FXDIB_Format : uint16_t {
  kInvalid = 0,
  k1bppRgb = 0x001,
  k8bppRgb = 0x008,
  kBgr = 0x018,
  kBgrx = 0x020,
  k1bppMask = 0x101,
  k8bppMask = 0x108,
  kBgra = 0x220,
#if defined(PDF_USE_SKIA)
  kBgraPremul = 0x620,
#endif
};

// Endian-dependent (in theory).
using FX_ARGB = uint32_t;  // A in high bits, ..., B in low bits.
using FX_CMYK = uint32_t;  // C in high bits, ..., K in low bits.

// FX_COLORREF, like win32 COLORREF, is BGR. i.e. 0x00BBGGRR.
// Note that while the non-existent alpha component should be set to 0, some
// parts of the codebase use 0xFFFFFFFF as a sentinel value to indicate error.
using FX_COLORREF = uint32_t;

// Endian-independent, name-ordered by increasing address.
template <typename T>
struct FX_RGB_STRUCT {
  T red = 0;
  T green = 0;
  T blue = 0;
};

template <typename T>
struct FX_BGR_STRUCT {
  T blue = 0;
  T green = 0;
  T red = 0;
};

template <typename T>
struct FX_ARGB_STRUCT {
  T alpha = 0;
  T red = 0;
  T green = 0;
  T blue = 0;
};

template <typename T>
struct FX_ABGR_STRUCT {
  T alpha = 0;
  T blue = 0;
  T green = 0;
  T red = 0;
};

template <typename T>
struct FX_RGBA_STRUCT {
  T red = 0;
  T green = 0;
  T blue = 0;
  T alpha = 0;
};

template <typename T>
struct FX_BGRA_STRUCT {
  T blue = 0;
  T green = 0;
  T red = 0;
  T alpha = 0;
};

template <typename T>
struct FX_CMYK_STRUCT {
  T cyan = 0;
  T magenta = 0;
  T yellow = 0;
  T key = 0;
};

template <typename T>
struct FX_LAB_STRUCT {
  T lightness_star = 0;
  T a_star = 0;
  T b_star = 0;
};

struct FXDIB_ResampleOptions {
  FXDIB_ResampleOptions();

  bool HasAnyOptions() const;

  bool bInterpolateBilinear = false;
  bool bHalftone = false;
  bool bNoSmoothing = false;
  bool bLossy = false;
};

// See PDF 1.7 spec, table 7.2 and 7.3. The enum values need to be in the same
// order as listed in the spec.
enum class BlendMode {
  kNormal = 0,
  kMultiply,
  kScreen,
  kOverlay,
  kDarken,
  kLighten,
  kColorDodge,
  kColorBurn,
  kHardLight,
  kSoftLight,
  kDifference,
  kExclusion,
  kHue,
  kSaturation,
  kColor,
  kLuminosity,
  kLast = kLuminosity,
};

constexpr uint32_t FXSYS_BGR(uint8_t b, uint8_t g, uint8_t r) {
  return (b << 16) | (g << 8) | r;
}

constexpr uint8_t FXSYS_GetRValue(uint32_t bgr) {
  return bgr & 0xff;
}

constexpr uint8_t FXSYS_GetGValue(uint32_t bgr) {
  return (bgr >> 8) & 0xff;
}

constexpr uint8_t FXSYS_GetBValue(uint32_t bgr) {
  return (bgr >> 16) & 0xff;
}

constexpr unsigned int FXSYS_GetUnsignedAlpha(float alpha) {
  return static_cast<unsigned int>(alpha * 255.f + 0.5f);
}

// Bits per pixel, not bytes.
inline int GetBppFromFormat(FXDIB_Format format) {
  return static_cast<uint16_t>(format) & 0xff;
}

// AKA bytes per pixel, assuming 8-bits per component.
inline int GetCompsFromFormat(FXDIB_Format format) {
  return (static_cast<uint16_t>(format) & 0xff) / 8;
}

inline bool GetIsMaskFromFormat(FXDIB_Format format) {
  return !!(static_cast<uint16_t>(format) & 0x100);
}

inline bool GetIsAlphaFromFormat(FXDIB_Format format) {
  return !!(static_cast<uint16_t>(format) & 0x200);
}

FX_BGRA_STRUCT<uint8_t> ArgbToBGRAStruct(FX_ARGB argb);

// Ignores alpha.
FX_BGR_STRUCT<uint8_t> ArgbToBGRStruct(FX_ARGB argb);

// Returns (a, FX_COLORREF)
std::pair<uint8_t, FX_COLORREF> ArgbToAlphaAndColorRef(FX_ARGB argb);

FX_COLORREF ArgbToColorRef(FX_ARGB argb);
FX_ARGB AlphaAndColorRefToArgb(int a, FX_COLORREF colorref);

constexpr FX_ARGB ArgbEncode(uint32_t a, uint32_t r, uint32_t g, uint32_t b) {
  return (a << 24) | (r << 16) | (g << 8) | b;
}

constexpr FX_CMYK CmykEncode(uint32_t c, uint32_t m, uint32_t y, uint32_t k) {
  return (c << 24) | (m << 16) | (y << 8) | k;
}

#define FXARGB_A(argb) ((uint8_t)((argb) >> 24))
#define FXARGB_R(argb) ((uint8_t)((argb) >> 16))
#define FXARGB_G(argb) ((uint8_t)((argb) >> 8))
#define FXARGB_B(argb) ((uint8_t)(argb))
#define FXARGB_MUL_ALPHA(argb, alpha) \
  (((((argb) >> 24) * (alpha) / 255) << 24) | ((argb)&0xffffff))

#define FXRGB2GRAY(r, g, b) (((b)*11 + (g)*59 + (r)*30) / 100)
#define FXDIB_ALPHA_MERGE(backdrop, source, source_alpha) \
  (((backdrop) * (255 - (source_alpha)) + (source) * (source_alpha)) / 255)

#define FXCMYK_TODIB(cmyk)                                    \
  ((uint8_t)((cmyk) >> 24) | ((uint8_t)((cmyk) >> 16)) << 8 | \
   ((uint8_t)((cmyk) >> 8)) << 16 | ((uint8_t)(cmyk) << 24))
#define FXARGB_TOBGRORDERDIB(argb)                       \
  ((uint8_t)(argb >> 16) | ((uint8_t)(argb >> 8)) << 8 | \
   ((uint8_t)(argb)) << 16 | ((uint8_t)(argb >> 24) << 24))

// SAFETY: Caller must ensure 4 valid bytes at `p`.
UNSAFE_BUFFER_USAGE inline FX_ARGB FXARGB_GetDIB(const uint8_t* p) {
  return ArgbEncode(UNSAFE_BUFFERS(p[3]), UNSAFE_BUFFERS(p[2]),
                    UNSAFE_BUFFERS(p[1]), UNSAFE_BUFFERS(p[0]));
}

// SAFETY: Caller must ensure 4 valid bytes at `p`.
UNSAFE_BUFFER_USAGE inline void FXARGB_SetDIB(uint8_t* p, uint32_t argb) {
  UNSAFE_BUFFERS(p[0]) = FXARGB_B(argb);
  UNSAFE_BUFFERS(p[1]) = FXARGB_G(argb);
  UNSAFE_BUFFERS(p[2]) = FXARGB_R(argb);
  UNSAFE_BUFFERS(p[3]) = FXARGB_A(argb);
}

// SAFETY: Caller must ensure 4 valid bytes at `p`.
UNSAFE_BUFFER_USAGE inline void FXARGB_SetRGBOrderDIB(uint8_t* p,
                                                      uint32_t argb) {
  UNSAFE_BUFFERS(p[0]) = FXARGB_R(argb);
  UNSAFE_BUFFERS(p[1]) = FXARGB_G(argb);
  UNSAFE_BUFFERS(p[2]) = FXARGB_B(argb);
  UNSAFE_BUFFERS(p[3]) = FXARGB_A(argb);
}

// SAFETY: Caller must ensure 3 valid bytes at `dest` and `src`.
UNSAFE_BUFFER_USAGE inline void ReverseCopy3Bytes(uint8_t* dest,
                                                  const uint8_t* src) {
  UNSAFE_BUFFERS(dest[2] = src[0]);
  UNSAFE_BUFFERS(dest[1] = src[1]);
  UNSAFE_BUFFERS(dest[0] = src[2]);
}

#if defined(PDF_USE_SKIA)
template <typename T>
T PreMultiplyColor(const T& input) {
  if (input.alpha == 255) {
    return input;
  }

  T output;
  output.alpha = input.alpha;
  output.blue = static_cast<float>(input.blue) * input.alpha / 255.0f;
  output.green = static_cast<float>(input.green) * input.alpha / 255.0f;
  output.red = static_cast<float>(input.red) * input.alpha / 255.0f;
  return output;
}

template <typename T>
T UnPreMultiplyColor(const T& input) {
  if (input.alpha == 255) {
    return input;
  }

  T output;
  output.alpha = input.alpha;
  if (input.alpha == 0) {
    output.blue = 0;
    output.green = 0;
    output.red = 0;
  } else {
    output.blue = static_cast<float>(input.blue) * 255.0f / input.alpha;
    output.green = static_cast<float>(input.green) * 255.0f / input.alpha;
    output.red = static_cast<float>(input.red) * 255.0f / input.alpha;
  }
  return output;
}
#endif  // defined(PDF_USE_SKIA)

#endif  // CORE_FXGE_DIB_FX_DIB_H_
