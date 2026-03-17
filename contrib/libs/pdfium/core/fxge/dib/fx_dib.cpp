// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxge/dib/fx_dib.h"

#include <tuple>
#include <type_traits>
#include <utility>

#include "build/build_config.h"

#if BUILDFLAG(IS_WIN)
#include <windows.h>
#endif

#if BUILDFLAG(IS_WIN)
static_assert(sizeof(FX_COLORREF) == sizeof(COLORREF),
              "FX_COLORREF vs. COLORREF mismatch");
#endif

// Assert that FX_*_STRUCTS are packed.
static_assert(sizeof(FX_RGB_STRUCT<uint8_t>) == 3u);
static_assert(sizeof(FX_BGR_STRUCT<uint8_t>) == 3u);
static_assert(sizeof(FX_ARGB_STRUCT<uint8_t>) == 4u);
static_assert(sizeof(FX_ABGR_STRUCT<uint8_t>) == 4u);
static_assert(sizeof(FX_RGBA_STRUCT<uint8_t>) == 4u);
static_assert(sizeof(FX_BGRA_STRUCT<uint8_t>) == 4u);
static_assert(sizeof(FX_CMYK_STRUCT<uint8_t>) == 4u);

// Assert that FX_*_STRUCTS remain aggregates.
static_assert(std::is_aggregate_v<FX_RGB_STRUCT<float>>);
static_assert(std::is_aggregate_v<FX_BGR_STRUCT<float>>);
static_assert(std::is_aggregate_v<FX_ARGB_STRUCT<float>>);
static_assert(std::is_aggregate_v<FX_ABGR_STRUCT<float>>);
static_assert(std::is_aggregate_v<FX_RGBA_STRUCT<float>>);
static_assert(std::is_aggregate_v<FX_BGRA_STRUCT<float>>);
static_assert(std::is_aggregate_v<FX_CMYK_STRUCT<float>>);

FXDIB_ResampleOptions::FXDIB_ResampleOptions() = default;

bool FXDIB_ResampleOptions::HasAnyOptions() const {
  return bInterpolateBilinear || bHalftone || bNoSmoothing || bLossy;
}

FX_BGRA_STRUCT<uint8_t> ArgbToBGRAStruct(FX_ARGB argb) {
  return {FXARGB_B(argb), FXARGB_G(argb), FXARGB_R(argb), FXARGB_A(argb)};
}

FX_BGR_STRUCT<uint8_t> ArgbToBGRStruct(FX_ARGB argb) {
  return {FXARGB_B(argb), FXARGB_G(argb), FXARGB_R(argb)};
}

std::pair<uint8_t, FX_COLORREF> ArgbToAlphaAndColorRef(FX_ARGB argb) {
  return {FXARGB_A(argb), ArgbToColorRef(argb)};
}

FX_COLORREF ArgbToColorRef(FX_ARGB argb) {
  return FXSYS_BGR(FXARGB_B(argb), FXARGB_G(argb), FXARGB_R(argb));
}

FX_ARGB AlphaAndColorRefToArgb(int a, FX_COLORREF colorref) {
  return ArgbEncode(a, FXSYS_GetRValue(colorref), FXSYS_GetGValue(colorref),
                    FXSYS_GetBValue(colorref));
}
