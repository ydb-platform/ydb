// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxge/dib/cfx_scanlinecompositor.h"

#include <algorithm>

#include "core/fxcrt/check.h"
#include "core/fxcrt/check_op.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/fx_memcpy_wrappers.h"
#include "core/fxcrt/notreached.h"
#include "core/fxcrt/span_util.h"
#include "core/fxcrt/stl_util.h"
#include "core/fxcrt/zip.h"
#include "core/fxge/dib/blend.h"
#include "core/fxge/dib/fx_dib.h"

using fxge::Blend;

namespace {

uint8_t AlphaUnion(uint8_t dest, uint8_t src) {
  return dest + src - dest * src / 255;
}

int Lum(FX_RGB_STRUCT<int> color) {
  return (color.red * 30 + color.green * 59 + color.blue * 11) / 100;
}

FX_RGB_STRUCT<int> ClipColor(FX_RGB_STRUCT<int> color) {
  int l = Lum(color);
  int n = std::min(color.red, std::min(color.green, color.blue));
  int x = std::max(color.red, std::max(color.green, color.blue));
  if (n < 0) {
    color.red = l + ((color.red - l) * l / (l - n));
    color.green = l + ((color.green - l) * l / (l - n));
    color.blue = l + ((color.blue - l) * l / (l - n));
  }
  if (x > 255) {
    color.red = l + ((color.red - l) * (255 - l) / (x - l));
    color.green = l + ((color.green - l) * (255 - l) / (x - l));
    color.blue = l + ((color.blue - l) * (255 - l) / (x - l));
  }
  return color;
}

FX_RGB_STRUCT<int> SetLum(FX_RGB_STRUCT<int> color, int l) {
  int d = l - Lum(color);
  color.red += d;
  color.green += d;
  color.blue += d;
  return ClipColor(color);
}

int Sat(FX_RGB_STRUCT<int> color) {
  return std::max(color.red, std::max(color.green, color.blue)) -
         std::min(color.red, std::min(color.green, color.blue));
}

FX_RGB_STRUCT<int> SetSat(FX_RGB_STRUCT<int> color, int s) {
  int min = std::min(color.red, std::min(color.green, color.blue));
  int max = std::max(color.red, std::max(color.green, color.blue));
  if (min == max)
    return {};

  color.red = (color.red - min) * s / (max - min);
  color.green = (color.green - min) * s / (max - min);
  color.blue = (color.blue - min) * s / (max - min);
  return color;
}

template <typename T, typename U>
FX_RGB_STRUCT<int> RgbBlend(BlendMode blend_type,
                            const T& src_in,
                            const U& back_in) {
  FX_RGB_STRUCT<int> src = {
      .red = src_in.red, .green = src_in.green, .blue = src_in.blue};
  FX_RGB_STRUCT<int> back = {
      .red = back_in.red, .green = back_in.green, .blue = back_in.blue};
  FX_RGB_STRUCT<int> result;
  switch (blend_type) {
    case BlendMode::kHue:
      result = SetLum(SetSat(src, Sat(back)), Lum(back));
      break;
    case BlendMode::kSaturation:
      result = SetLum(SetSat(back, Sat(src)), Lum(back));
      break;
    case BlendMode::kColor:
      result = SetLum(src, Lum(back));
      break;
    case BlendMode::kLuminosity:
      result = SetLum(back, Lum(src));
      break;
    default:
      break;
  }
  return result;
}

// Prefer RgbBlend() above in new code.
void RGB_Blend(BlendMode blend_mode,
               const uint8_t* src_scan,
               const uint8_t* dest_scan,
               int results[3]) {
  UNSAFE_TODO({
    FX_BGR_STRUCT<uint8_t> src = {
        .blue = src_scan[0], .green = src_scan[1], .red = src_scan[2]};
    FX_BGR_STRUCT<uint8_t> back = {
        .blue = dest_scan[0], .green = dest_scan[1], .red = dest_scan[2]};
    FX_RGB_STRUCT<int> result = RgbBlend(blend_mode, src, back);
    results[0] = result.blue;
    results[1] = result.green;
    results[2] = result.red;
  });
}

int GetAlpha(uint8_t src_alpha, const uint8_t* clip_scan, int col) {
  return clip_scan ? UNSAFE_TODO(clip_scan[col]) * src_alpha / 255 : src_alpha;
}

int GetAlphaWithSrc(uint8_t src_alpha,
                    pdfium::span<const uint8_t> clip_scan,
                    pdfium::span<const uint8_t> src_scan,
                    size_t col) {
  int result = src_alpha * src_scan[col];
  if (col < clip_scan.size()) {
    result *= clip_scan[col];
    result /= 255;
  }
  return result / 255;
}

template <typename T, typename U>
void AlphaMergeToDest(const T& input, U& output, uint8_t alpha) {
  output.blue = FXDIB_ALPHA_MERGE(output.blue, input.blue, alpha);
  output.green = FXDIB_ALPHA_MERGE(output.green, input.green, alpha);
  output.red = FXDIB_ALPHA_MERGE(output.red, input.red, alpha);
}

#if defined(PDF_USE_SKIA)
template <typename T, typename U>
void AlphaMergeToDestPremul(const T& input, U& output) {
  const int in_alpha = 255 - input.alpha;
  const int out_alpha = 255 - output.alpha;
  output.blue = (output.blue * in_alpha + input.blue * out_alpha) / 255;
  output.green = (output.green * in_alpha + input.green * out_alpha) / 255;
  output.red = (output.red * in_alpha + input.red * out_alpha) / 255;
}
#endif  // defined(PDF_USE_SKIA)

template <typename T, typename U>
void AlphaMergeToSource(const T& input, U& output, uint8_t alpha) {
  output.blue = FXDIB_ALPHA_MERGE(input.blue, output.blue, alpha);
  output.green = FXDIB_ALPHA_MERGE(input.green, output.green, alpha);
  output.red = FXDIB_ALPHA_MERGE(input.red, output.red, alpha);
}

void CompositePixelBgra2Mask(const FX_BGRA_STRUCT<uint8_t>& input,
                             uint8_t clip,
                             uint8_t& output) {
  const uint8_t src_alpha = input.alpha * clip / 255;
  if (output == 0) {
    output = src_alpha;
    return;
  }
  if (src_alpha == 0) {
    return;
  }
  output = AlphaUnion(output, src_alpha);
}

void CompositeRowBgra2Mask(pdfium::span<const FX_BGRA_STRUCT<uint8_t>> src_span,
                           pdfium::span<const uint8_t> clip_span,
                           pdfium::span<uint8_t> dest_span) {
  if (clip_span.empty()) {
    for (auto [input, output] : fxcrt::Zip(src_span, dest_span)) {
      CompositePixelBgra2Mask(input, /*clip=*/255, output);
    }
    return;
  }

  for (auto [input, clip, output] :
       fxcrt::Zip(src_span, clip_span, dest_span)) {
    CompositePixelBgra2Mask(input, clip, output);
  }
}

void CompositeRow_Rgb2Mask(pdfium::span<uint8_t> dest_span,
                           int width,
                           pdfium::span<const uint8_t> clip_span) {
  uint8_t* dest_scan = dest_span.data();
  const uint8_t* clip_scan = clip_span.data();
  UNSAFE_TODO({
    if (!clip_scan) {
      FXSYS_memset(dest_scan, 0xff, width);
      return;
    }
    for (int i = 0; i < width; ++i) {
      *dest_scan = AlphaUnion(*dest_scan, *clip_scan);
      ++dest_scan;
      ++clip_scan;
    }
  });
}

bool IsNonSeparableBlendMode(BlendMode mode) {
  switch (mode) {
    case BlendMode::kHue:
    case BlendMode::kSaturation:
    case BlendMode::kColor:
    case BlendMode::kLuminosity:
      return true;
    default:
      return false;
  }
}

template <typename T>
uint8_t GetGrayWithBlend(const T& input,
                         uint8_t output_value,
                         BlendMode blend_type) {
  uint8_t gray = FXRGB2GRAY(input.red, input.green, input.blue);
  if (IsNonSeparableBlendMode(blend_type)) {
    return blend_type == BlendMode::kLuminosity ? gray : output_value;
  }
  if (blend_type != BlendMode::kNormal) {
    return Blend(blend_type, output_value, gray);
  }
  return gray;
}

void CompositePixelBgra2Gray(const FX_BGRA_STRUCT<uint8_t>& input,
                             uint8_t clip,
                             uint8_t& output,
                             BlendMode blend_type) {
  const uint8_t src_alpha = input.alpha * clip / 255;
  if (src_alpha == 0) {
    return;
  }

  uint8_t gray = GetGrayWithBlend(input, output, blend_type);
  output = FXDIB_ALPHA_MERGE(output, gray, src_alpha);
}

void CompositeRowBgra2Gray(pdfium::span<const FX_BGRA_STRUCT<uint8_t>> src_span,
                           pdfium::span<const uint8_t> clip_span,
                           pdfium::span<uint8_t> dest_span,
                           BlendMode blend_type) {
  if (clip_span.empty()) {
    for (auto [input, output] : fxcrt::Zip(src_span, dest_span)) {
      CompositePixelBgra2Gray(input, /*clip=*/255, output, blend_type);
    }
    return;
  }

  for (auto [input, clip, output] :
       fxcrt::Zip(src_span, clip_span, dest_span)) {
    CompositePixelBgra2Gray(input, clip, output, blend_type);
  }
}

void CompositeRow_Rgb2Gray(pdfium::span<uint8_t> dest_span,
                           pdfium::span<const uint8_t> src_span,
                           int src_Bpp,
                           int pixel_count,
                           BlendMode blend_type,
                           pdfium::span<const uint8_t> clip_span) {
  uint8_t* dest_scan = dest_span.data();
  const uint8_t* src_scan = src_span.data();
  const uint8_t* clip_scan = clip_span.data();
  UNSAFE_TODO({
    for (int col = 0; col < pixel_count; ++col) {
      FX_BGR_STRUCT<uint8_t> input = {
          .blue = src_scan[0], .green = src_scan[1], .red = src_scan[2]};
      uint8_t gray = GetGrayWithBlend(input, *dest_scan, blend_type);
      if (clip_scan && clip_scan[col] < 255) {
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, gray, clip_scan[col]);
      } else {
        *dest_scan = gray;
      }
      ++dest_scan;
      src_scan += src_Bpp;
    }
  });
}

void CompositeRow_Bgr2Bgra_Blend_NoClip(pdfium::span<uint8_t> dest_span,
                                        pdfium::span<const uint8_t> src_span,
                                        int width,
                                        BlendMode blend_type,
                                        int src_Bpp) {
  uint8_t* dest_scan = dest_span.data();
  const uint8_t* src_scan = src_span.data();
  int blended_colors[3];
  bool bNonseparableBlend = IsNonSeparableBlendMode(blend_type);
  int src_gap = src_Bpp - 3;
  UNSAFE_TODO({
    for (int col = 0; col < width; ++col) {
      uint8_t* dest_alpha = &dest_scan[3];
      uint8_t back_alpha = *dest_alpha;
      if (back_alpha == 0) {
        if (src_Bpp == 4) {
          FXARGB_SetDIB(dest_scan, 0xff000000 | FXARGB_GetDIB(src_scan));
        } else {
          FXARGB_SetDIB(dest_scan, ArgbEncode(0xff, src_scan[2], src_scan[1],
                                              src_scan[0]));
        }
        dest_scan += 4;
        src_scan += src_Bpp;
        continue;
      }
      *dest_alpha = 0xff;
      if (bNonseparableBlend) {
        RGB_Blend(blend_type, src_scan, dest_scan, blended_colors);
      }
      for (int color = 0; color < 3; ++color) {
        int src_color = *src_scan;
        int blended = bNonseparableBlend
                          ? blended_colors[color]
                          : Blend(blend_type, *dest_scan, src_color);
        *dest_scan = FXDIB_ALPHA_MERGE(src_color, blended, back_alpha);
        ++dest_scan;
        ++src_scan;
      }
      ++dest_scan;
      src_scan += src_gap;
    }
  });
}

void CompositeRow_Bgr2Bgra_Blend_Clip(pdfium::span<uint8_t> dest_span,
                                      pdfium::span<const uint8_t> src_span,
                                      int width,
                                      BlendMode blend_type,
                                      int src_Bpp,
                                      pdfium::span<const uint8_t> clip_span) {
  uint8_t* dest_scan = dest_span.data();
  const uint8_t* src_scan = src_span.data();
  const uint8_t* clip_scan = clip_span.data();
  int blended_colors[3];
  bool bNonseparableBlend = IsNonSeparableBlendMode(blend_type);
  int src_gap = src_Bpp - 3;
  UNSAFE_TODO({
    for (int col = 0; col < width; ++col) {
      int src_alpha = *clip_scan++;
      uint8_t back_alpha = dest_scan[3];
      if (back_alpha == 0) {
        FXSYS_memcpy(dest_scan, src_scan, 3);
        dest_scan += 3;
        src_scan += src_Bpp;
        dest_scan++;
        continue;
      }
      if (src_alpha == 0) {
        dest_scan += 4;
        src_scan += src_Bpp;
        continue;
      }
      uint8_t dest_alpha = AlphaUnion(back_alpha, src_alpha);
      dest_scan[3] = dest_alpha;
      int alpha_ratio = src_alpha * 255 / dest_alpha;
      if (bNonseparableBlend) {
        RGB_Blend(blend_type, src_scan, dest_scan, blended_colors);
      }
      for (int color = 0; color < 3; color++) {
        int src_color = *src_scan;
        int blended = bNonseparableBlend
                          ? blended_colors[color]
                          : Blend(blend_type, *dest_scan, src_color);
        blended = FXDIB_ALPHA_MERGE(src_color, blended, back_alpha);
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, blended, alpha_ratio);
        dest_scan++;
        src_scan++;
      }
      src_scan += src_gap;
      dest_scan++;
    }
  });
}

void CompositeRow_Bgr2Bgra_NoBlend_Clip(pdfium::span<uint8_t> dest_span,
                                        pdfium::span<const uint8_t> src_span,
                                        int width,
                                        int src_Bpp,
                                        pdfium::span<const uint8_t> clip_span) {
  uint8_t* dest_scan = dest_span.data();
  const uint8_t* src_scan = src_span.data();
  const uint8_t* clip_scan = clip_span.data();
  int src_gap = src_Bpp - 3;
  UNSAFE_TODO({
    for (int col = 0; col < width; col++) {
      int src_alpha = clip_scan[col];
      if (src_alpha == 255) {
        FXSYS_memcpy(dest_scan, src_scan, 3);
        dest_scan += 3;
        *dest_scan++ = 255;
        src_scan += src_Bpp;
        continue;
      }
      if (src_alpha == 0) {
        dest_scan += 4;
        src_scan += src_Bpp;
        continue;
      }
      int back_alpha = dest_scan[3];
      uint8_t dest_alpha = AlphaUnion(back_alpha, src_alpha);
      dest_scan[3] = dest_alpha;
      int alpha_ratio = src_alpha * 255 / dest_alpha;
      for (int color = 0; color < 3; color++) {
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, *src_scan, alpha_ratio);
        dest_scan++;
        src_scan++;
      }
      dest_scan++;
      src_scan += src_gap;
    }
  });
}

void CompositeRow_Bgr2Bgra_NoBlend_NoClip(pdfium::span<uint8_t> dest_span,
                                          pdfium::span<const uint8_t> src_span,
                                          int width,
                                          int src_Bpp) {
  uint8_t* dest_scan = dest_span.data();
  const uint8_t* src_scan = src_span.data();
  UNSAFE_TODO({
    for (int col = 0; col < width; col++) {
      if (src_Bpp == 4) {
        FXARGB_SetDIB(dest_scan, 0xff000000 | FXARGB_GetDIB(src_scan));
      } else {
        FXARGB_SetDIB(dest_scan,
                      ArgbEncode(0xff, src_scan[2], src_scan[1], src_scan[0]));
      }
      dest_scan += 4;
      src_scan += src_Bpp;
    }
  });
}

template <typename DestPixelStruct>
void CompositePixelBgra2BgrNonSeparableBlend(
    const FX_BGRA_STRUCT<uint8_t>& input,
    uint8_t clip,
    DestPixelStruct& output,
    BlendMode blend_type) {
  const uint8_t src_alpha = input.alpha * clip / 255;
  if (src_alpha == 0) {
    return;
  }

  FX_RGB_STRUCT<int> blended_color = RgbBlend(blend_type, input, output);
  AlphaMergeToDest(blended_color, output, src_alpha);
}

template <typename DestPixelStruct>
void CompositePixelBgra2BgrBlend(const FX_BGRA_STRUCT<uint8_t>& input,
                                 uint8_t clip,
                                 DestPixelStruct& output,
                                 BlendMode blend_type) {
  const uint8_t src_alpha = input.alpha * clip / 255;
  if (src_alpha == 0) {
    return;
  }

  FX_RGB_STRUCT<int> blended_color = {
      .red = Blend(blend_type, output.red, input.red),
      .green = Blend(blend_type, output.green, input.green),
      .blue = Blend(blend_type, output.blue, input.blue),
  };
  AlphaMergeToDest(blended_color, output, src_alpha);
}

template <typename DestPixelStruct>
void CompositePixelBgra2BgrNoBlend(const FX_BGRA_STRUCT<uint8_t>& input,
                                   uint8_t clip,
                                   DestPixelStruct& output) {
  const uint8_t src_alpha = input.alpha * clip / 255;
  if (src_alpha == 255) {
    output.blue = input.blue;
    output.green = input.green;
    output.red = input.red;
    return;
  }
  if (src_alpha == 0) {
    return;
  }
  AlphaMergeToDest(input, output, src_alpha);
}

template <typename DestPixelStruct>
void CompositeRowBgra2Bgr(pdfium::span<const FX_BGRA_STRUCT<uint8_t>> src_span,
                          pdfium::span<const uint8_t> clip_span,
                          pdfium::span<DestPixelStruct> dest_span,
                          BlendMode blend_type) {
  const bool non_separable_blend = IsNonSeparableBlendMode(blend_type);
  if (clip_span.empty()) {
    if (non_separable_blend) {
      for (auto [input, output] : fxcrt::Zip(src_span, dest_span)) {
        CompositePixelBgra2BgrNonSeparableBlend(input, /*clip=*/255, output,
                                                blend_type);
      }
      return;
    }
    if (blend_type != BlendMode::kNormal) {
      for (auto [input, output] : fxcrt::Zip(src_span, dest_span)) {
        CompositePixelBgra2BgrBlend(input, /*clip=*/255, output, blend_type);
      }
      return;
    }
    for (auto [input, output] : fxcrt::Zip(src_span, dest_span)) {
      CompositePixelBgra2BgrNoBlend(input, /*clip=*/255, output);
    }
    return;
  }

  if (non_separable_blend) {
    for (auto [input, clip, output] :
         fxcrt::Zip(src_span, clip_span, dest_span)) {
      CompositePixelBgra2BgrNonSeparableBlend(input, clip, output, blend_type);
    }
    return;
  }
  if (blend_type != BlendMode::kNormal) {
    for (auto [input, clip, output] :
         fxcrt::Zip(src_span, clip_span, dest_span)) {
      CompositePixelBgra2BgrBlend(input, clip, output, blend_type);
    }
    return;
  }
  for (auto [input, clip, output] :
       fxcrt::Zip(src_span, clip_span, dest_span)) {
    CompositePixelBgra2BgrNoBlend(input, clip, output);
  }
}

// Returns 0 when no further work is required by the caller. Otherwise, returns
// `src_alpha` and the caller needs to use that to call one of the
// CompositePixelBgra2Bgra*Blend() functions.
template <typename DestPixelStruct>
uint8_t CompositePixelBgra2BgraCommon(const FX_BGRA_STRUCT<uint8_t>& input,
                                      uint8_t clip,
                                      DestPixelStruct& output) {
  const uint8_t src_alpha = input.alpha * clip / 255;
  if (output.alpha != 0) {
    return src_alpha;
  }

  output.blue = input.blue;
  output.green = input.green;
  output.red = input.red;
  output.alpha = src_alpha;
  return 0;
}

template <typename DestPixelStruct>
void CompositePixelBgra2BgraNonSeparableBlend(
    const FX_BGRA_STRUCT<uint8_t>& input,
    uint8_t src_alpha,
    DestPixelStruct& output,
    BlendMode blend_type) {
  const uint8_t dest_alpha = AlphaUnion(output.alpha, src_alpha);
  const int alpha_ratio = src_alpha * 255 / dest_alpha;
  FX_RGB_STRUCT<int> blended_color = RgbBlend(blend_type, input, output);
  AlphaMergeToSource(input, blended_color, output.alpha);
  AlphaMergeToDest(blended_color, output, alpha_ratio);
  output.alpha = dest_alpha;
}

template <typename DestPixelStruct>
void CompositePixelBgra2BgraBlend(const FX_BGRA_STRUCT<uint8_t>& input,
                                  uint8_t src_alpha,
                                  DestPixelStruct& output,
                                  BlendMode blend_type) {
  const uint8_t dest_alpha = AlphaUnion(output.alpha, src_alpha);
  const int alpha_ratio = src_alpha * 255 / dest_alpha;
  FX_RGB_STRUCT<int> blended_color = {
      .red = Blend(blend_type, output.red, input.red),
      .green = Blend(blend_type, output.green, input.green),
      .blue = Blend(blend_type, output.blue, input.blue),
  };
  AlphaMergeToSource(input, blended_color, output.alpha);
  AlphaMergeToDest(blended_color, output, alpha_ratio);
  output.alpha = dest_alpha;
}

template <typename DestPixelStruct>
void CompositePixelBgra2BgraNoBlend(const FX_BGRA_STRUCT<uint8_t>& input,
                                    uint8_t src_alpha,
                                    DestPixelStruct& output) {
  const uint8_t dest_alpha = AlphaUnion(output.alpha, src_alpha);
  const int alpha_ratio = src_alpha * 255 / dest_alpha;
  AlphaMergeToDest(input, output, alpha_ratio);
  output.alpha = dest_alpha;
}

template <typename DestPixelStruct>
void CompositeRowBgra2Bgra(pdfium::span<const FX_BGRA_STRUCT<uint8_t>> src_span,
                           pdfium::span<const uint8_t> clip_span,
                           pdfium::span<DestPixelStruct> dest_span,
                           BlendMode blend_type) {
  const bool non_separable_blend = IsNonSeparableBlendMode(blend_type);
  if (clip_span.empty()) {
    if (non_separable_blend) {
      for (auto [input, output] : fxcrt::Zip(src_span, dest_span)) {
        const uint8_t src_alpha =
            CompositePixelBgra2BgraCommon(input, /*clip=*/255, output);
        if (src_alpha != 0) {
          CompositePixelBgra2BgraNonSeparableBlend(input, src_alpha, output,
                                                   blend_type);
        }
      }
      return;
    }
    if (blend_type != BlendMode::kNormal) {
      for (auto [input, output] : fxcrt::Zip(src_span, dest_span)) {
        const uint8_t src_alpha =
            CompositePixelBgra2BgraCommon(input, /*clip=*/255, output);
        if (src_alpha != 0) {
          CompositePixelBgra2BgraBlend(input, src_alpha, output, blend_type);
        }
      }
      return;
    }
    for (auto [input, output] : fxcrt::Zip(src_span, dest_span)) {
      const uint8_t src_alpha =
          CompositePixelBgra2BgraCommon(input, /*clip=*/255, output);
      if (src_alpha != 0) {
        CompositePixelBgra2BgraNoBlend(input, src_alpha, output);
      }
    }
    return;
  }

  if (non_separable_blend) {
    for (auto [input, clip, output] :
         fxcrt::Zip(src_span, clip_span, dest_span)) {
      const uint8_t src_alpha =
          CompositePixelBgra2BgraCommon(input, clip, output);
      if (src_alpha != 0) {
        CompositePixelBgra2BgraNonSeparableBlend(input, src_alpha, output,
                                                 blend_type);
      }
    }
    return;
  }
  if (blend_type != BlendMode::kNormal) {
    for (auto [input, clip, output] :
         fxcrt::Zip(src_span, clip_span, dest_span)) {
      const uint8_t src_alpha =
          CompositePixelBgra2BgraCommon(input, clip, output);
      if (src_alpha != 0) {
        CompositePixelBgra2BgraBlend(input, src_alpha, output, blend_type);
      }
    }
    return;
  }
  for (auto [input, clip, output] :
       fxcrt::Zip(src_span, clip_span, dest_span)) {
    const uint8_t src_alpha =
        CompositePixelBgra2BgraCommon(input, clip, output);
    if (src_alpha != 0) {
      CompositePixelBgra2BgraNoBlend(input, src_alpha, output);
    }
  }
}

#if defined(PDF_USE_SKIA)
// Returns false when no further work is required by the caller. Otherwise,
// returns true and the caller needs to call one of the
// CompositePixelBgraPremul2BgraPremul*Blend() functions.
template <typename DestPixelStruct>
uint8_t CompositePixelBgraPremul2BgraPremulCommon(
    const FX_BGRA_STRUCT<uint8_t>& input,
    DestPixelStruct& output) {
  if (output.alpha != 0) {
    return true;
  }

  output.blue = input.blue;
  output.green = input.green;
  output.red = input.red;
  output.alpha = input.alpha;
  return false;
}

template <typename DestPixelStruct>
void CompositePixelBgraPremul2BgraPremulNonSeparableBlend(
    const FX_BGRA_STRUCT<uint8_t>& input,
    DestPixelStruct& output,
    BlendMode blend_type) {
  if (!CompositePixelBgraPremul2BgraPremulCommon(input, output)) {
    return;
  }

  FX_BGRA_STRUCT<uint8_t> input_for_blend;
  input_for_blend.blue = input.blue * output.alpha / 255;
  input_for_blend.green = input.green * output.alpha / 255;
  input_for_blend.red = input.red * output.alpha / 255;
  DestPixelStruct output_for_blend;
  output_for_blend.blue = output.blue * input.alpha / 255;
  output_for_blend.green = output.green * input.alpha / 255;
  output_for_blend.red = output.red * input.alpha / 255;
  FX_RGB_STRUCT<int> blended_color =
      RgbBlend(blend_type, input_for_blend, output_for_blend);

  AlphaMergeToDestPremul(input, output);
  output.blue += blended_color.blue;
  output.green += blended_color.green;
  output.red += blended_color.red;
  output.alpha = AlphaUnion(output.alpha, input.alpha);
}

template <typename DestPixelStruct>
void CompositePixelBgraPremul2BgraPremulBlend(
    const FX_BGRA_STRUCT<uint8_t>& input,
    DestPixelStruct& output,
    BlendMode blend_type) {
  if (!CompositePixelBgraPremul2BgraPremulCommon(input, output)) {
    return;
  }

  FX_BGR_STRUCT<int> blended_color = {
      .blue = Blend(blend_type, input.blue * output.alpha / 255,
                    output.blue * input.alpha / 255),
      .green = Blend(blend_type, input.green * output.alpha / 255,
                     output.green * input.alpha / 255),
      .red = Blend(blend_type, input.red * output.alpha / 255,
                   output.red * input.alpha / 255),
  };

  AlphaMergeToDestPremul(input, output);
  output.blue += blended_color.blue;
  output.green += blended_color.green;
  output.red += blended_color.red;
  output.alpha = AlphaUnion(output.alpha, input.alpha);
}

template <typename DestPixelStruct>
void CompositePixelBgraPremul2BgraPremulNoBlend(
    const FX_BGRA_STRUCT<uint8_t>& input,
    DestPixelStruct& output) {
  if (!CompositePixelBgraPremul2BgraPremulCommon(input, output)) {
    return;
  }

  const int in_alpha = 255 - input.alpha;
  output.blue = input.blue + output.blue * in_alpha / 255;
  output.green = input.green + output.green * in_alpha / 255;
  output.red = input.red + output.red * in_alpha / 255;
  output.alpha = AlphaUnion(output.alpha, input.alpha);
}

template <typename DestPixelStruct>
void CompositeRowBgraPremul2BgraPremul(
    pdfium::span<const FX_BGRA_STRUCT<uint8_t>> src_span,
    pdfium::span<DestPixelStruct> dest_span,
    BlendMode blend_type) {
  const bool non_separable_blend = IsNonSeparableBlendMode(blend_type);
  if (non_separable_blend) {
    for (auto [input, output] : fxcrt::Zip(src_span, dest_span)) {
      CompositePixelBgraPremul2BgraPremulNonSeparableBlend(input, output,
                                                           blend_type);
    }
    return;
  }
  if (blend_type != BlendMode::kNormal) {
    for (auto [input, output] : fxcrt::Zip(src_span, dest_span)) {
      CompositePixelBgraPremul2BgraPremulBlend(input, output, blend_type);
    }
    return;
  }
  for (auto [input, output] : fxcrt::Zip(src_span, dest_span)) {
    CompositePixelBgraPremul2BgraPremulNoBlend(input, output);
  }
}
#endif  // defined(PDF_USE_SKIA)

void CompositeRow_Rgb2Rgb_Blend_NoClip(pdfium::span<uint8_t> dest_span,
                                       pdfium::span<const uint8_t> src_span,
                                       int width,
                                       BlendMode blend_type,
                                       int dest_Bpp,
                                       int src_Bpp) {
  uint8_t* dest_scan = dest_span.data();
  const uint8_t* src_scan = src_span.data();
  int blended_colors[3];
  bool bNonseparableBlend = IsNonSeparableBlendMode(blend_type);
  int dest_gap = dest_Bpp - 3;
  int src_gap = src_Bpp - 3;
  UNSAFE_TODO({
    for (int col = 0; col < width; col++) {
      if (bNonseparableBlend) {
        RGB_Blend(blend_type, src_scan, dest_scan, blended_colors);
      }
      for (int color = 0; color < 3; color++) {
        int back_color = *dest_scan;
        int src_color = *src_scan;
        int blended = bNonseparableBlend
                          ? blended_colors[color]
                          : Blend(blend_type, back_color, src_color);
        *dest_scan = blended;
        dest_scan++;
        src_scan++;
      }
      dest_scan += dest_gap;
      src_scan += src_gap;
    }
  });
}

void CompositeRow_Rgb2Rgb_Blend_Clip(pdfium::span<uint8_t> dest_span,
                                     pdfium::span<const uint8_t> src_span,
                                     int width,
                                     BlendMode blend_type,
                                     int dest_Bpp,
                                     int src_Bpp,
                                     pdfium::span<const uint8_t> clip_span) {
  uint8_t* dest_scan = dest_span.data();
  const uint8_t* src_scan = src_span.data();
  const uint8_t* clip_scan = clip_span.data();
  int blended_colors[3];
  bool bNonseparableBlend = IsNonSeparableBlendMode(blend_type);
  int dest_gap = dest_Bpp - 3;
  int src_gap = src_Bpp - 3;
  UNSAFE_TODO({
    for (int col = 0; col < width; col++) {
      uint8_t src_alpha = *clip_scan++;
      if (src_alpha == 0) {
        dest_scan += dest_Bpp;
        src_scan += src_Bpp;
        continue;
      }
      if (bNonseparableBlend) {
        RGB_Blend(blend_type, src_scan, dest_scan, blended_colors);
      }
      for (int color = 0; color < 3; color++) {
        int src_color = *src_scan;
        int back_color = *dest_scan;
        int blended = bNonseparableBlend
                          ? blended_colors[color]
                          : Blend(blend_type, back_color, src_color);
        *dest_scan = FXDIB_ALPHA_MERGE(back_color, blended, src_alpha);
        dest_scan++;
        src_scan++;
      }
      dest_scan += dest_gap;
      src_scan += src_gap;
    }
  });
}

void CompositeRow_Rgb2Rgb_NoBlend_NoClip(pdfium::span<uint8_t> dest_span,
                                         pdfium::span<const uint8_t> src_span,
                                         int width,
                                         int dest_Bpp,
                                         int src_Bpp) {
  uint8_t* dest_scan = dest_span.data();
  const uint8_t* src_scan = src_span.data();
  UNSAFE_TODO({
    if (dest_Bpp == src_Bpp) {
      FXSYS_memcpy(dest_scan, src_scan, width * dest_Bpp);
      return;
    }
    for (int col = 0; col < width; col++) {
      FXSYS_memcpy(dest_scan, src_scan, 3);
      dest_scan += dest_Bpp;
      src_scan += src_Bpp;
    }
  });
}

void CompositeRow_Rgb2Rgb_NoBlend_Clip(pdfium::span<uint8_t> dest_span,
                                       pdfium::span<const uint8_t> src_span,
                                       int width,
                                       int dest_Bpp,
                                       int src_Bpp,
                                       pdfium::span<const uint8_t> clip_span) {
  uint8_t* dest_scan = dest_span.data();
  const uint8_t* src_scan = src_span.data();
  const uint8_t* clip_scan = clip_span.data();
  UNSAFE_TODO({
    for (int col = 0; col < width; col++) {
      int src_alpha = clip_scan[col];
      if (src_alpha == 255) {
        FXSYS_memcpy(dest_scan, src_scan, 3);
      } else if (src_alpha) {
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, *src_scan, src_alpha);
        dest_scan++;
        src_scan++;
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, *src_scan, src_alpha);
        dest_scan++;
        src_scan++;
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, *src_scan, src_alpha);
        dest_scan += dest_Bpp - 2;
        src_scan += src_Bpp - 2;
        continue;
      }
      dest_scan += dest_Bpp;
      src_scan += src_Bpp;
    }
  });
}

void CompositeRow_8bppPal2Gray(pdfium::span<uint8_t> dest_span,
                               pdfium::span<const uint8_t> src_span,
                               pdfium::span<const uint8_t> palette_span,
                               int pixel_count,
                               BlendMode blend_type,
                               pdfium::span<const uint8_t> clip_span) {
  uint8_t* dest_scan = dest_span.data();
  const uint8_t* src_scan = src_span.data();
  const uint8_t* clip_scan = clip_span.data();
  const uint8_t* pPalette = palette_span.data();
  UNSAFE_TODO({
    if (blend_type != BlendMode::kNormal) {
      bool bNonseparableBlend = IsNonSeparableBlendMode(blend_type);
      for (int col = 0; col < pixel_count; col++) {
        uint8_t gray = pPalette[*src_scan];
        if (bNonseparableBlend) {
          gray = blend_type == BlendMode::kLuminosity ? gray : *dest_scan;
        } else {
          gray = Blend(blend_type, *dest_scan, gray);
        }
        if (clip_scan && clip_scan[col] < 255) {
          *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, gray, clip_scan[col]);
        } else {
          *dest_scan = gray;
        }
        dest_scan++;
        src_scan++;
      }
      return;
    }
    for (int col = 0; col < pixel_count; col++) {
      uint8_t gray = pPalette[*src_scan];
      if (clip_scan && clip_scan[col] < 255)
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, gray, clip_scan[col]);
      else
        *dest_scan = gray;
      dest_scan++;
      src_scan++;
    }
  });
}

void CompositeRow_1bppPal2Gray(pdfium::span<uint8_t> dest_span,
                               pdfium::span<const uint8_t> src_span,
                               int src_left,
                               pdfium::span<const uint8_t> src_palette,
                               int pixel_count,
                               BlendMode blend_type,
                               pdfium::span<const uint8_t> clip_span) {
  uint8_t* dest_scan = dest_span.data();
  const uint8_t* src_scan = src_span.data();
  const uint8_t* clip_scan = clip_span.data();
  int reset_gray = src_palette[0];
  int set_gray = src_palette[1];
  UNSAFE_TODO({
    if (blend_type != BlendMode::kNormal) {
      bool bNonseparableBlend = IsNonSeparableBlendMode(blend_type);
      for (int col = 0; col < pixel_count; col++) {
        uint8_t gray =
            (src_scan[(col + src_left) / 8] & (1 << (7 - (col + src_left) % 8)))
                ? set_gray
                : reset_gray;
        if (bNonseparableBlend) {
          gray = blend_type == BlendMode::kLuminosity ? gray : *dest_scan;
        } else {
          gray = Blend(blend_type, *dest_scan, gray);
        }
        if (clip_scan && clip_scan[col] < 255) {
          *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, gray, clip_scan[col]);
        } else {
          *dest_scan = gray;
        }
        dest_scan++;
      }
      return;
    }
    for (int col = 0; col < pixel_count; col++) {
      uint8_t gray =
          (src_scan[(col + src_left) / 8] & (1 << (7 - (col + src_left) % 8)))
              ? set_gray
              : reset_gray;
      if (clip_scan && clip_scan[col] < 255) {
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, gray, clip_scan[col]);
      } else {
        *dest_scan = gray;
      }
      dest_scan++;
    }
  });
}

void CompositeRow_8bppRgb2Rgb_NoBlend(pdfium::span<uint8_t> dest_span,
                                      pdfium::span<const uint8_t> src_span,
                                      pdfium::span<const uint32_t> palette_span,
                                      int pixel_count,
                                      int DestBpp,
                                      pdfium::span<const uint8_t> clip_span) {
  uint8_t* dest_scan = dest_span.data();
  const uint8_t* src_scan = src_span.data();
  const uint8_t* clip_scan = clip_span.data();
  const uint32_t* pPalette = palette_span.data();
  FX_ARGB argb = 0;
  UNSAFE_TODO({
    for (int col = 0; col < pixel_count; col++) {
      argb = pPalette[*src_scan];
      int src_r = FXARGB_R(argb);
      int src_g = FXARGB_G(argb);
      int src_b = FXARGB_B(argb);
      if (clip_scan && clip_scan[col] < 255) {
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, src_b, clip_scan[col]);
        dest_scan++;
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, src_g, clip_scan[col]);
        dest_scan++;
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, src_r, clip_scan[col]);
        dest_scan++;
      } else {
        *dest_scan++ = src_b;
        *dest_scan++ = src_g;
        *dest_scan++ = src_r;
      }
      if (DestBpp == 4) {
        dest_scan++;
      }
      src_scan++;
    }
  });
}

void CompositeRow_1bppRgb2Rgb_NoBlend(pdfium::span<uint8_t> dest_span,
                                      pdfium::span<const uint8_t> src_span,
                                      int src_left,
                                      pdfium::span<const uint32_t> src_palette,
                                      int pixel_count,
                                      int DestBpp,
                                      pdfium::span<const uint8_t> clip_span) {
  uint8_t* dest_scan = dest_span.data();
  const uint8_t* src_scan = src_span.data();
  const uint8_t* clip_scan = clip_span.data();
  int reset_r = FXARGB_R(src_palette[0]);
  int reset_g = FXARGB_G(src_palette[0]);
  int reset_b = FXARGB_B(src_palette[0]);
  int set_r = FXARGB_R(src_palette[1]);
  int set_g = FXARGB_G(src_palette[1]);
  int set_b = FXARGB_B(src_palette[1]);
  UNSAFE_TODO({
    for (int col = 0; col < pixel_count; col++) {
      int src_r;
      int src_g;
      int src_b;
      if (src_scan[(col + src_left) / 8] & (1 << (7 - (col + src_left) % 8))) {
        src_r = set_r;
        src_g = set_g;
        src_b = set_b;
      } else {
        src_r = reset_r;
        src_g = reset_g;
        src_b = reset_b;
      }
      if (clip_scan && clip_scan[col] < 255) {
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, src_b, clip_scan[col]);
        dest_scan++;
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, src_g, clip_scan[col]);
        dest_scan++;
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, src_r, clip_scan[col]);
        dest_scan++;
      } else {
        *dest_scan++ = src_b;
        *dest_scan++ = src_g;
        *dest_scan++ = src_r;
      }
      if (DestBpp == 4) {
        dest_scan++;
      }
    }
  });
}

void CompositeRow_8bppBgr2Bgra_NoBlend(
    pdfium::span<uint8_t> dest_span,
    pdfium::span<const uint8_t> src_span,
    int width,
    pdfium::span<const uint32_t> palette_span,
    pdfium::span<const uint8_t> clip_span) {
  uint8_t* dest_scan = dest_span.data();
  const uint8_t* src_scan = src_span.data();
  const uint8_t* clip_scan = clip_span.data();
  const uint32_t* pPalette = palette_span.data();
  UNSAFE_TODO({
    for (int col = 0; col < width; col++) {
      FX_ARGB argb = pPalette[*src_scan];
      int src_r = FXARGB_R(argb);
      int src_g = FXARGB_G(argb);
      int src_b = FXARGB_B(argb);
      if (!clip_scan || clip_scan[col] == 255) {
        *dest_scan++ = src_b;
        *dest_scan++ = src_g;
        *dest_scan++ = src_r;
        *dest_scan++ = 255;
        src_scan++;
        continue;
      }
      int src_alpha = clip_scan[col];
      if (src_alpha == 0) {
        dest_scan += 4;
        src_scan++;
        continue;
      }
      int back_alpha = dest_scan[3];
      uint8_t dest_alpha = AlphaUnion(back_alpha, src_alpha);
      dest_scan[3] = dest_alpha;
      int alpha_ratio = src_alpha * 255 / dest_alpha;
      *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, src_b, alpha_ratio);
      dest_scan++;
      *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, src_g, alpha_ratio);
      dest_scan++;
      *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, src_r, alpha_ratio);
      dest_scan++;
      dest_scan++;
      src_scan++;
    }
  });
}

void CompositeRow_1bppBgr2Bgra_NoBlend(pdfium::span<uint8_t> dest_span,
                                       pdfium::span<const uint8_t> src_span,
                                       int src_left,
                                       int width,
                                       pdfium::span<const uint32_t> src_palette,
                                       pdfium::span<const uint8_t> clip_span) {
  uint8_t* dest_scan = dest_span.data();
  const uint8_t* src_scan = src_span.data();
  const uint8_t* clip_scan = clip_span.data();
  int reset_r = FXARGB_R(src_palette[0]);
  int reset_g = FXARGB_G(src_palette[0]);
  int reset_b = FXARGB_B(src_palette[0]);
  int set_r = FXARGB_R(src_palette[1]);
  int set_g = FXARGB_G(src_palette[1]);
  int set_b = FXARGB_B(src_palette[1]);
  UNSAFE_TODO({
    for (int col = 0; col < width; col++) {
      int src_r;
      int src_g;
      int src_b;
      if (src_scan[(col + src_left) / 8] & (1 << (7 - (col + src_left) % 8))) {
        src_r = set_r;
        src_g = set_g;
        src_b = set_b;
      } else {
        src_r = reset_r;
        src_g = reset_g;
        src_b = reset_b;
      }
      if (!clip_scan || clip_scan[col] == 255) {
        *dest_scan++ = src_b;
        *dest_scan++ = src_g;
        *dest_scan++ = src_r;
        *dest_scan++ = 255;
        continue;
      }
      int src_alpha = clip_scan[col];
      if (src_alpha == 0) {
        dest_scan += 4;
        continue;
      }
      int back_alpha = dest_scan[3];
      uint8_t dest_alpha = AlphaUnion(back_alpha, src_alpha);
      dest_scan[3] = dest_alpha;
      int alpha_ratio = src_alpha * 255 / dest_alpha;
      *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, src_b, alpha_ratio);
      dest_scan++;
      *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, src_g, alpha_ratio);
      dest_scan++;
      *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, src_r, alpha_ratio);
      dest_scan++;
      dest_scan++;
    }
  });
}

void CompositeRow_ByteMask2Bgra(pdfium::span<uint8_t> dest_span,
                                pdfium::span<const uint8_t> src_span,
                                int mask_alpha,
                                int src_r,
                                int src_g,
                                int src_b,
                                int pixel_count,
                                BlendMode blend_type,
                                pdfium::span<const uint8_t> clip_span) {
  uint8_t* dest_scan = dest_span.data();
  UNSAFE_TODO({
    for (int col = 0; col < pixel_count; col++) {
      int src_alpha = GetAlphaWithSrc(mask_alpha, clip_span, src_span, col);
      uint8_t back_alpha = dest_scan[3];
      if (back_alpha == 0) {
        FXARGB_SetDIB(dest_scan, ArgbEncode(src_alpha, src_r, src_g, src_b));
        dest_scan += 4;
        continue;
      }
      if (src_alpha == 0) {
        dest_scan += 4;
        continue;
      }
      uint8_t dest_alpha = AlphaUnion(back_alpha, src_alpha);
      dest_scan[3] = dest_alpha;
      int alpha_ratio = src_alpha * 255 / dest_alpha;
      if (IsNonSeparableBlendMode(blend_type)) {
        int blended_colors[3];
        uint8_t scan[3] = {static_cast<uint8_t>(src_b),
                           static_cast<uint8_t>(src_g),
                           static_cast<uint8_t>(src_r)};
        RGB_Blend(blend_type, scan, dest_scan, blended_colors);
        *dest_scan =
            FXDIB_ALPHA_MERGE(*dest_scan, blended_colors[0], alpha_ratio);
        dest_scan++;
        *dest_scan =
            FXDIB_ALPHA_MERGE(*dest_scan, blended_colors[1], alpha_ratio);
        dest_scan++;
        *dest_scan =
            FXDIB_ALPHA_MERGE(*dest_scan, blended_colors[2], alpha_ratio);
      } else if (blend_type != BlendMode::kNormal) {
        int blended = Blend(blend_type, *dest_scan, src_b);
        blended = FXDIB_ALPHA_MERGE(src_b, blended, back_alpha);
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, blended, alpha_ratio);
        dest_scan++;
        blended = Blend(blend_type, *dest_scan, src_g);
        blended = FXDIB_ALPHA_MERGE(src_g, blended, back_alpha);
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, blended, alpha_ratio);
        dest_scan++;
        blended = Blend(blend_type, *dest_scan, src_r);
        blended = FXDIB_ALPHA_MERGE(src_r, blended, back_alpha);
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, blended, alpha_ratio);
      } else {
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, src_b, alpha_ratio);
        dest_scan++;
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, src_g, alpha_ratio);
        dest_scan++;
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, src_r, alpha_ratio);
      }
      dest_scan += 2;
    }
  });
}

void CompositeRow_ByteMask2Rgb(pdfium::span<uint8_t> dest_span,
                               pdfium::span<const uint8_t> src_span,
                               int mask_alpha,
                               int src_r,
                               int src_g,
                               int src_b,
                               int pixel_count,
                               BlendMode blend_type,
                               int Bpp,
                               pdfium::span<const uint8_t> clip_span) {
  uint8_t* dest_scan = dest_span.data();
  UNSAFE_TODO({
    for (int col = 0; col < pixel_count; col++) {
      int src_alpha = GetAlphaWithSrc(mask_alpha, clip_span, src_span, col);
      if (src_alpha == 0) {
        dest_scan += Bpp;
        continue;
      }
      if (IsNonSeparableBlendMode(blend_type)) {
        int blended_colors[3];
        uint8_t scan[3] = {static_cast<uint8_t>(src_b),
                           static_cast<uint8_t>(src_g),
                           static_cast<uint8_t>(src_r)};
        RGB_Blend(blend_type, scan, dest_scan, blended_colors);
        *dest_scan =
            FXDIB_ALPHA_MERGE(*dest_scan, blended_colors[0], src_alpha);
        dest_scan++;
        *dest_scan =
            FXDIB_ALPHA_MERGE(*dest_scan, blended_colors[1], src_alpha);
        dest_scan++;
        *dest_scan =
            FXDIB_ALPHA_MERGE(*dest_scan, blended_colors[2], src_alpha);
      } else if (blend_type != BlendMode::kNormal) {
        int blended = Blend(blend_type, *dest_scan, src_b);
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, blended, src_alpha);
        dest_scan++;
        blended = Blend(blend_type, *dest_scan, src_g);
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, blended, src_alpha);
        dest_scan++;
        blended = Blend(blend_type, *dest_scan, src_r);
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, blended, src_alpha);
      } else {
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, src_b, src_alpha);
        dest_scan++;
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, src_g, src_alpha);
        dest_scan++;
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, src_r, src_alpha);
      }
      dest_scan += Bpp - 2;
    }
  });
}

void CompositeRow_ByteMask2Mask(pdfium::span<uint8_t> dest_span,
                                pdfium::span<const uint8_t> src_span,
                                int mask_alpha,
                                int pixel_count,
                                pdfium::span<const uint8_t> clip_span) {
  uint8_t* dest_scan = dest_span.data();
  for (int col = 0; col < pixel_count; col++) {
    int src_alpha = GetAlphaWithSrc(mask_alpha, clip_span, src_span, col);
    uint8_t back_alpha = *dest_scan;
    if (!back_alpha) {
      *dest_scan = src_alpha;
    } else if (src_alpha) {
      *dest_scan = AlphaUnion(back_alpha, src_alpha);
    }
    UNSAFE_TODO(dest_scan++);
  }
}

void CompositeRow_ByteMask2Gray(pdfium::span<uint8_t> dest_span,
                                pdfium::span<const uint8_t> src_span,
                                int mask_alpha,
                                int src_gray,
                                int pixel_count,
                                pdfium::span<const uint8_t> clip_span) {
  uint8_t* dest_scan = dest_span.data();
  for (int col = 0; col < pixel_count; col++) {
    int src_alpha = GetAlphaWithSrc(mask_alpha, clip_span, src_span, col);
    if (src_alpha) {
      *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, src_gray, src_alpha);
    }
    UNSAFE_TODO(dest_scan++);
  }
}

void CompositeRow_BitMask2Bgra(pdfium::span<uint8_t> dest_span,
                               pdfium::span<const uint8_t> src_span,
                               int mask_alpha,
                               int src_r,
                               int src_g,
                               int src_b,
                               int src_left,
                               int pixel_count,
                               BlendMode blend_type,
                               pdfium::span<const uint8_t> clip_span) {
  uint8_t* dest_scan = dest_span.data();
  const uint8_t* src_scan = src_span.data();
  const uint8_t* clip_scan = clip_span.data();
  UNSAFE_TODO({
    if (blend_type == BlendMode::kNormal && !clip_scan && mask_alpha == 255) {
      FX_ARGB argb = ArgbEncode(0xff, src_r, src_g, src_b);
      for (int col = 0; col < pixel_count; col++) {
        if (src_scan[(src_left + col) / 8] &
            (1 << (7 - (src_left + col) % 8))) {
          FXARGB_SetDIB(dest_scan, argb);
        }
        dest_scan += 4;
      }
      return;
    }
    for (int col = 0; col < pixel_count; col++) {
      if (!(src_scan[(src_left + col) / 8] &
            (1 << (7 - (src_left + col) % 8)))) {
        dest_scan += 4;
        continue;
      }
      int src_alpha = GetAlpha(mask_alpha, clip_scan, col);
      uint8_t back_alpha = dest_scan[3];
      if (back_alpha == 0) {
        FXARGB_SetDIB(dest_scan, ArgbEncode(src_alpha, src_r, src_g, src_b));
        dest_scan += 4;
        continue;
      }
      uint8_t dest_alpha = AlphaUnion(back_alpha, src_alpha);
      dest_scan[3] = dest_alpha;
      int alpha_ratio = src_alpha * 255 / dest_alpha;
      if (IsNonSeparableBlendMode(blend_type)) {
        int blended_colors[3];
        uint8_t scan[3] = {static_cast<uint8_t>(src_b),
                           static_cast<uint8_t>(src_g),
                           static_cast<uint8_t>(src_r)};
        RGB_Blend(blend_type, scan, dest_scan, blended_colors);
        *dest_scan =
            FXDIB_ALPHA_MERGE(*dest_scan, blended_colors[0], alpha_ratio);
        dest_scan++;
        *dest_scan =
            FXDIB_ALPHA_MERGE(*dest_scan, blended_colors[1], alpha_ratio);
        dest_scan++;
        *dest_scan =
            FXDIB_ALPHA_MERGE(*dest_scan, blended_colors[2], alpha_ratio);
      } else if (blend_type != BlendMode::kNormal) {
        int blended = Blend(blend_type, *dest_scan, src_b);
        blended = FXDIB_ALPHA_MERGE(src_b, blended, back_alpha);
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, blended, alpha_ratio);
        dest_scan++;
        blended = Blend(blend_type, *dest_scan, src_g);
        blended = FXDIB_ALPHA_MERGE(src_g, blended, back_alpha);
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, blended, alpha_ratio);
        dest_scan++;
        blended = Blend(blend_type, *dest_scan, src_r);
        blended = FXDIB_ALPHA_MERGE(src_r, blended, back_alpha);
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, blended, alpha_ratio);
      } else {
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, src_b, alpha_ratio);
        dest_scan++;
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, src_g, alpha_ratio);
        dest_scan++;
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, src_r, alpha_ratio);
      }
      dest_scan += 2;
    }
  });
}

void CompositeRow_BitMask2Rgb(pdfium::span<uint8_t> dest_span,
                              pdfium::span<const uint8_t> src_span,
                              int mask_alpha,
                              int src_r,
                              int src_g,
                              int src_b,
                              int src_left,
                              int pixel_count,
                              BlendMode blend_type,
                              int Bpp,
                              pdfium::span<const uint8_t> clip_span) {
  uint8_t* dest_scan = dest_span.data();
  const uint8_t* src_scan = src_span.data();
  const uint8_t* clip_scan = clip_span.data();
  UNSAFE_TODO({
    if (blend_type == BlendMode::kNormal && !clip_scan && mask_alpha == 255) {
      for (int col = 0; col < pixel_count; col++) {
        if (src_scan[(src_left + col) / 8] &
            (1 << (7 - (src_left + col) % 8))) {
          dest_scan[2] = src_r;
          dest_scan[1] = src_g;
          dest_scan[0] = src_b;
        }
        dest_scan += Bpp;
      }
      return;
    }
    for (int col = 0; col < pixel_count; col++) {
      if (!(src_scan[(src_left + col) / 8] &
            (1 << (7 - (src_left + col) % 8)))) {
        dest_scan += Bpp;
        continue;
      }
      int src_alpha = GetAlpha(mask_alpha, clip_scan, col);
      if (src_alpha == 0) {
        dest_scan += Bpp;
        continue;
      }
      if (IsNonSeparableBlendMode(blend_type)) {
        int blended_colors[3];
        uint8_t scan[3] = {static_cast<uint8_t>(src_b),
                           static_cast<uint8_t>(src_g),
                           static_cast<uint8_t>(src_r)};
        RGB_Blend(blend_type, scan, dest_scan, blended_colors);
        *dest_scan =
            FXDIB_ALPHA_MERGE(*dest_scan, blended_colors[0], src_alpha);
        dest_scan++;
        *dest_scan =
            FXDIB_ALPHA_MERGE(*dest_scan, blended_colors[1], src_alpha);
        dest_scan++;
        *dest_scan =
            FXDIB_ALPHA_MERGE(*dest_scan, blended_colors[2], src_alpha);
      } else if (blend_type != BlendMode::kNormal) {
        int blended = Blend(blend_type, *dest_scan, src_b);
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, blended, src_alpha);
        dest_scan++;
        blended = Blend(blend_type, *dest_scan, src_g);
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, blended, src_alpha);
        dest_scan++;
        blended = Blend(blend_type, *dest_scan, src_r);
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, blended, src_alpha);
      } else {
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, src_b, src_alpha);
        dest_scan++;
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, src_g, src_alpha);
        dest_scan++;
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, src_r, src_alpha);
      }
      dest_scan += Bpp - 2;
    }
  });
}

void CompositeRow_BitMask2Mask(pdfium::span<uint8_t> dest_span,
                               pdfium::span<const uint8_t> src_span,
                               int mask_alpha,
                               int src_left,
                               int pixel_count,
                               pdfium::span<const uint8_t> clip_span) {
  uint8_t* dest_scan = dest_span.data();
  const uint8_t* src_scan = src_span.data();
  const uint8_t* clip_scan = clip_span.data();
  UNSAFE_TODO({
    for (int col = 0; col < pixel_count; col++) {
      if (!(src_scan[(src_left + col) / 8] &
            (1 << (7 - (src_left + col) % 8)))) {
        dest_scan++;
        continue;
      }
      int src_alpha = GetAlpha(mask_alpha, clip_scan, col);
      uint8_t back_alpha = *dest_scan;
      if (!back_alpha) {
        *dest_scan = src_alpha;
      } else if (src_alpha) {
        *dest_scan = AlphaUnion(back_alpha, src_alpha);
      }
      dest_scan++;
    }
  });
}

void CompositeRow_BitMask2Gray(pdfium::span<uint8_t> dest_span,
                               pdfium::span<const uint8_t> src_span,
                               int mask_alpha,
                               int src_gray,
                               int src_left,
                               int pixel_count,
                               pdfium::span<const uint8_t> clip_span) {
  uint8_t* dest_scan = dest_span.data();
  const uint8_t* src_scan = src_span.data();
  const uint8_t* clip_scan = clip_span.data();
  UNSAFE_TODO({
    for (int col = 0; col < pixel_count; col++) {
      if (!(src_scan[(src_left + col) / 8] &
            (1 << (7 - (src_left + col) % 8)))) {
        dest_scan++;
        continue;
      }
      int src_alpha = GetAlpha(mask_alpha, clip_scan, col);
      if (src_alpha) {
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, src_gray, src_alpha);
      }
      dest_scan++;
    }
  });
}

void CompositeRow_Bgr2Bgra_Blend_NoClip_RgbByteOrder(
    pdfium::span<uint8_t> dest_span,
    pdfium::span<const uint8_t> src_span,
    int width,
    BlendMode blend_type,
    int src_Bpp) {
  uint8_t* dest_scan = dest_span.data();
  const uint8_t* src_scan = src_span.data();
  bool bNonseparableBlend = IsNonSeparableBlendMode(blend_type);
  int src_gap = src_Bpp - 3;
  int blended_colors[3];
  UNSAFE_TODO({
    for (int col = 0; col < width; col++) {
      uint8_t back_alpha = dest_scan[3];
      if (back_alpha == 0) {
        if (src_Bpp == 4) {
          FXARGB_SetRGBOrderDIB(dest_scan,
                                0xff000000 | FXARGB_GetDIB(src_scan));
        } else {
          FXARGB_SetRGBOrderDIB(
              dest_scan,
              ArgbEncode(0xff, src_scan[2], src_scan[1], src_scan[0]));
        }
        dest_scan += 4;
        src_scan += src_Bpp;
        continue;
      }
      dest_scan[3] = 0xff;
      if (bNonseparableBlend) {
        uint8_t dest_scan_o[3];
        ReverseCopy3Bytes(dest_scan_o, dest_scan);
        RGB_Blend(blend_type, src_scan, dest_scan_o, blended_colors);
      }
      for (int color = 0; color < 3; color++) {
        int index = 2 - color;
        int src_color = *src_scan;
        int blended = bNonseparableBlend
                          ? blended_colors[color]
                          : Blend(blend_type, dest_scan[index], src_color);
        dest_scan[index] = FXDIB_ALPHA_MERGE(src_color, blended, back_alpha);
        src_scan++;
      }
      dest_scan += 4;
      src_scan += src_gap;
    }
  });
}

void CompositeRow_Bgr2Bgra_NoBlend_NoClip_RgbByteOrder(
    pdfium::span<uint8_t> dest_span,
    pdfium::span<const uint8_t> src_span,
    int width,
    int src_Bpp) {
  uint8_t* dest_scan = dest_span.data();
  const uint8_t* src_scan = src_span.data();
  UNSAFE_TODO({
    for (int col = 0; col < width; col++) {
      if (src_Bpp == 4) {
        FXARGB_SetRGBOrderDIB(dest_scan, 0xff000000 | FXARGB_GetDIB(src_scan));
      } else {
        FXARGB_SetRGBOrderDIB(
            dest_scan, ArgbEncode(0xff, src_scan[2], src_scan[1], src_scan[0]));
      }
      dest_scan += 4;
      src_scan += src_Bpp;
    }
  });
}

void CompositeRow_Rgb2Rgb_Blend_NoClip_RgbByteOrder(
    pdfium::span<uint8_t> dest_span,
    pdfium::span<const uint8_t> src_span,
    int width,
    BlendMode blend_type,
    int dest_Bpp,
    int src_Bpp) {
  int blended_colors[3];
  bool bNonseparableBlend = IsNonSeparableBlendMode(blend_type);
  uint8_t* dest_scan = dest_span.data();
  const uint8_t* src_scan = src_span.data();
  int src_gap = src_Bpp - 3;
  UNSAFE_TODO({
    for (int col = 0; col < width; col++) {
      if (bNonseparableBlend) {
        uint8_t dest_scan_o[3];
        ReverseCopy3Bytes(dest_scan_o, dest_scan);
        RGB_Blend(blend_type, src_scan, dest_scan_o, blended_colors);
      }
      for (int color = 0; color < 3; color++) {
        int index = 2 - color;
        int back_color = dest_scan[index];
        int src_color = *src_scan;
        int blended = bNonseparableBlend
                          ? blended_colors[color]
                          : Blend(blend_type, back_color, src_color);
        dest_scan[index] = blended;
        src_scan++;
      }
      dest_scan += dest_Bpp;
      src_scan += src_gap;
    }
  });
}

void CompositeRow_Rgb2Rgb_NoBlend_NoClip_RgbByteOrder(
    pdfium::span<uint8_t> dest_span,
    pdfium::span<const uint8_t> src_span,
    int width,
    int dest_Bpp,
    int src_Bpp) {
  uint8_t* dest_scan = dest_span.data();
  const uint8_t* src_scan = src_span.data();
  UNSAFE_TODO({
    for (int col = 0; col < width; col++) {
      ReverseCopy3Bytes(dest_scan, src_scan);
      dest_scan += dest_Bpp;
      src_scan += src_Bpp;
    }
  });
}

void CompositeRow_Bgr2Bgra_Blend_Clip_RgbByteOrder(
    pdfium::span<uint8_t> dest_span,
    pdfium::span<const uint8_t> src_span,
    int width,
    BlendMode blend_type,
    int src_Bpp,
    pdfium::span<const uint8_t> clip_span) {
  uint8_t* dest_scan = dest_span.data();
  const uint8_t* src_scan = src_span.data();
  const uint8_t* clip_scan = clip_span.data();
  int blended_colors[3];
  bool bNonseparableBlend = IsNonSeparableBlendMode(blend_type);
  int src_gap = src_Bpp - 3;
  UNSAFE_TODO({
    for (int col = 0; col < width; col++) {
      int src_alpha = *clip_scan++;
      uint8_t back_alpha = dest_scan[3];
      if (back_alpha == 0) {
        ReverseCopy3Bytes(dest_scan, src_scan);
        src_scan += src_Bpp;
        dest_scan += 4;
        continue;
      }
      if (src_alpha == 0) {
        dest_scan += 4;
        src_scan += src_Bpp;
        continue;
      }
      uint8_t dest_alpha = AlphaUnion(back_alpha, src_alpha);
      dest_scan[3] = dest_alpha;
      int alpha_ratio = src_alpha * 255 / dest_alpha;
      if (bNonseparableBlend) {
        uint8_t dest_scan_o[3];
        ReverseCopy3Bytes(dest_scan_o, dest_scan);
        RGB_Blend(blend_type, src_scan, dest_scan_o, blended_colors);
      }
      for (int color = 0; color < 3; color++) {
        int index = 2 - color;
        int src_color = *src_scan;
        int blended = bNonseparableBlend
                          ? blended_colors[color]
                          : Blend(blend_type, dest_scan[index], src_color);
        blended = FXDIB_ALPHA_MERGE(src_color, blended, back_alpha);
        dest_scan[index] =
            FXDIB_ALPHA_MERGE(dest_scan[index], blended, alpha_ratio);
        src_scan++;
      }
      dest_scan += 4;
      src_scan += src_gap;
    }
  });
}

void CompositeRow_Rgb2Rgb_Blend_Clip_RgbByteOrder(
    pdfium::span<uint8_t> dest_span,
    pdfium::span<const uint8_t> src_span,
    int width,
    BlendMode blend_type,
    int dest_Bpp,
    int src_Bpp,
    pdfium::span<const uint8_t> clip_span) {
  uint8_t* dest_scan = dest_span.data();
  const uint8_t* src_scan = src_span.data();
  const uint8_t* clip_scan = clip_span.data();
  int blended_colors[3];
  bool bNonseparableBlend = IsNonSeparableBlendMode(blend_type);
  int src_gap = src_Bpp - 3;
  UNSAFE_TODO({
    for (int col = 0; col < width; col++) {
      uint8_t src_alpha = *clip_scan++;
      if (src_alpha == 0) {
        dest_scan += dest_Bpp;
        src_scan += src_Bpp;
        continue;
      }
      if (bNonseparableBlend) {
        uint8_t dest_scan_o[3];
        ReverseCopy3Bytes(dest_scan_o, dest_scan);
        RGB_Blend(blend_type, src_scan, dest_scan_o, blended_colors);
      }
      for (int color = 0; color < 3; color++) {
        int index = 2 - color;
        int src_color = *src_scan;
        int back_color = dest_scan[index];
        int blended = bNonseparableBlend
                          ? blended_colors[color]
                          : Blend(blend_type, back_color, src_color);
        dest_scan[index] = FXDIB_ALPHA_MERGE(back_color, blended, src_alpha);
        src_scan++;
      }
      dest_scan += dest_Bpp;
      src_scan += src_gap;
    }
  });
}

void CompositeRow_Bgr2Bgra_NoBlend_Clip_RgbByteOrder(
    pdfium::span<uint8_t> dest_span,
    pdfium::span<const uint8_t> src_span,
    int width,
    int src_Bpp,
    pdfium::span<const uint8_t> clip_span) {
  uint8_t* dest_scan = dest_span.data();
  const uint8_t* src_scan = src_span.data();
  const uint8_t* clip_scan = clip_span.data();
  int src_gap = src_Bpp - 3;
  UNSAFE_TODO({
    for (int col = 0; col < width; col++) {
      int src_alpha = clip_scan[col];
      if (src_alpha == 255) {
        ReverseCopy3Bytes(dest_scan, src_scan);
        dest_scan[3] = 255;
        dest_scan += 4;
        src_scan += src_Bpp;
        continue;
      }
      if (src_alpha == 0) {
        dest_scan += 4;
        src_scan += src_Bpp;
        continue;
      }
      int back_alpha = dest_scan[3];
      uint8_t dest_alpha = AlphaUnion(back_alpha, src_alpha);
      dest_scan[3] = dest_alpha;
      int alpha_ratio = src_alpha * 255 / dest_alpha;
      for (int color = 0; color < 3; color++) {
        int index = 2 - color;
        dest_scan[index] =
            FXDIB_ALPHA_MERGE(dest_scan[index], *src_scan, alpha_ratio);
        src_scan++;
      }
      dest_scan += 4;
      src_scan += src_gap;
    }
  });
}

void CompositeRow_Rgb2Rgb_NoBlend_Clip_RgbByteOrder(
    pdfium::span<uint8_t> dest_span,
    pdfium::span<const uint8_t> src_span,
    int width,
    int dest_Bpp,
    int src_Bpp,
    pdfium::span<const uint8_t> clip_span) {
  uint8_t* dest_scan = dest_span.data();
  const uint8_t* src_scan = src_span.data();
  const uint8_t* clip_scan = clip_span.data();
  UNSAFE_TODO({
    for (int col = 0; col < width; col++) {
      int src_alpha = clip_scan[col];
      if (src_alpha == 255) {
        ReverseCopy3Bytes(dest_scan, src_scan);
      } else if (src_alpha) {
        dest_scan[2] = FXDIB_ALPHA_MERGE(dest_scan[2], *src_scan, src_alpha);
        src_scan++;
        dest_scan[1] = FXDIB_ALPHA_MERGE(dest_scan[1], *src_scan, src_alpha);
        src_scan++;
        dest_scan[0] = FXDIB_ALPHA_MERGE(dest_scan[0], *src_scan, src_alpha);
        dest_scan += dest_Bpp;
        src_scan += src_Bpp - 2;
        continue;
      }
      dest_scan += dest_Bpp;
      src_scan += src_Bpp;
    }
  });
}

void CompositeRow_8bppRgb2Rgb_NoBlend_RgbByteOrder(
    pdfium::span<uint8_t> dest_span,
    pdfium::span<const uint8_t> src_span,
    const FX_ARGB* pPalette,
    int pixel_count,
    int DestBpp,
    pdfium::span<const uint8_t> clip_span) {
  uint8_t* dest_scan = dest_span.data();
  const uint8_t* src_scan = src_span.data();
  const uint8_t* clip_scan = clip_span.data();
  UNSAFE_TODO({
    for (int col = 0; col < pixel_count; col++) {
      FX_ARGB argb = pPalette ? pPalette[*src_scan]
                              : ArgbEncode(0, *src_scan, *src_scan, *src_scan);
      int src_r = FXARGB_R(argb);
      int src_g = FXARGB_G(argb);
      int src_b = FXARGB_B(argb);
      if (clip_scan && clip_scan[col] < 255) {
        dest_scan[2] = FXDIB_ALPHA_MERGE(dest_scan[2], src_b, clip_scan[col]);
        dest_scan[1] = FXDIB_ALPHA_MERGE(dest_scan[1], src_g, clip_scan[col]);
        dest_scan[0] = FXDIB_ALPHA_MERGE(dest_scan[0], src_r, clip_scan[col]);
      } else {
        dest_scan[2] = src_b;
        dest_scan[1] = src_g;
        dest_scan[0] = src_r;
      }
      dest_scan += DestBpp;
      src_scan++;
    }
  });
}

void CompositeRow_1bppRgb2Rgb_NoBlend_RgbByteOrder(
    pdfium::span<uint8_t> dest_span,
    pdfium::span<const uint8_t> src_span,
    int src_left,
    pdfium::span<const FX_ARGB> src_palette,
    int pixel_count,
    int DestBpp,
    pdfium::span<const uint8_t> clip_span) {
  uint8_t* dest_scan = dest_span.data();
  const uint8_t* src_scan = src_span.data();
  const uint8_t* clip_scan = clip_span.data();
  int reset_r;
  int reset_g;
  int reset_b;
  int set_r;
  int set_g;
  int set_b;
  if (!src_palette.empty()) {
    reset_r = FXARGB_R(src_palette[0]);
    reset_g = FXARGB_G(src_palette[0]);
    reset_b = FXARGB_B(src_palette[0]);
    set_r = FXARGB_R(src_palette[1]);
    set_g = FXARGB_G(src_palette[1]);
    set_b = FXARGB_B(src_palette[1]);
  } else {
    reset_r = reset_g = reset_b = 0;
    set_r = set_g = set_b = 255;
  }
  UNSAFE_TODO({
    for (int col = 0; col < pixel_count; col++) {
      int src_r;
      int src_g;
      int src_b;
      if (src_scan[(col + src_left) / 8] & (1 << (7 - (col + src_left) % 8))) {
        src_r = set_r;
        src_g = set_g;
        src_b = set_b;
      } else {
        src_r = reset_r;
        src_g = reset_g;
        src_b = reset_b;
      }
      if (clip_scan && clip_scan[col] < 255) {
        dest_scan[2] = FXDIB_ALPHA_MERGE(dest_scan[2], src_b, clip_scan[col]);
        dest_scan[1] = FXDIB_ALPHA_MERGE(dest_scan[1], src_g, clip_scan[col]);
        dest_scan[0] = FXDIB_ALPHA_MERGE(dest_scan[0], src_r, clip_scan[col]);
      } else {
        dest_scan[2] = src_b;
        dest_scan[1] = src_g;
        dest_scan[0] = src_r;
      }
      dest_scan += DestBpp;
    }
  });
}

void CompositeRow_8bppBgr2Bgra_NoBlend_RgbByteOrder(
    pdfium::span<uint8_t> dest_span,
    pdfium::span<const uint8_t> src_span,
    int width,
    const FX_ARGB* pPalette,
    pdfium::span<const uint8_t> clip_span) {
  uint8_t* dest_scan = dest_span.data();
  const uint8_t* src_scan = src_span.data();
  const uint8_t* clip_scan = clip_span.data();
  UNSAFE_TODO({
    for (int col = 0; col < width; col++) {
      int src_r;
      int src_g;
      int src_b;
      if (pPalette) {
        FX_ARGB argb = pPalette[*src_scan];
        src_r = FXARGB_R(argb);
        src_g = FXARGB_G(argb);
        src_b = FXARGB_B(argb);
      } else {
        src_r = src_g = src_b = *src_scan;
      }
      if (!clip_scan || clip_scan[col] == 255) {
        dest_scan[2] = src_b;
        dest_scan[1] = src_g;
        dest_scan[0] = src_r;
        dest_scan[3] = 255;
        src_scan++;
        dest_scan += 4;
        continue;
      }
      int src_alpha = clip_scan[col];
      if (src_alpha == 0) {
        dest_scan += 4;
        src_scan++;
        continue;
      }
      int back_alpha = dest_scan[3];
      uint8_t dest_alpha = AlphaUnion(back_alpha, src_alpha);
      dest_scan[3] = dest_alpha;
      int alpha_ratio = src_alpha * 255 / dest_alpha;
      dest_scan[2] = FXDIB_ALPHA_MERGE(dest_scan[2], src_b, alpha_ratio);
      dest_scan[1] = FXDIB_ALPHA_MERGE(dest_scan[1], src_g, alpha_ratio);
      dest_scan[0] = FXDIB_ALPHA_MERGE(dest_scan[0], src_r, alpha_ratio);
      dest_scan += 4;
      src_scan++;
    }
  });
}

void CompositeRow_1bppBgr2Bgra_NoBlend_RgbByteOrder(
    pdfium::span<uint8_t> dest_span,
    pdfium::span<const uint8_t> src_span,
    int src_left,
    int width,
    pdfium::span<const FX_ARGB> src_palette,
    pdfium::span<const uint8_t> clip_span) {
  uint8_t* dest_scan = dest_span.data();
  const uint8_t* src_scan = src_span.data();
  const uint8_t* clip_scan = clip_span.data();
  int reset_r;
  int reset_g;
  int reset_b;
  int set_r;
  int set_g;
  int set_b;
  if (!src_palette.empty()) {
    reset_r = FXARGB_R(src_palette[0]);
    reset_g = FXARGB_G(src_palette[0]);
    reset_b = FXARGB_B(src_palette[0]);
    set_r = FXARGB_R(src_palette[1]);
    set_g = FXARGB_G(src_palette[1]);
    set_b = FXARGB_B(src_palette[1]);
  } else {
    reset_r = reset_g = reset_b = 0;
    set_r = set_g = set_b = 255;
  }
  UNSAFE_TODO({
    for (int col = 0; col < width; col++) {
      int src_r;
      int src_g;
      int src_b;
      if (src_scan[(col + src_left) / 8] & (1 << (7 - (col + src_left) % 8))) {
        src_r = set_r;
        src_g = set_g;
        src_b = set_b;
      } else {
        src_r = reset_r;
        src_g = reset_g;
        src_b = reset_b;
      }
      if (!clip_scan || clip_scan[col] == 255) {
        dest_scan[2] = src_b;
        dest_scan[1] = src_g;
        dest_scan[0] = src_r;
        dest_scan[3] = 255;
        dest_scan += 4;
        continue;
      }
      int src_alpha = clip_scan[col];
      if (src_alpha == 0) {
        dest_scan += 4;
        continue;
      }
      int back_alpha = dest_scan[3];
      uint8_t dest_alpha = AlphaUnion(back_alpha, src_alpha);
      dest_scan[3] = dest_alpha;
      int alpha_ratio = src_alpha * 255 / dest_alpha;
      dest_scan[2] = FXDIB_ALPHA_MERGE(dest_scan[2], src_b, alpha_ratio);
      dest_scan[1] = FXDIB_ALPHA_MERGE(dest_scan[1], src_g, alpha_ratio);
      dest_scan[0] = FXDIB_ALPHA_MERGE(dest_scan[0], src_r, alpha_ratio);
      dest_scan += 4;
    }
  });
}

void CompositeRow_ByteMask2Bgra_RgbByteOrder(
    pdfium::span<uint8_t> dest_span,
    pdfium::span<const uint8_t> src_span,
    int mask_alpha,
    int src_r,
    int src_g,
    int src_b,
    int pixel_count,
    BlendMode blend_type,
    pdfium::span<const uint8_t> clip_span) {
  uint8_t* dest_scan = dest_span.data();
  UNSAFE_TODO({
    for (int col = 0; col < pixel_count; col++) {
      int src_alpha = GetAlphaWithSrc(mask_alpha, clip_span, src_span, col);
      uint8_t back_alpha = dest_scan[3];
      if (back_alpha == 0) {
        FXARGB_SetRGBOrderDIB(dest_scan,
                              ArgbEncode(src_alpha, src_r, src_g, src_b));
        dest_scan += 4;
        continue;
      }
      if (src_alpha == 0) {
        dest_scan += 4;
        continue;
      }
      uint8_t dest_alpha = AlphaUnion(back_alpha, src_alpha);
      dest_scan[3] = dest_alpha;
      int alpha_ratio = src_alpha * 255 / dest_alpha;
      if (IsNonSeparableBlendMode(blend_type)) {
        int blended_colors[3];
        uint8_t scan[3] = {static_cast<uint8_t>(src_b),
                           static_cast<uint8_t>(src_g),
                           static_cast<uint8_t>(src_r)};
        uint8_t dest_scan_o[3];
        ReverseCopy3Bytes(dest_scan_o, dest_scan);
        RGB_Blend(blend_type, scan, dest_scan_o, blended_colors);
        dest_scan[2] =
            FXDIB_ALPHA_MERGE(dest_scan[2], blended_colors[0], alpha_ratio);
        dest_scan[1] =
            FXDIB_ALPHA_MERGE(dest_scan[1], blended_colors[1], alpha_ratio);
        dest_scan[0] =
            FXDIB_ALPHA_MERGE(dest_scan[0], blended_colors[2], alpha_ratio);
      } else if (blend_type != BlendMode::kNormal) {
        int blended = Blend(blend_type, dest_scan[2], src_b);
        blended = FXDIB_ALPHA_MERGE(src_b, blended, back_alpha);
        dest_scan[2] = FXDIB_ALPHA_MERGE(dest_scan[2], blended, alpha_ratio);
        blended = Blend(blend_type, dest_scan[1], src_g);
        blended = FXDIB_ALPHA_MERGE(src_g, blended, back_alpha);
        dest_scan[1] = FXDIB_ALPHA_MERGE(dest_scan[1], blended, alpha_ratio);
        blended = Blend(blend_type, dest_scan[0], src_r);
        blended = FXDIB_ALPHA_MERGE(src_r, blended, back_alpha);
        dest_scan[0] = FXDIB_ALPHA_MERGE(dest_scan[0], blended, alpha_ratio);
      } else {
        dest_scan[2] = FXDIB_ALPHA_MERGE(dest_scan[2], src_b, alpha_ratio);
        dest_scan[1] = FXDIB_ALPHA_MERGE(dest_scan[1], src_g, alpha_ratio);
        dest_scan[0] = FXDIB_ALPHA_MERGE(dest_scan[0], src_r, alpha_ratio);
      }
      dest_scan += 4;
    }
  });
}

void CompositeRow_ByteMask2Rgb_RgbByteOrder(
    pdfium::span<uint8_t> dest_span,
    pdfium::span<const uint8_t> src_span,
    int mask_alpha,
    int src_r,
    int src_g,
    int src_b,
    int pixel_count,
    BlendMode blend_type,
    int Bpp,
    pdfium::span<const uint8_t> clip_span) {
  uint8_t* dest_scan = dest_span.data();
  UNSAFE_TODO({
    for (int col = 0; col < pixel_count; col++) {
      int src_alpha = GetAlphaWithSrc(mask_alpha, clip_span, src_span, col);
      if (src_alpha == 0) {
        dest_scan += Bpp;
        continue;
      }
      if (IsNonSeparableBlendMode(blend_type)) {
        int blended_colors[3];
        uint8_t scan[3] = {static_cast<uint8_t>(src_b),
                           static_cast<uint8_t>(src_g),
                           static_cast<uint8_t>(src_r)};
        uint8_t dest_scan_o[3];
        ReverseCopy3Bytes(dest_scan_o, dest_scan);
        RGB_Blend(blend_type, scan, dest_scan_o, blended_colors);
        dest_scan[2] =
            FXDIB_ALPHA_MERGE(dest_scan[2], blended_colors[0], src_alpha);
        dest_scan[1] =
            FXDIB_ALPHA_MERGE(dest_scan[1], blended_colors[1], src_alpha);
        dest_scan[0] =
            FXDIB_ALPHA_MERGE(dest_scan[0], blended_colors[2], src_alpha);
      } else if (blend_type != BlendMode::kNormal) {
        int blended = Blend(blend_type, dest_scan[2], src_b);
        dest_scan[2] = FXDIB_ALPHA_MERGE(dest_scan[2], blended, src_alpha);
        blended = Blend(blend_type, dest_scan[1], src_g);
        dest_scan[1] = FXDIB_ALPHA_MERGE(dest_scan[1], blended, src_alpha);
        blended = Blend(blend_type, dest_scan[0], src_r);
        dest_scan[0] = FXDIB_ALPHA_MERGE(dest_scan[0], blended, src_alpha);
      } else {
        dest_scan[2] = FXDIB_ALPHA_MERGE(dest_scan[2], src_b, src_alpha);
        dest_scan[1] = FXDIB_ALPHA_MERGE(dest_scan[1], src_g, src_alpha);
        dest_scan[0] = FXDIB_ALPHA_MERGE(dest_scan[0], src_r, src_alpha);
      }
      dest_scan += Bpp;
    }
  });
}

void CompositeRow_BitMask2Bgra_RgbByteOrder(
    pdfium::span<uint8_t> dest_span,
    pdfium::span<const uint8_t> src_span,
    int mask_alpha,
    int src_r,
    int src_g,
    int src_b,
    int src_left,
    int pixel_count,
    BlendMode blend_type,
    pdfium::span<const uint8_t> clip_span) {
  uint8_t* dest_scan = dest_span.data();
  const uint8_t* src_scan = src_span.data();
  const uint8_t* clip_scan = clip_span.data();
  UNSAFE_TODO({
    if (blend_type == BlendMode::kNormal && !clip_scan && mask_alpha == 255) {
      FX_ARGB argb = ArgbEncode(0xff, src_r, src_g, src_b);
      for (int col = 0; col < pixel_count; col++) {
        if (src_scan[(src_left + col) / 8] &
            (1 << (7 - (src_left + col) % 8))) {
          FXARGB_SetRGBOrderDIB(dest_scan, argb);
        }
        dest_scan += 4;
      }
      return;
    }
    for (int col = 0; col < pixel_count; col++) {
      if (!(src_scan[(src_left + col) / 8] &
            (1 << (7 - (src_left + col) % 8)))) {
        dest_scan += 4;
        continue;
      }
      int src_alpha = GetAlpha(mask_alpha, clip_scan, col);
      uint8_t back_alpha = dest_scan[3];
      if (back_alpha == 0) {
        FXARGB_SetRGBOrderDIB(dest_scan,
                              ArgbEncode(src_alpha, src_r, src_g, src_b));
        dest_scan += 4;
        continue;
      }
      uint8_t dest_alpha = AlphaUnion(back_alpha, src_alpha);
      dest_scan[3] = dest_alpha;
      int alpha_ratio = src_alpha * 255 / dest_alpha;
      if (IsNonSeparableBlendMode(blend_type)) {
        int blended_colors[3];
        uint8_t scan[3] = {static_cast<uint8_t>(src_b),
                           static_cast<uint8_t>(src_g),
                           static_cast<uint8_t>(src_r)};
        uint8_t dest_scan_o[3];
        ReverseCopy3Bytes(dest_scan_o, dest_scan);
        RGB_Blend(blend_type, scan, dest_scan_o, blended_colors);
        dest_scan[2] =
            FXDIB_ALPHA_MERGE(dest_scan[2], blended_colors[0], alpha_ratio);
        dest_scan[1] =
            FXDIB_ALPHA_MERGE(dest_scan[1], blended_colors[1], alpha_ratio);
        dest_scan[0] =
            FXDIB_ALPHA_MERGE(dest_scan[0], blended_colors[2], alpha_ratio);
      } else if (blend_type != BlendMode::kNormal) {
        int blended = Blend(blend_type, dest_scan[2], src_b);
        blended = FXDIB_ALPHA_MERGE(src_b, blended, back_alpha);
        dest_scan[2] = FXDIB_ALPHA_MERGE(dest_scan[2], blended, alpha_ratio);
        blended = Blend(blend_type, dest_scan[1], src_g);
        blended = FXDIB_ALPHA_MERGE(src_g, blended, back_alpha);
        dest_scan[1] = FXDIB_ALPHA_MERGE(dest_scan[1], blended, alpha_ratio);
        blended = Blend(blend_type, dest_scan[0], src_r);
        blended = FXDIB_ALPHA_MERGE(src_r, blended, back_alpha);
        dest_scan[0] = FXDIB_ALPHA_MERGE(dest_scan[0], blended, alpha_ratio);
      } else {
        dest_scan[2] = FXDIB_ALPHA_MERGE(dest_scan[2], src_b, alpha_ratio);
        dest_scan[1] = FXDIB_ALPHA_MERGE(dest_scan[1], src_g, alpha_ratio);
        dest_scan[0] = FXDIB_ALPHA_MERGE(dest_scan[0], src_r, alpha_ratio);
      }
      dest_scan += 4;
    }
  });
}

void CompositeRow_BitMask2Rgb_RgbByteOrder(
    pdfium::span<uint8_t> dest_span,
    pdfium::span<const uint8_t> src_span,
    int mask_alpha,
    int src_r,
    int src_g,
    int src_b,
    int src_left,
    int pixel_count,
    BlendMode blend_type,
    int Bpp,
    pdfium::span<const uint8_t> clip_span) {
  uint8_t* dest_scan = dest_span.data();
  const uint8_t* src_scan = src_span.data();
  const uint8_t* clip_scan = clip_span.data();
  UNSAFE_TODO({
    if (blend_type == BlendMode::kNormal && !clip_scan && mask_alpha == 255) {
      for (int col = 0; col < pixel_count; col++) {
        if (src_scan[(src_left + col) / 8] &
            (1 << (7 - (src_left + col) % 8))) {
          dest_scan[2] = src_b;
          dest_scan[1] = src_g;
          dest_scan[0] = src_r;
        }
        dest_scan += Bpp;
      }
      return;
    }
    for (int col = 0; col < pixel_count; col++) {
      if (!(src_scan[(src_left + col) / 8] &
            (1 << (7 - (src_left + col) % 8)))) {
        dest_scan += Bpp;
        continue;
      }
      int src_alpha = GetAlpha(mask_alpha, clip_scan, col);
      if (src_alpha == 0) {
        dest_scan += Bpp;
        continue;
      }
      if (IsNonSeparableBlendMode(blend_type)) {
        int blended_colors[3];
        uint8_t scan[3] = {static_cast<uint8_t>(src_b),
                           static_cast<uint8_t>(src_g),
                           static_cast<uint8_t>(src_r)};
        uint8_t dest_scan_o[3];
        ReverseCopy3Bytes(dest_scan_o, dest_scan);
        RGB_Blend(blend_type, scan, dest_scan_o, blended_colors);
        dest_scan[2] =
            FXDIB_ALPHA_MERGE(dest_scan[2], blended_colors[0], src_alpha);
        dest_scan[1] =
            FXDIB_ALPHA_MERGE(dest_scan[1], blended_colors[1], src_alpha);
        dest_scan[0] =
            FXDIB_ALPHA_MERGE(dest_scan[0], blended_colors[2], src_alpha);
      } else if (blend_type != BlendMode::kNormal) {
        int back_color = dest_scan[2];
        int blended = Blend(blend_type, back_color, src_b);
        dest_scan[2] = FXDIB_ALPHA_MERGE(back_color, blended, src_alpha);
        back_color = dest_scan[1];
        blended = Blend(blend_type, back_color, src_g);
        dest_scan[1] = FXDIB_ALPHA_MERGE(back_color, blended, src_alpha);
        back_color = dest_scan[0];
        blended = Blend(blend_type, back_color, src_r);
        dest_scan[0] = FXDIB_ALPHA_MERGE(back_color, blended, src_alpha);
      } else {
        dest_scan[2] = FXDIB_ALPHA_MERGE(dest_scan[2], src_b, src_alpha);
        dest_scan[1] = FXDIB_ALPHA_MERGE(dest_scan[1], src_g, src_alpha);
        dest_scan[0] = FXDIB_ALPHA_MERGE(dest_scan[0], src_r, src_alpha);
      }
      dest_scan += Bpp;
    }
  });
}

}  // namespace

CFX_ScanlineCompositor::CFX_ScanlineCompositor() = default;

CFX_ScanlineCompositor::~CFX_ScanlineCompositor() = default;

bool CFX_ScanlineCompositor::Init(FXDIB_Format dest_format,
                                  FXDIB_Format src_format,
                                  pdfium::span<const uint32_t> src_palette,
                                  uint32_t mask_color,
                                  BlendMode blend_type,
                                  bool bRgbByteOrder) {
  m_SrcFormat = src_format;
  m_DestFormat = dest_format;
  m_BlendType = blend_type;
  m_bRgbByteOrder = bRgbByteOrder;
  if (m_DestFormat == FXDIB_Format::kInvalid ||
      m_DestFormat == FXDIB_Format::k1bppMask ||
      m_DestFormat == FXDIB_Format::k1bppRgb) {
    return false;
  }

  if (m_bRgbByteOrder && (m_DestFormat == FXDIB_Format::k8bppMask ||
                          m_DestFormat == FXDIB_Format::k8bppRgb)) {
    return false;
  }

  if (m_SrcFormat == FXDIB_Format::k1bppMask ||
      m_SrcFormat == FXDIB_Format::k8bppMask) {
    InitSourceMask(mask_color);
    return true;
  }
  if ((m_SrcFormat == FXDIB_Format::k1bppRgb ||
       m_SrcFormat == FXDIB_Format::k8bppRgb) &&
      m_DestFormat != FXDIB_Format::k8bppMask) {
    InitSourcePalette(src_palette);
  }
  return true;
}

void CFX_ScanlineCompositor::InitSourceMask(uint32_t mask_color) {
  m_MaskAlpha = FXARGB_A(mask_color);
  m_MaskRed = FXARGB_R(mask_color);
  m_MaskGreen = FXARGB_G(mask_color);
  m_MaskBlue = FXARGB_B(mask_color);
  if (m_DestFormat == FXDIB_Format::k8bppMask)
    return;

  if (m_DestFormat == FXDIB_Format::k8bppRgb)
    m_MaskRed = FXRGB2GRAY(m_MaskRed, m_MaskGreen, m_MaskBlue);
}

void CFX_ScanlineCompositor::InitSourcePalette(
    pdfium::span<const uint32_t> src_palette) {
  DCHECK_NE(m_DestFormat, FXDIB_Format::k8bppMask);

  m_SrcPalette.Reset();
  const bool bIsDestBpp8 = m_DestFormat == FXDIB_Format::k8bppRgb;
  const size_t pal_count = static_cast<size_t>(1)
                           << GetBppFromFormat(m_SrcFormat);

  if (!src_palette.empty()) {
    if (bIsDestBpp8) {
      pdfium::span<uint8_t> gray_pal = m_SrcPalette.Make8BitPalette(pal_count);
      for (size_t i = 0; i < pal_count; ++i) {
        FX_ARGB argb = src_palette[i];
        gray_pal[i] =
            FXRGB2GRAY(FXARGB_R(argb), FXARGB_G(argb), FXARGB_B(argb));
      }
      return;
    }
    pdfium::span<uint32_t> pPalette = m_SrcPalette.Make32BitPalette(pal_count);
    fxcrt::Copy(src_palette.first(pal_count), pPalette);
    return;
  }
  if (bIsDestBpp8) {
    pdfium::span<uint8_t> gray_pal = m_SrcPalette.Make8BitPalette(pal_count);
    if (pal_count == 2) {
      gray_pal[0] = 0;
      gray_pal[1] = 255;
    } else {
      for (size_t i = 0; i < pal_count; ++i)
        gray_pal[i] = i;
    }
    return;
  }
  pdfium::span<uint32_t> pPalette = m_SrcPalette.Make32BitPalette(pal_count);
  if (pal_count == 2) {
    pPalette[0] = 0xff000000;
    pPalette[1] = 0xffffffff;
  } else {
    for (size_t i = 0; i < pal_count; ++i) {
      uint32_t v = static_cast<uint32_t>(i);
      pPalette[i] = ArgbEncode(0, v, v, v);
    }
  }
}

void CFX_ScanlineCompositor::CompositeRgbBitmapLine(
    pdfium::span<uint8_t> dest_scan,
    pdfium::span<const uint8_t> src_scan,
    int width,
    pdfium::span<const uint8_t> clip_scan) const {
  if (m_SrcFormat == FXDIB_Format::kBgr || m_SrcFormat == FXDIB_Format::kBgrx) {
    CompositeRgbBitmapLineSrcBgrx(dest_scan, src_scan, width, clip_scan);
    return;
  }
#if defined(PDF_USE_SKIA)
  if (m_SrcFormat == FXDIB_Format::kBgraPremul) {
    CHECK(clip_scan.empty());  // AGG-only.
    CompositeRgbBitmapLineSrcBgraPremul(dest_scan, src_scan, width);
    return;
  }
#endif
  CompositeRgbBitmapLineSrcBgra(dest_scan, src_scan, width, clip_scan);
}

void CFX_ScanlineCompositor::CompositeRgbBitmapLineSrcBgrx(
    pdfium::span<uint8_t> dest_scan,
    pdfium::span<const uint8_t> src_scan,
    int width,
    pdfium::span<const uint8_t> clip_scan) const {
  CHECK(m_SrcFormat == FXDIB_Format::kBgr ||
        m_SrcFormat == FXDIB_Format::kBgrx);

  const int src_Bpp = GetCompsFromFormat(m_SrcFormat);
  switch (m_DestFormat) {
    case FXDIB_Format::kInvalid:
    case FXDIB_Format::k1bppRgb:
    case FXDIB_Format::k1bppMask: {
      NOTREACHED_NORETURN();  // Disallowed by Init().
    }
    case FXDIB_Format::k8bppRgb: {
      CHECK(!m_bRgbByteOrder);  // Disallowed by Init();
      CompositeRow_Rgb2Gray(dest_scan, src_scan, src_Bpp, width, m_BlendType,
                            clip_scan);
      return;
    }
    case FXDIB_Format::k8bppMask: {
      CHECK(!m_bRgbByteOrder);  // Disallowed by Init();
      CompositeRow_Rgb2Mask(dest_scan, width, clip_scan);
      return;
    }
    case FXDIB_Format::kBgr:
    case FXDIB_Format::kBgrx: {
      const int dest_Bpp = GetCompsFromFormat(m_DestFormat);
      if (m_bRgbByteOrder) {
        if (m_BlendType == BlendMode::kNormal) {
          if (!clip_scan.empty()) {
            CompositeRow_Rgb2Rgb_NoBlend_Clip_RgbByteOrder(
                dest_scan, src_scan, width, dest_Bpp, src_Bpp, clip_scan);
            return;
          }
          CompositeRow_Rgb2Rgb_NoBlend_NoClip_RgbByteOrder(
              dest_scan, src_scan, width, dest_Bpp, src_Bpp);
          return;
        }
        if (!clip_scan.empty()) {
          CompositeRow_Rgb2Rgb_Blend_Clip_RgbByteOrder(
              dest_scan, src_scan, width, m_BlendType, dest_Bpp, src_Bpp,
              clip_scan);
          return;
        }
        CompositeRow_Rgb2Rgb_Blend_NoClip_RgbByteOrder(
            dest_scan, src_scan, width, m_BlendType, dest_Bpp, src_Bpp);
        return;
      }

      if (m_BlendType == BlendMode::kNormal) {
        if (!clip_scan.empty()) {
          CompositeRow_Rgb2Rgb_NoBlend_Clip(dest_scan, src_scan, width,
                                            dest_Bpp, src_Bpp, clip_scan);
          return;
        }
        CompositeRow_Rgb2Rgb_NoBlend_NoClip(dest_scan, src_scan, width,
                                            dest_Bpp, src_Bpp);
        return;
      }
      if (!clip_scan.empty()) {
        CompositeRow_Rgb2Rgb_Blend_Clip(dest_scan, src_scan, width, m_BlendType,
                                        dest_Bpp, src_Bpp, clip_scan);
        return;
      }
      CompositeRow_Rgb2Rgb_Blend_NoClip(dest_scan, src_scan, width, m_BlendType,
                                        dest_Bpp, src_Bpp);
      return;
    }
    case FXDIB_Format::kBgra: {
      if (m_bRgbByteOrder) {
        if (m_BlendType == BlendMode::kNormal) {
          if (!clip_scan.empty()) {
            CompositeRow_Bgr2Bgra_NoBlend_Clip_RgbByteOrder(
                dest_scan, src_scan, width, src_Bpp, clip_scan);
            return;
          }
          CompositeRow_Bgr2Bgra_NoBlend_NoClip_RgbByteOrder(dest_scan, src_scan,
                                                            width, src_Bpp);
          return;
        }
        if (!clip_scan.empty()) {
          CompositeRow_Bgr2Bgra_Blend_Clip_RgbByteOrder(
              dest_scan, src_scan, width, m_BlendType, src_Bpp, clip_scan);
          return;
        }
        CompositeRow_Bgr2Bgra_Blend_NoClip_RgbByteOrder(
            dest_scan, src_scan, width, m_BlendType, src_Bpp);
        return;
      }

      if (m_BlendType == BlendMode::kNormal) {
        if (!clip_scan.empty()) {
          CompositeRow_Bgr2Bgra_NoBlend_Clip(dest_scan, src_scan, width,
                                             src_Bpp, clip_scan);
          return;
        }
        CompositeRow_Bgr2Bgra_NoBlend_NoClip(dest_scan, src_scan, width,
                                             src_Bpp);
        return;
      }
      if (!clip_scan.empty()) {
        CompositeRow_Bgr2Bgra_Blend_Clip(dest_scan, src_scan, width,
                                         m_BlendType, src_Bpp, clip_scan);
        return;
      }
      CompositeRow_Bgr2Bgra_Blend_NoClip(dest_scan, src_scan, width,
                                         m_BlendType, src_Bpp);
      return;
    }
#if defined(PDF_USE_SKIA)
    case FXDIB_Format::kBgraPremul: {
      // TODO(crbug.com/42271020): Consider adding support for
      // `FXDIB_Format::kBgraPremul`
      NOTREACHED_NORETURN();
    }
#endif
  }
}

void CFX_ScanlineCompositor::CompositeRgbBitmapLineSrcBgra(
    pdfium::span<uint8_t> dest_scan,
    pdfium::span<const uint8_t> src_scan,
    int width,
    pdfium::span<const uint8_t> clip_scan) const {
  CHECK_EQ(m_SrcFormat, FXDIB_Format::kBgra);

  auto src_span =
      fxcrt::reinterpret_span<const FX_BGRA_STRUCT<uint8_t>>(src_scan).first(
          width);

  switch (m_DestFormat) {
    case FXDIB_Format::kInvalid:
    case FXDIB_Format::k1bppRgb:
    case FXDIB_Format::k1bppMask: {
      NOTREACHED_NORETURN();  // Disallowed by Init().
    }
    case FXDIB_Format::k8bppRgb: {
      CHECK(!m_bRgbByteOrder);  // Disallowed by Init();
      CompositeRowBgra2Gray(src_span, clip_scan, dest_scan, m_BlendType);
      return;
    }
    case FXDIB_Format::k8bppMask: {
      CHECK(!m_bRgbByteOrder);  // Disallowed by Init();
      CompositeRowBgra2Mask(src_span, clip_scan, dest_scan);
      return;
    }
    case FXDIB_Format::kBgr: {
      if (m_bRgbByteOrder) {
        auto dest_span =
            fxcrt::reinterpret_span<FX_RGB_STRUCT<uint8_t>>(dest_scan);
        CompositeRowBgra2Bgr(src_span, clip_scan, dest_span, m_BlendType);
        return;
      }

      auto dest_span =
          fxcrt::reinterpret_span<FX_BGR_STRUCT<uint8_t>>(dest_scan);
      CompositeRowBgra2Bgr(src_span, clip_scan, dest_span, m_BlendType);
      return;
    }
    case FXDIB_Format::kBgrx: {
      if (m_bRgbByteOrder) {
        auto dest_span =
            fxcrt::reinterpret_span<FX_RGBA_STRUCT<uint8_t>>(dest_scan);
        CompositeRowBgra2Bgr(src_span, clip_scan, dest_span, m_BlendType);
        return;
      }

      auto dest_span =
          fxcrt::reinterpret_span<FX_BGRA_STRUCT<uint8_t>>(dest_scan);
      CompositeRowBgra2Bgr(src_span, clip_scan, dest_span, m_BlendType);
      return;
    }
    case FXDIB_Format::kBgra: {
      if (m_bRgbByteOrder) {
        auto dest_span =
            fxcrt::reinterpret_span<FX_RGBA_STRUCT<uint8_t>>(dest_scan);
        CompositeRowBgra2Bgra(src_span, clip_scan, dest_span, m_BlendType);
        return;
      }
      auto dest_span =
          fxcrt::reinterpret_span<FX_BGRA_STRUCT<uint8_t>>(dest_scan);
      CompositeRowBgra2Bgra(src_span, clip_scan, dest_span, m_BlendType);
      return;
    }
#if defined(PDF_USE_SKIA)
    case FXDIB_Format::kBgraPremul: {
      // TODO(crbug.com/42271020): Consider adding support for
      // `FXDIB_Format::kBgraPremul`
      NOTREACHED_NORETURN();
    }
#endif
  }
}

#if defined(PDF_USE_SKIA)
void CFX_ScanlineCompositor::CompositeRgbBitmapLineSrcBgraPremul(
    pdfium::span<uint8_t> dest_scan,
    pdfium::span<const uint8_t> src_scan,
    int width) const {
  CHECK_EQ(m_SrcFormat, FXDIB_Format::kBgraPremul);

  auto src_span =
      fxcrt::reinterpret_span<const FX_BGRA_STRUCT<uint8_t>>(src_scan).first(
          width);

  switch (m_DestFormat) {
    case FXDIB_Format::kInvalid:
    case FXDIB_Format::k1bppRgb:
    case FXDIB_Format::k1bppMask: {
      NOTREACHED_NORETURN();  // Disallowed by Init().
    }
    case FXDIB_Format::k8bppRgb: {
      CHECK(!m_bRgbByteOrder);  // Disallowed by Init();
      // TODO(crbug.com/42271020): Consider adding support.
      NOTREACHED_NORETURN();
    }
    case FXDIB_Format::k8bppMask: {
      CHECK(!m_bRgbByteOrder);  // Disallowed by Init();
      // TODO(crbug.com/42271020): Consider adding support.
      NOTREACHED_NORETURN();
    }
    case FXDIB_Format::kBgr:
    case FXDIB_Format::kBgrx:
    case FXDIB_Format::kBgra: {
      // TODO(crbug.com/42271020): Consider adding support.
      NOTREACHED_NORETURN();
    }
    case FXDIB_Format::kBgraPremul: {
      if (m_bRgbByteOrder) {
        auto dest_span =
            fxcrt::reinterpret_span<FX_RGBA_STRUCT<uint8_t>>(dest_scan);
        CompositeRowBgraPremul2BgraPremul(src_span, dest_span, m_BlendType);
        return;
      }
      auto dest_span =
          fxcrt::reinterpret_span<FX_BGRA_STRUCT<uint8_t>>(dest_scan);
      CompositeRowBgraPremul2BgraPremul(src_span, dest_span, m_BlendType);
      return;
    }
  }
}
#endif  // defined(PDF_USE_SKIA)

void CFX_ScanlineCompositor::CompositePalBitmapLine(
    pdfium::span<uint8_t> dest_scan,
    pdfium::span<const uint8_t> src_scan,
    int src_left,
    int width,
    pdfium::span<const uint8_t> clip_scan) const {
  if (m_SrcFormat == FXDIB_Format::k1bppRgb) {
    CompositePalBitmapLineSrcBpp1(dest_scan, src_scan, src_left, width,
                                  clip_scan);
    return;
  }
  CompositePalBitmapLineSrcBpp8(dest_scan, src_scan, src_left, width,
                                clip_scan);
}

void CFX_ScanlineCompositor::CompositePalBitmapLineSrcBpp1(
    pdfium::span<uint8_t> dest_scan,
    pdfium::span<const uint8_t> src_scan,
    int src_left,
    int width,
    pdfium::span<const uint8_t> clip_scan) const {
  CHECK_EQ(m_SrcFormat, FXDIB_Format::k1bppRgb);

  switch (m_DestFormat) {
    case FXDIB_Format::kInvalid:
    case FXDIB_Format::k1bppRgb:
    case FXDIB_Format::k1bppMask: {
      NOTREACHED_NORETURN();  // Disallowed by Init().
    }
    case FXDIB_Format::k8bppRgb: {
      CHECK(!m_bRgbByteOrder);  // Disallowed by Init();
      CompositeRow_1bppPal2Gray(dest_scan, src_scan, src_left,
                                m_SrcPalette.Get8BitPalette(), width,
                                m_BlendType, clip_scan);
      return;
    }
    case FXDIB_Format::k8bppMask: {
      CHECK(!m_bRgbByteOrder);  // Disallowed by Init();
      CompositeRow_Rgb2Mask(dest_scan, width, clip_scan);
      return;
    }
    case FXDIB_Format::kBgr:
    case FXDIB_Format::kBgrx: {
      if (m_bRgbByteOrder) {
        CompositeRow_1bppRgb2Rgb_NoBlend_RgbByteOrder(
            dest_scan, src_scan, src_left, m_SrcPalette.Get32BitPalette(),
            width, GetCompsFromFormat(m_DestFormat), clip_scan);
        return;
      }
      CompositeRow_1bppRgb2Rgb_NoBlend(
          dest_scan, src_scan, src_left, m_SrcPalette.Get32BitPalette(), width,
          GetCompsFromFormat(m_DestFormat), clip_scan);
      return;
    }
    case FXDIB_Format::kBgra: {
      if (m_bRgbByteOrder) {
        CompositeRow_1bppBgr2Bgra_NoBlend_RgbByteOrder(
            dest_scan, src_scan, src_left, width,
            m_SrcPalette.Get32BitPalette(), clip_scan);
        return;
      }
      CompositeRow_1bppBgr2Bgra_NoBlend(dest_scan, src_scan, src_left, width,
                                        m_SrcPalette.Get32BitPalette(),
                                        clip_scan);
      return;
    }
#if defined(PDF_USE_SKIA)
    case FXDIB_Format::kBgraPremul: {
      // TODO(crbug.com/42271020): Consider adding support for
      // `FXDIB_Format::kBgraPremul`
      NOTREACHED_NORETURN();
    }
#endif
  }
}

void CFX_ScanlineCompositor::CompositePalBitmapLineSrcBpp8(
    pdfium::span<uint8_t> dest_scan,
    pdfium::span<const uint8_t> src_scan,
    int src_left,
    int width,
    pdfium::span<const uint8_t> clip_scan) const {
  CHECK_EQ(m_SrcFormat, FXDIB_Format::k8bppRgb);

  switch (m_DestFormat) {
    case FXDIB_Format::kInvalid:
    case FXDIB_Format::k1bppRgb:
    case FXDIB_Format::k1bppMask: {
      NOTREACHED_NORETURN();  // Disallowed by Init().
    }
    case FXDIB_Format::k8bppRgb: {
      CHECK(!m_bRgbByteOrder);  // Disallowed by Init();
      CompositeRow_8bppPal2Gray(dest_scan, src_scan,
                                m_SrcPalette.Get8BitPalette(), width,
                                m_BlendType, clip_scan);
      return;
    }
    case FXDIB_Format::k8bppMask: {
      CHECK(!m_bRgbByteOrder);  // Disallowed by Init();
      CompositeRow_Rgb2Mask(dest_scan, width, clip_scan);
      return;
    }
    case FXDIB_Format::kBgr:
    case FXDIB_Format::kBgrx: {
      if (m_bRgbByteOrder) {
        CompositeRow_8bppRgb2Rgb_NoBlend_RgbByteOrder(
            dest_scan, src_scan, m_SrcPalette.Get32BitPalette().data(), width,
            GetCompsFromFormat(m_DestFormat), clip_scan);
        return;
      }
      CompositeRow_8bppRgb2Rgb_NoBlend(
          dest_scan, src_scan, m_SrcPalette.Get32BitPalette(), width,
          GetCompsFromFormat(m_DestFormat), clip_scan);
      return;
    }
    case FXDIB_Format::kBgra: {
      if (m_bRgbByteOrder) {
        CompositeRow_8bppBgr2Bgra_NoBlend_RgbByteOrder(
            dest_scan, src_scan, width, m_SrcPalette.Get32BitPalette().data(),
            clip_scan);
        return;
      }
      CompositeRow_8bppBgr2Bgra_NoBlend(dest_scan, src_scan, width,
                                        m_SrcPalette.Get32BitPalette(),
                                        clip_scan);
      return;
    }
#if defined(PDF_USE_SKIA)
    case FXDIB_Format::kBgraPremul: {
      // TODO(crbug.com/42271020): Consider adding support for
      // `FXDIB_Format::kBgraPremul`
      NOTREACHED_NORETURN();
    }
#endif
  }
}

void CFX_ScanlineCompositor::CompositeByteMaskLine(
    pdfium::span<uint8_t> dest_scan,
    pdfium::span<const uint8_t> src_scan,
    int width,
    pdfium::span<const uint8_t> clip_scan) const {
  CHECK_EQ(m_SrcFormat, FXDIB_Format::k8bppMask);

  switch (m_DestFormat) {
    case FXDIB_Format::kInvalid:
    case FXDIB_Format::k1bppRgb:
    case FXDIB_Format::k1bppMask: {
      NOTREACHED_NORETURN();  // Disallowed by Init().
    }
    case FXDIB_Format::k8bppRgb: {
      CHECK(!m_bRgbByteOrder);  // Disallowed by Init();
      CompositeRow_ByteMask2Gray(dest_scan, src_scan, m_MaskAlpha, m_MaskRed,
                                 width, clip_scan);
      return;
    }
    case FXDIB_Format::k8bppMask: {
      CHECK(!m_bRgbByteOrder);  // Disallowed by Init();
      CompositeRow_ByteMask2Mask(dest_scan, src_scan, m_MaskAlpha, width,
                                 clip_scan);
      return;
    }
    case FXDIB_Format::kBgr:
    case FXDIB_Format::kBgrx: {
      if (m_bRgbByteOrder) {
        CompositeRow_ByteMask2Rgb_RgbByteOrder(
            dest_scan, src_scan, m_MaskAlpha, m_MaskRed, m_MaskGreen,
            m_MaskBlue, width, m_BlendType, GetCompsFromFormat(m_DestFormat),
            clip_scan);
        return;
      }
      CompositeRow_ByteMask2Rgb(dest_scan, src_scan, m_MaskAlpha, m_MaskRed,
                                m_MaskGreen, m_MaskBlue, width, m_BlendType,
                                GetCompsFromFormat(m_DestFormat), clip_scan);
      return;
    }
    case FXDIB_Format::kBgra: {
      if (m_bRgbByteOrder) {
        CompositeRow_ByteMask2Bgra_RgbByteOrder(
            dest_scan, src_scan, m_MaskAlpha, m_MaskRed, m_MaskGreen,
            m_MaskBlue, width, m_BlendType, clip_scan);
        return;
      }
      CompositeRow_ByteMask2Bgra(dest_scan, src_scan, m_MaskAlpha, m_MaskRed,
                                 m_MaskGreen, m_MaskBlue, width, m_BlendType,
                                 clip_scan);
      return;
    }
#if defined(PDF_USE_SKIA)
    case FXDIB_Format::kBgraPremul: {
      // TODO(crbug.com/42271020): Consider adding support for
      // `FXDIB_Format::kBgraPremul`
      NOTREACHED_NORETURN();
    }
#endif
  }
}

void CFX_ScanlineCompositor::CompositeBitMaskLine(
    pdfium::span<uint8_t> dest_scan,
    pdfium::span<const uint8_t> src_scan,
    int src_left,
    int width,
    pdfium::span<const uint8_t> clip_scan) const {
  CHECK_EQ(m_SrcFormat, FXDIB_Format::k1bppMask);

  switch (m_DestFormat) {
    case FXDIB_Format::kInvalid:
    case FXDIB_Format::k1bppRgb:
    case FXDIB_Format::k1bppMask: {
      NOTREACHED_NORETURN();  // Disallowed by Init().
    }
    case FXDIB_Format::k8bppRgb: {
      CHECK(!m_bRgbByteOrder);  // Disallowed by Init();
      CompositeRow_BitMask2Gray(dest_scan, src_scan, m_MaskAlpha, m_MaskRed,
                                src_left, width, clip_scan);
      return;
    }
    case FXDIB_Format::k8bppMask: {
      CHECK(!m_bRgbByteOrder);  // Disallowed by Init();
      CompositeRow_BitMask2Mask(dest_scan, src_scan, m_MaskAlpha, src_left,
                                width, clip_scan);
      return;
    }
    case FXDIB_Format::kBgr:
    case FXDIB_Format::kBgrx: {
      if (m_bRgbByteOrder) {
        CompositeRow_BitMask2Rgb_RgbByteOrder(
            dest_scan, src_scan, m_MaskAlpha, m_MaskRed, m_MaskGreen,
            m_MaskBlue, src_left, width, m_BlendType,
            GetCompsFromFormat(m_DestFormat), clip_scan);
        return;
      }
      CompositeRow_BitMask2Rgb(dest_scan, src_scan, m_MaskAlpha, m_MaskRed,
                               m_MaskGreen, m_MaskBlue, src_left, width,
                               m_BlendType, GetCompsFromFormat(m_DestFormat),
                               clip_scan);
      return;
    }
    case FXDIB_Format::kBgra: {
      if (m_bRgbByteOrder) {
        CompositeRow_BitMask2Bgra_RgbByteOrder(
            dest_scan, src_scan, m_MaskAlpha, m_MaskRed, m_MaskGreen,
            m_MaskBlue, src_left, width, m_BlendType, clip_scan);
        return;
      }
      CompositeRow_BitMask2Bgra(dest_scan, src_scan, m_MaskAlpha, m_MaskRed,
                                m_MaskGreen, m_MaskBlue, src_left, width,
                                m_BlendType, clip_scan);
      return;
    }
#if defined(PDF_USE_SKIA)
    case FXDIB_Format::kBgraPremul: {
      // TODO(crbug.com/42271020): Consider adding support for
      // `FXDIB_Format::kBgraPremul`
      NOTREACHED_NORETURN();
    }
#endif
  }
}

CFX_ScanlineCompositor::Palette::Palette() = default;

CFX_ScanlineCompositor::Palette::~Palette() = default;

void CFX_ScanlineCompositor::Palette::Reset() {
  m_Width = 0;
  m_nElements = 0;
  m_pData.reset();
}

pdfium::span<uint8_t> CFX_ScanlineCompositor::Palette::Make8BitPalette(
    size_t nElements) {
  m_Width = sizeof(uint8_t);
  m_nElements = nElements;
  m_pData.reset(reinterpret_cast<uint32_t*>(FX_Alloc(uint8_t, m_nElements)));
  // SAFETY: `m_nElements` passed to FX_Alloc() of type uint8_t.
  return UNSAFE_BUFFERS(pdfium::make_span(
      reinterpret_cast<uint8_t*>(m_pData.get()), m_nElements));
}

pdfium::span<uint32_t> CFX_ScanlineCompositor::Palette::Make32BitPalette(
    size_t nElements) {
  m_Width = sizeof(uint32_t);
  m_nElements = nElements;
  m_pData.reset(FX_Alloc(uint32_t, m_nElements));
  // SAFETY: `m_nElements` passed to FX_Alloc() of type uint32_t.
  return UNSAFE_BUFFERS(pdfium::make_span(m_pData.get(), m_nElements));
}

pdfium::span<const uint8_t> CFX_ScanlineCompositor::Palette::Get8BitPalette()
    const {
  CHECK(!m_pData || m_Width == sizeof(uint8_t));
  // SAFETY: `m_Width` only set to sizeof(uint8_t) just prior to passing
  // `m_nElements` to FX_Alloc() of type uint8_t.
  return UNSAFE_BUFFERS(pdfium::make_span(
      reinterpret_cast<const uint8_t*>(m_pData.get()), m_nElements));
}

pdfium::span<const uint32_t> CFX_ScanlineCompositor::Palette::Get32BitPalette()
    const {
  CHECK(!m_pData || m_Width == sizeof(uint32_t));
  // SAFETY: `m_Width` only set to sizeof(uint32_t) just prior to passing
  // `m_nElements` to FX_Alloc() of type uint32_t.
  return UNSAFE_BUFFERS(pdfium::make_span(m_pData.get(), m_nElements));
}
