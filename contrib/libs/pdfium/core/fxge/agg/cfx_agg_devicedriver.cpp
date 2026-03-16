// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxge/agg/cfx_agg_devicedriver.h"

#include <math.h>
#include <stdint.h>

#include <algorithm>
#include <utility>

#include "build/build_config.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/check_op.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/fx_safe_types.h"
#include "core/fxcrt/notreached.h"
#include "core/fxcrt/span.h"
#include "core/fxcrt/stl_util.h"
#include "core/fxcrt/unowned_ptr_exclusion.h"
#include "core/fxcrt/zip.h"
#include "core/fxge/agg/cfx_agg_cliprgn.h"
#include "core/fxge/agg/cfx_agg_imagerenderer.h"
#include "core/fxge/cfx_defaultrenderdevice.h"
#include "core/fxge/cfx_graphstatedata.h"
#include "core/fxge/cfx_path.h"
#include "core/fxge/dib/cfx_dibitmap.h"
#include "core/fxge/dib/cfx_imagestretcher.h"
#include <absl/types/variant.h>

// Ignore fallthrough warnings in agg23 headers.
#if defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wimplicit-fallthrough"
#endif
#include "third_party/agg23/agg_clip_liang_barsky.h"
#include "third_party/agg23/agg_conv_dash.h"
#include "third_party/agg23/agg_conv_stroke.h"
#include "third_party/agg23/agg_curves.h"
#include "third_party/agg23/agg_path_storage.h"
#include "third_party/agg23/agg_pixfmt_gray.h"
#include "third_party/agg23/agg_rasterizer_scanline_aa.h"
#include "third_party/agg23/agg_renderer_scanline.h"
#include "third_party/agg23/agg_scanline_u.h"
#if defined(__clang__)
#pragma GCC diagnostic pop
#endif

namespace pdfium {
namespace {

const float kMaxPos = 32000.0f;

CFX_PointF HardClip(const CFX_PointF& pos) {
  return CFX_PointF(std::clamp(pos.x, -kMaxPos, kMaxPos),
                    std::clamp(pos.y, -kMaxPos, kMaxPos));
}

template <typename T>
void DoAlphaMerge(T& pixel, int src_r, int src_g, int src_b, int src_alpha) {
  pixel.red = FXDIB_ALPHA_MERGE(pixel.red, src_r, src_alpha);
  pixel.green = FXDIB_ALPHA_MERGE(pixel.green, src_g, src_alpha);
  pixel.blue = FXDIB_ALPHA_MERGE(pixel.blue, src_b, src_alpha);
}

void RgbByteOrderCompositeRect(const RetainPtr<CFX_DIBitmap>& bitmap,
                               int left,
                               int top,
                               int width,
                               int height,
                               FX_ARGB src_argb) {
  int src_alpha = FXARGB_A(src_argb);
  if (src_alpha == 0) {
    return;
  }

  FX_RECT rect(left, top, left + width, top + height);
  rect.Intersect(0, 0, bitmap->GetWidth(), bitmap->GetHeight());
  width = rect.Width();
  const int src_r = FXARGB_R(src_argb);
  const int src_g = FXARGB_G(src_argb);
  const int src_b = FXARGB_B(src_argb);
  const int bytes_per_pixel = bitmap->GetBPP() / 8;
  if (src_alpha == 255) {
    if (bytes_per_pixel == 4) {
      const int src_abgr = FXARGB_TOBGRORDERDIB(src_argb);
      for (int row = rect.top; row < rect.bottom; row++) {
        auto dest_row_span = bitmap->GetWritableScanlineAs<uint32_t>(row);
        fxcrt::Fill(dest_row_span.subspan(rect.left, width), src_abgr);
      }
      return;
    }

    for (int row = rect.top; row < rect.bottom; row++) {
      auto dest_row_span =
          bitmap->GetWritableScanlineAs<FX_RGB_STRUCT<uint8_t>>(row);
      for (auto& rgb : dest_row_span.subspan(rect.left, width)) {
        rgb.red = src_r;
        rgb.green = src_g;
        rgb.blue = src_b;
      }
    }
    return;
  }

  if (bitmap->IsAlphaFormat()) {
    for (int row = rect.top; row < rect.bottom; row++) {
      auto dest_row_span =
          bitmap->GetWritableScanlineAs<FX_RGBA_STRUCT<uint8_t>>(row);
      for (auto& rgba : dest_row_span.subspan(rect.left, width)) {
        if (rgba.alpha == 0) {
          rgba.red = src_r;
          rgba.green = src_g;
          rgba.blue = src_b;
          rgba.alpha = src_alpha;
          continue;
        }

        const uint8_t dest_alpha =
            rgba.alpha + src_alpha - rgba.alpha * src_alpha / 255;
        const int alpha_ratio = src_alpha * 255 / dest_alpha;
        DoAlphaMerge(rgba, src_r, src_g, src_b, alpha_ratio);
      }
    }
    return;
  }

  if (bytes_per_pixel == 4) {
    for (int row = rect.top; row < rect.bottom; row++) {
      auto dest_row_span =
          bitmap->GetWritableScanlineAs<FX_RGBA_STRUCT<uint8_t>>(row);
      for (auto& rgba : dest_row_span.subspan(rect.left, width)) {
        DoAlphaMerge(rgba, src_r, src_g, src_b, src_alpha);
      }
    }
    return;
  }

  for (int row = rect.top; row < rect.bottom; row++) {
    auto dest_row_span =
        bitmap->GetWritableScanlineAs<FX_RGB_STRUCT<uint8_t>>(row);
    for (auto& rgb : dest_row_span.subspan(rect.left, width)) {
      DoAlphaMerge(rgb, src_r, src_g, src_b, src_alpha);
    }
  }
}

void RgbByteOrderTransferBitmap(RetainPtr<CFX_DIBitmap> pBitmap,
                                int width,
                                int height,
                                RetainPtr<const CFX_DIBBase> pSrcBitmap,
                                int src_left,
                                int src_top) {
  int dest_left = 0;
  int dest_top = 0;
  if (!pBitmap->GetOverlapRect(dest_left, dest_top, width, height,
                               pSrcBitmap->GetWidth(), pSrcBitmap->GetHeight(),
                               src_left, src_top, nullptr)) {
    return;
  }

  const FXDIB_Format dest_format = pBitmap->GetFormat();
  const FXDIB_Format src_format = pSrcBitmap->GetFormat();

  if (dest_format == src_format) {
    if (pBitmap->GetBPP() == 32) {
      for (int row = 0; row < height; row++) {
        int dest_row = dest_top + row;
        auto dest_scan =
            pBitmap->GetWritableScanlineAs<FX_RGBA_STRUCT<uint8_t>>(dest_row)
                .subspan(dest_left);
        int src_row = src_top + row;
        auto src_scan =
            pSrcBitmap->GetScanlineAs<FX_BGRA_STRUCT<uint8_t>>(src_row).subspan(
                src_left, width);
        for (auto [input, output] : fxcrt::Zip(src_scan, dest_scan)) {
          output.red = input.red;
          output.green = input.green;
          output.blue = input.blue;
          output.alpha = input.alpha;
        }
      }
      return;
    }

    CHECK_EQ(FXDIB_Format::kBgr, src_format);
    for (int row = 0; row < height; row++) {
      int dest_row = dest_top + row;
      auto dest_scan =
          pBitmap->GetWritableScanlineAs<FX_RGB_STRUCT<uint8_t>>(dest_row)
              .subspan(dest_left);
      int src_row = src_top + row;
      auto src_scan =
          pSrcBitmap->GetScanlineAs<FX_BGR_STRUCT<uint8_t>>(src_row).subspan(
              src_left, width);
      for (auto [input, output] : fxcrt::Zip(src_scan, dest_scan)) {
        output.red = input.red;
        output.green = input.green;
        output.blue = input.blue;
      }
    }
    return;
  }

  if (dest_format == FXDIB_Format::kBgr) {
    CHECK_EQ(src_format, FXDIB_Format::kBgrx);
    for (int row = 0; row < height; row++) {
      int dest_row = dest_top + row;
      auto dest_scan =
          pBitmap->GetWritableScanlineAs<FX_RGB_STRUCT<uint8_t>>(dest_row)
              .subspan(dest_left);
      int src_row = src_top + row;
      auto src_scan =
          pSrcBitmap->GetScanlineAs<FX_BGRA_STRUCT<uint8_t>>(src_row).subspan(
              src_left, width);
      for (auto [input, output] : fxcrt::Zip(src_scan, dest_scan)) {
        output.red = input.red;
        output.green = input.green;
        output.blue = input.blue;
      }
    }
    return;
  }

  CHECK(dest_format == FXDIB_Format::kBgra ||
        dest_format == FXDIB_Format::kBgrx);
  if (src_format == FXDIB_Format::kBgr) {
    for (int row = 0; row < height; row++) {
      int dest_row = dest_top + row;
      auto dest_scan =
          pBitmap->GetWritableScanlineAs<FX_RGBA_STRUCT<uint8_t>>(dest_row)
              .subspan(dest_left);
      int src_row = src_top + row;
      auto src_scan =
          pSrcBitmap->GetScanlineAs<FX_BGR_STRUCT<uint8_t>>(src_row).subspan(
              src_left, width);
      for (auto [input, output] : fxcrt::Zip(src_scan, dest_scan)) {
        output.red = input.red;
        output.green = input.green;
        output.blue = input.blue;
        output.alpha = 255;
      }
    }
    return;
  }
  if (src_format != FXDIB_Format::kBgrx) {
    return;
  }
  CHECK_EQ(dest_format, FXDIB_Format::kBgra);
  for (int row = 0; row < height; row++) {
    int dest_row = dest_top + row;
    auto dest_scan =
        pBitmap->GetWritableScanlineAs<FX_RGBA_STRUCT<uint8_t>>(dest_row)
            .subspan(dest_left);
    int src_row = src_top + row;
    auto src_scan =
        pSrcBitmap->GetScanlineAs<FX_BGRA_STRUCT<uint8_t>>(src_row).subspan(
            src_left, width);
    for (auto [input, output] : fxcrt::Zip(src_scan, dest_scan)) {
      output.red = input.red;
      output.green = input.green;
      output.blue = input.blue;
      output.alpha = 255;
    }
  }
}

void RasterizeStroke(agg::rasterizer_scanline_aa* rasterizer,
                     agg::path_storage* path_data,
                     const CFX_Matrix* pObject2Device,
                     const CFX_GraphStateData* pGraphState,
                     float scale,
                     bool bTextMode) {
  agg::line_cap_e cap;
  switch (pGraphState->m_LineCap) {
    case CFX_GraphStateData::LineCap::kRound:
      cap = agg::round_cap;
      break;
    case CFX_GraphStateData::LineCap::kSquare:
      cap = agg::square_cap;
      break;
    default:
      cap = agg::butt_cap;
      break;
  }
  agg::line_join_e join;
  switch (pGraphState->m_LineJoin) {
    case CFX_GraphStateData::LineJoin::kRound:
      join = agg::round_join;
      break;
    case CFX_GraphStateData::LineJoin::kBevel:
      join = agg::bevel_join;
      break;
    default:
      join = agg::miter_join_revert;
      break;
  }
  float width = pGraphState->m_LineWidth * scale;
  float unit = 1.0f;
  if (pObject2Device) {
    unit =
        1.0f / ((pObject2Device->GetXUnit() + pObject2Device->GetYUnit()) / 2);
  }
  width = std::max(width, unit);
  if (!pGraphState->m_DashArray.empty()) {
    using DashConverter = agg::conv_dash<agg::path_storage>;
    DashConverter dash(*path_data);
    for (size_t i = 0; i < (pGraphState->m_DashArray.size() + 1) / 2; i++) {
      float on = pGraphState->m_DashArray[i * 2];
      if (on <= 0.000001f)
        on = 0.1f;
      float off = i * 2 + 1 == pGraphState->m_DashArray.size()
                      ? on
                      : pGraphState->m_DashArray[i * 2 + 1];
      off = std::max(off, 0.0f);
      dash.add_dash(on * scale, off * scale);
    }
    dash.dash_start(pGraphState->m_DashPhase * scale);
    using DashStroke = agg::conv_stroke<DashConverter>;
    DashStroke stroke(dash);
    stroke.line_join(join);
    stroke.line_cap(cap);
    stroke.miter_limit(pGraphState->m_MiterLimit);
    stroke.width(width);
    rasterizer->add_path_transformed(stroke, pObject2Device);
    return;
  }
  agg::conv_stroke<agg::path_storage> stroke(*path_data);
  stroke.line_join(join);
  stroke.line_cap(cap);
  stroke.miter_limit(pGraphState->m_MiterLimit);
  stroke.width(width);
  rasterizer->add_path_transformed(stroke, pObject2Device);
}

agg::filling_rule_e GetAlternateOrWindingFillType(
    const CFX_FillRenderOptions& fill_options) {
  return fill_options.fill_type == CFX_FillRenderOptions::FillType::kWinding
             ? agg::fill_non_zero
             : agg::fill_even_odd;
}

RetainPtr<CFX_DIBitmap> GetClipMaskFromRegion(const CFX_AggClipRgn* r) {
  return (r && r->GetType() == CFX_AggClipRgn::kMaskF) ? r->GetMask() : nullptr;
}

FX_RECT GetClipBoxFromRegion(const RetainPtr<CFX_DIBitmap>& device,
                             const CFX_AggClipRgn* region) {
  if (region)
    return region->GetBox();
  return FX_RECT(0, 0, device->GetWidth(), device->GetHeight());
}

class CFX_AggRenderer {
 public:
  CFX_AggRenderer(const RetainPtr<CFX_DIBitmap>& pDevice,
                  const RetainPtr<CFX_DIBitmap>& pBackdropDevice,
                  const CFX_AggClipRgn* pClipRgn,
                  uint32_t color,
                  bool bFullCover,
                  bool bRgbByteOrder);

  // Needed for agg caller
  void prepare(unsigned) {}

  template <class Scanline>
  void render(const Scanline& sl);

 private:
  using CompositeSpanFunc = void (CFX_AggRenderer::*)(uint8_t*,
                                                      int,
                                                      int,
                                                      int,
                                                      const uint8_t*,
                                                      const uint8_t*);

  void CompositeSpan(uint8_t* dest_scan,
                     const uint8_t* backdrop_scan,
                     int bytes_per_pixel,
                     bool bDestAlpha,
                     int col_start,
                     int col_end,
                     const uint8_t* cover_scan,
                     const uint8_t* clip_scan);

  void CompositeSpanGray(uint8_t* dest_scan,
                         int bytes_per_pixel,
                         int col_start,
                         int col_end,
                         const uint8_t* cover_scan,
                         const uint8_t* clip_scan);

  void CompositeSpanARGB(uint8_t* dest_scan,
                         int bytes_per_pixel,
                         int col_start,
                         int col_end,
                         const uint8_t* cover_scan,
                         const uint8_t* clip_scan);

  void CompositeSpanRGB(uint8_t* dest_scan,
                        int bytes_per_pixel,
                        int col_start,
                        int col_end,
                        const uint8_t* cover_scan,
                        const uint8_t* clip_scan);

  static CompositeSpanFunc GetCompositeSpanFunc(
      const RetainPtr<CFX_DIBitmap>& device) {
    CHECK_NE(device->GetBPP(), 1);
    if (device->GetBPP() == 8) {
      return &CFX_AggRenderer::CompositeSpanGray;
    }
    const FXDIB_Format format = device->GetFormat();
    if (format == FXDIB_Format::kBgra) {
      return &CFX_AggRenderer::CompositeSpanARGB;
    }
    CHECK(format == FXDIB_Format::kBgr || format == FXDIB_Format::kBgrx);
    return &CFX_AggRenderer::CompositeSpanRGB;
  }

  inline int GetSrcAlpha(const uint8_t* clip_scan, int col) const {
    return clip_scan ? m_Alpha * UNSAFE_TODO(clip_scan[col]) / 255 : m_Alpha;
  }

  inline int GetSourceAlpha(const uint8_t* cover_scan,
                            const uint8_t* clip_scan,
                            int col) const {
    return UNSAFE_TODO(clip_scan ? m_Alpha * cover_scan[col] * clip_scan[col] /
                                       255 / 255
                                 : m_Alpha * cover_scan[col] / 255);
  }

  static int GetColStart(int span_left, int clip_left) {
    return span_left < clip_left ? clip_left - span_left : 0;
  }

  static int GetColEnd(int span_left, int span_len, int clip_right) {
    return span_left + span_len < clip_right ? span_len
                                             : clip_right - span_left;
  }

  const FX_BGR_STRUCT<uint8_t>& GetBGR() const {
    return absl::get<FX_BGR_STRUCT<uint8_t>>(m_ColorData);
  }
  int GetGray() const { return absl::get<int>(m_ColorData); }

  const int m_Alpha;
  absl::variant<FX_BGR_STRUCT<uint8_t>, int> m_ColorData;
  const uint32_t m_Color;
  const bool m_bFullCover;
  const bool m_bRgbByteOrder;
  const FX_RECT m_ClipBox;
  RetainPtr<CFX_DIBitmap> const m_pBackdropDevice;
  RetainPtr<CFX_DIBitmap> const m_pClipMask;
  RetainPtr<CFX_DIBitmap> const m_pDevice;
  UnownedPtr<const CFX_AggClipRgn> m_pClipRgn;
  const CompositeSpanFunc m_CompositeSpanFunc;
};

void CFX_AggRenderer::CompositeSpan(uint8_t* dest_scan,
                                    const uint8_t* backdrop_scan,
                                    int bytes_per_pixel,
                                    bool bDestAlpha,
                                    int col_start,
                                    int col_end,
                                    const uint8_t* cover_scan,
                                    const uint8_t* clip_scan) {
  CHECK(bytes_per_pixel);
  const auto& bgr = GetBGR();
  UNSAFE_TODO({
    dest_scan += col_start * bytes_per_pixel;
    backdrop_scan += col_start * bytes_per_pixel;
    if (m_bRgbByteOrder) {
      if (bytes_per_pixel == 4 && bDestAlpha) {
        for (int col = col_start; col < col_end; col++) {
          int src_alpha = GetSrcAlpha(clip_scan, col);
          uint8_t dest_alpha =
              backdrop_scan[3] + src_alpha - backdrop_scan[3] * src_alpha / 255;
          dest_scan[3] = dest_alpha;
          int alpha_ratio = src_alpha * 255 / dest_alpha;
          if (m_bFullCover) {
            *dest_scan++ =
                FXDIB_ALPHA_MERGE(*backdrop_scan++, bgr.red, alpha_ratio);
            *dest_scan++ =
                FXDIB_ALPHA_MERGE(*backdrop_scan++, bgr.green, alpha_ratio);
            *dest_scan++ =
                FXDIB_ALPHA_MERGE(*backdrop_scan++, bgr.blue, alpha_ratio);
            dest_scan++;
            backdrop_scan++;
          } else {
            int r = FXDIB_ALPHA_MERGE(*backdrop_scan++, bgr.red, alpha_ratio);
            int g = FXDIB_ALPHA_MERGE(*backdrop_scan++, bgr.green, alpha_ratio);
            int b = FXDIB_ALPHA_MERGE(*backdrop_scan++, bgr.blue, alpha_ratio);
            backdrop_scan++;
            *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, r, cover_scan[col]);
            dest_scan++;
            *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, g, cover_scan[col]);
            dest_scan++;
            *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, b, cover_scan[col]);
            dest_scan += 2;
          }
        }
        return;
      }
      if (bytes_per_pixel == 3 || bytes_per_pixel == 4) {
        for (int col = col_start; col < col_end; col++) {
          int src_alpha = GetSrcAlpha(clip_scan, col);
          int r = FXDIB_ALPHA_MERGE(*backdrop_scan++, bgr.red, src_alpha);
          int g = FXDIB_ALPHA_MERGE(*backdrop_scan++, bgr.green, src_alpha);
          int b = FXDIB_ALPHA_MERGE(*backdrop_scan, bgr.blue, src_alpha);
          backdrop_scan += bytes_per_pixel - 2;
          *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, r, cover_scan[col]);
          dest_scan++;
          *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, g, cover_scan[col]);
          dest_scan++;
          *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, b, cover_scan[col]);
          dest_scan += bytes_per_pixel - 2;
        }
      }
      return;
    }
    if (bytes_per_pixel == 4 && bDestAlpha) {
      for (int col = col_start; col < col_end; col++) {
        int src_alpha = GetSrcAlpha(clip_scan, col);
        int src_alpha_covered = src_alpha * cover_scan[col] / 255;
        if (src_alpha_covered == 0) {
          dest_scan += 4;
          continue;
        }
        if (cover_scan[col] == 255) {
          dest_scan[3] = src_alpha_covered;
          *dest_scan++ = bgr.blue;
          *dest_scan++ = bgr.green;
          *dest_scan = bgr.red;
          dest_scan += 2;
          continue;
        }
        if (dest_scan[3] == 0) {
          dest_scan[3] = src_alpha_covered;
          *dest_scan++ = bgr.blue;
          *dest_scan++ = bgr.green;
          *dest_scan = bgr.red;
          dest_scan += 2;
          continue;
        }
        uint8_t cover = cover_scan[col];
        dest_scan[3] = FXDIB_ALPHA_MERGE(dest_scan[3], src_alpha, cover);
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, bgr.blue, cover);
        dest_scan++;
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, bgr.green, cover);
        dest_scan++;
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, bgr.red, cover);
        dest_scan += 2;
      }
      return;
    }
    if (bytes_per_pixel == 3 || bytes_per_pixel == 4) {
      for (int col = col_start; col < col_end; col++) {
        int src_alpha = GetSrcAlpha(clip_scan, col);
        if (m_bFullCover) {
          *dest_scan++ =
              FXDIB_ALPHA_MERGE(*backdrop_scan++, bgr.blue, src_alpha);
          *dest_scan++ =
              FXDIB_ALPHA_MERGE(*backdrop_scan++, bgr.green, src_alpha);
          *dest_scan = FXDIB_ALPHA_MERGE(*backdrop_scan, bgr.red, src_alpha);
          dest_scan += bytes_per_pixel - 2;
          backdrop_scan += bytes_per_pixel - 2;
          continue;
        }
        int b = FXDIB_ALPHA_MERGE(*backdrop_scan++, bgr.blue, src_alpha);
        int g = FXDIB_ALPHA_MERGE(*backdrop_scan++, bgr.green, src_alpha);
        int r = FXDIB_ALPHA_MERGE(*backdrop_scan, bgr.red, src_alpha);
        backdrop_scan += bytes_per_pixel - 2;
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, b, cover_scan[col]);
        dest_scan++;
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, g, cover_scan[col]);
        dest_scan++;
        *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, r, cover_scan[col]);
        dest_scan += bytes_per_pixel - 2;
      }
      return;
    }
    CHECK_EQ(bytes_per_pixel, 1);
    const int gray = GetGray();
    for (int col = col_start; col < col_end; col++) {
      int src_alpha = GetSrcAlpha(clip_scan, col);
      if (m_bFullCover) {
        *dest_scan = FXDIB_ALPHA_MERGE(*backdrop_scan++, gray, src_alpha);
        continue;
      }
      int gray_merged = FXDIB_ALPHA_MERGE(*backdrop_scan++, gray, src_alpha);
      *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, gray_merged, cover_scan[col]);
      dest_scan++;
    }
  });
}

void CFX_AggRenderer::CompositeSpanGray(uint8_t* dest_scan,
                                        int bytes_per_pixel,
                                        int col_start,
                                        int col_end,
                                        const uint8_t* cover_scan,
                                        const uint8_t* clip_scan) {
  DCHECK(!m_bRgbByteOrder);
  const int gray = GetGray();
  UNSAFE_TODO({
    dest_scan += col_start;
    for (int col = col_start; col < col_end; col++) {
      int src_alpha = GetSourceAlpha(cover_scan, clip_scan, col);
      if (src_alpha) {
        if (src_alpha == 255) {
          *dest_scan = gray;
        } else {
          *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, gray, src_alpha);
        }
      }
      dest_scan++;
    }
  });
}

void CFX_AggRenderer::CompositeSpanARGB(uint8_t* dest_scan,
                                        int bytes_per_pixel,
                                        int col_start,
                                        int col_end,
                                        const uint8_t* cover_scan,
                                        const uint8_t* clip_scan) {
  const auto& bgr = GetBGR();
  UNSAFE_TODO({
    dest_scan += col_start * bytes_per_pixel;
    if (m_bRgbByteOrder) {
      for (int col = col_start; col < col_end; col++) {
        int src_alpha = m_bFullCover
                            ? GetSrcAlpha(clip_scan, col)
                            : GetSourceAlpha(cover_scan, clip_scan, col);
        if (src_alpha) {
          if (src_alpha == 255) {
            *(reinterpret_cast<uint32_t*>(dest_scan)) = m_Color;
          } else {
            uint8_t dest_alpha =
                dest_scan[3] + src_alpha - dest_scan[3] * src_alpha / 255;
            dest_scan[3] = dest_alpha;
            int alpha_ratio = src_alpha * 255 / dest_alpha;
            *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, bgr.red, alpha_ratio);
            dest_scan++;
            *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, bgr.green, alpha_ratio);
            dest_scan++;
            *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, bgr.blue, alpha_ratio);
            dest_scan += 2;
            continue;
          }
        }
        dest_scan += 4;
      }
      return;
    }
    for (int col = col_start; col < col_end; col++) {
      int src_alpha = m_bFullCover ? GetSrcAlpha(clip_scan, col)
                                   : GetSourceAlpha(cover_scan, clip_scan, col);
      if (src_alpha) {
        if (src_alpha == 255) {
          *(reinterpret_cast<uint32_t*>(dest_scan)) = m_Color;
        } else {
          if (dest_scan[3] == 0) {
            dest_scan[3] = src_alpha;
            *dest_scan++ = bgr.blue;
            *dest_scan++ = bgr.green;
            *dest_scan = bgr.red;
            dest_scan += 2;
            continue;
          }
          uint8_t dest_alpha =
              dest_scan[3] + src_alpha - dest_scan[3] * src_alpha / 255;
          dest_scan[3] = dest_alpha;
          int alpha_ratio = src_alpha * 255 / dest_alpha;
          *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, bgr.blue, alpha_ratio);
          dest_scan++;
          *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, bgr.green, alpha_ratio);
          dest_scan++;
          *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, bgr.red, alpha_ratio);
          dest_scan += 2;
          continue;
        }
      }
      dest_scan += bytes_per_pixel;
    }
  });
}

void CFX_AggRenderer::CompositeSpanRGB(uint8_t* dest_scan,
                                       int bytes_per_pixel,
                                       int col_start,
                                       int col_end,
                                       const uint8_t* cover_scan,
                                       const uint8_t* clip_scan) {
  const auto& bgr = GetBGR();
  UNSAFE_TODO({
    dest_scan += col_start * bytes_per_pixel;
    if (m_bRgbByteOrder) {
      for (int col = col_start; col < col_end; col++) {
        int src_alpha = GetSourceAlpha(cover_scan, clip_scan, col);
        if (src_alpha) {
          if (src_alpha == 255) {
            if (bytes_per_pixel == 4) {
              *(uint32_t*)dest_scan = m_Color;
            } else if (bytes_per_pixel == 3) {
              *dest_scan++ = bgr.red;
              *dest_scan++ = bgr.green;
              *dest_scan++ = bgr.blue;
              continue;
            }
          } else {
            *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, bgr.red, src_alpha);
            dest_scan++;
            *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, bgr.green, src_alpha);
            dest_scan++;
            *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, bgr.blue, src_alpha);
            dest_scan += bytes_per_pixel - 2;
            continue;
          }
        }
        dest_scan += bytes_per_pixel;
      }
      return;
    }
    for (int col = col_start; col < col_end; col++) {
      int src_alpha = m_bFullCover ? GetSrcAlpha(clip_scan, col)
                                   : GetSourceAlpha(cover_scan, clip_scan, col);
      if (src_alpha) {
        if (src_alpha == 255) {
          if (bytes_per_pixel == 4) {
            *(uint32_t*)dest_scan = m_Color;
          } else if (bytes_per_pixel == 3) {
            *dest_scan++ = bgr.blue;
            *dest_scan++ = bgr.green;
            *dest_scan++ = bgr.red;
            continue;
          }
        } else {
          *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, bgr.blue, src_alpha);
          dest_scan++;
          *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, bgr.green, src_alpha);
          dest_scan++;
          *dest_scan = FXDIB_ALPHA_MERGE(*dest_scan, bgr.red, src_alpha);
          dest_scan += bytes_per_pixel - 2;
          continue;
        }
      }
      dest_scan += bytes_per_pixel;
    }
  });
}

CFX_AggRenderer::CFX_AggRenderer(const RetainPtr<CFX_DIBitmap>& pDevice,
                                 const RetainPtr<CFX_DIBitmap>& pBackdropDevice,
                                 const CFX_AggClipRgn* pClipRgn,
                                 uint32_t color,
                                 bool bFullCover,
                                 bool bRgbByteOrder)
    : m_Alpha(FXARGB_A(color)),
      m_Color(bRgbByteOrder ? FXARGB_TOBGRORDERDIB(color) : color),
      m_bFullCover(bFullCover),
      m_bRgbByteOrder(bRgbByteOrder),
      m_ClipBox(GetClipBoxFromRegion(pDevice, pClipRgn)),
      m_pBackdropDevice(pBackdropDevice),
      m_pClipMask(GetClipMaskFromRegion(pClipRgn)),
      m_pDevice(pDevice),
      m_pClipRgn(pClipRgn),
      m_CompositeSpanFunc(GetCompositeSpanFunc(m_pDevice)) {
  if (m_pDevice->GetBPP() == 8) {
    DCHECK(!m_bRgbByteOrder);
    if (m_pDevice->IsMaskFormat()) {
      m_ColorData = 255;
    } else {
      m_ColorData =
          FXRGB2GRAY(FXARGB_R(color), FXARGB_G(color), FXARGB_B(color));
    }
    return;
  }

  m_ColorData = ArgbToBGRStruct(color);
}

template <class Scanline>
void CFX_AggRenderer::render(const Scanline& sl) {
  int y = sl.y();
  if (y < m_ClipBox.top || y >= m_ClipBox.bottom)
    return;

  uint8_t* dest_scan =
      m_pDevice->GetWritableBuffer().subspan(m_pDevice->GetPitch() * y).data();
  const uint8_t* backdrop_scan = nullptr;
  if (m_pBackdropDevice) {
    backdrop_scan = m_pBackdropDevice->GetBuffer()
                        .subspan(m_pBackdropDevice->GetPitch() * y)
                        .data();
  }
  const int bytes_per_pixel = m_pDevice->GetBPP() / 8;
  CHECK_NE(bytes_per_pixel, 0);
  bool bDestAlpha = m_pDevice->IsAlphaFormat() || m_pDevice->IsMaskFormat();
  unsigned num_spans = sl.num_spans();
  typename Scanline::const_iterator span = sl.begin();
  UNSAFE_TODO({
    while (true) {
      if (span->len <= 0) {
        break;
      }

      int x = span->x;
      uint8_t* dest_pos = dest_scan + x * bytes_per_pixel;
      const uint8_t* backdrop_pos =
          backdrop_scan ? backdrop_scan + x * bytes_per_pixel : nullptr;
      const uint8_t* clip_pos = nullptr;
      if (m_pClipMask) {
        // TODO(crbug.com/1382604): use subspan arithmetic.
        clip_pos = m_pClipMask->GetBuffer().data() +
                   (y - m_ClipBox.top) * m_pClipMask->GetPitch() + x -
                   m_ClipBox.left;
      }
      const int col_start = GetColStart(x, m_ClipBox.left);
      const int col_end = GetColEnd(x, span->len, m_ClipBox.right);
      if (backdrop_pos) {
        CompositeSpan(dest_pos, backdrop_pos, bytes_per_pixel, bDestAlpha,
                      col_start, col_end, span->covers, clip_pos);
      } else {
        (this->*m_CompositeSpanFunc)(dest_pos, bytes_per_pixel, col_start,
                                     col_end, span->covers, clip_pos);
      }
      if (--num_spans == 0) {
        break;
      }

      ++span;
    }
  });
}

template <class BaseRenderer>
class RendererScanLineAaOffset {
 public:
  typedef BaseRenderer base_ren_type;
  typedef typename base_ren_type::color_type color_type;
  RendererScanLineAaOffset(base_ren_type& ren, unsigned left, unsigned top)
      : m_ren(&ren), m_left(left), m_top(top) {}
  void color(const color_type& c) { m_color = c; }
  const color_type& color() const { return m_color; }
  void prepare(unsigned) {}
  template <class Scanline>
  void render(const Scanline& sl) {
    int y = sl.y();
    unsigned num_spans = sl.num_spans();
    typename Scanline::const_iterator span = sl.begin();
    while (true) {
      int x = span->x;
      if (span->len > 0) {
        m_ren->blend_solid_hspan(x - m_left, y - m_top, (unsigned)span->len,
                                 m_color, span->covers);
      } else {
        m_ren->blend_hline(x - m_left, y - m_top, (unsigned)(x - span->len - 1),
                           m_color, *(span->covers));
      }
      if (--num_spans == 0)
        break;

      UNSAFE_TODO(++span);
    }
  }

 private:
  UNOWNED_PTR_EXCLUSION base_ren_type* m_ren;
  color_type m_color;
  unsigned m_left;
  unsigned m_top;
};

agg::path_storage BuildAggPath(const CFX_Path& path,
                               const CFX_Matrix* pObject2Device) {
  agg::path_storage agg_path;
  pdfium::span<const CFX_Path::Point> points = path.GetPoints();
  for (size_t i = 0; i < points.size(); ++i) {
    CFX_PointF pos = points[i].m_Point;
    if (pObject2Device)
      pos = pObject2Device->Transform(pos);

    pos = HardClip(pos);
    CFX_Path::Point::Type point_type = points[i].m_Type;
    if (point_type == CFX_Path::Point::Type::kMove) {
      agg_path.move_to(pos.x, pos.y);
    } else if (point_type == CFX_Path::Point::Type::kLine) {
      if (i > 0 && points[i - 1].IsTypeAndOpen(CFX_Path::Point::Type::kMove) &&
          (i + 1 == points.size() ||
           points[i + 1].IsTypeAndOpen(CFX_Path::Point::Type::kMove)) &&
          points[i].m_Point == points[i - 1].m_Point) {
        pos.x += 1;
      }
      agg_path.line_to(pos.x, pos.y);
    } else if (point_type == CFX_Path::Point::Type::kBezier) {
      if (i > 0 && i + 2 < points.size()) {
        CFX_PointF pos0 = points[i - 1].m_Point;
        CFX_PointF pos2 = points[i + 1].m_Point;
        CFX_PointF pos3 = points[i + 2].m_Point;
        if (pObject2Device) {
          pos0 = pObject2Device->Transform(pos0);
          pos2 = pObject2Device->Transform(pos2);
          pos3 = pObject2Device->Transform(pos3);
        }
        pos0 = HardClip(pos0);
        pos2 = HardClip(pos2);
        pos3 = HardClip(pos3);
        agg::curve4 curve(pos0.x, pos0.y, pos.x, pos.y, pos2.x, pos2.y, pos3.x,
                          pos3.y);
        i += 2;
        agg_path.add_path(curve);
      }
    }
    if (points[i].m_CloseFigure)
      agg_path.end_poly();
  }
  return agg_path;
}

}  // namespace

CFX_AggDeviceDriver::CFX_AggDeviceDriver(
    RetainPtr<CFX_DIBitmap> pBitmap,
    bool bRgbByteOrder,
    RetainPtr<CFX_DIBitmap> pBackdropBitmap,
    bool bGroupKnockout)
    : m_pBitmap(std::move(pBitmap)),
      m_bRgbByteOrder(bRgbByteOrder),
      m_bGroupKnockout(bGroupKnockout),
      m_pBackdropBitmap(std::move(pBackdropBitmap)) {
  CHECK(m_pBitmap);
  CHECK_NE(m_pBitmap->GetFormat(), FXDIB_Format::k1bppMask);
  CHECK_NE(m_pBitmap->GetFormat(), FXDIB_Format::k1bppRgb);
  InitPlatform();
}

CFX_AggDeviceDriver::~CFX_AggDeviceDriver() {
  DestroyPlatform();
}

#if !BUILDFLAG(IS_APPLE)
void CFX_AggDeviceDriver::InitPlatform() {}

void CFX_AggDeviceDriver::DestroyPlatform() {}

bool CFX_AggDeviceDriver::DrawDeviceText(
    pdfium::span<const TextCharPos> pCharPos,
    CFX_Font* pFont,
    const CFX_Matrix& mtObject2Device,
    float font_size,
    uint32_t color,
    const CFX_TextRenderOptions& options) {
  return false;
}
#endif  // !BUILDFLAG(IS_APPLE)

DeviceType CFX_AggDeviceDriver::GetDeviceType() const {
  return DeviceType::kDisplay;
}

int CFX_AggDeviceDriver::GetDeviceCaps(int caps_id) const {
  switch (caps_id) {
    case FXDC_PIXEL_WIDTH:
      return m_pBitmap->GetWidth();
    case FXDC_PIXEL_HEIGHT:
      return m_pBitmap->GetHeight();
    case FXDC_BITS_PIXEL:
      return m_pBitmap->GetBPP();
    case FXDC_HORZ_SIZE:
    case FXDC_VERT_SIZE:
      return 0;
    case FXDC_RENDER_CAPS: {
      int flags = FXRC_GET_BITS | FXRC_ALPHA_PATH | FXRC_ALPHA_IMAGE |
                  FXRC_BLEND_MODE | FXRC_SOFT_CLIP;
      if (m_pBitmap->IsAlphaFormat()) {
        flags |= FXRC_ALPHA_OUTPUT;
      } else if (m_pBitmap->IsMaskFormat()) {
        CHECK_NE(m_pBitmap->GetBPP(), 1);  // Matches format CHECKs in the ctor.
        flags |= FXRC_BYTEMASK_OUTPUT;
      }
      return flags;
    }
    default:
      NOTREACHED_NORETURN();
  }
}

void CFX_AggDeviceDriver::SaveState() {
  std::unique_ptr<CFX_AggClipRgn> pClip;
  if (m_pClipRgn)
    pClip = std::make_unique<CFX_AggClipRgn>(*m_pClipRgn);
  m_StateStack.push_back(std::move(pClip));
}

void CFX_AggDeviceDriver::RestoreState(bool bKeepSaved) {
  m_pClipRgn.reset();

  if (m_StateStack.empty())
    return;

  if (bKeepSaved) {
    if (m_StateStack.back())
      m_pClipRgn = std::make_unique<CFX_AggClipRgn>(*m_StateStack.back());
  } else {
    m_pClipRgn = std::move(m_StateStack.back());
    m_StateStack.pop_back();
  }
}

void CFX_AggDeviceDriver::SetClipMask(agg::rasterizer_scanline_aa& rasterizer) {
  FX_RECT path_rect(rasterizer.min_x(), rasterizer.min_y(),
                    rasterizer.max_x() + 1, rasterizer.max_y() + 1);
  path_rect.Intersect(m_pClipRgn->GetBox());
  auto pThisLayer = pdfium::MakeRetain<CFX_DIBitmap>();
  if (!path_rect.IsEmpty()) {
    CHECK(pThisLayer->Create(path_rect.Width(), path_rect.Height(),
                             FXDIB_Format::k8bppMask));
    agg::rendering_buffer raw_buf(
        pThisLayer->GetWritableBuffer().data(), pThisLayer->GetWidth(),
        pThisLayer->GetHeight(), pThisLayer->GetPitch());
    agg::pixfmt_gray8 pixel_buf(raw_buf);
    agg::renderer_base<agg::pixfmt_gray8> base_buf(pixel_buf);
    RendererScanLineAaOffset<agg::renderer_base<agg::pixfmt_gray8>>
        final_render(base_buf, path_rect.left, path_rect.top);
    final_render.color(agg::gray8(255));
    agg::scanline_u8 scanline;
    agg::render_scanlines(rasterizer, scanline, final_render,
                          m_FillOptions.aliased_path);
  }
  m_pClipRgn->IntersectMaskF(path_rect.left, path_rect.top,
                             std::move(pThisLayer));
}

bool CFX_AggDeviceDriver::SetClip_PathFill(
    const CFX_Path& path,
    const CFX_Matrix* pObject2Device,
    const CFX_FillRenderOptions& fill_options) {
  DCHECK(fill_options.fill_type != CFX_FillRenderOptions::FillType::kNoFill);

  m_FillOptions = fill_options;
  if (!m_pClipRgn) {
    m_pClipRgn = std::make_unique<CFX_AggClipRgn>(
        GetDeviceCaps(FXDC_PIXEL_WIDTH), GetDeviceCaps(FXDC_PIXEL_HEIGHT));
  }
  std::optional<CFX_FloatRect> maybe_rectf = path.GetRect(pObject2Device);
  if (maybe_rectf.has_value()) {
    CFX_FloatRect& rectf = maybe_rectf.value();
    rectf.Intersect(
        CFX_FloatRect(0, 0, static_cast<float>(GetDeviceCaps(FXDC_PIXEL_WIDTH)),
                      static_cast<float>(GetDeviceCaps(FXDC_PIXEL_HEIGHT))));
    FX_RECT rect = rectf.GetOuterRect();
    m_pClipRgn->IntersectRect(rect);
    return true;
  }
  agg::path_storage path_data = BuildAggPath(path, pObject2Device);
  path_data.end_poly();
  agg::rasterizer_scanline_aa rasterizer;
  rasterizer.clip_box(0.0f, 0.0f,
                      static_cast<float>(GetDeviceCaps(FXDC_PIXEL_WIDTH)),
                      static_cast<float>(GetDeviceCaps(FXDC_PIXEL_HEIGHT)));
  rasterizer.add_path(path_data);
  rasterizer.filling_rule(GetAlternateOrWindingFillType(fill_options));
  SetClipMask(rasterizer);
  return true;
}

bool CFX_AggDeviceDriver::SetClip_PathStroke(
    const CFX_Path& path,
    const CFX_Matrix* pObject2Device,
    const CFX_GraphStateData* pGraphState) {
  if (!m_pClipRgn) {
    m_pClipRgn = std::make_unique<CFX_AggClipRgn>(
        GetDeviceCaps(FXDC_PIXEL_WIDTH), GetDeviceCaps(FXDC_PIXEL_HEIGHT));
  }
  agg::path_storage path_data = BuildAggPath(path, nullptr);
  agg::rasterizer_scanline_aa rasterizer;
  rasterizer.clip_box(0.0f, 0.0f,
                      static_cast<float>(GetDeviceCaps(FXDC_PIXEL_WIDTH)),
                      static_cast<float>(GetDeviceCaps(FXDC_PIXEL_HEIGHT)));
  RasterizeStroke(&rasterizer, &path_data, pObject2Device, pGraphState, 1.0f,
                  false);
  rasterizer.filling_rule(agg::fill_non_zero);
  SetClipMask(rasterizer);
  return true;
}

int CFX_AggDeviceDriver::GetDriverType() const {
  return 1;
}

bool CFX_AggDeviceDriver::MultiplyAlpha(float alpha) {
  return m_pBitmap->MultiplyAlpha(alpha);
}

bool CFX_AggDeviceDriver::MultiplyAlphaMask(
    RetainPtr<const CFX_DIBitmap> mask) {
  return m_pBitmap->MultiplyAlphaMask(std::move(mask));
}

void CFX_AggDeviceDriver::Clear(uint32_t color) {
  m_pBitmap->Clear(color);
}

void CFX_AggDeviceDriver::RenderRasterizer(
    agg::rasterizer_scanline_aa& rasterizer,
    uint32_t color,
    bool bFullCover,
    bool bGroupKnockout) {
  RetainPtr<CFX_DIBitmap> pt = bGroupKnockout ? m_pBackdropBitmap : nullptr;
  CFX_AggRenderer render(m_pBitmap, pt, m_pClipRgn.get(), color, bFullCover,
                         m_bRgbByteOrder);
  agg::scanline_u8 scanline;
  agg::render_scanlines(rasterizer, scanline, render,
                        m_FillOptions.aliased_path);
}

bool CFX_AggDeviceDriver::DrawPath(const CFX_Path& path,
                                   const CFX_Matrix* pObject2Device,
                                   const CFX_GraphStateData* pGraphState,
                                   uint32_t fill_color,
                                   uint32_t stroke_color,
                                   const CFX_FillRenderOptions& fill_options) {
  if (m_pBitmap->GetBuffer().empty()) {
    return true;
  }

  m_FillOptions = fill_options;
  if (fill_options.fill_type != CFX_FillRenderOptions::FillType::kNoFill &&
      fill_color) {
    agg::path_storage path_data = BuildAggPath(path, pObject2Device);
    agg::rasterizer_scanline_aa rasterizer;
    rasterizer.clip_box(0.0f, 0.0f,
                        static_cast<float>(GetDeviceCaps(FXDC_PIXEL_WIDTH)),
                        static_cast<float>(GetDeviceCaps(FXDC_PIXEL_HEIGHT)));
    rasterizer.add_path(path_data);
    rasterizer.filling_rule(GetAlternateOrWindingFillType(fill_options));
    RenderRasterizer(rasterizer, fill_color, fill_options.full_cover,
                     /*bGroupKnockout=*/false);
  }
  int stroke_alpha = FXARGB_A(stroke_color);
  if (!pGraphState || !stroke_alpha)
    return true;

  if (fill_options.zero_area) {
    agg::path_storage path_data = BuildAggPath(path, pObject2Device);
    agg::rasterizer_scanline_aa rasterizer;
    rasterizer.clip_box(0.0f, 0.0f,
                        static_cast<float>(GetDeviceCaps(FXDC_PIXEL_WIDTH)),
                        static_cast<float>(GetDeviceCaps(FXDC_PIXEL_HEIGHT)));
    RasterizeStroke(&rasterizer, &path_data, nullptr, pGraphState, 1,
                    fill_options.stroke_text_mode);
    RenderRasterizer(rasterizer, stroke_color, fill_options.full_cover,
                     m_bGroupKnockout);
    return true;
  }
  CFX_Matrix matrix1;
  CFX_Matrix matrix2;
  if (pObject2Device) {
    matrix1.a = std::max(fabs(pObject2Device->a), fabs(pObject2Device->b));
    matrix1.d = matrix1.a;
    matrix2 = CFX_Matrix(
        pObject2Device->a / matrix1.a, pObject2Device->b / matrix1.a,
        pObject2Device->c / matrix1.d, pObject2Device->d / matrix1.d, 0, 0);

    matrix1 = *pObject2Device * matrix2.GetInverse();
  }

  agg::path_storage path_data = BuildAggPath(path, &matrix1);
  agg::rasterizer_scanline_aa rasterizer;
  rasterizer.clip_box(0.0f, 0.0f,
                      static_cast<float>(GetDeviceCaps(FXDC_PIXEL_WIDTH)),
                      static_cast<float>(GetDeviceCaps(FXDC_PIXEL_HEIGHT)));
  RasterizeStroke(&rasterizer, &path_data, &matrix2, pGraphState, matrix1.a,
                  fill_options.stroke_text_mode);
  RenderRasterizer(rasterizer, stroke_color, fill_options.full_cover,
                   m_bGroupKnockout);
  return true;
}

bool CFX_AggDeviceDriver::FillRect(const FX_RECT& rect, uint32_t fill_color) {
  if (m_pBitmap->GetBuffer().empty()) {
    return true;
  }

  FX_RECT clip_rect = GetClipBox();
  FX_RECT draw_rect = clip_rect;
  draw_rect.Intersect(rect);
  if (draw_rect.IsEmpty()) {
    return true;
  }

  if (!m_pClipRgn || m_pClipRgn->GetType() == CFX_AggClipRgn::kRectI) {
    if (m_bRgbByteOrder) {
      RgbByteOrderCompositeRect(m_pBitmap, draw_rect.left, draw_rect.top,
                                draw_rect.Width(), draw_rect.Height(),
                                fill_color);
    } else {
      m_pBitmap->CompositeRect(draw_rect.left, draw_rect.top, draw_rect.Width(),
                               draw_rect.Height(), fill_color);
    }
    return true;
  }
  m_pBitmap->CompositeMask(draw_rect.left, draw_rect.top, draw_rect.Width(),
                           draw_rect.Height(), m_pClipRgn->GetMask(),
                           fill_color, draw_rect.left - clip_rect.left,
                           draw_rect.top - clip_rect.top, BlendMode::kNormal,
                           nullptr, m_bRgbByteOrder);
  return true;
}

FX_RECT CFX_AggDeviceDriver::GetClipBox() const {
  if (m_pClipRgn) {
    return m_pClipRgn->GetBox();
  }
  return FX_RECT(0, 0, GetDeviceCaps(FXDC_PIXEL_WIDTH),
                 GetDeviceCaps(FXDC_PIXEL_HEIGHT));
}

bool CFX_AggDeviceDriver::GetDIBits(RetainPtr<CFX_DIBitmap> bitmap,
                                    int left,
                                    int top) const {
  if (m_pBitmap->GetBuffer().empty())
    return true;

  FX_RECT rect(left, top, left + bitmap->GetWidth(), top + bitmap->GetHeight());
  RetainPtr<CFX_DIBitmap> pBack;
  if (m_pBackdropBitmap) {
    pBack = m_pBackdropBitmap->ClipTo(rect);
    if (!pBack)
      return true;

    pBack->CompositeBitmap(0, 0, pBack->GetWidth(), pBack->GetHeight(),
                           m_pBitmap, 0, 0, BlendMode::kNormal, nullptr, false);
  } else {
    pBack = m_pBitmap->ClipTo(rect);
    if (!pBack)
      return true;
  }

  left = std::min(left, 0);
  top = std::min(top, 0);
  if (m_bRgbByteOrder) {
    RgbByteOrderTransferBitmap(std::move(bitmap), rect.Width(), rect.Height(),
                               std::move(pBack), left, top);
    return true;
  }
  return bitmap->TransferBitmap(rect.Width(), rect.Height(), std::move(pBack),
                                left, top);
}

RetainPtr<const CFX_DIBitmap> CFX_AggDeviceDriver::GetBackDrop() const {
  return m_pBackdropBitmap;
}

bool CFX_AggDeviceDriver::SetDIBits(RetainPtr<const CFX_DIBBase> bitmap,
                                    uint32_t argb,
                                    const FX_RECT& src_rect,
                                    int left,
                                    int top,
                                    BlendMode blend_type) {
  if (m_pBitmap->GetBuffer().empty())
    return true;

  if (bitmap->IsMaskFormat()) {
    return m_pBitmap->CompositeMask(left, top, src_rect.Width(),
                                    src_rect.Height(), std::move(bitmap), argb,
                                    src_rect.left, src_rect.top, blend_type,
                                    m_pClipRgn.get(), m_bRgbByteOrder);
  }
  return m_pBitmap->CompositeBitmap(left, top, src_rect.Width(),
                                    src_rect.Height(), std::move(bitmap),
                                    src_rect.left, src_rect.top, blend_type,
                                    m_pClipRgn.get(), m_bRgbByteOrder);
}

bool CFX_AggDeviceDriver::StretchDIBits(RetainPtr<const CFX_DIBBase> bitmap,
                                        uint32_t argb,
                                        int dest_left,
                                        int dest_top,
                                        int dest_width,
                                        int dest_height,
                                        const FX_RECT* pClipRect,
                                        const FXDIB_ResampleOptions& options,
                                        BlendMode blend_type) {
  if (m_pBitmap->GetBuffer().empty())
    return true;

  if (dest_width == bitmap->GetWidth() && dest_height == bitmap->GetHeight()) {
    FX_RECT rect(0, 0, dest_width, dest_height);
    return SetDIBits(std::move(bitmap), argb, rect, dest_left, dest_top,
                     blend_type);
  }
  FX_RECT dest_rect(dest_left, dest_top, dest_left + dest_width,
                    dest_top + dest_height);
  dest_rect.Normalize();
  FX_RECT dest_clip = dest_rect;
  dest_clip.Intersect(*pClipRect);
  CFX_AggBitmapComposer composer;
  composer.Compose(m_pBitmap, m_pClipRgn.get(), /*alpha=*/1.0f, argb, dest_clip,
                   /*bVertical=*/false, /*bFlipX=*/false, /*bFlipY=*/false,
                   m_bRgbByteOrder, blend_type);
  dest_clip.Offset(-dest_rect.left, -dest_rect.top);
  CFX_ImageStretcher stretcher(&composer, std::move(bitmap), dest_width,
                               dest_height, dest_clip, options);
  if (stretcher.Start())
    stretcher.Continue(nullptr);
  return true;
}

RenderDeviceDriverIface::StartResult CFX_AggDeviceDriver::StartDIBits(
    RetainPtr<const CFX_DIBBase> bitmap,
    float alpha,
    uint32_t argb,
    const CFX_Matrix& matrix,
    const FXDIB_ResampleOptions& options,
    BlendMode blend_type) {
  if (m_pBitmap->GetBuffer().empty()) {
    return {Result::kSuccess, nullptr};
  }

  return {Result::kSuccess, std::make_unique<CFX_AggImageRenderer>(
                                m_pBitmap, m_pClipRgn.get(), std::move(bitmap),
                                alpha, argb, matrix, options, m_bRgbByteOrder)};
}

bool CFX_AggDeviceDriver::ContinueDIBits(CFX_AggImageRenderer* pHandle,
                                         PauseIndicatorIface* pPause) {
  return m_pBitmap->GetBuffer().empty() || pHandle->Continue(pPause);
}

}  // namespace pdfium

bool CFX_DefaultRenderDevice::AttachAggImpl(
    RetainPtr<CFX_DIBitmap> pBitmap,
    bool bRgbByteOrder,
    RetainPtr<CFX_DIBitmap> pBackdropBitmap,
    bool bGroupKnockout) {
  // Unlike the Skia version, all callers pass in a non-null `pBitmap`.
  CHECK(pBitmap);
  SetBitmap(pBitmap);
  SetDeviceDriver(std::make_unique<pdfium::CFX_AggDeviceDriver>(
      std::move(pBitmap), bRgbByteOrder, std::move(pBackdropBitmap),
      bGroupKnockout));
  return true;
}

bool CFX_DefaultRenderDevice::CreateAgg(
    int width,
    int height,
    FXDIB_Format format,
    RetainPtr<CFX_DIBitmap> pBackdropBitmap) {
  auto pBitmap = pdfium::MakeRetain<CFX_DIBitmap>();
  if (!pBitmap->Create(width, height, format))
    return false;

  SetBitmap(pBitmap);
  SetDeviceDriver(std::make_unique<pdfium::CFX_AggDeviceDriver>(
      std::move(pBitmap), false, std::move(pBackdropBitmap), false));
  return true;
}
