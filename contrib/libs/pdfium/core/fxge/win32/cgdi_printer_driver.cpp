// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxge/win32/cgdi_printer_driver.h"

#include <math.h>
#include <windows.h>

#include <algorithm>
#include <memory>
#include <utility>

#include "core/fxcrt/check.h"
#include "core/fxcrt/check_op.h"
#include "core/fxcrt/fx_memory.h"
#include "core/fxcrt/fx_system.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxge/agg/cfx_agg_imagerenderer.h"
#include "core/fxge/cfx_font.h"
#include "core/fxge/cfx_windowsrenderdevice.h"
#include "core/fxge/dib/cfx_dibbase.h"
#include "core/fxge/dib/cfx_dibitmap.h"
#include "core/fxge/render_defines.h"
#include "core/fxge/text_char_pos.h"

CGdiPrinterDriver::CGdiPrinterDriver(HDC hDC)
    : CGdiDeviceDriver(hDC, DeviceType::kPrinter),
      m_HorzSize(::GetDeviceCaps(m_hDC, HORZSIZE)),
      m_VertSize(::GetDeviceCaps(m_hDC, VERTSIZE)) {}

CGdiPrinterDriver::~CGdiPrinterDriver() = default;

int CGdiPrinterDriver::GetDeviceCaps(int caps_id) const {
  if (caps_id == FXDC_HORZ_SIZE)
    return m_HorzSize;
  if (caps_id == FXDC_VERT_SIZE)
    return m_VertSize;
  return CGdiDeviceDriver::GetDeviceCaps(caps_id);
}

bool CGdiPrinterDriver::SetDIBits(RetainPtr<const CFX_DIBBase> bitmap,
                                  uint32_t color,
                                  const FX_RECT& src_rect,
                                  int left,
                                  int top,
                                  BlendMode blend_type) {
  if (bitmap->IsMaskFormat()) {
    FX_RECT clip_rect(left, top, left + src_rect.Width(),
                      top + src_rect.Height());
    int dest_width = bitmap->GetWidth();
    int dest_height = bitmap->GetHeight();
    return StretchDIBits(std::move(bitmap), color, left - src_rect.left,
                         top - src_rect.top, dest_width, dest_height,
                         &clip_rect, FXDIB_ResampleOptions(),
                         BlendMode::kNormal);
  }

  DCHECK_EQ(blend_type, BlendMode::kNormal);
  if (bitmap->IsAlphaFormat()) {
    return false;
  }

  return GDI_SetDIBits(std::move(bitmap), src_rect, left, top);
}

bool CGdiPrinterDriver::StretchDIBits(RetainPtr<const CFX_DIBBase> bitmap,
                                      uint32_t color,
                                      int dest_left,
                                      int dest_top,
                                      int dest_width,
                                      int dest_height,
                                      const FX_RECT* pClipRect,
                                      const FXDIB_ResampleOptions& options,
                                      BlendMode blend_type) {
  if (bitmap->IsMaskFormat()) {
    int alpha = FXARGB_A(color);
    if (bitmap->GetBPP() != 1 || alpha != 255) {
      return false;
    }

    if (dest_width < 0 || dest_height < 0) {
      bitmap = bitmap->FlipImage(dest_width < 0, dest_height < 0);
      if (!bitmap) {
        return false;
      }

      if (dest_width < 0)
        dest_left += dest_width;
      if (dest_height < 0)
        dest_top += dest_height;

      dest_width = abs(dest_width);
      dest_height = abs(dest_height);
    }

    return GDI_StretchBitMask(std::move(bitmap), dest_left, dest_top,
                              dest_width, dest_height, color);
  }

  if (bitmap->IsAlphaFormat()) {
    return false;
  }

  if (dest_width < 0 || dest_height < 0) {
    bitmap = bitmap->FlipImage(dest_width < 0, dest_height < 0);
    if (!bitmap) {
      return false;
    }

    if (dest_width < 0)
      dest_left += dest_width;
    if (dest_height < 0)
      dest_top += dest_height;

    dest_width = abs(dest_width);
    dest_height = abs(dest_height);
  }

  return GDI_StretchDIBits(std::move(bitmap), dest_left, dest_top, dest_width,
                           dest_height, options);
}

RenderDeviceDriverIface::StartResult CGdiPrinterDriver::StartDIBits(
    RetainPtr<const CFX_DIBBase> bitmap,
    float alpha,
    uint32_t color,
    const CFX_Matrix& matrix,
    const FXDIB_ResampleOptions& options,
    BlendMode blend_type) {
  if (alpha != 1.0f || bitmap->IsAlphaFormat() ||
      (bitmap->IsMaskFormat() && (bitmap->GetBPP() != 1))) {
    return {Result::kNotSupported, nullptr};
  }
  CFX_FloatRect unit_rect = matrix.GetUnitRect();
  FX_RECT full_rect = unit_rect.GetOuterRect();
  if (fabs(matrix.b) < 0.5f && matrix.a != 0 && fabs(matrix.c) < 0.5f &&
      matrix.d != 0) {
    bool bFlipX = matrix.a < 0;
    bool bFlipY = matrix.d > 0;
    bool success = StretchDIBits(
        std::move(bitmap), color, bFlipX ? full_rect.right : full_rect.left,
        bFlipY ? full_rect.bottom : full_rect.top,
        bFlipX ? -full_rect.Width() : full_rect.Width(),
        bFlipY ? -full_rect.Height() : full_rect.Height(), nullptr,
        FXDIB_ResampleOptions(), blend_type);
    return {success ? Result::kSuccess : Result::kFailure, nullptr};
  }
  if (fabs(matrix.a) >= 0.5f || fabs(matrix.d) >= 0.5f) {
    return {Result::kNotSupported, nullptr};
  }

  const bool flip_x = matrix.c > 0;
  const bool flip_y = matrix.b < 0;
  bitmap = bitmap->SwapXY(flip_x, flip_y);
  if (!bitmap) {
    return {Result::kFailure, nullptr};
  }

  bool success =
      StretchDIBits(std::move(bitmap), color, full_rect.left, full_rect.top,
                    full_rect.Width(), full_rect.Height(), nullptr,
                    FXDIB_ResampleOptions(), blend_type);
  return {success ? Result::kSuccess : Result::kFailure, nullptr};
}

bool CGdiPrinterDriver::DrawDeviceText(
    pdfium::span<const TextCharPos> pCharPos,
    CFX_Font* pFont,
    const CFX_Matrix& mtObject2Device,
    float font_size,
    uint32_t color,
    const CFX_TextRenderOptions& /*options*/) {
  return false;
}
