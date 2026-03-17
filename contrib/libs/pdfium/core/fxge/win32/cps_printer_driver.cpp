// Copyright 2020 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxge/win32/cps_printer_driver.h"

#include <stdint.h>

#include <sstream>
#include <utility>

#include "core/fxcrt/check.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/data_vector.h"
#include "core/fxcrt/fx_system.h"
#include "core/fxcrt/notreached.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxge/agg/cfx_agg_imagerenderer.h"
#include "core/fxge/cfx_fillrenderoptions.h"
#include "core/fxge/cfx_path.h"
#include "core/fxge/dib/cfx_dibbase.h"
#include "core/fxge/dib/cfx_dibitmap.h"
#include "core/fxge/win32/cpsoutput.h"

namespace {

CFX_PSRenderer::RenderingLevel RenderingLevelFromWindowsPrintMode(
    WindowsPrintMode mode) {
  switch (mode) {
    case WindowsPrintMode::kPostScript2:
    case WindowsPrintMode::kPostScript2PassThrough:
      return CFX_PSRenderer::RenderingLevel::kLevel2;
    case WindowsPrintMode::kPostScript3:
    case WindowsPrintMode::kPostScript3PassThrough:
      return CFX_PSRenderer::RenderingLevel::kLevel3;
    case WindowsPrintMode::kPostScript3Type42:
    case WindowsPrintMode::kPostScript3Type42PassThrough:
      return CFX_PSRenderer::RenderingLevel::kLevel3Type42;
    default:
      // |mode| should be PostScript.
      NOTREACHED_NORETURN();
  }
}

}  // namespace

CPSPrinterDriver::CPSPrinterDriver(HDC hDC,
                                   WindowsPrintMode mode,
                                   CFX_PSFontTracker* ps_font_tracker,
                                   const EncoderIface* encoder_iface)
    : m_hDC(hDC), m_PSRenderer(ps_font_tracker, encoder_iface) {
  CFX_PSRenderer::RenderingLevel level =
      RenderingLevelFromWindowsPrintMode(mode);
  CPSOutput::OutputMode output_mode =
      (mode == WindowsPrintMode::kPostScript2 ||
       mode == WindowsPrintMode::kPostScript3 ||
       mode == WindowsPrintMode::kPostScript3Type42)
          ? CPSOutput::OutputMode::kGdiComment
          : CPSOutput::OutputMode::kExtEscape;

  m_HorzSize = ::GetDeviceCaps(m_hDC, HORZSIZE);
  m_VertSize = ::GetDeviceCaps(m_hDC, VERTSIZE);
  m_Width = ::GetDeviceCaps(m_hDC, HORZRES);
  m_Height = ::GetDeviceCaps(m_hDC, VERTRES);
  m_nBitsPerPixel = ::GetDeviceCaps(m_hDC, BITSPIXEL);

  m_PSRenderer.Init(pdfium::MakeRetain<CPSOutput>(m_hDC, output_mode), level,
                    m_Width, m_Height);
  HRGN hRgn = ::CreateRectRgn(0, 0, 1, 1);
  if (::GetClipRgn(m_hDC, hRgn) == 1) {
    DWORD dwCount = ::GetRegionData(hRgn, 0, nullptr);
    if (dwCount) {
      DataVector<uint8_t> buffer(dwCount);
      RGNDATA* pData = reinterpret_cast<RGNDATA*>(buffer.data());
      if (::GetRegionData(hRgn, dwCount, pData)) {
        CFX_Path path;
        for (uint32_t i = 0; i < pData->rdh.nCount; i++) {
          RECT* pRect = UNSAFE_TODO(
              reinterpret_cast<RECT*>(pData->Buffer + pData->rdh.nRgnSize * i));
          path.AppendRect(static_cast<float>(pRect->left),
                          static_cast<float>(pRect->bottom),
                          static_cast<float>(pRect->right),
                          static_cast<float>(pRect->top));
        }
        m_PSRenderer.SetClip_PathFill(path, nullptr,
                                      CFX_FillRenderOptions::WindingOptions());
      }
    }
  }
  ::DeleteObject(hRgn);
}

CPSPrinterDriver::~CPSPrinterDriver() = default;

DeviceType CPSPrinterDriver::GetDeviceType() const {
  return DeviceType::kPrinter;
}

int CPSPrinterDriver::GetDeviceCaps(int caps_id) const {
  switch (caps_id) {
    case FXDC_PIXEL_WIDTH:
      return m_Width;
    case FXDC_PIXEL_HEIGHT:
      return m_Height;
    case FXDC_BITS_PIXEL:
      return m_nBitsPerPixel;
    case FXDC_RENDER_CAPS:
      return 0;
    case FXDC_HORZ_SIZE:
      return m_HorzSize;
    case FXDC_VERT_SIZE:
      return m_VertSize;
    default:
      NOTREACHED_NORETURN();
  }
}

void CPSPrinterDriver::SaveState() {
  m_PSRenderer.SaveState();
}

void CPSPrinterDriver::RestoreState(bool bKeepSaved) {
  m_PSRenderer.RestoreState(bKeepSaved);
}

bool CPSPrinterDriver::SetClip_PathFill(
    const CFX_Path& path,
    const CFX_Matrix* pObject2Device,
    const CFX_FillRenderOptions& fill_options) {
  m_PSRenderer.SetClip_PathFill(path, pObject2Device, fill_options);
  return true;
}

bool CPSPrinterDriver::SetClip_PathStroke(
    const CFX_Path& path,
    const CFX_Matrix* pObject2Device,
    const CFX_GraphStateData* pGraphState) {
  m_PSRenderer.SetClip_PathStroke(path, pObject2Device, pGraphState);
  return true;
}

bool CPSPrinterDriver::DrawPath(const CFX_Path& path,
                                const CFX_Matrix* pObject2Device,
                                const CFX_GraphStateData* pGraphState,
                                FX_ARGB fill_color,
                                FX_ARGB stroke_color,
                                const CFX_FillRenderOptions& fill_options) {
  return m_PSRenderer.DrawPath(path, pObject2Device, pGraphState, fill_color,
                               stroke_color, fill_options);
}

FX_RECT CPSPrinterDriver::GetClipBox() const {
  return m_PSRenderer.GetClipBox();
}

bool CPSPrinterDriver::SetDIBits(RetainPtr<const CFX_DIBBase> bitmap,
                                 uint32_t color,
                                 const FX_RECT& src_rect,
                                 int left,
                                 int top,
                                 BlendMode blend_type) {
  if (blend_type != BlendMode::kNormal)
    return false;
  return m_PSRenderer.SetDIBits(std::move(bitmap), color, left, top);
}

bool CPSPrinterDriver::StretchDIBits(RetainPtr<const CFX_DIBBase> bitmap,
                                     uint32_t color,
                                     int dest_left,
                                     int dest_top,
                                     int dest_width,
                                     int dest_height,
                                     const FX_RECT* pClipRect,
                                     const FXDIB_ResampleOptions& options,
                                     BlendMode blend_type) {
  return blend_type == BlendMode::kNormal &&
         m_PSRenderer.StretchDIBits(std::move(bitmap), color, dest_left,
                                    dest_top, dest_width, dest_height, options);
}

RenderDeviceDriverIface::StartResult CPSPrinterDriver::StartDIBits(
    RetainPtr<const CFX_DIBBase> bitmap,
    float alpha,
    uint32_t color,
    const CFX_Matrix& matrix,
    const FXDIB_ResampleOptions& options,
    BlendMode blend_type) {
  if (blend_type != BlendMode::kNormal || alpha != 1.0f) {
    return {Result::kNotSupported, nullptr};
  }

  bool success =
      m_PSRenderer.DrawDIBits(std::move(bitmap), color, matrix, options);
  return {success ? Result::kSuccess : Result::kFailure, nullptr};
}

bool CPSPrinterDriver::DrawDeviceText(
    pdfium::span<const TextCharPos> pCharPos,
    CFX_Font* pFont,
    const CFX_Matrix& mtObject2Device,
    float font_size,
    uint32_t color,
    const CFX_TextRenderOptions& /*options*/) {
  return m_PSRenderer.DrawText(pCharPos.size(), pCharPos.data(), pFont,
                               mtObject2Device, font_size, color);
}

bool CPSPrinterDriver::MultiplyAlpha(float alpha) {
  // PostScript doesn't support transparency. All callers are using
  // `CFX_DIBitmap`-backed raster devices anyway.
  NOTREACHED_NORETURN();
}

bool CPSPrinterDriver::MultiplyAlphaMask(RetainPtr<const CFX_DIBitmap> mask) {
  // PostScript doesn't support transparency. All callers are using
  // `CFX_DIBitmap`-backed raster devices anyway.
  NOTREACHED_NORETURN();
}
