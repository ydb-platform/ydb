// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxge/renderdevicedriver_iface.h"

#include <utility>

#include "core/fxcrt/fx_coordinates.h"
#include "core/fxge/agg/cfx_agg_imagerenderer.h"
#include "core/fxge/cfx_path.h"
#include "core/fxge/dib/cfx_dibitmap.h"

RenderDeviceDriverIface::~RenderDeviceDriverIface() = default;

bool RenderDeviceDriverIface::SetClip_PathStroke(
    const CFX_Path& path,
    const CFX_Matrix* pObject2Device,
    const CFX_GraphStateData* pGraphState) {
  return false;
}

void RenderDeviceDriverIface::SetBaseClip(const FX_RECT& rect) {}

bool RenderDeviceDriverIface::FillRect(const FX_RECT& rect,
                                       uint32_t fill_color) {
  return false;
}

bool RenderDeviceDriverIface::DrawCosmeticLine(const CFX_PointF& ptMoveTo,
                                               const CFX_PointF& ptLineTo,
                                               uint32_t color) {
  return false;
}

bool RenderDeviceDriverIface::GetDIBits(RetainPtr<CFX_DIBitmap> bitmap,
                                        int left,
                                        int top) const {
  return false;
}

RetainPtr<const CFX_DIBitmap> RenderDeviceDriverIface::GetBackDrop() const {
  return RetainPtr<const CFX_DIBitmap>();
}

bool RenderDeviceDriverIface::ContinueDIBits(CFX_AggImageRenderer* handle,
                                             PauseIndicatorIface* pPause) {
  return false;
}

bool RenderDeviceDriverIface::DrawDeviceText(
    pdfium::span<const TextCharPos> pCharPos,
    CFX_Font* pFont,
    const CFX_Matrix& mtObject2Device,
    float font_size,
    uint32_t color,
    const CFX_TextRenderOptions& options) {
  return false;
}

int RenderDeviceDriverIface::GetDriverType() const {
  return 0;
}

#if defined(PDF_USE_SKIA)
bool RenderDeviceDriverIface::DrawShading(const CPDF_ShadingPattern& pattern,
                                          const CFX_Matrix& matrix,
                                          const FX_RECT& clip_rect,
                                          int alpha) {
  return false;
}

bool RenderDeviceDriverIface::SetBitsWithMask(
    RetainPtr<const CFX_DIBBase> bitmap,
    RetainPtr<const CFX_DIBBase> mask,
    int left,
    int top,
    float alpha,
    BlendMode blend_type) {
  return false;
}

void RenderDeviceDriverIface::SetGroupKnockout(bool group_knockout) {}

void RenderDeviceDriverIface::SyncInternalBitmaps() {}
#endif  // defined(PDF_USE_SKIA)

RenderDeviceDriverIface::StartResult::StartResult(
    Result result,
    std::unique_ptr<CFX_AggImageRenderer> agg_image_renderer)
    : result(result), agg_image_renderer(std::move(agg_image_renderer)) {}

RenderDeviceDriverIface::StartResult::~StartResult() = default;
