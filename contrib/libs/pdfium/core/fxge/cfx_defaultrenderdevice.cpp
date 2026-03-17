// Copyright 2022 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "core/fxge/cfx_defaultrenderdevice.h"

#include <utility>

#include "core/fxge/agg/cfx_agg_devicedriver.h"
#include "core/fxge/dib/cfx_dibitmap.h"

#if defined(PDF_USE_SKIA)
#include "core/fxge/skia/fx_skia_device.h"
#endif

namespace {

// When build variant is Skia then it is assumed as the default, but might be
// overridden at runtime.
#if defined(PDF_USE_SKIA)
CFX_DefaultRenderDevice::RendererType g_renderer_type =
    CFX_DefaultRenderDevice::kDefaultRenderer;
#endif

}  // namespace

// static
bool CFX_DefaultRenderDevice::UseSkiaRenderer() {
#if defined(PDF_USE_SKIA)
  return g_renderer_type == RendererType::kSkia;
#else
  return false;
#endif
}

#if defined(PDF_USE_SKIA)
// static
void CFX_DefaultRenderDevice::SetRendererType(RendererType renderer_type) {
  g_renderer_type = renderer_type;
}
#endif

CFX_DefaultRenderDevice::CFX_DefaultRenderDevice() = default;

CFX_DefaultRenderDevice::~CFX_DefaultRenderDevice() = default;

bool CFX_DefaultRenderDevice::Attach(RetainPtr<CFX_DIBitmap> pBitmap) {
  return AttachWithRgbByteOrder(std::move(pBitmap), false);
}

bool CFX_DefaultRenderDevice::AttachWithRgbByteOrder(
    RetainPtr<CFX_DIBitmap> pBitmap,
    bool bRgbByteOrder) {
  return AttachImpl(std::move(pBitmap), bRgbByteOrder, nullptr, false);
}

bool CFX_DefaultRenderDevice::AttachWithBackdropAndGroupKnockout(
    RetainPtr<CFX_DIBitmap> pBitmap,
    RetainPtr<CFX_DIBitmap> pBackdropBitmap,
    bool bGroupKnockout) {
  return AttachImpl(std::move(pBitmap), false, std::move(pBackdropBitmap),
                    bGroupKnockout);
}

bool CFX_DefaultRenderDevice::CFX_DefaultRenderDevice::AttachImpl(
    RetainPtr<CFX_DIBitmap> pBitmap,
    bool bRgbByteOrder,
    RetainPtr<CFX_DIBitmap> pBackdropBitmap,
    bool bGroupKnockout) {
#if defined(PDF_USE_SKIA)
  if (UseSkiaRenderer()) {
    return AttachSkiaImpl(std::move(pBitmap), bRgbByteOrder,
                          std::move(pBackdropBitmap), bGroupKnockout);
  }
#endif
  return AttachAggImpl(std::move(pBitmap), bRgbByteOrder,
                       std::move(pBackdropBitmap), bGroupKnockout);
}

bool CFX_DefaultRenderDevice::Create(int width,
                                     int height,
                                     FXDIB_Format format) {
  return CreateWithBackdrop(width, height, format, nullptr);
}

bool CFX_DefaultRenderDevice::CreateWithBackdrop(
    int width,
    int height,
    FXDIB_Format format,
    RetainPtr<CFX_DIBitmap> backdrop) {
#if defined(PDF_USE_SKIA)
  if (UseSkiaRenderer()) {
    return CreateSkia(width, height, format, backdrop);
  }
#endif
  return CreateAgg(width, height, format, backdrop);
}

void CFX_DefaultRenderDevice::Clear(uint32_t color) {
#if defined(PDF_USE_SKIA)
  if (UseSkiaRenderer()) {
    static_cast<CFX_SkiaDeviceDriver*>(GetDeviceDriver())->Clear(color);
    return;
  }
#endif
  static_cast<pdfium::CFX_AggDeviceDriver*>(GetDeviceDriver())->Clear(color);
}
