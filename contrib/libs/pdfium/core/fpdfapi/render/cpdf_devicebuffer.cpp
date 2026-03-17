// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/render/cpdf_devicebuffer.h"

#include <utility>

#include "build/build_config.h"
#include "core/fpdfapi/page/cpdf_pageobject.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/render/cpdf_renderoptions.h"
#include "core/fxge/cfx_defaultrenderdevice.h"
#include "core/fxge/cfx_renderdevice.h"
#include "core/fxge/dib/cfx_dibitmap.h"
#include "core/fxge/dib/fx_dib.h"

#if BUILDFLAG(IS_WIN)
#include "core/fpdfapi/render/cpdf_rendercontext.h"
#else
#include "core/fxcrt/notreached.h"
#endif

namespace {

#if BUILDFLAG(IS_WIN)
constexpr bool kScaleDeviceBuffer = true;
#else
constexpr bool kScaleDeviceBuffer = false;
#endif

}  // namespace

// static
CFX_Matrix CPDF_DeviceBuffer::CalculateMatrix(CFX_RenderDevice* pDevice,
                                              const FX_RECT& rect,
                                              int max_dpi,
                                              bool scale) {
  CFX_Matrix matrix;
  matrix.Translate(-rect.left, -rect.top);
  if (scale) {
    int horz_size = pDevice->GetDeviceCaps(FXDC_HORZ_SIZE);
    int vert_size = pDevice->GetDeviceCaps(FXDC_VERT_SIZE);
    if (horz_size && vert_size && max_dpi) {
      int dpih =
          pDevice->GetDeviceCaps(FXDC_PIXEL_WIDTH) * 254 / (horz_size * 10);
      int dpiv =
          pDevice->GetDeviceCaps(FXDC_PIXEL_HEIGHT) * 254 / (vert_size * 10);
      if (dpih > max_dpi)
        matrix.Scale(static_cast<float>(max_dpi) / dpih, 1.0f);
      if (dpiv > max_dpi)
        matrix.Scale(1.0f, static_cast<float>(max_dpi) / dpiv);
    }
  }
  return matrix;
}

CPDF_DeviceBuffer::CPDF_DeviceBuffer(CPDF_RenderContext* pContext,
                                     CFX_RenderDevice* pDevice,
                                     const FX_RECT& rect,
                                     const CPDF_PageObject* pObj,
                                     int max_dpi)
    : m_pDevice(pDevice),
#if BUILDFLAG(IS_WIN)
      m_pContext(pContext),
#endif
      m_pObject(pObj),
      m_pBitmap(pdfium::MakeRetain<CFX_DIBitmap>()),
      m_Rect(rect),
      m_Matrix(CalculateMatrix(pDevice, rect, max_dpi, kScaleDeviceBuffer)) {
}

CPDF_DeviceBuffer::~CPDF_DeviceBuffer() = default;

RetainPtr<CFX_DIBitmap> CPDF_DeviceBuffer::Initialize() {
  FX_RECT bitmap_rect =
      m_Matrix.TransformRect(CFX_FloatRect(m_Rect)).GetOuterRect();
  // TODO(crbug.com/355630557): Consider adding support for
  // `FXDIB_Format::kBgraPremul`
  if (!m_pBitmap->Create(bitmap_rect.Width(), bitmap_rect.Height(),
                         FXDIB_Format::kBgra)) {
    return nullptr;
  }
  return m_pBitmap;
}

void CPDF_DeviceBuffer::OutputToDevice() {
  if (m_pDevice->GetDeviceCaps(FXDC_RENDER_CAPS) & FXRC_GET_BITS) {
    if (m_Matrix.a == 1.0f && m_Matrix.d == 1.0f) {
      m_pDevice->SetDIBits(m_pBitmap, m_Rect.left, m_Rect.top);
      return;
    }

#if BUILDFLAG(IS_WIN)
    m_pDevice->StretchDIBits(m_pBitmap, m_Rect.left, m_Rect.top, m_Rect.Width(),
                             m_Rect.Height());
    return;
#else
    NOTREACHED_NORETURN();
#endif
  }

#if BUILDFLAG(IS_WIN)
  auto buffer = pdfium::MakeRetain<CFX_DIBitmap>();
  if (!m_pDevice->CreateCompatibleBitmap(buffer, m_pBitmap->GetWidth(),
                                         m_pBitmap->GetHeight())) {
    return;
  }
  m_pContext->GetBackgroundToBitmap(buffer, m_pObject, m_Matrix);
  buffer->CompositeBitmap(0, 0, buffer->GetWidth(), buffer->GetHeight(),
                          m_pBitmap, 0, 0, BlendMode::kNormal, nullptr, false);
  m_pDevice->StretchDIBits(std::move(buffer), m_Rect.left, m_Rect.top,
                           m_Rect.Width(), m_Rect.Height());
#else
  NOTREACHED_NORETURN();
#endif
}
