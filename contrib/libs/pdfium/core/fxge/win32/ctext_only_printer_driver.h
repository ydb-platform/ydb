// Copyright 2020 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FXGE_WIN32_CTEXT_ONLY_PRINTER_DRIVER_H_
#define CORE_FXGE_WIN32_CTEXT_ONLY_PRINTER_DRIVER_H_

#include <stdint.h>
#include <windows.h>

#include <memory>

#include "core/fxge/cfx_windowsrenderdevice.h"

class CTextOnlyPrinterDriver final : public RenderDeviceDriverIface {
 public:
  explicit CTextOnlyPrinterDriver(HDC hDC);
  ~CTextOnlyPrinterDriver() override;

 private:
  // RenderDeviceDriverIface:
  DeviceType GetDeviceType() const override;
  int GetDeviceCaps(int caps_id) const override;
  void SaveState() override;
  void RestoreState(bool bKeepSaved) override;
  bool SetClip_PathFill(const CFX_Path& path,
                        const CFX_Matrix* pObject2Device,
                        const CFX_FillRenderOptions& fill_options) override;
  bool SetClip_PathStroke(const CFX_Path& path,
                          const CFX_Matrix* pObject2Device,
                          const CFX_GraphStateData* pGraphState) override;
  bool DrawPath(const CFX_Path& path,
                const CFX_Matrix* pObject2Device,
                const CFX_GraphStateData* pGraphState,
                uint32_t fill_color,
                uint32_t stroke_color,
                const CFX_FillRenderOptions& fill_options) override;
  FX_RECT GetClipBox() const override;
  bool SetDIBits(RetainPtr<const CFX_DIBBase> bitmap,
                 uint32_t color,
                 const FX_RECT& src_rect,
                 int left,
                 int top,
                 BlendMode blend_type) override;
  bool StretchDIBits(RetainPtr<const CFX_DIBBase> bitmap,
                     uint32_t color,
                     int dest_left,
                     int dest_top,
                     int dest_width,
                     int dest_height,
                     const FX_RECT* pClipRect,
                     const FXDIB_ResampleOptions& options,
                     BlendMode blend_type) override;
  StartResult StartDIBits(RetainPtr<const CFX_DIBBase> bitmap,
                          float alpha,
                          uint32_t color,
                          const CFX_Matrix& matrix,
                          const FXDIB_ResampleOptions& options,
                          BlendMode blend_type) override;
  bool DrawDeviceText(pdfium::span<const TextCharPos> pCharPos,
                      CFX_Font* pFont,
                      const CFX_Matrix& mtObject2Device,
                      float font_size,
                      uint32_t color,
                      const CFX_TextRenderOptions& options) override;
  bool MultiplyAlpha(float alpha) override;
  bool MultiplyAlphaMask(RetainPtr<const CFX_DIBitmap> mask) override;

  HDC m_hDC;
  const int m_Width;
  const int m_Height;
  int m_nBitsPerPixel;
  const int m_HorzSize;
  const int m_VertSize;
  float m_OriginY;
  bool m_SetOrigin;
};

#endif  // CORE_FXGE_WIN32_CTEXT_ONLY_PRINTER_DRIVER_H_
