// Copyright 2020 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXGE_WIN32_CGDI_DEVICE_DRIVER_H_
#define CORE_FXGE_WIN32_CGDI_DEVICE_DRIVER_H_

#include <windows.h>

#include <optional>

#include "core/fxcrt/retain_ptr.h"
#include "core/fxge/renderdevicedriver_iface.h"

class CFX_DIBBase;

class CGdiDeviceDriver : public RenderDeviceDriverIface {
 protected:
  CGdiDeviceDriver(HDC hDC, DeviceType device_type);
  ~CGdiDeviceDriver() override;

  // RenderDeviceDriverIface:
  DeviceType GetDeviceType() const override;
  int GetDeviceCaps(int caps_id) const override;
  void SaveState() override;
  void RestoreState(bool bKeepSaved) override;
  void SetBaseClip(const FX_RECT& rect) override;
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
  bool FillRect(const FX_RECT& rect, uint32_t fill_color) override;
  bool DrawCosmeticLine(const CFX_PointF& ptMoveTo,
                        const CFX_PointF& ptLineTo,
                        uint32_t color) override;
  FX_RECT GetClipBox() const override;
  bool MultiplyAlpha(float alpha) override;
  bool MultiplyAlphaMask(RetainPtr<const CFX_DIBitmap> mask) override;

  void DrawLine(float x1, float y1, float x2, float y2);

  bool GDI_SetDIBits(RetainPtr<const CFX_DIBBase> source,
                     const FX_RECT& src_rect,
                     int left,
                     int top);
  bool GDI_StretchDIBits(RetainPtr<const CFX_DIBBase> source,
                         int dest_left,
                         int dest_top,
                         int dest_width,
                         int dest_height,
                         const FXDIB_ResampleOptions& options);
  bool GDI_StretchBitMask(RetainPtr<const CFX_DIBBase> source,
                          int dest_left,
                          int dest_top,
                          int dest_width,
                          int dest_height,
                          uint32_t bitmap_color);

  const HDC m_hDC;
  bool m_bMetafileDCType;
  int m_Width;
  int m_Height;
  int m_nBitsPerPixel;
  const DeviceType m_DeviceType;
  int m_RenderCaps;
  std::optional<FX_RECT> m_BaseClipBox;
};

#endif  // CORE_FXGE_WIN32_CGDI_DEVICE_DRIVER_H_
