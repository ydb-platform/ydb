// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXGE_CFX_RENDERDEVICE_H_
#define CORE_FXGE_CFX_RENDERDEVICE_H_

#include <memory>
#include <vector>

#include "build/build_config.h"
#include "core/fxcrt/fx_coordinates.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/span.h"
#include "core/fxcrt/unowned_ptr.h"
#include "core/fxge/cfx_path.h"
#include "core/fxge/dib/fx_dib.h"
#include "core/fxge/render_defines.h"
#include "core/fxge/renderdevicedriver_iface.h"

class CFX_AggImageRenderer;
class CFX_DIBBase;
class CFX_DIBitmap;
class CFX_Font;
class CFX_GraphStateData;
class PauseIndicatorIface;
class TextCharPos;
struct CFX_Color;
struct CFX_FillRenderOptions;
struct CFX_TextRenderOptions;

enum class BorderStyle { kSolid, kDash, kBeveled, kInset, kUnderline };

// Base class for all render devices. Derived classes must call
// SetDeviceDriver() to fully initialize the class. Until then, class methods
// are not safe to call, or may return invalid results.
class CFX_RenderDevice {
 public:
  class StateRestorer {
   public:
    explicit StateRestorer(CFX_RenderDevice* pDevice);
    ~StateRestorer();

   private:
    UnownedPtr<CFX_RenderDevice> m_pDevice;
  };

  virtual ~CFX_RenderDevice();

  static CFX_Matrix GetFlipMatrix(float width,
                                  float height,
                                  float left,
                                  float top);

  void SaveState();
  void RestoreState(bool bKeepSaved);

  int GetWidth() const { return m_Width; }
  int GetHeight() const { return m_Height; }
  DeviceType GetDeviceType() const { return m_DeviceType; }
  int GetRenderCaps() const { return m_RenderCaps; }
  int GetDeviceCaps(int id) const;
  RetainPtr<CFX_DIBitmap> GetBitmap();
  RetainPtr<const CFX_DIBitmap> GetBitmap() const;
  [[nodiscard]] bool CreateCompatibleBitmap(const RetainPtr<CFX_DIBitmap>& pDIB,
                                            int width,
                                            int height) const;
  const FX_RECT& GetClipBox() const { return m_ClipBox; }
  void SetBaseClip(const FX_RECT& rect);
  bool SetClip_PathFill(const CFX_Path& path,
                        const CFX_Matrix* pObject2Device,
                        const CFX_FillRenderOptions& fill_options);
  bool SetClip_PathStroke(const CFX_Path& path,
                          const CFX_Matrix* pObject2Device,
                          const CFX_GraphStateData* pGraphState);
  bool SetClip_Rect(const FX_RECT& pRect);
  bool DrawPath(const CFX_Path& path,
                const CFX_Matrix* pObject2Device,
                const CFX_GraphStateData* pGraphState,
                uint32_t fill_color,
                uint32_t stroke_color,
                const CFX_FillRenderOptions& fill_options);
  bool FillRect(const FX_RECT& rect, uint32_t color);

  RetainPtr<const CFX_DIBitmap> GetBackDrop() const;
  bool GetDIBits(RetainPtr<CFX_DIBitmap> bitmap, int left, int top) const;
  bool SetDIBits(RetainPtr<const CFX_DIBBase> bitmap, int left, int top);
  bool SetDIBitsWithBlend(RetainPtr<const CFX_DIBBase> bitmap,
                          int left,
                          int top,
                          BlendMode blend_mode);
  bool StretchDIBits(RetainPtr<const CFX_DIBBase> bitmap,
                     int left,
                     int top,
                     int dest_width,
                     int dest_height);
  bool StretchDIBitsWithFlagsAndBlend(RetainPtr<const CFX_DIBBase> bitmap,
                                      int left,
                                      int top,
                                      int dest_width,
                                      int dest_height,
                                      const FXDIB_ResampleOptions& options,
                                      BlendMode blend_mode);
  bool SetBitMask(RetainPtr<const CFX_DIBBase> bitmap,
                  int left,
                  int top,
                  uint32_t argb);
  bool StretchBitMask(RetainPtr<CFX_DIBBase> bitmap,
                      int left,
                      int top,
                      int dest_width,
                      int dest_height,
                      uint32_t color);
  bool StretchBitMaskWithFlags(RetainPtr<CFX_DIBBase> bitmap,
                               int left,
                               int top,
                               int dest_width,
                               int dest_height,
                               uint32_t argb,
                               const FXDIB_ResampleOptions& options);
  RenderDeviceDriverIface::StartResult StartDIBits(
      RetainPtr<const CFX_DIBBase> bitmap,
      float alpha,
      uint32_t argb,
      const CFX_Matrix& matrix,
      const FXDIB_ResampleOptions& options);
  RenderDeviceDriverIface::StartResult StartDIBitsWithBlend(
      RetainPtr<const CFX_DIBBase> bitmap,
      float alpha,
      uint32_t argb,
      const CFX_Matrix& matrix,
      const FXDIB_ResampleOptions& options,
      BlendMode blend_mode);
  bool ContinueDIBits(CFX_AggImageRenderer* handle,
                      PauseIndicatorIface* pPause);

  bool DrawNormalText(pdfium::span<const TextCharPos> pCharPos,
                      CFX_Font* pFont,
                      float font_size,
                      const CFX_Matrix& mtText2Device,
                      uint32_t fill_color,
                      const CFX_TextRenderOptions& options);
  bool DrawTextPath(pdfium::span<const TextCharPos> pCharPos,
                    CFX_Font* pFont,
                    float font_size,
                    const CFX_Matrix& mtText2User,
                    const CFX_Matrix* pUser2Device,
                    const CFX_GraphStateData* pGraphState,
                    uint32_t fill_color,
                    uint32_t stroke_color,
                    CFX_Path* pClippingPath,
                    const CFX_FillRenderOptions& fill_options);

  void DrawFillRect(const CFX_Matrix* pUser2Device,
                    const CFX_FloatRect& rect,
                    const CFX_Color& color,
                    int32_t nTransparency);
  void DrawFillRect(const CFX_Matrix* pUser2Device,
                    const CFX_FloatRect& rect,
                    const FX_COLORREF& color);
  void DrawStrokeRect(const CFX_Matrix& mtUser2Device,
                      const CFX_FloatRect& rect,
                      const FX_COLORREF& color,
                      float fWidth);
  void DrawStrokeLine(const CFX_Matrix* pUser2Device,
                      const CFX_PointF& ptMoveTo,
                      const CFX_PointF& ptLineTo,
                      const FX_COLORREF& color,
                      float fWidth);
  void DrawBorder(const CFX_Matrix* pUser2Device,
                  const CFX_FloatRect& rect,
                  float fWidth,
                  const CFX_Color& color,
                  const CFX_Color& crLeftTop,
                  const CFX_Color& crRightBottom,
                  BorderStyle nStyle,
                  int32_t nTransparency);
  void DrawFillArea(const CFX_Matrix& mtUser2Device,
                    const std::vector<CFX_PointF>& points,
                    const FX_COLORREF& color);
  void DrawShadow(const CFX_Matrix& mtUser2Device,
                  const CFX_FloatRect& rect,
                  int32_t nTransparency,
                  int32_t nStartGray,
                  int32_t nEndGray);

  // See RenderDeviceDriverIface methods of the same name.
  bool MultiplyAlpha(float alpha);
  bool MultiplyAlphaMask(RetainPtr<const CFX_DIBitmap> mask);

#if defined(PDF_USE_SKIA)
  bool DrawShading(const CPDF_ShadingPattern& pattern,
                   const CFX_Matrix& matrix,
                   const FX_RECT& clip_rect,
                   int alpha);
  bool SetBitsWithMask(RetainPtr<const CFX_DIBBase> bitmap,
                       RetainPtr<const CFX_DIBBase> mask,
                       int left,
                       int top,
                       float alpha,
                       BlendMode blend_type);
  void SyncInternalBitmaps();
#endif  // defined(PDF_USE_SKIA)

 protected:
  CFX_RenderDevice();

  void SetBitmap(RetainPtr<CFX_DIBitmap> bitmap);

  void SetDeviceDriver(std::unique_ptr<RenderDeviceDriverIface> pDriver);
  RenderDeviceDriverIface* GetDeviceDriver() const {
    return m_pDeviceDriver.get();
  }

 private:
  void InitDeviceInfo();
  void UpdateClipBox();
  bool DrawFillStrokePath(const CFX_Path& path,
                          const CFX_Matrix* pObject2Device,
                          const CFX_GraphStateData* pGraphState,
                          uint32_t fill_color,
                          uint32_t stroke_color,
                          const CFX_FillRenderOptions& fill_options);
  bool DrawCosmeticLine(const CFX_PointF& ptMoveTo,
                        const CFX_PointF& ptLineTo,
                        uint32_t color,
                        const CFX_FillRenderOptions& fill_options);
  void DrawZeroAreaPath(const std::vector<CFX_Path::Point>& path,
                        const CFX_Matrix* matrix,
                        bool adjust,
                        bool aliased_path,
                        uint32_t fill_color,
                        uint8_t fill_alpha);

  RetainPtr<CFX_DIBitmap> m_pBitmap;
  int m_Width = 0;
  int m_Height = 0;
  int m_bpp = 0;
  int m_RenderCaps = 0;
  DeviceType m_DeviceType = DeviceType::kDisplay;
  FX_RECT m_ClipBox;
  std::unique_ptr<RenderDeviceDriverIface> m_pDeviceDriver;
};

#endif  // CORE_FXGE_CFX_RENDERDEVICE_H_
