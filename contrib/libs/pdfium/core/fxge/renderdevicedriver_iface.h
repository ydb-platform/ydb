// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXGE_RENDERDEVICEDRIVER_IFACE_H_
#define CORE_FXGE_RENDERDEVICEDRIVER_IFACE_H_

#include <stdint.h>

#include <memory>

#include "build/build_config.h"
#include "core/fxcrt/fx_coordinates.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/span.h"
#include "core/fxge/dib/fx_dib.h"

class CFX_AggImageRenderer;
class CFX_DIBBase;
class CFX_DIBitmap;
class CFX_Font;
class CFX_GraphStateData;
class CFX_Matrix;
class CFX_Path;
class CPDF_ShadingPattern;
class PauseIndicatorIface;
class TextCharPos;
struct CFX_FillRenderOptions;
struct CFX_TextRenderOptions;
struct FX_RECT;

enum class DeviceType : bool {
  kDisplay,
#if BUILDFLAG(IS_WIN)
  kPrinter,
#endif
};

class RenderDeviceDriverIface {
 public:
  enum class Result {
    kFailure,
    kSuccess,
#if BUILDFLAG(IS_WIN)
    kNotSupported
#endif
  };

  struct StartResult {
    StartResult(Result result,
                std::unique_ptr<CFX_AggImageRenderer> agg_image_renderer);
    ~StartResult();

    const Result result;
    std::unique_ptr<CFX_AggImageRenderer> agg_image_renderer;
  };

  virtual ~RenderDeviceDriverIface();

  virtual DeviceType GetDeviceType() const = 0;
  virtual int GetDeviceCaps(int caps_id) const = 0;

  virtual void SaveState() = 0;
  virtual void RestoreState(bool bKeepSaved) = 0;

  virtual void SetBaseClip(const FX_RECT& rect);
  virtual bool SetClip_PathFill(const CFX_Path& path,
                                const CFX_Matrix* pObject2Device,
                                const CFX_FillRenderOptions& fill_options) = 0;
  virtual bool SetClip_PathStroke(const CFX_Path& path,
                                  const CFX_Matrix* pObject2Device,
                                  const CFX_GraphStateData* pGraphState);
  virtual bool DrawPath(const CFX_Path& path,
                        const CFX_Matrix* pObject2Device,
                        const CFX_GraphStateData* pGraphState,
                        uint32_t fill_color,
                        uint32_t stroke_color,
                        const CFX_FillRenderOptions& fill_options) = 0;
  virtual bool FillRect(const FX_RECT& rect, uint32_t fill_color);
  virtual bool DrawCosmeticLine(const CFX_PointF& ptMoveTo,
                                const CFX_PointF& ptLineTo,
                                uint32_t color);

  virtual FX_RECT GetClipBox() const = 0;
  virtual bool GetDIBits(RetainPtr<CFX_DIBitmap> bitmap,
                         int left,
                         int top) const;
  virtual RetainPtr<const CFX_DIBitmap> GetBackDrop() const;
  virtual bool SetDIBits(RetainPtr<const CFX_DIBBase> bitmap,
                         uint32_t color,
                         const FX_RECT& src_rect,
                         int dest_left,
                         int dest_top,
                         BlendMode blend_type) = 0;
  virtual bool StretchDIBits(RetainPtr<const CFX_DIBBase> bitmap,
                             uint32_t color,
                             int dest_left,
                             int dest_top,
                             int dest_width,
                             int dest_height,
                             const FX_RECT* pClipRect,
                             const FXDIB_ResampleOptions& options,
                             BlendMode blend_type) = 0;
  virtual StartResult StartDIBits(RetainPtr<const CFX_DIBBase> bitmap,
                                  float alpha,
                                  uint32_t color,
                                  const CFX_Matrix& matrix,
                                  const FXDIB_ResampleOptions& options,
                                  BlendMode blend_type) = 0;
  virtual bool ContinueDIBits(CFX_AggImageRenderer* handle,
                              PauseIndicatorIface* pPause);
  virtual bool DrawDeviceText(pdfium::span<const TextCharPos> pCharPos,
                              CFX_Font* pFont,
                              const CFX_Matrix& mtObject2Device,
                              float font_size,
                              uint32_t color,
                              const CFX_TextRenderOptions& options);
  virtual int GetDriverType() const;
#if defined(PDF_USE_SKIA)
  virtual bool DrawShading(const CPDF_ShadingPattern& pattern,
                           const CFX_Matrix& matrix,
                           const FX_RECT& clip_rect,
                           int alpha);
  virtual bool SetBitsWithMask(RetainPtr<const CFX_DIBBase> bitmap,
                               RetainPtr<const CFX_DIBBase> mask,
                               int left,
                               int top,
                               float alpha,
                               BlendMode blend_type);
  virtual void SetGroupKnockout(bool group_knockout);

  // For `CFX_SkiaDeviceDriver` only:
  // Syncs the current rendering result from the internal buffer to the output
  // bitmap if such internal buffer exists.
  virtual void SyncInternalBitmaps();
#endif  // defined(PDF_USE_SKIA)

  // Multiplies the device by a constant alpha, returning `true` on success.
  // Implementations CHECK the following conditions:
  // - `this` is bitmap-based and `this` is not of a mask format.
  //
  // The backing bitmap for `this` will be converted to format
  // `FXDIB_Format::kBgra` on success when `alpha` is not 1.
  virtual bool MultiplyAlpha(float alpha) = 0;

  // Multiplies the device by an alpha mask, returning `true` on success.
  // Implementations CHECK the following conditions:
  // - `this` is bitmap-based and of format `FXDIB_Format::kBgra` or
  //   `FXDIB_Format::kBgrx`.
  // - `mask` must be of format `FXDIB_Format::k8bppMask`.
  // - `mask` must have the same dimensions as `this`.
  //
  // The backing bitmap for `this` will be converted to format
  // `FXDIB_Format::kBgra` on success.
  virtual bool MultiplyAlphaMask(RetainPtr<const CFX_DIBitmap> mask) = 0;
};

#endif  // CORE_FXGE_RENDERDEVICEDRIVER_IFACE_H_
