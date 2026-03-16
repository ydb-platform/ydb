// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXGE_AGG_CFX_AGG_BITMAPCOMPOSER_H_
#define CORE_FXGE_AGG_CFX_AGG_BITMAPCOMPOSER_H_

#include <stdint.h>

#include "core/fxcrt/data_vector.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/unowned_ptr.h"
#include "core/fxge/dib/cfx_scanlinecompositor.h"
#include "core/fxge/dib/fx_dib.h"
#include "core/fxge/dib/scanlinecomposer_iface.h"

class CFX_AggClipRgn;
class CFX_DIBitmap;
struct FX_RECT;

class CFX_AggBitmapComposer final : public ScanlineComposerIface {
 public:
  CFX_AggBitmapComposer();
  ~CFX_AggBitmapComposer() override;

  void Compose(const RetainPtr<CFX_DIBitmap>& pDest,
               const CFX_AggClipRgn* pClipRgn,
               float alpha,
               uint32_t mask_color,
               const FX_RECT& dest_rect,
               bool bVertical,
               bool bFlipX,
               bool bFlipY,
               bool bRgbByteOrder,
               BlendMode blend_mode);

  // ScanlineComposerIface:
  bool SetInfo(int width,
               int height,
               FXDIB_Format src_format,
               DataVector<uint32_t> src_palette) override;
  void ComposeScanline(int line, pdfium::span<const uint8_t> scanline) override;

 private:
  void DoCompose(pdfium::span<uint8_t> dest_scan,
                 pdfium::span<const uint8_t> src_scan,
                 int dest_width,
                 pdfium::span<const uint8_t> clip_scan);
  void ComposeScanlineV(int line, pdfium::span<const uint8_t> scanline);

  RetainPtr<CFX_DIBitmap> m_pBitmap;
  UnownedPtr<const CFX_AggClipRgn> m_pClipRgn;
  FXDIB_Format m_SrcFormat;
  int m_DestLeft;
  int m_DestTop;
  int m_DestWidth;
  int m_DestHeight;
  float m_Alpha;
  uint32_t m_MaskColor;
  RetainPtr<CFX_DIBitmap> m_pClipMask;
  CFX_ScanlineCompositor m_Compositor;
  bool m_bVertical;
  bool m_bFlipX;
  bool m_bFlipY;
  bool m_bRgbByteOrder = false;
  BlendMode m_BlendMode = BlendMode::kNormal;
  DataVector<uint8_t> m_pScanlineV;
  DataVector<uint8_t> m_pClipScanV;
  DataVector<uint8_t> m_pAddClipScan;
};

#endif  // CORE_FXGE_AGG_CFX_AGG_BITMAPCOMPOSER_H_
