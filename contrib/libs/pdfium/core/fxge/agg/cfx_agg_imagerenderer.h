// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXGE_AGG_CFX_AGG_IMAGERENDERER_H_
#define CORE_FXGE_AGG_CFX_AGG_IMAGERENDERER_H_

#include <memory>

#include "core/fxcrt/fx_coordinates.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/unowned_ptr.h"
#include "core/fxge/agg/cfx_agg_bitmapcomposer.h"

class CFX_DIBBase;
class CFX_DIBitmap;
class CFX_ImageTransformer;
class CFX_ImageStretcher;
class PauseIndicatorIface;

class CFX_AggImageRenderer {
 public:
  CFX_AggImageRenderer(const RetainPtr<CFX_DIBitmap>& pDevice,
                       const CFX_AggClipRgn* pClipRgn,
                       RetainPtr<const CFX_DIBBase> source,
                       float alpha,
                       uint32_t mask_color,
                       const CFX_Matrix& matrix,
                       const FXDIB_ResampleOptions& options,
                       bool bRgbByteOrder);
  ~CFX_AggImageRenderer();

  bool Continue(PauseIndicatorIface* pPause);

 private:
  enum class State : uint8_t { kInitial = 0, kStretching, kTransforming };

  RetainPtr<CFX_DIBitmap> const m_pDevice;
  UnownedPtr<const CFX_AggClipRgn> const m_pClipRgn;
  const CFX_Matrix m_Matrix;
  std::unique_ptr<CFX_ImageTransformer> m_pTransformer;
  std::unique_ptr<CFX_ImageStretcher> m_Stretcher;
  CFX_AggBitmapComposer m_Composer;
  FX_RECT m_ClipBox;
  const float m_Alpha;
  uint32_t m_MaskColor;
  State m_State = State::kInitial;
  const bool m_bRgbByteOrder;
};

#endif  // CORE_FXGE_AGG_CFX_AGG_IMAGERENDERER_H_
