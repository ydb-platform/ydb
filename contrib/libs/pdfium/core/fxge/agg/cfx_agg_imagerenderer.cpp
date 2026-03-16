// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxge/agg/cfx_agg_imagerenderer.h"

#include <math.h>

#include <memory>
#include <utility>

#include "core/fxcrt/fx_system.h"
#include "core/fxge/agg/cfx_agg_cliprgn.h"
#include "core/fxge/dib/cfx_dibitmap.h"
#include "core/fxge/dib/cfx_imagestretcher.h"
#include "core/fxge/dib/cfx_imagetransformer.h"

CFX_AggImageRenderer::CFX_AggImageRenderer(
    const RetainPtr<CFX_DIBitmap>& pDevice,
    const CFX_AggClipRgn* pClipRgn,
    RetainPtr<const CFX_DIBBase> source,
    float alpha,
    uint32_t mask_color,
    const CFX_Matrix& matrix,
    const FXDIB_ResampleOptions& options,
    bool bRgbByteOrder)
    : m_pDevice(pDevice),
      m_pClipRgn(pClipRgn),
      m_Matrix(matrix),
      m_Alpha(alpha),
      m_MaskColor(mask_color),
      m_bRgbByteOrder(bRgbByteOrder) {
  FX_RECT image_rect = m_Matrix.GetUnitRect().GetOuterRect();
  m_ClipBox = pClipRgn
                  ? pClipRgn->GetBox()
                  : FX_RECT(0, 0, pDevice->GetWidth(), pDevice->GetHeight());
  m_ClipBox.Intersect(image_rect);
  if (m_ClipBox.IsEmpty())
    return;

  if ((fabs(m_Matrix.b) >= 0.5f || m_Matrix.a == 0) ||
      (fabs(m_Matrix.c) >= 0.5f || m_Matrix.d == 0)) {
    if (fabs(m_Matrix.a) < fabs(m_Matrix.b) / 20 &&
        fabs(m_Matrix.d) < fabs(m_Matrix.c) / 20 && fabs(m_Matrix.a) < 0.5f &&
        fabs(m_Matrix.d) < 0.5f) {
      int dest_width = image_rect.Width();
      int dest_height = image_rect.Height();
      FX_RECT bitmap_clip = m_ClipBox;
      bitmap_clip.Offset(-image_rect.left, -image_rect.top);
      bitmap_clip = bitmap_clip.SwappedClipBox(dest_width, dest_height,
                                               m_Matrix.c > 0, m_Matrix.b < 0);
      const bool flip_x = m_Matrix.c > 0;
      const bool flip_y = m_Matrix.b < 0;
      m_Composer.Compose(pDevice, pClipRgn, alpha, mask_color, m_ClipBox,
                         /*bVertical=*/true, flip_x, flip_y, m_bRgbByteOrder,
                         BlendMode::kNormal);
      m_Stretcher = std::make_unique<CFX_ImageStretcher>(
          &m_Composer, std::move(source), dest_height, dest_width, bitmap_clip,
          options);
      if (m_Stretcher->Start())
        m_State = State::kStretching;
      return;
    }
    m_State = State::kTransforming;
    m_pTransformer = std::make_unique<CFX_ImageTransformer>(
        std::move(source), m_Matrix, options, &m_ClipBox);
    return;
  }

  int dest_width = image_rect.Width();
  if (m_Matrix.a < 0)
    dest_width = -dest_width;

  int dest_height = image_rect.Height();
  if (m_Matrix.d > 0)
    dest_height = -dest_height;

  if (dest_width == 0 || dest_height == 0)
    return;

  FX_RECT bitmap_clip = m_ClipBox;
  bitmap_clip.Offset(-image_rect.left, -image_rect.top);
  m_Composer.Compose(pDevice, pClipRgn, alpha, mask_color, m_ClipBox,
                     /*bVertical=*/false, /*bFlipX=*/false, /*bFlipY=*/false,
                     m_bRgbByteOrder, BlendMode::kNormal);
  m_State = State::kStretching;
  m_Stretcher = std::make_unique<CFX_ImageStretcher>(
      &m_Composer, std::move(source), dest_width, dest_height, bitmap_clip,
      options);
  m_Stretcher->Start();
}

CFX_AggImageRenderer::~CFX_AggImageRenderer() = default;

bool CFX_AggImageRenderer::Continue(PauseIndicatorIface* pPause) {
  if (m_State == State::kStretching)
    return m_Stretcher->Continue(pPause);
  if (m_State != State::kTransforming)
    return false;
  if (m_pTransformer->Continue(pPause))
    return true;

  RetainPtr<CFX_DIBitmap> pBitmap = m_pTransformer->DetachBitmap();
  if (!pBitmap || pBitmap->GetBuffer().empty())
    return false;

  if (pBitmap->IsMaskFormat()) {
    if (m_Alpha != 1.0f) {
      m_MaskColor = FXARGB_MUL_ALPHA(m_MaskColor, FXSYS_roundf(m_Alpha * 255));
    }
    m_pDevice->CompositeMask(m_pTransformer->result().left,
                             m_pTransformer->result().top, pBitmap->GetWidth(),
                             pBitmap->GetHeight(), pBitmap, m_MaskColor, 0, 0,
                             BlendMode::kNormal, m_pClipRgn, m_bRgbByteOrder);
  } else {
    pBitmap->MultiplyAlpha(m_Alpha);
    m_pDevice->CompositeBitmap(
        m_pTransformer->result().left, m_pTransformer->result().top,
        pBitmap->GetWidth(), pBitmap->GetHeight(), pBitmap, 0, 0,
        BlendMode::kNormal, m_pClipRgn, m_bRgbByteOrder);
  }
  return false;
}
