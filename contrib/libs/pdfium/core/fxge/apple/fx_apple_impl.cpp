// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include <stdint.h>

#include <memory>
#include <vector>

#include "core/fxcrt/data_vector.h"
#include "core/fxcrt/fx_system.h"
#include "core/fxcrt/span.h"
#include "core/fxge/agg/cfx_agg_cliprgn.h"
#include "core/fxge/agg/cfx_agg_devicedriver.h"
#include "core/fxge/apple/fx_apple_platform.h"
#include "core/fxge/cfx_font.h"
#include "core/fxge/cfx_gemodule.h"
#include "core/fxge/cfx_glyphbitmap.h"
#include "core/fxge/cfx_glyphcache.h"
#include "core/fxge/cfx_renderdevice.h"
#include "core/fxge/cfx_substfont.h"
#include "core/fxge/dib/cfx_dibitmap.h"
#include "core/fxge/text_char_pos.h"

namespace {

void DoNothing(void* info, const void* data, size_t size) {}

bool CGDrawGlyphRun(CGContextRef pContext,
                    pdfium::span<const TextCharPos> pCharPos,
                    CFX_Font* pFont,
                    const CFX_Matrix& mtObject2Device,
                    float font_size,
                    uint32_t argb) {
  if (pCharPos.empty())
    return true;

  bool bNegSize = font_size < 0;
  if (bNegSize)
    font_size = -font_size;

  CFX_Matrix new_matrix = mtObject2Device;
  CQuartz2D& quartz2d =
      static_cast<CApplePlatform*>(CFX_GEModule::Get()->GetPlatform())
          ->m_quartz2d;
  if (!pFont->GetPlatformFont()) {
    if (pFont->GetPsName() == "DFHeiStd-W5")
      return false;

    pFont->SetPlatformFont(quartz2d.CreateFont(pFont->GetFontSpan()));
    if (!pFont->GetPlatformFont())
      return false;
  }
  DataVector<uint16_t> glyph_indices(pCharPos.size());
  std::vector<CGPoint> glyph_positions(pCharPos.size());
  for (size_t i = 0; i < pCharPos.size(); i++) {
    glyph_indices[i] =
        pCharPos[i].m_ExtGID ? pCharPos[i].m_ExtGID : pCharPos[i].m_GlyphIndex;
    if (bNegSize)
      glyph_positions[i].x = -pCharPos[i].m_Origin.x;
    else
      glyph_positions[i].x = pCharPos[i].m_Origin.x;
    glyph_positions[i].y = pCharPos[i].m_Origin.y;
  }
  if (bNegSize) {
    new_matrix.a = -new_matrix.a;
    new_matrix.c = -new_matrix.c;
  } else {
    new_matrix.b = -new_matrix.b;
    new_matrix.d = -new_matrix.d;
  }
  quartz2d.SetGraphicsTextMatrix(pContext, new_matrix);
  return quartz2d.DrawGraphicsString(pContext, pFont->GetPlatformFont(),
                                     font_size, glyph_indices, glyph_positions,
                                     argb);
}

}  // namespace

namespace pdfium {

void CFX_AggDeviceDriver::InitPlatform() {
  CQuartz2D& quartz2d =
      static_cast<CApplePlatform*>(CFX_GEModule::Get()->GetPlatform())
          ->m_quartz2d;
  m_pPlatformGraphics = quartz2d.CreateGraphics(m_pBitmap);
}

void CFX_AggDeviceDriver::DestroyPlatform() {
  CQuartz2D& quartz2d =
      static_cast<CApplePlatform*>(CFX_GEModule::Get()->GetPlatform())
          ->m_quartz2d;
  if (m_pPlatformGraphics) {
    quartz2d.DestroyGraphics(m_pPlatformGraphics);
    m_pPlatformGraphics = nullptr;
  }
}

bool CFX_AggDeviceDriver::DrawDeviceText(
    pdfium::span<const TextCharPos> pCharPos,
    CFX_Font* pFont,
    const CFX_Matrix& mtObject2Device,
    float font_size,
    uint32_t argb,
    const CFX_TextRenderOptions& /*options*/) {
  if (!pFont)
    return false;

  bool bBold = pFont->IsBold();
  if (!bBold && pFont->GetSubstFont() &&
      pFont->GetSubstFont()->m_Weight >= 500 &&
      pFont->GetSubstFont()->m_Weight <= 600) {
    return false;
  }
  for (const auto& cp : pCharPos) {
    if (cp.m_bGlyphAdjust)
      return false;
  }
  CGContextRef ctx = CGContextRef(m_pPlatformGraphics);
  if (!ctx)
    return false;

  CGContextSaveGState(ctx);
  CGContextSetTextDrawingMode(ctx, kCGTextFillClip);
  CGRect rect_cg;
  CGImageRef pImageCG = nullptr;
  if (m_pClipRgn) {
    rect_cg =
        CGRectMake(m_pClipRgn->GetBox().left, m_pClipRgn->GetBox().top,
                   m_pClipRgn->GetBox().Width(), m_pClipRgn->GetBox().Height());
    RetainPtr<CFX_DIBitmap> pClipMask = m_pClipRgn->GetMask();
    if (pClipMask) {
      CGDataProviderRef pClipMaskDataProvider = CGDataProviderCreateWithData(
          nullptr, pClipMask->GetBuffer().data(),
          pClipMask->GetPitch() * pClipMask->GetHeight(), DoNothing);
      CGFloat decode_f[2] = {255.f, 0.f};
      pImageCG = CGImageMaskCreate(
          pClipMask->GetWidth(), pClipMask->GetHeight(), 8, 8,
          pClipMask->GetPitch(), pClipMaskDataProvider, decode_f, false);
      CGDataProviderRelease(pClipMaskDataProvider);
    }
  } else {
    rect_cg = CGRectMake(0, 0, m_pBitmap->GetWidth(), m_pBitmap->GetHeight());
  }
  rect_cg = CGContextConvertRectToDeviceSpace(ctx, rect_cg);
  if (pImageCG)
    CGContextClipToMask(ctx, rect_cg, pImageCG);
  else
    CGContextClipToRect(ctx, rect_cg);

  bool ret =
      CGDrawGlyphRun(ctx, pCharPos, pFont, mtObject2Device, font_size, argb);
  if (pImageCG)
    CGImageRelease(pImageCG);
  CGContextRestoreGState(ctx);
  return ret;
}

}  // namespace pdfium

std::unique_ptr<CFX_GlyphBitmap> CFX_GlyphCache::RenderGlyph_Nativetext(
    const CFX_Font* pFont,
    uint32_t glyph_index,
    const CFX_Matrix& matrix,
    int dest_width,
    int anti_alias) {
  return nullptr;
}

void CFX_Font::ReleasePlatformResource() {
  if (m_pPlatformFont) {
    CQuartz2D& quartz2d =
        static_cast<CApplePlatform*>(CFX_GEModule::Get()->GetPlatform())
            ->m_quartz2d;
    quartz2d.DestroyFont(m_pPlatformFont);
    m_pPlatformFont = nullptr;
  }
}
