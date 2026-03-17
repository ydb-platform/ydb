// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/render/cpdf_textrenderer.h"

#include <algorithm>
#include <vector>

#include "core/fpdfapi/font/cpdf_font.h"
#include "core/fpdfapi/render/charposlist.h"
#include "core/fpdfapi/render/cpdf_renderoptions.h"
#include "core/fxge/cfx_graphstatedata.h"
#include "core/fxge/cfx_path.h"
#include "core/fxge/cfx_renderdevice.h"
#include "core/fxge/cfx_textrenderoptions.h"
#include "core/fxge/fx_font.h"
#include "core/fxge/text_char_pos.h"

namespace {

CFX_Font* GetFont(CPDF_Font* pFont, int32_t position) {
  return position == -1 ? pFont->GetFont() : pFont->GetFontFallback(position);
}

CFX_TextRenderOptions GetTextRenderOptionsHelper(
    const CPDF_Font* pFont,
    const CPDF_RenderOptions& options) {
  CFX_TextRenderOptions text_options;

  if (pFont->IsCIDFont())
    text_options.font_is_cid = true;

  if (options.GetOptions().bNoTextSmooth)
    text_options.aliasing_type = CFX_TextRenderOptions::kAliasing;
  else if (options.GetOptions().bClearType)
    text_options.aliasing_type = CFX_TextRenderOptions::kLcd;

  if (options.GetOptions().bNoNativeText)
    text_options.native_text = false;

  return text_options;
}

}  // namespace

// static
bool CPDF_TextRenderer::DrawTextPath(
    CFX_RenderDevice* pDevice,
    pdfium::span<const uint32_t> char_codes,
    pdfium::span<const float> char_pos,
    CPDF_Font* pFont,
    float font_size,
    const CFX_Matrix& mtText2User,
    const CFX_Matrix* pUser2Device,
    const CFX_GraphStateData* pGraphState,
    FX_ARGB fill_argb,
    FX_ARGB stroke_argb,
    CFX_Path* pClippingPath,
    const CFX_FillRenderOptions& fill_options) {
  std::vector<TextCharPos> pos =
      GetCharPosList(char_codes, char_pos, pFont, font_size);
  if (pos.empty())
    return true;

  bool bDraw = true;
  int32_t fontPosition = pos[0].m_FallbackFontPosition;
  size_t startIndex = 0;
  for (size_t i = 0; i < pos.size(); ++i) {
    int32_t curFontPosition = pos[i].m_FallbackFontPosition;
    if (fontPosition == curFontPosition)
      continue;

    CFX_Font* font = GetFont(pFont, fontPosition);
    if (!pDevice->DrawTextPath(
            pdfium::make_span(pos).subspan(startIndex, i - startIndex), font,
            font_size, mtText2User, pUser2Device, pGraphState, fill_argb,
            stroke_argb, pClippingPath, fill_options)) {
      bDraw = false;
    }
    fontPosition = curFontPosition;
    startIndex = i;
  }
  CFX_Font* font = GetFont(pFont, fontPosition);
  if (!pDevice->DrawTextPath(pdfium::make_span(pos).subspan(startIndex), font,
                             font_size, mtText2User, pUser2Device, pGraphState,
                             fill_argb, stroke_argb, pClippingPath,
                             fill_options)) {
    bDraw = false;
  }
  return bDraw;
}

// static
void CPDF_TextRenderer::DrawTextString(CFX_RenderDevice* pDevice,
                                       float origin_x,
                                       float origin_y,
                                       CPDF_Font* pFont,
                                       float font_size,
                                       const CFX_Matrix& matrix,
                                       const ByteString& str,
                                       FX_ARGB fill_argb,
                                       const CPDF_RenderOptions& options) {
  if (pFont->IsType3Font())
    return;

  size_t nChars = pFont->CountChar(str.AsStringView());
  if (nChars == 0)
    return;

  size_t offset = 0;
  std::vector<uint32_t> codes;
  std::vector<float> positions;
  codes.resize(nChars);
  positions.resize(nChars - 1);
  float cur_pos = 0;
  for (size_t i = 0; i < nChars; i++) {
    codes[i] = pFont->GetNextChar(str.AsStringView(), &offset);
    if (i)
      positions[i - 1] = cur_pos;
    cur_pos += pFont->GetCharWidthF(codes[i]) * font_size / 1000;
  }
  CFX_Matrix new_matrix = matrix;
  new_matrix.e = origin_x;
  new_matrix.f = origin_y;
  DrawNormalText(pDevice, codes, positions, pFont, font_size, new_matrix,
                 fill_argb, options);
}

// static
bool CPDF_TextRenderer::DrawNormalText(CFX_RenderDevice* pDevice,
                                       pdfium::span<const uint32_t> char_codes,
                                       pdfium::span<const float> char_pos,
                                       CPDF_Font* pFont,
                                       float font_size,
                                       const CFX_Matrix& mtText2Device,
                                       FX_ARGB fill_argb,
                                       const CPDF_RenderOptions& options) {
  std::vector<TextCharPos> pos =
      GetCharPosList(char_codes, char_pos, pFont, font_size);
  if (pos.empty())
    return true;

  CFX_TextRenderOptions text_options =
      GetTextRenderOptionsHelper(pFont, options);
  bool bDraw = true;
  int32_t fontPosition = pos[0].m_FallbackFontPosition;
  size_t startIndex = 0;
  for (size_t i = 0; i < pos.size(); ++i) {
    int32_t curFontPosition = pos[i].m_FallbackFontPosition;
    if (fontPosition == curFontPosition)
      continue;

    CFX_Font* font = GetFont(pFont, fontPosition);
    if (!pDevice->DrawNormalText(
            pdfium::make_span(pos).subspan(startIndex, i - startIndex), font,
            font_size, mtText2Device, fill_argb, text_options)) {
      bDraw = false;
    }
    fontPosition = curFontPosition;
    startIndex = i;
  }
  CFX_Font* font = GetFont(pFont, fontPosition);
  if (!pDevice->DrawNormalText(pdfium::make_span(pos).subspan(startIndex), font,
                               font_size, mtText2Device, fill_argb,
                               text_options)) {
    bDraw = false;
  }
  return bDraw;
}
