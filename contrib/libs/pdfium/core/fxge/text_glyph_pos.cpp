// Copyright 2019 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxge/text_glyph_pos.h"

#include "core/fxcrt/fx_safe_types.h"
#include "core/fxge/cfx_glyphbitmap.h"

TextGlyphPos::TextGlyphPos() = default;

TextGlyphPos::TextGlyphPos(const TextGlyphPos&) = default;

TextGlyphPos::~TextGlyphPos() = default;

std::optional<CFX_Point> TextGlyphPos::GetOrigin(
    const CFX_Point& offset) const {
  FX_SAFE_INT32 left = m_Origin.x;
  left += m_pGlyph->left();
  left -= offset.x;
  if (!left.IsValid())
    return std::nullopt;

  FX_SAFE_INT32 top = m_Origin.y;
  top -= m_pGlyph->top();
  top -= offset.y;
  if (!top.IsValid())
    return std::nullopt;

  return CFX_Point(left.ValueOrDie(), top.ValueOrDie());
}
