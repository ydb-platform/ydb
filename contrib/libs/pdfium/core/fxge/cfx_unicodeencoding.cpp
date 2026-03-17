// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxge/cfx_unicodeencoding.h"

#include "core/fxcrt/fx_codepage.h"
#include "core/fxge/cfx_font.h"
#include "core/fxge/cfx_substfont.h"
#include "core/fxge/fx_font.h"
#include "core/fxge/fx_fontencoding.h"

CFX_UnicodeEncoding::CFX_UnicodeEncoding(const CFX_Font* pFont)
    : m_pFont(pFont) {}

CFX_UnicodeEncoding::~CFX_UnicodeEncoding() = default;

uint32_t CFX_UnicodeEncoding::GlyphFromCharCode(uint32_t charcode) {
  RetainPtr<CFX_Face> face = m_pFont->GetFace();
  if (!face)
    return charcode;

  if (face->SelectCharMap(fxge::FontEncoding::kUnicode)) {
    return face->GetCharIndex(charcode);
  }

  if (m_pFont->GetSubstFont() &&
      m_pFont->GetSubstFont()->m_Charset == FX_Charset::kSymbol) {
    uint32_t index = 0;
    if (face->SelectCharMap(fxge::FontEncoding::kSymbol)) {
      index = face->GetCharIndex(charcode);
    }
    if (!index && face->SelectCharMap(fxge::FontEncoding::kAppleRoman)) {
      return face->GetCharIndex(charcode);
    }
  }
  return charcode;
}
