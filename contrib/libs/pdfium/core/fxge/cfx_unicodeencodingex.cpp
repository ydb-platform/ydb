// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxge/cfx_unicodeencodingex.h"

#include <memory>

#include "core/fxge/cfx_font.h"
#include "core/fxge/freetype/fx_freetype.h"
#include "core/fxge/fx_font.h"
#include "core/fxge/fx_fontencoding.h"

namespace {

constexpr fxge::FontEncoding kEncodingIDs[] = {
    fxge::FontEncoding::kSymbol,      fxge::FontEncoding::kUnicode,
    fxge::FontEncoding::kSjis,        fxge::FontEncoding::kGB2312,
    fxge::FontEncoding::kBig5,        fxge::FontEncoding::kWansung,
    fxge::FontEncoding::kJohab,       fxge::FontEncoding::kAdobeStandard,
    fxge::FontEncoding::kAdobeExpert, fxge::FontEncoding::kAdobeCustom,
    fxge::FontEncoding::kLatin1,      fxge::FontEncoding::kOldLatin2,
    fxge::FontEncoding::kAppleRoman,
};

std::unique_ptr<CFX_UnicodeEncodingEx> FXFM_CreateFontEncoding(
    CFX_Font* pFont,
    fxge::FontEncoding encoding_id) {
  if (!pFont->GetFace()->SelectCharMap(encoding_id)) {
    return nullptr;
  }
  return std::make_unique<CFX_UnicodeEncodingEx>(pFont, encoding_id);
}

}  // namespace

CFX_UnicodeEncodingEx::CFX_UnicodeEncodingEx(CFX_Font* pFont,
                                             fxge::FontEncoding encoding_id)
    : CFX_UnicodeEncoding(pFont), encoding_id_(encoding_id) {}

CFX_UnicodeEncodingEx::~CFX_UnicodeEncodingEx() = default;

uint32_t CFX_UnicodeEncodingEx::GlyphFromCharCode(uint32_t charcode) {
  RetainPtr<CFX_Face> face = m_pFont->GetFace();
  FT_UInt nIndex = face->GetCharIndex(charcode);
  if (nIndex > 0)
    return nIndex;

  size_t map_index = 0;
  while (map_index < face->GetCharMapCount()) {
    fxge::FontEncoding encoding_id =
        face->GetCharMapEncodingByIndex(map_index++);
    if (encoding_id_ == encoding_id) {
      continue;
    }
    if (!face->SelectCharMap(encoding_id)) {
      continue;
    }
    nIndex = face->GetCharIndex(charcode);
    if (nIndex > 0) {
      encoding_id_ = encoding_id;
      return nIndex;
    }
  }
  face->SelectCharMap(encoding_id_);
  return 0;
}

uint32_t CFX_UnicodeEncodingEx::CharCodeFromUnicode(wchar_t Unicode) const {
  if (encoding_id_ == fxge::FontEncoding::kUnicode ||
      encoding_id_ == fxge::FontEncoding::kSymbol) {
    return Unicode;
  }
  RetainPtr<CFX_Face> face = m_pFont->GetFace();
  for (size_t i = 0; i < face->GetCharMapCount(); i++) {
    fxge::FontEncoding encoding_id = face->GetCharMapEncodingByIndex(i);
    if (encoding_id == fxge::FontEncoding::kUnicode ||
        encoding_id == fxge::FontEncoding::kSymbol) {
      return Unicode;
    }
  }
  return kInvalidCharCode;
}

std::unique_ptr<CFX_UnicodeEncodingEx> FX_CreateFontEncodingEx(
    CFX_Font* pFont) {
  if (!pFont || !pFont->GetFace()) {
    return nullptr;
  }

  for (fxge::FontEncoding id : kEncodingIDs) {
    auto pFontEncoding = FXFM_CreateFontEncoding(pFont, id);
    if (pFontEncoding)
      return pFontEncoding;
  }
  return nullptr;
}
