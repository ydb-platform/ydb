// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/font/cpdf_simplefont.h"

#include <algorithm>
#include <iterator>
#include <utility>

#include "constants/font_encodings.h"
#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_name.h"
#include "core/fxcrt/fx_codepage.h"
#include "core/fxcrt/stl_util.h"
#include "core/fxge/freetype/fx_freetype.h"
#include "core/fxge/fx_font.h"

namespace {

void GetPredefinedEncoding(const ByteString& value, FontEncoding* basemap) {
  if (value == pdfium::font_encodings::kWinAnsiEncoding)
    *basemap = FontEncoding::kWinAnsi;
  else if (value == pdfium::font_encodings::kMacRomanEncoding)
    *basemap = FontEncoding::kMacRoman;
  else if (value == pdfium::font_encodings::kMacExpertEncoding)
    *basemap = FontEncoding::kMacExpert;
  else if (value == pdfium::font_encodings::kPDFDocEncoding)
    *basemap = FontEncoding::kPdfDoc;
}

}  // namespace

CPDF_SimpleFont::CPDF_SimpleFont(CPDF_Document* pDocument,
                                 RetainPtr<CPDF_Dictionary> pFontDict)
    : CPDF_Font(pDocument, std::move(pFontDict)) {
  m_CharWidth.fill(0xffff);
  m_GlyphIndex.fill(0xffff);
  m_CharBBox.fill(FX_RECT(-1, -1, -1, -1));
}

CPDF_SimpleFont::~CPDF_SimpleFont() = default;

int CPDF_SimpleFont::GlyphFromCharCode(uint32_t charcode, bool* pVertGlyph) {
  if (pVertGlyph)
    *pVertGlyph = false;

  if (charcode > 0xff)
    return -1;

  int index = m_GlyphIndex[charcode];
  if (index == 0xffff)
    return -1;

  return index;
}

void CPDF_SimpleFont::LoadCharMetrics(int charcode) {
  if (!m_Font.GetFaceRec())
    return;

  if (charcode < 0 || charcode > 0xff) {
    return;
  }
  int glyph_index = m_GlyphIndex[charcode];
  if (glyph_index == 0xffff) {
    if (!m_pFontFile && charcode != 32) {
      LoadCharMetrics(32);
      m_CharBBox[charcode] = m_CharBBox[32];
      if (m_bUseFontWidth) {
        m_CharWidth[charcode] = m_CharWidth[32];
      }
    }
    return;
  }
  RetainPtr<CFX_Face> face = m_Font.GetFace();
  if (!face) {
    return;
  }

  FXFT_FaceRec* face_rec = face->GetRec();
  int err =
      FT_Load_Glyph(face_rec, glyph_index,
                    FT_LOAD_NO_SCALE | FT_LOAD_IGNORE_GLOBAL_ADVANCE_WIDTH);
  if (err)
    return;

  m_CharBBox[charcode] = face->GetGlyphBBox();

  if (m_bUseFontWidth) {
    int TT_Width = NormalizeFontMetric(FXFT_Get_Glyph_HoriAdvance(face_rec),
                                       face->GetUnitsPerEm());
    if (m_CharWidth[charcode] == 0xffff) {
      m_CharWidth[charcode] = TT_Width;
    } else if (TT_Width && !IsEmbedded()) {
      m_CharBBox[charcode].right =
          m_CharBBox[charcode].right * m_CharWidth[charcode] / TT_Width;
      m_CharBBox[charcode].left =
          m_CharBBox[charcode].left * m_CharWidth[charcode] / TT_Width;
    }
  }
}

void CPDF_SimpleFont::LoadCharWidths(const CPDF_Dictionary* font_desc) {
  RetainPtr<const CPDF_Array> width_array = m_pFontDict->GetArrayFor("Widths");
  m_bUseFontWidth = !width_array;
  if (!width_array)
    return;

  if (font_desc && font_desc->KeyExist("MissingWidth")) {
    int missing_width = font_desc->GetIntegerFor("MissingWidth");
    std::fill(std::begin(m_CharWidth), std::end(m_CharWidth), missing_width);
  }

  size_t width_start = m_pFontDict->GetIntegerFor("FirstChar", 0);
  size_t width_end = m_pFontDict->GetIntegerFor("LastChar", 0);
  if (width_start > 255)
    return;

  if (width_end == 0 || width_end >= width_start + width_array->size())
    width_end = width_start + width_array->size() - 1;
  if (width_end > 255)
    width_end = 255;
  for (size_t i = width_start; i <= width_end; i++)
    m_CharWidth[i] = width_array->GetIntegerAt(i - width_start);
}

void CPDF_SimpleFont::LoadDifferences(const CPDF_Dictionary* encoding) {
  RetainPtr<const CPDF_Array> diffs = encoding->GetArrayFor("Differences");
  if (!diffs)
    return;

  m_CharNames.resize(kInternalTableSize);
  uint32_t cur_code = 0;
  for (uint32_t i = 0; i < diffs->size(); i++) {
    RetainPtr<const CPDF_Object> element = diffs->GetDirectObjectAt(i);
    if (!element)
      continue;

    const CPDF_Name* name = element->AsName();
    if (name) {
      if (cur_code < m_CharNames.size())
        m_CharNames[cur_code] = name->GetString();
      cur_code++;
    } else {
      cur_code = element->GetInteger();
    }
  }
}

void CPDF_SimpleFont::LoadPDFEncoding(bool bEmbedded, bool bTrueType) {
  RetainPtr<const CPDF_Object> pEncoding =
      m_pFontDict->GetDirectObjectFor("Encoding");
  if (!pEncoding) {
    if (m_BaseFontName == "Symbol") {
      m_BaseEncoding =
          bTrueType ? FontEncoding::kMsSymbol : FontEncoding::kAdobeSymbol;
    } else if (!bEmbedded && m_BaseEncoding == FontEncoding::kBuiltin) {
      m_BaseEncoding = FontEncoding::kWinAnsi;
    }
    return;
  }
  if (pEncoding->IsName()) {
    if (m_BaseEncoding == FontEncoding::kAdobeSymbol ||
        m_BaseEncoding == FontEncoding::kZapfDingbats) {
      return;
    }
    if (FontStyleIsSymbolic(m_Flags) && m_BaseFontName == "Symbol") {
      if (!bTrueType)
        m_BaseEncoding = FontEncoding::kAdobeSymbol;
      return;
    }
    ByteString bsEncoding = pEncoding->GetString();
    if (bsEncoding == pdfium::font_encodings::kMacExpertEncoding)
      bsEncoding = pdfium::font_encodings::kWinAnsiEncoding;
    GetPredefinedEncoding(bsEncoding, &m_BaseEncoding);
    return;
  }

  const CPDF_Dictionary* pDict = pEncoding->AsDictionary();
  if (!pDict)
    return;

  if (m_BaseEncoding != FontEncoding::kAdobeSymbol &&
      m_BaseEncoding != FontEncoding::kZapfDingbats) {
    ByteString bsEncoding = pDict->GetByteStringFor("BaseEncoding");
    if (bTrueType && bsEncoding == pdfium::font_encodings::kMacExpertEncoding)
      bsEncoding = pdfium::font_encodings::kWinAnsiEncoding;
    GetPredefinedEncoding(bsEncoding, &m_BaseEncoding);
  }
  if ((!bEmbedded || bTrueType) && m_BaseEncoding == FontEncoding::kBuiltin)
    m_BaseEncoding = FontEncoding::kStandard;

  LoadDifferences(pDict);
}

int CPDF_SimpleFont::GetCharWidthF(uint32_t charcode) {
  if (charcode > 0xff)
    charcode = 0;

  if (m_CharWidth[charcode] == 0xffff) {
    LoadCharMetrics(charcode);
    if (m_CharWidth[charcode] == 0xffff) {
      m_CharWidth[charcode] = 0;
    }
  }
  return m_CharWidth[charcode];
}

FX_RECT CPDF_SimpleFont::GetCharBBox(uint32_t charcode) {
  if (charcode > 0xff)
    charcode = 0;

  if (m_CharBBox[charcode].left == -1)
    LoadCharMetrics(charcode);

  return m_CharBBox[charcode];
}

bool CPDF_SimpleFont::LoadCommon() {
  RetainPtr<const CPDF_Dictionary> pFontDesc =
      m_pFontDict->GetDictFor("FontDescriptor");
  if (pFontDesc)
    LoadFontDescriptor(pFontDesc.Get());
  LoadCharWidths(pFontDesc.Get());
  if (m_pFontFile) {
    if (m_BaseFontName.GetLength() > 7 && m_BaseFontName[6] == '+') {
      m_BaseFontName = m_BaseFontName.Last(m_BaseFontName.GetLength() - 7);
    }
  } else {
    LoadSubstFont();
  }
  if (!FontStyleIsSymbolic(m_Flags))
    m_BaseEncoding = FontEncoding::kStandard;
  LoadPDFEncoding(!!m_pFontFile, m_Font.IsTTFont());
  LoadGlyphMap();
  m_CharNames.clear();
  if (!HasFace()) {
    return true;
  }

  if (FontStyleIsAllCaps(m_Flags)) {
    static const auto kLowercases =
        fxcrt::ToArray<std::pair<const uint8_t, const uint8_t>>(
            {{'a', 'z'}, {0xe0, 0xf6}, {0xf8, 0xfd}});
    for (const auto& lower : kLowercases) {
      for (int i = lower.first; i <= lower.second; ++i) {
        if (m_GlyphIndex[i] != 0xffff && m_pFontFile) {
          continue;
        }
        int j = i - 32;
        m_GlyphIndex[i] = m_GlyphIndex[j];
        if (m_CharWidth[j]) {
          m_CharWidth[i] = m_CharWidth[j];
          m_CharBBox[i] = m_CharBBox[j];
        }
      }
    }
  }
  CheckFontMetrics();
  return true;
}

void CPDF_SimpleFont::LoadSubstFont() {
  if (!m_bUseFontWidth && !FontStyleIsFixedPitch(m_Flags)) {
    int width = 0;
    size_t i;
    for (i = 0; i < kInternalTableSize; i++) {
      if (m_CharWidth[i] == 0 || m_CharWidth[i] == 0xffff)
        continue;

      if (width == 0)
        width = m_CharWidth[i];
      else if (width != m_CharWidth[i])
        break;
    }
    if (i == kInternalTableSize && width)
      m_Flags |= FXFONT_FIXED_PITCH;
  }
  m_Font.LoadSubst(m_BaseFontName, IsTrueTypeFont(), m_Flags, GetFontWeight(),
                   m_ItalicAngle, FX_CodePage::kDefANSI, false);
}

bool CPDF_SimpleFont::IsUnicodeCompatible() const {
  return m_BaseEncoding != FontEncoding::kBuiltin &&
         m_BaseEncoding != FontEncoding::kAdobeSymbol &&
         m_BaseEncoding != FontEncoding::kZapfDingbats;
}

WideString CPDF_SimpleFont::UnicodeFromCharCode(uint32_t charcode) const {
  WideString unicode = CPDF_Font::UnicodeFromCharCode(charcode);
  if (!unicode.IsEmpty())
    return unicode;
  wchar_t ret = m_Encoding.UnicodeFromCharCode((uint8_t)charcode);
  if (ret == 0)
    return WideString();
  return WideString(ret);
}

uint32_t CPDF_SimpleFont::CharCodeFromUnicode(wchar_t unicode) const {
  uint32_t ret = CPDF_Font::CharCodeFromUnicode(unicode);
  if (ret)
    return ret;
  return m_Encoding.CharCodeFromUnicode(unicode);
}

bool CPDF_SimpleFont::HasFontWidths() const {
  return !m_bUseFontWidth;
}
