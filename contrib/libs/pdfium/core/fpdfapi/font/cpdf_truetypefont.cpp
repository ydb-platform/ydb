// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/font/cpdf_truetypefont.h"

#include <algorithm>
#include <utility>

#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fxge/fx_font.h"

namespace {

constexpr uint8_t kPrefix[4] = {0x00, 0xf0, 0xf1, 0xf2};

uint16_t GetGlyphIndexForMSSymbol(const RetainPtr<CFX_Face>& face,
                                  uint32_t charcode) {
  for (uint8_t c : kPrefix) {
    uint16_t unicode = c * 256 + charcode;
    uint16_t val = face->GetCharIndex(unicode);
    if (val)
      return val;
  }
  return 0;
}

bool IsWinAnsiOrMacRomanEncoding(FontEncoding encoding) {
  return encoding == FontEncoding::kWinAnsi ||
         encoding == FontEncoding::kMacRoman;
}

}  // namespace

CPDF_TrueTypeFont::CPDF_TrueTypeFont(CPDF_Document* pDocument,
                                     RetainPtr<CPDF_Dictionary> pFontDict)
    : CPDF_SimpleFont(pDocument, std::move(pFontDict)) {}

CPDF_TrueTypeFont::~CPDF_TrueTypeFont() = default;

bool CPDF_TrueTypeFont::IsTrueTypeFont() const {
  return true;
}

const CPDF_TrueTypeFont* CPDF_TrueTypeFont::AsTrueTypeFont() const {
  return this;
}

CPDF_TrueTypeFont* CPDF_TrueTypeFont::AsTrueTypeFont() {
  return this;
}

bool CPDF_TrueTypeFont::Load() {
  return LoadCommon();
}

void CPDF_TrueTypeFont::LoadGlyphMap() {
  RetainPtr<CFX_Face> face = m_Font.GetFace();
  if (!face) {
    return;
  }

  const FontEncoding base_encoding = DetermineEncoding();
  if ((IsWinAnsiOrMacRomanEncoding(base_encoding) && m_CharNames.empty()) ||
      FontStyleIsNonSymbolic(m_Flags)) {
    if (m_Font.GetFace()->HasGlyphNames() && face->GetCharMapCount() == 0) {
      SetGlyphIndicesFromFirstChar();
      return;
    }

    const CharmapType charmap_type = DetermineCharmapType();
    bool bToUnicode = m_pFontDict->KeyExist("ToUnicode");
    for (uint32_t charcode = 0; charcode < 256; charcode++) {
      const char* name = GetAdobeCharName(base_encoding, m_CharNames, charcode);
      if (!name) {
        m_GlyphIndex[charcode] =
            m_pFontFile ? face->GetCharIndex(charcode) : -1;
        continue;
      }
      m_Encoding.SetUnicode(charcode, UnicodeFromAdobeName(name));
      if (charmap_type == CharmapType::kMSSymbol) {
        m_GlyphIndex[charcode] = GetGlyphIndexForMSSymbol(face, charcode);
      } else if (m_Encoding.UnicodeFromCharCode(charcode)) {
        if (charmap_type == CharmapType::kMSUnicode) {
          m_GlyphIndex[charcode] =
              face->GetCharIndex(m_Encoding.UnicodeFromCharCode(charcode));
        } else if (charmap_type == CharmapType::kMacRoman) {
          uint32_t maccode = CharCodeFromUnicodeForEncoding(
              fxge::FontEncoding::kAppleRoman,
              m_Encoding.UnicodeFromCharCode(charcode));
          if (!maccode) {
            m_GlyphIndex[charcode] = face->GetNameIndex(name);
          } else {
            m_GlyphIndex[charcode] = face->GetCharIndex(maccode);
          }
        }
      }
      if ((m_GlyphIndex[charcode] != 0 && m_GlyphIndex[charcode] != 0xffff) ||
          !name) {
        continue;
      }
      if (strcmp(name, ".notdef") == 0) {
        m_GlyphIndex[charcode] = face->GetCharIndex(32);
        continue;
      }
      m_GlyphIndex[charcode] = face->GetNameIndex(name);
      if (m_GlyphIndex[charcode] != 0 || !bToUnicode)
        continue;

      WideString wsUnicode = UnicodeFromCharCode(charcode);
      if (!wsUnicode.IsEmpty()) {
        m_GlyphIndex[charcode] = face->GetCharIndex(wsUnicode[0]);
        m_Encoding.SetUnicode(charcode, wsUnicode[0]);
      }
    }
    return;
  }
  if (UseTTCharmapMSSymbol(face)) {
    for (uint32_t charcode = 0; charcode < 256; charcode++)
      m_GlyphIndex[charcode] = GetGlyphIndexForMSSymbol(face, charcode);
    if (HasAnyGlyphIndex()) {
      if (base_encoding != FontEncoding::kBuiltin) {
        for (uint32_t charcode = 0; charcode < 256; charcode++) {
          const char* name =
              GetAdobeCharName(base_encoding, m_CharNames, charcode);
          if (name)
            m_Encoding.SetUnicode(charcode, UnicodeFromAdobeName(name));
        }
      } else if (UseTTCharmapMacRoman(face)) {
        for (uint32_t charcode = 0; charcode < 256; charcode++) {
          m_Encoding.SetUnicode(charcode,
                                UnicodeFromAppleRomanCharCode(charcode));
        }
      }
      return;
    }
  }
  if (UseTTCharmapMacRoman(face)) {
    for (uint32_t charcode = 0; charcode < 256; charcode++) {
      m_GlyphIndex[charcode] = face->GetCharIndex(charcode);
      m_Encoding.SetUnicode(charcode, UnicodeFromAppleRomanCharCode(charcode));
    }
    if (m_pFontFile || HasAnyGlyphIndex())
      return;
  }
  if (m_Font.GetFace()->SelectCharMap(fxge::FontEncoding::kUnicode)) {
    pdfium::span<const uint16_t> unicodes =
        UnicodesForPredefinedCharSet(base_encoding);
    for (uint32_t charcode = 0; charcode < 256; charcode++) {
      if (m_pFontFile) {
        m_Encoding.SetUnicode(charcode, charcode);
      } else {
        const char* name =
            GetAdobeCharName(FontEncoding::kBuiltin, m_CharNames, charcode);
        if (name) {
          m_Encoding.SetUnicode(charcode, UnicodeFromAdobeName(name));
        } else if (!unicodes.empty()) {
          m_Encoding.SetUnicode(charcode, unicodes[charcode]);
        }
      }
      m_GlyphIndex[charcode] =
          face->GetCharIndex(m_Encoding.UnicodeFromCharCode(charcode));
    }
    if (HasAnyGlyphIndex())
      return;
  }
  for (int charcode = 0; charcode < 256; charcode++)
    m_GlyphIndex[charcode] = charcode;
}

bool CPDF_TrueTypeFont::HasAnyGlyphIndex() const {
  for (uint32_t charcode = 0; charcode < kInternalTableSize; charcode++) {
    if (m_GlyphIndex[charcode])
      return true;
  }
  return false;
}

CPDF_TrueTypeFont::CharmapType CPDF_TrueTypeFont::DetermineCharmapType() const {
  if (UseTTCharmapMSUnicode(m_Font.GetFace())) {
    return CharmapType::kMSUnicode;
  }

  if (FontStyleIsNonSymbolic(m_Flags)) {
    if (UseTTCharmapMacRoman(m_Font.GetFace())) {
      return CharmapType::kMacRoman;
    }
    if (UseTTCharmapMSSymbol(m_Font.GetFace())) {
      return CharmapType::kMSSymbol;
    }
  } else {
    if (UseTTCharmapMSSymbol(m_Font.GetFace())) {
      return CharmapType::kMSSymbol;
    }
    if (UseTTCharmapMacRoman(m_Font.GetFace())) {
      return CharmapType::kMacRoman;
    }
  }
  return CharmapType::kOther;
}

FontEncoding CPDF_TrueTypeFont::DetermineEncoding() const {
  if (!m_pFontFile || !FontStyleIsSymbolic(m_Flags) ||
      !IsWinAnsiOrMacRomanEncoding(m_BaseEncoding)) {
    return m_BaseEncoding;
  }

  // Not null - caller checked.
  RetainPtr<CFX_Face> face = m_Font.GetFace();
  const size_t num_charmaps = face->GetCharMapCount();
  if (num_charmaps == 0) {
    return m_BaseEncoding;
  }

  bool support_win = false;
  bool support_mac = false;
  for (size_t i = 0; i < num_charmaps; i++) {
    int platform_id = face->GetCharMapPlatformIdByIndex(i);
    if (platform_id == kNamePlatformAppleUnicode ||
        platform_id == kNamePlatformWindows) {
      support_win = true;
    } else if (platform_id == kNamePlatformMac) {
      support_mac = true;
    }
    if (support_win && support_mac)
      break;
  }

  if (m_BaseEncoding == FontEncoding::kWinAnsi && !support_win)
    return support_mac ? FontEncoding::kMacRoman : FontEncoding::kBuiltin;
  if (m_BaseEncoding == FontEncoding::kMacRoman && !support_mac)
    return support_win ? FontEncoding::kWinAnsi : FontEncoding::kBuiltin;
  return m_BaseEncoding;
}

void CPDF_TrueTypeFont::SetGlyphIndicesFromFirstChar() {
  int start_char = m_pFontDict->GetIntegerFor("FirstChar");
  if (start_char < 0 || start_char > 255)
    return;

  auto it = std::begin(m_GlyphIndex);
  std::fill(it, it + start_char, 0);
  uint16_t glyph = 3;
  for (int charcode = start_char; charcode < 256; charcode++, glyph++)
    m_GlyphIndex[charcode] = glyph;
}
