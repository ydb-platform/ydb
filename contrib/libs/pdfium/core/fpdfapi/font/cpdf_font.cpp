// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/font/cpdf_font.h"

#include <algorithm>
#include <array>
#include <memory>
#include <utility>
#include <vector>

#include "build/build_config.h"
#include "constants/font_encodings.h"
#include "core/fpdfapi/font/cpdf_cidfont.h"
#include "core/fpdfapi/font/cpdf_fontencoding.h"
#include "core/fpdfapi/font/cpdf_fontglobals.h"
#include "core/fpdfapi/font/cpdf_tounicodemap.h"
#include "core/fpdfapi/font/cpdf_truetypefont.h"
#include "core/fpdfapi/font/cpdf_type1font.h"
#include "core/fpdfapi/font/cpdf_type3font.h"
#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fpdfapi/parser/cpdf_name.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fpdfapi/parser/cpdf_stream_acc.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/fx_codepage.h"
#include "core/fxcrt/fx_safe_types.h"
#include "core/fxcrt/stl_util.h"
#include "core/fxge/cfx_fontmapper.h"
#include "core/fxge/cfx_substfont.h"
#include "core/fxge/fx_font.h"

namespace {

constexpr std::array<const char*, 5> kChineseFontNames = {{
    "\xCB\xCE\xCC\xE5",
    "\xBF\xAC\xCC\xE5",
    "\xBA\xDA\xCC\xE5",
    "\xB7\xC2\xCB\xCE",
    "\xD0\xC2\xCB\xCE",
}};

}  // namespace

CPDF_Font::CPDF_Font(CPDF_Document* pDocument,
                     RetainPtr<CPDF_Dictionary> pFontDict)
    : m_pDocument(pDocument),
      m_pFontDict(std::move(pFontDict)),
      m_BaseFontName(m_pFontDict->GetByteStringFor("BaseFont")) {}

CPDF_Font::~CPDF_Font() {
  if (!m_bWillBeDestroyed && m_pFontFile) {
    m_pDocument->MaybePurgeFontFileStreamAcc(std::move(m_pFontFile));
  }
}

bool CPDF_Font::IsType1Font() const {
  return false;
}

bool CPDF_Font::IsTrueTypeFont() const {
  return false;
}

bool CPDF_Font::IsType3Font() const {
  return false;
}

bool CPDF_Font::IsCIDFont() const {
  return false;
}

const CPDF_Type1Font* CPDF_Font::AsType1Font() const {
  return nullptr;
}

CPDF_Type1Font* CPDF_Font::AsType1Font() {
  return nullptr;
}

const CPDF_TrueTypeFont* CPDF_Font::AsTrueTypeFont() const {
  return nullptr;
}

CPDF_TrueTypeFont* CPDF_Font::AsTrueTypeFont() {
  return nullptr;
}

const CPDF_Type3Font* CPDF_Font::AsType3Font() const {
  return nullptr;
}

CPDF_Type3Font* CPDF_Font::AsType3Font() {
  return nullptr;
}

const CPDF_CIDFont* CPDF_Font::AsCIDFont() const {
  return nullptr;
}

CPDF_CIDFont* CPDF_Font::AsCIDFont() {
  return nullptr;
}

size_t CPDF_Font::CountChar(ByteStringView pString) const {
  return pString.GetLength();
}

#if BUILDFLAG(IS_APPLE)
int CPDF_Font::GlyphFromCharCodeExt(uint32_t charcode) {
  return GlyphFromCharCode(charcode, nullptr);
}
#endif

void CPDF_Font::WillBeDestroyed() {
  m_bWillBeDestroyed = true;
}

bool CPDF_Font::IsVertWriting() const {
  const CPDF_CIDFont* pCIDFont = AsCIDFont();
  return pCIDFont ? pCIDFont->IsVertWriting() : m_Font.IsVertical();
}

void CPDF_Font::AppendChar(ByteString* str, uint32_t charcode) const {
  *str += static_cast<char>(charcode);
}

WideString CPDF_Font::UnicodeFromCharCode(uint32_t charcode) const {
  if (!m_bToUnicodeLoaded)
    LoadUnicodeMap();

  return m_pToUnicodeMap ? m_pToUnicodeMap->Lookup(charcode) : WideString();
}

uint32_t CPDF_Font::CharCodeFromUnicode(wchar_t unicode) const {
  if (!m_bToUnicodeLoaded)
    LoadUnicodeMap();

  return m_pToUnicodeMap ? m_pToUnicodeMap->ReverseLookup(unicode) : 0;
}

bool CPDF_Font::HasFontWidths() const {
  return true;
}

void CPDF_Font::LoadFontDescriptor(const CPDF_Dictionary* pFontDesc) {
  m_Flags = pFontDesc->GetIntegerFor("Flags", FXFONT_NONSYMBOLIC);
  int ItalicAngle = 0;
  bool bExistItalicAngle = false;
  if (pFontDesc->KeyExist("ItalicAngle")) {
    ItalicAngle = pFontDesc->GetIntegerFor("ItalicAngle");
    bExistItalicAngle = true;
  }
  if (ItalicAngle < 0) {
    m_Flags |= FXFONT_ITALIC;
    m_ItalicAngle = ItalicAngle;
  }
  bool bExistStemV = false;
  if (pFontDesc->KeyExist("StemV")) {
    m_StemV = pFontDesc->GetIntegerFor("StemV");
    bExistStemV = true;
  }
  bool bExistAscent = false;
  if (pFontDesc->KeyExist("Ascent")) {
    m_Ascent = pFontDesc->GetIntegerFor("Ascent");
    bExistAscent = true;
  }
  bool bExistDescent = false;
  if (pFontDesc->KeyExist("Descent")) {
    m_Descent = pFontDesc->GetIntegerFor("Descent");
    bExistDescent = true;
  }
  bool bExistCapHeight = false;
  if (pFontDesc->KeyExist("CapHeight"))
    bExistCapHeight = true;
  if (bExistItalicAngle && bExistAscent && bExistCapHeight && bExistDescent &&
      bExistStemV) {
    m_Flags |= FXFONT_USEEXTERNATTR;
  }
  if (m_Descent > 10)
    m_Descent = -m_Descent;
  RetainPtr<const CPDF_Array> pBBox = pFontDesc->GetArrayFor("FontBBox");
  if (pBBox) {
    m_FontBBox.left = pBBox->GetIntegerAt(0);
    m_FontBBox.bottom = pBBox->GetIntegerAt(1);
    m_FontBBox.right = pBBox->GetIntegerAt(2);
    m_FontBBox.top = pBBox->GetIntegerAt(3);
  }

  RetainPtr<const CPDF_Stream> pFontFile = pFontDesc->GetStreamFor("FontFile");
  if (!pFontFile)
    pFontFile = pFontDesc->GetStreamFor("FontFile2");
  if (!pFontFile)
    pFontFile = pFontDesc->GetStreamFor("FontFile3");
  if (!pFontFile)
    return;

  const uint64_t key = pFontFile->KeyForCache();
  m_pFontFile = m_pDocument->GetFontFileStreamAcc(std::move(pFontFile));
  if (!m_pFontFile)
    return;

  if (!m_Font.LoadEmbedded(m_pFontFile->GetSpan(), IsVertWriting(), key))
    m_pDocument->MaybePurgeFontFileStreamAcc(std::move(m_pFontFile));
}

void CPDF_Font::CheckFontMetrics() {
  if (m_FontBBox.top == 0 && m_FontBBox.bottom == 0 && m_FontBBox.left == 0 &&
      m_FontBBox.right == 0) {
    RetainPtr<CFX_Face> face = m_Font.GetFace();
    if (face) {
      // Note that `m_FontBBox` is deliberately flipped.
      const FX_RECT raw_bbox = face->GetBBox();
      const uint16_t upem = face->GetUnitsPerEm();
      m_FontBBox.left = NormalizeFontMetric(raw_bbox.left, upem);
      m_FontBBox.bottom = NormalizeFontMetric(raw_bbox.top, upem);
      m_FontBBox.right = NormalizeFontMetric(raw_bbox.right, upem);
      m_FontBBox.top = NormalizeFontMetric(raw_bbox.bottom, upem);
      m_Ascent = NormalizeFontMetric(face->GetAscender(), upem);
      m_Descent = NormalizeFontMetric(face->GetDescender(), upem);
    } else {
      bool bFirst = true;
      for (int i = 0; i < 256; i++) {
        FX_RECT rect = GetCharBBox(i);
        if (rect.left == rect.right) {
          continue;
        }
        if (bFirst) {
          m_FontBBox = rect;
          bFirst = false;
        } else {
          m_FontBBox.left = std::min(m_FontBBox.left, rect.left);
          m_FontBBox.top = std::max(m_FontBBox.top, rect.top);
          m_FontBBox.right = std::max(m_FontBBox.right, rect.right);
          m_FontBBox.bottom = std::min(m_FontBBox.bottom, rect.bottom);
        }
      }
    }
  }
  if (m_Ascent == 0 && m_Descent == 0) {
    FX_RECT rect = GetCharBBox('A');
    m_Ascent = rect.bottom == rect.top ? m_FontBBox.top : rect.top;
    rect = GetCharBBox('g');
    m_Descent = rect.bottom == rect.top ? m_FontBBox.bottom : rect.bottom;
  }
}

void CPDF_Font::LoadUnicodeMap() const {
  m_bToUnicodeLoaded = true;
  RetainPtr<const CPDF_Stream> pStream = m_pFontDict->GetStreamFor("ToUnicode");
  if (!pStream)
    return;

  m_pToUnicodeMap = std::make_unique<CPDF_ToUnicodeMap>(std::move(pStream));
}

int CPDF_Font::GetStringWidth(ByteStringView pString) {
  size_t offset = 0;
  int width = 0;
  while (offset < pString.GetLength())
    width += GetCharWidthF(GetNextChar(pString, &offset));
  return width;
}

// static
RetainPtr<CPDF_Font> CPDF_Font::GetStockFont(CPDF_Document* pDoc,
                                             ByteStringView name) {
  ByteString fontname(name);
  std::optional<CFX_FontMapper::StandardFont> font_id =
      CFX_FontMapper::GetStandardFontName(&fontname);
  if (!font_id.has_value())
    return nullptr;

  auto* pFontGlobals = CPDF_FontGlobals::GetInstance();
  RetainPtr<CPDF_Font> pFont = pFontGlobals->Find(pDoc, font_id.value());
  if (pFont)
    return pFont;

  auto pDict = pDoc->New<CPDF_Dictionary>();
  pDict->SetNewFor<CPDF_Name>("Type", "Font");
  pDict->SetNewFor<CPDF_Name>("Subtype", "Type1");
  pDict->SetNewFor<CPDF_Name>("BaseFont", fontname);
  pDict->SetNewFor<CPDF_Name>("Encoding",
                              pdfium::font_encodings::kWinAnsiEncoding);
  pFont = CPDF_Font::Create(nullptr, std::move(pDict), nullptr);
  pFontGlobals->Set(pDoc, font_id.value(), pFont);
  return pFont;
}

// static
RetainPtr<CPDF_Font> CPDF_Font::Create(CPDF_Document* pDoc,
                                       RetainPtr<CPDF_Dictionary> pFontDict,
                                       FormFactoryIface* pFactory) {
  ByteString type = pFontDict->GetByteStringFor("Subtype");
  RetainPtr<CPDF_Font> pFont;
  if (type == "TrueType") {
    ByteString tag = pFontDict->GetByteStringFor("BaseFont").First(4);
    for (const char* chinese_font_name : kChineseFontNames) {
      if (tag == chinese_font_name) {
        RetainPtr<const CPDF_Dictionary> pFontDesc =
            pFontDict->GetDictFor("FontDescriptor");
        if (!pFontDesc || !pFontDesc->KeyExist("FontFile2"))
          pFont = pdfium::MakeRetain<CPDF_CIDFont>(pDoc, std::move(pFontDict));
        break;
      }
    }
    if (!pFont)
      pFont = pdfium::MakeRetain<CPDF_TrueTypeFont>(pDoc, std::move(pFontDict));
  } else if (type == "Type3") {
    pFont = pdfium::MakeRetain<CPDF_Type3Font>(pDoc, std::move(pFontDict),
                                               pFactory);
  } else if (type == "Type0") {
    pFont = pdfium::MakeRetain<CPDF_CIDFont>(pDoc, std::move(pFontDict));
  } else {
    pFont = pdfium::MakeRetain<CPDF_Type1Font>(pDoc, std::move(pFontDict));
  }
  if (!pFont->Load())
    return nullptr;

  return pFont;
}

uint32_t CPDF_Font::GetNextChar(ByteStringView pString, size_t* pOffset) const {
  if (pString.IsEmpty())
    return 0;

  size_t& offset = *pOffset;
  return offset < pString.GetLength() ? pString[offset++] : pString.Back();
}

bool CPDF_Font::IsStandardFont() const {
  if (!IsType1Font())
    return false;
  if (m_pFontFile)
    return false;
  return AsType1Font()->IsBase14Font();
}

std::optional<FX_Charset> CPDF_Font::GetSubstFontCharset() const {
  CFX_SubstFont* pFont = m_Font.GetSubstFont();
  if (!pFont)
    return std::nullopt;
  return pFont->m_Charset;
}

// static
const char* CPDF_Font::GetAdobeCharName(
    FontEncoding base_encoding,
    const std::vector<ByteString>& charnames,
    uint32_t charcode) {
  if (charcode >= 256)
    return nullptr;

  if (!charnames.empty() && !charnames[charcode].IsEmpty())
    return charnames[charcode].c_str();

  const char* name = nullptr;
  if (base_encoding != FontEncoding::kBuiltin)
    name = CharNameFromPredefinedCharSet(base_encoding, charcode);
  if (!name)
    return nullptr;

  DCHECK(name[0]);
  return name;
}

uint32_t CPDF_Font::FallbackFontFromCharcode(uint32_t charcode) {
  if (m_FontFallbacks.empty()) {
    m_FontFallbacks.push_back(std::make_unique<CFX_Font>());
    FX_SAFE_INT32 safeWeight = m_StemV;
    safeWeight *= 5;
    m_FontFallbacks[0]->LoadSubst("Arial", IsTrueTypeFont(), m_Flags,
                                  safeWeight.ValueOrDefault(FXFONT_FW_NORMAL),
                                  m_ItalicAngle, FX_CodePage::kDefANSI,
                                  IsVertWriting());
  }
  return 0;
}

int CPDF_Font::FallbackGlyphFromCharcode(int fallbackFont, uint32_t charcode) {
  if (!fxcrt::IndexInBounds(m_FontFallbacks, fallbackFont))
    return -1;

  WideString str = UnicodeFromCharCode(charcode);
  uint32_t unicode = !str.IsEmpty() ? str[0] : charcode;
  int glyph = m_FontFallbacks[fallbackFont]->GetFace()->GetCharIndex(unicode);
  if (glyph == 0)
    return -1;

  return glyph;
}

CFX_Font* CPDF_Font::GetFontFallback(int position) {
  if (position < 0 || static_cast<size_t>(position) >= m_FontFallbacks.size())
    return nullptr;
  return m_FontFallbacks[position].get();
}

// static
bool CPDF_Font::UseTTCharmap(const RetainPtr<CFX_Face>& face,
                             int platform_id,
                             int encoding_id) {
  for (size_t i = 0; i < face->GetCharMapCount(); i++) {
    if (face->GetCharMapPlatformIdByIndex(i) == platform_id &&
        face->GetCharMapEncodingIdByIndex(i) == encoding_id) {
      face->SetCharMapByIndex(i);
      return true;
    }
  }
  return false;
}

int CPDF_Font::GetFontWeight() const {
  FX_SAFE_INT32 safeStemV(m_StemV);
  if (m_StemV < 140)
    safeStemV *= 5;
  else
    safeStemV = safeStemV * 4 + 140;
  return safeStemV.ValueOrDefault(FXFONT_FW_NORMAL);
}
