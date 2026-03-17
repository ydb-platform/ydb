// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/page/cpdf_docpagedata.h"

#include <algorithm>
#include <array>
#include <memory>
#include <set>
#include <utility>
#include <vector>

#include "build/build_config.h"
#include "constants/font_encodings.h"
#include "core/fpdfapi/font/cpdf_fontglobals.h"
#include "core/fpdfapi/font/cpdf_type1font.h"
#include "core/fpdfapi/page/cpdf_form.h"
#include "core/fpdfapi/page/cpdf_iccprofile.h"
#include "core/fpdfapi/page/cpdf_image.h"
#include "core/fpdfapi/page/cpdf_pattern.h"
#include "core/fpdfapi/page/cpdf_shadingpattern.h"
#include "core/fpdfapi/page/cpdf_tilingpattern.h"
#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_name.h"
#include "core/fpdfapi/parser/cpdf_number.h"
#include "core/fpdfapi/parser/cpdf_reference.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fpdfapi/parser/cpdf_stream_acc.h"
#include "core/fpdfapi/parser/cpdf_string.h"
#include "core/fxcodec/icc/icc_transform.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/containers/contains.h"
#include "core/fxcrt/fixed_size_data_vector.h"
#include "core/fxcrt/fx_codepage.h"
#include "core/fxcrt/fx_memory.h"
#include "core/fxcrt/fx_safe_types.h"
#include "core/fxcrt/scoped_set_insertion.h"
#include "core/fxcrt/span.h"
#include "core/fxge/cfx_font.h"
#include "core/fxge/cfx_fontmapper.h"
#include "core/fxge/cfx_substfont.h"
#include "core/fxge/cfx_unicodeencoding.h"
#include "core/fxge/fx_font.h"

namespace {

void InsertWidthArrayImpl(std::vector<int> widths, CPDF_Array* pWidthArray) {
  size_t i;
  for (i = 1; i < widths.size(); i++) {
    if (widths[i] != widths[0])
      break;
  }
  if (i == widths.size()) {
    int first = pWidthArray->GetIntegerAt(pWidthArray->size() - 1);
    pWidthArray->AppendNew<CPDF_Number>(first +
                                        static_cast<int>(widths.size()) - 1);
    pWidthArray->AppendNew<CPDF_Number>(widths[0]);
    return;
  }
  auto pWidthArray1 = pWidthArray->AppendNew<CPDF_Array>();
  for (int w : widths)
    pWidthArray1->AppendNew<CPDF_Number>(w);
}

#if BUILDFLAG(IS_WIN)
void InsertWidthArray(HDC hDC, int start, int end, CPDF_Array* pWidthArray) {
  std::vector<int> widths(end - start + 1);
  GetCharWidth(hDC, start, end, widths.data());
  InsertWidthArrayImpl(std::move(widths), pWidthArray);
}

ByteString GetPSNameFromTT(HDC hDC) {
  ByteString result;
  DWORD size = ::GetFontData(hDC, 'eman', 0, nullptr, 0);
  if (size != GDI_ERROR) {
    auto buffer = FixedSizeDataVector<BYTE>::Uninit(size);
    ::GetFontData(hDC, 'eman', 0, buffer.span().data(), buffer.size());
    result = GetNameFromTT(buffer, 6);
  }
  return result;
}
#endif  // BUILDFLAG(IS_WIN)

void InsertWidthArray1(CFX_Font* pFont,
                       CFX_UnicodeEncoding* pEncoding,
                       wchar_t start,
                       wchar_t end,
                       CPDF_Array* pWidthArray) {
  std::vector<int> widths(end - start + 1);
  for (size_t i = 0; i < widths.size(); ++i) {
    int glyph_index = pEncoding->GlyphFromCharCode(start + i);
    widths[i] = pFont->GetGlyphWidth(glyph_index);
  }
  InsertWidthArrayImpl(std::move(widths), pWidthArray);
}

int CalculateFlags(bool bold,
                   bool italic,
                   bool fixedPitch,
                   bool serif,
                   bool script,
                   bool symbolic) {
  int flags = 0;
  if (bold)
    flags |= FXFONT_FORCE_BOLD;
  if (italic)
    flags |= FXFONT_ITALIC;
  if (fixedPitch)
    flags |= FXFONT_FIXED_PITCH;
  if (serif)
    flags |= FXFONT_SERIF;
  if (script)
    flags |= FXFONT_SCRIPT;
  if (symbolic)
    flags |= FXFONT_SYMBOLIC;
  else
    flags |= FXFONT_NONSYMBOLIC;
  return flags;
}

void ProcessNonbCJK(RetainPtr<CPDF_Dictionary> pBaseDict,
                    bool bold,
                    bool italic,
                    ByteString basefont,
                    RetainPtr<CPDF_Array> pWidths) {
  if (bold && italic)
    basefont += ",BoldItalic";
  else if (bold)
    basefont += ",Bold";
  else if (italic)
    basefont += ",Italic";
  pBaseDict->SetNewFor<CPDF_Name>("Subtype", "TrueType");
  pBaseDict->SetNewFor<CPDF_Name>("BaseFont", basefont);
  pBaseDict->SetNewFor<CPDF_Number>("FirstChar", 32);
  pBaseDict->SetNewFor<CPDF_Number>("LastChar", 255);
  pBaseDict->SetFor("Widths", pWidths);
}

RetainPtr<CPDF_Dictionary> CalculateFontDesc(CPDF_Document* pDoc,
                                             ByteString basefont,
                                             int flags,
                                             int italicangle,
                                             int ascend,
                                             int descend,
                                             RetainPtr<CPDF_Array> bbox,
                                             int32_t stemV) {
  auto pFontDesc = pDoc->New<CPDF_Dictionary>();
  pFontDesc->SetNewFor<CPDF_Name>("Type", "FontDescriptor");
  pFontDesc->SetNewFor<CPDF_Name>("FontName", basefont);
  pFontDesc->SetNewFor<CPDF_Number>("Flags", flags);
  pFontDesc->SetFor("FontBBox", bbox);
  pFontDesc->SetNewFor<CPDF_Number>("ItalicAngle", italicangle);
  pFontDesc->SetNewFor<CPDF_Number>("Ascent", ascend);
  pFontDesc->SetNewFor<CPDF_Number>("Descent", descend);
  pFontDesc->SetNewFor<CPDF_Number>("StemV", stemV);
  return pFontDesc;
}

}  // namespace

// static
CPDF_DocPageData* CPDF_DocPageData::FromDocument(const CPDF_Document* pDoc) {
  return static_cast<CPDF_DocPageData*>(pDoc->GetPageData());
}

CPDF_DocPageData::CPDF_DocPageData() = default;

CPDF_DocPageData::~CPDF_DocPageData() {
  for (auto& it : m_ImageMap) {
    it.second->WillBeDestroyed();
  }
  for (auto& it : m_FontMap) {
    it.second->WillBeDestroyed();
  }
}

CPDF_DocPageData::HashIccProfileKey::HashIccProfileKey(
    DataVector<uint8_t> digest,
    uint32_t components)
    : digest(std::move(digest)), components(components) {}

CPDF_DocPageData::HashIccProfileKey::HashIccProfileKey(
    const HashIccProfileKey& that) = default;

CPDF_DocPageData::HashIccProfileKey::~HashIccProfileKey() = default;

bool CPDF_DocPageData::HashIccProfileKey::operator<(
    const HashIccProfileKey& other) const {
  if (components == other.components) {
    return digest < other.digest;
  }
  return components < other.components;
}

void CPDF_DocPageData::ClearStockFont() {
  CPDF_FontGlobals::GetInstance()->Clear(GetDocument());
}

RetainPtr<CPDF_Font> CPDF_DocPageData::GetFont(
    RetainPtr<CPDF_Dictionary> pFontDict) {
  if (!pFontDict)
    return nullptr;

  auto it = m_FontMap.find(pFontDict);
  if (it != m_FontMap.end() && it->second)
    return pdfium::WrapRetain(it->second.Get());

  RetainPtr<CPDF_Font> pFont =
      CPDF_Font::Create(GetDocument(), pFontDict, this);
  if (!pFont)
    return nullptr;

  m_FontMap[std::move(pFontDict)].Reset(pFont.Get());
  return pFont;
}

RetainPtr<CPDF_Font> CPDF_DocPageData::GetStandardFont(
    const ByteString& fontName,
    const CPDF_FontEncoding* pEncoding) {
  if (fontName.IsEmpty())
    return nullptr;

  for (auto& it : m_FontMap) {
    CPDF_Font* pFont = it.second.Get();
    if (!pFont)
      continue;
    if (pFont->GetBaseFontName() != fontName)
      continue;
    if (pFont->IsEmbedded())
      continue;
    if (!pFont->IsType1Font())
      continue;
    if (pFont->GetFontDict()->KeyExist("Widths"))
      continue;

    CPDF_Type1Font* pT1Font = pFont->AsType1Font();
    if (pEncoding && !pT1Font->GetEncoding()->IsIdentical(pEncoding))
      continue;

    return pdfium::WrapRetain(pFont);
  }

  auto pDict = GetDocument()->NewIndirect<CPDF_Dictionary>();
  pDict->SetNewFor<CPDF_Name>("Type", "Font");
  pDict->SetNewFor<CPDF_Name>("Subtype", "Type1");
  pDict->SetNewFor<CPDF_Name>("BaseFont", fontName);
  if (pEncoding) {
    pDict->SetFor("Encoding",
                  pEncoding->Realize(GetDocument()->GetByteStringPool()));
  }

  // Note: NULL FormFactoryIface OK since known Type1 font from above.
  RetainPtr<CPDF_Font> pFont = CPDF_Font::Create(GetDocument(), pDict, nullptr);
  if (!pFont)
    return nullptr;

  m_FontMap[std::move(pDict)].Reset(pFont.Get());
  return pFont;
}

RetainPtr<CPDF_ColorSpace> CPDF_DocPageData::GetColorSpace(
    const CPDF_Object* pCSObj,
    const CPDF_Dictionary* pResources) {
  std::set<const CPDF_Object*> visited;
  return GetColorSpaceGuarded(pCSObj, pResources, &visited);
}

RetainPtr<CPDF_ColorSpace> CPDF_DocPageData::GetColorSpaceGuarded(
    const CPDF_Object* pCSObj,
    const CPDF_Dictionary* pResources,
    std::set<const CPDF_Object*>* pVisited) {
  std::set<const CPDF_Object*> visitedLocal;
  return GetColorSpaceInternal(pCSObj, pResources, pVisited, &visitedLocal);
}

RetainPtr<CPDF_ColorSpace> CPDF_DocPageData::GetColorSpaceInternal(
    const CPDF_Object* pCSObj,
    const CPDF_Dictionary* pResources,
    std::set<const CPDF_Object*>* pVisited,
    std::set<const CPDF_Object*>* pVisitedInternal) {
  if (!pCSObj)
    return nullptr;

  if (pdfium::Contains(*pVisitedInternal, pCSObj))
    return nullptr;

  ScopedSetInsertion<const CPDF_Object*> insertion(pVisitedInternal, pCSObj);

  if (pCSObj->IsName()) {
    ByteString name = pCSObj->GetString();
    RetainPtr<CPDF_ColorSpace> pCS = CPDF_ColorSpace::GetStockCSForName(name);
    if (!pCS && pResources) {
      RetainPtr<const CPDF_Dictionary> pList =
          pResources->GetDictFor("ColorSpace");
      if (pList) {
        return GetColorSpaceInternal(pList->GetDirectObjectFor(name).Get(),
                                     nullptr, pVisited, pVisitedInternal);
      }
    }
    if (!pCS || !pResources)
      return pCS;

    RetainPtr<const CPDF_Dictionary> pColorSpaces =
        pResources->GetDictFor("ColorSpace");
    if (!pColorSpaces)
      return pCS;

    RetainPtr<const CPDF_Object> pDefaultCS;
    switch (pCS->GetFamily()) {
      case CPDF_ColorSpace::Family::kDeviceRGB:
        pDefaultCS = pColorSpaces->GetDirectObjectFor("DefaultRGB");
        break;
      case CPDF_ColorSpace::Family::kDeviceGray:
        pDefaultCS = pColorSpaces->GetDirectObjectFor("DefaultGray");
        break;
      case CPDF_ColorSpace::Family::kDeviceCMYK:
        pDefaultCS = pColorSpaces->GetDirectObjectFor("DefaultCMYK");
        break;
      default:
        break;
    }
    if (!pDefaultCS)
      return pCS;

    return GetColorSpaceInternal(pDefaultCS.Get(), nullptr, pVisited,
                                 pVisitedInternal);
  }

  RetainPtr<const CPDF_Array> pArray(pCSObj->AsArray());
  if (!pArray || pArray->IsEmpty())
    return nullptr;

  if (pArray->size() == 1) {
    return GetColorSpaceInternal(pArray->GetDirectObjectAt(0).Get(), pResources,
                                 pVisited, pVisitedInternal);
  }

  auto it = m_ColorSpaceMap.find(pArray);
  if (it != m_ColorSpaceMap.end() && it->second)
    return pdfium::WrapRetain(it->second.Get());

  RetainPtr<CPDF_ColorSpace> pCS =
      CPDF_ColorSpace::Load(GetDocument(), pArray.Get(), pVisited);
  if (!pCS)
    return nullptr;

  m_ColorSpaceMap[std::move(pArray)].Reset(pCS.Get());
  return pCS;
}

RetainPtr<CPDF_Pattern> CPDF_DocPageData::GetPattern(
    RetainPtr<CPDF_Object> pPatternObj,
    const CFX_Matrix& matrix) {
  CHECK(pPatternObj->IsDictionary() || pPatternObj->IsStream());

  auto it = m_PatternMap.find(pPatternObj);
  if (it != m_PatternMap.end() && it->second)
    return pdfium::WrapRetain(it->second.Get());

  RetainPtr<CPDF_Pattern> pattern;
  switch (pPatternObj->GetDict()->GetIntegerFor("PatternType")) {
    case CPDF_Pattern::kTiling:
      pattern = pdfium::MakeRetain<CPDF_TilingPattern>(GetDocument(),
                                                       pPatternObj, matrix);
      break;
    case CPDF_Pattern::kShading:
      pattern = pdfium::MakeRetain<CPDF_ShadingPattern>(
          GetDocument(), pPatternObj, false, matrix);
      break;
    default:
      return nullptr;
  }
  m_PatternMap[pPatternObj].Reset(pattern.Get());
  return pattern;
}

RetainPtr<CPDF_ShadingPattern> CPDF_DocPageData::GetShading(
    RetainPtr<CPDF_Object> pPatternObj,
    const CFX_Matrix& matrix) {
  CHECK(pPatternObj->IsDictionary() || pPatternObj->IsStream());

  auto it = m_PatternMap.find(pPatternObj);
  if (it != m_PatternMap.end() && it->second)
    return pdfium::WrapRetain(it->second->AsShadingPattern());

  auto pPattern = pdfium::MakeRetain<CPDF_ShadingPattern>(
      GetDocument(), pPatternObj, true, matrix);
  m_PatternMap[pPatternObj].Reset(pPattern.Get());
  return pPattern;
}

RetainPtr<CPDF_Image> CPDF_DocPageData::GetImage(uint32_t dwStreamObjNum) {
  DCHECK(dwStreamObjNum);
  auto it = m_ImageMap.find(dwStreamObjNum);
  if (it != m_ImageMap.end())
    return it->second;

  auto pImage = pdfium::MakeRetain<CPDF_Image>(GetDocument(), dwStreamObjNum);
  m_ImageMap[dwStreamObjNum] = pImage;
  return pImage;
}

void CPDF_DocPageData::MaybePurgeImage(uint32_t dwStreamObjNum) {
  DCHECK(dwStreamObjNum);
  auto it = m_ImageMap.find(dwStreamObjNum);
  if (it != m_ImageMap.end() && it->second->HasOneRef())
    m_ImageMap.erase(it);
}

RetainPtr<CPDF_IccProfile> CPDF_DocPageData::GetIccProfile(
    RetainPtr<const CPDF_Stream> pProfileStream) {
  CHECK(pProfileStream);

  auto it = m_IccProfileMap.find(pProfileStream);
  if (it != m_IccProfileMap.end()) {
    return it->second;
  }

  auto pAccessor = pdfium::MakeRetain<CPDF_StreamAcc>(pProfileStream);
  pAccessor->LoadAllDataFiltered();

  // This should not fail, as the caller should have checked this already.
  const int expected_components = pProfileStream->GetDict()->GetIntegerFor("N");
  CHECK(fxcodec::IccTransform::IsValidIccComponents(expected_components));

  // Since CPDF_IccProfile can behave differently depending on
  // `expected_components`, `hash_profile_key` needs to take that into
  // consideration, in addition to the digest value.
  const HashIccProfileKey hash_profile_key(pAccessor->ComputeDigest(),
                                           expected_components);
  auto hash_it = m_HashIccProfileMap.find(hash_profile_key);
  if (hash_it != m_HashIccProfileMap.end()) {
    auto it_copied_stream = m_IccProfileMap.find(hash_it->second);
    if (it_copied_stream != m_IccProfileMap.end()) {
      return it_copied_stream->second;
    }
  }
  auto pProfile =
      pdfium::MakeRetain<CPDF_IccProfile>(pAccessor, expected_components);
  m_IccProfileMap[pProfileStream] = pProfile;
  m_HashIccProfileMap[hash_profile_key] = std::move(pProfileStream);
  return pProfile;
}

RetainPtr<CPDF_StreamAcc> CPDF_DocPageData::GetFontFileStreamAcc(
    RetainPtr<const CPDF_Stream> pFontStream) {
  DCHECK(pFontStream);
  auto it = m_FontFileMap.find(pFontStream);
  if (it != m_FontFileMap.end())
    return it->second;

  RetainPtr<const CPDF_Dictionary> pFontDict = pFontStream->GetDict();
  int32_t len1 = pFontDict->GetIntegerFor("Length1");
  int32_t len2 = pFontDict->GetIntegerFor("Length2");
  int32_t len3 = pFontDict->GetIntegerFor("Length3");
  uint32_t org_size = 0;
  if (len1 >= 0 && len2 >= 0 && len3 >= 0) {
    FX_SAFE_UINT32 safe_org_size = len1;
    safe_org_size += len2;
    safe_org_size += len3;
    org_size = safe_org_size.ValueOrDefault(0);
  }

  auto pFontAcc = pdfium::MakeRetain<CPDF_StreamAcc>(pFontStream);
  pFontAcc->LoadAllDataFilteredWithEstimatedSize(org_size);
  m_FontFileMap[std::move(pFontStream)] = pFontAcc;
  return pFontAcc;
}

void CPDF_DocPageData::MaybePurgeFontFileStreamAcc(
    RetainPtr<CPDF_StreamAcc>&& pStreamAcc) {
  if (!pStreamAcc)
    return;

  RetainPtr<const CPDF_Stream> pFontStream = pStreamAcc->GetStream();
  if (!pFontStream)
    return;

  pStreamAcc.Reset();  // Drop moved caller's reference.
  auto it = m_FontFileMap.find(pFontStream);
  if (it != m_FontFileMap.end() && it->second->HasOneRef())
    m_FontFileMap.erase(it);
}

std::unique_ptr<CPDF_Font::FormIface> CPDF_DocPageData::CreateForm(
    CPDF_Document* pDocument,
    RetainPtr<CPDF_Dictionary> pPageResources,
    RetainPtr<CPDF_Stream> pFormStream) {
  return std::make_unique<CPDF_Form>(pDocument, std::move(pPageResources),
                                     std::move(pFormStream));
}

RetainPtr<CPDF_Font> CPDF_DocPageData::AddStandardFont(
    const ByteString& fontName,
    const CPDF_FontEncoding* pEncoding) {
  ByteString mutable_name(fontName);
  std::optional<CFX_FontMapper::StandardFont> font_id =
      CFX_FontMapper::GetStandardFontName(&mutable_name);
  if (!font_id.has_value())
    return nullptr;
  return GetStandardFont(mutable_name, pEncoding);
}

RetainPtr<CPDF_Font> CPDF_DocPageData::AddFont(std::unique_ptr<CFX_Font> pFont,
                                               FX_Charset charset) {
  if (!pFont)
    return nullptr;

  const bool bCJK = FX_CharSetIsCJK(charset);
  ByteString basefont = pFont->GetFamilyName();
  basefont.Replace(" ", "");
  int flags =
      CalculateFlags(pFont->IsBold(), pFont->IsItalic(), pFont->IsFixedWidth(),
                     false, false, charset == FX_Charset::kSymbol);

  auto pBaseDict = GetDocument()->NewIndirect<CPDF_Dictionary>();
  pBaseDict->SetNewFor<CPDF_Name>("Type", "Font");

  auto pEncoding = std::make_unique<CFX_UnicodeEncoding>(pFont.get());
  RetainPtr<CPDF_Dictionary> pFontDict = pBaseDict;
  if (!bCJK) {
    auto pWidths = pdfium::MakeRetain<CPDF_Array>();
    for (int charcode = 32; charcode < 128; charcode++) {
      int glyph_index = pEncoding->GlyphFromCharCode(charcode);
      int char_width = pFont->GetGlyphWidth(glyph_index);
      pWidths->AppendNew<CPDF_Number>(char_width);
    }
    if (charset == FX_Charset::kANSI || charset == FX_Charset::kDefault ||
        charset == FX_Charset::kSymbol) {
      pBaseDict->SetNewFor<CPDF_Name>("Encoding",
                                      pdfium::font_encodings::kWinAnsiEncoding);
      for (int charcode = 128; charcode <= 255; charcode++) {
        int glyph_index = pEncoding->GlyphFromCharCode(charcode);
        int char_width = pFont->GetGlyphWidth(glyph_index);
        pWidths->AppendNew<CPDF_Number>(char_width);
      }
    } else {
      size_t i = CalculateEncodingDict(charset, pBaseDict.Get());
      if (i < std::size(kFX_CharsetUnicodes)) {
        pdfium::span<const uint16_t> pUnicodes =
            kFX_CharsetUnicodes[i].m_pUnicodes;
        for (int j = 0; j < 128; j++) {
          int glyph_index = pEncoding->GlyphFromCharCode(pUnicodes[j]);
          int char_width = pFont->GetGlyphWidth(glyph_index);
          pWidths->AppendNew<CPDF_Number>(char_width);
        }
      }
    }
    ProcessNonbCJK(pBaseDict, pFont->IsBold(), pFont->IsItalic(), basefont,
                   std::move(pWidths));
  } else {
    pFontDict = ProcessbCJK(
        pBaseDict, charset, basefont,
        [&pFont, &pEncoding](wchar_t start, wchar_t end, CPDF_Array* widthArr) {
          InsertWidthArray1(pFont.get(), pEncoding.get(), start, end, widthArr);
        });
  }
  int italicangle = pFont->GetSubstFontItalicAngle();
  FX_RECT bbox = pFont->GetBBox().value_or(FX_RECT());
  auto pBBox = pdfium::MakeRetain<CPDF_Array>();
  pBBox->AppendNew<CPDF_Number>(bbox.left);
  pBBox->AppendNew<CPDF_Number>(bbox.bottom);
  pBBox->AppendNew<CPDF_Number>(bbox.right);
  pBBox->AppendNew<CPDF_Number>(bbox.top);
  int32_t nStemV = 0;
  if (pFont->GetSubstFont()) {
    nStemV = pFont->GetSubstFont()->m_Weight / 5;
  } else {
    static constexpr char kStemChars[] = {'i', 'I', '!', '1'};
    static constexpr pdfium::span<const char> kStemSpan{kStemChars};
    uint32_t glyph = pEncoding->GlyphFromCharCode(kStemSpan.front());
    const auto remaining = kStemSpan.subspan<1>();
    nStemV = pFont->GetGlyphWidth(glyph);
    for (auto ch : remaining) {
      glyph = pEncoding->GlyphFromCharCode(ch);
      int width = pFont->GetGlyphWidth(glyph);
      if (width > 0 && width < nStemV)
        nStemV = width;
    }
  }
  RetainPtr<CPDF_Dictionary> pFontDesc = CalculateFontDesc(
      GetDocument(), basefont, flags, italicangle, pFont->GetAscent(),
      pFont->GetDescent(), std::move(pBBox), nStemV);
  uint32_t new_objnum = GetDocument()->AddIndirectObject(std::move(pFontDesc));
  pFontDict->SetNewFor<CPDF_Reference>("FontDescriptor", GetDocument(),
                                       new_objnum);
  return GetFont(pBaseDict);
}

#if BUILDFLAG(IS_WIN)
RetainPtr<CPDF_Font> CPDF_DocPageData::AddWindowsFont(LOGFONTA* pLogFont) {
  pLogFont->lfHeight = -1000;
  pLogFont->lfWidth = 0;
  HGDIOBJ hFont = CreateFontIndirectA(pLogFont);
  HDC hDC = CreateCompatibleDC(nullptr);
  hFont = SelectObject(hDC, hFont);
  int tm_size = GetOutlineTextMetrics(hDC, 0, nullptr);
  if (tm_size == 0) {
    hFont = SelectObject(hDC, hFont);
    DeleteObject(hFont);
    DeleteDC(hDC);
    return nullptr;
  }

  LPBYTE tm_buf = FX_Alloc(BYTE, tm_size);
  OUTLINETEXTMETRIC* ptm = reinterpret_cast<OUTLINETEXTMETRIC*>(tm_buf);
  GetOutlineTextMetrics(hDC, tm_size, ptm);
  int flags = CalculateFlags(
      false, pLogFont->lfItalic != 0,
      (pLogFont->lfPitchAndFamily & 3) == FIXED_PITCH,
      (pLogFont->lfPitchAndFamily & 0xf8) == FF_ROMAN,
      (pLogFont->lfPitchAndFamily & 0xf8) == FF_SCRIPT,
      pLogFont->lfCharSet == static_cast<int>(FX_Charset::kSymbol));

  const FX_Charset eCharset = FX_GetCharsetFromInt(pLogFont->lfCharSet);
  const bool bCJK = FX_CharSetIsCJK(eCharset);
  ByteString basefont;
  if (bCJK)
    basefont = GetPSNameFromTT(hDC);

  if (basefont.IsEmpty())
    basefont = pLogFont->lfFaceName;

  int italicangle = ptm->otmItalicAngle / 10;
  int ascend = ptm->otmrcFontBox.top;
  int descend = ptm->otmrcFontBox.bottom;
  int capheight = ptm->otmsCapEmHeight;
  std::array<int, 4> bbox = {{ptm->otmrcFontBox.left, ptm->otmrcFontBox.bottom,
                              ptm->otmrcFontBox.right, ptm->otmrcFontBox.top}};
  FX_Free(tm_buf);
  basefont.Replace(" ", "");
  auto pBaseDict = GetDocument()->NewIndirect<CPDF_Dictionary>();
  pBaseDict->SetNewFor<CPDF_Name>("Type", "Font");
  RetainPtr<CPDF_Dictionary> pFontDict = pBaseDict;
  if (!bCJK) {
    if (eCharset == FX_Charset::kANSI || eCharset == FX_Charset::kDefault ||
        eCharset == FX_Charset::kSymbol) {
      pBaseDict->SetNewFor<CPDF_Name>("Encoding",
                                      pdfium::font_encodings::kWinAnsiEncoding);
    } else {
      CalculateEncodingDict(eCharset, pBaseDict.Get());
    }
    std::array<int, 224> char_widths;
    GetCharWidth(hDC, 32, 255, char_widths.data());
    auto pWidths = pdfium::MakeRetain<CPDF_Array>();
    for (const auto char_width : char_widths) {
      pWidths->AppendNew<CPDF_Number>(char_width);
    }
    ProcessNonbCJK(pBaseDict, pLogFont->lfWeight > FW_MEDIUM,
                   pLogFont->lfItalic != 0, basefont, std::move(pWidths));
  } else {
    pFontDict =
        ProcessbCJK(pBaseDict, eCharset, basefont,
                    [&hDC](wchar_t start, wchar_t end, CPDF_Array* widthArr) {
                      InsertWidthArray(hDC, start, end, widthArr);
                    });
  }
  auto pBBox = pdfium::MakeRetain<CPDF_Array>();
  for (const auto bound : bbox) {
    pBBox->AppendNew<CPDF_Number>(bound);
  }
  RetainPtr<CPDF_Dictionary> pFontDesc =
      CalculateFontDesc(GetDocument(), basefont, flags, italicangle, ascend,
                        descend, std::move(pBBox), pLogFont->lfWeight / 5);
  pFontDesc->SetNewFor<CPDF_Number>("CapHeight", capheight);
  GetDocument()->AddIndirectObject(pFontDesc);
  pFontDict->SetFor("FontDescriptor", pFontDesc->MakeReference(GetDocument()));
  hFont = SelectObject(hDC, hFont);
  DeleteObject(hFont);
  DeleteDC(hDC);
  return GetFont(std::move(pBaseDict));
}
#endif  //  BUILDFLAG(IS_WIN)

size_t CPDF_DocPageData::CalculateEncodingDict(FX_Charset charset,
                                               CPDF_Dictionary* pBaseDict) {
  size_t i;
  for (i = 0; i < std::size(kFX_CharsetUnicodes); ++i) {
    if (kFX_CharsetUnicodes[i].m_Charset == charset)
      break;
  }
  if (i == std::size(kFX_CharsetUnicodes))
    return i;

  auto pEncodingDict = GetDocument()->NewIndirect<CPDF_Dictionary>();
  pEncodingDict->SetNewFor<CPDF_Name>("BaseEncoding",
                                      pdfium::font_encodings::kWinAnsiEncoding);

  auto pArray = pEncodingDict->SetNewFor<CPDF_Array>("Differences");
  pArray->AppendNew<CPDF_Number>(128);

  pdfium::span<const uint16_t> pUnicodes = kFX_CharsetUnicodes[i].m_pUnicodes;
  for (int j = 0; j < 128; j++) {
    ByteString name = AdobeNameFromUnicode(pUnicodes[j]);
    pArray->AppendNew<CPDF_Name>(name.IsEmpty() ? ".notdef" : name);
  }
  pBaseDict->SetNewFor<CPDF_Reference>("Encoding", GetDocument(),
                                       pEncodingDict->GetObjNum());
  return i;
}

RetainPtr<CPDF_Dictionary> CPDF_DocPageData::ProcessbCJK(
    RetainPtr<CPDF_Dictionary> pBaseDict,
    FX_Charset charset,
    ByteString basefont,
    std::function<void(wchar_t, wchar_t, CPDF_Array*)> Insert) {
  auto pFontDict = GetDocument()->NewIndirect<CPDF_Dictionary>();
  ByteString cmap;
  ByteString ordering;
  int supplement = 0;
  auto pWidthArray = pFontDict->SetNewFor<CPDF_Array>("W");
  switch (charset) {
    case FX_Charset::kChineseTraditional:
      cmap = "ETenms-B5-H";
      ordering = "CNS1";
      supplement = 4;
      pWidthArray->AppendNew<CPDF_Number>(1);
      Insert(0x20, 0x7e, pWidthArray.Get());
      break;
    case FX_Charset::kChineseSimplified:
      cmap = "GBK-EUC-H";
      ordering = "GB1";
      supplement = 2;
      pWidthArray->AppendNew<CPDF_Number>(7716);
      Insert(0x20, 0x20, pWidthArray.Get());
      pWidthArray->AppendNew<CPDF_Number>(814);
      Insert(0x21, 0x7e, pWidthArray.Get());
      break;
    case FX_Charset::kHangul:
      cmap = "KSCms-UHC-H";
      ordering = "Korea1";
      supplement = 2;
      pWidthArray->AppendNew<CPDF_Number>(1);
      Insert(0x20, 0x7e, pWidthArray.Get());
      break;
    case FX_Charset::kShiftJIS:
      cmap = "90ms-RKSJ-H";
      ordering = "Japan1";
      supplement = 5;
      pWidthArray->AppendNew<CPDF_Number>(231);
      Insert(0x20, 0x7d, pWidthArray.Get());
      pWidthArray->AppendNew<CPDF_Number>(326);
      Insert(0xa0, 0xa0, pWidthArray.Get());
      pWidthArray->AppendNew<CPDF_Number>(327);
      Insert(0xa1, 0xdf, pWidthArray.Get());
      pWidthArray->AppendNew<CPDF_Number>(631);
      Insert(0x7e, 0x7e, pWidthArray.Get());
      break;
    default:
      break;
  }
  pBaseDict->SetNewFor<CPDF_Name>("Subtype", "Type0");
  pBaseDict->SetNewFor<CPDF_Name>("BaseFont", basefont);
  pBaseDict->SetNewFor<CPDF_Name>("Encoding", cmap);
  pFontDict->SetNewFor<CPDF_Name>("Type", "Font");
  pFontDict->SetNewFor<CPDF_Name>("Subtype", "CIDFontType2");
  pFontDict->SetNewFor<CPDF_Name>("BaseFont", basefont);

  auto pCIDSysInfo = pFontDict->SetNewFor<CPDF_Dictionary>("CIDSystemInfo");
  pCIDSysInfo->SetNewFor<CPDF_String>("Registry", "Adobe");
  pCIDSysInfo->SetNewFor<CPDF_String>("Ordering", ordering);
  pCIDSysInfo->SetNewFor<CPDF_Number>("Supplement", supplement);

  auto pArray = pBaseDict->SetNewFor<CPDF_Array>("DescendantFonts");
  pArray->AppendNew<CPDF_Reference>(GetDocument(), pFontDict->GetObjNum());
  return pFontDict;
}
