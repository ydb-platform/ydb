// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfdoc/cpdf_bafontmap.h"

#include <memory>
#include <utility>

#include "constants/annotation_common.h"
#include "core/fpdfapi/font/cpdf_font.h"
#include "core/fpdfapi/font/cpdf_fontencoding.h"
#include "core/fpdfapi/page/cpdf_docpagedata.h"
#include "core/fpdfapi/page/cpdf_page.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fpdfapi/parser/cpdf_parser.h"
#include "core/fpdfapi/parser/cpdf_reference.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fpdfapi/parser/fpdf_parser_utility.h"
#include "core/fpdfdoc/cpdf_defaultappearance.h"
#include "core/fpdfdoc/cpdf_formfield.h"
#include "core/fpdfdoc/ipvt_fontmap.h"
#include "core/fxcrt/fx_codepage.h"
#include "core/fxcrt/stl_util.h"
#include "core/fxge/cfx_fontmapper.h"
#include "core/fxge/cfx_fontmgr.h"
#include "core/fxge/cfx_gemodule.h"

namespace {

bool FindNativeTrueTypeFont(ByteStringView sFontFaceName) {
  CFX_FontMgr* pFontMgr = CFX_GEModule::Get()->GetFontMgr();
  CFX_FontMapper* pFontMapper = pFontMgr->GetBuiltinMapper();
  pFontMapper->LoadInstalledFonts();
  return pFontMapper->HasInstalledFont(sFontFaceName) ||
         pFontMapper->HasLocalizedFont(sFontFaceName);
}

RetainPtr<CPDF_Font> AddNativeTrueTypeFontToPDF(CPDF_Document* pDoc,
                                                ByteString sFontFaceName,
                                                FX_Charset nCharset) {
  if (!pDoc)
    return nullptr;

  auto pFXFont = std::make_unique<CFX_Font>();
  pFXFont->LoadSubst(sFontFaceName, true, 0, 0, 0,
                     FX_GetCodePageFromCharset(nCharset), false);

  auto* pDocPageData = CPDF_DocPageData::FromDocument(pDoc);
  return pDocPageData->AddFont(std::move(pFXFont), nCharset);
}

ByteString EncodeFontAlias(ByteString sFontName, FX_Charset nCharset) {
  sFontName.Remove(' ');
  sFontName += ByteString::Format("_%02X", nCharset);
  return sFontName;
}

}  // namespace

CPDF_BAFontMap::Data::Data() = default;

CPDF_BAFontMap::Data::~Data() = default;

CPDF_BAFontMap::CPDF_BAFontMap(CPDF_Document* pDocument,
                               RetainPtr<CPDF_Dictionary> pAnnotDict,
                               const ByteString& sAPType)
    : m_pDocument(pDocument),
      m_pAnnotDict(std::move(pAnnotDict)),
      m_sAPType(sAPType) {
  FX_Charset nCharset = FX_Charset::kDefault;
  m_pDefaultFont = GetAnnotDefaultFont(&m_sDefaultFontName);
  if (m_pDefaultFont) {
    auto maybe_charset = m_pDefaultFont->GetSubstFontCharset();
    if (maybe_charset.has_value()) {
      nCharset = maybe_charset.value();
    } else if (m_sDefaultFontName == "Wingdings" ||
               m_sDefaultFontName == "Wingdings2" ||
               m_sDefaultFontName == "Wingdings3" ||
               m_sDefaultFontName == "Webdings") {
      nCharset = FX_Charset::kSymbol;
    } else {
      nCharset = FX_Charset::kANSI;
    }
    AddFontData(m_pDefaultFont, m_sDefaultFontName, nCharset);
    AddFontToAnnotDict(m_pDefaultFont, m_sDefaultFontName);
  }

  if (nCharset != FX_Charset::kANSI)
    GetFontIndex(CFX_Font::kDefaultAnsiFontName, FX_Charset::kANSI, false);
}

CPDF_BAFontMap::~CPDF_BAFontMap() = default;

RetainPtr<CPDF_Font> CPDF_BAFontMap::GetPDFFont(int32_t nFontIndex) {
  if (fxcrt::IndexInBounds(m_Data, nFontIndex))
    return m_Data[nFontIndex]->pFont;
  return nullptr;
}

ByteString CPDF_BAFontMap::GetPDFFontAlias(int32_t nFontIndex) {
  if (fxcrt::IndexInBounds(m_Data, nFontIndex))
    return m_Data[nFontIndex]->sFontName;
  return ByteString();
}

int32_t CPDF_BAFontMap::GetWordFontIndex(uint16_t word,
                                         FX_Charset nCharset,
                                         int32_t nFontIndex) {
  if (nFontIndex > 0) {
    if (KnowWord(nFontIndex, word))
      return nFontIndex;
  } else {
    if (!m_Data.empty()) {
      const Data* pData = m_Data.front().get();
      if (nCharset == FX_Charset::kDefault ||
          pData->nCharset == FX_Charset::kSymbol ||
          nCharset == pData->nCharset) {
        if (KnowWord(0, word))
          return 0;
      }
    }
  }

  int32_t nNewFontIndex =
      GetFontIndex(GetCachedNativeFontName(nCharset), nCharset, true);
  if (nNewFontIndex >= 0) {
    if (KnowWord(nNewFontIndex, word))
      return nNewFontIndex;
  }
  nNewFontIndex = GetFontIndex(CFX_Font::kUniversalDefaultFontName,
                               FX_Charset::kDefault, false);
  if (nNewFontIndex >= 0) {
    if (KnowWord(nNewFontIndex, word))
      return nNewFontIndex;
  }
  return -1;
}

int32_t CPDF_BAFontMap::CharCodeFromUnicode(int32_t nFontIndex, uint16_t word) {
  if (!fxcrt::IndexInBounds(m_Data, nFontIndex))
    return -1;

  Data* pData = m_Data[nFontIndex].get();
  if (!pData->pFont)
    return -1;

  if (pData->pFont->IsUnicodeCompatible())
    return pData->pFont->CharCodeFromUnicode(word);

  return word < 0xFF ? word : -1;
}

FX_Charset CPDF_BAFontMap::CharSetFromUnicode(uint16_t word,
                                              FX_Charset nOldCharset) {
  // to avoid CJK Font to show ASCII
  if (word < 0x7F)
    return FX_Charset::kANSI;

  // follow the old charset
  if (nOldCharset != FX_Charset::kDefault)
    return nOldCharset;

  return CFX_Font::GetCharSetFromUnicode(word);
}

FX_Charset CPDF_BAFontMap::GetNativeCharset() {
  return FX_GetCharsetFromCodePage(FX_GetACP());
}

RetainPtr<CPDF_Font> CPDF_BAFontMap::FindFontSameCharset(ByteString* sFontAlias,
                                                         FX_Charset nCharset) {
  if (m_pAnnotDict->GetNameFor(pdfium::annotation::kSubtype) != "Widget")
    return nullptr;

  const CPDF_Dictionary* pRootDict = m_pDocument->GetRoot();
  if (!pRootDict)
    return nullptr;

  RetainPtr<const CPDF_Dictionary> pAcroFormDict =
      pRootDict->GetDictFor("AcroForm");
  if (!pAcroFormDict)
    return nullptr;

  RetainPtr<const CPDF_Dictionary> pDRDict = pAcroFormDict->GetDictFor("DR");
  if (!pDRDict)
    return nullptr;

  return FindResFontSameCharset(pDRDict.Get(), sFontAlias, nCharset);
}

RetainPtr<CPDF_Font> CPDF_BAFontMap::FindResFontSameCharset(
    const CPDF_Dictionary* pResDict,
    ByteString* sFontAlias,
    FX_Charset nCharset) {
  if (!pResDict)
    return nullptr;

  RetainPtr<const CPDF_Dictionary> pFonts = pResDict->GetDictFor("Font");
  if (!pFonts)
    return nullptr;

  RetainPtr<CPDF_Font> pFind;
  CPDF_DictionaryLocker locker(pFonts);
  for (const auto& it : locker) {
    const ByteString& csKey = it.first;
    RetainPtr<CPDF_Dictionary> pElement =
        ToDictionary(it.second->GetMutableDirect());
    if (!ValidateDictType(pElement.Get(), "Font"))
      continue;

    auto* pData = CPDF_DocPageData::FromDocument(m_pDocument);
    RetainPtr<CPDF_Font> pFont = pData->GetFont(std::move(pElement));
    if (!pFont)
      continue;

    auto maybe_charset = pFont->GetSubstFontCharset();
    if (maybe_charset.has_value() && maybe_charset.value() == nCharset) {
      *sFontAlias = csKey;
      pFind = std::move(pFont);
    }
  }
  return pFind;
}

RetainPtr<CPDF_Font> CPDF_BAFontMap::GetAnnotDefaultFont(ByteString* sAlias) {
  RetainPtr<CPDF_Dictionary> pAcroFormDict;
  const bool bWidget =
      (m_pAnnotDict->GetNameFor(pdfium::annotation::kSubtype) == "Widget");
  if (bWidget) {
    RetainPtr<CPDF_Dictionary> pRootDict = m_pDocument->GetMutableRoot();
    if (pRootDict)
      pAcroFormDict = pRootDict->GetMutableDictFor("AcroForm");
  }

  ByteString sDA;
  RetainPtr<const CPDF_Object> pObj =
      CPDF_FormField::GetFieldAttrForDict(m_pAnnotDict.Get(), "DA");
  if (pObj)
    sDA = pObj->GetString();

  if (bWidget) {
    if (sDA.IsEmpty()) {
      pObj = CPDF_FormField::GetFieldAttrForDict(pAcroFormDict.Get(), "DA");
      sDA = pObj ? pObj->GetString() : ByteString();
    }
  }
  if (sDA.IsEmpty())
    return nullptr;

  CPDF_DefaultAppearance appearance(sDA);
  float font_size;
  std::optional<ByteString> font = appearance.GetFont(&font_size);
  *sAlias = font.value_or(ByteString());

  RetainPtr<CPDF_Dictionary> pFontDict;
  if (RetainPtr<CPDF_Dictionary> pAPDict =
          m_pAnnotDict->GetMutableDictFor(pdfium::annotation::kAP)) {
    if (RetainPtr<CPDF_Dictionary> pNormalDict =
            pAPDict->GetMutableDictFor("N")) {
      if (RetainPtr<CPDF_Dictionary> pNormalResDict =
              pNormalDict->GetMutableDictFor("Resources")) {
        if (RetainPtr<CPDF_Dictionary> pResFontDict =
                pNormalResDict->GetMutableDictFor("Font")) {
          pFontDict = pResFontDict->GetMutableDictFor(*sAlias);
        }
      }
    }
  }
  if (bWidget && !pFontDict && pAcroFormDict) {
    if (RetainPtr<CPDF_Dictionary> pDRDict =
            pAcroFormDict->GetMutableDictFor("DR")) {
      if (RetainPtr<CPDF_Dictionary> pDRFontDict =
              pDRDict->GetMutableDictFor("Font")) {
        pFontDict = pDRFontDict->GetMutableDictFor(*sAlias);
      }
    }
  }
  if (!pFontDict)
    return nullptr;

  return CPDF_DocPageData::FromDocument(m_pDocument)->GetFont(pFontDict);
}

void CPDF_BAFontMap::AddFontToAnnotDict(const RetainPtr<CPDF_Font>& pFont,
                                        const ByteString& sAlias) {
  if (!pFont)
    return;

  RetainPtr<CPDF_Dictionary> pAPDict =
      m_pAnnotDict->GetOrCreateDictFor(pdfium::annotation::kAP);

  // to avoid checkbox and radiobutton
  if (ToDictionary(pAPDict->GetObjectFor(m_sAPType)))
    return;

  RetainPtr<CPDF_Stream> stream = pAPDict->GetMutableStreamFor(m_sAPType);
  if (!stream) {
    stream = m_pDocument->NewIndirect<CPDF_Stream>(
        m_pDocument->New<CPDF_Dictionary>());
    pAPDict->SetNewFor<CPDF_Reference>(m_sAPType, m_pDocument,
                                       stream->GetObjNum());
  }

  RetainPtr<CPDF_Dictionary> pStreamResList =
      stream->GetMutableDict()->GetOrCreateDictFor("Resources");
  RetainPtr<CPDF_Dictionary> pStreamResFontList =
      pStreamResList->GetMutableDictFor("Font");
  if (!pStreamResFontList) {
    pStreamResFontList = m_pDocument->NewIndirect<CPDF_Dictionary>();
    pStreamResList->SetNewFor<CPDF_Reference>("Font", m_pDocument,
                                              pStreamResFontList->GetObjNum());
  }
  if (!pStreamResFontList->KeyExist(sAlias)) {
    RetainPtr<const CPDF_Dictionary> pFontDict = pFont->GetFontDict();
    RetainPtr<CPDF_Object> pObject =
        pFontDict->IsInline() ? pFontDict->Clone()
                              : pFontDict->MakeReference(m_pDocument);
    pStreamResFontList->SetFor(sAlias, std::move(pObject));
  }
}

bool CPDF_BAFontMap::KnowWord(int32_t nFontIndex, uint16_t word) {
  return fxcrt::IndexInBounds(m_Data, nFontIndex) &&
         CharCodeFromUnicode(nFontIndex, word) >= 0;
}

int32_t CPDF_BAFontMap::GetFontIndex(const ByteString& sFontName,
                                     FX_Charset nCharset,
                                     bool bFind) {
  int32_t nFontIndex = FindFont(EncodeFontAlias(sFontName, nCharset), nCharset);
  if (nFontIndex >= 0)
    return nFontIndex;

  ByteString sAlias;
  RetainPtr<CPDF_Font> pFont =
      bFind ? FindFontSameCharset(&sAlias, nCharset) : nullptr;
  if (!pFont) {
    pFont = AddFontToDocument(sFontName, nCharset);
    sAlias = EncodeFontAlias(sFontName, nCharset);
  }
  AddFontToAnnotDict(pFont, sAlias);
  return AddFontData(pFont, sAlias, nCharset);
}

int32_t CPDF_BAFontMap::AddFontData(const RetainPtr<CPDF_Font>& pFont,
                                    const ByteString& sFontAlias,
                                    FX_Charset nCharset) {
  auto pNewData = std::make_unique<Data>();
  pNewData->pFont = pFont;
  pNewData->sFontName = sFontAlias;
  pNewData->nCharset = nCharset;
  m_Data.push_back(std::move(pNewData));
  return fxcrt::CollectionSize<int32_t>(m_Data) - 1;
}

int32_t CPDF_BAFontMap::FindFont(const ByteString& sFontName,
                                 FX_Charset nCharset) {
  int32_t i = 0;
  for (const auto& pData : m_Data) {
    if ((nCharset == FX_Charset::kDefault || nCharset == pData->nCharset) &&
        (sFontName.IsEmpty() || pData->sFontName == sFontName)) {
      return i;
    }
    ++i;
  }
  return -1;
}

ByteString CPDF_BAFontMap::GetNativeFontName(FX_Charset nCharset) {
  if (nCharset == FX_Charset::kDefault)
    nCharset = GetNativeCharset();

  ByteString sFontName = CFX_Font::GetDefaultFontNameByCharset(nCharset);
  if (!FindNativeTrueTypeFont(sFontName.AsStringView()))
    return ByteString();

  return sFontName;
}

ByteString CPDF_BAFontMap::GetCachedNativeFontName(FX_Charset nCharset) {
  for (const auto& pData : m_NativeFont) {
    if (pData && pData->nCharset == nCharset)
      return pData->sFontName;
  }

  ByteString sNew = GetNativeFontName(nCharset);
  if (sNew.IsEmpty())
    return ByteString();

  auto pNewData = std::make_unique<Native>();
  pNewData->nCharset = nCharset;
  pNewData->sFontName = sNew;
  m_NativeFont.push_back(std::move(pNewData));
  return sNew;
}

RetainPtr<CPDF_Font> CPDF_BAFontMap::AddFontToDocument(ByteString sFontName,
                                                       FX_Charset nCharset) {
  if (CFX_FontMapper::IsStandardFontName(sFontName))
    return AddStandardFont(sFontName);

  return AddSystemFont(sFontName, nCharset);
}

RetainPtr<CPDF_Font> CPDF_BAFontMap::AddStandardFont(ByteString sFontName) {
  auto* pPageData = CPDF_DocPageData::FromDocument(m_pDocument);
  if (sFontName == "ZapfDingbats")
    return pPageData->AddStandardFont(sFontName, nullptr);

  static const CPDF_FontEncoding fe(FontEncoding::kWinAnsi);
  return pPageData->AddStandardFont(sFontName, &fe);
}

RetainPtr<CPDF_Font> CPDF_BAFontMap::AddSystemFont(ByteString sFontName,
                                                   FX_Charset nCharset) {
  if (sFontName.IsEmpty())
    sFontName = GetNativeFontName(nCharset);

  if (nCharset == FX_Charset::kDefault)
    nCharset = GetNativeCharset();

  return AddNativeTrueTypeFontToPDF(m_pDocument, sFontName, nCharset);
}
