// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfdoc/cpdf_interactiveform.h"

#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

#include "build/build_config.h"
#include "constants/form_fields.h"
#include "constants/form_flags.h"
#include "constants/stream_dict_common.h"
#include "core/fpdfapi/font/cpdf_font.h"
#include "core/fpdfapi/font/cpdf_fontencoding.h"
#include "core/fpdfapi/page/cpdf_docpagedata.h"
#include "core/fpdfapi/page/cpdf_page.h"
#include "core/fpdfapi/parser/cfdf_document.h"
#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fpdfapi/parser/cpdf_name.h"
#include "core/fpdfapi/parser/cpdf_reference.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fpdfapi/parser/cpdf_string.h"
#include "core/fpdfapi/parser/fpdf_parser_utility.h"
#include "core/fpdfdoc/cpdf_filespec.h"
#include "core/fpdfdoc/cpdf_formcontrol.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/containers/contains.h"
#include "core/fxcrt/fx_codepage.h"
#include "core/fxcrt/fx_memcpy_wrappers.h"
#include "core/fxcrt/numerics/safe_conversions.h"
#include "core/fxcrt/stl_util.h"
#include "core/fxge/fx_font.h"

#if BUILDFLAG(IS_WIN)
#include "core/fxcrt/win/win_util.h"
#endif

namespace {

const int nMaxRecursion = 32;

#if BUILDFLAG(IS_WIN)
struct PDF_FONTDATA {
  bool bFind;
  LOGFONTA lf;
};

int CALLBACK EnumFontFamExProc(ENUMLOGFONTEXA* lpelfe,
                               NEWTEXTMETRICEX* lpntme,
                               DWORD FontType,
                               LPARAM lParam) {
  if (FontType != 0x004 || strchr(lpelfe->elfLogFont.lfFaceName, '@'))
    return 1;

  PDF_FONTDATA* pData = (PDF_FONTDATA*)lParam;
  pData->lf = lpelfe->elfLogFont;
  pData->bFind = true;
  return 0;
}

bool RetrieveSpecificFont(FX_Charset charSet,
                          LPCSTR pcsFontName,
                          LOGFONTA& lf) {
  lf = {};  // Aggregate initialization, not construction.
  static_assert(std::is_aggregate_v<std::remove_reference_t<decltype(lf)>>);
  lf.lfCharSet = static_cast<int>(charSet);
  lf.lfPitchAndFamily = DEFAULT_PITCH | FF_DONTCARE;
  if (pcsFontName) {
    // TODO(dsinclair): Should this be strncpy?
    // NOLINTNEXTLINE(runtime/printf)
    strcpy(lf.lfFaceName, pcsFontName);
  }

  PDF_FONTDATA fd = {};  // Aggregate initialization, not construction.
  static_assert(std::is_aggregate_v<decltype(fd)>);
  HDC hDC = ::GetDC(nullptr);
  EnumFontFamiliesExA(hDC, &lf, (FONTENUMPROCA)EnumFontFamExProc, (LPARAM)&fd,
                      0);
  ::ReleaseDC(nullptr, hDC);
  if (fd.bFind) {
    UNSAFE_TODO(FXSYS_memcpy(&lf, &fd.lf, sizeof(LOGFONTA)));
  }
  return fd.bFind;
}
#endif  // BUILDFLAG(IS_WIN)

ByteString GetNativeFontName(FX_Charset charSet, void* pLogFont) {
  ByteString csFontName;
#if BUILDFLAG(IS_WIN)
  LOGFONTA lf = {};
  if (charSet == FX_Charset::kANSI) {
    return CFX_Font::kDefaultAnsiFontName;
  }

  if (!pdfium::IsUser32AndGdi32Available()) {
    // Without GDI32 and User32, GetDC / EnumFontFamiliesExW / ReleaseDC all
    // fail, which is called by RetrieveSpecificFont. We won't be able to look
    // up native fonts without GDI.
    return ByteString();
  }

  bool bRet = false;
  const ByteString default_font_name =
      CFX_Font::GetDefaultFontNameByCharset(charSet);
  if (!default_font_name.IsEmpty())
    bRet = RetrieveSpecificFont(charSet, default_font_name.c_str(), lf);
  if (!bRet) {
    bRet =
        RetrieveSpecificFont(charSet, CFX_Font::kUniversalDefaultFontName, lf);
  }
  if (!bRet)
    bRet = RetrieveSpecificFont(charSet, "Microsoft Sans Serif", lf);
  if (!bRet)
    bRet = RetrieveSpecificFont(charSet, nullptr, lf);
  if (bRet) {
    if (pLogFont) {
      UNSAFE_TODO(FXSYS_memcpy(pLogFont, &lf, sizeof(LOGFONTA)));
    }
    csFontName = lf.lfFaceName;
  }
#endif
  return csFontName;
}

ByteString GenerateNewFontResourceName(const CPDF_Dictionary* pResDict,
                                       const ByteString& csPrefix) {
  static const char kDummyFontName[] = "ZiTi";
  ByteString csStr = csPrefix;
  if (csStr.IsEmpty())
    csStr = kDummyFontName;

  const size_t szCount = csStr.GetLength();
  size_t m = 0;
  ByteString csTmp;
  while (m < strlen(kDummyFontName) && m < szCount)
    csTmp += csStr[m++];
  while (m < strlen(kDummyFontName)) {
    csTmp += '0' + m % 10;
    m++;
  }

  RetainPtr<const CPDF_Dictionary> pDict = pResDict->GetDictFor("Font");
  DCHECK(pDict);

  int num = 0;
  ByteString bsNum;
  while (true) {
    ByteString csKey = csTmp + bsNum;
    if (!pDict->KeyExist(csKey))
      return csKey;

    if (m < szCount)
      csTmp += csStr[m++];
    else
      bsNum = ByteString::FormatInteger(num++);
    m++;
  }
}

RetainPtr<CPDF_Font> AddStandardFont(CPDF_Document* pDocument) {
  auto* pPageData = CPDF_DocPageData::FromDocument(pDocument);
  static const CPDF_FontEncoding encoding(FontEncoding::kWinAnsi);
  return pPageData->AddStandardFont(CFX_Font::kDefaultAnsiFontName, &encoding);
}

RetainPtr<CPDF_Font> AddNativeFont(FX_Charset charSet,
                                   CPDF_Document* pDocument) {
  DCHECK(pDocument);

#if BUILDFLAG(IS_WIN)
  LOGFONTA lf;
  ByteString csFontName = GetNativeFontName(charSet, &lf);
  if (!csFontName.IsEmpty()) {
    if (csFontName == CFX_Font::kDefaultAnsiFontName)
      return AddStandardFont(pDocument);
    return CPDF_DocPageData::FromDocument(pDocument)->AddWindowsFont(&lf);
  }
#endif
  return nullptr;
}

bool FindFont(const CPDF_Dictionary* pFormDict,
              const CPDF_Font* pFont,
              ByteString* csNameTag) {
  RetainPtr<const CPDF_Dictionary> pDR = pFormDict->GetDictFor("DR");
  if (!pDR)
    return false;

  RetainPtr<const CPDF_Dictionary> pFonts = pDR->GetDictFor("Font");
  // TODO(tsepez): this eventually locks the dict, pass locker instead.
  if (!ValidateFontResourceDict(pFonts.Get()))
    return false;

  CPDF_DictionaryLocker locker(std::move(pFonts));
  for (const auto& it : locker) {
    const ByteString& csKey = it.first;
    RetainPtr<const CPDF_Dictionary> pElement =
        ToDictionary(it.second->GetDirect());
    if (!ValidateDictType(pElement.Get(), "Font"))
      continue;
    if (pFont->FontDictIs(pElement)) {
      *csNameTag = csKey;
      return true;
    }
  }
  return false;
}

bool FindFontFromDoc(const CPDF_Dictionary* pFormDict,
                     CPDF_Document* pDocument,
                     ByteString csFontName,
                     RetainPtr<CPDF_Font>& pFont,
                     ByteString* csNameTag) {
  if (csFontName.IsEmpty())
    return false;

  RetainPtr<const CPDF_Dictionary> pDR = pFormDict->GetDictFor("DR");
  if (!pDR)
    return false;

  RetainPtr<const CPDF_Dictionary> pFonts = pDR->GetDictFor("Font");
  if (!ValidateFontResourceDict(pFonts.Get()))
    return false;

  csFontName.Remove(' ');
  CPDF_DictionaryLocker locker(pFonts);
  for (const auto& it : locker) {
    const ByteString& csKey = it.first;
    RetainPtr<CPDF_Dictionary> pElement =
        ToDictionary(it.second->GetMutableDirect());
    if (!ValidateDictType(pElement.Get(), "Font"))
      continue;

    auto* pData = CPDF_DocPageData::FromDocument(pDocument);
    pFont = pData->GetFont(std::move(pElement));
    if (!pFont)
      continue;

    ByteString csBaseFont = pFont->GetBaseFontName();
    csBaseFont.Remove(' ');
    if (csBaseFont == csFontName) {
      *csNameTag = csKey;
      return true;
    }
  }
  return false;
}

void AddFont(CPDF_Dictionary* pFormDict,
             CPDF_Document* pDocument,
             const RetainPtr<CPDF_Font>& pFont,
             ByteString* csNameTag) {
  DCHECK(pFormDict);
  DCHECK(pFont);

  ByteString csTag;
  if (FindFont(pFormDict, pFont.Get(), &csTag)) {
    *csNameTag = std::move(csTag);
    return;
  }

  RetainPtr<CPDF_Dictionary> pDR = pFormDict->GetOrCreateDictFor("DR");
  RetainPtr<CPDF_Dictionary> pFonts = pDR->GetOrCreateDictFor("Font");

  if (csNameTag->IsEmpty())
    *csNameTag = pFont->GetBaseFontName();

  csNameTag->Remove(' ');
  *csNameTag = GenerateNewFontResourceName(pDR.Get(), *csNameTag);
  pFonts->SetNewFor<CPDF_Reference>(*csNameTag, pDocument,
                                    pFont->GetFontDictObjNum());
}

FX_Charset GetNativeCharSet() {
  return FX_GetCharsetFromCodePage(FX_GetACP());
}

RetainPtr<CPDF_Dictionary> InitDict(CPDF_Document* pDocument) {
  auto pFormDict = pDocument->NewIndirect<CPDF_Dictionary>();
  pDocument->GetMutableRoot()->SetNewFor<CPDF_Reference>(
      "AcroForm", pDocument, pFormDict->GetObjNum());

  ByteString csBaseName;
  FX_Charset charSet = GetNativeCharSet();
  RetainPtr<CPDF_Font> pFont = AddStandardFont(pDocument);
  if (pFont) {
    AddFont(pFormDict.Get(), pDocument, pFont, &csBaseName);
  }
  if (charSet != FX_Charset::kANSI) {
    ByteString csFontName = GetNativeFontName(charSet, nullptr);
    if (!pFont || csFontName != CFX_Font::kDefaultAnsiFontName) {
      pFont = AddNativeFont(charSet, pDocument);
      if (pFont) {
        csBaseName.clear();
        AddFont(pFormDict.Get(), pDocument, pFont, &csBaseName);
      }
    }
  }
  ByteString csDA;
  if (pFont)
    csDA = "/" + PDF_NameEncode(csBaseName) + " 0 Tf ";
  csDA += "0 g";
  pFormDict->SetNewFor<CPDF_String>("DA", csDA);
  return pFormDict;
}

RetainPtr<CPDF_Font> GetNativeFont(const CPDF_Dictionary* pFormDict,
                                   CPDF_Document* pDocument,
                                   FX_Charset charSet,
                                   ByteString* csNameTag) {
  RetainPtr<const CPDF_Dictionary> pDR = pFormDict->GetDictFor("DR");
  if (!pDR)
    return nullptr;

  RetainPtr<const CPDF_Dictionary> pFonts = pDR->GetDictFor("Font");
  if (!ValidateFontResourceDict(pFonts.Get()))
    return nullptr;

  CPDF_DictionaryLocker locker(pFonts);
  for (const auto& it : locker) {
    const ByteString& csKey = it.first;
    RetainPtr<CPDF_Dictionary> pElement =
        ToDictionary(it.second->GetMutableDirect());
    if (!ValidateDictType(pElement.Get(), "Font"))
      continue;

    auto* pData = CPDF_DocPageData::FromDocument(pDocument);
    RetainPtr<CPDF_Font> pFind = pData->GetFont(std::move(pElement));
    if (!pFind)
      continue;

    auto maybe_charset = pFind->GetSubstFontCharset();
    if (maybe_charset.has_value() && maybe_charset.value() == charSet) {
      *csNameTag = csKey;
      return pFind;
    }
  }
  return nullptr;
}

class CFieldNameExtractor {
 public:
  explicit CFieldNameExtractor(const WideString& full_name)
      : m_FullName(full_name) {}

  WideStringView GetNext() {
    size_t start_pos = m_iCur;
    while (m_iCur < m_FullName.GetLength() && m_FullName[m_iCur] != L'.')
      ++m_iCur;

    size_t length = m_iCur - start_pos;
    if (m_iCur < m_FullName.GetLength() && m_FullName[m_iCur] == L'.')
      ++m_iCur;

    return m_FullName.AsStringView().Substr(start_pos, length);
  }

 protected:
  const WideString m_FullName;
  size_t m_iCur = 0;
};

}  // namespace

class CFieldTree {
 public:
  class Node {
   public:
    Node() : m_level(0) {}
    Node(const WideString& short_name, int level)
        : m_ShortName(short_name), m_level(level) {}
    ~Node() = default;

    void AddChildNode(std::unique_ptr<Node> pNode) {
      m_Children.push_back(std::move(pNode));
    }

    size_t GetChildrenCount() const { return m_Children.size(); }

    Node* GetChildAt(size_t i) { return m_Children[i].get(); }
    const Node* GetChildAt(size_t i) const { return m_Children[i].get(); }

    CPDF_FormField* GetFieldAtIndex(size_t index) {
      size_t nFieldsToGo = index;
      return GetFieldInternal(&nFieldsToGo);
    }

    size_t CountFields() const { return CountFieldsInternal(); }

    void SetField(std::unique_ptr<CPDF_FormField> pField) {
      m_pField = std::move(pField);
    }

    CPDF_FormField* GetField() const { return m_pField.get(); }
    WideString GetShortName() const { return m_ShortName; }
    int GetLevel() const { return m_level; }

   private:
    CPDF_FormField* GetFieldInternal(size_t* pFieldsToGo) {
      if (m_pField) {
        if (*pFieldsToGo == 0)
          return m_pField.get();

        --*pFieldsToGo;
      }
      for (size_t i = 0; i < GetChildrenCount(); ++i) {
        CPDF_FormField* pField = GetChildAt(i)->GetFieldInternal(pFieldsToGo);
        if (pField)
          return pField;
      }
      return nullptr;
    }

    size_t CountFieldsInternal() const {
      size_t count = 0;
      if (m_pField)
        ++count;

      for (size_t i = 0; i < GetChildrenCount(); ++i)
        count += GetChildAt(i)->CountFieldsInternal();
      return count;
    }

    std::vector<std::unique_ptr<Node>> m_Children;
    WideString m_ShortName;
    std::unique_ptr<CPDF_FormField> m_pField;
    const int m_level;
  };

  CFieldTree();
  ~CFieldTree();

  bool SetField(const WideString& full_name,
                std::unique_ptr<CPDF_FormField> pField);
  CPDF_FormField* GetField(const WideString& full_name);

  Node* GetRoot() { return m_pRoot.get(); }
  Node* FindNode(const WideString& full_name);
  Node* AddChild(Node* pParent, const WideString& short_name);
  Node* Lookup(Node* pParent, WideStringView short_name);

 private:
  std::unique_ptr<Node> m_pRoot;
};

CFieldTree::CFieldTree() : m_pRoot(std::make_unique<Node>()) {}

CFieldTree::~CFieldTree() = default;

CFieldTree::Node* CFieldTree::AddChild(Node* pParent,
                                       const WideString& short_name) {
  if (!pParent)
    return nullptr;

  int level = pParent->GetLevel() + 1;
  if (level > nMaxRecursion)
    return nullptr;

  auto pNew = std::make_unique<Node>(short_name, pParent->GetLevel() + 1);
  Node* pChild = pNew.get();
  pParent->AddChildNode(std::move(pNew));
  return pChild;
}

CFieldTree::Node* CFieldTree::Lookup(Node* pParent, WideStringView short_name) {
  if (!pParent)
    return nullptr;

  for (size_t i = 0; i < pParent->GetChildrenCount(); ++i) {
    Node* pNode = pParent->GetChildAt(i);
    if (pNode->GetShortName() == short_name)
      return pNode;
  }
  return nullptr;
}

bool CFieldTree::SetField(const WideString& full_name,
                          std::unique_ptr<CPDF_FormField> pField) {
  if (full_name.IsEmpty())
    return false;

  Node* pNode = GetRoot();
  Node* pLast = nullptr;
  CFieldNameExtractor name_extractor(full_name);
  while (true) {
    WideStringView name_view = name_extractor.GetNext();
    if (name_view.IsEmpty())
      break;
    pLast = pNode;
    pNode = Lookup(pLast, name_view);
    if (pNode)
      continue;
    pNode = AddChild(pLast, WideString(name_view));
    if (!pNode)
      return false;
  }
  if (pNode == GetRoot())
    return false;

  pNode->SetField(std::move(pField));
  return true;
}

CPDF_FormField* CFieldTree::GetField(const WideString& full_name) {
  if (full_name.IsEmpty())
    return nullptr;

  Node* pNode = GetRoot();
  Node* pLast = nullptr;
  CFieldNameExtractor name_extractor(full_name);
  while (pNode) {
    WideStringView name_view = name_extractor.GetNext();
    if (name_view.IsEmpty())
      break;
    pLast = pNode;
    pNode = Lookup(pLast, name_view);
  }
  return pNode ? pNode->GetField() : nullptr;
}

CFieldTree::Node* CFieldTree::FindNode(const WideString& full_name) {
  if (full_name.IsEmpty())
    return nullptr;

  Node* pNode = GetRoot();
  Node* pLast = nullptr;
  CFieldNameExtractor name_extractor(full_name);
  while (pNode) {
    WideStringView name_view = name_extractor.GetNext();
    if (name_view.IsEmpty())
      break;
    pLast = pNode;
    pNode = Lookup(pLast, name_view);
  }
  return pNode;
}

CPDF_InteractiveForm::CPDF_InteractiveForm(CPDF_Document* pDocument)
    : m_pDocument(pDocument), m_pFieldTree(std::make_unique<CFieldTree>()) {
  RetainPtr<CPDF_Dictionary> pRoot = m_pDocument->GetMutableRoot();
  if (!pRoot)
    return;

  m_pFormDict = pRoot->GetMutableDictFor("AcroForm");
  if (!m_pFormDict)
    return;

  RetainPtr<CPDF_Array> pFields = m_pFormDict->GetMutableArrayFor("Fields");
  if (!pFields)
    return;

  for (size_t i = 0; i < pFields->size(); ++i)
    LoadField(pFields->GetMutableDictAt(i), 0);
}

CPDF_InteractiveForm::~CPDF_InteractiveForm() = default;

bool CPDF_InteractiveForm::s_bUpdateAP = true;

// static
bool CPDF_InteractiveForm::IsUpdateAPEnabled() {
  return s_bUpdateAP;
}

// static
void CPDF_InteractiveForm::SetUpdateAP(bool bUpdateAP) {
  s_bUpdateAP = bUpdateAP;
}

// static
RetainPtr<CPDF_Font> CPDF_InteractiveForm::AddNativeInteractiveFormFont(
    CPDF_Document* pDocument,
    ByteString* csNameTag) {
  DCHECK(pDocument);
  DCHECK(csNameTag);

  RetainPtr<CPDF_Dictionary> pFormDict =
      pDocument->GetMutableRoot()->GetMutableDictFor("AcroForm");
  if (!pFormDict)
    pFormDict = InitDict(pDocument);

  FX_Charset charSet = GetNativeCharSet();
  ByteString csTemp;
  RetainPtr<CPDF_Font> pFont =
      GetNativeFont(pFormDict.Get(), pDocument, charSet, &csTemp);
  if (pFont) {
    *csNameTag = std::move(csTemp);
    return pFont;
  }
  ByteString csFontName = GetNativeFontName(charSet, nullptr);
  if (FindFontFromDoc(pFormDict.Get(), pDocument, csFontName, pFont, csNameTag))
    return pFont;

  pFont = AddNativeFont(charSet, pDocument);
  if (!pFont)
    return nullptr;

  AddFont(pFormDict.Get(), pDocument, pFont, csNameTag);
  return pFont;
}

size_t CPDF_InteractiveForm::CountFields(const WideString& csFieldName) const {
  if (csFieldName.IsEmpty())
    return m_pFieldTree->GetRoot()->CountFields();

  CFieldTree::Node* pNode = m_pFieldTree->FindNode(csFieldName);
  return pNode ? pNode->CountFields() : 0;
}

CPDF_FormField* CPDF_InteractiveForm::GetField(
    size_t index,
    const WideString& csFieldName) const {
  if (csFieldName.IsEmpty())
    return m_pFieldTree->GetRoot()->GetFieldAtIndex(index);

  CFieldTree::Node* pNode = m_pFieldTree->FindNode(csFieldName);
  return pNode ? pNode->GetFieldAtIndex(index) : nullptr;
}

CPDF_FormField* CPDF_InteractiveForm::GetFieldByDict(
    const CPDF_Dictionary* pFieldDict) const {
  if (!pFieldDict)
    return nullptr;

  WideString csWName = CPDF_FormField::GetFullNameForDict(pFieldDict);
  return m_pFieldTree->GetField(csWName);
}

const CPDF_FormControl* CPDF_InteractiveForm::GetControlAtPoint(
    const CPDF_Page* pPage,
    const CFX_PointF& point,
    int* z_order) const {
  RetainPtr<const CPDF_Array> pAnnotList = pPage->GetAnnotsArray();
  if (!pAnnotList)
    return nullptr;

  for (size_t i = pAnnotList->size(); i > 0; --i) {
    size_t annot_index = i - 1;
    RetainPtr<const CPDF_Dictionary> pAnnot =
        pAnnotList->GetDictAt(annot_index);
    if (!pAnnot)
      continue;

    const auto it = m_ControlMap.find(pAnnot.Get());
    if (it == m_ControlMap.end())
      continue;

    const CPDF_FormControl* pControl = it->second.get();
    if (!pControl->GetRect().Contains(point))
      continue;

    if (z_order)
      *z_order = static_cast<int>(annot_index);
    return pControl;
  }
  return nullptr;
}

CPDF_FormControl* CPDF_InteractiveForm::GetControlByDict(
    const CPDF_Dictionary* pWidgetDict) const {
  const auto it = m_ControlMap.find(pWidgetDict);
  return it != m_ControlMap.end() ? it->second.get() : nullptr;
}

bool CPDF_InteractiveForm::NeedConstructAP() const {
  return m_pFormDict && m_pFormDict->GetBooleanFor("NeedAppearances", false);
}

int CPDF_InteractiveForm::CountFieldsInCalculationOrder() {
  if (!m_pFormDict)
    return 0;

  RetainPtr<const CPDF_Array> pArray = m_pFormDict->GetArrayFor("CO");
  return pArray ? fxcrt::CollectionSize<int>(*pArray) : 0;
}

CPDF_FormField* CPDF_InteractiveForm::GetFieldInCalculationOrder(int index) {
  if (!m_pFormDict || index < 0)
    return nullptr;

  RetainPtr<const CPDF_Array> pArray = m_pFormDict->GetArrayFor("CO");
  if (!pArray)
    return nullptr;

  RetainPtr<const CPDF_Dictionary> pElement =
      ToDictionary(pArray->GetDirectObjectAt(index));
  return pElement ? GetFieldByDict(pElement.Get()) : nullptr;
}

int CPDF_InteractiveForm::FindFieldInCalculationOrder(
    const CPDF_FormField* pField) {
  if (!m_pFormDict)
    return -1;

  RetainPtr<const CPDF_Array> pArray = m_pFormDict->GetArrayFor("CO");
  if (!pArray)
    return -1;

  std::optional<size_t> maybe_found = pArray->Find(pField->GetFieldDict());
  if (!maybe_found.has_value())
    return -1;

  return pdfium::checked_cast<int>(maybe_found.value());
}

RetainPtr<CPDF_Font> CPDF_InteractiveForm::GetFormFont(
    ByteString csNameTag) const {
  ByteString csAlias = PDF_NameDecode(csNameTag.AsStringView());
  if (!m_pFormDict || csAlias.IsEmpty())
    return nullptr;

  RetainPtr<CPDF_Dictionary> pDR = m_pFormDict->GetMutableDictFor("DR");
  if (!pDR)
    return nullptr;

  RetainPtr<CPDF_Dictionary> pFonts = pDR->GetMutableDictFor("Font");
  if (!ValidateFontResourceDict(pFonts.Get()))
    return nullptr;

  RetainPtr<CPDF_Dictionary> pElement = pFonts->GetMutableDictFor(csAlias);
  if (!ValidateDictType(pElement.Get(), "Font"))
    return nullptr;

  return GetFontForElement(std::move(pElement));
}

RetainPtr<CPDF_Font> CPDF_InteractiveForm::GetFontForElement(
    RetainPtr<CPDF_Dictionary> pElement) const {
  auto* pData = CPDF_DocPageData::FromDocument(m_pDocument);
  return pData->GetFont(std::move(pElement));
}

CPDF_DefaultAppearance CPDF_InteractiveForm::GetDefaultAppearance() const {
  return CPDF_DefaultAppearance(
      m_pFormDict ? m_pFormDict->GetByteStringFor("DA") : "");
}

int CPDF_InteractiveForm::GetFormAlignment() const {
  return m_pFormDict ? m_pFormDict->GetIntegerFor("Q", 0) : 0;
}

void CPDF_InteractiveForm::ResetForm(pdfium::span<CPDF_FormField*> fields,
                                     bool bIncludeOrExclude) {
  CFieldTree::Node* pRoot = m_pFieldTree->GetRoot();
  const size_t nCount = pRoot->CountFields();
  for (size_t i = 0; i < nCount; ++i) {
    CPDF_FormField* pField = pRoot->GetFieldAtIndex(i);
    if (!pField)
      continue;

    if (bIncludeOrExclude == pdfium::Contains(fields, pField))
      pField->ResetField();
  }
  if (m_pFormNotify)
    m_pFormNotify->AfterFormReset(this);
}

void CPDF_InteractiveForm::ResetForm() {
  ResetForm(/*fields=*/{}, /*bIncludeOrExclude=*/false);
}

const std::vector<UnownedPtr<CPDF_FormControl>>&
CPDF_InteractiveForm::GetControlsForField(const CPDF_FormField* pField) {
  return m_ControlLists[pdfium::WrapUnowned(pField)];
}

void CPDF_InteractiveForm::LoadField(RetainPtr<CPDF_Dictionary> pFieldDict,
                                     int nLevel) {
  if (nLevel > nMaxRecursion)
    return;
  if (!pFieldDict)
    return;

  uint32_t dwParentObjNum = pFieldDict->GetObjNum();
  RetainPtr<CPDF_Array> pKids =
      pFieldDict->GetMutableArrayFor(pdfium::form_fields::kKids);
  if (!pKids) {
    AddTerminalField(std::move(pFieldDict));
    return;
  }

  RetainPtr<const CPDF_Dictionary> pFirstKid = pKids->GetDictAt(0);
  if (!pFirstKid)
    return;

  if (!pFirstKid->KeyExist(pdfium::form_fields::kT) &&
      !pFirstKid->KeyExist(pdfium::form_fields::kKids)) {
    AddTerminalField(std::move(pFieldDict));
    return;
  }
  for (size_t i = 0; i < pKids->size(); i++) {
    RetainPtr<CPDF_Dictionary> pChildDict = pKids->GetMutableDictAt(i);
    if (pChildDict && pChildDict->GetObjNum() != dwParentObjNum)
      LoadField(std::move(pChildDict), nLevel + 1);
  }
}

void CPDF_InteractiveForm::FixPageFields(CPDF_Page* pPage) {
  RetainPtr<CPDF_Array> pAnnots = pPage->GetMutableAnnotsArray();
  if (!pAnnots)
    return;

  for (size_t i = 0; i < pAnnots->size(); i++) {
    RetainPtr<CPDF_Dictionary> pAnnot = pAnnots->GetMutableDictAt(i);
    if (pAnnot && pAnnot->GetNameFor("Subtype") == "Widget")
      LoadField(std::move(pAnnot), 0);
  }
}

void CPDF_InteractiveForm::AddTerminalField(
    RetainPtr<CPDF_Dictionary> pFieldDict) {
  if (!pFieldDict->KeyExist(pdfium::form_fields::kFT)) {
    // Key "FT" is required for terminal fields, it is also inheritable.
    RetainPtr<const CPDF_Dictionary> pParentDict =
        pFieldDict->GetDictFor(pdfium::form_fields::kParent);
    if (!pParentDict || !pParentDict->KeyExist(pdfium::form_fields::kFT))
      return;
  }

  WideString csWName = CPDF_FormField::GetFullNameForDict(pFieldDict.Get());
  if (csWName.IsEmpty())
    return;

  CPDF_FormField* pField = nullptr;
  pField = m_pFieldTree->GetField(csWName);
  if (!pField) {
    RetainPtr<CPDF_Dictionary> pParent(pFieldDict);
    if (!pFieldDict->KeyExist(pdfium::form_fields::kT) &&
        pFieldDict->GetNameFor("Subtype") == "Widget") {
      pParent = pFieldDict->GetMutableDictFor(pdfium::form_fields::kParent);
      if (!pParent)
        pParent = pFieldDict;
    }

    if (pParent && pParent != pFieldDict &&
        !pParent->KeyExist(pdfium::form_fields::kFT)) {
      if (pFieldDict->KeyExist(pdfium::form_fields::kFT)) {
        RetainPtr<const CPDF_Object> pFTValue =
            pFieldDict->GetDirectObjectFor(pdfium::form_fields::kFT);
        if (pFTValue)
          pParent->SetFor(pdfium::form_fields::kFT, pFTValue->Clone());
      }

      if (pFieldDict->KeyExist(pdfium::form_fields::kFf)) {
        RetainPtr<const CPDF_Object> pFfValue =
            pFieldDict->GetDirectObjectFor(pdfium::form_fields::kFf);
        if (pFfValue)
          pParent->SetFor(pdfium::form_fields::kFf, pFfValue->Clone());
      }
    }

    auto newField = std::make_unique<CPDF_FormField>(this, std::move(pParent));
    pField = newField.get();
    RetainPtr<const CPDF_Object> pTObj =
        pFieldDict->GetObjectFor(pdfium::form_fields::kT);
    if (ToReference(pTObj)) {
      RetainPtr<CPDF_Object> pClone = pTObj->CloneDirectObject();
      if (pClone)
        pFieldDict->SetFor(pdfium::form_fields::kT, std::move(pClone));
      else
        pFieldDict->SetNewFor<CPDF_Name>(pdfium::form_fields::kT, ByteString());
    }
    if (!m_pFieldTree->SetField(csWName, std::move(newField)))
      return;
  }

  RetainPtr<CPDF_Array> pKids =
      pFieldDict->GetMutableArrayFor(pdfium::form_fields::kKids);
  if (!pKids) {
    if (pFieldDict->GetNameFor("Subtype") == "Widget")
      AddControl(pField, std::move(pFieldDict));
    return;
  }
  for (size_t i = 0; i < pKids->size(); i++) {
    RetainPtr<CPDF_Dictionary> pKid = pKids->GetMutableDictAt(i);
    if (pKid && pKid->GetNameFor("Subtype") == "Widget")
      AddControl(pField, std::move(pKid));
  }
}

CPDF_FormControl* CPDF_InteractiveForm::AddControl(
    CPDF_FormField* pField,
    RetainPtr<CPDF_Dictionary> pWidgetDict) {
  DCHECK(pWidgetDict);
  const auto it = m_ControlMap.find(pWidgetDict.Get());
  if (it != m_ControlMap.end())
    return it->second.get();

  auto pNew = std::make_unique<CPDF_FormControl>(pField, pWidgetDict, this);
  CPDF_FormControl* pControl = pNew.get();
  m_ControlMap[pWidgetDict] = std::move(pNew);
  m_ControlLists[pdfium::WrapUnowned(pField)].emplace_back(pControl);
  return pControl;
}

bool CPDF_InteractiveForm::CheckRequiredFields(
    const std::vector<CPDF_FormField*>* fields,
    bool bIncludeOrExclude) const {
  CFieldTree::Node* pRoot = m_pFieldTree->GetRoot();
  const size_t nCount = pRoot->CountFields();
  for (size_t i = 0; i < nCount; ++i) {
    CPDF_FormField* pField = pRoot->GetFieldAtIndex(i);
    if (!pField)
      continue;

    int32_t iType = pField->GetType();
    if (iType == CPDF_FormField::kPushButton ||
        iType == CPDF_FormField::kCheckBox ||
        iType == CPDF_FormField::kListBox) {
      continue;
    }
    if (pField->IsNoExport())
      continue;

    bool bFind = true;
    if (fields)
      bFind = pdfium::Contains(*fields, pField);
    if (bIncludeOrExclude == bFind) {
      RetainPtr<const CPDF_Dictionary> pFieldDict = pField->GetFieldDict();
      if (pField->IsRequired() &&
          pFieldDict->GetByteStringFor(pdfium::form_fields::kV).IsEmpty()) {
        return false;
      }
    }
  }
  return true;
}

std::unique_ptr<CFDF_Document> CPDF_InteractiveForm::ExportToFDF(
    const WideString& pdf_path) const {
  std::vector<CPDF_FormField*> fields;
  CFieldTree::Node* pRoot = m_pFieldTree->GetRoot();
  const size_t nCount = pRoot->CountFields();
  for (size_t i = 0; i < nCount; ++i)
    fields.push_back(pRoot->GetFieldAtIndex(i));
  return ExportToFDF(pdf_path, fields, true);
}

std::unique_ptr<CFDF_Document> CPDF_InteractiveForm::ExportToFDF(
    const WideString& pdf_path,
    const std::vector<CPDF_FormField*>& fields,
    bool bIncludeOrExclude) const {
  std::unique_ptr<CFDF_Document> pDoc = CFDF_Document::CreateNewDoc();
  if (!pDoc)
    return nullptr;

  RetainPtr<CPDF_Dictionary> pMainDict =
      pDoc->GetMutableRoot()->GetMutableDictFor("FDF");
  if (!pdf_path.IsEmpty()) {
    auto pNewDict = pDoc->New<CPDF_Dictionary>();
    pNewDict->SetNewFor<CPDF_Name>("Type", "Filespec");
    WideString wsStr = CPDF_FileSpec::EncodeFileName(pdf_path);
    pNewDict->SetNewFor<CPDF_String>(pdfium::stream::kF, wsStr.ToDefANSI());
    pNewDict->SetNewFor<CPDF_String>("UF", wsStr.AsStringView());
    pMainDict->SetFor("F", pNewDict);
  }

  auto pFields = pMainDict->SetNewFor<CPDF_Array>("Fields");
  CFieldTree::Node* pRoot = m_pFieldTree->GetRoot();
  const size_t nCount = pRoot->CountFields();
  for (size_t i = 0; i < nCount; ++i) {
    CPDF_FormField* pField = pRoot->GetFieldAtIndex(i);
    if (!pField || pField->GetType() == CPDF_FormField::kPushButton)
      continue;

    uint32_t dwFlags = pField->GetFieldFlags();
    if (dwFlags & pdfium::form_flags::kNoExport)
      continue;

    if (bIncludeOrExclude != pdfium::Contains(fields, pField))
      continue;

    if ((dwFlags & pdfium::form_flags::kRequired) != 0 &&
        pField->GetFieldDict()
            ->GetByteStringFor(pdfium::form_fields::kV)
            .IsEmpty()) {
      continue;
    }

    WideString fullname =
        CPDF_FormField::GetFullNameForDict(pField->GetFieldDict());
    auto pFieldDict = pDoc->New<CPDF_Dictionary>();
    pFieldDict->SetNewFor<CPDF_String>(pdfium::form_fields::kT,
                                       fullname.AsStringView());
    if (pField->GetType() == CPDF_FormField::kCheckBox ||
        pField->GetType() == CPDF_FormField::kRadioButton) {
      WideString csExport = pField->GetCheckValue(false);
      ByteString csBExport = PDF_EncodeText(csExport.AsStringView());
      RetainPtr<const CPDF_Object> pOpt = pField->GetFieldAttr("Opt");
      if (pOpt) {
        pFieldDict->SetNewFor<CPDF_String>(pdfium::form_fields::kV, csBExport);
      } else {
        pFieldDict->SetNewFor<CPDF_Name>(pdfium::form_fields::kV, csBExport);
      }
    } else {
      RetainPtr<const CPDF_Object> pV =
          pField->GetFieldAttr(pdfium::form_fields::kV);
      if (pV)
        pFieldDict->SetFor(pdfium::form_fields::kV, pV->CloneDirectObject());
    }
    pFields->Append(pFieldDict);
  }
  return pDoc;
}

void CPDF_InteractiveForm::SetNotifierIface(NotifierIface* pNotify) {
  m_pFormNotify = pNotify;
}

bool CPDF_InteractiveForm::NotifyBeforeValueChange(CPDF_FormField* pField,
                                                   const WideString& csValue) {
  return !m_pFormNotify || m_pFormNotify->BeforeValueChange(pField, csValue);
}

void CPDF_InteractiveForm::NotifyAfterValueChange(CPDF_FormField* pField) {
  if (m_pFormNotify)
    m_pFormNotify->AfterValueChange(pField);
}

bool CPDF_InteractiveForm::NotifyBeforeSelectionChange(
    CPDF_FormField* pField,
    const WideString& csValue) {
  return !m_pFormNotify ||
         m_pFormNotify->BeforeSelectionChange(pField, csValue);
}

void CPDF_InteractiveForm::NotifyAfterSelectionChange(CPDF_FormField* pField) {
  if (m_pFormNotify)
    m_pFormNotify->AfterSelectionChange(pField);
}

void CPDF_InteractiveForm::NotifyAfterCheckedStatusChange(
    CPDF_FormField* pField) {
  if (m_pFormNotify)
    m_pFormNotify->AfterCheckedStatusChange(pField);
}
