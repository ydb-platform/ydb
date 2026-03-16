// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/parser/cpdf_data_avail.h"

#include <algorithm>
#include <memory>
#include <utility>

#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_cross_ref_avail.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fpdfapi/parser/cpdf_hint_tables.h"
#include "core/fpdfapi/parser/cpdf_linearized_header.h"
#include "core/fpdfapi/parser/cpdf_name.h"
#include "core/fpdfapi/parser/cpdf_number.h"
#include "core/fpdfapi/parser/cpdf_page_object_avail.h"
#include "core/fpdfapi/parser/cpdf_read_validator.h"
#include "core/fpdfapi/parser/cpdf_reference.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fpdfapi/parser/cpdf_syntax_parser.h"
#include "core/fpdfapi/parser/fpdf_parser_utility.h"
#include "core/fxcrt/autorestorer.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/containers/contains.h"
#include "core/fxcrt/fx_extension.h"
#include "core/fxcrt/fx_safe_types.h"
#include "core/fxcrt/notreached.h"
#include "core/fxcrt/numerics/safe_conversions.h"
#include "core/fxcrt/stl_util.h"

namespace {

RetainPtr<CPDF_Object> GetResourceObject(RetainPtr<CPDF_Dictionary> pDict) {
  constexpr size_t kMaxHierarchyDepth = 64;
  size_t depth = 0;

  while (pDict) {
    RetainPtr<CPDF_Object> result = pDict->GetMutableObjectFor("Resources");
    if (result)
      return result;
    if (++depth > kMaxHierarchyDepth) {
      // We have cycle in parents hierarchy.
      return nullptr;
    }
    RetainPtr<CPDF_Object> parent = pDict->GetMutableObjectFor("Parent");
    pDict = parent ? parent->GetMutableDict() : nullptr;
  }
  return nullptr;
}

class HintsScope {
 public:
  HintsScope(RetainPtr<CPDF_ReadValidator> validator,
             CPDF_DataAvail::DownloadHints* hints)
      : validator_(std::move(validator)) {
    DCHECK(validator_);
    validator_->SetDownloadHints(hints);
  }

  ~HintsScope() { validator_->SetDownloadHints(nullptr); }

 private:
  RetainPtr<CPDF_ReadValidator> validator_;
};

}  // namespace

CPDF_DataAvail::FileAvail::~FileAvail() = default;

CPDF_DataAvail::DownloadHints::~DownloadHints() = default;

CPDF_DataAvail::CPDF_DataAvail(FileAvail* pFileAvail,
                               RetainPtr<IFX_SeekableReadStream> pFileRead)
    : m_pFileRead(pdfium::MakeRetain<CPDF_ReadValidator>(std::move(pFileRead),
                                                         pFileAvail)),
      m_dwFileLen(m_pFileRead->GetSize()) {}

CPDF_DataAvail::~CPDF_DataAvail() {
  m_pHintTables.reset();
  if (m_pDocument)
    m_pDocument->RemoveObserver(this);
}

void CPDF_DataAvail::OnObservableDestroyed() {
  m_pDocument = nullptr;
  m_pFormAvail.reset();
  m_PagesArray.clear();
  m_PagesObjAvail.clear();
  m_PagesResourcesAvail.clear();
}

CPDF_DataAvail::DocAvailStatus CPDF_DataAvail::IsDocAvail(
    DownloadHints* pHints) {
  if (!m_dwFileLen)
    return kDataError;

  DCHECK(m_SeenPageObjList.empty());
  AutoRestorer<std::set<uint32_t>> seen_objects_restorer(&m_SeenPageObjList);
  const HintsScope hints_scope(GetValidator(), pHints);
  while (!m_bDocAvail) {
    if (!CheckDocStatus())
      return kDataNotAvailable;
  }

  return kDataAvailable;
}

bool CPDF_DataAvail::CheckDocStatus() {
  switch (m_internalStatus) {
    case InternalStatus::kHeader:
      return CheckHeader();
    case InternalStatus::kFirstPage:
      return CheckFirstPage();
    case InternalStatus::kHintTable:
      return CheckHintTables();
    case InternalStatus::kLoadAllCrossRef:
      return CheckAndLoadAllXref();
    case InternalStatus::kLoadAllFile:
      return LoadAllFile();
    case InternalStatus::kRoot:
      return CheckRoot();
    case InternalStatus::kInfo:
      return CheckInfo();
    case InternalStatus::kPageTree:
      if (m_bTotalLoadPageTree)
        return CheckPages();
      return LoadDocPages();
    case InternalStatus::kPage:
      if (m_bTotalLoadPageTree)
        return CheckPage();
      m_internalStatus = InternalStatus::kPageLaterLoad;
      return true;
    case InternalStatus::kError:
      return LoadAllFile();
    case InternalStatus::kPageLaterLoad:
      m_internalStatus = InternalStatus::kPage;
      [[fallthrough]];
    default:
      m_bDocAvail = true;
      return true;
  }
}

bool CPDF_DataAvail::CheckPageStatus() {
  switch (m_internalStatus) {
    case InternalStatus::kPageTree:
      return CheckPages();
    case InternalStatus::kPage:
      return CheckPage();
    case InternalStatus::kError:
      return LoadAllFile();
    default:
      m_bPagesTreeLoad = true;
      m_bPagesLoad = true;
      return true;
  }
}

bool CPDF_DataAvail::LoadAllFile() {
  if (GetValidator()->CheckWholeFileAndRequestIfUnavailable()) {
    m_internalStatus = InternalStatus::kDone;
    return true;
  }
  return false;
}

bool CPDF_DataAvail::CheckAndLoadAllXref() {
  if (!m_pCrossRefAvail) {
    CPDF_ReadValidator::ScopedSession read_session(GetValidator());
    const FX_FILESIZE last_xref_offset = m_parser.ParseStartXRef();
    if (GetValidator()->has_read_problems())
      return false;

    if (last_xref_offset <= 0) {
      m_internalStatus = InternalStatus::kError;
      return false;
    }

    m_pCrossRefAvail = std::make_unique<CPDF_CrossRefAvail>(GetSyntaxParser(),
                                                            last_xref_offset);
  }

  switch (m_pCrossRefAvail->CheckAvail()) {
    case kDataAvailable:
      break;
    case kDataNotAvailable:
      return false;
    case kDataError:
      m_internalStatus = InternalStatus::kError;
      return false;
  }

  if (!m_parser.LoadAllCrossRefTablesAndStreams(
          m_pCrossRefAvail->last_crossref_offset())) {
    m_internalStatus = InternalStatus::kLoadAllFile;
    return false;
  }

  m_internalStatus = InternalStatus::kRoot;
  return true;
}

RetainPtr<CPDF_Object> CPDF_DataAvail::GetObject(uint32_t objnum,
                                                 bool* pExistInFile) {
  *pExistInFile = false;
  CPDF_Parser* pParser = m_pDocument ? m_pDocument->GetParser() : &m_parser;
  if (!pParser)
    return nullptr;

  CPDF_ReadValidator::ScopedSession read_session(GetValidator());
  RetainPtr<CPDF_Object> pRet = pParser->ParseIndirectObject(objnum);
  if (!pRet)
    return nullptr;

  *pExistInFile = true;
  if (GetValidator()->has_read_problems())
    return nullptr;

  return pRet;
}

bool CPDF_DataAvail::CheckInfo() {
  const uint32_t dwInfoObjNum = m_parser.GetInfoObjNum();
  if (dwInfoObjNum == CPDF_Object::kInvalidObjNum) {
    m_internalStatus = InternalStatus::kPageTree;
    return true;
  }

  CPDF_ReadValidator::ScopedSession read_session(GetValidator());
  m_parser.ParseIndirectObject(dwInfoObjNum);
  if (GetValidator()->has_read_problems())
    return false;

  m_internalStatus = InternalStatus::kPageTree;
  return true;
}

bool CPDF_DataAvail::CheckRoot() {
  const uint32_t dwRootObjNum = m_parser.GetRootObjNum();
  if (dwRootObjNum == CPDF_Object::kInvalidObjNum) {
    m_internalStatus = InternalStatus::kError;
    return true;
  }

  CPDF_ReadValidator::ScopedSession read_session(GetValidator());
  m_pRoot = ToDictionary(m_parser.ParseIndirectObject(dwRootObjNum));
  if (GetValidator()->has_read_problems())
    return false;

  if (!m_pRoot) {
    m_internalStatus = InternalStatus::kError;
    return false;
  }

  RetainPtr<const CPDF_Reference> pRef =
      ToReference(m_pRoot->GetObjectFor("Pages"));
  if (!pRef) {
    m_internalStatus = InternalStatus::kError;
    return false;
  }

  m_PagesObjNum = pRef->GetRefObjNum();
  m_internalStatus = InternalStatus::kInfo;
  return true;
}

bool CPDF_DataAvail::PreparePageItem() {
  const CPDF_Dictionary* pRoot = m_pDocument->GetRoot();
  if (!pRoot) {
    m_internalStatus = InternalStatus::kError;
    return false;
  }

  RetainPtr<const CPDF_Reference> pRef =
      ToReference(pRoot->GetObjectFor("Pages"));
  if (!pRef) {
    m_internalStatus = InternalStatus::kError;
    return false;
  }

  m_PagesObjNum = pRef->GetRefObjNum();
  m_internalStatus = InternalStatus::kPageTree;
  return true;
}

bool CPDF_DataAvail::IsFirstCheck(uint32_t dwPage) {
  return m_pageMapCheckState.insert(dwPage).second;
}

void CPDF_DataAvail::ResetFirstCheck(uint32_t dwPage) {
  m_pageMapCheckState.erase(dwPage);
}

bool CPDF_DataAvail::CheckPage() {
  std::vector<uint32_t> UnavailObjList;
  for (uint32_t dwPageObjNum : m_PageObjList) {
    bool bExists = false;
    RetainPtr<CPDF_Object> pObj = GetObject(dwPageObjNum, &bExists);
    if (!pObj) {
      if (bExists)
        UnavailObjList.push_back(dwPageObjNum);
      continue;
    }

    switch (pObj->GetType()) {
      case CPDF_Object::kArray: {
        CPDF_ArrayLocker locker(pObj->AsArray());
        for (const auto& pArrayObj : locker) {
          const CPDF_Reference* pRef = ToReference(pArrayObj.Get());
          if (pRef)
            UnavailObjList.push_back(pRef->GetRefObjNum());
        }
        break;
      }
      case CPDF_Object::kDictionary:
        if (pObj->GetDict()->GetNameFor("Type") == "Pages")
          m_PagesArray.push_back(std::move(pObj));
        break;
      default:
        break;
    }
  }
  m_PageObjList.clear();
  if (!UnavailObjList.empty()) {
    m_PageObjList = std::move(UnavailObjList);
    return false;
  }
  size_t iPages = m_PagesArray.size();
  for (size_t i = 0; i < iPages; ++i) {
    RetainPtr<CPDF_Object> pPages = std::move(m_PagesArray[i]);
    if (pPages && !GetPageKids(pPages.Get())) {
      m_PagesArray.clear();
      m_internalStatus = InternalStatus::kError;
      return false;
    }
  }
  m_PagesArray.clear();
  if (m_PageObjList.empty())
    m_internalStatus = InternalStatus::kDone;

  return true;
}

bool CPDF_DataAvail::GetPageKids(CPDF_Object* pPages) {
  RetainPtr<const CPDF_Dictionary> pDict = pPages->GetDict();
  if (!pDict)
    return true;

  RetainPtr<const CPDF_Object> pKids = pDict->GetObjectFor("Kids");
  if (!pKids)
    return true;

  std::vector<uint32_t> object_numbers;
  switch (pKids->GetType()) {
    case CPDF_Object::kReference:
      object_numbers.push_back(pKids->AsReference()->GetRefObjNum());
      break;
    case CPDF_Object::kArray: {
      CPDF_ArrayLocker locker(pKids->AsArray());
      for (const auto& pArrayObj : locker) {
        const CPDF_Reference* pRef = ToReference(pArrayObj.Get());
        if (pRef)
          object_numbers.push_back(pRef->GetRefObjNum());
      }
      break;
    }
    default:
      m_internalStatus = InternalStatus::kError;
      return false;
  }

  for (uint32_t num : object_numbers) {
    bool inserted = m_SeenPageObjList.insert(num).second;
    if (inserted)
      m_PageObjList.push_back(num);
  }
  return true;
}

bool CPDF_DataAvail::CheckPages() {
  bool bExists = false;
  RetainPtr<CPDF_Object> pPages = GetObject(m_PagesObjNum, &bExists);
  if (!bExists) {
    m_internalStatus = InternalStatus::kLoadAllFile;
    return true;
  }

  if (!pPages) {
    if (m_internalStatus == InternalStatus::kError) {
      m_internalStatus = InternalStatus::kLoadAllFile;
      return true;
    }
    return false;
  }

  if (!GetPageKids(pPages.Get())) {
    m_internalStatus = InternalStatus::kError;
    return false;
  }

  m_internalStatus = InternalStatus::kPage;
  return true;
}

bool CPDF_DataAvail::CheckHeader() {
  switch (CheckHeaderAndLinearized()) {
    case kDataAvailable:
      m_internalStatus = m_pLinearized ? InternalStatus::kFirstPage
                                       : InternalStatus::kLoadAllCrossRef;
      return true;
    case kDataNotAvailable:
      return false;
    case kDataError:
      m_internalStatus = InternalStatus::kError;
      return true;
  }
}

bool CPDF_DataAvail::CheckFirstPage() {
  if (!m_pLinearized->GetFirstPageEndOffset() ||
      !m_pLinearized->GetFileSize() ||
      !m_pLinearized->GetMainXRefTableFirstEntryOffset()) {
    m_internalStatus = InternalStatus::kError;
    return false;
  }

  uint32_t dwEnd = m_pLinearized->GetFirstPageEndOffset();
  dwEnd += 512;
  if ((FX_FILESIZE)dwEnd > m_dwFileLen)
    dwEnd = (uint32_t)m_dwFileLen;

  const FX_FILESIZE start_pos = m_dwFileLen > 1024 ? 1024 : m_dwFileLen;
  const size_t data_size = dwEnd > 1024 ? static_cast<size_t>(dwEnd - 1024) : 0;
  if (!GetValidator()->CheckDataRangeAndRequestIfUnavailable(start_pos,
                                                             data_size))
    return false;

  m_internalStatus = InternalStatus::kHintTable;
  return true;
}

bool CPDF_DataAvail::CheckHintTables() {
  CPDF_ReadValidator::ScopedSession read_session(GetValidator());
  m_pHintTables =
      CPDF_HintTables::Parse(GetSyntaxParser(), m_pLinearized.get());

  if (GetValidator()->read_error()) {
    m_internalStatus = InternalStatus::kError;
    return true;
  }
  if (GetValidator()->has_unavailable_data())
    return false;

  m_internalStatus = InternalStatus::kDone;
  return true;
}

RetainPtr<CPDF_Object> CPDF_DataAvail::ParseIndirectObjectAt(
    FX_FILESIZE pos,
    uint32_t objnum,
    CPDF_IndirectObjectHolder* pObjList) const {
  const FX_FILESIZE SavedPos = GetSyntaxParser()->GetPos();
  GetSyntaxParser()->SetPos(pos);
  RetainPtr<CPDF_Object> result = GetSyntaxParser()->GetIndirectObject(
      pObjList, CPDF_SyntaxParser::ParseType::kLoose);
  GetSyntaxParser()->SetPos(SavedPos);
  return (result && (!objnum || result->GetObjNum() == objnum))
             ? std::move(result)
             : nullptr;
}

CPDF_DataAvail::DocLinearizationStatus CPDF_DataAvail::IsLinearizedPDF() {
  switch (CheckHeaderAndLinearized()) {
    case kDataAvailable:
      return m_pLinearized ? kLinearized : kNotLinearized;
    case kDataNotAvailable:
      return kLinearizationUnknown;
    case kDataError:
      return kNotLinearized;
  }
}

CPDF_DataAvail::DocAvailStatus CPDF_DataAvail::CheckHeaderAndLinearized() {
  if (m_bHeaderAvail)
    return kDataAvailable;

  CPDF_ReadValidator::ScopedSession read_session(GetValidator());
  const std::optional<FX_FILESIZE> header_offset =
      GetHeaderOffset(GetValidator());
  if (GetValidator()->has_read_problems())
    return kDataNotAvailable;

  if (!header_offset.has_value())
    return kDataError;

  m_parser.m_pSyntax = std::make_unique<CPDF_SyntaxParser>(
      GetValidator(), header_offset.value());
  m_pLinearized = m_parser.ParseLinearizedHeader();
  if (GetValidator()->has_read_problems())
    return kDataNotAvailable;

  m_bHeaderAvail = true;
  return kDataAvailable;
}

bool CPDF_DataAvail::CheckPage(uint32_t dwPage) {
  while (true) {
    switch (m_internalStatus) {
      case InternalStatus::kPageTree:
        if (!LoadDocPages())
          return false;
        break;
      case InternalStatus::kPage:
        if (!LoadDocPage(dwPage))
          return false;
        break;
      case InternalStatus::kError:
        return LoadAllFile();
      default:
        m_bPagesTreeLoad = true;
        m_bPagesLoad = true;
        m_bCurPageDictLoadOK = true;
        m_internalStatus = InternalStatus::kPage;
        return true;
    }
  }
}

bool CPDF_DataAvail::CheckArrayPageNode(uint32_t dwPageNo,
                                        PageNode* pPageNode) {
  bool bExists = false;
  RetainPtr<CPDF_Object> pPages = GetObject(dwPageNo, &bExists);
  if (!bExists) {
    m_internalStatus = InternalStatus::kError;
    return false;
  }

  if (!pPages)
    return false;

  const CPDF_Array* pArray = pPages->AsArray();
  if (!pArray) {
    m_internalStatus = InternalStatus::kError;
    return false;
  }

  pPageNode->m_type = PageNode::Type::kPages;
  for (size_t i = 0; i < pArray->size(); ++i) {
    RetainPtr<const CPDF_Reference> pKid = ToReference(pArray->GetObjectAt(i));
    if (!pKid)
      continue;

    auto pNode = std::make_unique<PageNode>();
    pNode->m_dwPageNo = pKid->GetRefObjNum();
    pPageNode->m_ChildNodes.push_back(std::move(pNode));
  }
  return true;
}

bool CPDF_DataAvail::CheckUnknownPageNode(uint32_t dwPageNo,
                                          PageNode* pPageNode) {
  bool bExists = false;
  RetainPtr<CPDF_Object> pPage = GetObject(dwPageNo, &bExists);
  if (!bExists) {
    m_internalStatus = InternalStatus::kError;
    return false;
  }

  if (!pPage)
    return false;

  if (pPage->IsArray()) {
    pPageNode->m_dwPageNo = dwPageNo;
    pPageNode->m_type = PageNode::Type::kArray;
    return true;
  }

  if (!pPage->IsDictionary()) {
    m_internalStatus = InternalStatus::kError;
    return false;
  }

  pPageNode->m_dwPageNo = dwPageNo;
  RetainPtr<CPDF_Dictionary> pDict = pPage->GetMutableDict();
  const ByteString type = pDict->GetNameFor("Type");
  if (type == "Page") {
    pPageNode->m_type = PageNode::Type::kPage;
    return true;
  }

  if (type != "Pages") {
    m_internalStatus = InternalStatus::kError;
    return false;
  }

  pPageNode->m_type = PageNode::Type::kPages;
  RetainPtr<CPDF_Object> pKids = pDict->GetMutableObjectFor("Kids");
  if (!pKids) {
    m_internalStatus = InternalStatus::kPage;
    return true;
  }

  switch (pKids->GetType()) {
    case CPDF_Object::kReference: {
      const CPDF_Reference* pKid = pKids->AsReference();
      auto pNode = std::make_unique<PageNode>();
      pNode->m_dwPageNo = pKid->GetRefObjNum();
      pPageNode->m_ChildNodes.push_back(std::move(pNode));
      break;
    }
    case CPDF_Object::kArray: {
      const CPDF_Array* pKidsArray = pKids->AsArray();
      for (size_t i = 0; i < pKidsArray->size(); ++i) {
        RetainPtr<const CPDF_Reference> pKid =
            ToReference(pKidsArray->GetObjectAt(i));
        if (!pKid)
          continue;

        auto pNode = std::make_unique<PageNode>();
        pNode->m_dwPageNo = pKid->GetRefObjNum();
        pPageNode->m_ChildNodes.push_back(std::move(pNode));
      }
      break;
    }
    default:
      break;
  }
  return true;
}

bool CPDF_DataAvail::CheckPageNode(const CPDF_DataAvail::PageNode& pageNode,
                                   int32_t iPage,
                                   int32_t& iCount,
                                   int level) {
  if (level >= kMaxPageRecursionDepth)
    return false;

  int32_t iSize = fxcrt::CollectionSize<int32_t>(pageNode.m_ChildNodes);
  if (iSize <= 0 || iPage >= iSize) {
    m_internalStatus = InternalStatus::kError;
    return false;
  }
  for (int32_t i = 0; i < iSize; ++i) {
    PageNode* pNode = pageNode.m_ChildNodes[i].get();
    if (!pNode)
      continue;

    if (pNode->m_type == PageNode::Type::kUnknown) {
      // Updates the type for the unknown page node.
      if (!CheckUnknownPageNode(pNode->m_dwPageNo, pNode))
        return false;
    }
    if (pNode->m_type == PageNode::Type::kArray) {
      // Updates a more specific type for the array page node.
      if (!CheckArrayPageNode(pNode->m_dwPageNo, pNode))
        return false;
    }
    switch (pNode->m_type) {
      case PageNode::Type::kPage:
        iCount++;
        if (iPage == iCount && m_pDocument)
          m_pDocument->SetPageObjNum(iPage, pNode->m_dwPageNo);
        break;
      case PageNode::Type::kPages:
        if (!CheckPageNode(*pNode, iPage, iCount, level + 1))
          return false;
        break;
      case PageNode::Type::kUnknown:
      case PageNode::Type::kArray:
        // Already converted above, error if we get here.
        return false;
    }
    if (iPage == iCount) {
      m_internalStatus = InternalStatus::kDone;
      return true;
    }
  }
  return true;
}

bool CPDF_DataAvail::LoadDocPage(uint32_t dwPage) {
  int iPage = pdfium::checked_cast<int>(dwPage);
  if (m_pDocument->GetPageCount() <= iPage ||
      m_pDocument->IsPageLoaded(iPage)) {
    m_internalStatus = InternalStatus::kDone;
    return true;
  }
  if (m_PageNode.m_type == PageNode::Type::kPage) {
    m_internalStatus =
        iPage == 0 ? InternalStatus::kDone : InternalStatus::kError;
    return true;
  }
  int32_t iCount = -1;
  return CheckPageNode(m_PageNode, iPage, iCount, 0);
}

bool CPDF_DataAvail::CheckPageCount() {
  bool bExists = false;
  RetainPtr<CPDF_Object> pPages = GetObject(m_PagesObjNum, &bExists);
  if (!bExists) {
    m_internalStatus = InternalStatus::kError;
    return false;
  }
  if (!pPages)
    return false;

  RetainPtr<const CPDF_Dictionary> pPagesDict = pPages->GetDict();
  if (!pPagesDict) {
    m_internalStatus = InternalStatus::kError;
    return false;
  }
  if (!pPagesDict->KeyExist("Kids"))
    return true;

  return pPagesDict->GetIntegerFor("Count") > 0;
}

bool CPDF_DataAvail::LoadDocPages() {
  if (!CheckUnknownPageNode(m_PagesObjNum, &m_PageNode))
    return false;

  if (CheckPageCount()) {
    m_internalStatus = InternalStatus::kPage;
    return true;
  }

  m_bTotalLoadPageTree = true;
  return false;
}

bool CPDF_DataAvail::LoadPages() {
  while (!m_bPagesTreeLoad) {
    if (!CheckPageStatus())
      return false;
  }

  if (m_bPagesLoad)
    return true;

  m_pDocument->LoadPages();
  return false;
}

CPDF_DataAvail::DocAvailStatus CPDF_DataAvail::CheckLinearizedData() {
  if (m_bLinearedDataOK)
    return kDataAvailable;
  DCHECK(m_pLinearized);
  if (!m_pLinearized->GetMainXRefTableFirstEntryOffset() || !m_pDocument ||
      !m_pDocument->GetParser() || !m_pDocument->GetParser()->GetTrailer()) {
    return kDataError;
  }

  if (!m_bMainXRefLoadTried) {
    const FX_SAFE_FILESIZE prev =
        m_pDocument->GetParser()->GetTrailer()->GetIntegerFor("Prev");
    const FX_FILESIZE main_xref_offset = prev.ValueOrDefault(-1);
    if (main_xref_offset < 0)
      return kDataError;

    if (main_xref_offset == 0)
      return kDataAvailable;

    FX_SAFE_SIZE_T data_size = m_dwFileLen;
    data_size -= main_xref_offset;
    if (!data_size.IsValid())
      return kDataError;

    if (!GetValidator()->CheckDataRangeAndRequestIfUnavailable(
            main_xref_offset, data_size.ValueOrDie()))
      return kDataNotAvailable;

    CPDF_Parser::Error eRet =
        m_pDocument->GetParser()->LoadLinearizedMainXRefTable();
    m_bMainXRefLoadTried = true;
    if (eRet != CPDF_Parser::SUCCESS)
      return kDataError;

    if (!PreparePageItem())
      return kDataNotAvailable;

    m_bMainXRefLoadedOK = true;
    m_bLinearedDataOK = true;
  }

  return m_bLinearedDataOK ? kDataAvailable : kDataNotAvailable;
}

CPDF_DataAvail::DocAvailStatus CPDF_DataAvail::IsPageAvail(
    uint32_t dwPage,
    DownloadHints* pHints) {
  if (!m_pDocument)
    return kDataError;

  const int iPage = pdfium::checked_cast<int>(dwPage);
  if (iPage >= m_pDocument->GetPageCount()) {
    // This is XFA page.
    return kDataAvailable;
  }

  if (IsFirstCheck(dwPage)) {
    m_bCurPageDictLoadOK = false;
  }

  if (pdfium::Contains(m_pagesLoadState, dwPage))
    return kDataAvailable;

  const HintsScope hints_scope(GetValidator(), pHints);
  if (m_pLinearized) {
    if (dwPage == m_pLinearized->GetFirstPageNo()) {
      RetainPtr<const CPDF_Dictionary> pPageDict =
          m_pDocument->GetPageDictionary(iPage);
      if (!pPageDict)
        return kDataError;

      auto page_num_obj =
          std::make_pair(dwPage, std::make_unique<CPDF_PageObjectAvail>(
                                     GetValidator(), m_pDocument, pPageDict));

      CPDF_PageObjectAvail* page_obj_avail =
          m_PagesObjAvail.insert(std::move(page_num_obj)).first->second.get();
      // TODO(art-snake): Check resources.
      return page_obj_avail->CheckAvail();
    }

    DocAvailStatus nResult = CheckLinearizedData();
    if (nResult != kDataAvailable)
      return nResult;

    if (m_pHintTables) {
      nResult = m_pHintTables->CheckPage(dwPage);
      if (nResult != kDataAvailable)
        return nResult;
      if (GetPageDictionary(dwPage)) {
        m_pagesLoadState.insert(dwPage);
        return kDataAvailable;
      }
    }

    if (!m_bMainXRefLoadedOK) {
      if (!LoadAllFile())
        return kDataNotAvailable;
      m_pDocument->GetParser()->RebuildCrossRef();
      ResetFirstCheck(dwPage);
      return kDataAvailable;
    }
    if (m_bTotalLoadPageTree) {
      if (!LoadPages())
        return kDataNotAvailable;
    } else {
      if (!m_bCurPageDictLoadOK && !CheckPage(dwPage))
        return kDataNotAvailable;
    }
  } else {
    if (!m_bTotalLoadPageTree && !m_bCurPageDictLoadOK && !CheckPage(dwPage)) {
      return kDataNotAvailable;
    }
  }

  if (CheckAcroForm() == kFormNotAvailable)
    return kDataNotAvailable;

  RetainPtr<CPDF_Dictionary> pPageDict =
      m_pDocument->GetMutablePageDictionary(iPage);
  if (!pPageDict)
    return kDataError;

  {
    auto page_num_obj =
        std::make_pair(dwPage, std::make_unique<CPDF_PageObjectAvail>(
                                   GetValidator(), m_pDocument, pPageDict));
    CPDF_PageObjectAvail* page_obj_avail =
        m_PagesObjAvail.insert(std::move(page_num_obj)).first->second.get();
    const DocAvailStatus status = page_obj_avail->CheckAvail();
    if (status != kDataAvailable)
      return status;
  }

  const DocAvailStatus resources_status = CheckResources(std::move(pPageDict));
  if (resources_status != kDataAvailable)
    return resources_status;

  m_bCurPageDictLoadOK = false;
  ResetFirstCheck(dwPage);
  m_pagesLoadState.insert(dwPage);
  return kDataAvailable;
}

CPDF_DataAvail::DocAvailStatus CPDF_DataAvail::CheckResources(
    RetainPtr<CPDF_Dictionary> page) {
  DCHECK(page);
  CPDF_ReadValidator::ScopedSession read_session(GetValidator());
  RetainPtr<CPDF_Object> resources = GetResourceObject(std::move(page));
  if (GetValidator()->has_read_problems())
    return kDataNotAvailable;

  if (!resources)
    return kDataAvailable;

  CPDF_PageObjectAvail* resource_avail =
      m_PagesResourcesAvail
          .insert(std::make_pair(resources,
                                 std::make_unique<CPDF_PageObjectAvail>(
                                     GetValidator(), m_pDocument, resources)))
          .first->second.get();
  return resource_avail->CheckAvail();
}

RetainPtr<CPDF_ReadValidator> CPDF_DataAvail::GetValidator() const {
  return m_pFileRead;
}

CPDF_SyntaxParser* CPDF_DataAvail::GetSyntaxParser() const {
  return m_pDocument ? m_pDocument->GetParser()->m_pSyntax.get()
                     : m_parser.m_pSyntax.get();
}

int CPDF_DataAvail::GetPageCount() const {
  if (m_pLinearized)
    return m_pLinearized->GetPageCount();
  return m_pDocument ? m_pDocument->GetPageCount() : 0;
}

RetainPtr<const CPDF_Dictionary> CPDF_DataAvail::GetPageDictionary(
    int index) const {
  if (!m_pDocument || index < 0 || index >= GetPageCount())
    return nullptr;
  RetainPtr<const CPDF_Dictionary> page = m_pDocument->GetPageDictionary(index);
  if (page)
    return page;
  if (!m_pLinearized || !m_pHintTables)
    return nullptr;

  if (index == static_cast<int>(m_pLinearized->GetFirstPageNo()))
    return nullptr;
  FX_FILESIZE szPageStartPos = 0;
  FX_FILESIZE szPageLength = 0;
  uint32_t dwObjNum = 0;
  const bool bPagePosGot = m_pHintTables->GetPagePos(index, &szPageStartPos,
                                                     &szPageLength, &dwObjNum);
  if (!bPagePosGot || !dwObjNum)
    return nullptr;
  // We should say to the document, which object is the page.
  m_pDocument->SetPageObjNum(index, dwObjNum);
  // Page object already can be parsed in document.
  if (!m_pDocument->GetIndirectObject(dwObjNum)) {
    m_pDocument->ReplaceIndirectObjectIfHigherGeneration(
        dwObjNum, ParseIndirectObjectAt(szPageStartPos, dwObjNum, m_pDocument));
  }
  if (!ValidatePage(index))
    return nullptr;
  return m_pDocument->GetPageDictionary(index);
}

CPDF_DataAvail::DocFormStatus CPDF_DataAvail::IsFormAvail(
    DownloadHints* pHints) {
  const HintsScope hints_scope(GetValidator(), pHints);
  return CheckAcroForm();
}

CPDF_DataAvail::DocFormStatus CPDF_DataAvail::CheckAcroForm() {
  if (!m_pDocument)
    return kFormAvailable;

  if (m_pLinearized) {
    DocAvailStatus nDocStatus = CheckLinearizedData();
    if (nDocStatus == kDataError)
      return kFormError;
    if (nDocStatus == kDataNotAvailable)
      return kFormNotAvailable;
  }

  if (!m_pFormAvail) {
    const CPDF_Dictionary* pRoot = m_pDocument->GetRoot();
    if (!pRoot)
      return kFormAvailable;

    RetainPtr<const CPDF_Object> pAcroForm = pRoot->GetObjectFor("AcroForm");
    if (!pAcroForm)
      return kFormNotExist;

    m_pFormAvail = std::make_unique<CPDF_PageObjectAvail>(
        GetValidator(), m_pDocument, std::move(pAcroForm));
  }
  switch (m_pFormAvail->CheckAvail()) {
    case kDataError:
      return kFormError;
    case kDataNotAvailable:
      return kFormNotAvailable;
    case kDataAvailable:
      return kFormAvailable;
  }
}

bool CPDF_DataAvail::ValidatePage(uint32_t dwPage) const {
  int iPage = pdfium::checked_cast<int>(dwPage);
  RetainPtr<const CPDF_Dictionary> pPageDict =
      m_pDocument->GetPageDictionary(iPage);
  if (!pPageDict)
    return false;

  CPDF_PageObjectAvail obj_avail(GetValidator(), m_pDocument,
                                 std::move(pPageDict));
  return obj_avail.CheckAvail() == kDataAvailable;
}

std::pair<CPDF_Parser::Error, std::unique_ptr<CPDF_Document>>
CPDF_DataAvail::ParseDocument(
    std::unique_ptr<CPDF_Document::RenderDataIface> pRenderData,
    std::unique_ptr<CPDF_Document::PageDataIface> pPageData,
    const ByteString& password) {
  if (m_pDocument) {
    // We already returned parsed document.
    return std::make_pair(CPDF_Parser::HANDLER_ERROR, nullptr);
  }
  auto document = std::make_unique<CPDF_Document>(std::move(pRenderData),
                                                  std::move(pPageData));
  document->AddObserver(this);

  CPDF_ReadValidator::ScopedSession read_session(GetValidator());
  CPDF_Parser::Error error =
      document->LoadLinearizedDoc(GetValidator(), password);

  // Additional check, that all ok.
  if (GetValidator()->has_read_problems()) {
    NOTREACHED();
    return std::make_pair(CPDF_Parser::HANDLER_ERROR, nullptr);
  }

  if (error != CPDF_Parser::SUCCESS)
    return std::make_pair(error, nullptr);

  m_pDocument = document.get();
  return std::make_pair(CPDF_Parser::SUCCESS, std::move(document));
}

CPDF_DataAvail::PageNode::PageNode() = default;

CPDF_DataAvail::PageNode::~PageNode() = default;
