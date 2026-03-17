// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/parser/cpdf_document.h"

#include <algorithm>
#include <functional>
#include <optional>
#include <utility>

#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_linearized_header.h"
#include "core/fpdfapi/parser/cpdf_name.h"
#include "core/fpdfapi/parser/cpdf_null.h"
#include "core/fpdfapi/parser/cpdf_number.h"
#include "core/fpdfapi/parser/cpdf_parser.h"
#include "core/fpdfapi/parser/cpdf_read_validator.h"
#include "core/fpdfapi/parser/cpdf_reference.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fpdfapi/parser/cpdf_stream_acc.h"
#include "core/fpdfapi/parser/fpdf_parser_utility.h"
#include "core/fxcodec/jbig2/JBig2_DocumentContext.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/check_op.h"
#include "core/fxcrt/containers/contains.h"
#include "core/fxcrt/fx_codepage.h"
#include "core/fxcrt/scoped_set_insertion.h"
#include "core/fxcrt/span.h"
#include "core/fxcrt/stl_util.h"

namespace {

const int kMaxPageLevel = 1024;

enum class NodeType : bool {
  kBranch,  // /Type /Pages, AKA page tree node.
  kLeaf,    // /Type /Page, AKA page object.
};

// Note that this function may modify `kid_dict` to correct PDF spec violations.
// Same reasoning as CountPages() below.
NodeType GetNodeType(RetainPtr<CPDF_Dictionary> kid_dict) {
  const ByteString kid_type_value = kid_dict->GetNameFor("Type");
  if (kid_type_value == "Pages") {
    return NodeType::kBranch;
  }
  if (kid_type_value == "Page") {
    return NodeType::kLeaf;
  }

  // Even though /Type is required for page tree nodes and page objects, PDFs
  // may not have them or have the wrong type. Tolerate these errors and guess
  // the type. Then fix the in-memory representation.
  const bool has_kids = kid_dict->KeyExist("Kids");
  kid_dict->SetNewFor<CPDF_Name>("Type", has_kids ? "Pages" : "Page");
  return has_kids ? NodeType::kBranch : NodeType::kLeaf;
}

// Returns a value in the range [0, `CPDF_Document::kPageMaxNum`), or nullopt on
// error. Note that this function may modify `pages_dict` to correct PDF spec
// violations. By normalizing the in-memory representation, other code that
// reads the object do not have to deal with the same spec violations again.
// If the PDF gets saved, the saved copy will also be more spec-compliant.
std::optional<int> CountPages(
    RetainPtr<CPDF_Dictionary> pages_dict,
    std::set<RetainPtr<CPDF_Dictionary>>* visited_pages) {
  // Required. See ISO 32000-1:2008 spec, table 29, but tolerate page tree nodes
  // that violate the spec.
  int count_from_dict = pages_dict->GetIntegerFor("Count");
  if (count_from_dict > 0 && count_from_dict < CPDF_Document::kPageMaxNum) {
    return count_from_dict;
  }

  RetainPtr<CPDF_Array> kids_array = pages_dict->GetMutableArrayFor("Kids");
  if (!kids_array) {
    return 0;
  }

  int count = 0;
  for (size_t i = 0; i < kids_array->size(); i++) {
    RetainPtr<CPDF_Dictionary> kid_dict = kids_array->GetMutableDictAt(i);
    if (!kid_dict || pdfium::Contains(*visited_pages, kid_dict)) {
      continue;
    }

    NodeType kid_type = GetNodeType(kid_dict);
    if (kid_type == NodeType::kBranch) {
      // Use |visited_pages| to help detect circular references of pages.
      ScopedSetInsertion<RetainPtr<CPDF_Dictionary>> local_add(visited_pages,
                                                               kid_dict);
      std::optional<int> local_count =
          CountPages(std::move(kid_dict), visited_pages);
      if (!local_count.has_value()) {
        return std::nullopt;  // Propagate error.
      }
      count += local_count.value();
    } else {
      CHECK_EQ(kid_type, NodeType::kLeaf);
      count++;
    }

    if (count >= CPDF_Document::kPageMaxNum) {
      return std::nullopt;  // Error: too many pages.
    }
  }
  // Fix the in-memory representation for page tree nodes that violate the spec.
  pages_dict->SetNewFor<CPDF_Number>("Count", count);
  return count;
}

int FindPageIndex(const CPDF_Dictionary* pNode,
                  uint32_t* skip_count,
                  uint32_t objnum,
                  int* index,
                  int level) {
  if (!pNode->KeyExist("Kids")) {
    if (objnum == pNode->GetObjNum())
      return *index;

    if (*skip_count != 0)
      (*skip_count)--;

    (*index)++;
    return -1;
  }

  RetainPtr<const CPDF_Array> pKidList = pNode->GetArrayFor("Kids");
  if (!pKidList)
    return -1;

  if (level >= kMaxPageLevel)
    return -1;

  size_t count = pNode->GetIntegerFor("Count");
  if (count <= *skip_count) {
    (*skip_count) -= count;
    (*index) += count;
    return -1;
  }

  if (count && count == pKidList->size()) {
    for (size_t i = 0; i < count; i++) {
      RetainPtr<const CPDF_Reference> pKid =
          ToReference(pKidList->GetObjectAt(i));
      if (pKid && pKid->GetRefObjNum() == objnum)
        return static_cast<int>(*index + i);
    }
  }

  for (size_t i = 0; i < pKidList->size(); i++) {
    RetainPtr<const CPDF_Dictionary> pKid = pKidList->GetDictAt(i);
    if (!pKid || pKid == pNode)
      continue;

    int found_index =
        FindPageIndex(pKid.Get(), skip_count, objnum, index, level + 1);
    if (found_index >= 0)
      return found_index;
  }
  return -1;
}

}  // namespace

CPDF_Document::CPDF_Document(std::unique_ptr<RenderDataIface> pRenderData,
                             std::unique_ptr<PageDataIface> pPageData)
    : m_pDocRender(std::move(pRenderData)),
      m_pDocPage(std::move(pPageData)),
      m_StockFontClearer(m_pDocPage.get()) {
  m_pDocRender->SetDocument(this);
  m_pDocPage->SetDocument(this);
}

CPDF_Document::~CPDF_Document() {
  // Be absolutely certain that |m_pExtension| is null before destroying
  // the extension, to avoid re-entering it while being destroyed. clang
  // seems to already do this for us, but the C++ standards seem to
  // indicate the opposite.
  m_pExtension.reset();
}

// static
bool CPDF_Document::IsValidPageObject(const CPDF_Object* obj) {
  // See ISO 32000-1:2008 spec, table 30.
  return ValidateDictType(ToDictionary(obj), "Page");
}

RetainPtr<CPDF_Object> CPDF_Document::ParseIndirectObject(uint32_t objnum) {
  return m_pParser ? m_pParser->ParseIndirectObject(objnum) : nullptr;
}

bool CPDF_Document::TryInit() {
  SetLastObjNum(m_pParser->GetLastObjNum());

  RetainPtr<CPDF_Object> pRootObj =
      GetOrParseIndirectObject(m_pParser->GetRootObjNum());
  if (pRootObj)
    m_pRootDict = pRootObj->GetMutableDict();

  LoadPages();
  return GetRoot() && GetPageCount() > 0;
}

CPDF_Parser::Error CPDF_Document::LoadDoc(
    RetainPtr<IFX_SeekableReadStream> pFileAccess,
    const ByteString& password) {
  if (!m_pParser)
    SetParser(std::make_unique<CPDF_Parser>(this));

  return HandleLoadResult(
      m_pParser->StartParse(std::move(pFileAccess), password));
}

CPDF_Parser::Error CPDF_Document::LoadLinearizedDoc(
    RetainPtr<CPDF_ReadValidator> validator,
    const ByteString& password) {
  if (!m_pParser)
    SetParser(std::make_unique<CPDF_Parser>(this));

  return HandleLoadResult(
      m_pParser->StartLinearizedParse(std::move(validator), password));
}

void CPDF_Document::LoadPages() {
  const CPDF_LinearizedHeader* linearized_header =
      m_pParser->GetLinearizedHeader();
  if (!linearized_header) {
    m_PageList.resize(RetrievePageCount());
    return;
  }

  uint32_t objnum = linearized_header->GetFirstPageObjNum();
  if (!IsValidPageObject(GetOrParseIndirectObject(objnum).Get())) {
    m_PageList.resize(RetrievePageCount());
    return;
  }

  uint32_t first_page_num = linearized_header->GetFirstPageNo();
  uint32_t page_count = linearized_header->GetPageCount();
  DCHECK(first_page_num < page_count);
  m_PageList.resize(page_count);
  m_PageList[first_page_num] = objnum;
}

RetainPtr<CPDF_Dictionary> CPDF_Document::TraversePDFPages(int iPage,
                                                           int* nPagesToGo,
                                                           size_t level) {
  if (*nPagesToGo < 0 || m_bReachedMaxPageLevel)
    return nullptr;

  RetainPtr<CPDF_Dictionary> pPages = m_pTreeTraversal[level].first;
  RetainPtr<CPDF_Array> pKidList = pPages->GetMutableArrayFor("Kids");
  if (!pKidList) {
    m_pTreeTraversal.pop_back();
    if (*nPagesToGo != 1)
      return nullptr;
    m_PageList[iPage] = pPages->GetObjNum();
    return pPages;
  }
  if (level >= kMaxPageLevel) {
    m_pTreeTraversal.pop_back();
    m_bReachedMaxPageLevel = true;
    return nullptr;
  }
  RetainPtr<CPDF_Dictionary> page;
  for (size_t i = m_pTreeTraversal[level].second; i < pKidList->size(); i++) {
    if (*nPagesToGo == 0)
      break;
    pKidList->ConvertToIndirectObjectAt(i, this);
    RetainPtr<CPDF_Dictionary> pKid = pKidList->GetMutableDictAt(i);
    if (!pKid) {
      (*nPagesToGo)--;
      m_pTreeTraversal[level].second++;
      continue;
    }
    if (pKid == pPages) {
      m_pTreeTraversal[level].second++;
      continue;
    }
    if (!pKid->KeyExist("Kids")) {
      m_PageList[iPage - (*nPagesToGo) + 1] = pKid->GetObjNum();
      (*nPagesToGo)--;
      m_pTreeTraversal[level].second++;
      if (*nPagesToGo == 0) {
        page = std::move(pKid);
        break;
      }
    } else {
      // If the vector has size level+1, the child is not in yet
      if (m_pTreeTraversal.size() == level + 1)
        m_pTreeTraversal.emplace_back(std::move(pKid), 0);
      // Now m_pTreeTraversal[level+1] should exist and be equal to pKid.
      RetainPtr<CPDF_Dictionary> pPageKid =
          TraversePDFPages(iPage, nPagesToGo, level + 1);
      // Check if child was completely processed, i.e. it popped itself out
      if (m_pTreeTraversal.size() == level + 1)
        m_pTreeTraversal[level].second++;
      // If child did not finish, no pages to go, or max level reached, end
      if (m_pTreeTraversal.size() != level + 1 || *nPagesToGo == 0 ||
          m_bReachedMaxPageLevel) {
        page = std::move(pPageKid);
        break;
      }
    }
  }
  if (m_pTreeTraversal[level].second == pKidList->size())
    m_pTreeTraversal.pop_back();
  return page;
}

void CPDF_Document::ResetTraversal() {
  m_iNextPageToTraverse = 0;
  m_bReachedMaxPageLevel = false;
  m_pTreeTraversal.clear();
}

void CPDF_Document::SetParser(std::unique_ptr<CPDF_Parser> pParser) {
  DCHECK(!m_pParser);
  m_pParser = std::move(pParser);
}

CPDF_Parser::Error CPDF_Document::HandleLoadResult(CPDF_Parser::Error error) {
  if (error == CPDF_Parser::SUCCESS)
    m_bHasValidCrossReferenceTable = !m_pParser->xref_table_rebuilt();
  return error;
}

RetainPtr<const CPDF_Dictionary> CPDF_Document::GetPagesDict() const {
  const CPDF_Dictionary* pRoot = GetRoot();
  return pRoot ? pRoot->GetDictFor("Pages") : nullptr;
}

RetainPtr<CPDF_Dictionary> CPDF_Document::GetMutablePagesDict() {
  return pdfium::WrapRetain(
      const_cast<CPDF_Dictionary*>(this->GetPagesDict().Get()));
}

bool CPDF_Document::IsPageLoaded(int iPage) const {
  return !!m_PageList[iPage];
}

RetainPtr<const CPDF_Dictionary> CPDF_Document::GetPageDictionary(int iPage) {
  if (!fxcrt::IndexInBounds(m_PageList, iPage))
    return nullptr;

  const uint32_t objnum = m_PageList[iPage];
  if (objnum) {
    RetainPtr<CPDF_Dictionary> result =
        ToDictionary(GetOrParseIndirectObject(objnum));
    if (result)
      return result;
  }

  RetainPtr<CPDF_Dictionary> pPages = GetMutablePagesDict();
  if (!pPages)
    return nullptr;

  if (m_pTreeTraversal.empty()) {
    ResetTraversal();
    m_pTreeTraversal.emplace_back(std::move(pPages), 0);
  }
  int nPagesToGo = iPage - m_iNextPageToTraverse + 1;
  RetainPtr<CPDF_Dictionary> pPage = TraversePDFPages(iPage, &nPagesToGo, 0);
  m_iNextPageToTraverse = iPage + 1;
  return pPage;
}

RetainPtr<CPDF_Dictionary> CPDF_Document::GetMutablePageDictionary(int iPage) {
  return pdfium::WrapRetain(
      const_cast<CPDF_Dictionary*>(GetPageDictionary(iPage).Get()));
}

void CPDF_Document::SetPageObjNum(int iPage, uint32_t objNum) {
  m_PageList[iPage] = objNum;
}

JBig2_DocumentContext* CPDF_Document::GetOrCreateCodecContext() {
  if (!m_pCodecContext)
    m_pCodecContext = std::make_unique<JBig2_DocumentContext>();
  return m_pCodecContext.get();
}

RetainPtr<CPDF_Stream> CPDF_Document::CreateModifiedAPStream(
    RetainPtr<CPDF_Dictionary> dict) {
  auto stream = NewIndirect<CPDF_Stream>(std::move(dict));
  m_ModifiedAPStreamIDs.insert(stream->GetObjNum());
  return stream;
}

bool CPDF_Document::IsModifiedAPStream(const CPDF_Stream* stream) const {
  return stream && pdfium::Contains(m_ModifiedAPStreamIDs, stream->GetObjNum());
}

int CPDF_Document::GetPageIndex(uint32_t objnum) {
  uint32_t skip_count = 0;
  bool bSkipped = false;
  for (uint32_t i = 0; i < m_PageList.size(); ++i) {
    if (m_PageList[i] == objnum)
      return i;

    if (!bSkipped && m_PageList[i] == 0) {
      skip_count = i;
      bSkipped = true;
    }
  }
  RetainPtr<const CPDF_Dictionary> pPages = GetPagesDict();
  if (!pPages)
    return -1;

  int start_index = 0;
  int found_index = FindPageIndex(pPages, &skip_count, objnum, &start_index, 0);

  // Corrupt page tree may yield out-of-range results.
  if (!fxcrt::IndexInBounds(m_PageList, found_index))
    return -1;

  // Only update |m_PageList| when |objnum| points to a /Page object.
  if (IsValidPageObject(GetOrParseIndirectObject(objnum).Get()))
    m_PageList[found_index] = objnum;
  return found_index;
}

int CPDF_Document::GetPageCount() const {
  return fxcrt::CollectionSize<int>(m_PageList);
}

int CPDF_Document::RetrievePageCount() {
  RetainPtr<CPDF_Dictionary> pPages = GetMutablePagesDict();
  if (!pPages)
    return 0;

  if (!pPages->KeyExist("Kids"))
    return 1;

  std::set<RetainPtr<CPDF_Dictionary>> visited_pages = {pPages};
  return CountPages(std::move(pPages), &visited_pages).value_or(0);
}

uint32_t CPDF_Document::GetUserPermissions(bool get_owner_perms) const {
  return m_pParser ? m_pParser->GetPermissions(get_owner_perms) : 0;
}

RetainPtr<CPDF_StreamAcc> CPDF_Document::GetFontFileStreamAcc(
    RetainPtr<const CPDF_Stream> pFontStream) {
  return m_pDocPage->GetFontFileStreamAcc(std::move(pFontStream));
}

void CPDF_Document::MaybePurgeFontFileStreamAcc(
    RetainPtr<CPDF_StreamAcc>&& pStreamAcc) {
  m_pDocPage->MaybePurgeFontFileStreamAcc(std::move(pStreamAcc));
}

void CPDF_Document::MaybePurgeImage(uint32_t objnum) {
  m_pDocPage->MaybePurgeImage(objnum);
}

void CPDF_Document::CreateNewDoc() {
  DCHECK(!m_pRootDict);
  DCHECK(!m_pInfoDict);
  m_pRootDict = NewIndirect<CPDF_Dictionary>();
  m_pRootDict->SetNewFor<CPDF_Name>("Type", "Catalog");

  auto pPages = NewIndirect<CPDF_Dictionary>();
  pPages->SetNewFor<CPDF_Name>("Type", "Pages");
  pPages->SetNewFor<CPDF_Number>("Count", 0);
  pPages->SetNewFor<CPDF_Array>("Kids");
  m_pRootDict->SetNewFor<CPDF_Reference>("Pages", this, pPages->GetObjNum());
  m_pInfoDict = NewIndirect<CPDF_Dictionary>();
}

RetainPtr<CPDF_Dictionary> CPDF_Document::CreateNewPage(int iPage) {
  auto pDict = NewIndirect<CPDF_Dictionary>();
  pDict->SetNewFor<CPDF_Name>("Type", "Page");
  uint32_t dwObjNum = pDict->GetObjNum();
  if (!InsertNewPage(iPage, pDict)) {
    DeleteIndirectObject(dwObjNum);
    return nullptr;
  }
  return pDict;
}

bool CPDF_Document::InsertDeletePDFPage(
    RetainPtr<CPDF_Dictionary> pages_dict,
    int pages_to_go,
    RetainPtr<CPDF_Dictionary> page_dict,
    bool is_insert,
    std::set<RetainPtr<CPDF_Dictionary>>* visited) {
  RetainPtr<CPDF_Array> kids_list = pages_dict->GetMutableArrayFor("Kids");
  if (!kids_list) {
    return false;
  }

  for (size_t i = 0; i < kids_list->size(); i++) {
    RetainPtr<CPDF_Dictionary> kid_dict = kids_list->GetMutableDictAt(i);
    NodeType kid_type = GetNodeType(kid_dict);
    if (kid_type == NodeType::kLeaf) {
      if (pages_to_go != 0) {
        pages_to_go--;
        continue;
      }
      if (is_insert) {
        kids_list->InsertNewAt<CPDF_Reference>(i, this, page_dict->GetObjNum());
        page_dict->SetNewFor<CPDF_Reference>("Parent", this,
                                             pages_dict->GetObjNum());
      } else {
        kids_list->RemoveAt(i);
      }
      pages_dict->SetNewFor<CPDF_Number>(
          "Count", pages_dict->GetIntegerFor("Count") + (is_insert ? 1 : -1));
      ResetTraversal();
      break;
    }

    CHECK_EQ(kid_type, NodeType::kBranch);
    int page_count = kid_dict->GetIntegerFor("Count");
    if (pages_to_go >= page_count) {
      pages_to_go -= page_count;
      continue;
    }
    if (pdfium::Contains(*visited, kid_dict)) {
      return false;
    }

    ScopedSetInsertion<RetainPtr<CPDF_Dictionary>> insertion(visited, kid_dict);
    if (!InsertDeletePDFPage(std::move(kid_dict), pages_to_go, page_dict,
                             is_insert, visited)) {
      return false;
    }
    pages_dict->SetNewFor<CPDF_Number>(
        "Count", pages_dict->GetIntegerFor("Count") + (is_insert ? 1 : -1));
    break;
  }
  return true;
}

bool CPDF_Document::InsertNewPage(int iPage,
                                  RetainPtr<CPDF_Dictionary> pPageDict) {
  RetainPtr<CPDF_Dictionary> pRoot = GetMutableRoot();
  if (!pRoot)
    return false;

  RetainPtr<CPDF_Dictionary> pPages = pRoot->GetMutableDictFor("Pages");
  if (!pPages)
    return false;

  int nPages = GetPageCount();
  if (iPage < 0 || iPage > nPages)
    return false;

  if (iPage == nPages) {
    RetainPtr<CPDF_Array> pPagesList = pPages->GetOrCreateArrayFor("Kids");
    pPagesList->AppendNew<CPDF_Reference>(this, pPageDict->GetObjNum());
    pPages->SetNewFor<CPDF_Number>("Count", nPages + 1);
    pPageDict->SetNewFor<CPDF_Reference>("Parent", this, pPages->GetObjNum());
    ResetTraversal();
  } else {
    std::set<RetainPtr<CPDF_Dictionary>> stack = {pPages};
    if (!InsertDeletePDFPage(std::move(pPages), iPage, pPageDict, true, &stack))
      return false;
  }
  m_PageList.insert(m_PageList.begin() + iPage, pPageDict->GetObjNum());
  return true;
}

RetainPtr<CPDF_Dictionary> CPDF_Document::GetInfo() {
  if (m_pInfoDict)
    return m_pInfoDict;

  if (!m_pParser)
    return nullptr;

  uint32_t info_obj_num = m_pParser->GetInfoObjNum();
  if (info_obj_num == 0)
    return nullptr;

  auto ref = pdfium::MakeRetain<CPDF_Reference>(this, info_obj_num);
  m_pInfoDict = ToDictionary(ref->GetMutableDirect());
  return m_pInfoDict;
}

RetainPtr<const CPDF_Array> CPDF_Document::GetFileIdentifier() const {
  return m_pParser ? m_pParser->GetIDArray() : nullptr;
}

uint32_t CPDF_Document::DeletePage(int iPage) {
  RetainPtr<CPDF_Dictionary> pPages = GetMutablePagesDict();
  if (!pPages) {
    return 0;
  }

  int nPages = pPages->GetIntegerFor("Count");
  if (iPage < 0 || iPage >= nPages) {
    return 0;
  }

  RetainPtr<const CPDF_Dictionary> page_dict = GetPageDictionary(iPage);
  if (!page_dict) {
    return 0;
  }

  std::set<RetainPtr<CPDF_Dictionary>> stack = {pPages};
  if (!InsertDeletePDFPage(std::move(pPages), iPage, nullptr, false, &stack)) {
    return 0;
  }

  m_PageList.erase(m_PageList.begin() + iPage);
  return page_dict->GetObjNum();
}

void CPDF_Document::SetPageToNullObject(uint32_t page_obj_num) {
  if (!page_obj_num || m_PageList.empty()) {
    return;
  }

  // Load all pages so `m_PageList` has all the object numbers.
  for (size_t i = 0; i < m_PageList.size(); ++i) {
    GetPageDictionary(i);
  }

  if (pdfium::Contains(m_PageList, page_obj_num)) {
    return;
  }

  // If `page_dict` is no longer in the page tree, replace it with an object of
  // type null.
  //
  // Delete the object first from this container, so the conditional in the
  // replacement call always evaluates to true.
  DeleteIndirectObject(page_obj_num);
  const bool replaced = ReplaceIndirectObjectIfHigherGeneration(
      page_obj_num, pdfium::MakeRetain<CPDF_Null>());
  CHECK(replaced);
}

void CPDF_Document::SetRootForTesting(RetainPtr<CPDF_Dictionary> root) {
  m_pRootDict = std::move(root);
}

bool CPDF_Document::MovePages(pdfium::span<const int> page_indices,
                              int dest_page_index) {
  const CPDF_Dictionary* pages = GetPagesDict();
  const int num_pages_signed = pages ? pages->GetIntegerFor("Count") : 0;
  if (num_pages_signed <= 0) {
    return false;
  }
  const size_t num_pages = num_pages_signed;

  // Check the number of pages is in range.
  if (page_indices.empty() || page_indices.size() > num_pages) {
    return false;
  }

  // Check that destination page index is in range.
  if (dest_page_index < 0 ||
      static_cast<size_t>(dest_page_index) > num_pages - page_indices.size()) {
    return false;
  }

  // Check for if XFA is enabled.
  Extension* extension = GetExtension();
  if (extension && extension->ContainsExtensionForm()) {
    // Don't manipulate XFA PDFs.
    return false;
  }

  // Check for duplicate and out-of-range page indices
  std::set<int> unique_page_indices;
  // Store the pages that need to be moved. They'll be deleted then reinserted.
  std::vector<RetainPtr<CPDF_Dictionary>> pages_to_move;
  pages_to_move.reserve(page_indices.size());
  // Store the page indices that will be deleted (and moved).
  std::vector<int> page_indices_to_delete;
  page_indices_to_delete.reserve(page_indices.size());
  for (const int page_index : page_indices) {
    bool inserted = unique_page_indices.insert(page_index).second;
    if (!inserted) {
      // Duplicate page index found
      return false;
    }
    RetainPtr<CPDF_Dictionary> page = GetMutablePageDictionary(page_index);
    if (!page) {
      // Page not found, index might be out of range.
      return false;
    }
    pages_to_move.push_back(std::move(page));
    page_indices_to_delete.push_back(page_index);
  }

  // Sort the page indices to be deleted in descending order.
  std::sort(page_indices_to_delete.begin(), page_indices_to_delete.end(),
            std::greater<int>());
  // Delete the pages in descending order.
  if (extension) {
    for (int page_index : page_indices_to_delete) {
      extension->DeletePage(page_index);
    }
  } else {
    for (int page_index : page_indices_to_delete) {
      DeletePage(page_index);
    }
  }

  // Insert the deleted pages back into the document at the destination page
  // index.
  for (size_t i = 0; i < pages_to_move.size(); ++i) {
    if (!InsertNewPage(i + dest_page_index, pages_to_move[i])) {
      // Fail in an indeterminate state.
      return false;
    }
  }

  return true;
}

void CPDF_Document::ResizePageListForTesting(size_t size) {
  m_PageList.resize(size);
}

CPDF_Document::StockFontClearer::StockFontClearer(
    CPDF_Document::PageDataIface* pPageData)
    : m_pPageData(pPageData) {}

CPDF_Document::StockFontClearer::~StockFontClearer() {
  m_pPageData->ClearStockFont();
}

CPDF_Document::PageDataIface::PageDataIface() = default;

CPDF_Document::PageDataIface::~PageDataIface() = default;

CPDF_Document::RenderDataIface::RenderDataIface() = default;

CPDF_Document::RenderDataIface::~RenderDataIface() = default;
