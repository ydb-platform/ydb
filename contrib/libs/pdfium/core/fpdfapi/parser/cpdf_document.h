// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PARSER_CPDF_DOCUMENT_H_
#define CORE_FPDFAPI_PARSER_CPDF_DOCUMENT_H_

#include <memory>
#include <set>
#include <utility>
#include <vector>

#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_parser.h"
#include "core/fxcrt/fx_memory.h"
#include "core/fxcrt/observed_ptr.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/span.h"
#include "core/fxcrt/unowned_ptr.h"

class CPDF_ReadValidator;
class CPDF_StreamAcc;
class IFX_SeekableReadStream;
class JBig2_DocumentContext;

class CPDF_Document : public Observable,
                      public CPDF_Parser::ParsedObjectsHolder {
 public:
  // Type from which the XFA extension can subclass itself.
  class Extension {
   public:
    virtual ~Extension() = default;
    virtual int GetPageCount() const = 0;
    virtual uint32_t DeletePage(int page_index) = 0;
    virtual bool ContainsExtensionForm() const = 0;
    virtual bool ContainsExtensionFullForm() const = 0;
    virtual bool ContainsExtensionForegroundForm() const = 0;
  };

  class LinkListIface {
   public:
    // CPDF_Document merely helps manage the lifetime.
    virtual ~LinkListIface() = default;
  };

  class PageDataIface {
   public:
    PageDataIface();
    virtual ~PageDataIface();

    virtual void ClearStockFont() = 0;
    virtual RetainPtr<CPDF_StreamAcc> GetFontFileStreamAcc(
        RetainPtr<const CPDF_Stream> pFontStream) = 0;
    virtual void MaybePurgeFontFileStreamAcc(
        RetainPtr<CPDF_StreamAcc>&& pStreamAcc) = 0;
    virtual void MaybePurgeImage(uint32_t objnum) = 0;

    void SetDocument(CPDF_Document* pDoc) { m_pDoc = pDoc; }

   protected:
    CPDF_Document* GetDocument() const { return m_pDoc; }

   private:
    UnownedPtr<CPDF_Document> m_pDoc;
  };

  class RenderDataIface {
   public:
    RenderDataIface();
    virtual ~RenderDataIface();

    void SetDocument(CPDF_Document* pDoc) { m_pDoc = pDoc; }

   protected:
    CPDF_Document* GetDocument() const { return m_pDoc; }

   private:
    UnownedPtr<CPDF_Document> m_pDoc;
  };

  static constexpr int kPageMaxNum = 0xFFFFF;

  static bool IsValidPageObject(const CPDF_Object* obj);

  CPDF_Document(std::unique_ptr<RenderDataIface> pRenderData,
                std::unique_ptr<PageDataIface> pPageData);
  ~CPDF_Document() override;

  Extension* GetExtension() const { return m_pExtension.get(); }
  void SetExtension(std::unique_ptr<Extension> pExt) {
    m_pExtension = std::move(pExt);
  }

  CPDF_Parser* GetParser() const { return m_pParser.get(); }
  const CPDF_Dictionary* GetRoot() const { return m_pRootDict.Get(); }
  RetainPtr<CPDF_Dictionary> GetMutableRoot() { return m_pRootDict; }
  RetainPtr<CPDF_Dictionary> GetInfo();
  RetainPtr<const CPDF_Array> GetFileIdentifier() const;

  // Returns the object number for the deleted page, or 0 on failure.
  uint32_t DeletePage(int iPage);
  // `page_obj_num` is the return value from DeletePage(). If it is non-zero,
  // and it is no longer used in the page tree, then replace the page object
  // with a null object.
  void SetPageToNullObject(uint32_t page_obj_num);
  bool MovePages(pdfium::span<const int> page_indices, int dest_page_index);

  int GetPageCount() const;
  bool IsPageLoaded(int iPage) const;
  RetainPtr<const CPDF_Dictionary> GetPageDictionary(int iPage);
  RetainPtr<CPDF_Dictionary> GetMutablePageDictionary(int iPage);
  int GetPageIndex(uint32_t objnum);
  // When `get_owner_perms` is true, returns full permissions if unlocked by
  // owner.
  uint32_t GetUserPermissions(bool get_owner_perms) const;

  // PageDataIface wrappers, try to avoid explicit getter calls.
  RetainPtr<CPDF_StreamAcc> GetFontFileStreamAcc(
      RetainPtr<const CPDF_Stream> pFontStream);
  void MaybePurgeFontFileStreamAcc(RetainPtr<CPDF_StreamAcc>&& pStreamAcc);
  void MaybePurgeImage(uint32_t objnum);

  // Returns a valid pointer, unless it is called during destruction.
  PageDataIface* GetPageData() const { return m_pDocPage.get(); }
  RenderDataIface* GetRenderData() const { return m_pDocRender.get(); }

  void SetPageObjNum(int iPage, uint32_t objNum);

  JBig2_DocumentContext* GetOrCreateCodecContext();
  LinkListIface* GetLinksContext() const { return m_pLinksContext.get(); }
  void SetLinksContext(std::unique_ptr<LinkListIface> pContext) {
    m_pLinksContext = std::move(pContext);
  }

  // Behaves like NewIndirect<CPDF_Stream>(dict), but keeps track of the object
  // number assigned to the newly created stream.
  RetainPtr<CPDF_Stream> CreateModifiedAPStream(
      RetainPtr<CPDF_Dictionary> dict);

  // Returns whether CreateModifiedAPStream() created `stream`.
  bool IsModifiedAPStream(const CPDF_Stream* stream) const;

  // CPDF_Parser::ParsedObjectsHolder:
  bool TryInit() override;
  RetainPtr<CPDF_Object> ParseIndirectObject(uint32_t objnum) override;

  CPDF_Parser::Error LoadDoc(RetainPtr<IFX_SeekableReadStream> pFileAccess,
                             const ByteString& password);
  CPDF_Parser::Error LoadLinearizedDoc(RetainPtr<CPDF_ReadValidator> validator,
                                       const ByteString& password);
  bool has_valid_cross_reference_table() const {
    return m_bHasValidCrossReferenceTable;
  }

  void LoadPages();
  void CreateNewDoc();
  RetainPtr<CPDF_Dictionary> CreateNewPage(int iPage);

  void IncrementParsedPageCount() { ++m_ParsedPageCount; }
  uint32_t GetParsedPageCountForTesting() { return m_ParsedPageCount; }

  void SetRootForTesting(RetainPtr<CPDF_Dictionary> root);

 protected:
  void SetParser(std::unique_ptr<CPDF_Parser> pParser);

  void ResizePageListForTesting(size_t size);

 private:
  class StockFontClearer {
   public:
    FX_STACK_ALLOCATED();

    explicit StockFontClearer(CPDF_Document::PageDataIface* pPageData);
    ~StockFontClearer();

   private:
    UnownedPtr<CPDF_Document::PageDataIface> const m_pPageData;
  };

  // Retrieve page count information by getting count value from the tree nodes
  int RetrievePageCount();

  // When this method is called, m_pTreeTraversal[level] exists.
  RetainPtr<CPDF_Dictionary> TraversePDFPages(int iPage,
                                              int* nPagesToGo,
                                              size_t level);

  RetainPtr<const CPDF_Dictionary> GetPagesDict() const;
  RetainPtr<CPDF_Dictionary> GetMutablePagesDict();

  bool InsertDeletePDFPage(RetainPtr<CPDF_Dictionary> pages_dict,
                           int pages_to_go,
                           RetainPtr<CPDF_Dictionary> page_dict,
                           bool is_insert,
                           std::set<RetainPtr<CPDF_Dictionary>>* visited);

  bool InsertNewPage(int iPage, RetainPtr<CPDF_Dictionary> pPageDict);
  void ResetTraversal();
  CPDF_Parser::Error HandleLoadResult(CPDF_Parser::Error error);

  std::unique_ptr<CPDF_Parser> m_pParser;
  RetainPtr<CPDF_Dictionary> m_pRootDict;
  RetainPtr<CPDF_Dictionary> m_pInfoDict;

  // Vector of pairs to know current position in the page tree. The index in the
  // vector corresponds to the level being described. The pair contains a
  // pointer to the dictionary being processed at the level, and an index of the
  // of the child being processed within the dictionary's /Kids array.
  std::vector<std::pair<RetainPtr<CPDF_Dictionary>, size_t>> m_pTreeTraversal;

  // True if the CPDF_Parser succeeded without having to rebuild the cross
  // reference table.
  bool m_bHasValidCrossReferenceTable = false;

  // Index of the next page that will be traversed from the page tree.
  bool m_bReachedMaxPageLevel = false;
  int m_iNextPageToTraverse = 0;
  uint32_t m_ParsedPageCount = 0;

  std::unique_ptr<RenderDataIface> const m_pDocRender;
  // Must be after `m_pDocRender`.
  std::unique_ptr<PageDataIface> const m_pDocPage;
  std::unique_ptr<JBig2_DocumentContext> m_pCodecContext;
  std::unique_ptr<LinkListIface> m_pLinksContext;
  std::set<uint32_t> m_ModifiedAPStreamIDs;
  std::vector<uint32_t> m_PageList;  // Page number to page's dict objnum.

  // Must be second to last.
  StockFontClearer m_StockFontClearer;

  // Must be last. Destroy the extension before any non-extension teardown.
  std::unique_ptr<Extension> m_pExtension;
};

#endif  // CORE_FPDFAPI_PARSER_CPDF_DOCUMENT_H_
