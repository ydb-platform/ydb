// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFDOC_CPDF_ANNOTLIST_H_
#define CORE_FPDFDOC_CPDF_ANNOTLIST_H_

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <vector>

#include "core/fpdfapi/render/cpdf_pagerendercontext.h"
#include "core/fxcrt/fx_coordinates.h"
#include "core/fxcrt/unowned_ptr.h"

class CPDF_Annot;
class CPDF_Document;
class CPDF_Page;
class CPDF_RenderContext;

class CPDF_AnnotList final : public CPDF_PageRenderContext::AnnotListIface {
 public:
  explicit CPDF_AnnotList(CPDF_Page* pPage);
  ~CPDF_AnnotList() override;

  void DisplayAnnots(CPDF_RenderContext* pContext,
                     bool bPrinting,
                     const CFX_Matrix& mtUser2Device,
                     bool bShowWidget);

  size_t Count() const { return m_AnnotList.size(); }
  CPDF_Annot* GetAt(size_t index) const { return m_AnnotList[index].get(); }
  bool Contains(const CPDF_Annot* pAnnot) const;

 private:
  void DisplayPass(CPDF_RenderContext* pContext,
                   bool bPrinting,
                   const CFX_Matrix& mtMatrix,
                   bool bWidget);

  UnownedPtr<CPDF_Page> const m_pPage;
  UnownedPtr<CPDF_Document> const m_pDocument;

  // The first |m_nAnnotCount| elements are from the PDF itself. The rest are
  // generated pop-up annotations.
  std::vector<std::unique_ptr<CPDF_Annot>> m_AnnotList;
  size_t m_nAnnotCount = 0;
};

#endif  // CORE_FPDFDOC_CPDF_ANNOTLIST_H_
