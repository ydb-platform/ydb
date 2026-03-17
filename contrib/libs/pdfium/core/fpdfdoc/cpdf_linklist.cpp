// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfdoc/cpdf_linklist.h"

#include <utility>

#include "core/fpdfapi/page/cpdf_page.h"
#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fxcrt/numerics/safe_conversions.h"

CPDF_LinkList::CPDF_LinkList() = default;

CPDF_LinkList::~CPDF_LinkList() = default;

CPDF_Link CPDF_LinkList::GetLinkAtPoint(CPDF_Page* pPage,
                                        const CFX_PointF& point,
                                        int* z_order) {
  const std::vector<RetainPtr<CPDF_Dictionary>>* pPageLinkList =
      GetPageLinks(pPage);
  if (!pPageLinkList)
    return CPDF_Link();

  for (size_t i = pPageLinkList->size(); i > 0; --i) {
    size_t annot_index = i - 1;
    RetainPtr<CPDF_Dictionary> pAnnot = (*pPageLinkList)[annot_index];
    if (!pAnnot)
      continue;

    CPDF_Link link(std::move(pAnnot));
    if (!link.GetRect().Contains(point))
      continue;

    if (z_order)
      *z_order = pdfium::checked_cast<int32_t>(annot_index);
    return link;
  }
  return CPDF_Link();
}

const std::vector<RetainPtr<CPDF_Dictionary>>* CPDF_LinkList::GetPageLinks(
    CPDF_Page* pPage) {
  uint32_t objnum = pPage->GetDict()->GetObjNum();
  if (objnum == 0)
    return nullptr;

  auto it = m_PageMap.find(objnum);
  if (it != m_PageMap.end())
    return &it->second;

  // std::map::operator[] forces the creation of a map entry.
  auto* page_link_list = &m_PageMap[objnum];
  RetainPtr<CPDF_Array> pAnnotList = pPage->GetMutableAnnotsArray();
  if (!pAnnotList)
    return page_link_list;

  for (size_t i = 0; i < pAnnotList->size(); ++i) {
    RetainPtr<CPDF_Dictionary> pAnnot = pAnnotList->GetMutableDictAt(i);
    bool add_link = (pAnnot && pAnnot->GetByteStringFor("Subtype") == "Link");
    // Add non-links as nullptrs to preserve z-order.
    page_link_list->emplace_back(add_link ? pAnnot : nullptr);
  }
  return page_link_list;
}
