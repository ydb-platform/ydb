// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFDOC_CPDF_LINKLIST_H_
#define CORE_FPDFDOC_CPDF_LINKLIST_H_

#include <stdint.h>

#include <map>
#include <vector>

#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fpdfdoc/cpdf_link.h"
#include "core/fxcrt/retain_ptr.h"

class CPDF_Page;
class CPDF_Dictionary;

class CPDF_LinkList final : public CPDF_Document::LinkListIface {
 public:
  CPDF_LinkList();
  ~CPDF_LinkList() override;

  CPDF_Link GetLinkAtPoint(CPDF_Page* pPage,
                           const CFX_PointF& point,
                           int* z_order);

 private:
  const std::vector<RetainPtr<CPDF_Dictionary>>* GetPageLinks(CPDF_Page* pPage);

  std::map<uint32_t, std::vector<RetainPtr<CPDF_Dictionary>>> m_PageMap;
};

#endif  // CORE_FPDFDOC_CPDF_LINKLIST_H_
