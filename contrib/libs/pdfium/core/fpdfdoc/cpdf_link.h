// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFDOC_CPDF_LINK_H_
#define CORE_FPDFDOC_CPDF_LINK_H_

#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfdoc/cpdf_action.h"
#include "core/fpdfdoc/cpdf_dest.h"
#include "core/fxcrt/fx_coordinates.h"
#include "core/fxcrt/retain_ptr.h"

class CPDF_Link {
 public:
  CPDF_Link();
  explicit CPDF_Link(RetainPtr<CPDF_Dictionary> pDict);
  CPDF_Link(const CPDF_Link& that);
  ~CPDF_Link();

  RetainPtr<CPDF_Dictionary> GetMutableDict() const { return m_pDict; }
  CFX_FloatRect GetRect();
  CPDF_Dest GetDest(CPDF_Document* pDoc);
  CPDF_Action GetAction();

 private:
  RetainPtr<CPDF_Dictionary> m_pDict;
};

#endif  // CORE_FPDFDOC_CPDF_LINK_H_
