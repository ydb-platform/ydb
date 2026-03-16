// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFDOC_CPDF_BOOKMARK_H_
#define CORE_FPDFDOC_CPDF_BOOKMARK_H_

#include "core/fpdfdoc/cpdf_action.h"
#include "core/fpdfdoc/cpdf_dest.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/widestring.h"

class CPDF_Dictionary;
class CPDF_Document;

class CPDF_Bookmark {
 public:
  CPDF_Bookmark();
  CPDF_Bookmark(const CPDF_Bookmark& that);
  explicit CPDF_Bookmark(RetainPtr<const CPDF_Dictionary> pDict);
  ~CPDF_Bookmark();

  const CPDF_Dictionary* GetDict() const { return m_pDict.Get(); }

  WideString GetTitle() const;
  CPDF_Dest GetDest(CPDF_Document* pDocument) const;
  CPDF_Action GetAction() const;
  int GetCount() const;

 private:
  RetainPtr<const CPDF_Dictionary> m_pDict;
};

#endif  // CORE_FPDFDOC_CPDF_BOOKMARK_H_
