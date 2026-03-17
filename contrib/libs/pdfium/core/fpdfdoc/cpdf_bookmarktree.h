// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFDOC_CPDF_BOOKMARKTREE_H_
#define CORE_FPDFDOC_CPDF_BOOKMARKTREE_H_

#include "core/fpdfdoc/cpdf_bookmark.h"
#include "core/fxcrt/unowned_ptr.h"

class CPDF_Document;

class CPDF_BookmarkTree {
 public:
  explicit CPDF_BookmarkTree(const CPDF_Document* doc);
  ~CPDF_BookmarkTree();

  CPDF_Bookmark GetFirstChild(const CPDF_Bookmark& parent) const;
  CPDF_Bookmark GetNextSibling(const CPDF_Bookmark& bookmark) const;

 private:
  UnownedPtr<const CPDF_Document> const document_;
};

#endif  // CORE_FPDFDOC_CPDF_BOOKMARKTREE_H_
