// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFDOC_CPDF_PAGELABEL_H_
#define CORE_FPDFDOC_CPDF_PAGELABEL_H_

#include <optional>

#include "core/fxcrt/unowned_ptr.h"
#include "core/fxcrt/widestring.h"

class CPDF_Document;

class CPDF_PageLabel {
 public:
  explicit CPDF_PageLabel(CPDF_Document* doc);
  ~CPDF_PageLabel();

  std::optional<WideString> GetLabel(int page_index) const;

 private:
  UnownedPtr<CPDF_Document> const doc_;
};

#endif  // CORE_FPDFDOC_CPDF_PAGELABEL_H_
