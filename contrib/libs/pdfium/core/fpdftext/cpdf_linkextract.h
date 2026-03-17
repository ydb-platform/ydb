// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFTEXT_CPDF_LINKEXTRACT_H_
#define CORE_FPDFTEXT_CPDF_LINKEXTRACT_H_

#include <stddef.h>
#include <stdint.h>

#include <optional>
#include <vector>

#include "core/fxcrt/fx_coordinates.h"
#include "core/fxcrt/unowned_ptr.h"
#include "core/fxcrt/widestring.h"

class CPDF_TextPage;

class CPDF_LinkExtract {
 public:
  struct Range {
    size_t m_Start;
    size_t m_Count;
  };

  explicit CPDF_LinkExtract(const CPDF_TextPage* pTextPage);
  ~CPDF_LinkExtract();

  void ExtractLinks();
  size_t CountLinks() const { return m_LinkArray.size(); }
  WideString GetURL(size_t index) const;
  std::vector<CFX_FloatRect> GetRects(size_t index) const;
  std::optional<Range> GetTextRange(size_t index) const;

 protected:
  struct Link : public Range {
    WideString m_strUrl;
  };

  std::optional<Link> CheckWebLink(const WideString& str);
  bool CheckMailLink(WideString* str);

  UnownedPtr<const CPDF_TextPage> const m_pTextPage;
  std::vector<Link> m_LinkArray;
};

#endif  // CORE_FPDFTEXT_CPDF_LINKEXTRACT_H_
