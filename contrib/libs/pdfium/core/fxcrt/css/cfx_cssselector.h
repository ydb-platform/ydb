// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCRT_CSS_CFX_CSSSELECTOR_H_
#define CORE_FXCRT_CSS_CFX_CSSSELECTOR_H_

#include <memory>

#include "core/fxcrt/widestring.h"

class CFX_CSSSelector {
 public:
  static std::unique_ptr<CFX_CSSSelector> FromString(WideStringView str);

  CFX_CSSSelector(WideStringView str, std::unique_ptr<CFX_CSSSelector> next);
  ~CFX_CSSSelector();

  bool is_descendant() const { return is_descendant_; }
  uint32_t name_hash() const { return name_hash_; }
  const CFX_CSSSelector* next_selector() const { return next_.get(); }

 private:
  void set_is_descendant() { is_descendant_ = true; }

  bool is_descendant_ = false;
  const uint32_t name_hash_;
  const std::unique_ptr<CFX_CSSSelector> next_;
};

#endif  // CORE_FXCRT_CSS_CFX_CSSSELECTOR_H_
