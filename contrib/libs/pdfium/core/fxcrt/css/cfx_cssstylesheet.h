// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCRT_CSS_CFX_CSSSTYLESHEET_H_
#define CORE_FXCRT_CSS_CFX_CSSSTYLESHEET_H_

#include <memory>
#include <vector>

#include "core/fxcrt/css/cfx_csssyntaxparser.h"
#include "core/fxcrt/widestring.h"

class CFX_CSSStyleRule;

class CFX_CSSStyleSheet {
 public:
  CFX_CSSStyleSheet();
  ~CFX_CSSStyleSheet();

  bool LoadBuffer(WideStringView buffer);
  size_t CountRules() const;
  CFX_CSSStyleRule* GetRule(size_t index) const;

 private:
  CFX_CSSSyntaxParser::Status LoadStyleRule(CFX_CSSSyntaxParser* pSyntax);
  void SkipRuleSet(CFX_CSSSyntaxParser* pSyntax);

  std::vector<std::unique_ptr<CFX_CSSStyleRule>> m_RuleArray;
};

#endif  // CORE_FXCRT_CSS_CFX_CSSSTYLESHEET_H_
