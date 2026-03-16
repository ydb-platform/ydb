// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCRT_CSS_CFX_CSSRULECOLLECTION_H_
#define CORE_FXCRT_CSS_CFX_CSSRULECOLLECTION_H_

#include <map>
#include <memory>
#include <vector>

#include "core/fxcrt/unowned_ptr.h"
#include "core/fxcrt/widestring.h"

class CFX_CSSDeclaration;
class CFX_CSSSelector;
class CFX_CSSStyleRule;
class CFX_CSSStyleSheet;

class CFX_CSSRuleCollection {
 public:
  class Data {
   public:
    Data(CFX_CSSSelector* pSel, CFX_CSSDeclaration* pDecl);
    ~Data();

    UnownedPtr<CFX_CSSSelector> const pSelector;
    UnownedPtr<CFX_CSSDeclaration> const pDeclaration;
  };

  CFX_CSSRuleCollection();
  ~CFX_CSSRuleCollection();

  void SetRulesFromSheet(const CFX_CSSStyleSheet* sheet);

  const std::vector<std::unique_ptr<Data>>* GetTagRuleData(
      const WideString& tagname) const;

 private:
  void AddRule(CFX_CSSStyleRule* pRule);

  std::map<uint32_t, std::vector<std::unique_ptr<Data>>> m_TagRules;
};

#endif  // CORE_FXCRT_CSS_CFX_CSSRULECOLLECTION_H_
