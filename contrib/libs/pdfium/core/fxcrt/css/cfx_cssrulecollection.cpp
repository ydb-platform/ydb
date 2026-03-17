// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcrt/css/cfx_cssrulecollection.h"

#include <algorithm>
#include <utility>

#include "core/fxcrt/css/cfx_cssdeclaration.h"
#include "core/fxcrt/css/cfx_cssselector.h"
#include "core/fxcrt/css/cfx_cssstylerule.h"
#include "core/fxcrt/css/cfx_cssstylesheet.h"
#include "core/fxcrt/css/cfx_csssyntaxparser.h"

CFX_CSSRuleCollection::CFX_CSSRuleCollection() = default;

CFX_CSSRuleCollection::~CFX_CSSRuleCollection() = default;

const std::vector<std::unique_ptr<CFX_CSSRuleCollection::Data>>*
CFX_CSSRuleCollection::GetTagRuleData(const WideString& tagname) const {
  auto it = m_TagRules.find(FX_HashCode_GetLoweredW(tagname.AsStringView()));
  return it != m_TagRules.end() ? &it->second : nullptr;
}

void CFX_CSSRuleCollection::SetRulesFromSheet(const CFX_CSSStyleSheet* sheet) {
  m_TagRules.clear();
  for (size_t i = 0; i < sheet->CountRules(); ++i)
    AddRule(sheet->GetRule(i));
}

void CFX_CSSRuleCollection::AddRule(CFX_CSSStyleRule* pStyleRule) {
  CFX_CSSDeclaration* pDeclaration = pStyleRule->GetDeclaration();
  size_t nSelectors = pStyleRule->CountSelectorLists();
  for (size_t i = 0; i < nSelectors; ++i) {
    CFX_CSSSelector* pSelector = pStyleRule->GetSelectorList(i);
    m_TagRules[pSelector->name_hash()].push_back(
        std::make_unique<Data>(pSelector, pDeclaration));
  }
}

CFX_CSSRuleCollection::Data::Data(CFX_CSSSelector* pSel,
                                  CFX_CSSDeclaration* pDecl)
    : pSelector(pSel), pDeclaration(pDecl) {}

CFX_CSSRuleCollection::Data::~Data() = default;
