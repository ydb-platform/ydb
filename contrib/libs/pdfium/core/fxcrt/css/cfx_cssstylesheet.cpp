// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcrt/css/cfx_cssstylesheet.h"

#include <utility>

#include "core/fxcrt/css/cfx_cssdata.h"
#include "core/fxcrt/css/cfx_cssdeclaration.h"
#include "core/fxcrt/css/cfx_cssstylerule.h"
#include "core/fxcrt/fx_codepage.h"

CFX_CSSStyleSheet::CFX_CSSStyleSheet() = default;

CFX_CSSStyleSheet::~CFX_CSSStyleSheet() = default;

size_t CFX_CSSStyleSheet::CountRules() const {
  return m_RuleArray.size();
}

CFX_CSSStyleRule* CFX_CSSStyleSheet::GetRule(size_t index) const {
  return m_RuleArray[index].get();
}

bool CFX_CSSStyleSheet::LoadBuffer(WideStringView buffer) {
  m_RuleArray.clear();
  auto pSyntax = std::make_unique<CFX_CSSSyntaxParser>(buffer);
  while (true) {
    CFX_CSSSyntaxParser::Status eStatus = pSyntax->DoSyntaxParse();
    if (eStatus == CFX_CSSSyntaxParser::Status::kStyleRule)
      eStatus = LoadStyleRule(pSyntax.get());
    if (eStatus == CFX_CSSSyntaxParser::Status::kEOS)
      return true;
    if (eStatus == CFX_CSSSyntaxParser::Status::kError)
      return false;
  }
}

CFX_CSSSyntaxParser::Status CFX_CSSStyleSheet::LoadStyleRule(
    CFX_CSSSyntaxParser* pSyntax) {
  std::vector<std::unique_ptr<CFX_CSSSelector>> selectors;
  CFX_CSSStyleRule* pStyleRule = nullptr;
  int32_t iValueLen = 0;
  const CFX_CSSData::Property* property = nullptr;
  WideString wsName;
  while (true) {
    switch (pSyntax->DoSyntaxParse()) {
      case CFX_CSSSyntaxParser::Status::kSelector: {
        WideStringView strValue = pSyntax->GetCurrentString();
        auto pSelector = CFX_CSSSelector::FromString(strValue);
        if (pSelector)
          selectors.push_back(std::move(pSelector));
        break;
      }
      case CFX_CSSSyntaxParser::Status::kPropertyName: {
        WideStringView strValue = pSyntax->GetCurrentString();
        property = CFX_CSSData::GetPropertyByName(strValue);
        if (!property)
          wsName = WideString(strValue);
        break;
      }
      case CFX_CSSSyntaxParser::Status::kPropertyValue: {
        if (property || iValueLen > 0) {
          WideStringView strValue = pSyntax->GetCurrentString();
          auto* decl = pStyleRule->GetDeclaration();
          if (!strValue.IsEmpty()) {
            if (property) {
              decl->AddProperty(property, strValue);
            } else {
              decl->AddProperty(wsName, WideString(strValue));
            }
          }
        }
        break;
      }
      case CFX_CSSSyntaxParser::Status::kDeclOpen: {
        if (!pStyleRule && !selectors.empty()) {
          auto rule = std::make_unique<CFX_CSSStyleRule>();
          pStyleRule = rule.get();
          pStyleRule->SetSelector(&selectors);
          m_RuleArray.push_back(std::move(rule));
        } else {
          SkipRuleSet(pSyntax);
          return CFX_CSSSyntaxParser::Status::kNone;
        }
        break;
      }
      case CFX_CSSSyntaxParser::Status::kDeclClose: {
        if (pStyleRule && pStyleRule->GetDeclaration()->empty()) {
          m_RuleArray.pop_back();
          pStyleRule = nullptr;
        }
        return CFX_CSSSyntaxParser::Status::kNone;
      }
      case CFX_CSSSyntaxParser::Status::kEOS:
        return CFX_CSSSyntaxParser::Status::kEOS;
      case CFX_CSSSyntaxParser::Status::kError:
      default:
        return CFX_CSSSyntaxParser::Status::kError;
    }
  }
}

void CFX_CSSStyleSheet::SkipRuleSet(CFX_CSSSyntaxParser* pSyntax) {
  while (true) {
    switch (pSyntax->DoSyntaxParse()) {
      case CFX_CSSSyntaxParser::Status::kSelector:
      case CFX_CSSSyntaxParser::Status::kDeclOpen:
      case CFX_CSSSyntaxParser::Status::kPropertyName:
      case CFX_CSSSyntaxParser::Status::kPropertyValue:
        break;
      case CFX_CSSSyntaxParser::Status::kDeclClose:
      case CFX_CSSSyntaxParser::Status::kEOS:
      case CFX_CSSSyntaxParser::Status::kError:
      default:
        return;
    }
  }
}
