// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCRT_CSS_CFX_CSSSYNTAXPARSER_H_
#define CORE_FXCRT_CSS_CFX_CSSSYNTAXPARSER_H_

#include <stack>

#include "core/fxcrt/css/cfx_cssinputtextbuf.h"
#include "core/fxcrt/css/cfx_cssoutputtextbuf.h"
#include "core/fxcrt/widestring.h"

class CFX_CSSSyntaxParser {
 public:
  enum class Status : uint8_t {
    kError,
    kEOS,
    kNone,
    kStyleRule,
    kSelector,
    kDeclOpen,
    kDeclClose,
    kPropertyName,
    kPropertyValue,
  };

  explicit CFX_CSSSyntaxParser(WideStringView str);
  ~CFX_CSSSyntaxParser();

  void SetParseOnlyDeclarations();
  Status DoSyntaxParse();
  WideStringView GetCurrentString() const;

 private:
  enum class Mode : uint8_t {
    kRuleSet,
    kComment,
    kSelector,
    kPropertyName,
    kPropertyValue,
  };

  void SaveMode(Mode eMode);
  bool RestoreMode();

  bool m_bHasError = false;
  Mode m_eMode = Mode::kRuleSet;
  CFX_CSSOutputTextBuf m_Output;
  CFX_CSSInputTextBuf m_Input;
  std::stack<Mode> m_ModeStack;
};

#endif  // CORE_FXCRT_CSS_CFX_CSSSYNTAXPARSER_H_
