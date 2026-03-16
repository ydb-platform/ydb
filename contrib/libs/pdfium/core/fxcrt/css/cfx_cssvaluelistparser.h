// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCRT_CSS_CFX_CSSVALUELISTPARSER_H_
#define CORE_FXCRT_CSS_CFX_CSSVALUELISTPARSER_H_

#include <stdint.h>

#include <optional>

#include "core/fxcrt/css/cfx_cssvalue.h"
#include "core/fxcrt/widestring.h"

class CFX_CSSValueListParser {
 public:
  struct Result {
    CFX_CSSValue::PrimitiveType type = CFX_CSSValue::PrimitiveType::kUnknown;
    WideStringView string_view;
  };

  CFX_CSSValueListParser(WideStringView list, wchar_t separator);
  ~CFX_CSSValueListParser();

  std::optional<Result> NextValue();
  void UseCommaSeparator() { m_Separator = ','; }

 private:
  bool CharsRemain() const { return !m_Cur.IsEmpty(); }

  // Safe to call even when input exhausted, stays unchanged.
  void Advance() { m_Cur = m_Cur.Substr(1); }

  // Safe to call even when input exhausted, returns NUL.
  wchar_t CurrentChar() const { return static_cast<wchar_t>(m_Cur.Front()); }

  size_t SkipToChar(wchar_t wch);
  size_t SkipToCharMatchingParens(wchar_t wch);

  WideStringView m_Cur;
  wchar_t m_Separator;
};

#endif  // CORE_FXCRT_CSS_CFX_CSSVALUELISTPARSER_H_
