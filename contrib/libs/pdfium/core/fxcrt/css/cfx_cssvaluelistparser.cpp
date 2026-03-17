// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcrt/css/cfx_cssvaluelistparser.h"

#include "core/fxcrt/check.h"
#include "core/fxcrt/check_op.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/fx_extension.h"
#include "core/fxcrt/fx_system.h"

CFX_CSSValueListParser::CFX_CSSValueListParser(WideStringView list,
                                               wchar_t separator)
    : m_Cur(list), m_Separator(separator) {
  DCHECK(CharsRemain());
}

CFX_CSSValueListParser::~CFX_CSSValueListParser() = default;

std::optional<CFX_CSSValueListParser::Result>
CFX_CSSValueListParser::NextValue() {
  while (CharsRemain() &&
         (CurrentChar() <= ' ' || CurrentChar() == m_Separator)) {
    Advance();
  }
  if (!CharsRemain()) {
    return std::nullopt;
  }
  auto eType = CFX_CSSValue::PrimitiveType::kUnknown;
  WideStringView start = m_Cur;
  size_t nLength = 0;
  wchar_t wch = CurrentChar();
  if (wch == '#') {
    nLength = SkipToChar(' ');
    if (nLength == 4 || nLength == 7) {
      eType = CFX_CSSValue::PrimitiveType::kRGB;
    }
  } else if (FXSYS_IsDecimalDigit(wch) || wch == '.' || wch == '-' ||
             wch == '+') {
    while (CharsRemain() &&
           (CurrentChar() > ' ' && CurrentChar() != m_Separator)) {
      ++nLength;
      Advance();
    }
    eType = CFX_CSSValue::PrimitiveType::kNumber;
  } else if (wch == '\"' || wch == '\'') {
    start = start.Substr(1);
    Advance();
    nLength = SkipToChar(wch);
    Advance();
    eType = CFX_CSSValue::PrimitiveType::kString;
  } else if (m_Cur.First(4).EqualsASCIINoCase(
                 "rgb(")) {  // First() always safe.
    nLength = SkipToChar(')') + 1;
    Advance();
    eType = CFX_CSSValue::PrimitiveType::kRGB;
  } else {
    nLength = SkipToCharMatchingParens(m_Separator);
    eType = CFX_CSSValue::PrimitiveType::kString;
  }
  if (nLength == 0) {
    return std::nullopt;
  }
  return Result{eType, start.First(nLength)};
}

size_t CFX_CSSValueListParser::SkipToChar(wchar_t wch) {
  size_t count = 0;
  while (CharsRemain() && CurrentChar() != wch) {
    Advance();
    ++count;
  }
  return count;
}

size_t CFX_CSSValueListParser::SkipToCharMatchingParens(wchar_t wch) {
  size_t nLength = 0;
  int64_t bracketCount = 0;
  while (CharsRemain() && CurrentChar() != wch) {
    if (CurrentChar() <= ' ') {
      break;
    }
    if (CurrentChar() == '(') {
      bracketCount++;
    } else if (CurrentChar() == ')') {
      bracketCount--;
    }
    ++nLength;
    Advance();
  }
  while (bracketCount > 0 && CharsRemain()) {
    if (CurrentChar() == ')') {
      bracketCount--;
    }
    ++nLength;
    Advance();
  }
  return nLength;
}
