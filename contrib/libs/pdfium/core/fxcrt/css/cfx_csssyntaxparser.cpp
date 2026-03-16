// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcrt/css/cfx_csssyntaxparser.h"

#include "core/fxcrt/css/cfx_cssdata.h"
#include "core/fxcrt/css/cfx_cssdeclaration.h"
#include "core/fxcrt/fx_codepage.h"
#include "core/fxcrt/fx_extension.h"

namespace {

bool IsSelectorStart(wchar_t wch) {
  return wch == '.' || wch == '#' || wch == '*' ||
         (isascii(wch) && isalpha(wch));
}

}  // namespace

CFX_CSSSyntaxParser::CFX_CSSSyntaxParser(WideStringView str) : m_Input(str) {}

CFX_CSSSyntaxParser::~CFX_CSSSyntaxParser() = default;

void CFX_CSSSyntaxParser::SetParseOnlyDeclarations() {
  m_eMode = Mode::kPropertyName;
}

CFX_CSSSyntaxParser::Status CFX_CSSSyntaxParser::DoSyntaxParse() {
  m_Output.Clear();
  if (m_bHasError)
    return Status::kError;

  while (!m_Input.IsEOF()) {
    wchar_t wch = m_Input.GetChar();
    switch (m_eMode) {
      case Mode::kRuleSet:
        switch (wch) {
          case '}':
            m_bHasError = true;
            return Status::kError;
          case '/':
            if (m_Input.GetNextChar() == '*') {
              SaveMode(Mode::kRuleSet);
              m_eMode = Mode::kComment;
              break;
            }
            [[fallthrough]];
          default:
            if (wch <= ' ') {
              m_Input.MoveNext();
            } else if (IsSelectorStart(wch)) {
              m_eMode = Mode::kSelector;
              return Status::kStyleRule;
            } else {
              m_bHasError = true;
              return Status::kError;
            }
            break;
        }
        break;
      case Mode::kSelector:
        switch (wch) {
          case ',':
            m_Input.MoveNext();
            if (!m_Output.IsEmpty())
              return Status::kSelector;
            break;
          case '{':
            if (!m_Output.IsEmpty())
              return Status::kSelector;
            m_Input.MoveNext();
            SaveMode(Mode::kRuleSet);  // Back to validate ruleset again.
            m_eMode = Mode::kPropertyName;
            return Status::kDeclOpen;
          case '/':
            if (m_Input.GetNextChar() == '*') {
              SaveMode(Mode::kSelector);
              m_eMode = Mode::kComment;
              if (!m_Output.IsEmpty())
                return Status::kSelector;
              break;
            }
            [[fallthrough]];
          default:
            m_Output.AppendCharIfNotLeadingBlank(wch);
            m_Input.MoveNext();
            break;
        }
        break;
      case Mode::kPropertyName:
        switch (wch) {
          case ':':
            m_Input.MoveNext();
            m_eMode = Mode::kPropertyValue;
            return Status::kPropertyName;
          case '}':
            m_Input.MoveNext();
            if (!RestoreMode())
              return Status::kError;

            return Status::kDeclClose;
          case '/':
            if (m_Input.GetNextChar() == '*') {
              SaveMode(Mode::kPropertyName);
              m_eMode = Mode::kComment;
              if (!m_Output.IsEmpty())
                return Status::kPropertyName;
              break;
            }
            [[fallthrough]];
          default:
            m_Output.AppendCharIfNotLeadingBlank(wch);
            m_Input.MoveNext();
            break;
        }
        break;
      case Mode::kPropertyValue:
        switch (wch) {
          case ';':
            m_Input.MoveNext();
            [[fallthrough]];
          case '}':
            m_eMode = Mode::kPropertyName;
            return Status::kPropertyValue;
          case '/':
            if (m_Input.GetNextChar() == '*') {
              SaveMode(Mode::kPropertyValue);
              m_eMode = Mode::kComment;
              if (!m_Output.IsEmpty())
                return Status::kPropertyValue;
              break;
            }
            [[fallthrough]];
          default:
            m_Output.AppendCharIfNotLeadingBlank(wch);
            m_Input.MoveNext();
            break;
        }
        break;
      case Mode::kComment:
        if (wch == '*' && m_Input.GetNextChar() == '/') {
          if (!RestoreMode())
            return Status::kError;
          m_Input.MoveNext();
        }
        m_Input.MoveNext();
        break;
    }
  }
  if (m_eMode == Mode::kPropertyValue && !m_Output.IsEmpty())
    return Status::kPropertyValue;

  return Status::kEOS;
}

void CFX_CSSSyntaxParser::SaveMode(Mode mode) {
  m_ModeStack.push(mode);
}

bool CFX_CSSSyntaxParser::RestoreMode() {
  if (m_ModeStack.empty()) {
    m_bHasError = true;
    return false;
  }
  m_eMode = m_ModeStack.top();
  m_ModeStack.pop();
  return true;
}

WideStringView CFX_CSSSyntaxParser::GetCurrentString() const {
  return m_Output.GetTrailingBlankTrimmedString();
}
