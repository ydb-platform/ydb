// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcrt/css/cfx_cssoutputtextbuf.h"

CFX_CSSOutputTextBuf::CFX_CSSOutputTextBuf() {
  m_Buffer.reserve(32);
}

CFX_CSSOutputTextBuf::~CFX_CSSOutputTextBuf() = default;

void CFX_CSSOutputTextBuf::AppendCharIfNotLeadingBlank(wchar_t wch) {
  if (m_Buffer.empty() && wch <= ' ')
    return;

  m_Buffer.push_back(wch);
}

WideStringView CFX_CSSOutputTextBuf::GetTrailingBlankTrimmedString() const {
  WideStringView result(m_Buffer);
  while (!result.IsEmpty() && result.Back() <= ' ')
    result = result.First(result.GetLength() - 1);

  return result;
}
