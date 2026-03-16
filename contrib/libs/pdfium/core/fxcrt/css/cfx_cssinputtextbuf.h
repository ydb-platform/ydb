// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCRT_CSS_CFX_CSSINPUTTEXTBUF_H_
#define CORE_FXCRT_CSS_CFX_CSSINPUTTEXTBUF_H_

#include "core/fxcrt/widestring.h"

class CFX_CSSInputTextBuf {
 public:
  explicit CFX_CSSInputTextBuf(WideStringView str);
  ~CFX_CSSInputTextBuf();

  bool IsEOF() const { return m_iPos >= m_Buffer.GetLength(); }
  void MoveNext() { m_iPos++; }
  wchar_t GetChar() const { return m_Buffer[m_iPos]; }
  wchar_t GetNextChar() const {
    return m_iPos + 1 < m_Buffer.GetLength() ? m_Buffer[m_iPos + 1] : 0;
  }

 protected:
  const WideStringView m_Buffer;
  size_t m_iPos = 0;
};

#endif  // CORE_FXCRT_CSS_CFX_CSSINPUTTEXTBUF_H_
