// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCRT_CSS_CFX_CSSOUTPUTTEXTBUF_H_
#define CORE_FXCRT_CSS_CFX_CSSOUTPUTTEXTBUF_H_

#include "core/fxcrt/data_vector.h"
#include "core/fxcrt/widestring.h"

class CFX_CSSOutputTextBuf {
 public:
  CFX_CSSOutputTextBuf();
  ~CFX_CSSOutputTextBuf();

  void Clear() { m_Buffer.clear(); }
  bool IsEmpty() const { return m_Buffer.empty(); }
  void AppendCharIfNotLeadingBlank(wchar_t wch);
  WideStringView GetTrailingBlankTrimmedString() const;

 protected:
  DataVector<wchar_t> m_Buffer;
};

#endif  // CORE_FXCRT_CSS_CFX_CSSOUTPUTTEXTBUF_H_
