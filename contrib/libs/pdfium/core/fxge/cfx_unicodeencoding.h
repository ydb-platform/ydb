// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXGE_CFX_UNICODEENCODING_H_
#define CORE_FXGE_CFX_UNICODEENCODING_H_

#include <stdint.h>

#include "core/fxcrt/unowned_ptr.h"

class CFX_Font;

class CFX_UnicodeEncoding {
 public:
  explicit CFX_UnicodeEncoding(const CFX_Font* pFont);
  virtual ~CFX_UnicodeEncoding();

  virtual uint32_t GlyphFromCharCode(uint32_t charcode);

 protected:
  UnownedPtr<const CFX_Font> const m_pFont;
};

#endif  // CORE_FXGE_CFX_UNICODEENCODING_H_
