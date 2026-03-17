// Copyright 2019 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXGE_CFX_GLYPHBITMAP_H_
#define CORE_FXGE_CFX_GLYPHBITMAP_H_

#include "core/fxcrt/retain_ptr.h"

class CFX_DIBitmap;

class CFX_GlyphBitmap {
 public:
  CFX_GlyphBitmap(int left, int top);
  ~CFX_GlyphBitmap();

  CFX_GlyphBitmap(const CFX_GlyphBitmap&) = delete;
  CFX_GlyphBitmap& operator=(const CFX_GlyphBitmap&) = delete;

  const RetainPtr<CFX_DIBitmap>& GetBitmap() const { return m_pBitmap; }
  int left() const { return m_Left; }
  int top() const { return m_Top; }

 private:
  const int m_Left;
  const int m_Top;
  RetainPtr<CFX_DIBitmap> m_pBitmap;
};

#endif  // CORE_FXGE_CFX_GLYPHBITMAP_H_
