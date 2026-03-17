// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/font/cfx_stockfontarray.h"

#include <iterator>
#include <utility>

#include "core/fpdfapi/font/cpdf_font.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fxcrt/check_op.h"

CFX_StockFontArray::CFX_StockFontArray() = default;

CFX_StockFontArray::~CFX_StockFontArray() {
  for (size_t i = 0; i < std::size(m_StockFonts); ++i) {
    if (m_StockFonts[i]) {
      // Ensure m_StockFonts[i]'s dict is cleared before releasing what
      // may be the last reference to it.
      RetainPtr<CPDF_Dictionary> destroy =
          m_StockFonts[i]->GetMutableFontDict();
      m_StockFonts[i]->ClearFontDict();
    }
  }
}

RetainPtr<CPDF_Font> CFX_StockFontArray::GetFont(
    CFX_FontMapper::StandardFont index) const {
  CHECK_LT(index, std::size(m_StockFonts));
  return m_StockFonts[index];
}

void CFX_StockFontArray::SetFont(CFX_FontMapper::StandardFont index,
                                 RetainPtr<CPDF_Font> pFont) {
  if (index < std::size(m_StockFonts))
    m_StockFonts[index] = std::move(pFont);
}
