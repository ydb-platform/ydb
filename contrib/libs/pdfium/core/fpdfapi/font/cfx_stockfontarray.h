// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_FONT_CFX_STOCKFONTARRAY_H_
#define CORE_FPDFAPI_FONT_CFX_STOCKFONTARRAY_H_

#include <array>

#include "core/fxcrt/retain_ptr.h"
#include "core/fxge/cfx_fontmapper.h"

class CPDF_Font;

class CFX_StockFontArray {
 public:
  CFX_StockFontArray();
  ~CFX_StockFontArray();

  RetainPtr<CPDF_Font> GetFont(CFX_FontMapper::StandardFont index) const;
  void SetFont(CFX_FontMapper::StandardFont index, RetainPtr<CPDF_Font> pFont);

 private:
  std::array<RetainPtr<CPDF_Font>, 14> m_StockFonts;
};

#endif  // CORE_FPDFAPI_FONT_CFX_STOCKFONTARRAY_H_
