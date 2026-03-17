// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFDOC_CPVT_WORDINFO_H_
#define CORE_FPDFDOC_CPVT_WORDINFO_H_

#include <stdint.h>

#include "core/fxcrt/fx_codepage_forward.h"

struct CPVT_WordInfo {
  CPVT_WordInfo();
  CPVT_WordInfo(uint16_t word, FX_Charset charset, int32_t fontIndex);
  CPVT_WordInfo(const CPVT_WordInfo& word);
  ~CPVT_WordInfo();

  CPVT_WordInfo& operator=(const CPVT_WordInfo& word);

  uint16_t Word;
  FX_Charset nCharset;
  float fWordX;
  float fWordY;
  float fWordTail;
  int32_t nFontIndex;
};

#endif  // CORE_FPDFDOC_CPVT_WORDINFO_H_
