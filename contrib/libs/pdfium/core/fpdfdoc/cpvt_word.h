// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFDOC_CPVT_WORD_H_
#define CORE_FPDFDOC_CPVT_WORD_H_

#include <stdint.h>

#include "core/fpdfdoc/cpvt_wordplace.h"
#include "core/fxcrt/fx_codepage.h"

class CPVT_Word {
 public:
  CPVT_Word();

  uint16_t Word;
  FX_Charset nCharset;
  CPVT_WordPlace WordPlace;
  CFX_PointF ptWord;
  float fAscent;
  float fDescent;
  float fWidth;
  int32_t nFontIndex;
  float fFontSize;
};

inline CPVT_Word::CPVT_Word()
    : Word(0),
      nCharset(FX_Charset::kANSI),
      fAscent(0.0f),
      fDescent(0.0f),
      fWidth(0.0f),
      nFontIndex(-1),
      fFontSize(0.0f) {}

#endif  // CORE_FPDFDOC_CPVT_WORD_H_
