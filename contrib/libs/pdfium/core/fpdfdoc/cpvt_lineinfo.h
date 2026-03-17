// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFDOC_CPVT_LINEINFO_H_
#define CORE_FPDFDOC_CPVT_LINEINFO_H_

#include <stdint.h>

struct CPVT_LineInfo {
  int32_t nTotalWord = 0;
  int32_t nBeginWordIndex = -1;
  int32_t nEndWordIndex = -1;
  float fLineX = 0.0f;
  float fLineY = 0.0f;
  float fLineWidth = 0.0f;
  float fLineAscent = 0.0f;
  float fLineDescent = 0.0f;
};

#endif  // CORE_FPDFDOC_CPVT_LINEINFO_H_
