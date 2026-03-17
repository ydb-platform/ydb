// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_CMAPS_FPDF_CMAPS_H_
#define CORE_FPDFAPI_CMAPS_FPDF_CMAPS_H_

#include <stdint.h>

#include "core/fxcrt/unowned_ptr_exclusion.h"

namespace fxcmap {

struct DWordCIDMap {
  uint16_t m_HiWord;
  uint16_t m_LoWordLow;
  uint16_t m_LoWordHigh;
  uint16_t m_CID;
};

struct CMap {
  enum class Type : bool { kSingle, kRange };

  UNOWNED_PTR_EXCLUSION const char* m_Name;              // POD struct.
  UNOWNED_PTR_EXCLUSION const uint16_t* m_pWordMap;      // POD struct.
  UNOWNED_PTR_EXCLUSION const DWordCIDMap* m_pDWordMap;  // POD struct.
  uint16_t m_WordCount;
  uint16_t m_DWordCount;
  Type m_WordMapType;
  int8_t m_UseOffset;
};

uint16_t CIDFromCharCode(const CMap* pMap, uint32_t charcode);
uint32_t CharCodeFromCID(const CMap* pMap, uint16_t cid);

}  // namespace fxcmap

#endif  // CORE_FPDFAPI_CMAPS_FPDF_CMAPS_H_
