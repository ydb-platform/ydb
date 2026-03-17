// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_FONT_CPDF_CMAPPARSER_H_
#define CORE_FPDFAPI_FONT_CPDF_CMAPPARSER_H_

#include <array>
#include <optional>
#include <utility>
#include <vector>

#include "core/fpdfapi/font/cpdf_cidfont.h"
#include "core/fpdfapi/font/cpdf_cmap.h"
#include "core/fxcrt/unowned_ptr.h"

class CPDF_CMapParser {
 public:
  explicit CPDF_CMapParser(CPDF_CMap* pCMap);
  ~CPDF_CMapParser();

  void ParseWord(ByteStringView word);

  static CIDSet CharsetFromOrdering(ByteStringView ordering);

 private:
  friend class cpdf_cmapparser_GetCode_Test;
  friend class cpdf_cmapparser_GetCodeRange_Test;

  enum Status {
    kStart,
    kProcessingCidChar,
    kProcessingCidRange,
    kProcessingRegistry,
    kProcessingOrdering,
    kProcessingSupplement,
    kProcessingWMode,
    kProcessingCodeSpaceRange,
  };

  void HandleCid(ByteStringView word);
  void HandleCodeSpaceRange(ByteStringView word);

  static uint32_t GetCode(ByteStringView word);
  static std::optional<CPDF_CMap::CodeRange> GetCodeRange(
      ByteStringView first,
      ByteStringView second);

  Status m_Status = kStart;
  int m_CodeSeq = 0;
  UnownedPtr<CPDF_CMap> const m_pCMap;
  std::vector<CPDF_CMap::CodeRange> m_Ranges;
  std::vector<CPDF_CMap::CodeRange> m_PendingRanges;
  std::vector<CPDF_CMap::CIDRange> m_AdditionalCharcodeToCIDMappings;
  ByteString m_LastWord;
  std::array<uint32_t, 4> m_CodePoints = {};
};

#endif  // CORE_FPDFAPI_FONT_CPDF_CMAPPARSER_H_
