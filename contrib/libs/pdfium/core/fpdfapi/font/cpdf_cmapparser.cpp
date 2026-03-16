// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/font/cpdf_cmapparser.h"

#include <ctype.h>

#include <array>
#include <iterator>

#include "core/fpdfapi/cmaps/fpdf_cmaps.h"
#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_simple_parser.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/fx_extension.h"
#include "core/fxcrt/fx_safe_types.h"

namespace {

ByteStringView CMap_GetString(ByteStringView word) {
  if (word.GetLength() <= 2)
    return ByteStringView();
  return word.Last(word.GetLength() - 2);
}

}  // namespace

CPDF_CMapParser::CPDF_CMapParser(CPDF_CMap* pCMap) : m_pCMap(pCMap) {}

CPDF_CMapParser::~CPDF_CMapParser() {
  m_pCMap->SetAdditionalMappings(std::move(m_AdditionalCharcodeToCIDMappings));
  m_pCMap->SetMixedFourByteLeadingRanges(std::move(m_Ranges));
}

void CPDF_CMapParser::ParseWord(ByteStringView word) {
  DCHECK(!word.IsEmpty());

  if (word == "begincidchar") {
    m_Status = kProcessingCidChar;
    m_CodeSeq = 0;
  } else if (word == "begincidrange") {
    m_Status = kProcessingCidRange;
    m_CodeSeq = 0;
  } else if (word == "endcidrange" || word == "endcidchar") {
    m_Status = kStart;
  } else if (word == "/WMode") {
    m_Status = kProcessingWMode;
  } else if (word == "/Registry") {
    m_Status = kProcessingRegistry;
  } else if (word == "/Ordering") {
    m_Status = kProcessingOrdering;
  } else if (word == "/Supplement") {
    m_Status = kProcessingSupplement;
  } else if (word == "begincodespacerange") {
    m_Status = kProcessingCodeSpaceRange;
    m_CodeSeq = 0;
  } else if (word == "usecmap") {
  } else if (m_Status == kProcessingCidChar) {
    HandleCid(word);
  } else if (m_Status == kProcessingCidRange) {
    HandleCid(word);
  } else if (m_Status == kProcessingRegistry) {
    m_Status = kStart;
  } else if (m_Status == kProcessingOrdering) {
    m_pCMap->SetCharset(CharsetFromOrdering(CMap_GetString(word)));
    m_Status = kStart;
  } else if (m_Status == kProcessingSupplement) {
    m_Status = kStart;
  } else if (m_Status == kProcessingWMode) {
    m_pCMap->SetVertical(GetCode(word) != 0);
    m_Status = kStart;
  } else if (m_Status == kProcessingCodeSpaceRange) {
    HandleCodeSpaceRange(word);
  }
  m_LastWord = word;
}

void CPDF_CMapParser::HandleCid(ByteStringView word) {
  DCHECK(m_Status == kProcessingCidChar || m_Status == kProcessingCidRange);
  bool bChar = m_Status == kProcessingCidChar;

  m_CodePoints[m_CodeSeq] = GetCode(word);
  m_CodeSeq++;
  int nRequiredCodePoints = bChar ? 2 : 3;
  if (m_CodeSeq < nRequiredCodePoints)
    return;

  uint32_t StartCode = m_CodePoints[0];
  uint32_t EndCode;
  uint16_t StartCID;
  if (bChar) {
    EndCode = StartCode;
    StartCID = static_cast<uint16_t>(m_CodePoints[1]);
  } else {
    EndCode = m_CodePoints[1];
    StartCID = static_cast<uint16_t>(m_CodePoints[2]);
  }
  if (EndCode < CPDF_CMap::kDirectMapTableSize) {
    m_pCMap->SetDirectCharcodeToCIDTableRange(StartCode, EndCode, StartCID);
  } else {
    m_AdditionalCharcodeToCIDMappings.push_back({StartCode, EndCode, StartCID});
  }
  m_CodeSeq = 0;
}

void CPDF_CMapParser::HandleCodeSpaceRange(ByteStringView word) {
  if (word != "endcodespacerange") {
    if (word.IsEmpty() || word[0] != '<')
      return;

    if (m_CodeSeq % 2) {
      std::optional<CPDF_CMap::CodeRange> range =
          GetCodeRange(m_LastWord.AsStringView(), word);
      if (range.has_value())
        m_PendingRanges.push_back(range.value());
    }
    m_CodeSeq++;
    return;
  }

  size_t nSegs = m_Ranges.size() + m_PendingRanges.size();
  if (nSegs == 1) {
    const auto& first_range =
        !m_Ranges.empty() ? m_Ranges[0] : m_PendingRanges[0];
    m_pCMap->SetCodingScheme(first_range.m_CharSize == 2 ? CPDF_CMap::TwoBytes
                                                         : CPDF_CMap::OneByte);
  } else if (nSegs > 1) {
    m_pCMap->SetCodingScheme(CPDF_CMap::MixedFourBytes);
    m_Ranges.reserve(nSegs);
    std::move(m_PendingRanges.begin(), m_PendingRanges.end(),
              std::back_inserter(m_Ranges));
    m_PendingRanges.clear();
  }
  m_Status = kStart;
}

// static
uint32_t CPDF_CMapParser::GetCode(ByteStringView word) {
  if (word.IsEmpty())
    return 0;

  FX_SAFE_UINT32 num = 0;
  if (word[0] == '<') {
    for (size_t i = 1; i < word.GetLength() && isxdigit(word[i]); ++i) {
      num = num * 16 + FXSYS_HexCharToInt(word[i]);
      if (!num.IsValid())
        return 0;
    }
    return num.ValueOrDie();
  }

  for (size_t i = 0; i < word.GetLength() && isdigit(word[i]); ++i) {
    num = num * 10 + FXSYS_DecimalCharToInt(static_cast<wchar_t>(word[i]));
    if (!num.IsValid())
      return 0;
  }
  return num.ValueOrDie();
}

// static
std::optional<CPDF_CMap::CodeRange> CPDF_CMapParser::GetCodeRange(
    ByteStringView first,
    ByteStringView second) {
  if (first.IsEmpty() || first[0] != '<')
    return std::nullopt;

  size_t i;
  for (i = 1; i < first.GetLength(); ++i) {
    if (first[i] == '>')
      break;
  }
  size_t char_size = (i - 1) / 2;
  if (char_size > 4)
    return std::nullopt;

  CPDF_CMap::CodeRange range;
  range.m_CharSize = char_size;
  for (i = 0; i < range.m_CharSize; ++i) {
    uint8_t digit1 = first[i * 2 + 1];
    uint8_t digit2 = first[i * 2 + 2];
    range.m_Lower[i] =
        FXSYS_HexCharToInt(digit1) * 16 + FXSYS_HexCharToInt(digit2);
  }

  size_t size = second.GetLength();
  for (i = 0; i < range.m_CharSize; ++i) {
    size_t i1 = i * 2 + 1;
    size_t i2 = i1 + 1;
    uint8_t digit1 = i1 < size ? second[i1] : '0';
    uint8_t digit2 = i2 < size ? second[i2] : '0';
    range.m_Upper[i] =
        FXSYS_HexCharToInt(digit1) * 16 + FXSYS_HexCharToInt(digit2);
  }
  return range;
}

// static
CIDSet CPDF_CMapParser::CharsetFromOrdering(ByteStringView ordering) {
  static const std::array<const char*, CIDSET_NUM_SETS> kCharsetNames = {
      {nullptr, "GB1", "CNS1", "Japan1", "Korea1", "UCS"}};

  for (size_t charset = 1; charset < std::size(kCharsetNames); ++charset) {
    if (ordering == kCharsetNames[charset])
      return static_cast<CIDSet>(charset);
  }
  return CIDSET_UNKNOWN;
}
