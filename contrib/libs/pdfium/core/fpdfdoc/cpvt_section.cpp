// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfdoc/cpvt_section.h"

#include <algorithm>
#include <array>

#include "core/fpdfdoc/cpvt_variabletext.h"
#include "core/fpdfdoc/cpvt_wordinfo.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/stl_util.h"

namespace {

constexpr std::array<const uint8_t, 128> kSpecialChars = {{
    0x00, 0x0C, 0x08, 0x0C, 0x08, 0x00, 0x20, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x08, 0x00,
    0x10, 0x00, 0x00, 0x28, 0x0C, 0x08, 0x00, 0x00, 0x28, 0x28, 0x28, 0x28,
    0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x08, 0x08,
    0x00, 0x00, 0x00, 0x08, 0x00, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
    0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
    0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x0C, 0x00, 0x08, 0x00, 0x00,
    0x00, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
    0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
    0x01, 0x01, 0x01, 0x0C, 0x00, 0x08, 0x00, 0x00,
}};

bool IsLatin(uint16_t word) {
  if (word <= 0x007F) {
    return !!(kSpecialChars[word] & 0x01);
  }
  return ((word >= 0x00C0 && word <= 0x00FF) ||
          (word >= 0x0100 && word <= 0x024F) ||
          (word >= 0x1E00 && word <= 0x1EFF) ||
          (word >= 0x2C60 && word <= 0x2C7F) ||
          (word >= 0xA720 && word <= 0xA7FF) ||
          (word >= 0xFF21 && word <= 0xFF3A) ||
          (word >= 0xFF41 && word <= 0xFF5A));
}

bool IsDigit(uint32_t word) {
  return word >= 0x0030 && word <= 0x0039;
}

bool IsCJK(uint32_t word) {
  if ((word >= 0x1100 && word <= 0x11FF) ||
      (word >= 0x2E80 && word <= 0x2FFF) ||
      (word >= 0x3040 && word <= 0x9FBF) ||
      (word >= 0xAC00 && word <= 0xD7AF) ||
      (word >= 0xF900 && word <= 0xFAFF) ||
      (word >= 0xFE30 && word <= 0xFE4F) ||
      (word >= 0x20000 && word <= 0x2A6DF) ||
      (word >= 0x2F800 && word <= 0x2FA1F)) {
    return true;
  }
  if (word >= 0x3000 && word <= 0x303F) {
    return (
        word == 0x3005 || word == 0x3006 || word == 0x3021 || word == 0x3022 ||
        word == 0x3023 || word == 0x3024 || word == 0x3025 || word == 0x3026 ||
        word == 0x3027 || word == 0x3028 || word == 0x3029 || word == 0x3031 ||
        word == 0x3032 || word == 0x3033 || word == 0x3034 || word == 0x3035);
  }
  return word >= 0xFF66 && word <= 0xFF9D;
}

bool IsPunctuation(uint32_t word) {
  if (word <= 0x007F) {
    return !!(kSpecialChars[word] & 0x08);
  }
  if (word >= 0x0080 && word <= 0x00FF) {
    return (word == 0x0082 || word == 0x0084 || word == 0x0085 ||
            word == 0x0091 || word == 0x0092 || word == 0x0093 ||
            word <= 0x0094 || word == 0x0096 || word == 0x00B4 ||
            word == 0x00B8);
  }
  if (word >= 0x2000 && word <= 0x206F) {
    return (
        word == 0x2010 || word == 0x2011 || word == 0x2012 || word == 0x2013 ||
        word == 0x2018 || word == 0x2019 || word == 0x201A || word == 0x201B ||
        word == 0x201C || word == 0x201D || word == 0x201E || word == 0x201F ||
        word == 0x2032 || word == 0x2033 || word == 0x2034 || word == 0x2035 ||
        word == 0x2036 || word == 0x2037 || word == 0x203C || word == 0x203D ||
        word == 0x203E || word == 0x2044);
  }
  if (word >= 0x3000 && word <= 0x303F) {
    return (
        word == 0x3001 || word == 0x3002 || word == 0x3003 || word == 0x3005 ||
        word == 0x3009 || word == 0x300A || word == 0x300B || word == 0x300C ||
        word == 0x300D || word == 0x300F || word == 0x300E || word == 0x3010 ||
        word == 0x3011 || word == 0x3014 || word == 0x3015 || word == 0x3016 ||
        word == 0x3017 || word == 0x3018 || word == 0x3019 || word == 0x301A ||
        word == 0x301B || word == 0x301D || word == 0x301E || word == 0x301F);
  }
  if (word >= 0xFE50 && word <= 0xFE6F) {
    return (word >= 0xFE50 && word <= 0xFE5E) || word == 0xFE63;
  }
  if (word >= 0xFF00 && word <= 0xFFEF) {
    return (
        word == 0xFF01 || word == 0xFF02 || word == 0xFF07 || word == 0xFF08 ||
        word == 0xFF09 || word == 0xFF0C || word == 0xFF0E || word == 0xFF0F ||
        word == 0xFF1A || word == 0xFF1B || word == 0xFF1F || word == 0xFF3B ||
        word == 0xFF3D || word == 0xFF40 || word == 0xFF5B || word == 0xFF5C ||
        word == 0xFF5D || word == 0xFF61 || word == 0xFF62 || word == 0xFF63 ||
        word == 0xFF64 || word == 0xFF65 || word == 0xFF9E || word == 0xFF9F);
  }
  return false;
}

bool IsConnectiveSymbol(uint32_t word) {
  return word <= 0x007F && (kSpecialChars[word] & 0x20);
}

bool IsOpenStylePunctuation(uint32_t word) {
  if (word <= 0x007F) {
    return !!(kSpecialChars[word] & 0x04);
  }
  return (word == 0x300A || word == 0x300C || word == 0x300E ||
          word == 0x3010 || word == 0x3014 || word == 0x3016 ||
          word == 0x3018 || word == 0x301A || word == 0xFF08 ||
          word == 0xFF3B || word == 0xFF5B || word == 0xFF62);
}

bool IsCurrencySymbol(uint16_t word) {
  return (word == 0x0024 || word == 0x0080 || word == 0x00A2 ||
          word == 0x00A3 || word == 0x00A4 || word == 0x00A5 ||
          (word >= 0x20A0 && word <= 0x20CF) || word == 0xFE69 ||
          word == 0xFF04 || word == 0xFFE0 || word == 0xFFE1 ||
          word == 0xFFE5 || word == 0xFFE6);
}

bool IsPrefixSymbol(uint16_t word) {
  return IsCurrencySymbol(word) || word == 0x2116;
}

bool IsSpace(uint16_t word) {
  return word == 0x0020 || word == 0x3000;
}

bool NeedDivision(uint16_t prevWord, uint16_t curWord) {
  if ((IsLatin(prevWord) || IsDigit(prevWord)) &&
      (IsLatin(curWord) || IsDigit(curWord))) {
    return false;
  }
  if (IsSpace(curWord) || IsPunctuation(curWord)) {
    return false;
  }
  if (IsConnectiveSymbol(prevWord) || IsConnectiveSymbol(curWord)) {
    return false;
  }
  if (IsSpace(prevWord) || IsPunctuation(prevWord)) {
    return true;
  }
  if (IsPrefixSymbol(prevWord)) {
    return false;
  }
  if (IsPrefixSymbol(curWord) || IsCJK(curWord)) {
    return true;
  }
  if (IsCJK(prevWord)) {
    return true;
  }
  return false;
}

}  // namespace

CPVT_Section::Line::Line(const CPVT_LineInfo& lineinfo)
    : m_LineInfo(lineinfo) {}

CPVT_Section::Line::~Line() = default;

CPVT_WordPlace CPVT_Section::Line::GetBeginWordPlace() const {
  return CPVT_WordPlace(m_LinePlace.nSecIndex, m_LinePlace.nLineIndex, -1);
}

CPVT_WordPlace CPVT_Section::Line::GetEndWordPlace() const {
  return CPVT_WordPlace(m_LinePlace.nSecIndex, m_LinePlace.nLineIndex,
                        m_LineInfo.nEndWordIndex);
}

CPVT_WordPlace CPVT_Section::Line::GetPrevWordPlace(
    const CPVT_WordPlace& place) const {
  if (place.nWordIndex > m_LineInfo.nEndWordIndex) {
    return CPVT_WordPlace(place.nSecIndex, place.nLineIndex,
                          m_LineInfo.nEndWordIndex);
  }
  return CPVT_WordPlace(place.nSecIndex, place.nLineIndex,
                        place.nWordIndex - 1);
}

CPVT_WordPlace CPVT_Section::Line::GetNextWordPlace(
    const CPVT_WordPlace& place) const {
  if (place.nWordIndex < m_LineInfo.nBeginWordIndex) {
    return CPVT_WordPlace(place.nSecIndex, place.nLineIndex,
                          m_LineInfo.nBeginWordIndex);
  }
  return CPVT_WordPlace(place.nSecIndex, place.nLineIndex,
                        place.nWordIndex + 1);
}

CPVT_Section::CPVT_Section(CPVT_VariableText* pVT) : m_pVT(pVT) {
  DCHECK(m_pVT);
}

CPVT_Section::~CPVT_Section() = default;

void CPVT_Section::ResetLinePlace() {
  int32_t i = 0;
  for (auto& pLine : m_LineArray) {
    pLine->m_LinePlace = CPVT_WordPlace(m_SecPlace.nSecIndex, i, -1);
    ++i;
  }
}

CPVT_WordPlace CPVT_Section::AddWord(const CPVT_WordPlace& place,
                                     const CPVT_WordInfo& wordinfo) {
  int32_t nWordIndex = std::clamp(place.nWordIndex, 0,
                                  fxcrt::CollectionSize<int32_t>(m_WordArray));
  m_WordArray.insert(m_WordArray.begin() + nWordIndex,
                     std::make_unique<CPVT_WordInfo>(wordinfo));
  return place;
}

CPVT_WordPlace CPVT_Section::AddLine(const CPVT_LineInfo& lineinfo) {
  m_LineArray.push_back(std::make_unique<Line>(lineinfo));
  return CPVT_WordPlace(m_SecPlace.nSecIndex,
                        fxcrt::CollectionSize<int32_t>(m_LineArray) - 1, -1);
}

CPVT_FloatRect CPVT_Section::Rearrange() {
  if (m_pVT->GetCharArray() > 0)
    return RearrangeCharArray();
  return RearrangeTypeset();
}

CFX_SizeF CPVT_Section::GetSectionSize(float fFontSize) {
  CPVT_FloatRect result = SplitLines(/*bTypeset=*/false, fFontSize);
  return CFX_SizeF(result.Width(), result.Height());
}

CPVT_WordPlace CPVT_Section::GetBeginWordPlace() const {
  if (m_LineArray.empty())
    return m_SecPlace;
  return m_LineArray.front()->GetBeginWordPlace();
}

CPVT_WordPlace CPVT_Section::GetEndWordPlace() const {
  if (m_LineArray.empty())
    return m_SecPlace;
  return m_LineArray.back()->GetEndWordPlace();
}

CPVT_WordPlace CPVT_Section::GetPrevWordPlace(
    const CPVT_WordPlace& place) const {
  if (place.nLineIndex < 0)
    return GetBeginWordPlace();

  if (place.nLineIndex >= fxcrt::CollectionSize<int32_t>(m_LineArray))
    return GetEndWordPlace();

  Line* pLine = m_LineArray[place.nLineIndex].get();
  if (place.nWordIndex == pLine->m_LineInfo.nBeginWordIndex)
    return CPVT_WordPlace(place.nSecIndex, place.nLineIndex, -1);

  if (place.nWordIndex >= pLine->m_LineInfo.nBeginWordIndex)
    return pLine->GetPrevWordPlace(place);

  if (!fxcrt::IndexInBounds(m_LineArray, place.nLineIndex - 1))
    return place;

  return m_LineArray[place.nLineIndex - 1]->GetEndWordPlace();
}

CPVT_WordPlace CPVT_Section::GetNextWordPlace(
    const CPVT_WordPlace& place) const {
  if (place.nLineIndex < 0)
    return GetBeginWordPlace();

  if (place.nLineIndex >= fxcrt::CollectionSize<int32_t>(m_LineArray))
    return GetEndWordPlace();

  Line* pLine = m_LineArray[place.nLineIndex].get();
  if (place.nWordIndex < pLine->m_LineInfo.nEndWordIndex)
    return pLine->GetNextWordPlace(place);

  if (!fxcrt::IndexInBounds(m_LineArray, place.nLineIndex + 1))
    return place;

  return m_LineArray[place.nLineIndex + 1]->GetBeginWordPlace();
}

void CPVT_Section::UpdateWordPlace(CPVT_WordPlace& place) const {
  int32_t nLeft = 0;
  int32_t nRight = fxcrt::CollectionSize<int32_t>(m_LineArray) - 1;
  int32_t nMid = (nLeft + nRight) / 2;
  while (nLeft <= nRight) {
    Line* pLine = m_LineArray[nMid].get();
    if (place.nWordIndex < pLine->m_LineInfo.nBeginWordIndex) {
      nRight = nMid - 1;
      nMid = (nLeft + nRight) / 2;
    } else if (place.nWordIndex > pLine->m_LineInfo.nEndWordIndex) {
      nLeft = nMid + 1;
      nMid = (nLeft + nRight) / 2;
    } else {
      place.nLineIndex = nMid;
      return;
    }
  }
}

CPVT_WordPlace CPVT_Section::SearchWordPlace(const CFX_PointF& point) const {
  CPVT_WordPlace place = GetBeginWordPlace();
  bool bUp = true;
  bool bDown = true;
  int32_t nLeft = 0;
  int32_t nRight = fxcrt::CollectionSize<int32_t>(m_LineArray) - 1;
  int32_t nMid = fxcrt::CollectionSize<int32_t>(m_LineArray) / 2;
  while (nLeft <= nRight) {
    Line* pLine = m_LineArray[nMid].get();
    float fTop = pLine->m_LineInfo.fLineY - pLine->m_LineInfo.fLineAscent -
                 m_pVT->GetLineLeading();
    float fBottom = pLine->m_LineInfo.fLineY - pLine->m_LineInfo.fLineDescent;
    if (FXSYS_IsFloatBigger(point.y, fTop))
      bUp = false;
    if (FXSYS_IsFloatSmaller(point.y, fBottom))
      bDown = false;
    if (FXSYS_IsFloatSmaller(point.y, fTop)) {
      nRight = nMid - 1;
      nMid = (nLeft + nRight) / 2;
      continue;
    }
    if (FXSYS_IsFloatBigger(point.y, fBottom)) {
      nLeft = nMid + 1;
      nMid = (nLeft + nRight) / 2;
      continue;
    }
    place = SearchWordPlace(
        point.x,
        CPVT_WordRange(pLine->GetNextWordPlace(pLine->GetBeginWordPlace()),
                       pLine->GetEndWordPlace()));
    place.nLineIndex = nMid;
    return place;
  }
  if (bUp)
    place = GetBeginWordPlace();
  if (bDown)
    place = GetEndWordPlace();
  return place;
}

CPVT_WordPlace CPVT_Section::SearchWordPlace(
    float fx,
    const CPVT_WordPlace& lineplace) const {
  if (!fxcrt::IndexInBounds(m_LineArray, lineplace.nLineIndex))
    return GetBeginWordPlace();

  Line* pLine = m_LineArray[lineplace.nLineIndex].get();
  return SearchWordPlace(
      fx - m_Rect.left,
      CPVT_WordRange(pLine->GetNextWordPlace(pLine->GetBeginWordPlace()),
                     pLine->GetEndWordPlace()));
}

CPVT_WordPlace CPVT_Section::SearchWordPlace(
    float fx,
    const CPVT_WordRange& range) const {
  CPVT_WordPlace wordplace = range.BeginPos;
  wordplace.nWordIndex = -1;

  int32_t nLeft = range.BeginPos.nWordIndex;
  int32_t nRight = range.EndPos.nWordIndex + 1;
  int32_t nMid = (nLeft + nRight) / 2;
  while (nLeft < nRight) {
    if (nMid == nLeft)
      break;
    if (nMid == nRight) {
      nMid--;
      break;
    }
    if (!fxcrt::IndexInBounds(m_WordArray, nMid))
      break;
    CPVT_WordInfo* pWord = m_WordArray[nMid].get();
    if (fx > pWord->fWordX + m_pVT->GetWordWidth(*pWord) * 0.5f) {
      nLeft = nMid;
      nMid = (nLeft + nRight) / 2;
      continue;
    }
    nRight = nMid;
    nMid = (nLeft + nRight) / 2;
  }
  if (fxcrt::IndexInBounds(m_WordArray, nMid)) {
    CPVT_WordInfo* pWord = m_WordArray[nMid].get();
    if (fx > pWord->fWordX + m_pVT->GetWordWidth(*pWord) * 0.5f)
      wordplace.nWordIndex = nMid;
  }
  return wordplace;
}

int32_t CPVT_Section::GetLineArraySize() const {
  return fxcrt::CollectionSize<int32_t>(m_LineArray);
}

const CPVT_Section::Line* CPVT_Section::GetLineFromArray(int32_t index) const {
  if (!fxcrt::IndexInBounds(m_LineArray, index))
    return nullptr;

  return m_LineArray[index].get();
}

int32_t CPVT_Section::GetWordArraySize() const {
  return fxcrt::CollectionSize<int32_t>(m_WordArray);
}

const CPVT_WordInfo* CPVT_Section::GetWordFromArray(int32_t index) const {
  if (!fxcrt::IndexInBounds(m_WordArray, index))
    return nullptr;

  return m_WordArray[index].get();
}

void CPVT_Section::EraseWordsFrom(int32_t index) {
  if (!fxcrt::IndexInBounds(m_WordArray, index))
    return;

  m_WordArray.erase(m_WordArray.begin() + index, m_WordArray.end());
}

CPVT_FloatRect CPVT_Section::RearrangeCharArray() const {
  if (m_LineArray.empty())
    return CPVT_FloatRect();

  float fNodeWidth = m_pVT->GetPlateWidth() /
                     (m_pVT->GetCharArray() <= 0 ? 1 : m_pVT->GetCharArray());
  float fLineAscent =
      m_pVT->GetFontAscent(m_pVT->GetDefaultFontIndex(), m_pVT->GetFontSize());
  float fLineDescent =
      m_pVT->GetFontDescent(m_pVT->GetDefaultFontIndex(), m_pVT->GetFontSize());
  float x = 0.0f;
  float y = m_pVT->GetLineLeading() + fLineAscent;
  int32_t nStart = 0;
  CPVT_Section::Line* pLine = m_LineArray.front().get();
  switch (m_pVT->GetAlignment()) {
    case 0:
      pLine->m_LineInfo.fLineX = fNodeWidth * 0.5f;
      break;
    case 1:
      nStart = (m_pVT->GetCharArray() -
                fxcrt::CollectionSize<int32_t>(m_WordArray)) /
               2;
      pLine->m_LineInfo.fLineX = fNodeWidth * nStart - fNodeWidth * 0.5f;
      break;
    case 2:
      nStart =
          m_pVT->GetCharArray() - fxcrt::CollectionSize<int32_t>(m_WordArray);
      pLine->m_LineInfo.fLineX = fNodeWidth * nStart - fNodeWidth * 0.5f;
      break;
  }
  for (int32_t w = 0, sz = fxcrt::CollectionSize<int32_t>(m_WordArray); w < sz;
       w++) {
    if (w >= m_pVT->GetCharArray())
      break;

    float fNextWidth = 0;
    if (fxcrt::IndexInBounds(m_WordArray, w + 1)) {
      CPVT_WordInfo* pNextWord = m_WordArray[w + 1].get();
      pNextWord->fWordTail = 0;
      fNextWidth = m_pVT->GetWordWidth(*pNextWord);
    }
    CPVT_WordInfo* pWord = m_WordArray[w].get();
    pWord->fWordTail = 0;
    float fWordWidth = m_pVT->GetWordWidth(*pWord);
    float fWordAscent = m_pVT->GetWordAscent(*pWord);
    float fWordDescent = m_pVT->GetWordDescent(*pWord);
    x = (float)(fNodeWidth * (w + nStart + 0.5) - fWordWidth * 0.5f);
    pWord->fWordX = x;
    pWord->fWordY = y;
    if (w == 0) {
      pLine->m_LineInfo.fLineX = x;
    }
    if (w != fxcrt::CollectionSize<int32_t>(m_WordArray) - 1) {
      pWord->fWordTail = (fNodeWidth - (fWordWidth + fNextWidth) * 0.5f > 0
                              ? fNodeWidth - (fWordWidth + fNextWidth) * 0.5f
                              : 0);
    } else {
      pWord->fWordTail = 0;
    }
    x += fWordWidth;
    fLineAscent = std::max(fLineAscent, fWordAscent);
    fLineDescent = std::min(fLineDescent, fWordDescent);
  }
  pLine->m_LineInfo.nBeginWordIndex = 0;
  pLine->m_LineInfo.nEndWordIndex =
      fxcrt::CollectionSize<int32_t>(m_WordArray) - 1;
  pLine->m_LineInfo.fLineY = y;
  pLine->m_LineInfo.fLineWidth = x - pLine->m_LineInfo.fLineX;
  pLine->m_LineInfo.fLineAscent = fLineAscent;
  pLine->m_LineInfo.fLineDescent = fLineDescent;
  return CPVT_FloatRect(0, 0, x, y - fLineDescent);
}

CPVT_FloatRect CPVT_Section::RearrangeTypeset() {
  m_LineArray.clear();
  return OutputLines(SplitLines(/*bTypeset=*/true, /*fFontSize=*/0.0f));
}

CPVT_FloatRect CPVT_Section::SplitLines(bool bTypeset, float fFontSize) {
  CPVT_LineInfo line;
  if (m_WordArray.empty()) {
    float fLineAscent;
    float fLineDescent;
    if (bTypeset) {
      fLineAscent = m_pVT->GetLineAscent();
      fLineDescent = m_pVT->GetLineDescent();
      line.nBeginWordIndex = -1;
      line.nEndWordIndex = -1;
      line.nTotalWord = 0;
      line.fLineWidth = 0;
      line.fLineAscent = fLineAscent;
      line.fLineDescent = fLineDescent;
      AddLine(line);
    } else {
      fLineAscent =
          m_pVT->GetFontAscent(m_pVT->GetDefaultFontIndex(), fFontSize);
      fLineDescent =
          m_pVT->GetFontDescent(m_pVT->GetDefaultFontIndex(), fFontSize);
    }
    float fMaxY = m_pVT->GetLineLeading() + fLineAscent - fLineDescent;
    return CPVT_FloatRect(0, 0, 0, fMaxY);
  }

  int32_t nLineHead = 0;
  int32_t nLineTail = 0;
  float fMaxX = 0.0f;
  float fMaxY = 0.0f;
  float fLineWidth = 0.0f;
  float fBackupLineWidth = 0.0f;
  float fLineAscent = 0.0f;
  float fBackupLineAscent = 0.0f;
  float fLineDescent = 0.0f;
  float fBackupLineDescent = 0.0f;
  int32_t nWordStartPos = 0;
  bool bFullWord = false;
  int32_t nLineFullWordIndex = 0;
  int32_t nCharIndex = 0;
  float fWordWidth = 0;
  float fTypesetWidth =
      std::max(m_pVT->GetPlateWidth() - m_pVT->GetLineIndent(), 0.0f);
  int32_t nTotalWords = fxcrt::CollectionSize<int32_t>(m_WordArray);
  bool bOpened = false;
  int32_t i = 0;
  while (i < nTotalWords) {
    CPVT_WordInfo* pWord = m_WordArray[i].get();
    CPVT_WordInfo* pOldWord = pWord;
    if (i > 0) {
      pOldWord = m_WordArray[i - 1].get();
    }
    if (pWord) {
      if (bTypeset) {
        fLineAscent = std::max(fLineAscent, m_pVT->GetWordAscent(*pWord));
        fLineDescent = std::min(fLineDescent, m_pVT->GetWordDescent(*pWord));
        fWordWidth = m_pVT->GetWordWidth(*pWord);
      } else {
        fLineAscent =
            std::max(fLineAscent, m_pVT->GetWordAscent(*pWord, fFontSize));
        fLineDescent =
            std::min(fLineDescent, m_pVT->GetWordDescent(*pWord, fFontSize));
        fWordWidth = m_pVT->GetWordWidth(pWord->nFontIndex, pWord->Word,
                                         m_pVT->GetSubWord(), fFontSize,
                                         pWord->fWordTail);
      }
      if (!bOpened) {
        if (IsOpenStylePunctuation(pWord->Word)) {
          bOpened = true;
          bFullWord = true;
        } else if (pOldWord) {
          if (NeedDivision(pOldWord->Word, pWord->Word)) {
            bFullWord = true;
          }
        }
      } else {
        if (!IsSpace(pWord->Word) && !IsOpenStylePunctuation(pWord->Word)) {
          bOpened = false;
        }
      }
      if (bFullWord) {
        bFullWord = false;
        if (nCharIndex > 0) {
          nLineFullWordIndex++;
        }
        nWordStartPos = i;
        fBackupLineWidth = fLineWidth;
        fBackupLineAscent = fLineAscent;
        fBackupLineDescent = fLineDescent;
      }
      nCharIndex++;
    }
    if (m_pVT->IsAutoReturn() && fTypesetWidth > 0 &&
        fLineWidth + fWordWidth > fTypesetWidth) {
      if (nLineFullWordIndex > 0) {
        i = nWordStartPos;
        fLineWidth = fBackupLineWidth;
        fLineAscent = fBackupLineAscent;
        fLineDescent = fBackupLineDescent;
      }
      if (nCharIndex == 1) {
        fLineWidth = fWordWidth;
        i++;
      }
      nLineTail = i - 1;
      if (bTypeset) {
        line.nBeginWordIndex = nLineHead;
        line.nEndWordIndex = nLineTail;
        line.nTotalWord = nLineTail - nLineHead + 1;
        line.fLineWidth = fLineWidth;
        line.fLineAscent = fLineAscent;
        line.fLineDescent = fLineDescent;
        AddLine(line);
      }
      fMaxY += (fLineAscent + m_pVT->GetLineLeading());
      fMaxY -= fLineDescent;
      fMaxX = std::max(fLineWidth, fMaxX);
      nLineHead = i;
      fLineWidth = 0.0f;
      fLineAscent = 0.0f;
      fLineDescent = 0.0f;
      nCharIndex = 0;
      nLineFullWordIndex = 0;
      bFullWord = false;
    } else {
      fLineWidth += fWordWidth;
      i++;
    }
  }
  if (nLineHead <= nTotalWords - 1) {
    nLineTail = nTotalWords - 1;
    if (bTypeset) {
      line.nBeginWordIndex = nLineHead;
      line.nEndWordIndex = nLineTail;
      line.nTotalWord = nLineTail - nLineHead + 1;
      line.fLineWidth = fLineWidth;
      line.fLineAscent = fLineAscent;
      line.fLineDescent = fLineDescent;
      AddLine(line);
    }
    fMaxY += (fLineAscent + m_pVT->GetLineLeading());
    fMaxY -= fLineDescent;
    fMaxX = std::max(fLineWidth, fMaxX);
  }
  return CPVT_FloatRect(0, 0, fMaxX, fMaxY);
}

CPVT_FloatRect CPVT_Section::OutputLines(const CPVT_FloatRect& rect) const {
  float fMinX;
  float fLineIndent = m_pVT->GetLineIndent();
  float fTypesetWidth = std::max(m_pVT->GetPlateWidth() - fLineIndent, 0.0f);
  switch (m_pVT->GetAlignment()) {
    default:
    case 0:
      fMinX = 0.0f;
      break;
    case 1:
      fMinX = (fTypesetWidth - rect.Width()) * 0.5f;
      break;
    case 2:
      fMinX = fTypesetWidth - rect.Width();
      break;
  }
  float fMaxX = fMinX + rect.Width();
  float fMinY = 0.0f;
  float fMaxY = rect.Height();
  int32_t nTotalLines = fxcrt::CollectionSize<int32_t>(m_LineArray);
  if (nTotalLines > 0) {
    float fPosX = 0.0f;
    float fPosY = 0.0f;
    for (int32_t l = 0; l < nTotalLines; l++) {
      CPVT_Section::Line* pLine = m_LineArray[l].get();
      switch (m_pVT->GetAlignment()) {
        default:
        case 0:
          fPosX = 0;
          break;
        case 1:
          fPosX = (fTypesetWidth - pLine->m_LineInfo.fLineWidth) * 0.5f;
          break;
        case 2:
          fPosX = fTypesetWidth - pLine->m_LineInfo.fLineWidth;
          break;
      }
      fPosX += fLineIndent;
      fPosY += m_pVT->GetLineLeading();
      fPosY += pLine->m_LineInfo.fLineAscent;
      pLine->m_LineInfo.fLineX = fPosX - fMinX;
      pLine->m_LineInfo.fLineY = fPosY - fMinY;
      for (int32_t w = pLine->m_LineInfo.nBeginWordIndex;
           w <= pLine->m_LineInfo.nEndWordIndex; w++) {
        if (fxcrt::IndexInBounds(m_WordArray, w)) {
          CPVT_WordInfo* pWord = m_WordArray[w].get();
          pWord->fWordX = fPosX - fMinX;
          pWord->fWordY = fPosY - fMinY;

          fPosX += m_pVT->GetWordWidth(*pWord);
        }
      }
      fPosY -= pLine->m_LineInfo.fLineDescent;
    }
  }
  return CPVT_FloatRect(fMinX, fMinY, fMaxX, fMaxY);
}

void CPVT_Section::ClearLeftWords(int32_t nWordIndex) {
  for (int32_t i = nWordIndex; i >= 0; i--) {
    if (fxcrt::IndexInBounds(m_WordArray, i))
      m_WordArray.erase(m_WordArray.begin() + i);
  }
}

void CPVT_Section::ClearRightWords(int32_t nWordIndex) {
  int32_t sz = fxcrt::CollectionSize<int32_t>(m_WordArray);
  for (int32_t i = sz - 1; i > nWordIndex; i--) {
    if (fxcrt::IndexInBounds(m_WordArray, i))
      m_WordArray.erase(m_WordArray.begin() + i);
  }
}

void CPVT_Section::ClearMidWords(int32_t nBeginIndex, int32_t nEndIndex) {
  for (int32_t i = nEndIndex; i > nBeginIndex; i--) {
    if (fxcrt::IndexInBounds(m_WordArray, i))
      m_WordArray.erase(m_WordArray.begin() + i);
  }
}

void CPVT_Section::ClearWords(const CPVT_WordRange& PlaceRange) {
  CPVT_WordPlace SecBeginPos = GetBeginWordPlace();
  CPVT_WordPlace SecEndPos = GetEndWordPlace();
  if (PlaceRange.BeginPos >= SecBeginPos) {
    if (PlaceRange.EndPos <= SecEndPos) {
      ClearMidWords(PlaceRange.BeginPos.nWordIndex,
                    PlaceRange.EndPos.nWordIndex);
    } else {
      ClearRightWords(PlaceRange.BeginPos.nWordIndex);
    }
  } else if (PlaceRange.EndPos <= SecEndPos) {
    ClearLeftWords(PlaceRange.EndPos.nWordIndex);
  } else {
    m_WordArray.clear();
  }
}

void CPVT_Section::ClearWord(const CPVT_WordPlace& place) {
  if (fxcrt::IndexInBounds(m_WordArray, place.nWordIndex))
    m_WordArray.erase(m_WordArray.begin() + place.nWordIndex);
}
