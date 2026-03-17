// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfdoc/cpvt_variabletext.h"

#include <algorithm>
#include <array>
#include <utility>

#include "core/fpdfapi/font/cpdf_font.h"
#include "core/fpdfdoc/cpvt_section.h"
#include "core/fpdfdoc/cpvt_word.h"
#include "core/fpdfdoc/cpvt_wordinfo.h"
#include "core/fpdfdoc/ipvt_fontmap.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/fx_codepage.h"
#include "core/fxcrt/fx_safe_types.h"
#include "core/fxcrt/stl_util.h"

namespace {

constexpr float kFontScale = 0.001f;
constexpr uint8_t kReturnLength = 1;

constexpr auto kFontSizeSteps = fxcrt::ToArray<const uint8_t>(
    {4,  6,  8,  9,  10, 12, 14, 18,  20,  25,  30,  35, 40,
     45, 50, 55, 60, 70, 80, 90, 100, 110, 120, 130, 144});

}  // namespace

CPVT_VariableText::Provider::Provider(IPVT_FontMap* pFontMap)
    : m_pFontMap(pFontMap) {
  DCHECK(m_pFontMap);
}

CPVT_VariableText::Provider::~Provider() = default;

int CPVT_VariableText::Provider::GetCharWidth(int32_t nFontIndex,
                                              uint16_t word) {
  RetainPtr<CPDF_Font> pPDFFont = m_pFontMap->GetPDFFont(nFontIndex);
  if (!pPDFFont)
    return 0;

  uint32_t charcode = pPDFFont->CharCodeFromUnicode(word);
  if (charcode == CPDF_Font::kInvalidCharCode)
    return 0;

  return pPDFFont->GetCharWidthF(charcode);
}

int32_t CPVT_VariableText::Provider::GetTypeAscent(int32_t nFontIndex) {
  RetainPtr<CPDF_Font> pPDFFont = m_pFontMap->GetPDFFont(nFontIndex);
  return pPDFFont ? pPDFFont->GetTypeAscent() : 0;
}

int32_t CPVT_VariableText::Provider::GetTypeDescent(int32_t nFontIndex) {
  RetainPtr<CPDF_Font> pPDFFont = m_pFontMap->GetPDFFont(nFontIndex);
  return pPDFFont ? pPDFFont->GetTypeDescent() : 0;
}

int32_t CPVT_VariableText::Provider::GetWordFontIndex(uint16_t word,
                                                      FX_Charset charset,
                                                      int32_t nFontIndex) {
  if (RetainPtr<CPDF_Font> pDefFont = m_pFontMap->GetPDFFont(0)) {
    if (pDefFont->CharCodeFromUnicode(word) != CPDF_Font::kInvalidCharCode)
      return 0;
  }
  if (RetainPtr<CPDF_Font> pSysFont = m_pFontMap->GetPDFFont(1)) {
    if (pSysFont->CharCodeFromUnicode(word) != CPDF_Font::kInvalidCharCode)
      return 1;
  }
  return -1;
}

int32_t CPVT_VariableText::Provider::GetDefaultFontIndex() {
  return 0;
}

CPVT_VariableText::Iterator::Iterator(CPVT_VariableText* pVT) : m_pVT(pVT) {
  DCHECK(m_pVT);
}

CPVT_VariableText::Iterator::~Iterator() = default;

void CPVT_VariableText::Iterator::SetAt(int32_t nWordIndex) {
  m_CurPos = m_pVT->WordIndexToWordPlace(nWordIndex);
}

void CPVT_VariableText::Iterator::SetAt(const CPVT_WordPlace& place) {
  m_CurPos = place;
}

bool CPVT_VariableText::Iterator::NextWord() {
  if (m_CurPos == m_pVT->GetEndWordPlace())
    return false;

  m_CurPos = m_pVT->GetNextWordPlace(m_CurPos);
  return true;
}

bool CPVT_VariableText::Iterator::NextLine() {
  if (!fxcrt::IndexInBounds(m_pVT->m_SectionArray, m_CurPos.nSecIndex))
    return false;

  CPVT_Section* pSection = m_pVT->m_SectionArray[m_CurPos.nSecIndex].get();
  if (m_CurPos.nLineIndex < pSection->GetLineArraySize() - 1) {
    m_CurPos = CPVT_WordPlace(m_CurPos.nSecIndex, m_CurPos.nLineIndex + 1, -1);
    return true;
  }
  if (m_CurPos.nSecIndex <
      fxcrt::CollectionSize<int32_t>(m_pVT->m_SectionArray) - 1) {
    m_CurPos = CPVT_WordPlace(m_CurPos.nSecIndex + 1, 0, -1);
    return true;
  }
  return false;
}

bool CPVT_VariableText::Iterator::GetWord(CPVT_Word& word) const {
  word.WordPlace = m_CurPos;
  if (!fxcrt::IndexInBounds(m_pVT->m_SectionArray, m_CurPos.nSecIndex))
    return false;

  CPVT_Section* pSection = m_pVT->m_SectionArray[m_CurPos.nSecIndex].get();
  if (!pSection->GetLineFromArray(m_CurPos.nLineIndex))
    return false;

  const CPVT_WordInfo* pInfo = pSection->GetWordFromArray(m_CurPos.nWordIndex);
  if (!pInfo)
    return false;

  word.Word = pInfo->Word;
  word.nCharset = pInfo->nCharset;
  word.fWidth = m_pVT->GetWordWidth(*pInfo);
  word.ptWord =
      m_pVT->InToOut(CFX_PointF(pInfo->fWordX + pSection->GetRect().left,
                                pInfo->fWordY + pSection->GetRect().top));
  word.fAscent = m_pVT->GetWordAscent(*pInfo);
  word.fDescent = m_pVT->GetWordDescent(*pInfo);
  word.nFontIndex = pInfo->nFontIndex;
  word.fFontSize = m_pVT->GetWordFontSize();
  return true;
}

bool CPVT_VariableText::Iterator::GetLine(CPVT_Line& line) const {
  DCHECK(m_pVT);
  line.lineplace = CPVT_WordPlace(m_CurPos.nSecIndex, m_CurPos.nLineIndex, -1);
  if (!fxcrt::IndexInBounds(m_pVT->m_SectionArray, m_CurPos.nSecIndex))
    return false;

  CPVT_Section* pSection = m_pVT->m_SectionArray[m_CurPos.nSecIndex].get();
  const CPVT_Section::Line* pLine =
      pSection->GetLineFromArray(m_CurPos.nLineIndex);
  if (!pLine)
    return false;

  line.ptLine = m_pVT->InToOut(
      CFX_PointF(pLine->m_LineInfo.fLineX + pSection->GetRect().left,
                 pLine->m_LineInfo.fLineY + pSection->GetRect().top));
  line.fLineWidth = pLine->m_LineInfo.fLineWidth;
  line.fLineAscent = pLine->m_LineInfo.fLineAscent;
  line.fLineDescent = pLine->m_LineInfo.fLineDescent;
  line.lineEnd = pLine->GetEndWordPlace();
  return true;
}

CPVT_VariableText::CPVT_VariableText(Provider* pProvider)
    : m_pVTProvider(pProvider) {}

CPVT_VariableText::~CPVT_VariableText() = default;

void CPVT_VariableText::Initialize() {
  if (m_bInitialized)
    return;

  CPVT_WordPlace place;
  place.nSecIndex = 0;
  AddSection(place);

  CPVT_LineInfo lineinfo;
  lineinfo.fLineAscent = GetFontAscent(GetDefaultFontIndex(), GetFontSize());
  lineinfo.fLineDescent = GetFontDescent(GetDefaultFontIndex(), GetFontSize());
  AddLine(place, lineinfo);

  if (!m_SectionArray.empty())
    m_SectionArray.front()->ResetLinePlace();

  m_bInitialized = true;
}

CPVT_WordPlace CPVT_VariableText::InsertWord(const CPVT_WordPlace& place,
                                             uint16_t word,
                                             FX_Charset charset) {
  int32_t nTotalWords = GetTotalWords();
  if (m_nLimitChar > 0 && nTotalWords >= m_nLimitChar)
    return place;
  if (m_nCharArray > 0 && nTotalWords >= m_nCharArray)
    return place;

  CPVT_WordPlace newplace = place;
  newplace.nWordIndex++;
  int32_t nFontIndex =
      GetSubWord() > 0 ? GetDefaultFontIndex()
                       : GetWordFontIndex(word, charset, GetDefaultFontIndex());
  return AddWord(newplace, CPVT_WordInfo(word, charset, nFontIndex));
}

CPVT_WordPlace CPVT_VariableText::InsertSection(const CPVT_WordPlace& place) {
  int32_t nTotalWords = GetTotalWords();
  if (m_nLimitChar > 0 && nTotalWords >= m_nLimitChar)
    return place;
  if (m_nCharArray > 0 && nTotalWords >= m_nCharArray)
    return place;
  if (!m_bMultiLine)
    return place;

  CPVT_WordPlace wordplace = place;
  UpdateWordPlace(wordplace);
  if (!fxcrt::IndexInBounds(m_SectionArray, wordplace.nSecIndex))
    return place;

  CPVT_Section* pSection = m_SectionArray[wordplace.nSecIndex].get();
  CPVT_WordPlace NewPlace(wordplace.nSecIndex + 1, 0, -1);
  AddSection(NewPlace);
  CPVT_WordPlace result = NewPlace;
  if (fxcrt::IndexInBounds(m_SectionArray, NewPlace.nSecIndex)) {
    CPVT_Section* pNewSection = m_SectionArray[NewPlace.nSecIndex].get();
    for (int32_t w = wordplace.nWordIndex + 1; w < pSection->GetWordArraySize();
         ++w) {
      NewPlace.nWordIndex++;
      pNewSection->AddWord(NewPlace, *pSection->GetWordFromArray(w));
    }
  }
  ClearSectionRightWords(wordplace);
  return result;
}

CPVT_WordPlace CPVT_VariableText::DeleteWords(
    const CPVT_WordRange& PlaceRange) {
  bool bLastSecPos =
      fxcrt::IndexInBounds(m_SectionArray, PlaceRange.EndPos.nSecIndex) &&
      PlaceRange.EndPos ==
          m_SectionArray[PlaceRange.EndPos.nSecIndex]->GetEndWordPlace();

  ClearWords(PlaceRange);
  if (PlaceRange.BeginPos.nSecIndex != PlaceRange.EndPos.nSecIndex) {
    ClearEmptySections(PlaceRange);
    if (!bLastSecPos)
      LinkLatterSection(PlaceRange.BeginPos);
  }
  return PlaceRange.BeginPos;
}

CPVT_WordPlace CPVT_VariableText::DeleteWord(const CPVT_WordPlace& place) {
  return ClearRightWord(PrevLineHeaderPlace(place));
}

CPVT_WordPlace CPVT_VariableText::BackSpaceWord(const CPVT_WordPlace& place) {
  return ClearLeftWord(PrevLineHeaderPlace(place));
}

void CPVT_VariableText::SetText(const WideString& swText) {
  DeleteWords(CPVT_WordRange(GetBeginWordPlace(), GetEndWordPlace()));
  CPVT_WordPlace wp(0, 0, -1);
  if (!m_SectionArray.empty())
    m_SectionArray.front()->SetRect(CPVT_FloatRect());

  FX_SAFE_INT32 nCharCount = 0;
  for (size_t i = 0, sz = swText.GetLength(); i < sz; i++) {
    if (m_nLimitChar > 0 && nCharCount.ValueOrDie() >= m_nLimitChar)
      break;
    if (m_nCharArray > 0 && nCharCount.ValueOrDie() >= m_nCharArray)
      break;

    uint16_t word = swText[i];
    switch (word) {
      case 0x0D:
        if (m_bMultiLine) {
          if (i + 1 < sz && swText[i + 1] == 0x0A)
            i++;
          wp.AdvanceSection();
          AddSection(wp);
        }
        break;
      case 0x0A:
        if (m_bMultiLine) {
          if (i + 1 < sz && swText[i + 1] == 0x0D)
            i++;
          wp.AdvanceSection();
          AddSection(wp);
        }
        break;
      case 0x09:
        word = 0x20;
        [[fallthrough]];
      default:
        wp = InsertWord(wp, word, FX_Charset::kDefault);
        break;
    }
    nCharCount++;
  }
}

void CPVT_VariableText::UpdateWordPlace(CPVT_WordPlace& place) const {
  if (place.nSecIndex < 0)
    place = GetBeginWordPlace();
  if (static_cast<size_t>(place.nSecIndex) >= m_SectionArray.size())
    place = GetEndWordPlace();

  place = PrevLineHeaderPlace(place);
  if (fxcrt::IndexInBounds(m_SectionArray, place.nSecIndex))
    m_SectionArray[place.nSecIndex]->UpdateWordPlace(place);
}

int32_t CPVT_VariableText::WordPlaceToWordIndex(
    const CPVT_WordPlace& place) const {
  CPVT_WordPlace newplace = place;
  UpdateWordPlace(newplace);
  int32_t nIndex = 0;
  int32_t i = 0;
  int32_t sz = 0;
  for (i = 0, sz = fxcrt::CollectionSize<int32_t>(m_SectionArray);
       i < sz && i < newplace.nSecIndex; i++) {
    CPVT_Section* pSection = m_SectionArray[i].get();
    nIndex += pSection->GetWordArraySize();
    if (i != sz - 1)
      nIndex += kReturnLength;
  }
  if (fxcrt::IndexInBounds(m_SectionArray, i))
    nIndex += newplace.nWordIndex + kReturnLength;
  return nIndex;
}

CPVT_WordPlace CPVT_VariableText::WordIndexToWordPlace(int32_t index) const {
  CPVT_WordPlace place = GetBeginWordPlace();
  int32_t nOldIndex = 0;
  int32_t nIndex = 0;
  bool bFound = false;
  for (size_t i = 0; i < m_SectionArray.size(); ++i) {
    CPVT_Section* pSection = m_SectionArray[i].get();
    nIndex += pSection->GetWordArraySize();
    if (nIndex == index) {
      place = pSection->GetEndWordPlace();
      bFound = true;
      break;
    }
    if (nIndex > index) {
      place.nSecIndex = pdfium::checked_cast<int32_t>(i);
      place.nWordIndex = index - nOldIndex - 1;
      pSection->UpdateWordPlace(place);
      bFound = true;
      break;
    }
    if (i != m_SectionArray.size() - 1)
      nIndex += kReturnLength;
    nOldIndex = nIndex;
  }
  if (!bFound)
    place = GetEndWordPlace();
  return place;
}

CPVT_WordPlace CPVT_VariableText::GetBeginWordPlace() const {
  return m_bInitialized ? CPVT_WordPlace(0, 0, -1) : CPVT_WordPlace();
}

CPVT_WordPlace CPVT_VariableText::GetEndWordPlace() const {
  if (m_SectionArray.empty())
    return CPVT_WordPlace();
  return m_SectionArray.back()->GetEndWordPlace();
}

CPVT_WordPlace CPVT_VariableText::GetPrevWordPlace(
    const CPVT_WordPlace& place) const {
  if (place.nSecIndex < 0)
    return GetBeginWordPlace();
  if (static_cast<size_t>(place.nSecIndex) >= m_SectionArray.size())
    return GetEndWordPlace();

  CPVT_Section* pSection = m_SectionArray[place.nSecIndex].get();
  if (place > pSection->GetBeginWordPlace())
    return pSection->GetPrevWordPlace(place);
  if (!fxcrt::IndexInBounds(m_SectionArray, place.nSecIndex - 1))
    return GetBeginWordPlace();
  return m_SectionArray[place.nSecIndex - 1]->GetEndWordPlace();
}

CPVT_WordPlace CPVT_VariableText::GetNextWordPlace(
    const CPVT_WordPlace& place) const {
  if (place.nSecIndex < 0)
    return GetBeginWordPlace();
  if (static_cast<size_t>(place.nSecIndex) >= m_SectionArray.size())
    return GetEndWordPlace();

  CPVT_Section* pSection = m_SectionArray[place.nSecIndex].get();
  if (place < pSection->GetEndWordPlace())
    return pSection->GetNextWordPlace(place);
  if (!fxcrt::IndexInBounds(m_SectionArray, place.nSecIndex + 1))
    return GetEndWordPlace();
  return m_SectionArray[place.nSecIndex + 1]->GetBeginWordPlace();
}

CPVT_WordPlace CPVT_VariableText::SearchWordPlace(
    const CFX_PointF& point) const {
  CFX_PointF pt = OutToIn(point);
  CPVT_WordPlace place = GetBeginWordPlace();
  int32_t nLeft = 0;
  int32_t nRight = fxcrt::CollectionSize<int32_t>(m_SectionArray) - 1;
  int32_t nMid = fxcrt::CollectionSize<int32_t>(m_SectionArray) / 2;
  bool bUp = true;
  bool bDown = true;
  while (nLeft <= nRight) {
    if (!fxcrt::IndexInBounds(m_SectionArray, nMid))
      break;
    CPVT_Section* pSection = m_SectionArray[nMid].get();
    if (FXSYS_IsFloatBigger(pt.y, pSection->GetRect().top))
      bUp = false;
    if (FXSYS_IsFloatBigger(pSection->GetRect().bottom, pt.y))
      bDown = false;
    if (FXSYS_IsFloatSmaller(pt.y, pSection->GetRect().top)) {
      nRight = nMid - 1;
      nMid = (nLeft + nRight) / 2;
      continue;
    }
    if (FXSYS_IsFloatBigger(pt.y, pSection->GetRect().bottom)) {
      nLeft = nMid + 1;
      nMid = (nLeft + nRight) / 2;
      continue;
    }
    place = pSection->SearchWordPlace(CFX_PointF(
        pt.x - pSection->GetRect().left, pt.y - pSection->GetRect().top));
    place.nSecIndex = nMid;
    return place;
  }
  if (bUp)
    place = GetBeginWordPlace();
  if (bDown)
    place = GetEndWordPlace();
  return place;
}

CPVT_WordPlace CPVT_VariableText::GetUpWordPlace(
    const CPVT_WordPlace& place,
    const CFX_PointF& point) const {
  if (!fxcrt::IndexInBounds(m_SectionArray, place.nSecIndex))
    return place;

  CPVT_Section* pSection = m_SectionArray[place.nSecIndex].get();
  CPVT_WordPlace temp = place;
  CFX_PointF pt = OutToIn(point);
  if (temp.nLineIndex-- > 0) {
    return pSection->SearchWordPlace(pt.x - pSection->GetRect().left, temp);
  }
  if (temp.nSecIndex-- > 0) {
    if (fxcrt::IndexInBounds(m_SectionArray, temp.nSecIndex)) {
      CPVT_Section* pLastSection = m_SectionArray[temp.nSecIndex].get();
      temp.nLineIndex = pLastSection->GetLineArraySize() - 1;
      return pLastSection->SearchWordPlace(pt.x - pLastSection->GetRect().left,
                                           temp);
    }
  }
  return place;
}

CPVT_WordPlace CPVT_VariableText::GetDownWordPlace(
    const CPVT_WordPlace& place,
    const CFX_PointF& point) const {
  if (!fxcrt::IndexInBounds(m_SectionArray, place.nSecIndex))
    return place;

  CPVT_Section* pSection = m_SectionArray[place.nSecIndex].get();
  CPVT_WordPlace temp = place;
  CFX_PointF pt = OutToIn(point);
  if (temp.nLineIndex++ < pSection->GetLineArraySize() - 1) {
    return pSection->SearchWordPlace(pt.x - pSection->GetRect().left, temp);
  }
  temp.AdvanceSection();
  if (!fxcrt::IndexInBounds(m_SectionArray, temp.nSecIndex))
    return place;

  return m_SectionArray[temp.nSecIndex]->SearchWordPlace(
      pt.x - pSection->GetRect().left, temp);
}

CPVT_WordPlace CPVT_VariableText::GetLineBeginPlace(
    const CPVT_WordPlace& place) const {
  return CPVT_WordPlace(place.nSecIndex, place.nLineIndex, -1);
}

CPVT_WordPlace CPVT_VariableText::GetLineEndPlace(
    const CPVT_WordPlace& place) const {
  if (!fxcrt::IndexInBounds(m_SectionArray, place.nSecIndex))
    return place;

  CPVT_Section* pSection = m_SectionArray[place.nSecIndex].get();
  const CPVT_Section::Line* pLine =
      pSection->GetLineFromArray(place.nLineIndex);
  if (!pLine)
    return place;

  return pLine->GetEndWordPlace();
}

CPVT_WordPlace CPVT_VariableText::GetSectionBeginPlace(
    const CPVT_WordPlace& place) const {
  return CPVT_WordPlace(place.nSecIndex, 0, -1);
}

CPVT_WordPlace CPVT_VariableText::GetSectionEndPlace(
    const CPVT_WordPlace& place) const {
  if (!fxcrt::IndexInBounds(m_SectionArray, place.nSecIndex))
    return place;

  return m_SectionArray[place.nSecIndex]->GetEndWordPlace();
}

int32_t CPVT_VariableText::GetTotalWords() const {
  int32_t nTotal = 0;
  for (const auto& pSection : m_SectionArray) {
    nTotal += pSection->GetWordArraySize() + kReturnLength;
  }
  return nTotal - kReturnLength;
}

CPVT_WordPlace CPVT_VariableText::AddSection(const CPVT_WordPlace& place) {
  if (IsValid() && !m_bMultiLine)
    return place;

  int32_t nSecIndex = std::clamp(
      place.nSecIndex, 0, fxcrt::CollectionSize<int32_t>(m_SectionArray));

  auto pSection = std::make_unique<CPVT_Section>(this);
  pSection->SetRect(CPVT_FloatRect());
  pSection->SetPlaceIndex(nSecIndex);
  m_SectionArray.insert(m_SectionArray.begin() + nSecIndex,
                        std::move(pSection));
  return place;
}

CPVT_WordPlace CPVT_VariableText::AddLine(const CPVT_WordPlace& place,
                                          const CPVT_LineInfo& lineinfo) {
  if (!fxcrt::IndexInBounds(m_SectionArray, place.nSecIndex))
    return place;

  return m_SectionArray[place.nSecIndex]->AddLine(lineinfo);
}

CPVT_WordPlace CPVT_VariableText::AddWord(const CPVT_WordPlace& place,
                                          const CPVT_WordInfo& wordinfo) {
  if (m_SectionArray.empty())
    return place;

  CPVT_WordPlace newplace = place;
  newplace.nSecIndex =
      std::clamp(newplace.nSecIndex, 0,
                 fxcrt::CollectionSize<int32_t>(m_SectionArray) - 1);
  return m_SectionArray[newplace.nSecIndex]->AddWord(newplace, wordinfo);
}

void CPVT_VariableText::SetPlateRect(const CFX_FloatRect& rect) {
  m_rcPlate = rect;
}

CFX_FloatRect CPVT_VariableText::GetContentRect() const {
  return InToOut(m_rcContent);
}

const CFX_FloatRect& CPVT_VariableText::GetPlateRect() const {
  return m_rcPlate;
}

float CPVT_VariableText::GetWordFontSize() const {
  return GetFontSize();
}

float CPVT_VariableText::GetWordWidth(int32_t nFontIndex,
                                      uint16_t Word,
                                      uint16_t SubWord,
                                      float fFontSize,
                                      float fWordTail) const {
  return GetCharWidth(nFontIndex, Word, SubWord) * fFontSize * kFontScale +
         fWordTail;
}

float CPVT_VariableText::GetWordWidth(const CPVT_WordInfo& WordInfo) const {
  return GetWordWidth(WordInfo.nFontIndex, WordInfo.Word, GetSubWord(),
                      GetWordFontSize(), WordInfo.fWordTail);
}

float CPVT_VariableText::GetLineAscent() {
  return GetFontAscent(GetDefaultFontIndex(), GetFontSize());
}

float CPVT_VariableText::GetLineDescent() {
  return GetFontDescent(GetDefaultFontIndex(), GetFontSize());
}

float CPVT_VariableText::GetFontAscent(int32_t nFontIndex,
                                       float fFontSize) const {
  float ascent = m_pVTProvider ? m_pVTProvider->GetTypeAscent(nFontIndex) : 0;
  return ascent * fFontSize * kFontScale;
}

float CPVT_VariableText::GetFontDescent(int32_t nFontIndex,
                                        float fFontSize) const {
  float descent = m_pVTProvider ? m_pVTProvider->GetTypeDescent(nFontIndex) : 0;
  return descent * fFontSize * kFontScale;
}

float CPVT_VariableText::GetWordAscent(const CPVT_WordInfo& WordInfo,
                                       float fFontSize) const {
  return GetFontAscent(WordInfo.nFontIndex, fFontSize);
}

float CPVT_VariableText::GetWordDescent(const CPVT_WordInfo& WordInfo,
                                        float fFontSize) const {
  return GetFontDescent(WordInfo.nFontIndex, fFontSize);
}

float CPVT_VariableText::GetWordAscent(const CPVT_WordInfo& WordInfo) const {
  return GetFontAscent(WordInfo.nFontIndex, GetWordFontSize());
}

float CPVT_VariableText::GetWordDescent(const CPVT_WordInfo& WordInfo) const {
  return GetFontDescent(WordInfo.nFontIndex, GetWordFontSize());
}

float CPVT_VariableText::GetLineLeading() {
  return m_fLineLeading;
}

float CPVT_VariableText::GetLineIndent() {
  return 0.0f;
}

void CPVT_VariableText::ClearSectionRightWords(const CPVT_WordPlace& place) {
  CPVT_WordPlace wordplace = PrevLineHeaderPlace(place);
  if (!fxcrt::IndexInBounds(m_SectionArray, place.nSecIndex))
    return;

  CPVT_Section* pSection = m_SectionArray[place.nSecIndex].get();
  pSection->EraseWordsFrom(wordplace.nWordIndex + 1);
}

CPVT_WordPlace CPVT_VariableText::PrevLineHeaderPlace(
    const CPVT_WordPlace& place) const {
  if (place.nWordIndex < 0 && place.nLineIndex > 0)
    return GetPrevWordPlace(place);
  return place;
}

CPVT_WordPlace CPVT_VariableText::NextLineHeaderPlace(
    const CPVT_WordPlace& place) const {
  if (place.nWordIndex < 0 && place.nLineIndex > 0)
    return GetNextWordPlace(place);
  return place;
}

bool CPVT_VariableText::ClearEmptySection(const CPVT_WordPlace& place) {
  if (place.nSecIndex == 0 && m_SectionArray.size() == 1)
    return false;

  if (!fxcrt::IndexInBounds(m_SectionArray, place.nSecIndex))
    return false;

  if (m_SectionArray[place.nSecIndex]->GetWordArraySize() != 0)
    return false;

  m_SectionArray.erase(m_SectionArray.begin() + place.nSecIndex);
  return true;
}

void CPVT_VariableText::ClearEmptySections(const CPVT_WordRange& PlaceRange) {
  CPVT_WordPlace wordplace;
  for (int32_t s = PlaceRange.EndPos.nSecIndex;
       s > PlaceRange.BeginPos.nSecIndex; s--) {
    wordplace.nSecIndex = s;
    ClearEmptySection(wordplace);
  }
}

void CPVT_VariableText::LinkLatterSection(const CPVT_WordPlace& place) {
  CPVT_WordPlace oldplace = PrevLineHeaderPlace(place);
  if (!fxcrt::IndexInBounds(m_SectionArray, place.nSecIndex + 1))
    return;

  CPVT_Section* pNextSection = m_SectionArray[place.nSecIndex + 1].get();
  if (fxcrt::IndexInBounds(m_SectionArray, oldplace.nSecIndex)) {
    CPVT_Section* pSection = m_SectionArray[oldplace.nSecIndex].get();
    for (int32_t i = 0; i < pNextSection->GetWordArraySize(); ++i) {
      oldplace.nWordIndex++;
      pSection->AddWord(oldplace, *pNextSection->GetWordFromArray(i));
    }
  }
  m_SectionArray.erase(m_SectionArray.begin() + place.nSecIndex + 1);
}

void CPVT_VariableText::ClearWords(const CPVT_WordRange& PlaceRange) {
  CPVT_WordRange NewRange;
  NewRange.BeginPos = PrevLineHeaderPlace(PlaceRange.BeginPos);
  NewRange.EndPos = PrevLineHeaderPlace(PlaceRange.EndPos);
  for (int32_t s = NewRange.EndPos.nSecIndex; s >= NewRange.BeginPos.nSecIndex;
       s--) {
    if (fxcrt::IndexInBounds(m_SectionArray, s))
      m_SectionArray[s]->ClearWords(NewRange);
  }
}

CPVT_WordPlace CPVT_VariableText::ClearLeftWord(const CPVT_WordPlace& place) {
  if (!fxcrt::IndexInBounds(m_SectionArray, place.nSecIndex))
    return place;

  CPVT_Section* pSection = m_SectionArray[place.nSecIndex].get();
  CPVT_WordPlace leftplace = GetPrevWordPlace(place);
  if (leftplace == place)
    return place;

  if (leftplace.nSecIndex != place.nSecIndex) {
    if (pSection->GetWordArraySize() == 0)
      ClearEmptySection(place);
    else
      LinkLatterSection(leftplace);
  } else {
    pSection->ClearWord(place);
  }
  return leftplace;
}

CPVT_WordPlace CPVT_VariableText::ClearRightWord(const CPVT_WordPlace& place) {
  if (!fxcrt::IndexInBounds(m_SectionArray, place.nSecIndex))
    return place;

  CPVT_Section* pSection = m_SectionArray[place.nSecIndex].get();
  CPVT_WordPlace rightplace = NextLineHeaderPlace(GetNextWordPlace(place));
  if (rightplace == place)
    return place;

  if (rightplace.nSecIndex != place.nSecIndex)
    LinkLatterSection(place);
  else
    pSection->ClearWord(rightplace);
  return place;
}

void CPVT_VariableText::RearrangeAll() {
  Rearrange(CPVT_WordRange(GetBeginWordPlace(), GetEndWordPlace()));
}

void CPVT_VariableText::RearrangePart(const CPVT_WordRange& PlaceRange) {
  Rearrange(PlaceRange);
}

void CPVT_VariableText::Rearrange(const CPVT_WordRange& PlaceRange) {
  CPVT_FloatRect rcRet;
  if (IsValid()) {
    if (m_bAutoFontSize) {
      SetFontSize(GetAutoFontSize());
      rcRet = RearrangeSections(
          CPVT_WordRange(GetBeginWordPlace(), GetEndWordPlace()));
    } else {
      rcRet = RearrangeSections(PlaceRange);
    }
  }
  m_rcContent = rcRet;
}

float CPVT_VariableText::GetAutoFontSize() {
  int32_t nTotal = sizeof(kFontSizeSteps) / sizeof(uint8_t);
  if (IsMultiLine())
    nTotal /= 4;
  if (nTotal <= 0)
    return 0;
  if (GetPlateWidth() <= 0)
    return 0;

  // TODO(tsepez): replace with std::lower_bound().
  int32_t nLeft = 0;
  int32_t nRight = nTotal - 1;
  int32_t nMid = nTotal / 2;
  while (nLeft <= nRight) {
    if (IsBigger(kFontSizeSteps[nMid])) {
      nRight = nMid - 1;
    } else {
      nLeft = nMid + 1;
    }
    nMid = (nLeft + nRight) / 2;
  }
  return static_cast<float>(kFontSizeSteps[nMid]);
}

bool CPVT_VariableText::IsBigger(float fFontSize) const {
  CFX_SizeF szTotal;
  for (const auto& pSection : m_SectionArray) {
    CFX_SizeF size = pSection->GetSectionSize(fFontSize);
    szTotal.width = std::max(size.width, szTotal.width);
    szTotal.height += size.height;
    if (FXSYS_IsFloatBigger(szTotal.width, GetPlateWidth()) ||
        FXSYS_IsFloatBigger(szTotal.height, GetPlateHeight())) {
      return true;
    }
  }
  return false;
}

CPVT_FloatRect CPVT_VariableText::RearrangeSections(
    const CPVT_WordRange& PlaceRange) {
  float fPosY = 0;
  CPVT_FloatRect rcRet;
  for (int32_t s = 0, sz = fxcrt::CollectionSize<int32_t>(m_SectionArray);
       s < sz; s++) {
    CPVT_WordPlace place;
    place.nSecIndex = s;
    CPVT_Section* pSection = m_SectionArray[s].get();
    pSection->SetPlace(place);
    CPVT_FloatRect rcSec = pSection->GetRect();
    if (s >= PlaceRange.BeginPos.nSecIndex) {
      if (s <= PlaceRange.EndPos.nSecIndex) {
        rcSec = pSection->Rearrange();
        rcSec.top += fPosY;
        rcSec.bottom += fPosY;
      } else {
        float fOldHeight = pSection->GetRect().bottom - pSection->GetRect().top;
        rcSec.top = fPosY;
        rcSec.bottom = fPosY + fOldHeight;
      }
      pSection->SetRect(rcSec);
      pSection->ResetLinePlace();
    }
    if (s == 0) {
      rcRet = rcSec;
    } else {
      rcRet.left = std::min(rcSec.left, rcRet.left);
      rcRet.top = std::min(rcSec.top, rcRet.top);
      rcRet.right = std::max(rcSec.right, rcRet.right);
      rcRet.bottom = std::max(rcSec.bottom, rcRet.bottom);
    }
    fPosY += rcSec.Height();
  }
  return rcRet;
}

int CPVT_VariableText::GetCharWidth(int32_t nFontIndex,
                                    uint16_t Word,
                                    uint16_t SubWord) const {
  if (!m_pVTProvider)
    return 0;
  uint16_t word = SubWord ? SubWord : Word;
  return m_pVTProvider->GetCharWidth(nFontIndex, word);
}

int32_t CPVT_VariableText::GetWordFontIndex(uint16_t word,
                                            FX_Charset charset,
                                            int32_t nFontIndex) {
  return m_pVTProvider
             ? m_pVTProvider->GetWordFontIndex(word, charset, nFontIndex)
             : -1;
}

int32_t CPVT_VariableText::GetDefaultFontIndex() {
  return m_pVTProvider ? m_pVTProvider->GetDefaultFontIndex() : -1;
}

CPVT_VariableText::Iterator* CPVT_VariableText::GetIterator() {
  if (!m_pVTIterator)
    m_pVTIterator = std::make_unique<CPVT_VariableText::Iterator>(this);
  return m_pVTIterator.get();
}

void CPVT_VariableText::SetProvider(Provider* pProvider) {
  m_pVTProvider = pProvider;
}

CFX_PointF CPVT_VariableText::GetBTPoint() const {
  return CFX_PointF(m_rcPlate.left, m_rcPlate.top);
}

CFX_PointF CPVT_VariableText::GetETPoint() const {
  return CFX_PointF(m_rcPlate.right, m_rcPlate.bottom);
}

CFX_PointF CPVT_VariableText::InToOut(const CFX_PointF& point) const {
  return CFX_PointF(point.x + GetBTPoint().x, GetBTPoint().y - point.y);
}

CFX_PointF CPVT_VariableText::OutToIn(const CFX_PointF& point) const {
  return CFX_PointF(point.x - GetBTPoint().x, GetBTPoint().y - point.y);
}

CFX_FloatRect CPVT_VariableText::InToOut(const CPVT_FloatRect& rect) const {
  CFX_PointF ptLeftTop = InToOut(CFX_PointF(rect.left, rect.top));
  CFX_PointF ptRightBottom = InToOut(CFX_PointF(rect.right, rect.bottom));
  return CFX_FloatRect(ptLeftTop.x, ptRightBottom.y, ptRightBottom.x,
                       ptLeftTop.y);
}
