// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdftext/cpdf_textpagefind.h"

#include <wchar.h>

#include <vector>

#include "core/fpdftext/cpdf_textpage.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/fx_extension.h"
#include "core/fxcrt/fx_string.h"
#include "core/fxcrt/fx_system.h"
#include "core/fxcrt/fx_unicode.h"
#include "core/fxcrt/ptr_util.h"
#include "core/fxcrt/stl_util.h"

namespace {

constexpr wchar_t kNonBreakingSpace = 160;

bool IsIgnoreSpaceCharacter(wchar_t curChar) {
  if (curChar < 255 || (curChar >= 0x0600 && curChar <= 0x06FF) ||
      (curChar >= 0xFE70 && curChar <= 0xFEFF) ||
      (curChar >= 0xFB50 && curChar <= 0xFDFF) ||
      (curChar >= 0x0400 && curChar <= 0x04FF) ||
      (curChar >= 0x0500 && curChar <= 0x052F) ||
      (curChar >= 0xA640 && curChar <= 0xA69F) ||
      (curChar >= 0x2DE0 && curChar <= 0x2DFF) || curChar == 8467 ||
      (curChar >= 0x2000 && curChar <= 0x206F)) {
    return false;
  }
  return true;
}

bool IsMatchWholeWord(const WideString& csPageText,
                      size_t startPos,
                      size_t endPos) {
  if (startPos > endPos)
    return false;
  wchar_t char_left = 0;
  wchar_t char_right = 0;
  size_t char_count = endPos - startPos + 1;
  if (char_count == 0)
    return false;
  if (char_count == 1 && csPageText[startPos] > 255)
    return true;
  if (startPos >= 1)
    char_left = csPageText[startPos - 1];
  if (startPos + char_count < csPageText.GetLength())
    char_right = csPageText[startPos + char_count];
  if ((char_left > 'A' && char_left < 'a') ||
      (char_left > 'a' && char_left < 'z') ||
      (char_left > 0xfb00 && char_left < 0xfb06) ||
      FXSYS_IsDecimalDigit(char_left) ||
      (char_right > 'A' && char_right < 'a') ||
      (char_right > 'a' && char_right < 'z') ||
      (char_right > 0xfb00 && char_right < 0xfb06) ||
      FXSYS_IsDecimalDigit(char_right)) {
    return false;
  }
  if (!(('A' > char_left || char_left > 'Z') &&
        ('a' > char_left || char_left > 'z') &&
        ('A' > char_right || char_right > 'Z') &&
        ('a' > char_right || char_right > 'z'))) {
    return false;
  }
  if (char_count > 0) {
    if (FXSYS_IsDecimalDigit(char_left) &&
        FXSYS_IsDecimalDigit(csPageText[startPos])) {
      return false;
    }
    if (FXSYS_IsDecimalDigit(char_right) &&
        FXSYS_IsDecimalDigit(csPageText[endPos])) {
      return false;
    }
  }
  return true;
}

WideString GetStringCase(const WideString& wsOriginal, bool bMatchCase) {
  if (bMatchCase)
    return wsOriginal;

  WideString wsLower = wsOriginal;
  wsLower.MakeLower();
  return wsLower;
}

std::optional<WideString> ExtractSubString(const wchar_t* lpszFullString,
                                           int iSubString) {
  DCHECK(lpszFullString);
  UNSAFE_TODO({
    while (iSubString--) {
      lpszFullString = wcschr(lpszFullString, L' ');
      if (!lpszFullString) {
        return std::nullopt;
      }

      lpszFullString++;
      while (*lpszFullString == L' ') {
        lpszFullString++;
      }
    }

    const wchar_t* lpchEnd = wcschr(lpszFullString, L' ');
    int nLen = lpchEnd ? static_cast<int>(lpchEnd - lpszFullString)
                       : static_cast<int>(wcslen(lpszFullString));
    if (nLen < 0) {
      return std::nullopt;
    }

    return WideString(lpszFullString, static_cast<size_t>(nLen));
  });
}

std::vector<WideString> ExtractFindWhat(const WideString& findwhat) {
  std::vector<WideString> findwhat_array;

  size_t len = findwhat.GetLength();
  size_t i = 0;
  for (i = 0; i < len; ++i)
    if (findwhat[i] != ' ')
      break;
  if (i == len) {
    findwhat_array.push_back(findwhat);
    return findwhat_array;
  }

  int index = 0;
  while (true) {
    std::optional<WideString> word = ExtractSubString(findwhat.c_str(), index);
    if (!word.has_value())
      break;

    if (word->IsEmpty()) {
      findwhat_array.push_back(L"");
      index++;
      continue;
    }

    size_t pos = 0;
    while (pos < word->GetLength()) {
      WideString curStr = word->Substr(pos, 1);
      wchar_t curChar = word.value()[pos];
      if (IsIgnoreSpaceCharacter(curChar)) {
        if (pos > 0 && curChar == pdfium::unicode::kRightSingleQuotationMark) {
          pos++;
          continue;
        }
        if (pos > 0)
          findwhat_array.push_back(word->First(pos));
        findwhat_array.push_back(curStr);
        if (pos == word->GetLength() - 1) {
          word->clear();
          break;
        }
        word.emplace(word->Last(word->GetLength() - pos - 1));
        pos = 0;
        continue;
      }
      pos++;
    }

    if (!word->IsEmpty())
      findwhat_array.push_back(word.value());
    index++;
  }
  return findwhat_array;
}

}  // namespace

// static
std::unique_ptr<CPDF_TextPageFind> CPDF_TextPageFind::Create(
    const CPDF_TextPage* pTextPage,
    const WideString& findwhat,
    const Options& options,
    std::optional<size_t> startPos) {
  std::vector<WideString> findwhat_array =
      ExtractFindWhat(GetStringCase(findwhat, options.bMatchCase));
  auto find = pdfium::WrapUnique(
      new CPDF_TextPageFind(pTextPage, findwhat_array, options, startPos));
  find->FindFirst();
  return find;
}

CPDF_TextPageFind::CPDF_TextPageFind(
    const CPDF_TextPage* pTextPage,
    const std::vector<WideString>& findwhat_array,
    const Options& options,
    std::optional<size_t> startPos)
    : m_pTextPage(pTextPage),
      m_strText(GetStringCase(pTextPage->GetAllPageText(), options.bMatchCase)),
      m_csFindWhatArray(findwhat_array),
      m_options(options) {
  if (!m_strText.IsEmpty()) {
    m_findNextStart = startPos;
    m_findPreStart = startPos.value_or(m_strText.GetLength() - 1);
  }
}

CPDF_TextPageFind::~CPDF_TextPageFind() = default;

int CPDF_TextPageFind::GetCharIndex(int index) const {
  return m_pTextPage->CharIndexFromTextIndex(index);
}

bool CPDF_TextPageFind::FindFirst() {
  return m_strText.IsEmpty() || !m_csFindWhatArray.empty();
}

bool CPDF_TextPageFind::FindNext() {
  if (m_strText.IsEmpty() || !m_findNextStart.has_value())
    return false;

  const size_t strLen = m_strText.GetLength();
  size_t nStartPos = m_findNextStart.value();
  if (nStartPos >= strLen) {
    return false;
  }

  int nCount = fxcrt::CollectionSize<int>(m_csFindWhatArray);
  std::optional<size_t> nResultPos = 0;
  bool bSpaceStart = false;
  for (int iWord = 0; iWord < nCount; iWord++) {
    WideString csWord = m_csFindWhatArray[iWord];
    if (csWord.IsEmpty()) {
      if (iWord == nCount - 1) {
        if (nStartPos >= strLen) {
          return false;
        }
        wchar_t strInsert = m_strText[nStartPos];
        if (strInsert == L'\n' || strInsert == L' ' || strInsert == L'\r' ||
            strInsert == kNonBreakingSpace) {
          nResultPos = nStartPos + 1;
          break;
        }
        iWord = -1;
      } else if (iWord == 0) {
        bSpaceStart = true;
      }
      continue;
    }
    nResultPos = m_strText.Find(csWord.AsStringView(), nStartPos);
    if (!nResultPos.has_value())
      return false;

    size_t endIndex = nResultPos.value() + csWord.GetLength() - 1;
    if (iWord == 0)
      m_resStart = nResultPos.value();
    bool bMatch = true;
    if (iWord != 0 && !bSpaceStart) {
      size_t PreResEndPos = nStartPos;
      int curChar = csWord[0];
      WideString lastWord = m_csFindWhatArray[iWord - 1];
      int lastChar = lastWord.Back();
      if (nStartPos == nResultPos.value() &&
          !(IsIgnoreSpaceCharacter(lastChar) ||
            IsIgnoreSpaceCharacter(curChar))) {
        bMatch = false;
      }
      for (size_t d = PreResEndPos; d < nResultPos.value(); d++) {
        wchar_t strInsert = m_strText[d];
        if (strInsert != L'\n' && strInsert != L' ' && strInsert != L'\r' &&
            strInsert != kNonBreakingSpace) {
          bMatch = false;
          break;
        }
      }
    } else if (bSpaceStart) {
      if (nResultPos.value() > 0) {
        wchar_t strInsert = m_strText[nResultPos.value() - 1];
        if (strInsert != L'\n' && strInsert != L' ' && strInsert != L'\r' &&
            strInsert != kNonBreakingSpace) {
          bMatch = false;
          m_resStart = nResultPos.value();
        } else {
          m_resStart = nResultPos.value() - 1;
        }
      }
    }
    if (m_options.bMatchWholeWord && bMatch)
      bMatch = IsMatchWholeWord(m_strText, nResultPos.value(), endIndex);

    if (bMatch) {
      nStartPos = endIndex + 1;
    } else {
      iWord = -1;
      size_t index = bSpaceStart ? 1 : 0;
      nStartPos = m_resStart + m_csFindWhatArray[index].GetLength();
    }
  }
  m_resEnd = nResultPos.value() + m_csFindWhatArray.back().GetLength() - 1;
  if (m_options.bConsecutive) {
    m_findNextStart = m_resStart + 1;
    m_findPreStart = m_resEnd - 1;
  } else {
    m_findNextStart = m_resEnd + 1;
    m_findPreStart = m_resStart - 1;
  }
  return true;
}

bool CPDF_TextPageFind::FindPrev() {
  if (m_strText.IsEmpty() || !m_findPreStart.has_value())
    return false;

  CPDF_TextPageFind find_engine(m_pTextPage, m_csFindWhatArray, m_options, 0);
  if (!find_engine.FindFirst())
    return false;

  int order = -1;
  int matches = 0;
  while (find_engine.FindNext()) {
    int cur_order = find_engine.GetCurOrder();
    int cur_match = find_engine.GetMatchedCount();
    int temp = cur_order + cur_match;
    if (temp < 0 || static_cast<size_t>(temp) > m_findPreStart.value() + 1)
      break;

    order = cur_order;
    matches = cur_match;
  }
  if (order == -1)
    return false;

  m_resStart = m_pTextPage->TextIndexFromCharIndex(order);
  m_resEnd = m_pTextPage->TextIndexFromCharIndex(order + matches - 1);
  if (m_options.bConsecutive) {
    m_findNextStart = m_resStart + 1;
    m_findPreStart = m_resEnd - 1;
  } else {
    m_findNextStart = m_resEnd + 1;
    m_findPreStart = m_resStart - 1;
  }
  return true;
}

int CPDF_TextPageFind::GetCurOrder() const {
  return GetCharIndex(m_resStart);
}

int CPDF_TextPageFind::GetMatchedCount() const {
  int resStart = GetCharIndex(m_resStart);
  int resEnd = GetCharIndex(m_resEnd);
  return resEnd - resStart + 1;
}
