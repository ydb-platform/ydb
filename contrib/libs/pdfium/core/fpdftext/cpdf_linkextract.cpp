// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdftext/cpdf_linkextract.h"

#include <wchar.h>

#include <vector>

#include "core/fpdftext/cpdf_textpage.h"
#include "core/fxcrt/fx_extension.h"
#include "core/fxcrt/fx_string.h"
#include "core/fxcrt/fx_system.h"

namespace {

// Find the end of a web link starting from offset |start| and ending at offset
// |end|. The purpose of this function is to separate url from the surrounding
// context characters, we do not intend to fully validate the url. |str|
// contains lower case characters only.
size_t FindWebLinkEnding(const WideString& str, size_t start, size_t end) {
  if (str.Contains(L'/', start)) {
    // When there is a path and query after '/', most ASCII chars are allowed.
    // We don't sanitize in this case.
    return end;
  }

  // When there is no path, it only has IP address or host name.
  // Port is optional at the end.
  if (str[start] == L'[') {
    // IPv6 reference.
    // Find the end of the reference.
    auto result = str.Find(L']', start + 1);
    if (result.has_value()) {
      end = result.value();
      if (end > start + 1) {  // Has content inside brackets.
        size_t len = str.GetLength();
        size_t off = end + 1;
        if (off < len && str[off] == L':') {
          off++;
          while (off < len && FXSYS_IsDecimalDigit(str[off]))
            off++;
          if (off > end + 2 &&
              off <= len)   // At least one digit in port number.
            end = off - 1;  // |off| is offset of the first invalid char.
        }
      }
    }
    return end;
  }

  // According to RFC1123, host name only has alphanumeric chars, hyphens,
  // and periods. Hyphen should not at the end though.
  // Non-ASCII chars are ignored during checking.
  while (end > start && str[end] < 0x80) {
    if (FXSYS_IsDecimalDigit(str[end]) ||
        (str[end] >= L'a' && str[end] <= L'z') || str[end] == L'.') {
      break;
    }
    end--;
  }
  return end;
}

// Remove characters from the end of |str|, delimited by |start| and |end|, up
// to and including |charToFind|. No-op if |charToFind| is not present. Updates
// |end| if characters were removed.
void TrimBackwardsToChar(const WideString& str,
                         wchar_t charToFind,
                         size_t start,
                         size_t* end) {
  for (size_t pos = *end; pos >= start; pos--) {
    if (str[pos] == charToFind) {
      *end = pos - 1;
      break;
    }
  }
}

// Finds opening brackets ()[]{}<> and quotes "'  before the URL delimited by
// |start| and |end| in |str|. Matches a closing bracket or quote for each
// opening character and, if present, removes everything afterwards. Returns the
// new end position for the string.
size_t TrimExternalBracketsFromWebLink(const WideString& str,
                                       size_t start,
                                       size_t end) {
  for (size_t pos = 0; pos < start; pos++) {
    if (str[pos] == '(') {
      TrimBackwardsToChar(str, ')', start, &end);
    } else if (str[pos] == '[') {
      TrimBackwardsToChar(str, ']', start, &end);
    } else if (str[pos] == '{') {
      TrimBackwardsToChar(str, '}', start, &end);
    } else if (str[pos] == '<') {
      TrimBackwardsToChar(str, '>', start, &end);
    } else if (str[pos] == '"') {
      TrimBackwardsToChar(str, '"', start, &end);
    } else if (str[pos] == '\'') {
      TrimBackwardsToChar(str, '\'', start, &end);
    }
  }
  return end;
}

}  // namespace

CPDF_LinkExtract::CPDF_LinkExtract(const CPDF_TextPage* pTextPage)
    : m_pTextPage(pTextPage) {}

CPDF_LinkExtract::~CPDF_LinkExtract() = default;

void CPDF_LinkExtract::ExtractLinks() {
  m_LinkArray.clear();
  size_t start = 0;
  size_t pos = 0;
  bool bAfterHyphen = false;
  bool bLineBreak = false;
  const size_t nTotalChar = m_pTextPage->CountChars();
  const WideString page_text = m_pTextPage->GetAllPageText();
  while (pos < nTotalChar) {
    const CPDF_TextPage::CharInfo& char_info = m_pTextPage->GetCharInfo(pos);
    if (char_info.m_CharType != CPDF_TextPage::CharType::kGenerated &&
        char_info.m_Unicode != L' ' && pos != nTotalChar - 1) {
      bAfterHyphen =
          (char_info.m_CharType == CPDF_TextPage::CharType::kHyphen ||
           (char_info.m_CharType == CPDF_TextPage::CharType::kNormal &&
            char_info.m_Unicode == L'-'));
      ++pos;
      continue;
    }

    size_t nCount = pos - start;
    if (pos == nTotalChar - 1) {
      ++nCount;
    } else if (bAfterHyphen &&
               (char_info.m_Unicode == L'\n' || char_info.m_Unicode == L'\r')) {
      // Handle text breaks with a hyphen to the next line.
      bLineBreak = true;
      ++pos;
      continue;
    }

    WideString strBeCheck = page_text.Substr(start, nCount);
    if (bLineBreak) {
      strBeCheck.Remove(L'\n');
      strBeCheck.Remove(L'\r');
      bLineBreak = false;
    }
    // Replace the generated code with the hyphen char.
    strBeCheck.Replace(L"\xfffe", L"-");

    if (strBeCheck.GetLength() > 5) {
      while (strBeCheck.GetLength() > 0) {
        wchar_t ch = strBeCheck.Back();
        if (ch != L')' && ch != L',' && ch != L'>' && ch != L'.')
          break;

        strBeCheck = strBeCheck.First(strBeCheck.GetLength() - 1);
        nCount--;
      }

      // Check for potential web URLs and email addresses.
      // Ftp address, file system links, data, blob etc. are not checked.
      if (nCount > 5) {
        auto maybe_link = CheckWebLink(strBeCheck);
        if (maybe_link.has_value()) {
          maybe_link.value().m_Start += start;
          m_LinkArray.push_back(maybe_link.value());
        } else if (CheckMailLink(&strBeCheck)) {
          m_LinkArray.push_back(Link{{start, nCount}, strBeCheck});
        }
      }
    }
    start = ++pos;
  }
}

std::optional<CPDF_LinkExtract::Link> CPDF_LinkExtract::CheckWebLink(
    const WideString& strBeCheck) {
  static const wchar_t kHttpScheme[] = L"http";
  static const wchar_t kWWWAddrStart[] = L"www.";

  const size_t kHttpSchemeLen = wcslen(kHttpScheme);
  const size_t kWWWAddrStartLen = wcslen(kWWWAddrStart);

  WideString str = strBeCheck;
  str.MakeLower();

  // First, try to find the scheme.
  auto start = str.Find(kHttpScheme);
  if (start.has_value()) {
    size_t off = start.value() + kHttpSchemeLen;  // move after "http".
    if (str.GetLength() > off + 4) {  // At least "://<char>" follows.
      if (str[off] == L's')  // "https" scheme is accepted.
        off++;
      if (str[off] == L':' && str[off + 1] == L'/' && str[off + 2] == L'/') {
        off += 3;
        const size_t end =
            FindWebLinkEnding(str, off,
                              TrimExternalBracketsFromWebLink(
                                  str, start.value(), str.GetLength() - 1));
        if (end > off) {  // Non-empty host name.
          const size_t nStart = start.value();
          const size_t nCount = end - nStart + 1;
          return Link{{nStart, nCount}, strBeCheck.Substr(nStart, nCount)};
        }
      }
    }
  }

  // When there is no scheme, try to find url starting with "www.".
  start = str.Find(kWWWAddrStart);
  if (start.has_value()) {
    size_t off = start.value() + kWWWAddrStartLen;
    if (str.GetLength() > off) {
      const size_t end =
          FindWebLinkEnding(str, start.value(),
                            TrimExternalBracketsFromWebLink(
                                str, start.value(), str.GetLength() - 1));
      if (end > off) {
        const size_t nStart = start.value();
        const size_t nCount = end - nStart + 1;
        return Link{{nStart, nCount},
                    L"http://" + strBeCheck.Substr(nStart, nCount)};
      }
    }
  }

  return std::nullopt;
}

bool CPDF_LinkExtract::CheckMailLink(WideString* str) {
  auto aPos = str->Find(L'@');
  // Invalid when no '@' or when starts/ends with '@'.
  if (!aPos.has_value() || aPos.value() == 0 || aPos == str->GetLength() - 1)
    return false;

  // Check the local part.
  size_t pPos = aPos.value();  // Used to track the position of '@' or '.'.
  for (size_t i = aPos.value(); i > 0; i--) {
    wchar_t ch = (*str)[i - 1];
    if (ch == L'_' || ch == L'-' || FXSYS_iswalnum(ch))
      continue;

    if (ch != L'.' || i == pPos || i == 1) {
      if (i == aPos.value()) {
        // There is '.' or invalid char before '@'.
        return false;
      }
      // End extracting for other invalid chars, '.' at the beginning, or
      // consecutive '.'.
      size_t removed_len = i == pPos ? i + 1 : i;
      *str = str->Last(str->GetLength() - removed_len);
      break;
    }
    // Found a valid '.'.
    pPos = i - 1;
  }

  // Check the domain name part.
  aPos = str->Find(L'@');
  if (!aPos.has_value() || aPos.value() == 0)
    return false;

  str->TrimBack(L'.');
  // At least one '.' in domain name, but not at the beginning.
  // TODO(weili): RFC5322 allows domain names to be a local name without '.'.
  // Check whether we should remove this check.
  auto ePos = str->Find(L'.', aPos.value() + 1);
  if (!ePos.has_value() || ePos.value() == aPos.value() + 1)
    return false;

  // Validate all other chars in domain name.
  size_t nLen = str->GetLength();
  pPos = 0;  // Used to track the position of '.'.
  for (size_t i = aPos.value() + 1; i < nLen; i++) {
    wchar_t wch = (*str)[i];
    if (wch == L'-' || FXSYS_iswalnum(wch))
      continue;

    if (wch != L'.' || i == pPos + 1) {
      // Domain name should end before invalid char.
      size_t host_end = i == pPos + 1 ? i - 2 : i - 1;
      if (pPos > 0 && host_end - aPos.value() >= 3) {
        // Trim the ending invalid chars if there is at least one '.' and name.
        *str = str->First(host_end + 1);
        break;
      }
      return false;
    }
    pPos = i;
  }

  if (!str->Contains(L"mailto:"))
    *str = L"mailto:" + *str;

  return true;
}

WideString CPDF_LinkExtract::GetURL(size_t index) const {
  return index < m_LinkArray.size() ? m_LinkArray[index].m_strUrl
                                    : WideString();
}

std::vector<CFX_FloatRect> CPDF_LinkExtract::GetRects(size_t index) const {
  if (index >= m_LinkArray.size())
    return std::vector<CFX_FloatRect>();

  return m_pTextPage->GetRectArray(m_LinkArray[index].m_Start,
                                   m_LinkArray[index].m_Count);
}

std::optional<CPDF_LinkExtract::Range> CPDF_LinkExtract::GetTextRange(
    size_t index) const {
  if (index >= m_LinkArray.size())
    return std::nullopt;
  return m_LinkArray[index];
}
