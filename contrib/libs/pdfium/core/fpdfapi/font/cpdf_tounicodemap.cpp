// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/font/cpdf_tounicodemap.h"

#include <limits>
#include <set>
#include <utility>

#include "core/fpdfapi/font/cpdf_cid2unicodemap.h"
#include "core/fpdfapi/font/cpdf_fontglobals.h"
#include "core/fpdfapi/parser/cpdf_simple_parser.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fpdfapi/parser/fpdf_parser_utility.h"
#include "core/fxcrt/containers/contains.h"
#include "core/fxcrt/fx_extension.h"
#include "core/fxcrt/fx_safe_types.h"

namespace {

WideString StringDataAdd(WideString str) {
  WideString ret;
  wchar_t value = 1;
  for (size_t i = str.GetLength(); i > 0; --i) {
    wchar_t ch = str[i - 1] + value;
    if (ch < str[i - 1]) {
      ret.InsertAtFront(0);
    } else {
      ret.InsertAtFront(ch);
      value = 0;
    }
  }
  if (value)
    ret.InsertAtFront(value);
  return ret;
}

}  // namespace

CPDF_ToUnicodeMap::CPDF_ToUnicodeMap(RetainPtr<const CPDF_Stream> pStream) {
  Load(std::move(pStream));
}

CPDF_ToUnicodeMap::~CPDF_ToUnicodeMap() = default;

WideString CPDF_ToUnicodeMap::Lookup(uint32_t charcode) const {
  auto it = m_Multimap.find(charcode);
  if (it == m_Multimap.end()) {
    if (!m_pBaseMap)
      return WideString();
    return WideString(
        m_pBaseMap->UnicodeFromCID(static_cast<uint16_t>(charcode)));
  }

  uint32_t value = *it->second.begin();
  wchar_t unicode = static_cast<wchar_t>(value & 0xffff);
  if (unicode != 0xffff)
    return WideString(unicode);

  size_t index = value >> 16;
  return index < m_MultiCharVec.size() ? m_MultiCharVec[index] : WideString();
}

uint32_t CPDF_ToUnicodeMap::ReverseLookup(wchar_t unicode) const {
  for (const auto& pair : m_Multimap) {
    if (pdfium::Contains(pair.second, static_cast<uint32_t>(unicode)))
      return pair.first;
  }
  return 0;
}

size_t CPDF_ToUnicodeMap::GetUnicodeCountByCharcodeForTesting(
    uint32_t charcode) const {
  auto it = m_Multimap.find(charcode);
  return it != m_Multimap.end() ? it->second.size() : 0u;
}

// static
std::optional<uint32_t> CPDF_ToUnicodeMap::StringToCode(ByteStringView input) {
  // Ignore whitespaces within `input`. See https://crbug.com/pdfium/2065.
  std::set<char> seen_whitespace_chars;
  for (char c : input) {
    if (PDFCharIsWhitespace(c)) {
      seen_whitespace_chars.insert(c);
    }
  }
  ByteString str_without_whitespace_chars;  // Must outlive `str`.
  ByteStringView str;
  if (seen_whitespace_chars.empty()) {
    str = input;
  } else {
    str_without_whitespace_chars.Reserve(input.GetLength());
    for (char c : input) {
      if (!pdfium::Contains(seen_whitespace_chars, c)) {
        str_without_whitespace_chars += c;
      }
    }
    str = str_without_whitespace_chars.AsStringView();
  }

  size_t len = str.GetLength();
  if (len <= 2 || str[0] != '<' || str[len - 1] != '>')
    return std::nullopt;

  FX_SAFE_UINT32 code = 0;
  for (char c : str.Substr(1, len - 2)) {
    if (!FXSYS_IsHexDigit(c))
      return std::nullopt;

    code = code * 16 + FXSYS_HexCharToInt(c);
    if (!code.IsValid())
      return std::nullopt;
  }
  return std::optional<uint32_t>(code.ValueOrDie());
}

// static
WideString CPDF_ToUnicodeMap::StringToWideString(ByteStringView str) {
  size_t len = str.GetLength();
  if (len <= 2 || str[0] != '<' || str[len - 1] != '>')
    return WideString();

  WideString result;
  int byte_pos = 0;
  wchar_t ch = 0;
  for (char c : str.Substr(1, len - 2)) {
    if (!FXSYS_IsHexDigit(c))
      break;

    ch = ch * 16 + FXSYS_HexCharToInt(c);
    byte_pos++;
    if (byte_pos == 4) {
      result += ch;
      byte_pos = 0;
      ch = 0;
    }
  }
  return result;
}

void CPDF_ToUnicodeMap::Load(RetainPtr<const CPDF_Stream> pStream) {
  CIDSet cid_set = CIDSET_UNKNOWN;
  auto pAcc = pdfium::MakeRetain<CPDF_StreamAcc>(std::move(pStream));
  pAcc->LoadAllDataFiltered();
  CPDF_SimpleParser parser(pAcc->GetSpan());
  while (true) {
    ByteStringView word = parser.GetWord();
    if (word.IsEmpty())
      break;

    if (word == "beginbfchar")
      HandleBeginBFChar(&parser);
    else if (word == "beginbfrange")
      HandleBeginBFRange(&parser);
    else if (word == "/Adobe-Korea1-UCS2")
      cid_set = CIDSET_KOREA1;
    else if (word == "/Adobe-Japan1-UCS2")
      cid_set = CIDSET_JAPAN1;
    else if (word == "/Adobe-CNS1-UCS2")
      cid_set = CIDSET_CNS1;
    else if (word == "/Adobe-GB1-UCS2")
      cid_set = CIDSET_GB1;
  }
  if (cid_set != CIDSET_UNKNOWN) {
    m_pBaseMap = CPDF_FontGlobals::GetInstance()->GetCID2UnicodeMap(cid_set);
  }
}

void CPDF_ToUnicodeMap::HandleBeginBFChar(CPDF_SimpleParser* pParser) {
  while (true) {
    ByteStringView word = pParser->GetWord();
    if (word.IsEmpty() || word == "endbfchar")
      return;

    std::optional<uint32_t> code = StringToCode(word);
    if (!code.has_value())
      return;

    SetCode(code.value(), StringToWideString(pParser->GetWord()));
  }
}

void CPDF_ToUnicodeMap::HandleBeginBFRange(CPDF_SimpleParser* pParser) {
  while (true) {
    ByteStringView lowcode_str = pParser->GetWord();
    if (lowcode_str.IsEmpty() || lowcode_str == "endbfrange")
      return;

    std::optional<uint32_t> lowcode_opt = StringToCode(lowcode_str);
    if (!lowcode_opt.has_value())
      return;

    ByteStringView highcode_str = pParser->GetWord();
    std::optional<uint32_t> highcode_opt = StringToCode(highcode_str);
    if (!highcode_opt.has_value())
      return;

    uint32_t lowcode = lowcode_opt.value();
    uint32_t highcode = (lowcode & 0xffffff00) | (highcode_opt.value() & 0xff);

    ByteStringView start = pParser->GetWord();
    if (start == "[") {
      for (uint32_t code = lowcode; code <= highcode; ++code) {
        SetCode(code, StringToWideString(pParser->GetWord()));
        if (code == std::numeric_limits<uint32_t>::max()) {
          break;
        }
      }
      pParser->GetWord();
      continue;
    }

    WideString destcode = StringToWideString(start);
    if (destcode.GetLength() == 1) {
      std::optional<uint32_t> value_or_error = StringToCode(start);
      if (!value_or_error.has_value())
        return;

      uint32_t value = value_or_error.value();
      for (uint32_t code = lowcode; code <= highcode; ++code) {
        InsertIntoMultimap(code, value++);
        if (code == std::numeric_limits<uint32_t>::max()) {
          break;
        }
      }
    } else {
      for (uint32_t code = lowcode; code <= highcode; ++code) {
        WideString retcode =
            code == lowcode ? destcode : StringDataAdd(destcode);
        InsertIntoMultimap(code, GetMultiCharIndexIndicator());
        m_MultiCharVec.push_back(retcode);
        destcode = std::move(retcode);
        if (code == std::numeric_limits<uint32_t>::max()) {
          break;
        }
      }
    }
  }
}

uint32_t CPDF_ToUnicodeMap::GetMultiCharIndexIndicator() const {
  FX_SAFE_UINT32 uni = m_MultiCharVec.size();
  uni = uni * 0x10000 + 0xffff;
  return uni.ValueOrDefault(0);
}

void CPDF_ToUnicodeMap::SetCode(uint32_t srccode, WideString destcode) {
  size_t len = destcode.GetLength();
  if (len == 0)
    return;

  if (len == 1) {
    InsertIntoMultimap(srccode, destcode[0]);
  } else {
    InsertIntoMultimap(srccode, GetMultiCharIndexIndicator());
    m_MultiCharVec.push_back(destcode);
  }
}

void CPDF_ToUnicodeMap::InsertIntoMultimap(uint32_t code, uint32_t destcode) {
  auto it = m_Multimap.find(code);
  if (it == m_Multimap.end()) {
    m_Multimap.emplace(code, std::set<uint32_t>{destcode});
    return;
  }

  it->second.emplace(destcode);
}
