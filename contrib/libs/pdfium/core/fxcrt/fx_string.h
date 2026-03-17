// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCRT_FX_STRING_H_
#define CORE_FXCRT_FX_STRING_H_

#include <stdint.h>

#include <string>
#include <vector>

#include "core/fxcrt/bytestring.h"
#include "core/fxcrt/span.h"
#include "core/fxcrt/widestring.h"

constexpr uint32_t FXBSTR_ID(uint8_t c1, uint8_t c2, uint8_t c3, uint8_t c4) {
  return static_cast<uint32_t>(c1) << 24 | static_cast<uint32_t>(c2) << 16 |
         static_cast<uint32_t>(c3) << 8 | static_cast<uint32_t>(c4);
}

ByteString FX_UTF8Encode(WideStringView wsStr);
std::u16string FX_UTF16Encode(WideStringView wsStr);

float StringToFloat(ByteStringView str);
float StringToFloat(WideStringView wsStr);

double StringToDouble(ByteStringView str);
double StringToDouble(WideStringView wsStr);

namespace fxcrt {

template <typename StrType>
std::vector<StrType> Split(const StrType& that, typename StrType::CharType ch) {
  std::vector<StrType> result;
  StringViewTemplate<typename StrType::CharType> remaining(that.span());
  while (true) {
    std::optional<size_t> index = remaining.Find(ch);
    if (!index.has_value())
      break;
    result.emplace_back(remaining.First(index.value()));
    remaining = remaining.Last(remaining.GetLength() - index.value() - 1);
  }
  result.emplace_back(remaining);
  return result;
}

extern template std::vector<ByteString> Split<ByteString>(
    const ByteString& that,
    ByteString::CharType ch);
extern template std::vector<WideString> Split<WideString>(
    const WideString& that,
    WideString::CharType ch);

}  // namespace fxcrt

#endif  // CORE_FXCRT_FX_STRING_H_
