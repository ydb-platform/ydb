// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcrt/fx_string.h"

#include <stdint.h>

#include <array>
#include <string>
#include <vector>

#include "build/build_config.h"
#include "core/fxcrt/bytestring.h"
#include "core/fxcrt/code_point_view.h"
#include "core/fxcrt/fx_extension.h"
#include "core/fxcrt/span.h"
#include "core/fxcrt/utf16.h"
#include "core/fxcrt/widestring.h"

#if !defined(WCHAR_T_IS_16_BIT) && !defined(WCHAR_T_IS_32_BIT)
#error "Unknown wchar_t size"
#endif
#if defined(WCHAR_T_IS_16_BIT) && defined(WCHAR_T_IS_32_BIT)
#error "Conflicting wchar_t sizes"
#endif

namespace {

// Appends a Unicode code point to a `ByteString` using UTF-8.
//
// TODO(crbug.com/pdfium/2041): Migrate to `ByteString`.
void AppendCodePointToByteString(char32_t code_point, ByteString& buffer) {
  if (code_point > pdfium::kMaximumSupplementaryCodePoint) {
    // Invalid code point above U+10FFFF.
    return;
  }

  if (code_point < 0x80) {
    // 7-bit code points are unchanged in UTF-8.
    buffer += code_point;
    return;
  }

  int byte_size;
  if (code_point < 0x800) {
    byte_size = 2;
  } else if (code_point < 0x10000) {
    byte_size = 3;
  } else {
    byte_size = 4;
  }

  static constexpr std::array<uint8_t, 3> kPrefix = {{0xc0, 0xe0, 0xf0}};
  int order = 1 << ((byte_size - 1) * 6);
  buffer += kPrefix[byte_size - 2] | (code_point / order);
  for (int i = 0; i < byte_size - 1; i++) {
    code_point = code_point % order;
    order >>= 6;
    buffer += 0x80 | (code_point / order);
  }
}

}  // namespace

ByteString FX_UTF8Encode(WideStringView wsStr) {
  ByteString buffer;
  for (char32_t code_point : pdfium::CodePointView(wsStr)) {
    AppendCodePointToByteString(code_point, buffer);
  }
  return buffer;
}

std::u16string FX_UTF16Encode(WideStringView wsStr) {
  if (wsStr.IsEmpty()) {
    return {};
  }

  std::u16string result;
  result.reserve(wsStr.GetLength());

  for (wchar_t c : wsStr) {
#if defined(WCHAR_T_IS_32_BIT)
    if (pdfium::IsSupplementary(c)) {
      pdfium::SurrogatePair pair(c);
      result.push_back(pair.high());
      result.push_back(pair.low());
      continue;
    }
#endif  // defined(WCHAR_T_IS_32_BIT)
    result.push_back(c);
  }

  return result;
}

namespace {

constexpr float kFractionScalesFloat[] = {
    0.1f,         0.01f,         0.001f,        0.0001f,
    0.00001f,     0.000001f,     0.0000001f,    0.00000001f,
    0.000000001f, 0.0000000001f, 0.00000000001f};

const double kFractionScalesDouble[] = {
    0.1,       0.01,       0.001,       0.0001,       0.00001,      0.000001,
    0.0000001, 0.00000001, 0.000000001, 0.0000000001, 0.00000000001};

template <class T>
T StringTo(ByteStringView strc, pdfium::span<const T> fractional_scales) {
  if (strc.IsEmpty())
    return 0;

  bool bNegative = false;
  size_t cc = 0;
  size_t len = strc.GetLength();
  if (strc[0] == '+') {
    cc++;
  } else if (strc[0] == '-') {
    bNegative = true;
    cc++;
  }
  while (cc < len) {
    if (strc[cc] != '+' && strc[cc] != '-')
      break;
    cc++;
  }
  T value = 0;
  while (cc < len) {
    if (strc[cc] == '.')
      break;
    value = value * 10 + FXSYS_DecimalCharToInt(strc.CharAt(cc));
    cc++;
  }
  size_t scale = 0;
  if (cc < len && strc[cc] == '.') {
    cc++;
    while (cc < len) {
      value +=
          fractional_scales[scale] * FXSYS_DecimalCharToInt(strc.CharAt(cc));
      scale++;
      if (scale == fractional_scales.size())
        break;
      cc++;
    }
  }
  return bNegative ? -value : value;
}

}  // namespace

float StringToFloat(ByteStringView strc) {
  return StringTo<float>(strc, kFractionScalesFloat);
}

float StringToFloat(WideStringView wsStr) {
  return StringToFloat(FX_UTF8Encode(wsStr).AsStringView());
}

double StringToDouble(ByteStringView strc) {
  return StringTo<double>(strc, kFractionScalesDouble);
}

double StringToDouble(WideStringView wsStr) {
  return StringToDouble(FX_UTF8Encode(wsStr).AsStringView());
}

namespace fxcrt {

template std::vector<ByteString> Split<ByteString>(const ByteString& that,
                                                   ByteString::CharType ch);
template std::vector<WideString> Split<WideString>(const WideString& that,
                                                   WideString::CharType ch);

}  // namespace fxcrt
