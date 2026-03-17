// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_FONT_CPDF_FONTENCODING_H_
#define CORE_FPDFAPI_FONT_CPDF_FONTENCODING_H_

#include <array>

#include "core/fxcrt/bytestring.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/span.h"
#include "core/fxcrt/string_pool_template.h"
#include "core/fxcrt/weak_ptr.h"
#include "core/fxge/fx_fontencoding.h"

enum class FontEncoding {
  kBuiltin = 0,
  kWinAnsi = 1,
  kMacRoman = 2,
  kMacExpert = 3,
  kStandard = 4,
  kAdobeSymbol = 5,
  kZapfDingbats = 6,
  kPdfDoc = 7,
  kMsSymbol = 8,
};

uint32_t CharCodeFromUnicodeForEncoding(fxge::FontEncoding encoding,
                                        wchar_t unicode);

wchar_t UnicodeFromAppleRomanCharCode(uint8_t charcode);

pdfium::span<const uint16_t> UnicodesForPredefinedCharSet(
    FontEncoding encoding);
const char* CharNameFromPredefinedCharSet(FontEncoding encoding,
                                          uint8_t charcode);

class CPDF_Object;

class CPDF_FontEncoding {
 public:
  static constexpr size_t kEncodingTableSize = 256;

  explicit CPDF_FontEncoding(FontEncoding predefined_encoding);

  bool IsIdentical(const CPDF_FontEncoding* pAnother) const;

  wchar_t UnicodeFromCharCode(uint8_t charcode) const {
    return m_Unicodes[charcode];
  }
  int CharCodeFromUnicode(wchar_t unicode) const;

  void SetUnicode(uint8_t charcode, wchar_t unicode) {
    m_Unicodes[charcode] = unicode;
  }

  RetainPtr<CPDF_Object> Realize(WeakPtr<ByteStringPool> pPool) const;

 private:
  std::array<wchar_t, kEncodingTableSize> m_Unicodes = {};
};

#endif  // CORE_FPDFAPI_FONT_CPDF_FONTENCODING_H_
