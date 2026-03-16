// Copyright 2023 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FXGE_FX_FONTENCODING_H_
#define CORE_FXGE_FX_FONTENCODING_H_

#include <stdint.h>

namespace fxge {

enum class FontEncoding : uint32_t {
  kAdobeCustom,
  kAdobeExpert,
  kAdobeStandard,
  kAppleRoman,
  kBig5,
  kGB2312,
  kJohab,
  kLatin1,
  kNone,
  kOldLatin2,
  kSjis,
  kSymbol,
  kUnicode,
  kWansung,
};

}  // namespace fxge

#endif  // CORE_FXGE_FX_FONTENCODING_H_
