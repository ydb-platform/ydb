// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXGE_CFX_UNICODEENCODINGEX_H_
#define CORE_FXGE_CFX_UNICODEENCODINGEX_H_

#include <stdint.h>

#include <memory>

#include "core/fxge/cfx_face.h"
#include "core/fxge/cfx_unicodeencoding.h"
#include "core/fxge/fx_fontencoding.h"

class CFX_UnicodeEncodingEx final : public CFX_UnicodeEncoding {
 public:
  static constexpr uint32_t kInvalidCharCode = static_cast<uint32_t>(-1);

  CFX_UnicodeEncodingEx(CFX_Font* pFont, fxge::FontEncoding encoding_id);
  ~CFX_UnicodeEncodingEx() override;

  // CFX_UnicodeEncoding:
  uint32_t GlyphFromCharCode(uint32_t charcode) override;

  // Returns |kInvalidCharCode| on error.
  uint32_t CharCodeFromUnicode(wchar_t Unicode) const;

 private:
  fxge::FontEncoding encoding_id_;
};

std::unique_ptr<CFX_UnicodeEncodingEx> FX_CreateFontEncodingEx(CFX_Font* pFont);

#endif  // CORE_FXGE_CFX_UNICODEENCODINGEX_H_
