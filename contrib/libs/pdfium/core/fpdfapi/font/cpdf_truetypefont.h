// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_FONT_CPDF_TRUETYPEFONT_H_
#define CORE_FPDFAPI_FONT_CPDF_TRUETYPEFONT_H_

#include "core/fpdfapi/font/cpdf_simplefont.h"
#include "core/fxcrt/retain_ptr.h"

class CPDF_TrueTypeFont final : public CPDF_SimpleFont {
 public:
  CONSTRUCT_VIA_MAKE_RETAIN;
  ~CPDF_TrueTypeFont() override;

  // CPDF_Font:
  bool IsTrueTypeFont() const override;
  const CPDF_TrueTypeFont* AsTrueTypeFont() const override;
  CPDF_TrueTypeFont* AsTrueTypeFont() override;

 private:
  enum class CharmapType { kMSUnicode, kMSSymbol, kMacRoman, kOther };

  CPDF_TrueTypeFont(CPDF_Document* pDocument,
                    RetainPtr<CPDF_Dictionary> pFontDict);

  // CPDF_Font:
  bool Load() override;

  // CPDF_SimpleFont:
  void LoadGlyphMap() override;

  bool HasAnyGlyphIndex() const;
  CharmapType DetermineCharmapType() const;
  FontEncoding DetermineEncoding() const;
  void SetGlyphIndicesFromFirstChar();
};

#endif  // CORE_FPDFAPI_FONT_CPDF_TRUETYPEFONT_H_
