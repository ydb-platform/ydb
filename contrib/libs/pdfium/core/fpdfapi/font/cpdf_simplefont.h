// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_FONT_CPDF_SIMPLEFONT_H_
#define CORE_FPDFAPI_FONT_CPDF_SIMPLEFONT_H_

#include <stdint.h>

#include <array>
#include <vector>

#include "core/fpdfapi/font/cpdf_font.h"
#include "core/fpdfapi/font/cpdf_fontencoding.h"
#include "core/fxcrt/fx_string.h"

class CPDF_SimpleFont : public CPDF_Font {
 public:
  ~CPDF_SimpleFont() override;

  // CPDF_Font
  int GetCharWidthF(uint32_t charcode) override;
  FX_RECT GetCharBBox(uint32_t charcode) override;
  int GlyphFromCharCode(uint32_t charcode, bool* pVertGlyph) override;
  bool IsUnicodeCompatible() const override;
  WideString UnicodeFromCharCode(uint32_t charcode) const override;
  uint32_t CharCodeFromUnicode(wchar_t Unicode) const override;

  const CPDF_FontEncoding* GetEncoding() const { return &m_Encoding; }

  bool HasFontWidths() const override;

 protected:
  static constexpr size_t kInternalTableSize = 256;

  CPDF_SimpleFont(CPDF_Document* pDocument,
                  RetainPtr<CPDF_Dictionary> pFontDict);

  virtual void LoadGlyphMap() = 0;

  bool LoadCommon();
  void LoadSubstFont();
  void LoadCharMetrics(int charcode);
  void LoadCharWidths(const CPDF_Dictionary* font_desc);
  void LoadDifferences(const CPDF_Dictionary* encoding);
  void LoadPDFEncoding(bool bEmbedded, bool bTrueType);

  CPDF_FontEncoding m_Encoding{FontEncoding::kBuiltin};
  FontEncoding m_BaseEncoding = FontEncoding::kBuiltin;
  bool m_bUseFontWidth = false;
  std::vector<ByteString> m_CharNames;
  std::array<uint16_t, kInternalTableSize> m_GlyphIndex;
  std::array<uint16_t, kInternalTableSize> m_CharWidth;
  std::array<FX_RECT, kInternalTableSize> m_CharBBox;
};

#endif  // CORE_FPDFAPI_FONT_CPDF_SIMPLEFONT_H_
