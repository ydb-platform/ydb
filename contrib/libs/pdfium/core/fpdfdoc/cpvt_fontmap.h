// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFDOC_CPVT_FONTMAP_H_
#define CORE_FPDFDOC_CPVT_FONTMAP_H_

#include <stdint.h>

#include "core/fpdfdoc/ipvt_fontmap.h"
#include "core/fxcrt/bytestring.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/unowned_ptr.h"

class CPDF_Document;
class CPDF_Dictionary;
class CPDF_Font;

class CPVT_FontMap final : public IPVT_FontMap {
 public:
  CPVT_FontMap(CPDF_Document* pDoc,
               RetainPtr<CPDF_Dictionary> pResDict,
               RetainPtr<CPDF_Font> pDefFont,
               const ByteString& sDefFontAlias);
  ~CPVT_FontMap() override;

  // IPVT_FontMap:
  RetainPtr<CPDF_Font> GetPDFFont(int32_t nFontIndex) override;
  ByteString GetPDFFontAlias(int32_t nFontIndex) override;
  int32_t GetWordFontIndex(uint16_t word,
                           FX_Charset charset,
                           int32_t nFontIndex) override;
  int32_t CharCodeFromUnicode(int32_t nFontIndex, uint16_t word) override;
  FX_Charset CharSetFromUnicode(uint16_t word, FX_Charset nOldCharset) override;

 private:
  void SetupAnnotSysPDFFont();

  UnownedPtr<CPDF_Document> const m_pDocument;
  RetainPtr<CPDF_Dictionary> const m_pResDict;
  RetainPtr<CPDF_Font> const m_pDefFont;
  RetainPtr<CPDF_Font> m_pSysFont;
  const ByteString m_sDefFontAlias;
  ByteString m_sSysFontAlias;
};

#endif  // CORE_FPDFDOC_CPVT_FONTMAP_H_
