// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFDOC_CPDF_BAFONTMAP_H_
#define CORE_FPDFDOC_CPDF_BAFONTMAP_H_

#include <memory>
#include <vector>

#include "core/fpdfdoc/ipvt_fontmap.h"
#include "core/fxcrt/fx_codepage.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/unowned_ptr.h"

class CPDF_Dictionary;
class CPDF_Document;

class CPDF_BAFontMap final : public IPVT_FontMap {
 public:
  static FX_Charset GetNativeCharset();

  CPDF_BAFontMap(CPDF_Document* pDocument,
                 RetainPtr<CPDF_Dictionary> pAnnotDict,
                 const ByteString& sAPType);
  ~CPDF_BAFontMap() override;

  // IPVT_FontMap:
  RetainPtr<CPDF_Font> GetPDFFont(int32_t nFontIndex) override;
  ByteString GetPDFFontAlias(int32_t nFontIndex) override;
  int32_t GetWordFontIndex(uint16_t word,
                           FX_Charset nCharset,
                           int32_t nFontIndex) override;
  int32_t CharCodeFromUnicode(int32_t nFontIndex, uint16_t word) override;
  FX_Charset CharSetFromUnicode(uint16_t word, FX_Charset nOldCharset) override;

 private:
  struct Data {
    Data();
    ~Data();

    FX_Charset nCharset = FX_Charset::kANSI;
    RetainPtr<CPDF_Font> pFont;
    ByteString sFontName;
  };

  struct Native {
    FX_Charset nCharset;
    ByteString sFontName;
  };

  RetainPtr<CPDF_Font> FindFontSameCharset(ByteString* sFontAlias,
                                           FX_Charset nCharset);
  RetainPtr<CPDF_Font> FindResFontSameCharset(const CPDF_Dictionary* pResDict,
                                              ByteString* sFontAlias,
                                              FX_Charset nCharset);
  RetainPtr<CPDF_Font> GetAnnotDefaultFont(ByteString* sAlias);
  void AddFontToAnnotDict(const RetainPtr<CPDF_Font>& pFont,
                          const ByteString& sAlias);

  bool KnowWord(int32_t nFontIndex, uint16_t word);

  int32_t GetFontIndex(const ByteString& sFontName,
                       FX_Charset nCharset,
                       bool bFind);
  int32_t AddFontData(const RetainPtr<CPDF_Font>& pFont,
                      const ByteString& sFontAlias,
                      FX_Charset nCharset);

  int32_t FindFont(const ByteString& sFontName, FX_Charset nCharset);
  ByteString GetNativeFontName(FX_Charset nCharset);
  ByteString GetCachedNativeFontName(FX_Charset nCharset);
  RetainPtr<CPDF_Font> AddFontToDocument(ByteString sFontName,
                                         FX_Charset nCharset);
  RetainPtr<CPDF_Font> AddStandardFont(ByteString sFontName);
  RetainPtr<CPDF_Font> AddSystemFont(ByteString sFontName, FX_Charset nCharset);

  std::vector<std::unique_ptr<Data>> m_Data;
  std::vector<std::unique_ptr<Native>> m_NativeFont;
  UnownedPtr<CPDF_Document> const m_pDocument;
  RetainPtr<CPDF_Dictionary> const m_pAnnotDict;
  RetainPtr<CPDF_Font> m_pDefaultFont;
  ByteString m_sDefaultFontName;
  const ByteString m_sAPType;
};

#endif  // CORE_FPDFDOC_CPDF_BAFONTMAP_H_
