// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_FONT_CPDF_CID2UNICODEMAP_H_
#define CORE_FPDFAPI_FONT_CPDF_CID2UNICODEMAP_H_

#include "core/fpdfapi/font/cpdf_cidfont.h"
#include "core/fxcrt/raw_span.h"

class CPDF_CID2UnicodeMap {
 public:
  explicit CPDF_CID2UnicodeMap(CIDSet charset);
  ~CPDF_CID2UnicodeMap();

  bool IsLoaded() const;
  wchar_t UnicodeFromCID(uint16_t cid) const;

 private:
  const CIDSet m_Charset;
  const pdfium::raw_span<const uint16_t> m_pEmbeddedMap;
};

#endif  // CORE_FPDFAPI_FONT_CPDF_CID2UNICODEMAP_H_
