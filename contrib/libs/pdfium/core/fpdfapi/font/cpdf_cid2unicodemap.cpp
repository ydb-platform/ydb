// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/font/cpdf_cid2unicodemap.h"

#include "core/fpdfapi/font/cpdf_fontglobals.h"

CPDF_CID2UnicodeMap::CPDF_CID2UnicodeMap(CIDSet charset)
    : m_Charset(charset),
      m_pEmbeddedMap(
          CPDF_FontGlobals::GetInstance()->GetEmbeddedToUnicode(m_Charset)) {}

CPDF_CID2UnicodeMap::~CPDF_CID2UnicodeMap() = default;

bool CPDF_CID2UnicodeMap::IsLoaded() const {
  return !m_pEmbeddedMap.empty();
}

wchar_t CPDF_CID2UnicodeMap::UnicodeFromCID(uint16_t cid) const {
  if (m_Charset == CIDSET_UNICODE)
    return cid;
  return cid < m_pEmbeddedMap.size() ? m_pEmbeddedMap[cid] : 0;
}
