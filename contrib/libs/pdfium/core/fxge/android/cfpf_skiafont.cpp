// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxge/android/cfpf_skiafont.h"

#include <algorithm>

#include "core/fxcrt/fx_codepage.h"
#include "core/fxcrt/fx_coordinates.h"
#include "core/fxcrt/fx_system.h"
#include "core/fxcrt/numerics/safe_conversions.h"
#include "core/fxge/android/cfpf_skiafontmgr.h"
#include "core/fxge/android/cfpf_skiapathfont.h"
#include "core/fxge/fx_fontencoding.h"

CFPF_SkiaFont::CFPF_SkiaFont(CFPF_SkiaFontMgr* pFontMgr,
                             const CFPF_SkiaPathFont* pFont,
                             FX_Charset uCharset)
    : m_pFontMgr(pFontMgr),
      m_pFont(pFont),
      m_Face(m_pFontMgr->GetFontFace(m_pFont->path(), m_pFont->face_index())),
      m_uCharset(uCharset) {}

CFPF_SkiaFont::~CFPF_SkiaFont() = default;

ByteString CFPF_SkiaFont::GetFamilyName() {
  if (!m_Face)
    return ByteString();
  return m_Face->GetFamilyName();
}

uint32_t CFPF_SkiaFont::GetFontData(uint32_t dwTable,
                                    pdfium::span<uint8_t> pBuffer) {
  if (!m_Face)
    return 0;

  return pdfium::checked_cast<uint32_t>(m_Face->GetSfntTable(dwTable, pBuffer));
}
