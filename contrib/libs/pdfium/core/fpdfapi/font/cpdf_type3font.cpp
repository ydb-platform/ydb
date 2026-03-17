// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/font/cpdf_type3font.h"

#include <algorithm>
#include <iterator>
#include <type_traits>
#include <utility>

#include "core/fpdfapi/font/cpdf_type3char.h"
#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fxcrt/autorestorer.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/fx_system.h"

namespace {

constexpr int kMaxType3FormLevel = 4;

}  // namespace

CPDF_Type3Font::CPDF_Type3Font(CPDF_Document* pDocument,
                               RetainPtr<CPDF_Dictionary> pFontDict,
                               FormFactoryIface* pFormFactory)
    : CPDF_SimpleFont(pDocument, std::move(pFontDict)),
      m_pFormFactory(pFormFactory) {
  DCHECK(GetDocument());
}

CPDF_Type3Font::~CPDF_Type3Font() = default;

bool CPDF_Type3Font::IsType3Font() const {
  return true;
}

const CPDF_Type3Font* CPDF_Type3Font::AsType3Font() const {
  return this;
}

CPDF_Type3Font* CPDF_Type3Font::AsType3Font() {
  return this;
}

void CPDF_Type3Font::WillBeDestroyed() {
  m_bWillBeDestroyed = true;

  // Last reference to |this| may be through one of its CPDF_Type3Chars.
  RetainPtr<CPDF_Font> protector(this);
  for (const auto& item : m_CacheMap) {
    if (item.second) {
      item.second->WillBeDestroyed();
    }
  }
}

bool CPDF_Type3Font::Load() {
  m_pFontResources = m_pFontDict->GetMutableDictFor("Resources");
  RetainPtr<const CPDF_Array> pMatrix = m_pFontDict->GetArrayFor("FontMatrix");
  float xscale = 1.0f;
  float yscale = 1.0f;
  if (pMatrix) {
    m_FontMatrix = pMatrix->GetMatrix();
    xscale = m_FontMatrix.a;
    yscale = m_FontMatrix.d;
  }

  RetainPtr<const CPDF_Array> pBBox = m_pFontDict->GetArrayFor("FontBBox");
  if (pBBox) {
    CFX_FloatRect box(
        pBBox->GetFloatAt(0) * xscale, pBBox->GetFloatAt(1) * yscale,
        pBBox->GetFloatAt(2) * xscale, pBBox->GetFloatAt(3) * yscale);
    CPDF_Type3Char::TextUnitRectToGlyphUnitRect(&box);
    m_FontBBox = box.ToFxRect();
  }

  const size_t kCharLimit = m_CharWidthL.size();
  int StartChar = m_pFontDict->GetIntegerFor("FirstChar");
  if (StartChar >= 0 && static_cast<size_t>(StartChar) < kCharLimit) {
    RetainPtr<const CPDF_Array> pWidthArray =
        m_pFontDict->GetArrayFor("Widths");
    if (pWidthArray) {
      size_t count = std::min(pWidthArray->size(), kCharLimit);
      count = std::min(count, kCharLimit - StartChar);
      for (size_t i = 0; i < count; i++) {
        m_CharWidthL[StartChar + i] =
            FXSYS_roundf(CPDF_Type3Char::TextUnitToGlyphUnit(
                pWidthArray->GetFloatAt(i) * xscale));
      }
    }
  }
  m_pCharProcs = m_pFontDict->GetMutableDictFor("CharProcs");
  if (m_pFontDict->GetDirectObjectFor("Encoding"))
    LoadPDFEncoding(false, false);
  return true;
}

void CPDF_Type3Font::LoadGlyphMap() {}

void CPDF_Type3Font::CheckType3FontMetrics() {
  CheckFontMetrics();
}

CPDF_Type3Char* CPDF_Type3Font::LoadChar(uint32_t charcode) {
  if (m_CharLoadingDepth >= kMaxType3FormLevel)
    return nullptr;

  auto it = m_CacheMap.find(charcode);
  if (it != m_CacheMap.end())
    return it->second.get();

  const char* name = GetAdobeCharName(m_BaseEncoding, m_CharNames, charcode);
  if (!name)
    return nullptr;

  if (!m_pCharProcs)
    return nullptr;

  RetainPtr<CPDF_Stream> pStream =
      ToStream(m_pCharProcs->GetMutableDirectObjectFor(name));
  if (!pStream)
    return nullptr;

  std::unique_ptr<CPDF_Font::FormIface> pForm = m_pFormFactory->CreateForm(
      m_pDocument, m_pFontResources ? m_pFontResources : m_pPageResources,
      pStream);

  auto pNewChar = std::make_unique<CPDF_Type3Char>();

  // This can trigger recursion into this method. The content of |m_CacheMap|
  // can change as a result. Thus after it returns, check the cache again for
  // a cache hit.
  {
    AutoRestorer<int> restorer(&m_CharLoadingDepth);
    m_CharLoadingDepth++;
    pForm->ParseContentForType3Char(pNewChar.get());
  }
  it = m_CacheMap.find(charcode);
  if (it != m_CacheMap.end())
    return it->second.get();

  pNewChar->Transform(pForm.get(), m_FontMatrix);
  if (pForm->HasPageObjects())
    pNewChar->SetForm(std::move(pForm));

  CPDF_Type3Char* pCachedChar = pNewChar.get();
  m_CacheMap[charcode] = std::move(pNewChar);
  return pCachedChar;
}

int CPDF_Type3Font::GetCharWidthF(uint32_t charcode) {
  if (charcode >= std::size(m_CharWidthL))
    charcode = 0;

  if (m_CharWidthL[charcode])
    return m_CharWidthL[charcode];

  const CPDF_Type3Char* pChar = LoadChar(charcode);
  return pChar ? pChar->width() : 0;
}

FX_RECT CPDF_Type3Font::GetCharBBox(uint32_t charcode) {
  FX_RECT ret;
  const CPDF_Type3Char* pChar = LoadChar(charcode);
  if (pChar)
    ret = pChar->bbox();
  return ret;
}
