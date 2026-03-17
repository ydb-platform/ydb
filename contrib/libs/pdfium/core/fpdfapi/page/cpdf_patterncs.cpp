// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/page/cpdf_patterncs.h"

#include <optional>

#include "core/fpdfapi/page/cpdf_docpagedata.h"
#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fxcrt/notreached.h"

CPDF_PatternCS::CPDF_PatternCS() : CPDF_BasedCS(Family::kPattern) {}

CPDF_PatternCS::~CPDF_PatternCS() = default;

void CPDF_PatternCS::InitializeStockPattern() {
  SetComponentsForStockCS(1);
}

uint32_t CPDF_PatternCS::v_Load(CPDF_Document* pDoc,
                                const CPDF_Array* pArray,
                                std::set<const CPDF_Object*>* pVisited) {
  RetainPtr<const CPDF_Object> pBaseCS = pArray->GetDirectObjectAt(1);
  if (HasSameArray(pBaseCS.Get()))
    return 0;

  auto* pDocPageData = CPDF_DocPageData::FromDocument(pDoc);
  m_pBaseCS =
      pDocPageData->GetColorSpaceGuarded(pBaseCS.Get(), nullptr, pVisited);
  if (!m_pBaseCS)
    return 1;

  if (m_pBaseCS->GetFamily() == Family::kPattern)
    return 0;

  if (m_pBaseCS->ComponentCount() > kMaxPatternColorComps) {
    return 0;
  }

  return m_pBaseCS->ComponentCount() + 1;
}

std::optional<FX_RGB_STRUCT<float>> CPDF_PatternCS::GetRGB(
    pdfium::span<const float> pBuf) const {
  NOTREACHED_NORETURN();
}

const CPDF_PatternCS* CPDF_PatternCS::AsPatternCS() const {
  return this;
}

std::optional<FX_RGB_STRUCT<float>> CPDF_PatternCS::GetPatternRGB(
    const PatternValue& value) const {
  if (!m_pBaseCS) {
    return std::nullopt;
  }

  return m_pBaseCS->GetRGB(value.GetComps());
}
