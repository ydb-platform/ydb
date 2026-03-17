// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/page/cpdf_pattern.h"

#include <utility>

#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fxcrt/check.h"

CPDF_Pattern::CPDF_Pattern(CPDF_Document* pDoc,
                           RetainPtr<CPDF_Object> pObj,
                           const CFX_Matrix& parentMatrix)
    : m_pDocument(pDoc),
      m_pPatternObj(std::move(pObj)),
      m_ParentMatrix(parentMatrix) {
  DCHECK(m_pDocument);
  DCHECK(m_pPatternObj);
}

CPDF_Pattern::~CPDF_Pattern() = default;

CPDF_TilingPattern* CPDF_Pattern::AsTilingPattern() {
  return nullptr;
}

CPDF_ShadingPattern* CPDF_Pattern::AsShadingPattern() {
  return nullptr;
}

void CPDF_Pattern::SetPatternToFormMatrix() {
  RetainPtr<const CPDF_Dictionary> pDict = pattern_obj()->GetDict();
  m_Pattern2Form = pDict->GetMatrixFor("Matrix") * m_ParentMatrix;
}
