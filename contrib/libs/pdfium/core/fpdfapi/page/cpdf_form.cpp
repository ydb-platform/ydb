// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/page/cpdf_form.h"

#include <algorithm>
#include <memory>

#include "core/fpdfapi/page/cpdf_contentparser.h"
#include "core/fpdfapi/page/cpdf_imageobject.h"
#include "core/fpdfapi/page/cpdf_pageobject.h"
#include "core/fpdfapi/page/cpdf_pageobjectholder.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fxcrt/check_op.h"
#include "core/fxge/dib/cfx_dibitmap.h"

CPDF_Form::RecursionState::RecursionState() = default;

CPDF_Form::RecursionState::~RecursionState() = default;

// static
CPDF_Dictionary* CPDF_Form::ChooseResourcesDict(
    CPDF_Dictionary* pResources,
    CPDF_Dictionary* pParentResources,
    CPDF_Dictionary* pPageResources) {
  if (pResources)
    return pResources;
  return pParentResources ? pParentResources : pPageResources;
}

CPDF_Form::CPDF_Form(CPDF_Document* pDoc,
                     RetainPtr<CPDF_Dictionary> pPageResources,
                     RetainPtr<CPDF_Stream> pFormStream)
    : CPDF_Form(pDoc,
                std::move(pPageResources),
                std::move(pFormStream),
                nullptr) {}

CPDF_Form::CPDF_Form(CPDF_Document* pDoc,
                     RetainPtr<CPDF_Dictionary> pPageResources,
                     RetainPtr<CPDF_Stream> pFormStream,
                     CPDF_Dictionary* pParentResources)
    : CPDF_PageObjectHolder(pDoc,
                            pFormStream->GetMutableDict(),
                            pPageResources,
                            pdfium::WrapRetain(ChooseResourcesDict(
                                pFormStream->GetMutableDict()
                                    ->GetMutableDictFor("Resources")
                                    .Get(),
                                pParentResources,
                                pPageResources.Get()))),
      m_pFormStream(std::move(pFormStream)) {
  LoadTransparencyInfo();
}

CPDF_Form::~CPDF_Form() = default;

void CPDF_Form::ParseContent() {
  ParseContentInternal(nullptr, nullptr, nullptr, nullptr);
}

void CPDF_Form::ParseContent(const CPDF_AllStates* pGraphicStates,
                             const CFX_Matrix* pParentMatrix,
                             RecursionState* recursion_state) {
  ParseContentInternal(pGraphicStates, pParentMatrix, nullptr, recursion_state);
}

void CPDF_Form::ParseContentForType3Char(CPDF_Type3Char* pType3Char) {
  ParseContentInternal(nullptr, nullptr, pType3Char, nullptr);
}

void CPDF_Form::ParseContentInternal(const CPDF_AllStates* pGraphicStates,
                                     const CFX_Matrix* pParentMatrix,
                                     CPDF_Type3Char* pType3Char,
                                     RecursionState* recursion_state) {
  if (GetParseState() == ParseState::kParsed)
    return;

  if (GetParseState() == ParseState::kNotParsed) {
    StartParse(std::make_unique<CPDF_ContentParser>(
        GetStream(), this, pGraphicStates, pParentMatrix, pType3Char,
        recursion_state ? recursion_state : &m_RecursionState));
  }
  DCHECK_EQ(GetParseState(), ParseState::kParsing);
  ContinueParse(nullptr);
}

bool CPDF_Form::HasPageObjects() const {
  return GetPageObjectCount() != 0;
}

CFX_FloatRect CPDF_Form::CalcBoundingBox() const {
  if (GetPageObjectCount() == 0)
    return CFX_FloatRect();

  float left = 1000000.0f;
  float right = -1000000.0f;
  float bottom = 1000000.0f;
  float top = -1000000.0f;
  for (const auto& pObj : *this) {
    const auto& rect = pObj->GetRect();
    left = std::min(left, rect.left);
    right = std::max(right, rect.right);
    bottom = std::min(bottom, rect.bottom);
    top = std::max(top, rect.top);
  }
  return CFX_FloatRect(left, bottom, right, top);
}

RetainPtr<const CPDF_Stream> CPDF_Form::GetStream() const {
  return m_pFormStream;
}

std::optional<std::pair<RetainPtr<CFX_DIBitmap>, CFX_Matrix>>
CPDF_Form::GetBitmapAndMatrixFromSoleImageOfForm() const {
  if (GetPageObjectCount() != 1)
    return std::nullopt;

  CPDF_ImageObject* pImageObject = (*begin())->AsImage();
  if (!pImageObject)
    return std::nullopt;

  return {{pImageObject->GetIndependentBitmap(), pImageObject->matrix()}};
}
