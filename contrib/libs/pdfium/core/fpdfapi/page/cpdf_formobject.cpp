// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/page/cpdf_formobject.h"

#include <utility>

#include "core/fpdfapi/page/cpdf_form.h"

CPDF_FormObject::CPDF_FormObject(int32_t content_stream,
                                 std::unique_ptr<CPDF_Form> pForm,
                                 const CFX_Matrix& matrix)
    : CPDF_PageObject(content_stream),
      m_pForm(std::move(pForm)),
      m_FormMatrix(matrix) {}

CPDF_FormObject::~CPDF_FormObject() = default;

void CPDF_FormObject::Transform(const CFX_Matrix& matrix) {
  m_FormMatrix.Concat(matrix);
  CalcBoundingBox();
  SetDirty(true);
}

bool CPDF_FormObject::IsForm() const {
  return true;
}

CPDF_FormObject* CPDF_FormObject::AsForm() {
  return this;
}

const CPDF_FormObject* CPDF_FormObject::AsForm() const {
  return this;
}

CPDF_PageObject::Type CPDF_FormObject::GetType() const {
  return Type::kForm;
}

void CPDF_FormObject::CalcBoundingBox() {
  SetRect(m_FormMatrix.TransformRect(m_pForm->CalcBoundingBox()));
}

void CPDF_FormObject::SetFormMatrix(const CFX_Matrix& matrix) {
  m_FormMatrix = matrix;
  CalcBoundingBox();
}
