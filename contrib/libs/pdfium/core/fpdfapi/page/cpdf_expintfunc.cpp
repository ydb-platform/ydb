// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/page/cpdf_expintfunc.h"

#include <math.h>

#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_number.h"
#include "core/fxcrt/data_vector.h"
#include "core/fxcrt/fx_2d_size.h"
#include "core/fxcrt/fx_safe_types.h"
#include "core/fxcrt/stl_util.h"

CPDF_ExpIntFunc::CPDF_ExpIntFunc()
    : CPDF_Function(Type::kType2ExponentialInterpolation) {}

CPDF_ExpIntFunc::~CPDF_ExpIntFunc() = default;

bool CPDF_ExpIntFunc::v_Init(const CPDF_Object* pObj, VisitedSet* pVisited) {
  CHECK(pObj->IsDictionary() || pObj->IsStream());
  RetainPtr<const CPDF_Dictionary> pDict = pObj->GetDict();
  RetainPtr<const CPDF_Number> pExponent = pDict->GetNumberFor("N");
  if (!pExponent)
    return false;

  m_Exponent = pExponent->GetNumber();

  RetainPtr<const CPDF_Array> pArray0 = pDict->GetArrayFor("C0");
  if (pArray0 && m_nOutputs == 0)
    m_nOutputs = fxcrt::CollectionSize<uint32_t>(*pArray0);
  if (m_nOutputs == 0)
    m_nOutputs = 1;

  RetainPtr<const CPDF_Array> pArray1 = pDict->GetArrayFor("C1");
  m_BeginValues = DataVector<float>(Fx2DSizeOrDie(m_nOutputs, 2));
  m_EndValues = DataVector<float>(m_BeginValues.size());
  for (uint32_t i = 0; i < m_nOutputs; i++) {
    m_BeginValues[i] = pArray0 ? pArray0->GetFloatAt(i) : 0.0f;
    m_EndValues[i] = pArray1 ? pArray1->GetFloatAt(i) : 1.0f;
  }

  FX_SAFE_UINT32 nOutputs = m_nOutputs;
  nOutputs *= m_nInputs;
  if (!nOutputs.IsValid())
    return false;

  m_nOrigOutputs = m_nOutputs;
  m_nOutputs = nOutputs.ValueOrDie();
  return true;
}

bool CPDF_ExpIntFunc::v_Call(pdfium::span<const float> inputs,
                             pdfium::span<float> results) const {
  for (uint32_t i = 0; i < m_nInputs; i++) {
    for (uint32_t j = 0; j < m_nOrigOutputs; j++) {
      results[i * m_nOrigOutputs + j] =
          m_BeginValues[j] +
          powf(inputs[i], m_Exponent) * (m_EndValues[j] - m_BeginValues[j]);
    }
  }
  return true;
}
