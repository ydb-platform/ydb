// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/page/cpdf_stitchfunc.h"

#include <utility>

#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/fpdf_parser_utility.h"
#include "core/fxcrt/fx_safe_types.h"
#include "core/fxcrt/stl_util.h"

namespace {

constexpr uint32_t kRequiredNumInputs = 1;

}  // namespace

CPDF_StitchFunc::CPDF_StitchFunc() : CPDF_Function(Type::kType3Stitching) {}

CPDF_StitchFunc::~CPDF_StitchFunc() = default;

bool CPDF_StitchFunc::v_Init(const CPDF_Object* pObj, VisitedSet* pVisited) {
  if (m_nInputs != kRequiredNumInputs)
    return false;

  CHECK(pObj->IsDictionary() || pObj->IsStream());
  RetainPtr<const CPDF_Dictionary> pDict = pObj->GetDict();
  RetainPtr<const CPDF_Array> pFunctionsArray = pDict->GetArrayFor("Functions");
  if (!pFunctionsArray)
    return false;

  RetainPtr<const CPDF_Array> pBoundsArray = pDict->GetArrayFor("Bounds");
  if (!pBoundsArray)
    return false;

  RetainPtr<const CPDF_Array> pEncodeArray = pDict->GetArrayFor("Encode");
  if (!pEncodeArray)
    return false;

  const uint32_t nSubs = fxcrt::CollectionSize<uint32_t>(*pFunctionsArray);
  if (nSubs == 0)
    return false;

  // Check array sizes. The checks are slightly relaxed to allow the "Bounds"
  // and "Encode" arrays to have more than the required number of elements.
  {
    if (pBoundsArray->size() < nSubs - 1)
      return false;

    FX_SAFE_UINT32 nExpectedEncodeSize = nSubs;
    nExpectedEncodeSize *= 2;
    if (!nExpectedEncodeSize.IsValid())
      return false;

    if (pEncodeArray->size() < nExpectedEncodeSize.ValueOrDie())
      return false;
  }

  // Check sub-functions.
  {
    std::optional<uint32_t> nOutputs;
    for (uint32_t i = 0; i < nSubs; ++i) {
      RetainPtr<const CPDF_Object> pSub = pFunctionsArray->GetDirectObjectAt(i);
      if (pSub == pObj)
        return false;

      std::unique_ptr<CPDF_Function> pFunc =
          CPDF_Function::Load(std::move(pSub), pVisited);
      if (!pFunc)
        return false;

      // Check that the input dimensionality is 1, and that all output
      // dimensionalities are the same.
      if (pFunc->InputCount() != kRequiredNumInputs) {
        return false;
      }

      uint32_t nFuncOutputs = pFunc->OutputCount();
      if (nFuncOutputs == 0)
        return false;

      if (nOutputs.has_value()) {
        if (nOutputs != nFuncOutputs)
          return false;
      } else {
        nOutputs = nFuncOutputs;
      }
      m_pSubFunctions.push_back(std::move(pFunc));
    }
    m_nOutputs = nOutputs.value();
  }

  m_bounds.reserve(nSubs + 1);
  m_bounds.push_back(m_Domains[0]);
  for (uint32_t i = 0; i < nSubs - 1; i++)
    m_bounds.push_back(pBoundsArray->GetFloatAt(i));
  m_bounds.push_back(m_Domains[1]);

  m_encode = ReadArrayElementsToVector(pEncodeArray.Get(), nSubs * 2);
  return true;
}

bool CPDF_StitchFunc::v_Call(pdfium::span<const float> inputs,
                             pdfium::span<float> results) const {
  float input = inputs[0];
  size_t i;
  for (i = 0; i + 1 < m_pSubFunctions.size(); i++) {
    if (input < m_bounds[i + 1])
      break;
  }
  input = Interpolate(input, m_bounds[i], m_bounds[i + 1], m_encode[i * 2],
                      m_encode[i * 2 + 1]);
  return m_pSubFunctions[i]
      ->Call(pdfium::span_from_ref(input), results)
      .has_value();
}
