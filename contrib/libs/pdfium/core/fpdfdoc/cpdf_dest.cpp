// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfdoc/cpdf_dest.h"

#include <algorithm>
#include <iterator>
#include <utility>

#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fpdfapi/parser/cpdf_name.h"
#include "core/fpdfapi/parser/cpdf_number.h"
#include "core/fpdfdoc/cpdf_nametree.h"
#include "core/fxcrt/stl_util.h"

namespace {

// These arrays are indexed by the PDFDEST_VIEW_* constants.

constexpr auto kZoomModes =
    fxcrt::ToArray<const char*>({"Unknown", "XYZ", "Fit", "FitH", "FitV",
                                 "FitR", "FitB", "FitBH", "FitBV"});

constexpr auto kZoomModeMaxParamCount =
    fxcrt::ToArray<const uint8_t>({0, 3, 0, 1, 1, 4, 0, 1, 1});

}  // namespace

CPDF_Dest::CPDF_Dest(RetainPtr<const CPDF_Array> pArray)
    : m_pArray(std::move(pArray)) {}

CPDF_Dest::CPDF_Dest(const CPDF_Dest& that) = default;

CPDF_Dest::~CPDF_Dest() = default;

// static
CPDF_Dest CPDF_Dest::Create(CPDF_Document* pDoc,
                            RetainPtr<const CPDF_Object> pDest) {
  if (!pDest)
    return CPDF_Dest(nullptr);

  if (pDest->IsString() || pDest->IsName())
    return CPDF_Dest(CPDF_NameTree::LookupNamedDest(pDoc, pDest->GetString()));

  return CPDF_Dest(ToArray(pDest));
}

int CPDF_Dest::GetDestPageIndex(CPDF_Document* pDoc) const {
  if (!m_pArray)
    return -1;

  RetainPtr<const CPDF_Object> pPage = m_pArray->GetDirectObjectAt(0);
  if (!pPage)
    return -1;

  if (pPage->IsNumber())
    return pPage->GetInteger();

  if (!pPage->IsDictionary())
    return -1;

  return pDoc->GetPageIndex(pPage->GetObjNum());
}

std::vector<float> CPDF_Dest::GetScrollPositionArray() const {
  std::vector<float> result;
  if (m_pArray) {
    // Skip over index 0 which contains destination page details, and index 1
    // which contains a parameter that describes the rest of the array.
    for (size_t i = 2; i < m_pArray->size(); i++)
      result.push_back(m_pArray->GetFloatAt(i));
  }
  return result;
}

int CPDF_Dest::GetZoomMode() const {
  if (!m_pArray) {
    return 0;
  }
  RetainPtr<const CPDF_Object> pArray = m_pArray->GetDirectObjectAt(1);
  if (!pArray) {
    return 0;
  }
  ByteString mode = pArray->GetString();
  for (size_t i = 1; i < std::size(kZoomModes); ++i) {
    if (mode == kZoomModes[i]) {
      return static_cast<int>(i);
    }
  }
  return 0;
}

bool CPDF_Dest::GetXYZ(bool* pHasX,
                       bool* pHasY,
                       bool* pHasZoom,
                       float* pX,
                       float* pY,
                       float* pZoom) const {
  *pHasX = false;
  *pHasY = false;
  *pHasZoom = false;

  if (!m_pArray)
    return false;

  if (m_pArray->size() < 5)
    return false;

  RetainPtr<const CPDF_Name> xyz = ToName(m_pArray->GetDirectObjectAt(1));
  if (!xyz || xyz->GetString() != "XYZ")
    return false;

  RetainPtr<const CPDF_Number> numX = ToNumber(m_pArray->GetDirectObjectAt(2));
  RetainPtr<const CPDF_Number> numY = ToNumber(m_pArray->GetDirectObjectAt(3));
  RetainPtr<const CPDF_Number> numZoom =
      ToNumber(m_pArray->GetDirectObjectAt(4));

  // If the value is a CPDF_Null then ToNumber will return nullptr.
  *pHasX = !!numX;
  *pHasY = !!numY;
  *pHasZoom = !!numZoom;

  if (numX)
    *pX = numX->GetNumber();
  if (numY)
    *pY = numY->GetNumber();

  // A zoom value of 0 is equivalent to a null value, so treat it as a null.
  if (numZoom) {
    float num = numZoom->GetNumber();
    if (num == 0.0)
      *pHasZoom = false;
    else
      *pZoom = num;
  }

  return true;
}

size_t CPDF_Dest::GetNumParams() const {
  if (!m_pArray || m_pArray->size() < 2)
    return 0;

  size_t maxParamsForFitType = kZoomModeMaxParamCount[GetZoomMode()];
  size_t numParamsInArray = m_pArray->size() - 2;
  return std::min(maxParamsForFitType, numParamsInArray);
}

float CPDF_Dest::GetParam(size_t index) const {
  return m_pArray ? m_pArray->GetFloatAt(2 + index) : 0;
}
