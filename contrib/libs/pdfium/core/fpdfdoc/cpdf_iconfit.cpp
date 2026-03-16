// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfdoc/cpdf_iconfit.h"

#include <algorithm>
#include <utility>

#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fxcrt/fx_string.h"

namespace {

constexpr float kDefaultPosition = 0.5f;

}  // namespace

CPDF_IconFit::CPDF_IconFit(RetainPtr<const CPDF_Dictionary> pDict)
    : m_pDict(std::move(pDict)) {}

CPDF_IconFit::CPDF_IconFit(const CPDF_IconFit& that) = default;

CPDF_IconFit::~CPDF_IconFit() = default;

CPDF_IconFit::ScaleMethod CPDF_IconFit::GetScaleMethod() const {
  if (!m_pDict)
    return ScaleMethod::kAlways;

  ByteString csSW = m_pDict->GetByteStringFor("SW", "A");
  if (csSW == "B")
    return ScaleMethod::kBigger;
  if (csSW == "S")
    return ScaleMethod::kSmaller;
  if (csSW == "N")
    return ScaleMethod::kNever;
  return ScaleMethod::kAlways;
}

bool CPDF_IconFit::IsProportionalScale() const {
  return !m_pDict || m_pDict->GetByteStringFor("S", "P") != "A";
}

CFX_PointF CPDF_IconFit::GetIconBottomLeftPosition() const {
  float fLeft = kDefaultPosition;
  float fBottom = kDefaultPosition;
  if (!m_pDict)
    return {fLeft, fBottom};

  RetainPtr<const CPDF_Array> pA = m_pDict->GetArrayFor("A");
  if (!pA)
    return {fLeft, fBottom};

  size_t dwCount = pA->size();
  if (dwCount > 0)
    fLeft = pA->GetFloatAt(0);
  if (dwCount > 1)
    fBottom = pA->GetFloatAt(1);
  return {fLeft, fBottom};
}

bool CPDF_IconFit::GetFittingBounds() const {
  return m_pDict && m_pDict->GetBooleanFor("FB", false);
}

CFX_PointF CPDF_IconFit::GetIconPosition() const {
  if (!m_pDict)
    return CFX_PointF();

  RetainPtr<const CPDF_Array> pA = m_pDict->GetArrayFor("A");
  if (!pA)
    return CFX_PointF();

  size_t dwCount = pA->size();
  return {dwCount > 0 ? pA->GetFloatAt(0) : 0.0f,
          dwCount > 1 ? pA->GetFloatAt(1) : 0.0f};
}

CFX_VectorF CPDF_IconFit::GetScale(const CFX_SizeF& image_size,
                                   const CFX_FloatRect& rcPlate) const {
  float fHScale = 1.0f;
  float fVScale = 1.0f;
  const float fPlateWidth = rcPlate.Width();
  const float fPlateHeight = rcPlate.Height();
  const float fImageWidth = image_size.width;
  const float fImageHeight = image_size.height;
  switch (GetScaleMethod()) {
    case CPDF_IconFit::ScaleMethod::kAlways:
      fHScale = fPlateWidth / std::max(fImageWidth, 1.0f);
      fVScale = fPlateHeight / std::max(fImageHeight, 1.0f);
      break;
    case CPDF_IconFit::ScaleMethod::kBigger:
      if (fPlateWidth < fImageWidth)
        fHScale = fPlateWidth / std::max(fImageWidth, 1.0f);
      if (fPlateHeight < fImageHeight)
        fVScale = fPlateHeight / std::max(fImageHeight, 1.0f);
      break;
    case CPDF_IconFit::ScaleMethod::kSmaller:
      if (fPlateWidth > fImageWidth)
        fHScale = fPlateWidth / std::max(fImageWidth, 1.0f);
      if (fPlateHeight > fImageHeight)
        fVScale = fPlateHeight / std::max(fImageHeight, 1.0f);
      break;
    case CPDF_IconFit::ScaleMethod::kNever:
      break;
  }

  if (IsProportionalScale()) {
    float min_scale = std::min(fHScale, fVScale);
    fHScale = min_scale;
    fVScale = min_scale;
  }
  return {fHScale, fVScale};
}

CFX_VectorF CPDF_IconFit::GetImageOffset(const CFX_SizeF& image_size,
                                         const CFX_VectorF& scale,
                                         const CFX_FloatRect& rcPlate) const {
  const CFX_PointF icon_position = GetIconPosition();
  const float fImageFactWidth = image_size.width * scale.x;
  const float fImageFactHeight = image_size.height * scale.y;
  return {(rcPlate.Width() - fImageFactWidth) * icon_position.x,
          (rcPlate.Height() - fImageFactHeight) * icon_position.y};
}
