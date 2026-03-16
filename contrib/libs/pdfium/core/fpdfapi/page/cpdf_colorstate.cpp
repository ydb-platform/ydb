// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/page/cpdf_colorstate.h"

#include <optional>
#include <utility>

#include "core/fpdfapi/page/cpdf_colorspace.h"
#include "core/fpdfapi/page/cpdf_pattern.h"
#include "core/fpdfapi/page/cpdf_tilingpattern.h"
#include "core/fxge/dib/fx_dib.h"

CPDF_ColorState::CPDF_ColorState() = default;

CPDF_ColorState::CPDF_ColorState(const CPDF_ColorState& that) = default;

CPDF_ColorState::~CPDF_ColorState() = default;

void CPDF_ColorState::Emplace() {
  m_Ref.Emplace();
}

void CPDF_ColorState::SetDefault() {
  m_Ref.GetPrivateCopy()->SetDefault();
}

FX_COLORREF CPDF_ColorState::GetFillColorRef() const {
  return m_Ref.GetObject()->m_FillColorRef;
}

void CPDF_ColorState::SetFillColorRef(FX_COLORREF colorref) {
  if (!m_Ref || GetFillColorRef() != colorref) {
    m_Ref.GetPrivateCopy()->m_FillColorRef = colorref;
  }
}

FX_COLORREF CPDF_ColorState::GetStrokeColorRef() const {
  return m_Ref.GetObject()->m_StrokeColorRef;
}

void CPDF_ColorState::SetStrokeColorRef(FX_COLORREF colorref) {
  if (!m_Ref || GetStrokeColorRef() != colorref) {
    m_Ref.GetPrivateCopy()->m_StrokeColorRef = colorref;
  }
}

const CPDF_Color* CPDF_ColorState::GetFillColor() const {
  const ColorData* data = m_Ref.GetObject();
  return data ? &data->m_FillColor : nullptr;
}

CPDF_Color* CPDF_ColorState::GetMutableFillColor() {
  return &m_Ref.GetPrivateCopy()->m_FillColor;
}

bool CPDF_ColorState::HasFillColor() const {
  const CPDF_Color* pColor = GetFillColor();
  return pColor && !pColor->IsNull();
}

const CPDF_Color* CPDF_ColorState::GetStrokeColor() const {
  const ColorData* data = m_Ref.GetObject();
  return data ? &data->m_StrokeColor : nullptr;
}

CPDF_Color* CPDF_ColorState::GetMutableStrokeColor() {
  return &m_Ref.GetPrivateCopy()->m_StrokeColor;
}

bool CPDF_ColorState::HasStrokeColor() const {
  const CPDF_Color* pColor = GetStrokeColor();
  return pColor && !pColor->IsNull();
}

void CPDF_ColorState::SetFillColor(RetainPtr<CPDF_ColorSpace> colorspace,
                                   std::vector<float> values) {
  ColorData* data = m_Ref.GetPrivateCopy();
  std::optional<FX_COLORREF> colorref =
      SetColor(std::move(colorspace), std::move(values), data->m_FillColor);
  if (colorref.has_value()) {
    data->m_FillColorRef = colorref.value();
  }
}

void CPDF_ColorState::SetStrokeColor(RetainPtr<CPDF_ColorSpace> colorspace,
                                     std::vector<float> values) {
  ColorData* data = m_Ref.GetPrivateCopy();
  std::optional<FX_COLORREF> colorref =
      SetColor(std::move(colorspace), std::move(values), data->m_StrokeColor);
  if (colorref.has_value()) {
    data->m_StrokeColorRef = colorref.value();
  }
}

void CPDF_ColorState::SetFillPattern(RetainPtr<CPDF_Pattern> pattern,
                                     pdfium::span<float> values) {
  ColorData* data = m_Ref.GetPrivateCopy();
  data->m_FillColorRef =
      SetPattern(std::move(pattern), values, data->m_FillColor);
}

void CPDF_ColorState::SetStrokePattern(RetainPtr<CPDF_Pattern> pattern,
                                       pdfium::span<float> values) {
  ColorData* data = m_Ref.GetPrivateCopy();
  data->m_StrokeColorRef =
      SetPattern(std::move(pattern), values, data->m_StrokeColor);
}

std::optional<FX_COLORREF> CPDF_ColorState::SetColor(
    RetainPtr<CPDF_ColorSpace> colorspace,
    std::vector<float> values,
    CPDF_Color& color) {
  if (colorspace) {
    color.SetColorSpace(std::move(colorspace));
  } else if (color.IsNull()) {
    color.SetColorSpace(
        CPDF_ColorSpace::GetStockCS(CPDF_ColorSpace::Family::kDeviceGray));
  }
  if (color.ComponentCount() > values.size()) {
    return std::nullopt;
  }

  if (!color.IsPattern()) {
    color.SetValueForNonPattern(std::move(values));
  }
  return color.GetColorRef().value_or(0xFFFFFFFF);
}

FX_COLORREF CPDF_ColorState::SetPattern(RetainPtr<CPDF_Pattern> pattern,
                                        pdfium::span<float> values,
                                        CPDF_Color& color) {
  color.SetValueForPattern(pattern, values);
  std::optional<FX_COLORREF> colorref = color.GetColorRef();
  if (colorref.has_value()) {
    return colorref.value();
  }

  CPDF_TilingPattern* tiling = pattern->AsTilingPattern();
  return tiling && tiling->colored() ? 0x00BFBFBF : 0xFFFFFFFF;
}

CPDF_ColorState::ColorData::ColorData() = default;

CPDF_ColorState::ColorData::ColorData(const ColorData& src)
    : m_FillColorRef(src.m_FillColorRef),
      m_StrokeColorRef(src.m_StrokeColorRef),
      m_FillColor(src.m_FillColor),
      m_StrokeColor(src.m_StrokeColor) {}

CPDF_ColorState::ColorData::~ColorData() = default;

void CPDF_ColorState::ColorData::SetDefault() {
  m_FillColorRef = 0;
  m_StrokeColorRef = 0;
  m_FillColor.SetColorSpace(
      CPDF_ColorSpace::GetStockCS(CPDF_ColorSpace::Family::kDeviceGray));
  m_StrokeColor.SetColorSpace(
      CPDF_ColorSpace::GetStockCS(CPDF_ColorSpace::Family::kDeviceGray));
}

RetainPtr<CPDF_ColorState::ColorData> CPDF_ColorState::ColorData::Clone()
    const {
  return pdfium::MakeRetain<CPDF_ColorState::ColorData>(*this);
}
