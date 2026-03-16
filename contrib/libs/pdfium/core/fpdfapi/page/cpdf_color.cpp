// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/page/cpdf_color.h"

#include <optional>
#include <utility>

#include "core/fpdfapi/page/cpdf_patterncs.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/check_op.h"

CPDF_Color::CPDF_Color() = default;

CPDF_Color::CPDF_Color(const CPDF_Color& that) {
  *this = that;
}

CPDF_Color::~CPDF_Color() = default;

bool CPDF_Color::IsNull() const {
  return absl::holds_alternative<absl::monostate>(color_data_);
}

bool CPDF_Color::IsPattern() const {
  return cs_ && IsPatternInternal();
}

bool CPDF_Color::IsPatternInternal() const {
  return cs_->GetFamily() == CPDF_ColorSpace::Family::kPattern;
}

void CPDF_Color::SetColorSpace(RetainPtr<CPDF_ColorSpace> colorspace) {
  cs_ = std::move(colorspace);
  if (IsPatternInternal()) {
    color_data_ = std::make_unique<PatternValue>();
  } else {
    color_data_ = cs_->CreateBufAndSetDefaultColor();
  }
}

void CPDF_Color::SetValueForNonPattern(std::vector<float> values) {
  CHECK(!IsPatternInternal());
  CHECK_LE(cs_->ComponentCount(), values.size());
  color_data_ = std::move(values);
}

void CPDF_Color::SetValueForPattern(RetainPtr<CPDF_Pattern> pattern,
                                    pdfium::span<float> values) {
  if (values.size() > kMaxPatternColorComps) {
    return;
  }

  if (!IsPattern()) {
    SetColorSpace(
        CPDF_ColorSpace::GetStockCS(CPDF_ColorSpace::Family::kPattern));
  }

  auto& pattern_value = absl::get<std::unique_ptr<PatternValue>>(color_data_);
  pattern_value->SetPattern(std::move(pattern));
  pattern_value->SetComps(values);
}

CPDF_Color& CPDF_Color::operator=(const CPDF_Color& that) {
  if (this == &that) {
    return *this;
  }

  cs_ = that.cs_;

  if (absl::holds_alternative<std::vector<float>>(that.color_data_)) {
    color_data_ = absl::get<std::vector<float>>(that.color_data_);
  } else if (absl::holds_alternative<std::unique_ptr<PatternValue>>(
                 that.color_data_)) {
    auto& pattern_value =
        absl::get<std::unique_ptr<PatternValue>>(that.color_data_);
    color_data_ = std::make_unique<PatternValue>(*pattern_value);
  } else {
    color_data_ = absl::monostate();
  }

  return *this;
}

uint32_t CPDF_Color::ComponentCount() const {
  return cs_->ComponentCount();
}

bool CPDF_Color::IsColorSpaceRGB() const {
  return cs_ ==
         CPDF_ColorSpace::GetStockCS(CPDF_ColorSpace::Family::kDeviceRGB);
}

bool CPDF_Color::IsColorSpaceGray() const {
  return cs_ ==
         CPDF_ColorSpace::GetStockCS(CPDF_ColorSpace::Family::kDeviceGray);
}

std::optional<FX_COLORREF> CPDF_Color::GetColorRef() const {
  std::optional<FX_RGB_STRUCT<float>> maybe_rgb = GetRGB();
  if (!maybe_rgb.has_value()) {
    return std::nullopt;
  }

  const float r = std::clamp(maybe_rgb.value().red, 0.0f, 1.0f);
  const float g = std::clamp(maybe_rgb.value().green, 0.0f, 1.0f);
  const float b = std::clamp(maybe_rgb.value().blue, 0.0f, 1.0f);
  return FXSYS_BGR(FXSYS_roundf(b * 255.0f), FXSYS_roundf(g * 255.0f),
                   FXSYS_roundf(r * 255.0f));
}

std::optional<FX_RGB_STRUCT<float>> CPDF_Color::GetRGB() const {
  if (IsPatternInternal()) {
    if (absl::holds_alternative<std::unique_ptr<PatternValue>>(color_data_)) {
      const auto& pattern_value =
          absl::get<std::unique_ptr<PatternValue>>(color_data_);
      return cs_->AsPatternCS()->GetPatternRGB(*pattern_value);
    }
  } else {
    if (absl::holds_alternative<std::vector<float>>(color_data_)) {
      const auto& buffer = absl::get<std::vector<float>>(color_data_);
      return cs_->GetRGB(buffer);
    }
  }
  return std::nullopt;
}

RetainPtr<CPDF_Pattern> CPDF_Color::GetPattern() const {
  DCHECK(IsPattern());

  const auto& pattern_value =
      absl::get<std::unique_ptr<PatternValue>>(color_data_);
  return pattern_value->GetPattern();
}
