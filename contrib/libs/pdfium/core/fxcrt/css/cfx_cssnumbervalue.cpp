// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcrt/css/cfx_cssnumbervalue.h"

#include <math.h>

CFX_CSSNumberValue::CFX_CSSNumberValue(CFX_CSSNumber number)
    : CFX_CSSValue(PrimitiveType::kNumber), number_(number) {
  if (number_.unit == CFX_CSSNumber::Unit::kNumber &&
      fabs(number_.value) < 0.001f) {
    number_.value = 0.0f;
  }
}

CFX_CSSNumberValue::~CFX_CSSNumberValue() = default;

float CFX_CSSNumberValue::Apply(float percentBase) const {
  switch (number_.unit) {
    case CFX_CSSNumber::Unit::kPixels:
    case CFX_CSSNumber::Unit::kNumber:
      return number_.value * 72 / 96;
    case CFX_CSSNumber::Unit::kEMS:
    case CFX_CSSNumber::Unit::kEXS:
      return number_.value * percentBase;
    case CFX_CSSNumber::Unit::kPercent:
      return number_.value * percentBase / 100.0f;
    case CFX_CSSNumber::Unit::kCentiMeters:
      return number_.value * 28.3464f;
    case CFX_CSSNumber::Unit::kMilliMeters:
      return number_.value * 2.8346f;
    case CFX_CSSNumber::Unit::kInches:
      return number_.value * 72.0f;
    case CFX_CSSNumber::Unit::kPicas:
      return number_.value / 12.0f;
    case CFX_CSSNumber::Unit::kPoints:
      return number_.value;
  }
}
