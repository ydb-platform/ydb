// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCRT_CSS_CFX_CSSNUMBERVALUE_H_
#define CORE_FXCRT_CSS_CFX_CSSNUMBERVALUE_H_

#include "core/fxcrt/css/cfx_cssvalue.h"

struct CFX_CSSNumber {
  enum class Unit {
    kNumber,
    kPercent,
    kEMS,
    kEXS,
    kPixels,
    kCentiMeters,
    kMilliMeters,
    kInches,
    kPoints,
    kPicas,
  };

  Unit unit;
  float value;
};

class CFX_CSSNumberValue final : public CFX_CSSValue {
 public:
  explicit CFX_CSSNumberValue(CFX_CSSNumber number);
  ~CFX_CSSNumberValue() override;

  CFX_CSSNumber::Unit unit() const { return number_.unit; }
  float value() const { return number_.value; }
  float Apply(float percentBase) const;

 private:
  CFX_CSSNumber number_;
};

#endif  // CORE_FXCRT_CSS_CFX_CSSNUMBERVALUE_H_
