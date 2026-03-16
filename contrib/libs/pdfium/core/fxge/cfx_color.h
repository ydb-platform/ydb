// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXGE_CFX_COLOR_H_
#define CORE_FXGE_CFX_COLOR_H_

#include "core/fxge/dib/fx_dib.h"

struct CFX_Color {
  // Ordered by increasing number of components.
  enum class Type { kTransparent = 0, kGray, kRGB, kCMYK };

  struct TypeAndARGB {
    TypeAndARGB(CFX_Color::Type type_in, FX_ARGB argb_in)
        : color_type(type_in), argb(argb_in) {}

    CFX_Color::Type color_type;
    FX_ARGB argb;
  };

  explicit constexpr CFX_Color(FX_COLORREF ref)
      : CFX_Color(FXARGB_R(ref), FXARGB_G(ref), FXARGB_B(ref)) {}

  constexpr CFX_Color(Type type = CFX_Color::Type::kTransparent,
                      float color1 = 0.0f,
                      float color2 = 0.0f,
                      float color3 = 0.0f,
                      float color4 = 0.0f)
      : nColorType(type),
        fColor1(color1),
        fColor2(color2),
        fColor3(color3),
        fColor4(color4) {}

  constexpr CFX_Color(int32_t r, int32_t g, int32_t b)
      : nColorType(CFX_Color::Type::kRGB),
        fColor1(r / 255.0f),
        fColor2(g / 255.0f),
        fColor3(b / 255.0f),
        fColor4(0) {}

  CFX_Color(const CFX_Color& that) = default;
  CFX_Color& operator=(const CFX_Color& that) = default;

  CFX_Color operator/(float fColorDivide) const;
  CFX_Color operator-(float fColorSub) const;

  CFX_Color ConvertColorType(Type nConvertColorType) const;
  FX_COLORREF ToFXColor(int32_t nTransparency) const;

  Type nColorType;
  float fColor1;
  float fColor2;
  float fColor3;
  float fColor4;
};

inline bool operator==(const CFX_Color& c1, const CFX_Color& c2) {
  return c1.nColorType == c2.nColorType && c1.fColor1 - c2.fColor1 < 0.0001 &&
         c1.fColor1 - c2.fColor1 > -0.0001 &&
         c1.fColor2 - c2.fColor2 < 0.0001 &&
         c1.fColor2 - c2.fColor2 > -0.0001 &&
         c1.fColor3 - c2.fColor3 < 0.0001 &&
         c1.fColor3 - c2.fColor3 > -0.0001 &&
         c1.fColor4 - c2.fColor4 < 0.0001 && c1.fColor4 - c2.fColor4 > -0.0001;
}

inline bool operator!=(const CFX_Color& c1, const CFX_Color& c2) {
  return !(c1 == c2);
}

#endif  // CORE_FXGE_CFX_COLOR_H_
