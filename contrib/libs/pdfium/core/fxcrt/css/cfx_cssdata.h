// Copyright 2018 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCRT_CSS_CFX_CSSDATA_H_
#define CORE_FXCRT_CSS_CFX_CSSDATA_H_

#include "core/fxcrt/css/cfx_css.h"
#include "core/fxcrt/css/cfx_cssnumbervalue.h"
#include "core/fxcrt/css/cfx_cssvalue.h"
#include "core/fxcrt/widestring.h"
#include "core/fxge/dib/fx_dib.h"

class CFX_CSSData {
 public:
  struct Property {
    CFX_CSSProperty eName;
    uint32_t dwHash;  // Hashed as wide string.
    CFX_CSSValueTypeMask dwTypes;
  };

  struct PropertyValue {
    CFX_CSSPropertyValue eName;
    uint32_t dwHash;  // Hashed as wide string.
  };

  struct LengthUnit {
    const char* value;  // Raw, POD struct.
    CFX_CSSNumber::Unit type;
  };

  struct Color {
    const char* name;  // Raw, POD struct.
    FX_ARGB value;
  };

  static const Property* GetPropertyByName(WideStringView name);
  static const Property* GetPropertyByEnum(CFX_CSSProperty property);
  static const PropertyValue* GetPropertyValueByName(WideStringView wsName);
  static const LengthUnit* GetLengthUnitByName(WideStringView wsName);
  static const Color* GetColorByName(WideStringView wsName);
};

#endif  // CORE_FXCRT_CSS_CFX_CSSDATA_H_
