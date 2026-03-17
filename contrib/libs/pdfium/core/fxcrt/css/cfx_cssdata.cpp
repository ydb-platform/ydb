// Copyright 2018 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcrt/css/cfx_cssdata.h"

#include <algorithm>
#include <utility>

#include "core/fxcrt/check_op.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/css/cfx_cssstyleselector.h"
#include "core/fxcrt/css/cfx_cssvaluelistparser.h"
#include "core/fxcrt/fx_codepage.h"
#include "core/fxcrt/fx_extension.h"

namespace {

#undef CSS_PROP____
#define CSS_PROP____(a, b, c, d) {CFX_CSSProperty::a, c, d},
const CFX_CSSData::Property kPropertyTable[] = {
#include "core/fxcrt/css/properties.inc"
};
#undef CSS_PROP____

#undef CSS_PROP_VALUE____
#define CSS_PROP_VALUE____(a, b, c) {CFX_CSSPropertyValue::a, c},
const CFX_CSSData::PropertyValue kPropertyValueTable[] = {
#include "core/fxcrt/css/property_values.inc"
};
#undef CSS_PROP_VALUE____

const CFX_CSSData::LengthUnit kLengthUnitTable[] = {
    {"cm", CFX_CSSNumber::Unit::kCentiMeters},
    {"em", CFX_CSSNumber::Unit::kEMS},
    {"ex", CFX_CSSNumber::Unit::kEXS},
    {"in", CFX_CSSNumber::Unit::kInches},
    {"mm", CFX_CSSNumber::Unit::kMilliMeters},
    {"pc", CFX_CSSNumber::Unit::kPicas},
    {"pt", CFX_CSSNumber::Unit::kPoints},
    {"px", CFX_CSSNumber::Unit::kPixels},
};

// 16 colours from CSS 2.0 + alternate spelling of grey/gray.
const CFX_CSSData::Color kColorTable[] = {
    {"aqua", 0xff00ffff},    {"black", 0xff000000}, {"blue", 0xff0000ff},
    {"fuchsia", 0xffff00ff}, {"gray", 0xff808080},  {"green", 0xff008000},
    {"grey", 0xff808080},    {"lime", 0xff00ff00},  {"maroon", 0xff800000},
    {"navy", 0xff000080},    {"olive", 0xff808000}, {"orange", 0xffffa500},
    {"purple", 0xff800080},  {"red", 0xffff0000},   {"silver", 0xffc0c0c0},
    {"teal", 0xff008080},    {"white", 0xffffffff}, {"yellow", 0xffffff00},
};

}  // namespace

const CFX_CSSData::Property* CFX_CSSData::GetPropertyByName(
    WideStringView name) {
  if (name.IsEmpty())
    return nullptr;

  uint32_t hash = FX_HashCode_GetLoweredW(name);
  auto* result = std::lower_bound(
      std::begin(kPropertyTable), std::end(kPropertyTable), hash,
      [](const CFX_CSSData::Property& iter, const uint32_t& hash) {
        return iter.dwHash < hash;
      });

  if (result != std::end(kPropertyTable) && result->dwHash == hash) {
    return result;
  }
  return nullptr;
}

const CFX_CSSData::Property* CFX_CSSData::GetPropertyByEnum(
    CFX_CSSProperty property) {
  auto index = static_cast<size_t>(property);
  CHECK_LT(index, std::size(kPropertyTable));
  // SAFETY: CHECK() on previous line ensures index is in bounds.
  return UNSAFE_BUFFERS(&kPropertyTable[index]);
}

const CFX_CSSData::PropertyValue* CFX_CSSData::GetPropertyValueByName(
    WideStringView wsName) {
  if (wsName.IsEmpty())
    return nullptr;

  uint32_t hash = FX_HashCode_GetLoweredW(wsName);
  auto* result = std::lower_bound(
      std::begin(kPropertyValueTable), std::end(kPropertyValueTable), hash,
      [](const PropertyValue& iter, const uint32_t& hash) {
        return iter.dwHash < hash;
      });

  if (result != std::end(kPropertyValueTable) && result->dwHash == hash) {
    return result;
  }
  return nullptr;
}

const CFX_CSSData::LengthUnit* CFX_CSSData::GetLengthUnitByName(
    WideStringView wsName) {
  if (wsName.IsEmpty() || wsName.GetLength() != 2) {
    return nullptr;
  }
  auto* iter =
      std::find_if(std::begin(kLengthUnitTable), std::end(kLengthUnitTable),
                   [wsName](const CFX_CSSData::LengthUnit& unit) {
                     return wsName.EqualsASCIINoCase(unit.value);
                   });
  return iter != std::end(kLengthUnitTable) ? iter : nullptr;
}

const CFX_CSSData::Color* CFX_CSSData::GetColorByName(WideStringView wsName) {
  if (wsName.IsEmpty()) {
    return nullptr;
  }
  auto* iter = std::find_if(std::begin(kColorTable), std::end(kColorTable),
                            [wsName](const CFX_CSSData::Color& color) {
                              return wsName.EqualsASCIINoCase(color.name);
                            });
  return iter != std::end(kColorTable) ? iter : nullptr;
}
