// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcrt/css/cfx_cssdeclaration.h"

#include <math.h>

#include <array>
#include <utility>

#include "core/fxcrt/check.h"
#include "core/fxcrt/check_op.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/css/cfx_csscolorvalue.h"
#include "core/fxcrt/css/cfx_csscustomproperty.h"
#include "core/fxcrt/css/cfx_cssenumvalue.h"
#include "core/fxcrt/css/cfx_cssnumbervalue.h"
#include "core/fxcrt/css/cfx_csspropertyholder.h"
#include "core/fxcrt/css/cfx_cssstringvalue.h"
#include "core/fxcrt/css/cfx_cssvaluelist.h"
#include "core/fxcrt/css/cfx_cssvaluelistparser.h"
#include "core/fxcrt/fx_extension.h"
#include "core/fxcrt/fx_system.h"
#include "core/fxcrt/notreached.h"

namespace {

uint8_t Hex2Dec(uint8_t hexHigh, uint8_t hexLow) {
  return (FXSYS_HexCharToInt(hexHigh) << 4) + FXSYS_HexCharToInt(hexLow);
}

std::optional<CFX_CSSNumber> ParseCSSNumber(WideStringView view) {
  DCHECK(!view.IsEmpty());

  size_t nUsedLen = 0;
  float value = FXSYS_wcstof(view, &nUsedLen);
  if (nUsedLen == 0 || !isfinite(value)) {
    return std::nullopt;
  }
  view = view.Substr(nUsedLen);
  if (view.Front() == '%') {  // NOTE: empty-tolerant Front().
    return CFX_CSSNumber{CFX_CSSNumber::Unit::kPercent, value};
  }
  if (view.GetLength() == 2) {
    const CFX_CSSData::LengthUnit* pUnit =
        CFX_CSSData::GetLengthUnitByName(view.First(2));
    if (pUnit) {
      return CFX_CSSNumber{pUnit->type, value};
    }
  }
  return CFX_CSSNumber{CFX_CSSNumber::Unit::kNumber, value};
}

}  // namespace

// static
std::optional<WideStringView> CFX_CSSDeclaration::ParseCSSString(
    WideStringView value) {
  if (value.GetLength() >= 2) {
    wchar_t first = value.Front();
    wchar_t last = value.Back();
    if ((first == '\"' && last == '\"') || (first == '\'' && last == '\'')) {
      value = value.Substr(1, value.GetLength() - 2);
    }
  }
  if (value.IsEmpty()) {
    return std::nullopt;
  }
  return value;
}

// static.
std::optional<FX_ARGB> CFX_CSSDeclaration::ParseCSSColor(WideStringView value) {
  if (value.Front() == '#') {  // Note: empty-tolerant Front().
    switch (value.GetLength()) {
      case 4: {
        uint8_t red = Hex2Dec((uint8_t)value[1], (uint8_t)value[1]);
        uint8_t green = Hex2Dec((uint8_t)value[2], (uint8_t)value[2]);
        uint8_t blue = Hex2Dec((uint8_t)value[3], (uint8_t)value[3]);
        return ArgbEncode(255, red, green, blue);
      }
      case 7: {
        uint8_t red = Hex2Dec((uint8_t)value[1], (uint8_t)value[2]);
        uint8_t green = Hex2Dec((uint8_t)value[3], (uint8_t)value[4]);
        uint8_t blue = Hex2Dec((uint8_t)value[5], (uint8_t)value[6]);
        return ArgbEncode(255, red, green, blue);
      }
      default:
        return std::nullopt;
    }
  }

  if (value.GetLength() >= 10) {
    if (!value.First(4).EqualsASCIINoCase("rgb(") || value.Back() != ')') {
      return std::nullopt;
    }
    std::array<uint8_t, 3> rgb = {};
    CFX_CSSValueListParser list(value.Substr(4, value.GetLength() - 5), ',');
    for (auto& component : rgb) {
      auto maybe_value = list.NextValue();
      if (!maybe_value.has_value() ||
          maybe_value.value().type != CFX_CSSValue::PrimitiveType::kNumber) {
        return std::nullopt;
      }
      auto maybe_number = ParseCSSNumber(maybe_value.value().string_view);
      if (!maybe_number.has_value()) {
        return std::nullopt;
      }
      component = maybe_number.value().unit == CFX_CSSNumber::Unit::kPercent
                      ? FXSYS_roundf(maybe_number.value().value * 2.55f)
                      : FXSYS_roundf(maybe_number.value().value);
    }
    return ArgbEncode(255, rgb[0], rgb[1], rgb[2]);
  }

  const CFX_CSSData::Color* pColor = CFX_CSSData::GetColorByName(value);
  if (!pColor) {
    return std::nullopt;
  }
  return pColor->value;
}

CFX_CSSDeclaration::CFX_CSSDeclaration() = default;

CFX_CSSDeclaration::~CFX_CSSDeclaration() = default;

RetainPtr<CFX_CSSValue> CFX_CSSDeclaration::GetProperty(
    CFX_CSSProperty eProperty,
    bool* bImportant) const {
  for (const auto& p : properties_) {
    if (p->eProperty == eProperty) {
      *bImportant = p->bImportant;
      return p->pValue;
    }
  }
  return nullptr;
}

void CFX_CSSDeclaration::AddPropertyHolder(CFX_CSSProperty eProperty,
                                           RetainPtr<CFX_CSSValue> pValue,
                                           bool bImportant) {
  auto pHolder = std::make_unique<CFX_CSSPropertyHolder>();
  pHolder->bImportant = bImportant;
  pHolder->eProperty = eProperty;
  pHolder->pValue = std::move(pValue);
  properties_.push_back(std::move(pHolder));
}

void CFX_CSSDeclaration::AddProperty(const CFX_CSSData::Property* property,
                                     WideStringView value) {
  DCHECK(!value.IsEmpty());

  bool bImportant = false;
  WideStringView last_ten = value.Last(10);  // NOTE: empty-tolerant Last().
  if (last_ten.EqualsASCIINoCase("!important")) {
    value = value.First(value.GetLength() - 10);
    if (value.IsEmpty()) {
      return;
    }
    bImportant = true;
  }
  const CFX_CSSValueTypeMask dwType = property->dwTypes;
  switch (dwType & 0x0F) {
    case CFX_CSSVALUETYPE_Primitive: {
      static constexpr CFX_CSSVALUETYPE kValueGuessOrder[] = {
          CFX_CSSVALUETYPE_MaybeNumber,
          CFX_CSSVALUETYPE_MaybeEnum,
          CFX_CSSVALUETYPE_MaybeColor,
          CFX_CSSVALUETYPE_MaybeString,
      };
      for (CFX_CSSVALUETYPE guess : kValueGuessOrder) {
        const CFX_CSSValueTypeMask dwMatch = dwType & guess;
        if (dwMatch == 0) {
          continue;
        }
        RetainPtr<CFX_CSSValue> pCSSValue;
        switch (dwMatch) {
          case CFX_CSSVALUETYPE_MaybeNumber:
            pCSSValue = ParseNumber(value);
            break;
          case CFX_CSSVALUETYPE_MaybeEnum:
            pCSSValue = ParseEnum(value);
            break;
          case CFX_CSSVALUETYPE_MaybeColor:
            pCSSValue = ParseColor(value);
            break;
          case CFX_CSSVALUETYPE_MaybeString:
            pCSSValue = ParseString(value);
            break;
          default:
            break;
        }
        if (pCSSValue) {
          AddPropertyHolder(property->eName, pCSSValue, bImportant);
          return;
        }
        if ((dwType & ~guess) == CFX_CSSVALUETYPE_Primitive) {
          return;
        }
      }
      break;
    }
    case CFX_CSSVALUETYPE_Shorthand: {
      switch (property->eName) {
        case CFX_CSSProperty::Font: {
          ParseFontProperty(value, bImportant);
          return;
        }
        case CFX_CSSProperty::Border: {
          RetainPtr<CFX_CSSValue> pWidth = ParseBorderProperty(value);
          AddPropertyHolder(CFX_CSSProperty::BorderLeftWidth, pWidth,
                            bImportant);
          AddPropertyHolder(CFX_CSSProperty::BorderTopWidth, pWidth,
                            bImportant);
          AddPropertyHolder(CFX_CSSProperty::BorderRightWidth, pWidth,
                            bImportant);
          AddPropertyHolder(CFX_CSSProperty::BorderBottomWidth, pWidth,
                            bImportant);
          return;
        }
        case CFX_CSSProperty::BorderLeft: {
          AddPropertyHolder(CFX_CSSProperty::BorderLeftWidth,
                            ParseBorderProperty(value), bImportant);
          break;
        }
        case CFX_CSSProperty::BorderTop: {
          AddPropertyHolder(CFX_CSSProperty::BorderTopWidth,
                            ParseBorderProperty(value), bImportant);
          return;
        }
        case CFX_CSSProperty::BorderRight: {
          AddPropertyHolder(CFX_CSSProperty::BorderRightWidth,
                            ParseBorderProperty(value), bImportant);
          return;
        }
        case CFX_CSSProperty::BorderBottom: {
          AddPropertyHolder(CFX_CSSProperty::BorderBottomWidth,
                            ParseBorderProperty(value), bImportant);
          return;
        }
        default:
          break;
      }
      break;
    }
    case CFX_CSSVALUETYPE_List:
      ParseValueListProperty(property, value, bImportant);
      return;
    default:
      NOTREACHED_NORETURN();
  }
}

void CFX_CSSDeclaration::AddProperty(const WideString& prop,
                                     const WideString& value) {
  custom_properties_.push_back(
      std::make_unique<CFX_CSSCustomProperty>(prop, value));
}

RetainPtr<CFX_CSSValue> CFX_CSSDeclaration::ParseNumber(WideStringView view) {
  std::optional<CFX_CSSNumber> maybe_number = ParseCSSNumber(view);
  if (!maybe_number.has_value()) {
    return nullptr;
  }
  return pdfium::MakeRetain<CFX_CSSNumberValue>(maybe_number.value());
}

RetainPtr<CFX_CSSValue> CFX_CSSDeclaration::ParseEnum(WideStringView value) {
  const CFX_CSSData::PropertyValue* pValue =
      CFX_CSSData::GetPropertyValueByName(value);
  return pValue ? pdfium::MakeRetain<CFX_CSSEnumValue>(pValue->eName) : nullptr;
}

RetainPtr<CFX_CSSValue> CFX_CSSDeclaration::ParseColor(WideStringView value) {
  auto maybe_color = ParseCSSColor(value);
  if (!maybe_color.has_value()) {
    return nullptr;
  }
  return pdfium::MakeRetain<CFX_CSSColorValue>(maybe_color.value());
}

RetainPtr<CFX_CSSValue> CFX_CSSDeclaration::ParseString(WideStringView value) {
  auto maybe_string = ParseCSSString(value);
  if (!maybe_string.has_value() || maybe_string.value().IsEmpty()) {
    return nullptr;
  }
  return pdfium::MakeRetain<CFX_CSSStringValue>(maybe_string.value());
}

void CFX_CSSDeclaration::ParseValueListProperty(
    const CFX_CSSData::Property* pProperty,
    WideStringView value,
    bool bImportant) {
  wchar_t separator =
      (pProperty->eName == CFX_CSSProperty::FontFamily) ? ',' : ' ';
  CFX_CSSValueListParser parser(value, separator);
  const CFX_CSSValueTypeMask dwType = pProperty->dwTypes;
  std::vector<RetainPtr<CFX_CSSValue>> list;
  while (1) {
    auto maybe_next = parser.NextValue();
    if (!maybe_next.has_value()) {
      break;
    }
    switch (maybe_next.value().type) {
      case CFX_CSSValue::PrimitiveType::kNumber:
        if (dwType & CFX_CSSVALUETYPE_MaybeNumber) {
          auto maybe_number = ParseCSSNumber(maybe_next.value().string_view);
          if (maybe_number.has_value()) {
            list.push_back(
                pdfium::MakeRetain<CFX_CSSNumberValue>(maybe_number.value()));
          }
        }
        break;
      case CFX_CSSValue::PrimitiveType::kString:
        if (dwType & CFX_CSSVALUETYPE_MaybeColor) {
          auto maybe_color = ParseCSSColor(maybe_next.value().string_view);
          if (maybe_color.has_value()) {
            list.push_back(
                pdfium::MakeRetain<CFX_CSSColorValue>(maybe_color.value()));
            continue;
          }
        }
        if (dwType & CFX_CSSVALUETYPE_MaybeEnum) {
          const CFX_CSSData::PropertyValue* pPropValue =
              CFX_CSSData::GetPropertyValueByName(
                  maybe_next.value().string_view);
          if (pPropValue) {
            list.push_back(
                pdfium::MakeRetain<CFX_CSSEnumValue>(pPropValue->eName));
            continue;
          }
        }
        if (dwType & CFX_CSSVALUETYPE_MaybeString) {
          list.push_back(pdfium::MakeRetain<CFX_CSSStringValue>(
              maybe_next.value().string_view));
        }
        break;
      case CFX_CSSValue::PrimitiveType::kRGB:
        if (dwType & CFX_CSSVALUETYPE_MaybeColor) {
          FX_ARGB color =
              ParseCSSColor(maybe_next.value().string_view).value_or(0);
          list.push_back(pdfium::MakeRetain<CFX_CSSColorValue>(color));
        }
        break;
      default:
        break;
    }
  }
  if (list.empty()) {
    return;
  }
  switch (pProperty->eName) {
    case CFX_CSSProperty::BorderWidth:
      Add4ValuesProperty(list, bImportant, CFX_CSSProperty::BorderLeftWidth,
                         CFX_CSSProperty::BorderTopWidth,
                         CFX_CSSProperty::BorderRightWidth,
                         CFX_CSSProperty::BorderBottomWidth);
      return;
    case CFX_CSSProperty::Margin:
      Add4ValuesProperty(list, bImportant, CFX_CSSProperty::MarginLeft,
                         CFX_CSSProperty::MarginTop,
                         CFX_CSSProperty::MarginRight,
                         CFX_CSSProperty::MarginBottom);
      return;
    case CFX_CSSProperty::Padding:
      Add4ValuesProperty(list, bImportant, CFX_CSSProperty::PaddingLeft,
                         CFX_CSSProperty::PaddingTop,
                         CFX_CSSProperty::PaddingRight,
                         CFX_CSSProperty::PaddingBottom);
      return;
    default: {
      auto value_list = pdfium::MakeRetain<CFX_CSSValueList>(std::move(list));
      AddPropertyHolder(pProperty->eName, std::move(value_list), bImportant);
      return;
    }
  }
}

void CFX_CSSDeclaration::Add4ValuesProperty(
    const std::vector<RetainPtr<CFX_CSSValue>>& list,
    bool bImportant,
    CFX_CSSProperty eLeft,
    CFX_CSSProperty eTop,
    CFX_CSSProperty eRight,
    CFX_CSSProperty eBottom) {
  switch (list.size()) {
    case 1:
      AddPropertyHolder(eLeft, list[0], bImportant);
      AddPropertyHolder(eTop, list[0], bImportant);
      AddPropertyHolder(eRight, list[0], bImportant);
      AddPropertyHolder(eBottom, list[0], bImportant);
      return;
    case 2:
      AddPropertyHolder(eLeft, list[1], bImportant);
      AddPropertyHolder(eTop, list[0], bImportant);
      AddPropertyHolder(eRight, list[1], bImportant);
      AddPropertyHolder(eBottom, list[0], bImportant);
      return;
    case 3:
      AddPropertyHolder(eLeft, list[1], bImportant);
      AddPropertyHolder(eTop, list[0], bImportant);
      AddPropertyHolder(eRight, list[1], bImportant);
      AddPropertyHolder(eBottom, list[2], bImportant);
      return;
    case 4:
      AddPropertyHolder(eLeft, list[3], bImportant);
      AddPropertyHolder(eTop, list[0], bImportant);
      AddPropertyHolder(eRight, list[1], bImportant);
      AddPropertyHolder(eBottom, list[2], bImportant);
      return;
    default:
      break;
  }
}

RetainPtr<CFX_CSSValue> CFX_CSSDeclaration::ParseBorderProperty(
    WideStringView value) const {
  RetainPtr<CFX_CSSValue> pWidth;
  CFX_CSSValueListParser parser(value, ' ');
  while (1) {
    auto maybe_next = parser.NextValue();
    if (!maybe_next.has_value()) {
      break;
    }
    switch (maybe_next.value().type) {
      case CFX_CSSValue::PrimitiveType::kNumber: {
        if (pWidth) {
          continue;
        }
        auto maybe_number = ParseCSSNumber(maybe_next.value().string_view);
        if (maybe_number.has_value()) {
          pWidth = pdfium::MakeRetain<CFX_CSSNumberValue>(maybe_number.value());
        }
        break;
      }
      case CFX_CSSValue::PrimitiveType::kString: {
        const CFX_CSSData::Color* pColorItem =
            CFX_CSSData::GetColorByName(maybe_next.value().string_view);
        if (pColorItem) {
          continue;
        }
        const CFX_CSSData::PropertyValue* pValue =
            CFX_CSSData::GetPropertyValueByName(maybe_next.value().string_view);
        if (!pValue) {
          continue;
        }
        switch (pValue->eName) {
          case CFX_CSSPropertyValue::Thin:
          case CFX_CSSPropertyValue::Thick:
          case CFX_CSSPropertyValue::Medium:
            if (!pWidth)
              pWidth = pdfium::MakeRetain<CFX_CSSEnumValue>(pValue->eName);
            break;
          default:
            break;
        }
        break;
      }
      default:
        break;
    }
  }
  if (pWidth) {
    return pWidth;
  }
  return pdfium::MakeRetain<CFX_CSSNumberValue>(
      CFX_CSSNumber{CFX_CSSNumber::Unit::kNumber, 0.0f});
}

void CFX_CSSDeclaration::ParseFontProperty(WideStringView value,
                                           bool bImportant) {
  RetainPtr<CFX_CSSValue> pStyle;
  RetainPtr<CFX_CSSValue> pVariant;
  RetainPtr<CFX_CSSValue> pWeight;
  RetainPtr<CFX_CSSValue> pFontSize;
  RetainPtr<CFX_CSSValue> pLineHeight;
  std::vector<RetainPtr<CFX_CSSValue>> family_list;
  CFX_CSSValueListParser parser(value, '/');
  while (1) {
    auto maybe_next = parser.NextValue();
    if (!maybe_next.has_value()) {
      break;
    }
    switch (maybe_next.value().type) {
      case CFX_CSSValue::PrimitiveType::kString: {
        const CFX_CSSData::PropertyValue* pValue =
            CFX_CSSData::GetPropertyValueByName(maybe_next.value().string_view);
        if (pValue) {
          switch (pValue->eName) {
            case CFX_CSSPropertyValue::XxSmall:
            case CFX_CSSPropertyValue::XSmall:
            case CFX_CSSPropertyValue::Small:
            case CFX_CSSPropertyValue::Medium:
            case CFX_CSSPropertyValue::Large:
            case CFX_CSSPropertyValue::XLarge:
            case CFX_CSSPropertyValue::XxLarge:
            case CFX_CSSPropertyValue::Smaller:
            case CFX_CSSPropertyValue::Larger:
              if (!pFontSize)
                pFontSize = pdfium::MakeRetain<CFX_CSSEnumValue>(pValue->eName);
              continue;
            case CFX_CSSPropertyValue::Bold:
            case CFX_CSSPropertyValue::Bolder:
            case CFX_CSSPropertyValue::Lighter:
              if (!pWeight)
                pWeight = pdfium::MakeRetain<CFX_CSSEnumValue>(pValue->eName);
              continue;
            case CFX_CSSPropertyValue::Italic:
            case CFX_CSSPropertyValue::Oblique:
              if (!pStyle)
                pStyle = pdfium::MakeRetain<CFX_CSSEnumValue>(pValue->eName);
              continue;
            case CFX_CSSPropertyValue::SmallCaps:
              if (!pVariant)
                pVariant = pdfium::MakeRetain<CFX_CSSEnumValue>(pValue->eName);
              continue;
            case CFX_CSSPropertyValue::Normal:
              if (!pStyle)
                pStyle = pdfium::MakeRetain<CFX_CSSEnumValue>(pValue->eName);
              else if (!pVariant)
                pVariant = pdfium::MakeRetain<CFX_CSSEnumValue>(pValue->eName);
              else if (!pWeight)
                pWeight = pdfium::MakeRetain<CFX_CSSEnumValue>(pValue->eName);
              else if (!pFontSize)
                pFontSize = pdfium::MakeRetain<CFX_CSSEnumValue>(pValue->eName);
              else if (!pLineHeight)
                pLineHeight =
                    pdfium::MakeRetain<CFX_CSSEnumValue>(pValue->eName);
              continue;
            default:
              break;
          }
        }
        if (pFontSize) {
          family_list.push_back(pdfium::MakeRetain<CFX_CSSStringValue>(
              maybe_next.value().string_view));
        }
        parser.UseCommaSeparator();
        break;
      }
      case CFX_CSSValue::PrimitiveType::kNumber: {
        auto maybe_number = ParseCSSNumber(maybe_next.value().string_view);
        if (!maybe_number.has_value()) {
          break;
        }
        if (maybe_number.value().unit == CFX_CSSNumber::Unit::kNumber) {
          switch (static_cast<int32_t>(maybe_number.value().value)) {
            case 100:
            case 200:
            case 300:
            case 400:
            case 500:
            case 600:
            case 700:
            case 800:
            case 900:
              if (!pWeight) {
                pWeight = pdfium::MakeRetain<CFX_CSSNumberValue>(
                    maybe_number.value());
              }
              continue;
          }
        }
        if (!pFontSize) {
          pFontSize =
              pdfium::MakeRetain<CFX_CSSNumberValue>(maybe_number.value());
        } else if (!pLineHeight) {
          pLineHeight =
              pdfium::MakeRetain<CFX_CSSNumberValue>(maybe_number.value());
        }
        break;
      }
      default:
        break;
    }
  }

  if (!pStyle) {
    pStyle = pdfium::MakeRetain<CFX_CSSEnumValue>(CFX_CSSPropertyValue::Normal);
  }
  if (!pVariant) {
    pVariant =
        pdfium::MakeRetain<CFX_CSSEnumValue>(CFX_CSSPropertyValue::Normal);
  }
  if (!pWeight) {
    pWeight =
        pdfium::MakeRetain<CFX_CSSEnumValue>(CFX_CSSPropertyValue::Normal);
  }
  if (!pFontSize) {
    pFontSize =
        pdfium::MakeRetain<CFX_CSSEnumValue>(CFX_CSSPropertyValue::Medium);
  }
  if (!pLineHeight) {
    pLineHeight =
        pdfium::MakeRetain<CFX_CSSEnumValue>(CFX_CSSPropertyValue::Normal);
  }

  AddPropertyHolder(CFX_CSSProperty::FontStyle, pStyle, bImportant);
  AddPropertyHolder(CFX_CSSProperty::FontVariant, pVariant, bImportant);
  AddPropertyHolder(CFX_CSSProperty::FontWeight, pWeight, bImportant);
  AddPropertyHolder(CFX_CSSProperty::FontSize, pFontSize, bImportant);
  AddPropertyHolder(CFX_CSSProperty::LineHeight, pLineHeight, bImportant);
  if (!family_list.empty()) {
    auto value_list =
        pdfium::MakeRetain<CFX_CSSValueList>(std::move(family_list));
    AddPropertyHolder(CFX_CSSProperty::FontFamily, value_list, bImportant);
  }
}

size_t CFX_CSSDeclaration::PropertyCountForTesting() const {
  return properties_.size();
}
