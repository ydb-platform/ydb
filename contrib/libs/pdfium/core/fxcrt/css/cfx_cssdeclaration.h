// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCRT_CSS_CFX_CSSDECLARATION_H_
#define CORE_FXCRT_CSS_CFX_CSSDECLARATION_H_

#include <memory>
#include <optional>
#include <vector>

#include "core/fxcrt/css/cfx_cssdata.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/widestring.h"

class CFX_CSSPropertyHolder;
class CFX_CSSCustomProperty;

class CFX_CSSDeclaration {
 public:
  using const_prop_iterator =
      std::vector<std::unique_ptr<CFX_CSSPropertyHolder>>::const_iterator;
  using const_custom_iterator =
      std::vector<std::unique_ptr<CFX_CSSCustomProperty>>::const_iterator;

  static std::optional<WideStringView> ParseCSSString(WideStringView value);
  static std::optional<FX_ARGB> ParseCSSColor(WideStringView value);

  CFX_CSSDeclaration();
  ~CFX_CSSDeclaration();

  RetainPtr<CFX_CSSValue> GetProperty(CFX_CSSProperty eProperty,
                                      bool* bImportant) const;

  bool empty() const { return properties_.empty(); }
  const_prop_iterator begin() const { return properties_.begin(); }
  const_prop_iterator end() const { return properties_.end(); }

  const_custom_iterator custom_begin() const {
    return custom_properties_.begin();
  }
  const_custom_iterator custom_end() const { return custom_properties_.end(); }

  void AddProperty(const CFX_CSSData::Property* property, WideStringView value);
  void AddProperty(const WideString& prop, const WideString& value);
  size_t PropertyCountForTesting() const;

  std::optional<FX_ARGB> ParseColorForTest(WideStringView value);

 private:
  void ParseFontProperty(WideStringView value, bool bImportant);

  // Never returns nullptr, instead returns a CSSValue representing zero
  // if the input cannot be parsed.
  RetainPtr<CFX_CSSValue> ParseBorderProperty(WideStringView value) const;

  void ParseValueListProperty(const CFX_CSSData::Property* pProperty,
                              WideStringView value,
                              bool bImportant);
  void Add4ValuesProperty(const std::vector<RetainPtr<CFX_CSSValue>>& list,
                          bool bImportant,
                          CFX_CSSProperty eLeft,
                          CFX_CSSProperty eTop,
                          CFX_CSSProperty eRight,
                          CFX_CSSProperty eBottom);
  RetainPtr<CFX_CSSValue> ParseNumber(WideStringView value);
  RetainPtr<CFX_CSSValue> ParseEnum(WideStringView value);
  RetainPtr<CFX_CSSValue> ParseColor(WideStringView value);
  RetainPtr<CFX_CSSValue> ParseString(WideStringView value);
  void AddPropertyHolder(CFX_CSSProperty eProperty,
                         RetainPtr<CFX_CSSValue> pValue,
                         bool bImportant);

  std::vector<std::unique_ptr<CFX_CSSPropertyHolder>> properties_;
  std::vector<std::unique_ptr<CFX_CSSCustomProperty>> custom_properties_;
};

#endif  // CORE_FXCRT_CSS_CFX_CSSDECLARATION_H_
