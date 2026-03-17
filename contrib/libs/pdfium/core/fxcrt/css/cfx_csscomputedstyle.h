// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCRT_CSS_CFX_CSSCOMPUTEDSTYLE_H_
#define CORE_FXCRT_CSS_CFX_CSSCOMPUTEDSTYLE_H_

#include <optional>
#include <vector>

#include "core/fxcrt/css/cfx_css.h"
#include "core/fxcrt/css/cfx_csscustomproperty.h"
#include "core/fxcrt/mask.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/widestring.h"
#include "core/fxge/dib/fx_dib.h"

class CFX_CSSValueList;

class CFX_CSSComputedStyle final : public Retainable {
 public:
  class InheritedData {
   public:
    InheritedData();
    ~InheritedData();

    CFX_CSSLength m_LetterSpacing{CFX_CSSLengthUnit::Normal, 0};
    CFX_CSSLength m_WordSpacing{CFX_CSSLengthUnit::Normal, 0};
    CFX_CSSLength m_TextIndent{CFX_CSSLengthUnit::Point, 0};
    RetainPtr<CFX_CSSValueList> m_pFontFamily;
    float m_fFontSize = 12.0f;
    float m_fLineHeight = 14.0f;
    FX_ARGB m_dwFontColor = 0xFF000000;
    uint16_t m_wFontWeight = 400;
    CFX_CSSFontVariant m_eFontVariant = CFX_CSSFontVariant::Normal;
    CFX_CSSFontStyle m_eFontStyle = CFX_CSSFontStyle::Normal;
    CFX_CSSTextAlign m_eTextAlign = CFX_CSSTextAlign::Left;
  };

  class NonInheritedData {
   public:
    NonInheritedData();

    CFX_CSSRect m_MarginWidth{CFX_CSSLengthUnit::Point, 0};
    CFX_CSSRect m_BorderWidth{CFX_CSSLengthUnit::Point, 0};
    CFX_CSSRect m_PaddingWidth{CFX_CSSLengthUnit::Point, 0};
    CFX_CSSLength m_Top;
    CFX_CSSLength m_Bottom;
    CFX_CSSLength m_Left;
    CFX_CSSLength m_Right;
    float m_fVerticalAlign = 0.0f;
    CFX_CSSDisplay m_eDisplay = CFX_CSSDisplay::Inline;
    CFX_CSSVerticalAlign m_eVerticalAlignType = CFX_CSSVerticalAlign::Baseline;
    Mask<CFX_CSSTEXTDECORATION> m_dwTextDecoration;
    bool m_bHasMargin = false;
    bool m_bHasBorder = false;
    bool m_bHasPadding = false;
  };

  CONSTRUCT_VIA_MAKE_RETAIN;

  std::optional<WideString> GetLastFontFamily() const;
  uint16_t GetFontWeight() const;
  CFX_CSSFontVariant GetFontVariant() const;
  CFX_CSSFontStyle GetFontStyle() const;
  float GetFontSize() const;
  FX_ARGB GetColor() const;
  void SetFontWeight(uint16_t wFontWeight);
  void SetFontVariant(CFX_CSSFontVariant eFontVariant);
  void SetFontStyle(CFX_CSSFontStyle eFontStyle);
  void SetFontSize(float fFontSize);
  void SetColor(FX_ARGB dwFontColor);

  const CFX_CSSRect* GetBorderWidth() const;
  const CFX_CSSRect* GetMarginWidth() const;
  const CFX_CSSRect* GetPaddingWidth() const;
  void SetMarginWidth(const CFX_CSSRect& rect);
  void SetPaddingWidth(const CFX_CSSRect& rect);

  CFX_CSSDisplay GetDisplay() const;

  float GetLineHeight() const;
  const CFX_CSSLength& GetTextIndent() const;
  CFX_CSSTextAlign GetTextAlign() const;
  CFX_CSSVerticalAlign GetVerticalAlign() const;
  float GetNumberVerticalAlign() const;
  Mask<CFX_CSSTEXTDECORATION> GetTextDecoration() const;
  const CFX_CSSLength& GetLetterSpacing() const;
  void SetLineHeight(float fLineHeight);
  void SetTextIndent(const CFX_CSSLength& textIndent);
  void SetTextAlign(CFX_CSSTextAlign eTextAlign);
  void SetNumberVerticalAlign(float fAlign);
  void SetTextDecoration(Mask<CFX_CSSTEXTDECORATION> dwTextDecoration);
  void SetLetterSpacing(const CFX_CSSLength& letterSpacing);
  void AddCustomStyle(const CFX_CSSCustomProperty& prop);

  bool GetCustomStyle(const WideString& wsName, WideString* pValue) const;

  InheritedData m_InheritedData;
  NonInheritedData m_NonInheritedData;

 private:
  CFX_CSSComputedStyle();
  ~CFX_CSSComputedStyle() override;

  std::vector<CFX_CSSCustomProperty> m_CustomProperties;
};

#endif  // CORE_FXCRT_CSS_CFX_CSSCOMPUTEDSTYLE_H_
