// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfdoc/cpdf_formcontrol.h"

#include <array>
#include <iterator>
#include <utility>

#include "constants/form_fields.h"
#include "core/fpdfapi/font/cpdf_font.h"
#include "core/fpdfapi/page/cpdf_docpagedata.h"
#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_name.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fpdfapi/parser/fpdf_parser_decode.h"
#include "core/fpdfapi/parser/fpdf_parser_utility.h"
#include "core/fpdfdoc/cpdf_interactiveform.h"
#include "core/fxcrt/check.h"

namespace {

constexpr std::array<char, 5> kHighlightModes = {{'N', 'I', 'O', 'P', 'T'}};

// Order of |kHighlightModes| must match order of HighlightingMode enum.
static_assert(kHighlightModes[CPDF_FormControl::kNone] == 'N',
              "HighlightingMode mismatch");
static_assert(kHighlightModes[CPDF_FormControl::kInvert] == 'I',
              "HighlightingMode mismatch");
static_assert(kHighlightModes[CPDF_FormControl::kOutline] == 'O',
              "HighlightingMode mismatch");
static_assert(kHighlightModes[CPDF_FormControl::kPush] == 'P',
              "HighlightingMode mismatch");
static_assert(kHighlightModes[CPDF_FormControl::kToggle] == 'T',
              "HighlightingMode mismatch");

}  // namespace

CPDF_FormControl::CPDF_FormControl(CPDF_FormField* pField,
                                   RetainPtr<CPDF_Dictionary> pWidgetDict,
                                   CPDF_InteractiveForm* pForm)
    : m_pField(pField), m_pWidgetDict(std::move(pWidgetDict)), m_pForm(pForm) {
  DCHECK(m_pWidgetDict);
}

CPDF_FormControl::~CPDF_FormControl() = default;

CFX_FloatRect CPDF_FormControl::GetRect() const {
  return m_pWidgetDict->GetRectFor("Rect");
}

ByteString CPDF_FormControl::GetOnStateName() const {
  DCHECK(GetType() == CPDF_FormField::kCheckBox ||
         GetType() == CPDF_FormField::kRadioButton);
  RetainPtr<const CPDF_Dictionary> pAP = m_pWidgetDict->GetDictFor("AP");
  if (!pAP)
    return ByteString();

  RetainPtr<const CPDF_Dictionary> pN = pAP->GetDictFor("N");
  if (!pN)
    return ByteString();

  CPDF_DictionaryLocker locker(pN);
  for (const auto& it : locker) {
    if (it.first != "Off")
      return it.first;
  }
  return ByteString();
}

ByteString CPDF_FormControl::GetCheckedAPState() const {
  DCHECK(GetType() == CPDF_FormField::kCheckBox ||
         GetType() == CPDF_FormField::kRadioButton);
  ByteString csOn = GetOnStateName();
  if (ToArray(m_pField->GetFieldAttr("Opt")))
    csOn = ByteString::FormatInteger(m_pField->GetControlIndex(this));
  if (csOn.IsEmpty())
    csOn = "Yes";
  return csOn;
}

WideString CPDF_FormControl::GetExportValue() const {
  DCHECK(GetType() == CPDF_FormField::kCheckBox ||
         GetType() == CPDF_FormField::kRadioButton);
  ByteString csOn = GetOnStateName();
  RetainPtr<const CPDF_Array> pArray = ToArray(m_pField->GetFieldAttr("Opt"));
  if (pArray)
    csOn = pArray->GetByteStringAt(m_pField->GetControlIndex(this));
  if (csOn.IsEmpty())
    csOn = "Yes";
  return PDF_DecodeText(csOn.unsigned_span());
}

bool CPDF_FormControl::IsChecked() const {
  DCHECK(GetType() == CPDF_FormField::kCheckBox ||
         GetType() == CPDF_FormField::kRadioButton);
  ByteString csOn = GetOnStateName();
  ByteString csAS = m_pWidgetDict->GetByteStringFor("AS");
  return csAS == csOn;
}

bool CPDF_FormControl::IsDefaultChecked() const {
  DCHECK(GetType() == CPDF_FormField::kCheckBox ||
         GetType() == CPDF_FormField::kRadioButton);
  RetainPtr<const CPDF_Object> pDV = m_pField->GetFieldAttr("DV");
  if (!pDV)
    return false;

  ByteString csDV = pDV->GetString();
  ByteString csOn = GetOnStateName();
  return (csDV == csOn);
}

void CPDF_FormControl::CheckControl(bool bChecked) {
  DCHECK(GetType() == CPDF_FormField::kCheckBox ||
         GetType() == CPDF_FormField::kRadioButton);
  ByteString csOldAS = m_pWidgetDict->GetByteStringFor("AS", "Off");
  ByteString csAS = "Off";
  if (bChecked)
    csAS = GetOnStateName();
  if (csOldAS == csAS)
    return;
  m_pWidgetDict->SetNewFor<CPDF_Name>("AS", csAS);
}

CPDF_FormControl::HighlightingMode CPDF_FormControl::GetHighlightingMode()
    const {
  ByteString csH = m_pWidgetDict->GetByteStringFor("H", "I");
  for (size_t i = 0; i < std::size(kHighlightModes); ++i) {
    // TODO(tsepez): disambiguate string ctors.
      if (csH == ByteStringView(kHighlightModes[i])) {
        return static_cast<HighlightingMode>(i);
      }
  }
  return kInvert;
}

CPDF_ApSettings CPDF_FormControl::GetMK() const {
  return CPDF_ApSettings(m_pWidgetDict->GetMutableDictFor("MK"));
}

bool CPDF_FormControl::HasMKEntry(const ByteString& csEntry) const {
  return GetMK().HasMKEntry(csEntry);
}

int CPDF_FormControl::GetRotation() const {
  return GetMK().GetRotation();
}

CFX_Color::TypeAndARGB CPDF_FormControl::GetColorARGB(
    const ByteString& csEntry) {
  return GetMK().GetColorARGB(csEntry);
}

float CPDF_FormControl::GetOriginalColorComponent(int index,
                                                  const ByteString& csEntry) {
  return GetMK().GetOriginalColorComponent(index, csEntry);
}

CFX_Color CPDF_FormControl::GetOriginalColor(const ByteString& csEntry) {
  return GetMK().GetOriginalColor(csEntry);
}

WideString CPDF_FormControl::GetCaption(const ByteString& csEntry) const {
  return GetMK().GetCaption(csEntry);
}

RetainPtr<CPDF_Stream> CPDF_FormControl::GetIcon(const ByteString& csEntry) {
  return GetMK().GetIcon(csEntry);
}

CPDF_IconFit CPDF_FormControl::GetIconFit() const {
  return GetMK().GetIconFit();
}

int CPDF_FormControl::GetTextPosition() const {
  return GetMK().GetTextPosition();
}

CPDF_DefaultAppearance CPDF_FormControl::GetDefaultAppearance() const {
  if (m_pWidgetDict->KeyExist(pdfium::form_fields::kDA)) {
    return CPDF_DefaultAppearance(
        m_pWidgetDict->GetByteStringFor(pdfium::form_fields::kDA));
  }
  RetainPtr<const CPDF_Object> pObj =
      m_pField->GetFieldAttr(pdfium::form_fields::kDA);
  if (pObj)
    return CPDF_DefaultAppearance(pObj->GetString());

  return m_pForm->GetDefaultAppearance();
}

std::optional<WideString> CPDF_FormControl::GetDefaultControlFontName() const {
  RetainPtr<CPDF_Font> pFont = GetDefaultControlFont();
  if (!pFont)
    return std::nullopt;

  return WideString::FromDefANSI(pFont->GetBaseFontName().AsStringView());
}

RetainPtr<CPDF_Font> CPDF_FormControl::GetDefaultControlFont() const {
  float fFontSize;
  CPDF_DefaultAppearance cDA = GetDefaultAppearance();
  std::optional<ByteString> csFontNameTag = cDA.GetFont(&fFontSize);
  if (!csFontNameTag.has_value() || csFontNameTag->IsEmpty())
    return nullptr;

  RetainPtr<CPDF_Dictionary> pDRDict = ToDictionary(
      CPDF_FormField::GetMutableFieldAttrForDict(m_pWidgetDict.Get(), "DR"));
  if (pDRDict) {
    RetainPtr<CPDF_Dictionary> pFonts = pDRDict->GetMutableDictFor("Font");
    if (ValidateFontResourceDict(pFonts.Get())) {
      RetainPtr<CPDF_Dictionary> pElement =
          pFonts->GetMutableDictFor(csFontNameTag.value());
      if (pElement) {
        RetainPtr<CPDF_Font> pFont =
            m_pForm->GetFontForElement(std::move(pElement));
        if (pFont)
          return pFont;
      }
    }
  }
  RetainPtr<CPDF_Font> pFormFont = m_pForm->GetFormFont(csFontNameTag.value());
  if (pFormFont)
    return pFormFont;

  RetainPtr<CPDF_Dictionary> pPageDict = m_pWidgetDict->GetMutableDictFor("P");
  RetainPtr<CPDF_Dictionary> pDict = ToDictionary(
      CPDF_FormField::GetMutableFieldAttrForDict(pPageDict.Get(), "Resources"));
  if (!pDict)
    return nullptr;

  RetainPtr<CPDF_Dictionary> pFonts = pDict->GetMutableDictFor("Font");
  if (!ValidateFontResourceDict(pFonts.Get()))
    return nullptr;

  RetainPtr<CPDF_Dictionary> pElement =
      pFonts->GetMutableDictFor(csFontNameTag.value());
  if (!pElement)
    return nullptr;

  return m_pForm->GetFontForElement(std::move(pElement));
}

int CPDF_FormControl::GetControlAlignment() const {
  if (m_pWidgetDict->KeyExist(pdfium::form_fields::kQ))
    return m_pWidgetDict->GetIntegerFor(pdfium::form_fields::kQ, 0);

  RetainPtr<const CPDF_Object> pObj =
      m_pField->GetFieldAttr(pdfium::form_fields::kQ);
  if (pObj)
    return pObj->GetInteger();

  return m_pForm->GetFormAlignment();
}
