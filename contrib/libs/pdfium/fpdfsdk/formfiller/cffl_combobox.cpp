// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "fpdfsdk/formfiller/cffl_combobox.h"

#include <utility>

#include "constants/form_flags.h"
#include "core/fpdfdoc/cpdf_bafontmap.h"
#include "fpdfsdk/cpdfsdk_widget.h"
#include "fpdfsdk/formfiller/cffl_interactiveformfiller.h"
#include "fpdfsdk/formfiller/cffl_perwindowdata.h"
#include "fpdfsdk/pwl/cpwl_combo_box.h"
#include "fpdfsdk/pwl/cpwl_edit.h"

CFFL_ComboBox::CFFL_ComboBox(CFFL_InteractiveFormFiller* pFormFiller,
                             CPDFSDK_Widget* pWidget)
    : CFFL_TextObject(pFormFiller, pWidget) {}

CFFL_ComboBox::~CFFL_ComboBox() {
  // See comment in cffl_formfiller.h.
  // The font map should be stored somewhere more appropriate so it will live
  // until the PWL_Edit is done with it. pdfium:566
  DestroyWindows();
}

CPWL_Wnd::CreateParams CFFL_ComboBox::GetCreateParam() {
  CPWL_Wnd::CreateParams cp = CFFL_TextObject::GetCreateParam();
  if (m_pWidget->GetFieldFlags() & pdfium::form_flags::kChoiceEdit)
    cp.dwFlags |= PCBS_ALLOWCUSTOMTEXT;

  cp.pFontMap = GetOrCreateFontMap();
  return cp;
}

std::unique_ptr<CPWL_Wnd> CFFL_ComboBox::NewPWLWindow(
    const CPWL_Wnd::CreateParams& cp,
    std::unique_ptr<IPWL_FillerNotify::PerWindowData> pAttachedData) {
  static_cast<CFFL_PerWindowData*>(pAttachedData.get())->SetFormField(this);
  auto pWnd = std::make_unique<CPWL_ComboBox>(cp, std::move(pAttachedData));
  pWnd->Realize();

  int32_t nCurSel = m_pWidget->GetSelectedIndex(0);
  WideString swText;
  if (nCurSel < 0)
    swText = m_pWidget->GetValue();
  else
    swText = m_pWidget->GetOptionLabel(nCurSel);

  for (int32_t i = 0, sz = m_pWidget->CountOptions(); i < sz; i++)
    pWnd->AddString(m_pWidget->GetOptionLabel(i));

  pWnd->SetSelect(nCurSel);
  pWnd->SetText(swText);
  return pWnd;
}

bool CFFL_ComboBox::OnChar(CPDFSDK_Widget* pWidget,
                           uint32_t nChar,
                           Mask<FWL_EVENTFLAG> nFlags) {
  return CFFL_TextObject::OnChar(pWidget, nChar, nFlags);
}

bool CFFL_ComboBox::IsDataChanged(const CPDFSDK_PageView* pPageView) {
  auto* pWnd = GetPWLComboBox(pPageView);
  if (!pWnd)
    return false;

  int32_t nCurSel = pWnd->GetSelect();
  if (!(m_pWidget->GetFieldFlags() & pdfium::form_flags::kChoiceEdit))
    return nCurSel != m_pWidget->GetSelectedIndex(0);

  if (nCurSel >= 0)
    return nCurSel != m_pWidget->GetSelectedIndex(0);

  return pWnd->GetText() != m_pWidget->GetValue();
}

void CFFL_ComboBox::SaveData(const CPDFSDK_PageView* pPageView) {
  ObservedPtr<CFFL_ComboBox> observed_this(this);
  CPWL_ComboBox* pWnd = observed_this->GetPWLComboBox(pPageView);
  if (!pWnd) {
    return;
  }
  WideString swText = pWnd->GetText();
  int32_t nCurSel = pWnd->GetSelect();
  bool bSetValue = false;
  ObservedPtr<CPDFSDK_Widget> observed_widget(observed_this->m_pWidget);
  if (observed_widget->GetFieldFlags() & pdfium::form_flags::kChoiceEdit) {
    bSetValue =
        (nCurSel < 0) || (swText != observed_widget->GetOptionLabel(nCurSel));
  }
  if (bSetValue) {
    observed_widget->SetValue(swText);
  } else {
    observed_widget->GetSelectedIndex(0);
    observed_widget->SetOptionSelection(nCurSel);
  }
  if (!observed_widget) {
    return;
  }
  observed_widget->ResetFieldAppearance();
  if (!observed_widget) {
    return;
  }
  observed_widget->UpdateField();
  if (!observed_widget || !observed_this) {
    return;
  }
  observed_this->SetChangeMark();
}

void CFFL_ComboBox::GetActionData(const CPDFSDK_PageView* pPageView,
                                  CPDF_AAction::AActionType type,
                                  CFFL_FieldAction& fa) {
  switch (type) {
    case CPDF_AAction::kKeyStroke:
      if (CPWL_ComboBox* pComboBox = GetPWLComboBox(pPageView)) {
        if (CPWL_Edit* pEdit = pComboBox->GetEdit()) {
          fa.bFieldFull = pEdit->IsTextFull();
          std::tie(fa.nSelStart, fa.nSelEnd) = pEdit->GetSelection();
          fa.sValue = pEdit->GetText();
          fa.sChangeEx = GetSelectExportText();

          if (fa.bFieldFull) {
            fa.sChange.clear();
            fa.sChangeEx.clear();
          }
        }
      }
      break;
    case CPDF_AAction::kValidate:
      if (CPWL_ComboBox* pComboBox = GetPWLComboBox(pPageView)) {
        if (CPWL_Edit* pEdit = pComboBox->GetEdit()) {
          fa.sValue = pEdit->GetText();
        }
      }
      break;
    case CPDF_AAction::kLoseFocus:
    case CPDF_AAction::kGetFocus:
      fa.sValue = m_pWidget->GetValue();
      break;
    default:
      break;
  }
}

void CFFL_ComboBox::SetActionData(const CPDFSDK_PageView* pPageView,
                                  CPDF_AAction::AActionType type,
                                  const CFFL_FieldAction& fa) {
  switch (type) {
    case CPDF_AAction::kKeyStroke:
      if (CPWL_ComboBox* pComboBox = GetPWLComboBox(pPageView)) {
        if (CPWL_Edit* pEdit = pComboBox->GetEdit()) {
          pEdit->SetSelection(fa.nSelStart, fa.nSelEnd);
          pEdit->ReplaceSelection(fa.sChange);
        }
      }
      break;
    default:
      break;
  }
}

void CFFL_ComboBox::SavePWLWindowState(const CPDFSDK_PageView* pPageView) {
  CPWL_ComboBox* pComboBox = GetPWLComboBox(pPageView);
  if (!pComboBox)
    return;

  m_State.nIndex = pComboBox->GetSelect();

  CPWL_Edit* pEdit = pComboBox->GetEdit();
  if (!pEdit)
    return;

  std::tie(m_State.nStart, m_State.nEnd) = pEdit->GetSelection();
  m_State.sValue = pEdit->GetText();
}

void CFFL_ComboBox::RecreatePWLWindowFromSavedState(
    const CPDFSDK_PageView* pPageView) {
  CPWL_ComboBox* pComboBox = CreateOrUpdatePWLComboBox(pPageView);
  if (!pComboBox)
    return;

  if (m_State.nIndex >= 0) {
    pComboBox->SetSelect(m_State.nIndex);
    return;
  }

  CPWL_Edit* pEdit = pComboBox->GetEdit();
  if (!pEdit)
    return;

  pEdit->SetText(m_State.sValue);
  pEdit->SetSelection(m_State.nStart, m_State.nEnd);
}

bool CFFL_ComboBox::SetIndexSelected(int index, bool selected) {
  if (!IsValid() || !selected)
    return false;

  if (index < 0 || index >= m_pWidget->CountOptions())
    return false;

  CPWL_ComboBox* pWnd = GetPWLComboBox(GetCurPageView());
  if (!pWnd)
    return false;

  pWnd->SetSelect(index);
  return true;
}

bool CFFL_ComboBox::IsIndexSelected(int index) {
  if (!IsValid())
    return false;

  if (index < 0 || index >= m_pWidget->CountOptions())
    return false;

  CPWL_ComboBox* pWnd = GetPWLComboBox(GetCurPageView());
  return pWnd && index == pWnd->GetSelect();
}

#ifdef PDF_ENABLE_XFA
bool CFFL_ComboBox::IsFieldFull(const CPDFSDK_PageView* pPageView) {
  CPWL_ComboBox* pComboBox = GetPWLComboBox(pPageView);
  if (!pComboBox)
    return false;

  CPWL_Edit* pEdit = pComboBox->GetEdit();
  return pEdit && pEdit->IsTextFull();
}
#endif  // PDF_ENABLE_XFA

void CFFL_ComboBox::OnSetFocusForEdit(CPWL_Edit* pEdit) {
  pEdit->SetCharSet(FX_Charset::kChineseSimplified);
  pEdit->SetReadyToInput();
  m_pFormFiller->OnSetFieldInputFocus(pEdit->GetText());
}

WideString CFFL_ComboBox::GetSelectExportText() {
  CPWL_ComboBox* pComboBox = GetPWLComboBox(GetCurPageView());
  int nExport = pComboBox ? pComboBox->GetSelect() : -1;
  return m_pWidget->GetSelectExportText(nExport);
}

CPWL_ComboBox* CFFL_ComboBox::GetPWLComboBox(
    const CPDFSDK_PageView* pPageView) const {
  return static_cast<CPWL_ComboBox*>(GetPWLWindow(pPageView));
}

CPWL_ComboBox* CFFL_ComboBox::CreateOrUpdatePWLComboBox(
    const CPDFSDK_PageView* pPageView) {
  return static_cast<CPWL_ComboBox*>(CreateOrUpdatePWLWindow(pPageView));
}
