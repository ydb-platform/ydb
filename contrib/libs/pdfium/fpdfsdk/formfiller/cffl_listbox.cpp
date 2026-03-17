// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "fpdfsdk/formfiller/cffl_listbox.h"

#include <utility>

#include "constants/form_flags.h"
#include "core/fpdfdoc/cpdf_bafontmap.h"
#include "core/fxcrt/containers/contains.h"
#include "fpdfsdk/cpdfsdk_widget.h"
#include "fpdfsdk/formfiller/cffl_interactiveformfiller.h"
#include "fpdfsdk/formfiller/cffl_perwindowdata.h"
#include "fpdfsdk/pwl/cpwl_list_box.h"

CFFL_ListBox::CFFL_ListBox(CFFL_InteractiveFormFiller* pFormFiller,
                           CPDFSDK_Widget* pWidget)
    : CFFL_TextObject(pFormFiller, pWidget) {}

CFFL_ListBox::~CFFL_ListBox() = default;

CPWL_Wnd::CreateParams CFFL_ListBox::GetCreateParam() {
  CPWL_Wnd::CreateParams cp = CFFL_TextObject::GetCreateParam();
  uint32_t dwFieldFlag = m_pWidget->GetFieldFlags();
  if (dwFieldFlag & pdfium::form_flags::kChoiceMultiSelect)
    cp.dwFlags |= PLBS_MULTIPLESEL;

  cp.dwFlags |= PWS_VSCROLL;

  if (cp.dwFlags & PWS_AUTOFONTSIZE) {
    constexpr float kDefaultListBoxFontSize = 12.0f;
    cp.fFontSize = kDefaultListBoxFontSize;
  }

  cp.pFontMap = GetOrCreateFontMap();
  return cp;
}

std::unique_ptr<CPWL_Wnd> CFFL_ListBox::NewPWLWindow(
    const CPWL_Wnd::CreateParams& cp,
    std::unique_ptr<IPWL_FillerNotify::PerWindowData> pAttachedData) {
  static_cast<CFFL_PerWindowData*>(pAttachedData.get())->SetFormField(this);
  auto pWnd = std::make_unique<CPWL_ListBox>(cp, std::move(pAttachedData));
  pWnd->Realize();

  for (int32_t i = 0, sz = m_pWidget->CountOptions(); i < sz; i++)
    pWnd->AddString(m_pWidget->GetOptionLabel(i));

  if (pWnd->HasFlag(PLBS_MULTIPLESEL)) {
    m_OriginSelections.clear();

    bool bSetCaret = false;
    for (int32_t i = 0, sz = m_pWidget->CountOptions(); i < sz; i++) {
      if (m_pWidget->IsOptionSelected(i)) {
        if (!bSetCaret) {
          pWnd->SetCaret(i);
          bSetCaret = true;
        }
        pWnd->Select(i);
        m_OriginSelections.insert(i);
      }
    }
  } else {
    for (int i = 0, sz = m_pWidget->CountOptions(); i < sz; i++) {
      if (m_pWidget->IsOptionSelected(i)) {
        pWnd->Select(i);
        break;
      }
    }
  }

  pWnd->SetTopVisibleIndex(m_pWidget->GetTopVisibleIndex());
  return pWnd;
}

bool CFFL_ListBox::OnChar(CPDFSDK_Widget* pWidget,
                          uint32_t nChar,
                          Mask<FWL_EVENTFLAG> nFlags) {
  return CFFL_TextObject::OnChar(pWidget, nChar, nFlags);
}

bool CFFL_ListBox::IsDataChanged(const CPDFSDK_PageView* pPageView) {
  CPWL_ListBox* pListBox = GetPWLListBox(pPageView);
  if (!pListBox)
    return false;

  if (m_pWidget->GetFieldFlags() & pdfium::form_flags::kChoiceMultiSelect) {
    size_t nSelCount = 0;
    for (int32_t i = 0, sz = pListBox->GetCount(); i < sz; ++i) {
      if (pListBox->IsItemSelected(i)) {
        if (!pdfium::Contains(m_OriginSelections, i))
          return true;

        ++nSelCount;
      }
    }

    return nSelCount != m_OriginSelections.size();
  }
  return pListBox->GetCurSel() != m_pWidget->GetSelectedIndex(0);
}

void CFFL_ListBox::SaveData(const CPDFSDK_PageView* pPageView) {
  CPWL_ListBox* pListBox = GetPWLListBox(pPageView);
  if (!pListBox) {
    return;
  }
  int32_t nNewTopIndex = pListBox->GetTopVisibleIndex();
  ObservedPtr<CPWL_ListBox> observed_box(pListBox);
  ObservedPtr<CPDFSDK_Widget> observed_widget(m_pWidget);
  observed_widget->ClearSelection();
  if (!observed_box || !observed_widget) {
    return;
  }
  if (observed_widget->GetFieldFlags() &
      pdfium::form_flags::kChoiceMultiSelect) {
    for (int32_t i = 0, sz = observed_box->GetCount(); i < sz; i++) {
      if (observed_box->IsItemSelected(i)) {
        observed_widget->SetOptionSelection(i);
        if (!observed_box || !observed_widget) {
          return;
        }
      }
    }
  } else {
    observed_widget->SetOptionSelection(observed_box->GetCurSel());
    if (!observed_box || !observed_widget) {
      return;
    }
  }
  ObservedPtr<CFFL_ListBox> observed_this(this);
  observed_widget->SetTopVisibleIndex(nNewTopIndex);
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

void CFFL_ListBox::GetActionData(const CPDFSDK_PageView* pPageView,
                                 CPDF_AAction::AActionType type,
                                 CFFL_FieldAction& fa) {
  switch (type) {
    case CPDF_AAction::kValidate:
      if (m_pWidget->GetFieldFlags() & pdfium::form_flags::kChoiceMultiSelect) {
        fa.sValue.clear();
      } else {
        CPWL_ListBox* pListBox = GetPWLListBox(pPageView);
        if (pListBox) {
          int32_t nCurSel = pListBox->GetCurSel();
          if (nCurSel >= 0)
            fa.sValue = m_pWidget->GetOptionLabel(nCurSel);
        }
      }
      break;
    case CPDF_AAction::kLoseFocus:
    case CPDF_AAction::kGetFocus:
      if (m_pWidget->GetFieldFlags() & pdfium::form_flags::kChoiceMultiSelect) {
        fa.sValue.clear();
      } else {
        int32_t nCurSel = m_pWidget->GetSelectedIndex(0);
        if (nCurSel >= 0)
          fa.sValue = m_pWidget->GetOptionLabel(nCurSel);
      }
      break;
    default:
      break;
  }
}

void CFFL_ListBox::SavePWLWindowState(const CPDFSDK_PageView* pPageView) {
  CPWL_ListBox* pListBox = GetPWLListBox(pPageView);
  if (!pListBox)
    return;

  for (int32_t i = 0, sz = pListBox->GetCount(); i < sz; i++) {
    if (pListBox->IsItemSelected(i))
      m_State.push_back(i);
  }
}

void CFFL_ListBox::RecreatePWLWindowFromSavedState(
    const CPDFSDK_PageView* pPageView) {
  CPWL_ListBox* pListBox = CreateOrUpdatePWLListBox(pPageView);
  if (!pListBox)
    return;

  for (const auto& item : m_State)
    pListBox->Select(item);
}

bool CFFL_ListBox::SetIndexSelected(int index, bool selected) {
  if (!IsValid())
    return false;

  if (index < 0 || index >= m_pWidget->CountOptions())
    return false;

  CPWL_ListBox* pListBox = GetPWLListBox(GetCurPageView());
  if (!pListBox)
    return false;

  if (selected) {
    pListBox->Select(index);
    pListBox->SetCaret(index);
  } else {
    pListBox->Deselect(index);
    pListBox->SetCaret(index);
  }

  return true;
}

bool CFFL_ListBox::IsIndexSelected(int index) {
  if (!IsValid())
    return false;

  if (index < 0 || index >= m_pWidget->CountOptions())
    return false;

  CPWL_ListBox* pListBox = GetPWLListBox(GetCurPageView());
  return pListBox && pListBox->IsItemSelected(index);
}

CPWL_ListBox* CFFL_ListBox::GetPWLListBox(
    const CPDFSDK_PageView* pPageView) const {
  return static_cast<CPWL_ListBox*>(GetPWLWindow(pPageView));
}

CPWL_ListBox* CFFL_ListBox::CreateOrUpdatePWLListBox(
    const CPDFSDK_PageView* pPageView) {
  return static_cast<CPWL_ListBox*>(CreateOrUpdatePWLWindow(pPageView));
}
