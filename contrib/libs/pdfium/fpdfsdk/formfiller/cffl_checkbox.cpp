// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "fpdfsdk/formfiller/cffl_checkbox.h"

#include <utility>

#include "constants/ascii.h"
#include "core/fpdfdoc/cpdf_formcontrol.h"
#include "core/fxcrt/check.h"
#include "fpdfsdk/cpdfsdk_widget.h"
#include "fpdfsdk/formfiller/cffl_formfield.h"
#include "fpdfsdk/pwl/cpwl_special_button.h"
#include "public/fpdf_fwlevent.h"

CFFL_CheckBox::CFFL_CheckBox(CFFL_InteractiveFormFiller* pFormFiller,
                             CPDFSDK_Widget* pWidget)
    : CFFL_Button(pFormFiller, pWidget) {}

CFFL_CheckBox::~CFFL_CheckBox() = default;

std::unique_ptr<CPWL_Wnd> CFFL_CheckBox::NewPWLWindow(
    const CPWL_Wnd::CreateParams& cp,
    std::unique_ptr<IPWL_FillerNotify::PerWindowData> pAttachedData) {
  auto pWnd = std::make_unique<CPWL_CheckBox>(cp, std::move(pAttachedData));
  pWnd->Realize();
  pWnd->SetCheck(m_pWidget->IsChecked());
  return pWnd;
}

bool CFFL_CheckBox::OnKeyDown(FWL_VKEYCODE nKeyCode,
                              Mask<FWL_EVENTFLAG> nFlags) {
  switch (nKeyCode) {
    case FWL_VKEY_Return:
    case FWL_VKEY_Space:
      return true;
    default:
      return CFFL_FormField::OnKeyDown(nKeyCode, nFlags);
  }
}

bool CFFL_CheckBox::OnChar(CPDFSDK_Widget* pWidget,
                           uint32_t nChar,
                           Mask<FWL_EVENTFLAG> nFlags) {
  switch (nChar) {
    case pdfium::ascii::kReturn:
    case pdfium::ascii::kSpace: {
      CPDFSDK_PageView* pPageView = pWidget->GetPageView();
      DCHECK(pPageView);

      ObservedPtr<CPDFSDK_Widget> pObserved(m_pWidget);
      if (m_pFormFiller->OnButtonUp(pObserved, pPageView, nFlags)) {
        if (!pObserved)
          m_pWidget = nullptr;
        return true;
      }
      if (!pObserved) {
        m_pWidget = nullptr;
        return true;
      }

      CFFL_FormField::OnChar(pWidget, nChar, nFlags);

      CPWL_CheckBox* pWnd = CreateOrUpdatePWLCheckBox(pPageView);
      if (pWnd && !pWnd->IsReadOnly()) {
        ObservedPtr<CPWL_CheckBox> pObservedBox(pWnd);
        const bool is_checked = pWidget->IsChecked();
        if (pObservedBox) {
          pObservedBox->SetCheck(!is_checked);
        }
      }
      return CommitData(pPageView, nFlags);
    }
    default:
      return CFFL_FormField::OnChar(pWidget, nChar, nFlags);
  }
}

bool CFFL_CheckBox::OnLButtonUp(CPDFSDK_PageView* pPageView,
                                CPDFSDK_Widget* pWidget,
                                Mask<FWL_EVENTFLAG> nFlags,
                                const CFX_PointF& point) {
  CFFL_Button::OnLButtonUp(pPageView, pWidget, nFlags, point);
  if (!IsValid()) {
    return true;
  }
  ObservedPtr<CPWL_CheckBox> pWnd(CreateOrUpdatePWLCheckBox(pPageView));
  if (pWnd) {
    // IsChecked() may invalidate check box.
    const bool is_checked = pWidget->IsChecked();
    if (pWnd) {
      pWnd->SetCheck(!is_checked);
    }
  }
  return CommitData(pPageView, nFlags);
}

bool CFFL_CheckBox::IsDataChanged(const CPDFSDK_PageView* pPageView) {
  CPWL_CheckBox* pWnd = GetPWLCheckBox(pPageView);
  return pWnd && pWnd->IsChecked() != m_pWidget->IsChecked();
}

void CFFL_CheckBox::SaveData(const CPDFSDK_PageView* pPageView) {
  ObservedPtr<CFFL_CheckBox> observed_this(this);
  CPWL_CheckBox* pWnd = observed_this->GetPWLCheckBox(pPageView);
  if (!pWnd) {
    return;
  }
  bool bNewChecked = pWnd->IsChecked();
  ObservedPtr<CPDFSDK_Widget> observed_widget(m_pWidget);
  observed_widget->SetCheck(bNewChecked);
  if (!observed_widget) {
    return;
  }
  observed_widget->UpdateField();
  if (!observed_widget || !observed_this) {
    return;
  }
  observed_this->SetChangeMark();
}

CPWL_CheckBox* CFFL_CheckBox::GetPWLCheckBox(
    const CPDFSDK_PageView* pPageView) const {
  return static_cast<CPWL_CheckBox*>(GetPWLWindow(pPageView));
}

CPWL_CheckBox* CFFL_CheckBox::CreateOrUpdatePWLCheckBox(
    const CPDFSDK_PageView* pPageView) {
  return static_cast<CPWL_CheckBox*>(CreateOrUpdatePWLWindow(pPageView));
}
