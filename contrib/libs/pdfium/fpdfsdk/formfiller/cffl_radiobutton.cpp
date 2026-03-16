// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "fpdfsdk/formfiller/cffl_radiobutton.h"

#include <utility>

#include "constants/ascii.h"
#include "core/fpdfdoc/cpdf_formcontrol.h"
#include "core/fxcrt/check.h"
#include "fpdfsdk/cpdfsdk_widget.h"
#include "fpdfsdk/formfiller/cffl_formfield.h"
#include "fpdfsdk/pwl/cpwl_special_button.h"
#include "public/fpdf_fwlevent.h"

CFFL_RadioButton::CFFL_RadioButton(CFFL_InteractiveFormFiller* pFormFiller,
                                   CPDFSDK_Widget* pWidget)
    : CFFL_Button(pFormFiller, pWidget) {}

CFFL_RadioButton::~CFFL_RadioButton() = default;

std::unique_ptr<CPWL_Wnd> CFFL_RadioButton::NewPWLWindow(
    const CPWL_Wnd::CreateParams& cp,
    std::unique_ptr<IPWL_FillerNotify::PerWindowData> pAttachedData) {
  auto pWnd = std::make_unique<CPWL_RadioButton>(cp, std::move(pAttachedData));
  pWnd->Realize();
  pWnd->SetCheck(m_pWidget->IsChecked());
  return pWnd;
}

bool CFFL_RadioButton::OnKeyDown(FWL_VKEYCODE nKeyCode,
                                 Mask<FWL_EVENTFLAG> nFlags) {
  switch (nKeyCode) {
    case FWL_VKEY_Return:
    case FWL_VKEY_Space:
      return true;
    default:
      return CFFL_FormField::OnKeyDown(nKeyCode, nFlags);
  }
}

bool CFFL_RadioButton::OnChar(CPDFSDK_Widget* pWidget,
                              uint32_t nChar,
                              Mask<FWL_EVENTFLAG> nFlags) {
  switch (nChar) {
    case pdfium::ascii::kReturn:
    case pdfium::ascii::kSpace: {
      CPDFSDK_PageView* pPageView = pWidget->GetPageView();
      DCHECK(pPageView);

      ObservedPtr<CPDFSDK_Widget> pObserved(m_pWidget);
      if (m_pFormFiller->OnButtonUp(pObserved, pPageView, nFlags) ||
          !pObserved) {
        return true;
      }

      CFFL_FormField::OnChar(pWidget, nChar, nFlags);
      CPWL_RadioButton* pWnd = CreateOrUpdatePWLRadioButton(pPageView);
      if (pWnd && !pWnd->IsReadOnly())
        pWnd->SetCheck(true);
      return CommitData(pPageView, nFlags);
    }
    default:
      return CFFL_FormField::OnChar(pWidget, nChar, nFlags);
  }
}

bool CFFL_RadioButton::OnLButtonUp(CPDFSDK_PageView* pPageView,
                                   CPDFSDK_Widget* pWidget,
                                   Mask<FWL_EVENTFLAG> nFlags,
                                   const CFX_PointF& point) {
  CFFL_Button::OnLButtonUp(pPageView, pWidget, nFlags, point);

  if (!IsValid())
    return true;

  CPWL_RadioButton* pWnd = CreateOrUpdatePWLRadioButton(pPageView);
  if (pWnd)
    pWnd->SetCheck(true);

  return CommitData(pPageView, nFlags);
}

bool CFFL_RadioButton::IsDataChanged(const CPDFSDK_PageView* pPageView) {
  CPWL_RadioButton* pWnd = GetPWLRadioButton(pPageView);
  return pWnd && pWnd->IsChecked() != m_pWidget->IsChecked();
}

void CFFL_RadioButton::SaveData(const CPDFSDK_PageView* pPageView) {
  ObservedPtr<CFFL_RadioButton> observed_this(this);
  CPWL_RadioButton* pWnd = observed_this->GetPWLRadioButton(pPageView);
  if (!pWnd) {
    return;
  }
  bool bNewChecked = pWnd->IsChecked();
  ObservedPtr<CPDFSDK_Widget> observed_widget(observed_this->m_pWidget);
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

CPWL_RadioButton* CFFL_RadioButton::GetPWLRadioButton(
    const CPDFSDK_PageView* pPageView) const {
  return static_cast<CPWL_RadioButton*>(GetPWLWindow(pPageView));
}

CPWL_RadioButton* CFFL_RadioButton::CreateOrUpdatePWLRadioButton(
    const CPDFSDK_PageView* pPageView) {
  return static_cast<CPWL_RadioButton*>(CreateOrUpdatePWLWindow(pPageView));
}
