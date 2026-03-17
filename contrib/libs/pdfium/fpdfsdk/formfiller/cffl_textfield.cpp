// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "fpdfsdk/formfiller/cffl_textfield.h"

#include <utility>

#include "constants/ascii.h"
#include "constants/form_flags.h"
#include "core/fpdfdoc/cpdf_bafontmap.h"
#include "core/fxcrt/check.h"
#include "fpdfsdk/cpdfsdk_widget.h"
#include "fpdfsdk/formfiller/cffl_perwindowdata.h"
#include "fpdfsdk/pwl/cpwl_edit.h"
#include "public/fpdf_fwlevent.h"

namespace {

// PDF 1.7 spec, Table 8.25
enum Alignment {
  kLeft = 0,
  kCenter = 1,
  kRight = 2,
};

}  // namespace

CFFL_TextField::CFFL_TextField(CFFL_InteractiveFormFiller* pFormFiller,
                               CPDFSDK_Widget* pWidget)
    : CFFL_TextObject(pFormFiller, pWidget) {}

CFFL_TextField::~CFFL_TextField() {
  // See comment in cffl_formfiller.h.
  // The font map should be stored somewhere more appropriate so it will live
  // until the PWL_Edit is done with it. pdfium:566
  DestroyWindows();
}

CPWL_Wnd::CreateParams CFFL_TextField::GetCreateParam() {
  CPWL_Wnd::CreateParams cp = CFFL_TextObject::GetCreateParam();
  int nFlags = m_pWidget->GetFieldFlags();
  if (nFlags & pdfium::form_flags::kTextPassword)
    cp.dwFlags |= PES_PASSWORD;

  if (nFlags & pdfium::form_flags::kTextMultiline) {
    cp.dwFlags |= PES_MULTILINE | PES_AUTORETURN | PES_TOP;
    if (!(nFlags & pdfium::form_flags::kTextDoNotScroll))
      cp.dwFlags |= PWS_VSCROLL | PES_AUTOSCROLL;
  } else {
    cp.dwFlags |= PES_CENTER;
    if (!(nFlags & pdfium::form_flags::kTextDoNotScroll))
      cp.dwFlags |= PES_AUTOSCROLL;
  }

  if (nFlags & pdfium::form_flags::kTextComb)
    cp.dwFlags |= PES_CHARARRAY;

  if (nFlags & pdfium::form_flags::kTextRichText)
    cp.dwFlags |= PES_RICH;

  cp.dwFlags |= PES_UNDO;

  switch (m_pWidget->GetAlignment()) {
    default:
    case kLeft:
      cp.dwFlags |= PES_LEFT;
      break;
    case kCenter:
      cp.dwFlags |= PES_MIDDLE;
      break;
    case kRight:
      cp.dwFlags |= PES_RIGHT;
      break;
  }
  cp.pFontMap = GetOrCreateFontMap();
  return cp;
}

std::unique_ptr<CPWL_Wnd> CFFL_TextField::NewPWLWindow(
    const CPWL_Wnd::CreateParams& cp,
    std::unique_ptr<IPWL_FillerNotify::PerWindowData> pAttachedData) {
  static_cast<CFFL_PerWindowData*>(pAttachedData.get())->SetFormField(this);
  auto pWnd = std::make_unique<CPWL_Edit>(cp, std::move(pAttachedData));
  pWnd->Realize();

  int32_t nMaxLen = m_pWidget->GetMaxLen();
  WideString swValue = m_pWidget->GetValue();
  if (nMaxLen > 0) {
    if (pWnd->HasFlag(PES_CHARARRAY)) {
      pWnd->SetCharArray(nMaxLen);
      pWnd->SetAlignFormatVerticalCenter();
    } else {
      pWnd->SetLimitChar(nMaxLen);
    }
  }
  pWnd->SetText(swValue);
  return pWnd;
}

bool CFFL_TextField::OnChar(CPDFSDK_Widget* pWidget,
                            uint32_t nChar,
                            Mask<FWL_EVENTFLAG> nFlags) {
  switch (nChar) {
    case pdfium::ascii::kReturn: {
      if (m_pWidget->GetFieldFlags() & pdfium::form_flags::kTextMultiline)
        break;

      CPDFSDK_PageView* pPageView = GetCurPageView();
      DCHECK(pPageView);
      m_bValid = !m_bValid;
      m_pFormFiller->Invalidate(pWidget->GetPage(),
                                pWidget->GetRect().GetOuterRect());
      if (m_bValid) {
        if (CPWL_Wnd* pWnd = CreateOrUpdatePWLWindow(pPageView))
          pWnd->SetFocus();
        break;
      }

      if (!CommitData(pPageView, nFlags))
        return false;

      DestroyPWLWindow(pPageView);
      return true;
    }
    case pdfium::ascii::kEscape: {
      CPDFSDK_PageView* pPageView = GetCurPageView();
      DCHECK(pPageView);
      EscapeFiller(pPageView, true);
      return true;
    }
  }

  return CFFL_TextObject::OnChar(pWidget, nChar, nFlags);
}

bool CFFL_TextField::IsDataChanged(const CPDFSDK_PageView* pPageView) {
  CPWL_Edit* pEdit = GetPWLEdit(pPageView);
  return pEdit && pEdit->GetText() != m_pWidget->GetValue();
}

void CFFL_TextField::SaveData(const CPDFSDK_PageView* pPageView) {
  ObservedPtr<CFFL_TextField> observed_this(this);
  ObservedPtr<CPWL_Edit> observed_edit(observed_this->GetPWLEdit(pPageView));
  if (!observed_edit) {
    return;
  }
  WideString sOldValue = observed_this->m_pWidget->GetValue();
  if (!observed_edit) {
    return;
  }
  WideString sNewValue = observed_edit->GetText();
  ObservedPtr<CPDFSDK_Widget> observed_widget(observed_this->m_pWidget);
  observed_widget->SetValue(sNewValue);
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

void CFFL_TextField::GetActionData(const CPDFSDK_PageView* pPageView,
                                   CPDF_AAction::AActionType type,
                                   CFFL_FieldAction& fa) {
  switch (type) {
    case CPDF_AAction::kKeyStroke:
      if (CPWL_Edit* pWnd = GetPWLEdit(pPageView)) {
        fa.bFieldFull = pWnd->IsTextFull();
        fa.sValue = pWnd->GetText();
        if (fa.bFieldFull) {
          fa.sChange.clear();
          fa.sChangeEx.clear();
        }
      }
      break;
    case CPDF_AAction::kValidate:
      if (CPWL_Edit* pWnd = GetPWLEdit(pPageView)) {
        fa.sValue = pWnd->GetText();
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

void CFFL_TextField::SetActionData(const CPDFSDK_PageView* pPageView,
                                   CPDF_AAction::AActionType type,
                                   const CFFL_FieldAction& fa) {
  switch (type) {
    case CPDF_AAction::kKeyStroke:
      if (CPWL_Edit* pEdit = GetPWLEdit(pPageView)) {
        pEdit->SetFocus();
        pEdit->SetSelection(fa.nSelStart, fa.nSelEnd);
        pEdit->ReplaceSelection(fa.sChange);
      }
      break;
    default:
      break;
  }
}

void CFFL_TextField::SavePWLWindowState(const CPDFSDK_PageView* pPageView) {
  CPWL_Edit* pWnd = GetPWLEdit(pPageView);
  if (!pWnd)
    return;

  std::tie(m_State.nStart, m_State.nEnd) = pWnd->GetSelection();
  m_State.sValue = pWnd->GetText();
}

void CFFL_TextField::RecreatePWLWindowFromSavedState(
    const CPDFSDK_PageView* pPageView) {
  CPWL_Edit* pWnd = CreateOrUpdatePWLEdit(pPageView);
  if (!pWnd)
    return;

  pWnd->SetText(m_State.sValue);
  pWnd->SetSelection(m_State.nStart, m_State.nEnd);
}

#ifdef PDF_ENABLE_XFA
bool CFFL_TextField::IsFieldFull(const CPDFSDK_PageView* pPageView) {
  CPWL_Edit* pWnd = GetPWLEdit(pPageView);
  return pWnd && pWnd->IsTextFull();
}
#endif  // PDF_ENABLE_XFA

void CFFL_TextField::OnSetFocusForEdit(CPWL_Edit* pEdit) {
  pEdit->SetCharSet(FX_Charset::kChineseSimplified);
  pEdit->SetReadyToInput();
  m_pFormFiller->OnSetFieldInputFocus(pEdit->GetText());
}

CPWL_Edit* CFFL_TextField::GetPWLEdit(const CPDFSDK_PageView* pPageView) const {
  return static_cast<CPWL_Edit*>(GetPWLWindow(pPageView));
}

CPWL_Edit* CFFL_TextField::CreateOrUpdatePWLEdit(
    const CPDFSDK_PageView* pPageView) {
  return static_cast<CPWL_Edit*>(CreateOrUpdatePWLWindow(pPageView));
}
