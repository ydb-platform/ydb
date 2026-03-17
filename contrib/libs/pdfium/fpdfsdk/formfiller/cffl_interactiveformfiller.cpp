// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "fpdfsdk/formfiller/cffl_interactiveformfiller.h"

#include <algorithm>

#include "constants/access_permissions.h"
#include "constants/ascii.h"
#include "constants/form_flags.h"
#include "core/fpdfapi/page/cpdf_page.h"
#include "core/fxcrt/autorestorer.h"
#include "core/fxcrt/check.h"
#include "core/fxge/cfx_drawutils.h"
#include "fpdfsdk/cpdfsdk_pageview.h"
#include "fpdfsdk/cpdfsdk_widget.h"
#include "fpdfsdk/formfiller/cffl_checkbox.h"
#include "fpdfsdk/formfiller/cffl_combobox.h"
#include "fpdfsdk/formfiller/cffl_formfield.h"
#include "fpdfsdk/formfiller/cffl_listbox.h"
#include "fpdfsdk/formfiller/cffl_perwindowdata.h"
#include "fpdfsdk/formfiller/cffl_pushbutton.h"
#include "fpdfsdk/formfiller/cffl_radiobutton.h"
#include "fpdfsdk/formfiller/cffl_textfield.h"
#include "public/fpdf_fwlevent.h"

CFFL_InteractiveFormFiller::CFFL_InteractiveFormFiller(
    CallbackIface* pCallbackIface)
    : m_pCallbackIface(pCallbackIface) {}

CFFL_InteractiveFormFiller::~CFFL_InteractiveFormFiller() = default;

bool CFFL_InteractiveFormFiller::Annot_HitTest(const CPDFSDK_Widget* pWidget,
                                               const CFX_PointF& point) {
  return pWidget->GetRect().Contains(point);
}

FX_RECT CFFL_InteractiveFormFiller::GetViewBBox(
    const CPDFSDK_PageView* pPageView,
    CPDFSDK_Widget* pWidget) {
  if (CFFL_FormField* pFormField = GetFormField(pWidget))
    return pFormField->GetViewBBox(pPageView);

  DCHECK(pPageView);

  CPDF_Annot* pPDFAnnot = pWidget->GetPDFAnnot();
  CFX_FloatRect rcWin = pPDFAnnot->GetRect();
  if (!rcWin.IsEmpty()) {
    rcWin.Inflate(1, 1);
    rcWin.Normalize();
  }
  return rcWin.GetOuterRect();
}

void CFFL_InteractiveFormFiller::OnDraw(CPDFSDK_PageView* pPageView,
                                        CPDFSDK_Widget* pWidget,
                                        CFX_RenderDevice* pDevice,
                                        const CFX_Matrix& mtUser2Device) {
  DCHECK(pPageView);
  if (!IsVisible(pWidget))
    return;

  CFFL_FormField* pFormField = GetFormField(pWidget);
  if (pFormField && pFormField->IsValid()) {
    pFormField->OnDraw(pPageView, pWidget, pDevice, mtUser2Device);
    if (m_pCallbackIface->GetFocusAnnot() != pWidget)
      return;

    CFX_FloatRect rcFocus = pFormField->GetFocusBox(pPageView);
    if (rcFocus.IsEmpty())
      return;

    CFX_DrawUtils::DrawFocusRect(pDevice, mtUser2Device, rcFocus);

    return;
  }

  if (pFormField) {
    pFormField->OnDrawDeactive(pPageView, pWidget, pDevice, mtUser2Device);
  } else {
    pWidget->DrawAppearance(pDevice, mtUser2Device,
                            CPDF_Annot::AppearanceMode::kNormal);
  }

  if (!IsReadOnly(pWidget) && IsFillingAllowed(pWidget))
    pWidget->DrawShadow(pDevice, pPageView);
}

void CFFL_InteractiveFormFiller::OnDelete(CPDFSDK_Widget* pWidget) {
  UnregisterFormField(pWidget);
}

void CFFL_InteractiveFormFiller::OnMouseEnter(
    CPDFSDK_PageView* pPageView,
    ObservedPtr<CPDFSDK_Widget>& pWidget,
    Mask<FWL_EVENTFLAG> nFlag) {
  if (!m_bNotifying) {
    if (pWidget->GetAAction(CPDF_AAction::kCursorEnter).HasDict()) {
      uint32_t nValueAge = pWidget->GetValueAge();
      pWidget->ClearAppModified();
      DCHECK(pPageView);
      {
        AutoRestorer<bool> restorer(&m_bNotifying);
        m_bNotifying = true;

        CFFL_FieldAction fa;
        fa.bModifier = CPWL_Wnd::IsCTRLKeyDown(nFlag);
        fa.bShift = CPWL_Wnd::IsSHIFTKeyDown(nFlag);
        pWidget->OnAAction(CPDF_AAction::kCursorEnter, &fa, pPageView);
      }
      if (!pWidget)
        return;

      if (pWidget->IsAppModified()) {
        CFFL_FormField* pFormField = GetFormField(pWidget.Get());
        if (pFormField)
          pFormField->ResetPWLWindowForValueAge(pPageView, pWidget.Get(),
                                                nValueAge);
      }
    }
  }
  if (CFFL_FormField* pFormField = GetOrCreateFormField(pWidget.Get()))
    pFormField->OnMouseEnter(pPageView);
}

void CFFL_InteractiveFormFiller::OnMouseExit(
    CPDFSDK_PageView* pPageView,
    ObservedPtr<CPDFSDK_Widget>& pWidget,
    Mask<FWL_EVENTFLAG> nFlag) {
  if (!m_bNotifying) {
    if (pWidget->GetAAction(CPDF_AAction::kCursorExit).HasDict()) {
      uint32_t nValueAge = pWidget->GetValueAge();
      pWidget->ClearAppModified();
      DCHECK(pPageView);
      {
        AutoRestorer<bool> restorer(&m_bNotifying);
        m_bNotifying = true;

        CFFL_FieldAction fa;
        fa.bModifier = CPWL_Wnd::IsCTRLKeyDown(nFlag);
        fa.bShift = CPWL_Wnd::IsSHIFTKeyDown(nFlag);
        pWidget->OnAAction(CPDF_AAction::kCursorExit, &fa, pPageView);
      }
      if (!pWidget)
        return;

      if (pWidget->IsAppModified()) {
        CFFL_FormField* pFormField = GetFormField(pWidget.Get());
        if (pFormField) {
          pFormField->ResetPWLWindowForValueAge(pPageView, pWidget.Get(),
                                                nValueAge);
        }
      }
    }
  }
  if (CFFL_FormField* pFormField = GetFormField(pWidget.Get()))
    pFormField->OnMouseExit(pPageView);
}

bool CFFL_InteractiveFormFiller::OnLButtonDown(
    CPDFSDK_PageView* pPageView,
    ObservedPtr<CPDFSDK_Widget>& pWidget,
    Mask<FWL_EVENTFLAG> nFlags,
    const CFX_PointF& point) {
  if (!m_bNotifying) {
    if (Annot_HitTest(pWidget.Get(), point) &&
        pWidget->GetAAction(CPDF_AAction::kButtonDown).HasDict()) {
      uint32_t nValueAge = pWidget->GetValueAge();
      pWidget->ClearAppModified();
      DCHECK(pPageView);
      {
        AutoRestorer<bool> restorer(&m_bNotifying);
        m_bNotifying = true;

        CFFL_FieldAction fa;
        fa.bModifier = CPWL_Wnd::IsCTRLKeyDown(nFlags);
        fa.bShift = CPWL_Wnd::IsSHIFTKeyDown(nFlags);
        pWidget->OnAAction(CPDF_AAction::kButtonDown, &fa, pPageView);
      }
      if (!pWidget)
        return true;

      if (!IsValidAnnot(pPageView, pWidget.Get()))
        return true;

      if (pWidget->IsAppModified()) {
        CFFL_FormField* pFormField = GetFormField(pWidget.Get());
        if (pFormField) {
          pFormField->ResetPWLWindowForValueAge(pPageView, pWidget.Get(),
                                                nValueAge);
        }
      }
    }
  }
  CFFL_FormField* pFormField = GetFormField(pWidget.Get());
  return pFormField &&
         pFormField->OnLButtonDown(pPageView, pWidget.Get(), nFlags, point);
}

bool CFFL_InteractiveFormFiller::OnLButtonUp(
    CPDFSDK_PageView* pPageView,
    ObservedPtr<CPDFSDK_Widget>& pWidget,
    Mask<FWL_EVENTFLAG> nFlags,
    const CFX_PointF& point) {
  bool bSetFocus;
  switch (pWidget->GetFieldType()) {
    case FormFieldType::kPushButton:
    case FormFieldType::kCheckBox:
    case FormFieldType::kRadioButton: {
      FX_RECT bbox = GetViewBBox(pPageView, pWidget.Get());
      bSetFocus =
          bbox.Contains(static_cast<int>(point.x), static_cast<int>(point.y));
      break;
    }
    default:
      bSetFocus = true;
      break;
  }
  if (bSetFocus) {
    ObservedPtr<CPDFSDK_Annot> pObserved(pWidget.Get());
    m_pCallbackIface->SetFocusAnnot(pObserved);
  }

  CFFL_FormField* pFormField = GetFormField(pWidget.Get());
  bool bRet = pFormField &&
              pFormField->OnLButtonUp(pPageView, pWidget.Get(), nFlags, point);
  if (m_pCallbackIface->GetFocusAnnot() != pWidget.Get())
    return bRet;
  if (OnButtonUp(pWidget, pPageView, nFlags) || !pWidget)
    return true;
#ifdef PDF_ENABLE_XFA
  if (OnClick(pWidget, pPageView, nFlags) || !pWidget)
    return true;
#endif  // PDF_ENABLE_XFA
  return bRet;
}

bool CFFL_InteractiveFormFiller::OnButtonUp(
    ObservedPtr<CPDFSDK_Widget>& pWidget,
    const CPDFSDK_PageView* pPageView,
    Mask<FWL_EVENTFLAG> nFlag) {
  if (m_bNotifying)
    return false;

  if (!pWidget->GetAAction(CPDF_AAction::kButtonUp).HasDict())
    return false;

  uint32_t nAge = pWidget->GetAppearanceAge();
  uint32_t nValueAge = pWidget->GetValueAge();
  DCHECK(pPageView);
  {
    AutoRestorer<bool> restorer(&m_bNotifying);
    m_bNotifying = true;

    CFFL_FieldAction fa;
    fa.bModifier = CPWL_Wnd::IsCTRLKeyDown(nFlag);
    fa.bShift = CPWL_Wnd::IsSHIFTKeyDown(nFlag);
    pWidget->OnAAction(CPDF_AAction::kButtonUp, &fa, pPageView);
  }
  if (!pWidget || !IsValidAnnot(pPageView, pWidget.Get()))
    return true;
  if (nAge == pWidget->GetAppearanceAge())
    return false;

  CFFL_FormField* pFormField = GetFormField(pWidget.Get());
  if (pFormField)
    pFormField->ResetPWLWindowForValueAge(pPageView, pWidget.Get(), nValueAge);
  return true;
}

bool CFFL_InteractiveFormFiller::SetIndexSelected(
    ObservedPtr<CPDFSDK_Widget>& pWidget,
    int index,
    bool selected) {
  CFFL_FormField* pFormField = GetFormField(pWidget.Get());
  return pFormField && pFormField->SetIndexSelected(index, selected);
}

bool CFFL_InteractiveFormFiller::IsIndexSelected(
    ObservedPtr<CPDFSDK_Widget>& pWidget,
    int index) {
  CFFL_FormField* pFormField = GetFormField(pWidget.Get());
  return pFormField && pFormField->IsIndexSelected(index);
}

bool CFFL_InteractiveFormFiller::OnLButtonDblClk(
    CPDFSDK_PageView* pPageView,
    ObservedPtr<CPDFSDK_Widget>& pWidget,
    Mask<FWL_EVENTFLAG> nFlags,
    const CFX_PointF& point) {
  CFFL_FormField* pFormField = GetFormField(pWidget.Get());
  return pFormField && pFormField->OnLButtonDblClk(pPageView, nFlags, point);
}

bool CFFL_InteractiveFormFiller::OnMouseMove(
    CPDFSDK_PageView* pPageView,
    ObservedPtr<CPDFSDK_Widget>& pWidget,
    Mask<FWL_EVENTFLAG> nFlags,
    const CFX_PointF& point) {
  CFFL_FormField* pFormField = GetOrCreateFormField(pWidget.Get());
  return pFormField && pFormField->OnMouseMove(pPageView, nFlags, point);
}

bool CFFL_InteractiveFormFiller::OnMouseWheel(
    CPDFSDK_PageView* pPageView,
    ObservedPtr<CPDFSDK_Widget>& pWidget,
    Mask<FWL_EVENTFLAG> nFlags,
    const CFX_PointF& point,
    const CFX_Vector& delta) {
  CFFL_FormField* pFormField = GetFormField(pWidget.Get());
  return pFormField &&
         pFormField->OnMouseWheel(pPageView, nFlags, point, delta);
}

bool CFFL_InteractiveFormFiller::OnRButtonDown(
    CPDFSDK_PageView* pPageView,
    ObservedPtr<CPDFSDK_Widget>& pWidget,
    Mask<FWL_EVENTFLAG> nFlags,
    const CFX_PointF& point) {
  CFFL_FormField* pFormField = GetFormField(pWidget.Get());
  return pFormField && pFormField->OnRButtonDown(pPageView, nFlags, point);
}

bool CFFL_InteractiveFormFiller::OnRButtonUp(
    CPDFSDK_PageView* pPageView,
    ObservedPtr<CPDFSDK_Widget>& pWidget,
    Mask<FWL_EVENTFLAG> nFlags,
    const CFX_PointF& point) {
  CFFL_FormField* pFormField = GetFormField(pWidget.Get());
  return pFormField && pFormField->OnRButtonUp(pPageView, nFlags, point);
}

bool CFFL_InteractiveFormFiller::OnKeyDown(CPDFSDK_Widget* pWidget,
                                           FWL_VKEYCODE nKeyCode,
                                           Mask<FWL_EVENTFLAG> nFlags) {
  CFFL_FormField* pFormField = GetFormField(pWidget);
  return pFormField && pFormField->OnKeyDown(nKeyCode, nFlags);
}

bool CFFL_InteractiveFormFiller::OnChar(CPDFSDK_Widget* pWidget,
                                        uint32_t nChar,
                                        Mask<FWL_EVENTFLAG> nFlags) {
  if (nChar == pdfium::ascii::kTab)
    return true;

  CFFL_FormField* pFormField = GetFormField(pWidget);
  return pFormField && pFormField->OnChar(pWidget, nChar, nFlags);
}

bool CFFL_InteractiveFormFiller::OnSetFocus(
    ObservedPtr<CPDFSDK_Widget>& pWidget,
    Mask<FWL_EVENTFLAG> nFlag) {
  if (!pWidget)
    return false;

  if (!m_bNotifying) {
    if (pWidget->GetAAction(CPDF_AAction::kGetFocus).HasDict()) {
      uint32_t nValueAge = pWidget->GetValueAge();
      pWidget->ClearAppModified();

      CFFL_FormField* pFormField = GetOrCreateFormField(pWidget.Get());
      if (!pFormField)
        return false;

      CPDFSDK_PageView* pPageView = pWidget->GetPageView();
      DCHECK(pPageView);
      {
        AutoRestorer<bool> restorer(&m_bNotifying);
        m_bNotifying = true;

        CFFL_FieldAction fa;
        fa.bModifier = CPWL_Wnd::IsCTRLKeyDown(nFlag);
        fa.bShift = CPWL_Wnd::IsSHIFTKeyDown(nFlag);
        pFormField->GetActionData(pPageView, CPDF_AAction::kGetFocus, fa);
        pWidget->OnAAction(CPDF_AAction::kGetFocus, &fa, pPageView);
      }
      if (!pWidget)
        return false;

      if (pWidget->IsAppModified()) {
        CFFL_FormField* pFiller = GetFormField(pWidget.Get());
        if (pFiller) {
          pFiller->ResetPWLWindowForValueAge(pPageView, pWidget.Get(),
                                             nValueAge);
        }
      }
    }
  }

  if (CFFL_FormField* pFormField = GetOrCreateFormField(pWidget.Get()))
    pFormField->SetFocusForAnnot(pWidget.Get(), nFlag);

  return true;
}

bool CFFL_InteractiveFormFiller::OnKillFocus(
    ObservedPtr<CPDFSDK_Widget>& pWidget,
    Mask<FWL_EVENTFLAG> nFlag) {
  if (!pWidget)
    return false;

  CFFL_FormField* pFormField = GetFormField(pWidget.Get());
  if (!pFormField)
    return true;

  pFormField->KillFocusForAnnot(nFlag);
  if (!pWidget)
    return false;

  if (m_bNotifying)
    return true;

  if (!pWidget->GetAAction(CPDF_AAction::kLoseFocus).HasDict())
    return true;

  pWidget->ClearAppModified();

  CPDFSDK_PageView* pPageView = pWidget->GetPageView();
  DCHECK(pPageView);
  {
    AutoRestorer<bool> restorer(&m_bNotifying);
    m_bNotifying = true;

    CFFL_FieldAction fa;
    fa.bModifier = CPWL_Wnd::IsCTRLKeyDown(nFlag);
    fa.bShift = CPWL_Wnd::IsSHIFTKeyDown(nFlag);
    pFormField->GetActionData(pPageView, CPDF_AAction::kLoseFocus, fa);
    pWidget->OnAAction(CPDF_AAction::kLoseFocus, &fa, pPageView);
  }
  return !!pWidget;
}

void CFFL_InteractiveFormFiller::OnSetFieldInputFocus(const WideString& text) {
  m_pCallbackIface->OnSetFieldInputFocus(text);
}

void CFFL_InteractiveFormFiller::Invalidate(IPDF_Page* pPage,
                                            const FX_RECT& rect) {
  m_pCallbackIface->Invalidate(pPage, rect);
}

CPDFSDK_PageView* CFFL_InteractiveFormFiller::GetOrCreatePageView(
    IPDF_Page* pPage) {
  return m_pCallbackIface->GetOrCreatePageView(pPage);
}

CPDFSDK_PageView* CFFL_InteractiveFormFiller::GetPageView(IPDF_Page* pPage) {
  return m_pCallbackIface->GetPageView(pPage);
}

CFX_Timer::HandlerIface* CFFL_InteractiveFormFiller::GetTimerHandler() {
  return m_pCallbackIface->GetTimerHandler();
}

void CFFL_InteractiveFormFiller::OnChange() {
  m_pCallbackIface->OnChange();
}

bool CFFL_InteractiveFormFiller::IsVisible(CPDFSDK_Widget* pWidget) {
  return pWidget->IsVisible();
}

bool CFFL_InteractiveFormFiller::IsReadOnly(CPDFSDK_Widget* pWidget) {
  int nFieldFlags = pWidget->GetFieldFlags();
  return !!(nFieldFlags & pdfium::form_flags::kReadOnly);
}

bool CFFL_InteractiveFormFiller::IsFillingAllowed(
    CPDFSDK_Widget* pWidget) const {
  if (pWidget->GetFieldType() == FormFieldType::kPushButton)
    return false;

  return m_pCallbackIface->HasPermissions(
      pdfium::access_permissions::kFillForm |
      pdfium::access_permissions::kModifyAnnotation |
      pdfium::access_permissions::kModifyContent);
}

CFFL_FormField* CFFL_InteractiveFormFiller::GetFormField(
    CPDFSDK_Widget* pWidget) {
  auto it = m_Map.find(pWidget);
  return it != m_Map.end() ? it->second.get() : nullptr;
}

CFFL_FormField* CFFL_InteractiveFormFiller::GetOrCreateFormField(
    CPDFSDK_Widget* pWidget) {
  CFFL_FormField* result = GetFormField(pWidget);
  if (result)
    return result;

  std::unique_ptr<CFFL_FormField> pFormField;
  switch (pWidget->GetFieldType()) {
    case FormFieldType::kPushButton:
      pFormField = std::make_unique<CFFL_PushButton>(this, pWidget);
      break;
    case FormFieldType::kCheckBox:
      pFormField = std::make_unique<CFFL_CheckBox>(this, pWidget);
      break;
    case FormFieldType::kRadioButton:
      pFormField = std::make_unique<CFFL_RadioButton>(this, pWidget);
      break;
    case FormFieldType::kTextField:
      pFormField = std::make_unique<CFFL_TextField>(this, pWidget);
      break;
    case FormFieldType::kListBox:
      pFormField = std::make_unique<CFFL_ListBox>(this, pWidget);
      break;
    case FormFieldType::kComboBox:
      pFormField = std::make_unique<CFFL_ComboBox>(this, pWidget);
      break;
    case FormFieldType::kUnknown:
    default:
      return nullptr;
  }

  result = pFormField.get();
  m_Map[pWidget] = std::move(pFormField);
  return result;
}

WideString CFFL_InteractiveFormFiller::GetText(CPDFSDK_Widget* pWidget) {
  CFFL_FormField* pFormField = GetFormField(pWidget);
  return pFormField ? pFormField->GetText() : WideString();
}

WideString CFFL_InteractiveFormFiller::GetSelectedText(
    CPDFSDK_Widget* pWidget) {
  CFFL_FormField* pFormField = GetFormField(pWidget);
  return pFormField ? pFormField->GetSelectedText() : WideString();
}

void CFFL_InteractiveFormFiller::ReplaceAndKeepSelection(
    CPDFSDK_Widget* pWidget,
    const WideString& text) {
  CFFL_FormField* pFormField = GetFormField(pWidget);
  if (!pFormField)
    return;

  pFormField->ReplaceAndKeepSelection(text);
}

void CFFL_InteractiveFormFiller::ReplaceSelection(CPDFSDK_Widget* pWidget,
                                                  const WideString& text) {
  CFFL_FormField* pFormField = GetFormField(pWidget);
  if (!pFormField)
    return;

  pFormField->ReplaceSelection(text);
}

bool CFFL_InteractiveFormFiller::SelectAllText(CPDFSDK_Widget* pWidget) {
  CFFL_FormField* pFormField = GetFormField(pWidget);
  return pFormField && pFormField->SelectAllText();
}

bool CFFL_InteractiveFormFiller::CanUndo(CPDFSDK_Widget* pWidget) {
  CFFL_FormField* pFormField = GetFormField(pWidget);
  return pFormField && pFormField->CanUndo();
}

bool CFFL_InteractiveFormFiller::CanRedo(CPDFSDK_Widget* pWidget) {
  CFFL_FormField* pFormField = GetFormField(pWidget);
  return pFormField && pFormField->CanRedo();
}

bool CFFL_InteractiveFormFiller::Undo(CPDFSDK_Widget* pWidget) {
  CFFL_FormField* pFormField = GetFormField(pWidget);
  return pFormField && pFormField->Undo();
}

bool CFFL_InteractiveFormFiller::Redo(CPDFSDK_Widget* pWidget) {
  CFFL_FormField* pFormField = GetFormField(pWidget);
  return pFormField && pFormField->Redo();
}

void CFFL_InteractiveFormFiller::UnregisterFormField(CPDFSDK_Widget* pWidget) {
  auto it = m_Map.find(pWidget);
  if (it == m_Map.end())
    return;

  m_Map.erase(it);
}

void CFFL_InteractiveFormFiller::InvalidateRect(PerWindowData* pWidgetData,
                                                const CFX_FloatRect& rect) {
  auto* pPrivateData = static_cast<CFFL_PerWindowData*>(pWidgetData);
  CPDFSDK_Widget* pWidget = pPrivateData->GetWidget();
  if (!pWidget)
    return;

  m_pCallbackIface->InvalidateRect(pWidget, rect);
}

void CFFL_InteractiveFormFiller::OutputSelectedRect(PerWindowData* pWidgetData,
                                                    const CFX_FloatRect& rect) {
  auto* pPrivateData = static_cast<CFFL_PerWindowData*>(pWidgetData);
  if (!pPrivateData)
    return;

  CFFL_FormField* pFormField = pPrivateData->GetFormField();
  if (!pFormField)
    return;

  m_pCallbackIface->OutputSelectedRect(pFormField, rect);
}

bool CFFL_InteractiveFormFiller::IsSelectionImplemented() const {
  return m_pCallbackIface->IsSelectionImplemented();
}

void CFFL_InteractiveFormFiller::SetCursor(CursorStyle nCursorStyle) {
  m_pCallbackIface->SetCursor(nCursorStyle);
}

void CFFL_InteractiveFormFiller::QueryWherePopup(
    const IPWL_FillerNotify::PerWindowData* pAttached,
    float fPopupMin,
    float fPopupMax,
    bool* bBottom,
    float* fPopupRet) {
  auto* pData = static_cast<const CFFL_PerWindowData*>(pAttached);
  CPDFSDK_Widget* pWidget = pData->GetWidget();
  CPDF_Page* pPage = pWidget->GetPDFPage();

  CFX_FloatRect rcPageView(0, pPage->GetPageHeight(), pPage->GetPageWidth(), 0);
  rcPageView.Normalize();

  CFX_FloatRect rcAnnot = pWidget->GetRect();
  float fTop = 0.0f;
  float fBottom = 0.0f;
  switch (pWidget->GetRotate() / 90) {
    default:
    case 0:
      fTop = rcPageView.top - rcAnnot.top;
      fBottom = rcAnnot.bottom - rcPageView.bottom;
      break;
    case 1:
      fTop = rcAnnot.left - rcPageView.left;
      fBottom = rcPageView.right - rcAnnot.right;
      break;
    case 2:
      fTop = rcAnnot.bottom - rcPageView.bottom;
      fBottom = rcPageView.top - rcAnnot.top;
      break;
    case 3:
      fTop = rcPageView.right - rcAnnot.right;
      fBottom = rcAnnot.left - rcPageView.left;
      break;
  }

  constexpr float kMaxListBoxHeight = 140;
  const float fMaxListBoxHeight =
      std::clamp(kMaxListBoxHeight, fPopupMin, fPopupMax);

  if (fBottom > fMaxListBoxHeight) {
    *fPopupRet = fMaxListBoxHeight;
    *bBottom = true;
    return;
  }

  if (fTop > fMaxListBoxHeight) {
    *fPopupRet = fMaxListBoxHeight;
    *bBottom = false;
    return;
  }

  if (fTop > fBottom) {
    *fPopupRet = fTop;
    *bBottom = false;
  } else {
    *fPopupRet = fBottom;
    *bBottom = true;
  }
}

bool CFFL_InteractiveFormFiller::OnKeyStrokeCommit(
    ObservedPtr<CPDFSDK_Widget>& pWidget,
    const CPDFSDK_PageView* pPageView,
    Mask<FWL_EVENTFLAG> nFlag) {
  if (m_bNotifying)
    return true;

  if (!pWidget->GetAAction(CPDF_AAction::kKeyStroke).HasDict())
    return true;

  DCHECK(pPageView);
  pWidget->ClearAppModified();

  AutoRestorer<bool> restorer(&m_bNotifying);
  m_bNotifying = true;

  CFFL_FieldAction fa;
  fa.bModifier = CPWL_Wnd::IsCTRLKeyDown(nFlag);
  fa.bShift = CPWL_Wnd::IsSHIFTKeyDown(nFlag);
  fa.bWillCommit = true;
  fa.bKeyDown = true;
  fa.bRC = true;

  CFFL_FormField* pFormField = GetFormField(pWidget.Get());
  pFormField->GetActionData(pPageView, CPDF_AAction::kKeyStroke, fa);
  pFormField->SavePWLWindowState(pPageView);
  pWidget->OnAAction(CPDF_AAction::kKeyStroke, &fa, pPageView);

  if (!pWidget)
    return true;

  return fa.bRC;
}

bool CFFL_InteractiveFormFiller::OnValidate(
    ObservedPtr<CPDFSDK_Widget>& pWidget,
    const CPDFSDK_PageView* pPageView,
    Mask<FWL_EVENTFLAG> nFlag) {
  if (m_bNotifying)
    return true;

  if (!pWidget->GetAAction(CPDF_AAction::kValidate).HasDict())
    return true;

  DCHECK(pPageView);
  pWidget->ClearAppModified();

  AutoRestorer<bool> restorer(&m_bNotifying);
  m_bNotifying = true;

  CFFL_FieldAction fa;
  fa.bModifier = CPWL_Wnd::IsCTRLKeyDown(nFlag);
  fa.bShift = CPWL_Wnd::IsSHIFTKeyDown(nFlag);
  fa.bKeyDown = true;
  fa.bRC = true;

  CFFL_FormField* pFormField = GetFormField(pWidget.Get());
  pFormField->GetActionData(pPageView, CPDF_AAction::kValidate, fa);
  pFormField->SavePWLWindowState(pPageView);
  pWidget->OnAAction(CPDF_AAction::kValidate, &fa, pPageView);

  if (!pWidget)
    return true;

  return fa.bRC;
}

void CFFL_InteractiveFormFiller::OnCalculate(
    ObservedPtr<CPDFSDK_Widget>& pWidget) {
  if (m_bNotifying)
    return;

  ObservedPtr<CPDFSDK_Annot> pObserved(pWidget.Get());
  m_pCallbackIface->OnCalculate(pObserved);
}

void CFFL_InteractiveFormFiller::OnFormat(
    ObservedPtr<CPDFSDK_Widget>& pWidget) {
  if (m_bNotifying)
    return;

  ObservedPtr<CPDFSDK_Annot> pObserved(pWidget.Get());
  m_pCallbackIface->OnFormat(pObserved);
}

#ifdef PDF_ENABLE_XFA
bool CFFL_InteractiveFormFiller::OnClick(ObservedPtr<CPDFSDK_Widget>& pWidget,
                                         const CPDFSDK_PageView* pPageView,
                                         Mask<FWL_EVENTFLAG> nFlag) {
  if (m_bNotifying)
    return false;

  if (!pWidget->HasXFAAAction(PDFSDK_XFA_Click))
    return false;

  uint32_t nAge = pWidget->GetAppearanceAge();
  uint32_t nValueAge = pWidget->GetValueAge();
  {
    AutoRestorer<bool> restorer(&m_bNotifying);
    m_bNotifying = true;

    CFFL_FieldAction fa;
    fa.bModifier = CPWL_Wnd::IsCTRLKeyDown(nFlag);
    fa.bShift = CPWL_Wnd::IsSHIFTKeyDown(nFlag);

    pWidget->OnXFAAAction(PDFSDK_XFA_Click, &fa, pPageView);
  }
  if (!pWidget || !IsValidAnnot(pPageView, pWidget.Get()))
    return true;
  if (nAge == pWidget->GetAppearanceAge())
    return false;

  CFFL_FormField* pFormField = GetFormField(pWidget.Get());
  if (pFormField)
    pFormField->ResetPWLWindowForValueAge(pPageView, pWidget.Get(), nValueAge);
  return false;
}

bool CFFL_InteractiveFormFiller::OnFull(ObservedPtr<CPDFSDK_Widget>& pWidget,
                                        const CPDFSDK_PageView* pPageView,
                                        Mask<FWL_EVENTFLAG> nFlag) {
  if (m_bNotifying)
    return false;

  if (!pWidget->HasXFAAAction(PDFSDK_XFA_Full))
    return false;

  uint32_t nAge = pWidget->GetAppearanceAge();
  uint32_t nValueAge = pWidget->GetValueAge();
  {
    AutoRestorer<bool> restorer(&m_bNotifying);
    m_bNotifying = true;

    CFFL_FieldAction fa;
    fa.bModifier = CPWL_Wnd::IsCTRLKeyDown(nFlag);
    fa.bShift = CPWL_Wnd::IsSHIFTKeyDown(nFlag);
    pWidget->OnXFAAAction(PDFSDK_XFA_Full, &fa, pPageView);
  }
  if (!pWidget || !IsValidAnnot(pPageView, pWidget.Get()))
    return true;
  if (nAge == pWidget->GetAppearanceAge())
    return false;

  CFFL_FormField* pFormField = GetFormField(pWidget.Get());
  if (pFormField)
    pFormField->ResetPWLWindowForValueAge(pPageView, pWidget.Get(), nValueAge);
  return true;
}

bool CFFL_InteractiveFormFiller::OnPreOpen(ObservedPtr<CPDFSDK_Widget>& pWidget,
                                           const CPDFSDK_PageView* pPageView,
                                           Mask<FWL_EVENTFLAG> nFlag) {
  if (m_bNotifying)
    return false;

  if (!pWidget->HasXFAAAction(PDFSDK_XFA_PreOpen))
    return false;

  uint32_t nAge = pWidget->GetAppearanceAge();
  uint32_t nValueAge = pWidget->GetValueAge();
  {
    AutoRestorer<bool> restorer(&m_bNotifying);
    m_bNotifying = true;

    CFFL_FieldAction fa;
    fa.bModifier = CPWL_Wnd::IsCTRLKeyDown(nFlag);
    fa.bShift = CPWL_Wnd::IsSHIFTKeyDown(nFlag);
    pWidget->OnXFAAAction(PDFSDK_XFA_PreOpen, &fa, pPageView);
  }
  if (!pWidget || !IsValidAnnot(pPageView, pWidget.Get()))
    return true;
  if (nAge == pWidget->GetAppearanceAge())
    return false;

  CFFL_FormField* pFormField = GetFormField(pWidget.Get());
  if (pFormField)
    pFormField->ResetPWLWindowForValueAge(pPageView, pWidget.Get(), nValueAge);
  return true;
}

bool CFFL_InteractiveFormFiller::OnPostOpen(
    ObservedPtr<CPDFSDK_Widget>& pWidget,
    const CPDFSDK_PageView* pPageView,
    Mask<FWL_EVENTFLAG> nFlag) {
  if (m_bNotifying)
    return false;

  if (!pWidget->HasXFAAAction(PDFSDK_XFA_PostOpen))
    return false;

  uint32_t nAge = pWidget->GetAppearanceAge();
  uint32_t nValueAge = pWidget->GetValueAge();
  {
    AutoRestorer<bool> restorer(&m_bNotifying);
    m_bNotifying = true;

    CFFL_FieldAction fa;
    fa.bModifier = CPWL_Wnd::IsCTRLKeyDown(nFlag);
    fa.bShift = CPWL_Wnd::IsSHIFTKeyDown(nFlag);
    pWidget->OnXFAAAction(PDFSDK_XFA_PostOpen, &fa, pPageView);
  }
  if (!pWidget || !IsValidAnnot(pPageView, pWidget.Get()))
    return true;

  if (nAge == pWidget->GetAppearanceAge())
    return false;

  CFFL_FormField* pFormField = GetFormField(pWidget.Get());
  if (pFormField)
    pFormField->ResetPWLWindowForValueAge(pPageView, pWidget.Get(), nValueAge);
  return true;
}
#endif  // PDF_ENABLE_XFA

// static
bool CFFL_InteractiveFormFiller::IsValidAnnot(const CPDFSDK_PageView* pPageView,
                                              CPDFSDK_Widget* pWidget) {
  return pPageView && pPageView->IsValidAnnot(pWidget->GetPDFAnnot());
}

IPWL_FillerNotify::BeforeKeystrokeResult
CFFL_InteractiveFormFiller::OnBeforeKeyStroke(
    const IPWL_FillerNotify::PerWindowData* pAttached,
    WideString& strChange,
    const WideString& strChangeEx,
    int nSelStart,
    int nSelEnd,
    bool bKeyDown,
    Mask<FWL_EVENTFLAG> nFlag) {
  // Copy out of private data since the window owning it may not survive.
  auto* pPrivateData = static_cast<const CFFL_PerWindowData*>(pAttached);
  const CPDFSDK_PageView* pPageView = pPrivateData->GetPageView();
  ObservedPtr<CPDFSDK_Widget> pWidget(pPrivateData->GetWidget());
  DCHECK(pWidget);

  CFFL_FormField* pFormField = GetFormField(pWidget.Get());

#ifdef PDF_ENABLE_XFA
  if (pFormField->IsFieldFull(pPageView)) {
    if (OnFull(pWidget, pPageView, nFlag) || !pWidget)
      return {true, true};
  }
#endif  // PDF_ENABLE_XFA

  if (m_bNotifying ||
      !pWidget->GetAAction(CPDF_AAction::kKeyStroke).HasDict()) {
    return {true, false};
  }

  AutoRestorer<bool> restorer(&m_bNotifying);
  m_bNotifying = true;

  uint32_t nAge = pWidget->GetAppearanceAge();
  uint32_t nValueAge = pWidget->GetValueAge();

  CFFL_FieldAction fa;
  fa.bModifier = CPWL_Wnd::IsCTRLKeyDown(nFlag);
  fa.bShift = CPWL_Wnd::IsSHIFTKeyDown(nFlag);
  fa.sChange = strChange;
  fa.sChangeEx = strChangeEx;
  fa.bKeyDown = bKeyDown;
  fa.bWillCommit = false;
  fa.bRC = true;
  fa.nSelStart = nSelStart;
  fa.nSelEnd = nSelEnd;
  pFormField->GetActionData(pPageView, CPDF_AAction::kKeyStroke, fa);
  pFormField->SavePWLWindowState(pPageView);

  bool action_status =
      pWidget->OnAAction(CPDF_AAction::kKeyStroke, &fa, pPageView);

  if (!pWidget || !IsValidAnnot(pPageView, pWidget.Get())) {
    return {true, true};
  }
  if (!action_status)
    return {true, false};

  bool bExit = false;
  if (nAge != pWidget->GetAppearanceAge()) {
    pFormField->ResetPWLWindowForValueAge(pPageView, pWidget.Get(), nValueAge);
    pPrivateData = pFormField->GetPerPWLWindowData(pPageView);
    if (!pPrivateData)
      return {true, true};

    pWidget.Reset(pPrivateData->GetWidget());
    pPageView = pPrivateData->GetPageView();
    bExit = true;
  }
  if (fa.bRC) {
    pFormField->SetActionData(pPageView, CPDF_AAction::kKeyStroke, fa);
  } else {
    pFormField->RecreatePWLWindowFromSavedState(pPageView);
  }
  if (m_pCallbackIface->GetFocusAnnot() == pWidget)
    return {false, bExit};

  pFormField->CommitData(pPageView, nFlag);
  return {false, true};
}

bool CFFL_InteractiveFormFiller::OnPopupPreOpen(
    const IPWL_FillerNotify::PerWindowData* pAttached,
    Mask<FWL_EVENTFLAG> nFlag) {
#ifdef PDF_ENABLE_XFA
  auto* pData = static_cast<const CFFL_PerWindowData*>(pAttached);
  DCHECK(pData->GetWidget());

  ObservedPtr<CPDFSDK_Widget> pObserved(pData->GetWidget());
  return OnPreOpen(pObserved, pData->GetPageView(), nFlag) || !pObserved;
#else
  return false;
#endif
}

bool CFFL_InteractiveFormFiller::OnPopupPostOpen(
    const IPWL_FillerNotify::PerWindowData* pAttached,
    Mask<FWL_EVENTFLAG> nFlag) {
#ifdef PDF_ENABLE_XFA
  auto* pData = static_cast<const CFFL_PerWindowData*>(pAttached);
  DCHECK(pData->GetWidget());

  ObservedPtr<CPDFSDK_Widget> pObserved(pData->GetWidget());
  return OnPostOpen(pObserved, pData->GetPageView(), nFlag) || !pObserved;
#else
  return false;
#endif
}
