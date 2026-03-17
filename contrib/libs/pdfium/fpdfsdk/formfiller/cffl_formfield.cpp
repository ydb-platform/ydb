// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "fpdfsdk/formfiller/cffl_formfield.h"

#include <utility>

#include "constants/form_flags.h"
#include "core/fpdfapi/page/cpdf_page.h"
#include "core/fxcrt/check.h"
#include "core/fxge/cfx_renderdevice.h"
#include "fpdfsdk/cpdfsdk_pageview.h"
#include "fpdfsdk/cpdfsdk_widget.h"
#include "fpdfsdk/formfiller/cffl_perwindowdata.h"

CFFL_FormField::CFFL_FormField(CFFL_InteractiveFormFiller* pFormFiller,
                               CPDFSDK_Widget* pWidget)
    : m_pFormFiller(pFormFiller), m_pWidget(pWidget) {
  DCHECK(m_pFormFiller);
}

CFFL_FormField::~CFFL_FormField() {
  DestroyWindows();
}

void CFFL_FormField::DestroyWindows() {
  while (!m_Maps.empty()) {
    auto it = m_Maps.begin();
    std::unique_ptr<CPWL_Wnd> pWnd = std::move(it->second);
    m_Maps.erase(it);
    pWnd->InvalidateProvider(this);
    pWnd->Destroy();
  }
}

FX_RECT CFFL_FormField::GetViewBBox(const CPDFSDK_PageView* pPageView) {
  CPWL_Wnd* pWnd = GetPWLWindow(pPageView);
  CFX_FloatRect rcAnnot =
      pWnd ? PWLtoFFL(pWnd->GetWindowRect()) : m_pWidget->GetRect();
  CFX_FloatRect rcFocus = GetFocusBox(pPageView);

  CFX_FloatRect rcWin = rcAnnot;
  if (!rcFocus.IsEmpty())
    rcWin.Union(rcFocus);
  if (!rcWin.IsEmpty()) {
    rcWin.Inflate(1, 1);
    rcWin.Normalize();
  }

  return rcWin.GetOuterRect();
}

void CFFL_FormField::OnDraw(CPDFSDK_PageView* pPageView,
                            CPDFSDK_Widget* pWidget,
                            CFX_RenderDevice* pDevice,
                            const CFX_Matrix& mtUser2Device) {
  CPWL_Wnd* pWnd = GetPWLWindow(pPageView);
  if (pWnd) {
    pWnd->DrawAppearance(pDevice, GetCurMatrix() * mtUser2Device);
    return;
  }
  if (!CFFL_InteractiveFormFiller::IsVisible(pWidget))
    return;

  pWidget->DrawAppearance(pDevice, mtUser2Device,
                          CPDF_Annot::AppearanceMode::kNormal);
}

void CFFL_FormField::OnDrawDeactive(CPDFSDK_PageView* pPageView,
                                    CPDFSDK_Widget* pWidget,
                                    CFX_RenderDevice* pDevice,
                                    const CFX_Matrix& mtUser2Device) {
  pWidget->DrawAppearance(pDevice, mtUser2Device,
                          CPDF_Annot::AppearanceMode::kNormal);
}

void CFFL_FormField::OnMouseEnter(CPDFSDK_PageView* pPageView) {}

void CFFL_FormField::OnMouseExit(CPDFSDK_PageView* pPageView) {
  m_pTimer.reset();
  DCHECK(m_pWidget);
}

bool CFFL_FormField::OnLButtonDown(CPDFSDK_PageView* pPageView,
                                   CPDFSDK_Widget* pWidget,
                                   Mask<FWL_EVENTFLAG> nFlags,
                                   const CFX_PointF& point) {
  CPWL_Wnd* pWnd = CreateOrUpdatePWLWindow(pPageView);
  if (!pWnd)
    return false;

  m_bValid = true;
  FX_RECT rect = GetViewBBox(pPageView);
  InvalidateRect(rect);
  if (!rect.Contains(static_cast<int>(point.x), static_cast<int>(point.y)))
    return false;
  return pWnd->OnLButtonDown(nFlags, FFLtoPWL(point));
}

bool CFFL_FormField::OnLButtonUp(CPDFSDK_PageView* pPageView,
                                 CPDFSDK_Widget* pWidget,
                                 Mask<FWL_EVENTFLAG> nFlags,
                                 const CFX_PointF& point) {
  CPWL_Wnd* pWnd = GetPWLWindow(pPageView);
  if (!pWnd)
    return false;

  InvalidateRect(GetViewBBox(pPageView));
  pWnd->OnLButtonUp(nFlags, FFLtoPWL(point));
  return true;
}

bool CFFL_FormField::OnLButtonDblClk(CPDFSDK_PageView* pPageView,
                                     Mask<FWL_EVENTFLAG> nFlags,
                                     const CFX_PointF& point) {
  CPWL_Wnd* pWnd = GetPWLWindow(pPageView);
  if (!pWnd)
    return false;

  pWnd->OnLButtonDblClk(nFlags, FFLtoPWL(point));
  return true;
}

bool CFFL_FormField::OnMouseMove(CPDFSDK_PageView* pPageView,
                                 Mask<FWL_EVENTFLAG> nFlags,
                                 const CFX_PointF& point) {
  CPWL_Wnd* pWnd = GetPWLWindow(pPageView);
  if (!pWnd)
    return false;

  pWnd->OnMouseMove(nFlags, FFLtoPWL(point));
  return true;
}

bool CFFL_FormField::OnMouseWheel(CPDFSDK_PageView* pPageView,
                                  Mask<FWL_EVENTFLAG> nFlags,
                                  const CFX_PointF& point,
                                  const CFX_Vector& delta) {
  if (!IsValid())
    return false;

  CPWL_Wnd* pWnd = CreateOrUpdatePWLWindow(pPageView);
  return pWnd && pWnd->OnMouseWheel(nFlags, FFLtoPWL(point), delta);
}

bool CFFL_FormField::OnRButtonDown(CPDFSDK_PageView* pPageView,
                                   Mask<FWL_EVENTFLAG> nFlags,
                                   const CFX_PointF& point) {
  CPWL_Wnd* pWnd = CreateOrUpdatePWLWindow(pPageView);
  return pWnd && pWnd->OnRButtonDown(nFlags, FFLtoPWL(point));
}

bool CFFL_FormField::OnRButtonUp(CPDFSDK_PageView* pPageView,
                                 Mask<FWL_EVENTFLAG> nFlags,
                                 const CFX_PointF& point) {
  CPWL_Wnd* pWnd = GetPWLWindow(pPageView);
  return pWnd && pWnd->OnRButtonUp(nFlags, FFLtoPWL(point));
}

bool CFFL_FormField::OnKeyDown(FWL_VKEYCODE nKeyCode,
                               Mask<FWL_EVENTFLAG> nFlags) {
  if (!IsValid())
    return false;

  CPWL_Wnd* pWnd = GetPWLWindow(GetCurPageView());
  return pWnd && pWnd->OnKeyDown(nKeyCode, nFlags);
}

bool CFFL_FormField::OnChar(CPDFSDK_Widget* pWidget,
                            uint32_t nChar,
                            Mask<FWL_EVENTFLAG> nFlags) {
  if (!IsValid())
    return false;

  CPWL_Wnd* pWnd = GetPWLWindow(GetCurPageView());
  return pWnd && pWnd->OnChar(nChar, nFlags);
}

bool CFFL_FormField::SetIndexSelected(int index, bool selected) {
  return false;
}

bool CFFL_FormField::IsIndexSelected(int index) {
  return false;
}

WideString CFFL_FormField::GetText() {
  if (!IsValid())
    return WideString();

  CPWL_Wnd* pWnd = GetPWLWindow(GetCurPageView());
  return pWnd ? pWnd->GetText() : WideString();
}

WideString CFFL_FormField::GetSelectedText() {
  if (!IsValid())
    return WideString();

  CPWL_Wnd* pWnd = GetPWLWindow(GetCurPageView());
  return pWnd ? pWnd->GetSelectedText() : WideString();
}

void CFFL_FormField::ReplaceAndKeepSelection(const WideString& text) {
  if (!IsValid())
    return;

  CPWL_Wnd* pWnd = GetPWLWindow(GetCurPageView());
  if (!pWnd)
    return;

  pWnd->ReplaceAndKeepSelection(text);
}

void CFFL_FormField::ReplaceSelection(const WideString& text) {
  if (!IsValid())
    return;

  CPWL_Wnd* pWnd = GetPWLWindow(GetCurPageView());
  if (!pWnd)
    return;

  pWnd->ReplaceSelection(text);
}

bool CFFL_FormField::SelectAllText() {
  if (!IsValid())
    return false;

  CPWL_Wnd* pWnd = GetPWLWindow(GetCurPageView());
  return pWnd && pWnd->SelectAllText();
}

bool CFFL_FormField::CanUndo() {
  if (!IsValid())
    return false;

  CPWL_Wnd* pWnd = GetPWLWindow(GetCurPageView());
  return pWnd && pWnd->CanUndo();
}

bool CFFL_FormField::CanRedo() {
  if (!IsValid())
    return false;

  CPWL_Wnd* pWnd = GetPWLWindow(GetCurPageView());
  return pWnd && pWnd->CanRedo();
}

bool CFFL_FormField::Undo() {
  if (!IsValid())
    return false;

  CPWL_Wnd* pWnd = GetPWLWindow(GetCurPageView());
  return pWnd && pWnd->Undo();
}

bool CFFL_FormField::Redo() {
  if (!IsValid())
    return false;

  CPWL_Wnd* pWnd = GetPWLWindow(GetCurPageView());
  return pWnd && pWnd->Redo();
}

void CFFL_FormField::SetFocusForAnnot(CPDFSDK_Widget* pWidget,
                                      Mask<FWL_EVENTFLAG> nFlag) {
  CPDFSDK_PageView* pPageView =
      m_pFormFiller->GetOrCreatePageView(pWidget->GetPage());
  CPWL_Wnd* pWnd = CreateOrUpdatePWLWindow(pPageView);
  if (pWnd)
    pWnd->SetFocus();

  m_bValid = true;
  InvalidateRect(GetViewBBox(pPageView));
}

void CFFL_FormField::KillFocusForAnnot(Mask<FWL_EVENTFLAG> nFlag) {
  if (!IsValid())
    return;

  CPDFSDK_PageView* pPageView =
      m_pFormFiller->GetPageView(m_pWidget->GetPage());
  if (!pPageView || !CommitData(pPageView, nFlag))
    return;
  if (CPWL_Wnd* pWnd = GetPWLWindow(pPageView))
    pWnd->KillFocus();

  bool bDestroyPWLWindow;
  switch (m_pWidget->GetFieldType()) {
    case FormFieldType::kPushButton:
    case FormFieldType::kCheckBox:
    case FormFieldType::kRadioButton:
      bDestroyPWLWindow = true;
      break;
    default:
      bDestroyPWLWindow = false;
      break;
  }
  EscapeFiller(pPageView, bDestroyPWLWindow);
}

bool CFFL_FormField::IsValid() const {
  return m_bValid;
}

CPWL_Wnd::CreateParams CFFL_FormField::GetCreateParam() {
  CPWL_Wnd::CreateParams cp(m_pFormFiller->GetTimerHandler(), m_pFormFiller,
                            this);

  cp.rcRectWnd = GetPDFAnnotRect();

  uint32_t dwCreateFlags = PWS_BORDER | PWS_BACKGROUND | PWS_VISIBLE;
  uint32_t dwFieldFlag = m_pWidget->GetFieldFlags();
  if (dwFieldFlag & pdfium::form_flags::kReadOnly)
    dwCreateFlags |= PWS_READONLY;

  std::optional<FX_COLORREF> color = m_pWidget->GetFillColor();
  if (color.has_value())
    cp.sBackgroundColor = CFX_Color(color.value());
  color = m_pWidget->GetBorderColor();
  if (color.has_value())
    cp.sBorderColor = CFX_Color(color.value());

  cp.sTextColor = CFX_Color(CFX_Color::Type::kGray, 0);

  color = m_pWidget->GetTextColor();
  if (color.has_value())
    cp.sTextColor = CFX_Color(color.value());

  cp.fFontSize = m_pWidget->GetFontSize();
  cp.dwBorderWidth = m_pWidget->GetBorderWidth();

  cp.nBorderStyle = m_pWidget->GetBorderStyle();
  switch (cp.nBorderStyle) {
    case BorderStyle::kDash:
      cp.sDash = CPWL_Dash(3, 3, 0);
      break;
    case BorderStyle::kBeveled:
    case BorderStyle::kInset:
      cp.dwBorderWidth *= 2;
      break;
    default:
      break;
  }

  if (cp.fFontSize <= 0)
    dwCreateFlags |= PWS_AUTOFONTSIZE;

  cp.dwFlags = dwCreateFlags;
  return cp;
}

CPWL_Wnd* CFFL_FormField::GetPWLWindow(
    const CPDFSDK_PageView* pPageView) const {
  DCHECK(pPageView);
  auto it = m_Maps.find(pPageView);
  return it != m_Maps.end() ? it->second.get() : nullptr;
}

CPWL_Wnd* CFFL_FormField::CreateOrUpdatePWLWindow(
    const CPDFSDK_PageView* pPageView) {
  DCHECK(pPageView);
  CPWL_Wnd* pWnd = GetPWLWindow(pPageView);
  if (!pWnd) {
    CPWL_Wnd::CreateParams cp = GetCreateParam();
    // TODO(tsepez): maybe pass widget's value age as 4th arg.
    auto pPrivateData = std::make_unique<CFFL_PerWindowData>(
        m_pWidget, pPageView, m_pWidget->GetAppearanceAge(), 0);
    m_Maps[pPageView] = NewPWLWindow(cp, std::move(pPrivateData));
    return m_Maps[pPageView].get();
  }
  const auto* pPrivateData =
      static_cast<const CFFL_PerWindowData*>(pWnd->GetAttachedData());
  if (pPrivateData->AppearanceAgeEquals(m_pWidget->GetAppearanceAge()))
    return pWnd;

  return ResetPWLWindowForValueAgeInternal(pPageView, m_pWidget,
                                           pPrivateData->GetValueAge());
}

void CFFL_FormField::DestroyPWLWindow(const CPDFSDK_PageView* pPageView) {
  auto it = m_Maps.find(pPageView);
  if (it == m_Maps.end())
    return;

  std::unique_ptr<CPWL_Wnd> pWnd = std::move(it->second);
  m_Maps.erase(it);
  pWnd->Destroy();
}

CFX_Matrix CFFL_FormField::GetWindowMatrix(
    const IPWL_FillerNotify::PerWindowData* pAttached) {
  const auto* pPrivateData = static_cast<const CFFL_PerWindowData*>(pAttached);
  if (!pPrivateData)
    return CFX_Matrix();

  const CPDFSDK_PageView* pPageView = pPrivateData->GetPageView();
  if (!pPageView)
    return CFX_Matrix();

  return GetCurMatrix() * pPageView->GetCurrentMatrix();
}

void CFFL_FormField::OnSetFocusForEdit(CPWL_Edit* pEdit) {
  // Only sub-classes might have a subordinate edit to focus.
}

CFX_Matrix CFFL_FormField::GetCurMatrix() {
  CFX_Matrix mt;
  CFX_FloatRect rcDA = m_pWidget->GetPDFAnnot()->GetRect();
  switch (m_pWidget->GetRotate()) {
    case 90:
      mt = CFX_Matrix(0, 1, -1, 0, rcDA.right - rcDA.left, 0);
      break;
    case 180:
      mt = CFX_Matrix(-1, 0, 0, -1, rcDA.right - rcDA.left,
                      rcDA.top - rcDA.bottom);
      break;
    case 270:
      mt = CFX_Matrix(0, -1, 1, 0, 0, rcDA.top - rcDA.bottom);
      break;
    case 0:
    default:
      break;
  }
  mt.e += rcDA.left;
  mt.f += rcDA.bottom;

  return mt;
}

CFX_FloatRect CFFL_FormField::GetPDFAnnotRect() const {
  CFX_FloatRect rectAnnot = m_pWidget->GetPDFAnnot()->GetRect();
  float fWidth = rectAnnot.Width();
  float fHeight = rectAnnot.Height();
  if ((m_pWidget->GetRotate() / 90) & 0x01)
    std::swap(fWidth, fHeight);
  return CFX_FloatRect(0, 0, fWidth, fHeight);
}

CPDFSDK_PageView* CFFL_FormField::GetCurPageView() {
  return m_pFormFiller->GetOrCreatePageView(m_pWidget->GetPage());
}

CFX_FloatRect CFFL_FormField::GetFocusBox(const CPDFSDK_PageView* pPageView) {
  CPWL_Wnd* pWnd = GetPWLWindow(pPageView);
  if (!pWnd)
    return CFX_FloatRect();

  CFX_FloatRect rcFocus = PWLtoFFL(pWnd->GetFocusRect());
  return pPageView->GetPDFPage()->GetBBox().Contains(rcFocus) ? rcFocus
                                                              : CFX_FloatRect();
}

CFX_FloatRect CFFL_FormField::FFLtoPWL(const CFX_FloatRect& rect) {
  return GetCurMatrix().GetInverse().TransformRect(rect);
}

CFX_FloatRect CFFL_FormField::PWLtoFFL(const CFX_FloatRect& rect) {
  return GetCurMatrix().TransformRect(rect);
}

CFX_PointF CFFL_FormField::FFLtoPWL(const CFX_PointF& point) {
  return GetCurMatrix().GetInverse().Transform(point);
}

CFX_PointF CFFL_FormField::PWLtoFFL(const CFX_PointF& point) {
  return GetCurMatrix().Transform(point);
}

bool CFFL_FormField::CommitData(const CPDFSDK_PageView* pPageView,
                                Mask<FWL_EVENTFLAG> nFlag) {
  if (!IsDataChanged(pPageView))
    return true;

  ObservedPtr<CPDFSDK_Widget> pObserved(m_pWidget);
  if (!m_pFormFiller->OnKeyStrokeCommit(pObserved, pPageView, nFlag)) {
    if (!pObserved)
      return false;
    ResetPWLWindow(pPageView);
    return true;
  }
  if (!pObserved)
    return false;

  if (!m_pFormFiller->OnValidate(pObserved, pPageView, nFlag)) {
    if (!pObserved)
      return false;
    ResetPWLWindow(pPageView);
    return true;
  }
  if (!pObserved)
    return false;

  SaveData(pPageView);  // may invoking JS to delete this widget.
  if (!pObserved)
    return false;

  m_pFormFiller->OnCalculate(pObserved);
  if (!pObserved)
    return false;

  m_pFormFiller->OnFormat(pObserved);
  if (!pObserved)
    return false;

  return true;
}

bool CFFL_FormField::IsDataChanged(const CPDFSDK_PageView* pPageView) {
  return false;
}

void CFFL_FormField::SaveData(const CPDFSDK_PageView* pPageView) {}

#ifdef PDF_ENABLE_XFA
bool CFFL_FormField::IsFieldFull(const CPDFSDK_PageView* pPageView) {
  return false;
}
#endif  // PDF_ENABLE_XFA

void CFFL_FormField::SetChangeMark() {
  m_pFormFiller->OnChange();
}

void CFFL_FormField::GetActionData(const CPDFSDK_PageView* pPageView,
                                   CPDF_AAction::AActionType type,
                                   CFFL_FieldAction& fa) {
  fa.sValue = m_pWidget->GetValue();
}

void CFFL_FormField::SetActionData(const CPDFSDK_PageView* pPageView,
                                   CPDF_AAction::AActionType type,
                                   const CFFL_FieldAction& fa) {}

void CFFL_FormField::SavePWLWindowState(const CPDFSDK_PageView* pPageView) {}

void CFFL_FormField::RecreatePWLWindowFromSavedState(
    const CPDFSDK_PageView* pPageView) {}

CFFL_PerWindowData* CFFL_FormField::GetPerPWLWindowData(
    const CPDFSDK_PageView* pPageView) {
  CPWL_Wnd* pWnd = GetPWLWindow(pPageView);
  if (!pWnd)
    return nullptr;

  return static_cast<CFFL_PerWindowData*>(pWnd->GetAttachedData());
}

void CFFL_FormField::ResetPWLWindowForValueAge(
    const CPDFSDK_PageView* pPageView,
    CPDFSDK_Widget* pWidget,
    uint32_t nValueAge) {
  // Don't leak PWL_Wnd result to public callers.
  ResetPWLWindowForValueAgeInternal(pPageView, pWidget, nValueAge);
}

CPWL_Wnd* CFFL_FormField::ResetPWLWindowForValueAgeInternal(
    const CPDFSDK_PageView* pPageView,
    CPDFSDK_Widget* pWidget,
    uint32_t nValueAge) {
  return nValueAge == pWidget->GetValueAge() ? RestorePWLWindow(pPageView)
                                             : ResetPWLWindow(pPageView);
}

CPWL_Wnd* CFFL_FormField::ResetPWLWindow(const CPDFSDK_PageView* pPageView) {
  return GetPWLWindow(pPageView);
}

CPWL_Wnd* CFFL_FormField::RestorePWLWindow(const CPDFSDK_PageView* pPageView) {
  return GetPWLWindow(pPageView);
}

void CFFL_FormField::OnTimerFired() {}

void CFFL_FormField::EscapeFiller(CPDFSDK_PageView* pPageView,
                                  bool bDestroyPWLWindow) {
  m_bValid = false;

  InvalidateRect(GetViewBBox(pPageView));
  if (bDestroyPWLWindow)
    DestroyPWLWindow(pPageView);
}

void CFFL_FormField::InvalidateRect(const FX_RECT& rect) {
  m_pFormFiller->Invalidate(m_pWidget->GetPage(), rect);
}
