// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "fpdfsdk/cpdfsdk_annot.h"

#include "core/fxcrt/check.h"
#include "fpdfsdk/cpdfsdk_pageview.h"

CPDFSDK_Annot::CPDFSDK_Annot(CPDFSDK_PageView* pPageView)
    : m_pPageView(pPageView) {
  DCHECK(m_pPageView);
}

CPDFSDK_Annot::~CPDFSDK_Annot() = default;

CPDFSDK_BAAnnot* CPDFSDK_Annot::AsBAAnnot() {
  return nullptr;
}

CPDFXFA_Widget* CPDFSDK_Annot::AsXFAWidget() {
  return nullptr;
}

// static
void CPDFSDK_Annot::OnMouseEnter(ObservedPtr<CPDFSDK_Annot>& pAnnot,
                                 Mask<FWL_EVENTFLAG> nFlags) {
  pAnnot->GetUnsafeInputHandlers()->OnMouseEnter(nFlags);
}

// static
void CPDFSDK_Annot::OnMouseExit(ObservedPtr<CPDFSDK_Annot>& pAnnot,
                                Mask<FWL_EVENTFLAG> nFlags) {
  pAnnot->GetUnsafeInputHandlers()->OnMouseExit(nFlags);
}

// static
bool CPDFSDK_Annot::OnLButtonDown(ObservedPtr<CPDFSDK_Annot>& pAnnot,
                                  Mask<FWL_EVENTFLAG> nFlags,
                                  const CFX_PointF& point) {
  return pAnnot->GetUnsafeInputHandlers()->OnLButtonDown(nFlags, point);
}

// static
bool CPDFSDK_Annot::OnLButtonUp(ObservedPtr<CPDFSDK_Annot>& pAnnot,
                                Mask<FWL_EVENTFLAG> nFlags,
                                const CFX_PointF& point) {
  return pAnnot->GetUnsafeInputHandlers()->OnLButtonUp(nFlags, point);
}

// static
bool CPDFSDK_Annot::OnLButtonDblClk(ObservedPtr<CPDFSDK_Annot>& pAnnot,
                                    Mask<FWL_EVENTFLAG> nFlags,
                                    const CFX_PointF& point) {
  return pAnnot->GetUnsafeInputHandlers()->OnLButtonDblClk(nFlags, point);
}

// static
bool CPDFSDK_Annot::OnMouseMove(ObservedPtr<CPDFSDK_Annot>& pAnnot,
                                Mask<FWL_EVENTFLAG> nFlags,
                                const CFX_PointF& point) {
  return pAnnot->GetUnsafeInputHandlers()->OnMouseMove(nFlags, point);
}

// static
bool CPDFSDK_Annot::OnMouseWheel(ObservedPtr<CPDFSDK_Annot>& pAnnot,
                                 Mask<FWL_EVENTFLAG> nFlags,
                                 const CFX_PointF& point,
                                 const CFX_Vector& delta) {
  return pAnnot->GetUnsafeInputHandlers()->OnMouseWheel(nFlags, point, delta);
}

// static
bool CPDFSDK_Annot::OnRButtonDown(ObservedPtr<CPDFSDK_Annot>& pAnnot,
                                  Mask<FWL_EVENTFLAG> nFlags,
                                  const CFX_PointF& point) {
  return pAnnot->GetUnsafeInputHandlers()->OnRButtonDown(nFlags, point);
}

// static
bool CPDFSDK_Annot::OnRButtonUp(ObservedPtr<CPDFSDK_Annot>& pAnnot,
                                Mask<FWL_EVENTFLAG> nFlags,
                                const CFX_PointF& point) {
  return pAnnot->GetUnsafeInputHandlers()->OnRButtonUp(nFlags, point);
}

// static
bool CPDFSDK_Annot::OnChar(ObservedPtr<CPDFSDK_Annot>& pAnnot,
                           uint32_t nChar,
                           Mask<FWL_EVENTFLAG> nFlags) {
  return pAnnot->GetUnsafeInputHandlers()->OnChar(nChar, nFlags);
}

// static
bool CPDFSDK_Annot::OnKeyDown(ObservedPtr<CPDFSDK_Annot>& pAnnot,
                              FWL_VKEYCODE nKeyCode,
                              Mask<FWL_EVENTFLAG> nFlags) {
  return pAnnot->GetUnsafeInputHandlers()->OnKeyDown(nKeyCode, nFlags);
}

// static
bool CPDFSDK_Annot::OnSetFocus(ObservedPtr<CPDFSDK_Annot>& pAnnot,
                               Mask<FWL_EVENTFLAG> nFlags) {
  return pAnnot->GetUnsafeInputHandlers()->OnSetFocus(nFlags);
}

// static
bool CPDFSDK_Annot::OnKillFocus(ObservedPtr<CPDFSDK_Annot>& pAnnot,
                                Mask<FWL_EVENTFLAG> nFlags) {
  return pAnnot->GetUnsafeInputHandlers()->OnKillFocus(nFlags);
}

IPDF_Page* CPDFSDK_Annot::GetXFAPage() {
#ifdef PDF_ENABLE_XFA
  return m_pPageView->GetXFAPage();
#else
  return nullptr;
#endif
}

int CPDFSDK_Annot::GetLayoutOrder() const {
  return 5;
}

CPDF_Annot* CPDFSDK_Annot::GetPDFAnnot() const {
  return nullptr;
}

IPDF_Page* CPDFSDK_Annot::GetPage() {
#ifdef PDF_ENABLE_XFA
  IPDF_Page* pXFAPage = GetXFAPage();
  if (pXFAPage)
    return pXFAPage;
#endif  // PDF_ENABLE_XFA
  return GetPDFPage();
}

CPDF_Page* CPDFSDK_Annot::GetPDFPage() {
  return m_pPageView->GetPDFPage();
}
