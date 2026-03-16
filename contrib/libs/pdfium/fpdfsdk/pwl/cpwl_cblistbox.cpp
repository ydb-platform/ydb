// Copyright 2020 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "fpdfsdk/pwl/cpwl_cblistbox.h"

#include <utility>

#include "core/fxcrt/notreached.h"
#include "fpdfsdk/pwl/cpwl_combo_box.h"
#include "fpdfsdk/pwl/cpwl_list_ctrl.h"
#include "public/fpdf_fwlevent.h"

CPWL_CBListBox::CPWL_CBListBox(
    const CreateParams& cp,
    std::unique_ptr<IPWL_FillerNotify::PerWindowData> pAttachedData)
    : CPWL_ListBox(cp, std::move(pAttachedData)) {}

CPWL_CBListBox::~CPWL_CBListBox() = default;

bool CPWL_CBListBox::OnLButtonUp(Mask<FWL_EVENTFLAG> nFlag,
                                 const CFX_PointF& point) {
  CPWL_Wnd::OnLButtonUp(nFlag, point);

  if (!m_bMouseDown)
    return true;

  ReleaseCapture();
  m_bMouseDown = false;

  if (!ClientHitTest(point))
    return true;
  if (CPWL_Wnd* pParent = GetParentWindow())
    pParent->NotifyLButtonUp(this, point);

  return !OnNotifySelectionChanged(false, nFlag);
}

bool CPWL_CBListBox::IsMovementKey(FWL_VKEYCODE nKeyCode) const {
  switch (nKeyCode) {
    case FWL_VKEY_Up:
    case FWL_VKEY_Down:
    case FWL_VKEY_Home:
    case FWL_VKEY_Left:
    case FWL_VKEY_End:
    case FWL_VKEY_Right:
      return true;
    default:
      return false;
  }
}

bool CPWL_CBListBox::OnMovementKeyDown(FWL_VKEYCODE nKeyCode,
                                       Mask<FWL_EVENTFLAG> nFlag) {
  switch (nKeyCode) {
    case FWL_VKEY_Up:
      m_pListCtrl->OnVK_UP(IsSHIFTKeyDown(nFlag), IsCTRLKeyDown(nFlag));
      break;
    case FWL_VKEY_Down:
      m_pListCtrl->OnVK_DOWN(IsSHIFTKeyDown(nFlag), IsCTRLKeyDown(nFlag));
      break;
    case FWL_VKEY_Home:
      m_pListCtrl->OnVK_HOME(IsSHIFTKeyDown(nFlag), IsCTRLKeyDown(nFlag));
      break;
    case FWL_VKEY_Left:
      m_pListCtrl->OnVK_LEFT(IsSHIFTKeyDown(nFlag), IsCTRLKeyDown(nFlag));
      break;
    case FWL_VKEY_End:
      m_pListCtrl->OnVK_END(IsSHIFTKeyDown(nFlag), IsCTRLKeyDown(nFlag));
      break;
    case FWL_VKEY_Right:
      m_pListCtrl->OnVK_RIGHT(IsSHIFTKeyDown(nFlag), IsCTRLKeyDown(nFlag));
      break;
    default:
      NOTREACHED_NORETURN();
  }
  return OnNotifySelectionChanged(true, nFlag);
}

bool CPWL_CBListBox::IsChar(uint16_t nChar, Mask<FWL_EVENTFLAG> nFlag) const {
  return m_pListCtrl->OnChar(nChar, IsSHIFTKeyDown(nFlag),
                             IsCTRLKeyDown(nFlag));
}

bool CPWL_CBListBox::OnCharNotify(uint16_t nChar, Mask<FWL_EVENTFLAG> nFlag) {
  if (auto* pComboBox = static_cast<CPWL_ComboBox*>(GetParentWindow()))
    pComboBox->SetSelectText();

  return OnNotifySelectionChanged(true, nFlag);
}
