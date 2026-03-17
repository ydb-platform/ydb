// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "fpdfsdk/pwl/cpwl_combo_box.h"

#include <algorithm>
#include <utility>

#include "constants/ascii.h"
#include "fpdfsdk/pwl/cpwl_cbbutton.h"
#include "fpdfsdk/pwl/cpwl_cblistbox.h"
#include "fpdfsdk/pwl/cpwl_edit.h"
#include "fpdfsdk/pwl/ipwl_fillernotify.h"
#include "public/fpdf_fwlevent.h"

namespace {

constexpr float kComboBoxDefaultFontSize = 12.0f;
constexpr int kDefaultButtonWidth = 13;

}  // namespace

CPWL_ComboBox::CPWL_ComboBox(
    const CreateParams& cp,
    std::unique_ptr<IPWL_FillerNotify::PerWindowData> pAttachedData)
    : CPWL_Wnd(cp, std::move(pAttachedData)) {
  GetCreationParams()->dwFlags &= ~PWS_VSCROLL;
}

CPWL_ComboBox::~CPWL_ComboBox() = default;

void CPWL_ComboBox::OnDestroy() {
  // Until cleanup takes place in the virtual destructor for CPWL_Wnd
  // subclasses, implement the virtual OnDestroy method that does the
  // cleanup first, then invokes the superclass OnDestroy ... gee,
  // like a dtor would.
  m_pList.ExtractAsDangling();
  m_pButton.ExtractAsDangling();
  m_pEdit.ExtractAsDangling();
  CPWL_Wnd::OnDestroy();
}

void CPWL_ComboBox::SetFocus() {
  if (m_pEdit)
    m_pEdit->SetFocus();
}

void CPWL_ComboBox::KillFocus() {
  if (!SetPopup(false))
    return;

  CPWL_Wnd::KillFocus();
}

WideString CPWL_ComboBox::GetSelectedText() {
  if (m_pEdit)
    return m_pEdit->GetSelectedText();

  return WideString();
}

void CPWL_ComboBox::ReplaceAndKeepSelection(const WideString& text) {
  if (m_pEdit)
    m_pEdit->ReplaceAndKeepSelection(text);
}

void CPWL_ComboBox::ReplaceSelection(const WideString& text) {
  if (m_pEdit)
    m_pEdit->ReplaceSelection(text);
}

bool CPWL_ComboBox::SelectAllText() {
  return m_pEdit && m_pEdit->SelectAllText();
}

bool CPWL_ComboBox::CanUndo() {
  return m_pEdit && m_pEdit->CanUndo();
}

bool CPWL_ComboBox::CanRedo() {
  return m_pEdit && m_pEdit->CanRedo();
}

bool CPWL_ComboBox::Undo() {
  return m_pEdit && m_pEdit->Undo();
}

bool CPWL_ComboBox::Redo() {
  return m_pEdit && m_pEdit->Redo();
}

WideString CPWL_ComboBox::GetText() {
  return m_pEdit ? m_pEdit->GetText() : WideString();
}

void CPWL_ComboBox::SetText(const WideString& text) {
  if (m_pEdit)
    m_pEdit->SetText(text);
}

void CPWL_ComboBox::AddString(const WideString& str) {
  if (m_pList)
    m_pList->AddString(str);
}

int32_t CPWL_ComboBox::GetSelect() const {
  return m_nSelectItem;
}

void CPWL_ComboBox::SetSelect(int32_t nItemIndex) {
  if (m_pList)
    m_pList->Select(nItemIndex);

  m_pEdit->SetText(m_pList->GetText());
  m_nSelectItem = nItemIndex;
}

void CPWL_ComboBox::SetEditSelection(int32_t nStartChar, int32_t nEndChar) {
  if (m_pEdit)
    m_pEdit->SetSelection(nStartChar, nEndChar);
}

void CPWL_ComboBox::ClearSelection() {
  if (m_pEdit)
    m_pEdit->ClearSelection();
}

void CPWL_ComboBox::CreateChildWnd(const CreateParams& cp) {
  CreateEdit(cp);
  CreateButton(cp);
  CreateListBox(cp);
}

void CPWL_ComboBox::CreateEdit(const CreateParams& cp) {
  if (m_pEdit)
    return;

  CreateParams ecp = cp;
  ecp.dwFlags =
      PWS_VISIBLE | PWS_BORDER | PES_CENTER | PES_AUTOSCROLL | PES_UNDO;

  if (HasFlag(PWS_AUTOFONTSIZE))
    ecp.dwFlags |= PWS_AUTOFONTSIZE;

  if (!HasFlag(PCBS_ALLOWCUSTOMTEXT))
    ecp.dwFlags |= PWS_READONLY;

  ecp.rcRectWnd = CFX_FloatRect();
  ecp.dwBorderWidth = 0;
  ecp.nBorderStyle = BorderStyle::kSolid;

  auto pEdit = std::make_unique<CPWL_Edit>(ecp, CloneAttachedData());
  m_pEdit = pEdit.get();
  AddChild(std::move(pEdit));
  m_pEdit->Realize();
}

void CPWL_ComboBox::CreateButton(const CreateParams& cp) {
  if (m_pButton)
    return;

  CreateParams bcp = cp;
  bcp.dwFlags = PWS_VISIBLE | PWS_BORDER | PWS_BACKGROUND;
  bcp.sBackgroundColor = CFX_Color(CFX_Color::Type::kRGB, 220.0f / 255.0f,
                                   220.0f / 255.0f, 220.0f / 255.0f);
  bcp.sBorderColor = kDefaultBlackColor;
  bcp.dwBorderWidth = 2;
  bcp.nBorderStyle = BorderStyle::kBeveled;
  bcp.eCursorType = IPWL_FillerNotify::CursorStyle::kArrow;

  auto pButton = std::make_unique<CPWL_CBButton>(bcp, CloneAttachedData());
  m_pButton = pButton.get();
  AddChild(std::move(pButton));
  m_pButton->Realize();
}

void CPWL_ComboBox::CreateListBox(const CreateParams& cp) {
  if (m_pList)
    return;

  CreateParams lcp = cp;
  lcp.dwFlags = PWS_BORDER | PWS_BACKGROUND | PLBS_HOVERSEL | PWS_VSCROLL;
  lcp.nBorderStyle = BorderStyle::kSolid;
  lcp.dwBorderWidth = 1;
  lcp.eCursorType = IPWL_FillerNotify::CursorStyle::kArrow;
  lcp.rcRectWnd = CFX_FloatRect();
  lcp.fFontSize =
      (cp.dwFlags & PWS_AUTOFONTSIZE) ? kComboBoxDefaultFontSize : cp.fFontSize;

  if (cp.sBorderColor.nColorType == CFX_Color::Type::kTransparent)
    lcp.sBorderColor = kDefaultBlackColor;

  if (cp.sBackgroundColor.nColorType == CFX_Color::Type::kTransparent)
    lcp.sBackgroundColor = kDefaultWhiteColor;

  auto pList = std::make_unique<CPWL_CBListBox>(lcp, CloneAttachedData());
  m_pList = pList.get();
  AddChild(std::move(pList));
  m_pList->Realize();
}

bool CPWL_ComboBox::RepositionChildWnd() {
  ObservedPtr<CPWL_ComboBox> this_observed(this);
  const CFX_FloatRect rcClient = this_observed->GetClientRect();
  if (this_observed->m_bPopup) {
    const float fOldWindowHeight = this_observed->m_rcOldWindow.Height();
    const float fOldClientHeight = fOldWindowHeight - GetBorderWidth() * 2;
    CFX_FloatRect rcList = CPWL_Wnd::GetWindowRect();
    CFX_FloatRect rcButton = rcClient;
    rcButton.left =
        std::max(rcButton.right - kDefaultButtonWidth, rcClient.left);
    CFX_FloatRect rcEdit = rcClient;
    rcEdit.right = std::max(rcButton.left - 1.0f, rcEdit.left);
    if (this_observed->m_bBottom) {
      rcButton.bottom = rcButton.top - fOldClientHeight;
      rcEdit.bottom = rcEdit.top - fOldClientHeight;
      rcList.top -= fOldWindowHeight;
    } else {
      rcButton.top = rcButton.bottom + fOldClientHeight;
      rcEdit.top = rcEdit.bottom + fOldClientHeight;
      rcList.bottom += fOldWindowHeight;
    }
    if (this_observed->m_pButton) {
      this_observed->m_pButton->Move(rcButton, true, false);
      if (!this_observed) {
        return false;
      }
    }
    if (this_observed->m_pEdit) {
      this_observed->m_pEdit->Move(rcEdit, true, false);
      if (!this_observed) {
        return false;
      }
    }
    if (this_observed->m_pList) {
      if (!this_observed->m_pList->SetVisible(true) || !this_observed) {
        return false;
      }
      if (!this_observed->m_pList->Move(rcList, true, false) ||
          !this_observed) {
        return false;
      }
      this_observed->m_pList->ScrollToListItem(this_observed->m_nSelectItem);
      if (!this_observed) {
        return false;
      }
    }
    return true;
  }

  CFX_FloatRect rcButton = rcClient;
  rcButton.left = std::max(rcButton.right - kDefaultButtonWidth, rcClient.left);
  if (this_observed->m_pButton) {
    this_observed->m_pButton->Move(rcButton, true, false);
    if (!this_observed) {
      return false;
    }
  }

  CFX_FloatRect rcEdit = rcClient;
  rcEdit.right = std::max(rcButton.left - 1.0f, rcEdit.left);
  if (this_observed->m_pEdit) {
    this_observed->m_pEdit->Move(rcEdit, true, false);
    if (!this_observed) {
      return false;
    }
  }
  if (this_observed->m_pList) {
    if (!this_observed->m_pList->SetVisible(false)) {
      this_observed->m_pList = nullptr;  // Gone, dangling even.
      return false;
    }
    if (!this_observed) {
      return false;
    }
  }
  return true;
}

void CPWL_ComboBox::SelectAll() {
  if (m_pEdit && HasFlag(PCBS_ALLOWCUSTOMTEXT))
    m_pEdit->SelectAllText();
}

CFX_FloatRect CPWL_ComboBox::GetFocusRect() const {
  return CFX_FloatRect();
}

bool CPWL_ComboBox::SetPopup(bool bPopup) {
  ObservedPtr<CPWL_ComboBox> this_observed(this);
  if (!this_observed->m_pList) {
    return true;
  }
  if (bPopup == this_observed->m_bPopup) {
    return true;
  }
  float fListHeight = this_observed->m_pList->GetContentRect().Height();
  if (!FXSYS_IsFloatBigger(fListHeight, 0.0f)) {
    return true;
  }
  if (!bPopup) {
    this_observed->m_bPopup = false;
    return Move(this_observed->m_rcOldWindow, true, true);
  }
  if (GetFillerNotify()->OnPopupPreOpen(GetAttachedData(), {})) {
    return !!this_observed;
  }
  if (!this_observed) {
    return false;
  }
  float fBorderWidth = this_observed->m_pList->GetBorderWidth() * 2;
  float fPopupMin = 0.0f;
  if (this_observed->m_pList->GetCount() > 3) {
    fPopupMin = this_observed->m_pList->GetFirstHeight() * 3 + fBorderWidth;
  }
  float fPopupMax = fListHeight + fBorderWidth;
  bool bBottom;
  float fPopupRet;
  this_observed->GetFillerNotify()->QueryWherePopup(
      this_observed->GetAttachedData(), fPopupMin, fPopupMax, &bBottom,
      &fPopupRet);
  if (!FXSYS_IsFloatBigger(fPopupRet, 0.0f)) {
    return true;
  }
  this_observed->m_rcOldWindow = this_observed->CPWL_Wnd::GetWindowRect();
  this_observed->m_bPopup = bPopup;
  this_observed->m_bBottom = bBottom;

  CFX_FloatRect rcWindow = this_observed->m_rcOldWindow;
  if (bBottom) {
    rcWindow.bottom -= fPopupRet;
  } else {
    rcWindow.top += fPopupRet;
  }
  if (!this_observed->Move(rcWindow, true, true)) {
    return false;
  }
  this_observed->GetFillerNotify()->OnPopupPostOpen(
      this_observed->GetAttachedData(), {});
  return !!this_observed;
}

bool CPWL_ComboBox::OnKeyDown(FWL_VKEYCODE nKeyCode,
                              Mask<FWL_EVENTFLAG> nFlag) {
  ObservedPtr<CPWL_ComboBox> this_observed(this);
  if (!this_observed->m_pList) {
    return false;
  }
  if (!this_observed->m_pEdit) {
    return false;
  }
  this_observed->m_nSelectItem = -1;

  switch (nKeyCode) {
    case FWL_VKEY_Up:
      if (this_observed->m_pList->GetCurSel() > 0) {
        if (this_observed->GetFillerNotify()->OnPopupPreOpen(GetAttachedData(),
                                                             nFlag) ||
            !this_observed) {
          return false;
        }
        if (this_observed->GetFillerNotify()->OnPopupPostOpen(GetAttachedData(),
                                                              nFlag) ||
            !this_observed) {
          return false;
        }
        if (this_observed->m_pList->IsMovementKey(nKeyCode)) {
          if (this_observed->m_pList->OnMovementKeyDown(nKeyCode, nFlag) ||
              !this_observed) {
            return false;
          }
          this_observed->SetSelectText();
        }
      }
      return true;
    case FWL_VKEY_Down:
      if (this_observed->m_pList->GetCurSel() <
          this_observed->m_pList->GetCount() - 1) {
        if (this_observed->GetFillerNotify()->OnPopupPreOpen(GetAttachedData(),
                                                             nFlag) ||
            !this_observed) {
          return false;
        }
        if (this_observed->GetFillerNotify()->OnPopupPostOpen(GetAttachedData(),
                                                              nFlag) ||
            !this_observed) {
          return false;
        }
        if (this_observed->m_pList->IsMovementKey(nKeyCode)) {
          if (this_observed->m_pList->OnMovementKeyDown(nKeyCode, nFlag) ||
              !this_observed) {
            return false;
          }
          this_observed->SetSelectText();
        }
      }
      return true;
    default:
      break;
  }
  if (this_observed->HasFlag(PCBS_ALLOWCUSTOMTEXT)) {
    return this_observed->m_pEdit->OnKeyDown(nKeyCode, nFlag);
  }
  return false;
}

bool CPWL_ComboBox::OnChar(uint16_t nChar, Mask<FWL_EVENTFLAG> nFlag) {
  ObservedPtr<CPWL_ComboBox> this_observed(this);
  if (!this_observed->m_pList) {
    return false;
  }
  if (!this_observed->m_pEdit) {
    return false;
  }
  // In a combo box if the ENTER/SPACE key is pressed, show the combo box
  // options.
  switch (nChar) {
    case pdfium::ascii::kReturn:
      if (!this_observed->SetPopup(!this_observed->IsPopup())) {
        return false;
      }
      this_observed->SetSelectText();
      return true;
    case pdfium::ascii::kSpace:
      // Show the combo box options with space only if the combo box is not
      // editable
      if (!this_observed->HasFlag(PCBS_ALLOWCUSTOMTEXT)) {
        if (!this_observed->IsPopup()) {
          if (!this_observed->SetPopup(/*bPopUp=*/true)) {
            return false;
          }
          this_observed->SetSelectText();
        }
        return true;
      }
      break;
    default:
      break;
  }

  this_observed->m_nSelectItem = -1;
  if (this_observed->HasFlag(PCBS_ALLOWCUSTOMTEXT)) {
    return this_observed->m_pEdit->OnChar(nChar, nFlag);
  }
  if (this_observed->GetFillerNotify()->OnPopupPreOpen(GetAttachedData(),
                                                       nFlag) ||
      !this_observed) {
    return false;
  }
  if (this_observed->GetFillerNotify()->OnPopupPostOpen(GetAttachedData(),
                                                        nFlag) ||
      !this_observed) {
    return false;
  }
  if (!this_observed->m_pList->IsChar(nChar, nFlag)) {
    return false;
  }
  return this_observed->m_pList->OnCharNotify(nChar, nFlag);
}

void CPWL_ComboBox::NotifyLButtonDown(CPWL_Wnd* child, const CFX_PointF& pos) {
  if (child == m_pButton) {
    (void)SetPopup(!m_bPopup);
    // Note, |this| may no longer be viable at this point. If more work needs to
    // be done, check the return value of SetPopup().
  }
}

void CPWL_ComboBox::NotifyLButtonUp(CPWL_Wnd* child, const CFX_PointF& pos) {
  if (!m_pEdit || !m_pList || child != m_pList)
    return;

  SetSelectText();
  SelectAllText();
  m_pEdit->SetFocus();
  (void)SetPopup(false);
  // Note, |this| may no longer be viable at this point. If more work needs to
  // be done, check the return value of SetPopup().
}

bool CPWL_ComboBox::IsPopup() const {
  return m_bPopup;
}

void CPWL_ComboBox::SetSelectText() {
  m_pEdit->SelectAllText();
  m_pEdit->ReplaceSelection(m_pList->GetText());
  m_pEdit->SelectAllText();
  m_nSelectItem = m_pList->GetCurSel();
}
