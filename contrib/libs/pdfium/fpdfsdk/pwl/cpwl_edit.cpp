// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "fpdfsdk/pwl/cpwl_edit.h"

#include <algorithm>
#include <memory>
#include <sstream>
#include <utility>

#include "constants/ascii.h"
#include "core/fpdfapi/font/cpdf_font.h"
#include "core/fpdfdoc/cpvt_word.h"
#include "core/fpdfdoc/ipvt_fontmap.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/fx_safe_types.h"
#include "core/fxge/cfx_fillrenderoptions.h"
#include "core/fxge/cfx_graphstatedata.h"
#include "core/fxge/cfx_path.h"
#include "core/fxge/cfx_renderdevice.h"
#include "core/fxge/fx_font.h"
#include "fpdfsdk/pwl/cpwl_caret.h"
#include "fpdfsdk/pwl/cpwl_edit_impl.h"
#include "fpdfsdk/pwl/cpwl_scroll_bar.h"
#include "fpdfsdk/pwl/cpwl_wnd.h"
#include "fpdfsdk/pwl/ipwl_fillernotify.h"
#include "public/fpdf_fwlevent.h"

CPWL_Edit::CPWL_Edit(
    const CreateParams& cp,
    std::unique_ptr<IPWL_FillerNotify::PerWindowData> pAttachedData)
    : CPWL_Wnd(cp, std::move(pAttachedData)),
      m_pEditImpl(std::make_unique<CPWL_EditImpl>()) {
  GetCreationParams()->eCursorType = IPWL_FillerNotify::CursorStyle::kVBeam;
}

CPWL_Edit::~CPWL_Edit() {
  DCHECK(!m_bFocus);
}

void CPWL_Edit::SetText(const WideString& csText) {
  m_pEditImpl->SetText(csText);
  m_pEditImpl->Paint();
}

bool CPWL_Edit::RepositionChildWnd() {
  ObservedPtr<CPWL_Edit> this_observed(this);
  if (CPWL_ScrollBar* pVSB = this_observed->GetVScrollBar()) {
    CFX_FloatRect rcWindow = this_observed->m_rcOldWindow;
    CFX_FloatRect rcVScroll =
        CFX_FloatRect(rcWindow.right, rcWindow.bottom,
                      rcWindow.right + CPWL_ScrollBar::kWidth, rcWindow.top);
    pVSB->Move(rcVScroll, true, false);
    if (!this_observed) {
      return false;
    }
  }
  if (this_observed->m_pCaret && !HasFlag(PES_TEXTOVERFLOW)) {
    CFX_FloatRect rect = this_observed->GetClientRect();
    if (!rect.IsEmpty()) {
      // +1 for caret beside border
      rect.Inflate(1.0f, 1.0f);
      rect.Normalize();
    }
    this_observed->m_pCaret->SetClipRect(rect);
  }
  this_observed->m_pEditImpl->SetPlateRect(GetClientRect());
  this_observed->m_pEditImpl->Paint();
  return true;
}

CFX_FloatRect CPWL_Edit::GetClientRect() const {
  float width = static_cast<float>(GetBorderWidth() + GetInnerBorderWidth());
  CFX_FloatRect rcClient = GetWindowRect().GetDeflated(width, width);
  CPWL_ScrollBar* pVSB = GetVScrollBar();
  if (pVSB && pVSB->IsVisible())
    rcClient.right -= CPWL_ScrollBar::kWidth;
  return rcClient;
}

void CPWL_Edit::SetAlignFormatVerticalCenter() {
  m_pEditImpl->SetAlignmentV(static_cast<int32_t>(PEAV_CENTER));
  m_pEditImpl->Paint();
}

bool CPWL_Edit::CanSelectAll() const {
  return GetSelectWordRange() != m_pEditImpl->GetWholeWordRange();
}

bool CPWL_Edit::CanCopy() const {
  return !HasFlag(PES_PASSWORD) && m_pEditImpl->IsSelected();
}

bool CPWL_Edit::CanCut() const {
  return CanCopy() && !IsReadOnly();
}

void CPWL_Edit::CutText() {
  if (!CanCut())
    return;
  m_pEditImpl->ClearSelection();
}

void CPWL_Edit::OnCreated() {
  SetFontSize(GetCreationParams()->fFontSize);
  m_pEditImpl->SetFontMap(GetFontMap());
  m_pEditImpl->SetNotify(this);
  m_pEditImpl->Initialize();

  if (CPWL_ScrollBar* pScroll = GetVScrollBar()) {
    pScroll->RemoveFlag(PWS_AUTOTRANSPARENT);
    pScroll->SetTransparency(255);
  }

  SetParamByFlag();
  m_rcOldWindow = GetWindowRect();
}

void CPWL_Edit::SetParamByFlag() {
  if (HasFlag(PES_RIGHT)) {
    m_pEditImpl->SetAlignmentH(2);
  } else if (HasFlag(PES_MIDDLE)) {
    m_pEditImpl->SetAlignmentH(1);
  } else {
    m_pEditImpl->SetAlignmentH(0);
  }

  if (HasFlag(PES_CENTER)) {
    m_pEditImpl->SetAlignmentV(1);
  } else {
    m_pEditImpl->SetAlignmentV(0);
  }

  if (HasFlag(PES_PASSWORD)) {
    m_pEditImpl->SetPasswordChar('*');
  }

  m_pEditImpl->SetMultiLine(HasFlag(PES_MULTILINE));
  m_pEditImpl->SetAutoReturn(HasFlag(PES_AUTORETURN));
  m_pEditImpl->SetAutoFontSize(HasFlag(PWS_AUTOFONTSIZE));
  m_pEditImpl->SetAutoScroll(HasFlag(PES_AUTOSCROLL));
  m_pEditImpl->EnableUndo(HasFlag(PES_UNDO));

  if (HasFlag(PES_TEXTOVERFLOW)) {
    SetClipRect(CFX_FloatRect());
    m_pEditImpl->SetTextOverflow(true);
  } else {
    if (m_pCaret) {
      CFX_FloatRect rect = GetClientRect();
      if (!rect.IsEmpty()) {
        // +1 for caret beside border
        rect.Inflate(1.0f, 1.0f);
        rect.Normalize();
      }
      m_pCaret->SetClipRect(rect);
    }
  }
}

void CPWL_Edit::DrawThisAppearance(CFX_RenderDevice* pDevice,
                                   const CFX_Matrix& mtUser2Device) {
  CPWL_Wnd::DrawThisAppearance(pDevice, mtUser2Device);

  const CFX_FloatRect rcClient = GetClientRect();
  const BorderStyle border_style = GetBorderStyle();
  const int32_t nCharArray = m_pEditImpl->GetCharArray();
  bool draw_border = nCharArray > 0 && (border_style == BorderStyle::kSolid ||
                                        border_style == BorderStyle::kDash);
  if (draw_border) {
    FX_SAFE_INT32 nCharArraySafe = nCharArray;
    nCharArraySafe -= 1;
    nCharArraySafe *= 2;
    draw_border = nCharArraySafe.IsValid();
  }

  if (draw_border) {
    CFX_GraphStateData gsd;
    gsd.m_LineWidth = GetBorderWidth();
    if (border_style == BorderStyle::kDash) {
      gsd.m_DashArray = {static_cast<float>(GetBorderDash().nDash),
                         static_cast<float>(GetBorderDash().nGap)};
      gsd.m_DashPhase = GetBorderDash().nPhase;
    }

    const float width = (rcClient.right - rcClient.left) / nCharArray;
    CFX_Path path;
    CFX_PointF bottom(0, rcClient.bottom);
    CFX_PointF top(0, rcClient.top);
    for (int32_t i = 0; i < nCharArray - 1; ++i) {
      bottom.x = rcClient.left + width * (i + 1);
      top.x = bottom.x;
      path.AppendPoint(bottom, CFX_Path::Point::Type::kMove);
      path.AppendPoint(top, CFX_Path::Point::Type::kLine);
    }
    if (!path.GetPoints().empty()) {
      pDevice->DrawPath(path, &mtUser2Device, &gsd, 0,
                        GetBorderColor().ToFXColor(255),
                        CFX_FillRenderOptions::EvenOddOptions());
    }
  }

  CFX_FloatRect rcClip;
  CPVT_WordRange wrRange = m_pEditImpl->GetVisibleWordRange();
  CPVT_WordRange* pRange = nullptr;
  if (!HasFlag(PES_TEXTOVERFLOW)) {
    rcClip = GetClientRect();
    pRange = &wrRange;
  }
  m_pEditImpl->DrawEdit(
      pDevice, mtUser2Device, GetTextColor().ToFXColor(GetTransparency()),
      rcClip, CFX_PointF(), pRange, GetFillerNotify(), GetAttachedData());
}

void CPWL_Edit::OnSetFocus() {
  ObservedPtr<CPWL_Edit> this_observed(this);
  this_observed->SetEditCaret(true);
  if (!this_observed) {
    return;
  }
  if (!this_observed->IsReadOnly()) {
    CPWL_Wnd::ProviderIface* pProvider = this_observed->GetProvider();
    if (pProvider) {
      pProvider->OnSetFocusForEdit(this);
      if (!this_observed) {
        return;
      }
    }
  }
  this_observed->m_bFocus = true;
}

void CPWL_Edit::OnKillFocus() {
  ObservedPtr<CPWL_Edit> this_observed(this);
  CPWL_ScrollBar* pScroll = this_observed->GetVScrollBar();
  if (pScroll && pScroll->IsVisible()) {
    if (!pScroll->SetVisible(false)) {
      return;
    }
    if (!this_observed) {
      return;
    }
    if (!this_observed->Move(this_observed->m_rcOldWindow, true, true)) {
      return;
    }
  }
  this_observed->m_pEditImpl->SelectNone();
  if (!this_observed) {
    return;
  }
  if (!this_observed->SetCaret(false, CFX_PointF(), CFX_PointF())) {
    return;
  }
  this_observed->SetCharSet(FX_Charset::kANSI);
  this_observed->m_bFocus = false;
}

CPVT_WordRange CPWL_Edit::GetSelectWordRange() const {
  if (!m_pEditImpl->IsSelected())
    return CPVT_WordRange();

  auto [nStart, nEnd] = m_pEditImpl->GetSelection();

  CPVT_WordPlace wpStart = m_pEditImpl->WordIndexToWordPlace(nStart);
  CPVT_WordPlace wpEnd = m_pEditImpl->WordIndexToWordPlace(nEnd);
  return CPVT_WordRange(wpStart, wpEnd);
}

bool CPWL_Edit::IsTextFull() const {
  return m_pEditImpl->IsTextFull();
}

float CPWL_Edit::GetCharArrayAutoFontSize(const CPDF_Font* pFont,
                                          const CFX_FloatRect& rcPlate,
                                          int32_t nCharArray) {
  if (!pFont || pFont->IsStandardFont())
    return 0.0f;

  const FX_RECT& rcBBox = pFont->GetFontBBox();

  CFX_FloatRect rcCell = rcPlate;
  float xdiv = rcCell.Width() / nCharArray * 1000.0f / rcBBox.Width();
  float ydiv = -rcCell.Height() * 1000.0f / rcBBox.Height();

  return xdiv < ydiv ? xdiv : ydiv;
}

void CPWL_Edit::SetCharArray(int32_t nCharArray) {
  if (!HasFlag(PES_CHARARRAY) || nCharArray <= 0)
    return;

  m_pEditImpl->SetCharArray(nCharArray);
  m_pEditImpl->SetTextOverflow(true);
  m_pEditImpl->Paint();

  if (!HasFlag(PWS_AUTOFONTSIZE))
    return;

  IPVT_FontMap* pFontMap = GetFontMap();
  if (!pFontMap)
    return;

  float fFontSize = GetCharArrayAutoFontSize(pFontMap->GetPDFFont(0).Get(),
                                             GetClientRect(), nCharArray);
  if (fFontSize <= 0.0f)
    return;

  m_pEditImpl->SetAutoFontSize(false);
  m_pEditImpl->SetFontSize(fFontSize);
  m_pEditImpl->Paint();
}

void CPWL_Edit::SetLimitChar(int32_t nLimitChar) {
  m_pEditImpl->SetLimitChar(nLimitChar);
  m_pEditImpl->Paint();
}

CFX_FloatRect CPWL_Edit::GetFocusRect() const {
  return CFX_FloatRect();
}

bool CPWL_Edit::IsVScrollBarVisible() const {
  CPWL_ScrollBar* pScroll = GetVScrollBar();
  return pScroll && pScroll->IsVisible();
}

bool CPWL_Edit::OnKeyDown(FWL_VKEYCODE nKeyCode, Mask<FWL_EVENTFLAG> nFlag) {
  ObservedPtr<CPWL_Edit> this_observed(this);
  if (this_observed->m_bMouseDown) {
    return true;
  }
  if (nKeyCode == FWL_VKEY_Delete) {
    WideString strChange;
    WideString strChangeEx;
    auto [nSelStart, nSelEnd] = this_observed->GetSelection();
    if (nSelStart == nSelEnd) {
      nSelEnd = nSelStart + 1;
    }
    IPWL_FillerNotify::BeforeKeystrokeResult result =
        this_observed->GetFillerNotify()->OnBeforeKeyStroke(
            this_observed->GetAttachedData(), strChange, strChangeEx, nSelStart,
            nSelEnd, true, nFlag);

    if (!this_observed) {
      return false;
    }
    if (!result.rc) {
      return false;
    }
    if (result.exit) {
      return false;
    }
  }

  bool bRet = this_observed->OnKeyDownInternal(nKeyCode, nFlag);

  // In case of implementation swallow the OnKeyDown event.
  if (this_observed->IsProceedtoOnChar(nKeyCode, nFlag)) {
    return true;
  }
  return bRet;
}

// static
bool CPWL_Edit::IsProceedtoOnChar(FWL_VKEYCODE nKeyCode,
                                  Mask<FWL_EVENTFLAG> nFlag) {
  bool bCtrl = IsPlatformShortcutKey(nFlag);
  bool bAlt = IsALTKeyDown(nFlag);
  if (bCtrl && !bAlt) {
    // hot keys for edit control.
    switch (nKeyCode) {
      case FWL_VKEY_A:
      case FWL_VKEY_C:
      case FWL_VKEY_V:
      case FWL_VKEY_X:
      case FWL_VKEY_Z:
        return true;
      default:
        break;
    }
  }
  // control characters.
  switch (nKeyCode) {
    case FWL_VKEY_Escape:
    case FWL_VKEY_Back:
    case FWL_VKEY_Return:
    case FWL_VKEY_Space:
      return true;
    default:
      return false;
  }
}

bool CPWL_Edit::OnChar(uint16_t nChar, Mask<FWL_EVENTFLAG> nFlag) {
  ObservedPtr<CPWL_Edit> this_observed(this);
  if (this_observed->m_bMouseDown) {
    return true;
  }
  if (!this_observed->IsCTRLKeyDown(nFlag)) {
    WideString swChange;
    auto [nSelStart, nSelEnd] = this_observed->GetSelection();
    switch (nChar) {
      case pdfium::ascii::kBackspace:
        if (nSelStart == nSelEnd)
          nSelStart = nSelEnd - 1;
        break;
      case pdfium::ascii::kReturn:
        break;
      default:
        swChange += nChar;
        break;
    }
    WideString strChangeEx;
    IPWL_FillerNotify::BeforeKeystrokeResult result =
        this_observed->GetFillerNotify()->OnBeforeKeyStroke(
            this_observed->GetAttachedData(), swChange, strChangeEx, nSelStart,
            nSelEnd, true, nFlag);

    if (!this_observed) {
      return false;
    }
    if (!result.rc) {
      return true;
    }
    if (result.exit) {
      return false;
    }
  }
  if (IPVT_FontMap* pFontMap = this_observed->GetFontMap()) {
    FX_Charset nOldCharSet = this_observed->GetCharSet();
    FX_Charset nNewCharSet =
        pFontMap->CharSetFromUnicode(nChar, FX_Charset::kDefault);
    if (nOldCharSet != nNewCharSet) {
      this_observed->SetCharSet(nNewCharSet);
    }
  }
  return this_observed->OnCharInternal(nChar, nFlag);
}

bool CPWL_Edit::OnMouseWheel(Mask<FWL_EVENTFLAG> nFlag,
                             const CFX_PointF& point,
                             const CFX_Vector& delta) {
  if (!HasFlag(PES_MULTILINE))
    return false;

  CFX_PointF ptScroll = GetScrollPos();
  if (delta.y > 0)
    ptScroll.y += GetFontSize();
  else
    ptScroll.y -= GetFontSize();
  SetScrollPos(ptScroll);
  return true;
}

void CPWL_Edit::OnDestroy() {
  m_pCaret.ExtractAsDangling();
}

bool CPWL_Edit::IsWndHorV() const {
  CFX_Matrix mt = GetWindowMatrix();
  return mt.Transform(CFX_PointF(1, 1)).y == mt.Transform(CFX_PointF(0, 1)).y;
}

void CPWL_Edit::SetCursor() {
  if (IsValid()) {
    GetFillerNotify()->SetCursor(IsWndHorV()
                                     ? IPWL_FillerNotify::CursorStyle::kVBeam
                                     : IPWL_FillerNotify::CursorStyle::kHBeam);
  }
}

WideString CPWL_Edit::GetSelectedText() {
  return m_pEditImpl->GetSelectedText();
}

void CPWL_Edit::ReplaceAndKeepSelection(const WideString& text) {
  m_pEditImpl->ReplaceAndKeepSelection(text);
}

void CPWL_Edit::ReplaceSelection(const WideString& text) {
  m_pEditImpl->ReplaceSelection(text);
}

bool CPWL_Edit::SelectAllText() {
  m_pEditImpl->SelectAll();
  return true;
}

void CPWL_Edit::SetScrollInfo(const PWL_SCROLL_INFO& info) {
  if (CPWL_Wnd* pChild = GetVScrollBar())
    pChild->SetScrollInfo(info);
}

void CPWL_Edit::SetScrollPosition(float pos) {
  if (CPWL_Wnd* pChild = GetVScrollBar())
    pChild->SetScrollPosition(pos);
}

void CPWL_Edit::ScrollWindowVertically(float pos) {
  m_pEditImpl->SetScrollPos(CFX_PointF(m_pEditImpl->GetScrollPos().x, pos));
}

void CPWL_Edit::CreateChildWnd(const CreateParams& cp) {
  if (!IsReadOnly())
    CreateEditCaret(cp);
}

void CPWL_Edit::CreateEditCaret(const CreateParams& cp) {
  if (m_pCaret)
    return;

  CreateParams ecp = cp;
  ecp.dwFlags = PWS_NOREFRESHCLIP;
  ecp.dwBorderWidth = 0;
  ecp.nBorderStyle = BorderStyle::kSolid;
  ecp.rcRectWnd = CFX_FloatRect();

  auto pCaret = std::make_unique<CPWL_Caret>(ecp, CloneAttachedData());
  m_pCaret = pCaret.get();
  m_pCaret->SetInvalidRect(GetClientRect());
  AddChild(std::move(pCaret));
  m_pCaret->Realize();
}

void CPWL_Edit::SetFontSize(float fFontSize) {
  m_pEditImpl->SetFontSize(fFontSize);
  m_pEditImpl->Paint();
}

float CPWL_Edit::GetFontSize() const {
  return m_pEditImpl->GetFontSize();
}

bool CPWL_Edit::OnKeyDownInternal(FWL_VKEYCODE nKeyCode,
                                  Mask<FWL_EVENTFLAG> nFlag) {
  if (m_bMouseDown)
    return true;

  bool bRet = CPWL_Wnd::OnKeyDown(nKeyCode, nFlag);

  // FILTER
  switch (nKeyCode) {
    default:
      return false;
    case FWL_VKEY_Delete:
    case FWL_VKEY_Up:
    case FWL_VKEY_Down:
    case FWL_VKEY_Left:
    case FWL_VKEY_Right:
    case FWL_VKEY_Home:
    case FWL_VKEY_End:
    case FWL_VKEY_Insert:
    case FWL_VKEY_A:
    case FWL_VKEY_C:
    case FWL_VKEY_V:
    case FWL_VKEY_X:
    case FWL_VKEY_Z:
      break;
  }

  if (nKeyCode == FWL_VKEY_Delete && m_pEditImpl->IsSelected())
    nKeyCode = FWL_VKEY_Unknown;

  switch (nKeyCode) {
    case FWL_VKEY_Delete:
      Delete();
      return true;
    case FWL_VKEY_Insert:
      if (IsSHIFTKeyDown(nFlag))
        PasteText();
      return true;
    case FWL_VKEY_Up:
      m_pEditImpl->OnVK_UP(IsSHIFTKeyDown(nFlag));
      return true;
    case FWL_VKEY_Down:
      m_pEditImpl->OnVK_DOWN(IsSHIFTKeyDown(nFlag));
      return true;
    case FWL_VKEY_Left:
      m_pEditImpl->OnVK_LEFT(IsSHIFTKeyDown(nFlag));
      return true;
    case FWL_VKEY_Right:
      m_pEditImpl->OnVK_RIGHT(IsSHIFTKeyDown(nFlag));
      return true;
    case FWL_VKEY_Home:
      m_pEditImpl->OnVK_HOME(IsSHIFTKeyDown(nFlag), IsCTRLKeyDown(nFlag));
      return true;
    case FWL_VKEY_End:
      m_pEditImpl->OnVK_END(IsSHIFTKeyDown(nFlag), IsCTRLKeyDown(nFlag));
      return true;
    case FWL_VKEY_Unknown:
      if (!IsSHIFTKeyDown(nFlag))
        ClearSelection();
      else
        CutText();
      return true;
    default:
      break;
  }

  return bRet;
}

bool CPWL_Edit::OnCharInternal(uint16_t nChar, Mask<FWL_EVENTFLAG> nFlag) {
  if (m_bMouseDown)
    return true;

  CPWL_Wnd::OnChar(nChar, nFlag);

  // FILTER
  switch (nChar) {
    case pdfium::ascii::kNewline:
    case pdfium::ascii::kEscape:
      return false;
    default:
      break;
  }

  bool bCtrl = IsPlatformShortcutKey(nFlag);
  bool bAlt = IsALTKeyDown(nFlag);
  bool bShift = IsSHIFTKeyDown(nFlag);

  uint16_t word = nChar;

  if (bCtrl && !bAlt) {
    switch (nChar) {
      case pdfium::ascii::kControlC:
        CopyText();
        return true;
      case pdfium::ascii::kControlV:
        PasteText();
        return true;
      case pdfium::ascii::kControlX:
        CutText();
        return true;
      case pdfium::ascii::kControlA:
        SelectAllText();
        return true;
      case pdfium::ascii::kControlZ:
        if (bShift)
          Redo();
        else
          Undo();
        return true;
      default:
        if (nChar < 32)
          return false;
    }
  }

  if (IsReadOnly())
    return true;

  if (m_pEditImpl->IsSelected() && word == pdfium::ascii::kBackspace)
    word = pdfium::ascii::kNul;

  ClearSelection();

  switch (word) {
    case pdfium::ascii::kBackspace:
      Backspace();
      break;
    case pdfium::ascii::kReturn:
      InsertReturn();
      break;
    case pdfium::ascii::kNul:
      break;
    default:
      InsertWord(word, GetCharSet());
      break;
  }

  return true;
}

bool CPWL_Edit::OnLButtonDown(Mask<FWL_EVENTFLAG> nFlag,
                              const CFX_PointF& point) {
  CPWL_Wnd::OnLButtonDown(nFlag, point);
  if (HasFlag(PES_TEXTOVERFLOW) || ClientHitTest(point)) {
    if (m_bMouseDown && !InvalidateRect(nullptr))
      return true;

    m_bMouseDown = true;
    SetCapture();
    m_pEditImpl->OnMouseDown(point, IsSHIFTKeyDown(nFlag),
                             IsCTRLKeyDown(nFlag));
  }
  return true;
}

bool CPWL_Edit::OnLButtonUp(Mask<FWL_EVENTFLAG> nFlag,
                            const CFX_PointF& point) {
  CPWL_Wnd::OnLButtonUp(nFlag, point);
  if (m_bMouseDown) {
    // can receive keybord message
    if (ClientHitTest(point) && !IsFocused())
      SetFocus();

    ReleaseCapture();
    m_bMouseDown = false;
  }
  return true;
}

bool CPWL_Edit::OnLButtonDblClk(Mask<FWL_EVENTFLAG> nFlag,
                                const CFX_PointF& point) {
  CPWL_Wnd::OnLButtonDblClk(nFlag, point);
  if (HasFlag(PES_TEXTOVERFLOW) || ClientHitTest(point))
    m_pEditImpl->SelectAll();

  return true;
}

bool CPWL_Edit::OnRButtonUp(Mask<FWL_EVENTFLAG> nFlag,
                            const CFX_PointF& point) {
  if (m_bMouseDown)
    return false;

  CPWL_Wnd::OnRButtonUp(nFlag, point);
  if (!HasFlag(PES_TEXTOVERFLOW) && !ClientHitTest(point))
    return true;

  SetFocus();
  return false;
}

bool CPWL_Edit::OnMouseMove(Mask<FWL_EVENTFLAG> nFlag,
                            const CFX_PointF& point) {
  CPWL_Wnd::OnMouseMove(nFlag, point);

  if (m_bMouseDown)
    m_pEditImpl->OnMouseMove(point, false, false);

  return true;
}

void CPWL_Edit::SetEditCaret(bool bVisible) {
  CFX_PointF ptHead;
  CFX_PointF ptFoot;
  if (bVisible)
    GetCaretInfo(&ptHead, &ptFoot);

  SetCaret(bVisible, ptHead, ptFoot);
  // Note, |this| may no longer be viable at this point. If more work needs to
  // be done, check the return value of SetCaret().
}

void CPWL_Edit::GetCaretInfo(CFX_PointF* ptHead, CFX_PointF* ptFoot) const {
  CPWL_EditImpl::Iterator* pIterator = m_pEditImpl->GetIterator();
  pIterator->SetAt(m_pEditImpl->GetCaret());
  CPVT_Word word;
  CPVT_Line line;
  if (pIterator->GetWord(word)) {
    ptHead->x = word.ptWord.x + word.fWidth;
    ptHead->y = word.ptWord.y + word.fAscent;
    ptFoot->x = word.ptWord.x + word.fWidth;
    ptFoot->y = word.ptWord.y + word.fDescent;
  } else if (pIterator->GetLine(line)) {
    ptHead->x = line.ptLine.x;
    ptHead->y = line.ptLine.y + line.fLineAscent;
    ptFoot->x = line.ptLine.x;
    ptFoot->y = line.ptLine.y + line.fLineDescent;
  }
}

bool CPWL_Edit::SetCaret(bool bVisible,
                         const CFX_PointF& ptHead,
                         const CFX_PointF& ptFoot) {
  ObservedPtr<CPWL_Edit> this_observed(this);
  if (!this_observed->m_pCaret) {
    return true;
  }
  if (!this_observed->IsFocused() || this_observed->m_pEditImpl->IsSelected()) {
    bVisible = false;
  }
  this_observed->m_pCaret->SetCaret(bVisible, ptHead, ptFoot);
  return !!this_observed;
}

WideString CPWL_Edit::GetText() {
  return m_pEditImpl->GetText();
}

void CPWL_Edit::SetSelection(int32_t nStartChar, int32_t nEndChar) {
  m_pEditImpl->SetSelection(nStartChar, nEndChar);
}

std::pair<int32_t, int32_t> CPWL_Edit::GetSelection() const {
  return m_pEditImpl->GetSelection();
}

void CPWL_Edit::ClearSelection() {
  if (!IsReadOnly())
    m_pEditImpl->ClearSelection();
}

void CPWL_Edit::SetScrollPos(const CFX_PointF& point) {
  m_pEditImpl->SetScrollPos(point);
}

CFX_PointF CPWL_Edit::GetScrollPos() const {
  return m_pEditImpl->GetScrollPos();
}

void CPWL_Edit::CopyText() {}

void CPWL_Edit::PasteText() {}

void CPWL_Edit::InsertWord(uint16_t word, FX_Charset nCharset) {
  if (!IsReadOnly())
    m_pEditImpl->InsertWord(word, nCharset);
}

void CPWL_Edit::InsertReturn() {
  if (!IsReadOnly())
    m_pEditImpl->InsertReturn();
}

void CPWL_Edit::Delete() {
  if (!IsReadOnly())
    m_pEditImpl->Delete();
}

void CPWL_Edit::Backspace() {
  if (!IsReadOnly())
    m_pEditImpl->Backspace();
}

bool CPWL_Edit::CanUndo() {
  return !IsReadOnly() && m_pEditImpl->CanUndo();
}

bool CPWL_Edit::CanRedo() {
  return !IsReadOnly() && m_pEditImpl->CanRedo();
}

bool CPWL_Edit::Undo() {
  return CanUndo() && m_pEditImpl->Undo();
}

bool CPWL_Edit::Redo() {
  return CanRedo() && m_pEditImpl->Redo();
}

void CPWL_Edit::SetReadyToInput() {
  if (m_bMouseDown) {
    ReleaseCapture();
    m_bMouseDown = false;
  }
}
