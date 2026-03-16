// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "fpdfsdk/pwl/cpwl_wnd.h"

#include <sstream>
#include <utility>
#include <vector>

#include "build/build_config.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/check_op.h"
#include "core/fxcrt/containers/contains.h"
#include "core/fxcrt/stl_util.h"
#include "core/fxge/cfx_renderdevice.h"
#include "fpdfsdk/pwl/cpwl_scroll_bar.h"
#include "public/fpdf_fwlevent.h"

namespace {

constexpr float kDefaultFontSize = 9.0f;

}  // namespace

// static
const CFX_Color CPWL_Wnd::kDefaultBlackColor =
    CFX_Color(CFX_Color::Type::kGray, 0);

// static
const CFX_Color CPWL_Wnd::kDefaultWhiteColor =
    CFX_Color(CFX_Color::Type::kGray, 1);

CPWL_Wnd::CreateParams::CreateParams(CFX_Timer::HandlerIface* timer_handler,
                                     IPWL_FillerNotify* filler_notify,
                                     ProviderIface* provider)
    : pTimerHandler(timer_handler),
      pFillerNotify(filler_notify),
      pProvider(provider),
      fFontSize(kDefaultFontSize),
      sDash(3, 0, 0) {}

CPWL_Wnd::CreateParams::CreateParams(const CreateParams& other) = default;

CPWL_Wnd::CreateParams::~CreateParams() = default;

// For a compound window (a window containing a child window as occurs in a
// list box, combo box, or even a scroll bar), this class contains information
// shared amongst the parent and children.
class CPWL_Wnd::SharedCaptureFocusState final : public Observable {
 public:
  explicit SharedCaptureFocusState(const CPWL_Wnd* pOwnerWnd)
      : m_pOwnerWnd(pOwnerWnd) {}
  ~SharedCaptureFocusState() = default;

  bool IsOwnedByWnd(const CPWL_Wnd* pWnd) const { return m_pOwnerWnd == pWnd; }

  bool IsWndCaptureMouse(const CPWL_Wnd* pWnd) const {
    return pWnd && pdfium::Contains(m_MousePaths, pWnd);
  }

  bool IsMainCaptureKeyboard(const CPWL_Wnd* pWnd) const {
    return pWnd == m_pMainKeyboardWnd;
  }

  bool IsWndCaptureKeyboard(const CPWL_Wnd* pWnd) const {
    return pWnd && pdfium::Contains(m_KeyboardPaths, pWnd);
  }

  void SetCapture(CPWL_Wnd* pWnd) { m_MousePaths = pWnd->GetAncestors(); }
  void ReleaseCapture() { m_MousePaths.clear(); }

  void SetFocus(CPWL_Wnd* pWnd) {
    m_KeyboardPaths = pWnd->GetAncestors();
    m_pMainKeyboardWnd = pWnd;

    // Note, pWnd may get destroyed in the OnSetFocus call.
    pWnd->OnSetFocus();
  }

  void ReleaseFocus() {
    ObservedPtr<SharedCaptureFocusState> this_observed(this);
    if (!this_observed->m_KeyboardPaths.empty()) {
      CPWL_Wnd* pWnd = this_observed->m_KeyboardPaths.front();
      if (pWnd)
        pWnd->OnKillFocus();
    }
    if (!this_observed) {
      return;
    }
    this_observed->m_pMainKeyboardWnd = nullptr;
    this_observed->m_KeyboardPaths.clear();
  }

  void RemoveWnd(CPWL_Wnd* pWnd) {
    if (pWnd == m_pOwnerWnd) {
      m_pOwnerWnd = nullptr;
    }
    if (pWnd == m_pMainKeyboardWnd) {
      m_pMainKeyboardWnd = nullptr;
    }
    auto mouse_it = std::find(m_MousePaths.begin(), m_MousePaths.end(), pWnd);
    if (mouse_it != m_MousePaths.end()) {
      m_MousePaths.erase(mouse_it);
    }
    auto keyboard_it =
        std::find(m_KeyboardPaths.begin(), m_KeyboardPaths.end(), pWnd);
    if (keyboard_it != m_KeyboardPaths.end()) {
      m_KeyboardPaths.erase(keyboard_it);
    }
  }

 private:
  UnownedPtr<const CPWL_Wnd> m_pOwnerWnd;
  UnownedPtr<const CPWL_Wnd> m_pMainKeyboardWnd;
  std::vector<UnownedPtr<CPWL_Wnd>> m_MousePaths;
  std::vector<UnownedPtr<CPWL_Wnd>> m_KeyboardPaths;
};

// static
bool CPWL_Wnd::IsSHIFTKeyDown(Mask<FWL_EVENTFLAG> nFlag) {
  return !!(nFlag & FWL_EVENTFLAG_ShiftKey);
}

// static
bool CPWL_Wnd::IsCTRLKeyDown(Mask<FWL_EVENTFLAG> nFlag) {
  return !!(nFlag & FWL_EVENTFLAG_ControlKey);
}

// static
bool CPWL_Wnd::IsALTKeyDown(Mask<FWL_EVENTFLAG> nFlag) {
  return !!(nFlag & FWL_EVENTFLAG_AltKey);
}

// static
bool CPWL_Wnd::IsMETAKeyDown(Mask<FWL_EVENTFLAG> nFlag) {
  return !!(nFlag & FWL_EVENTFLAG_MetaKey);
}

// static
bool CPWL_Wnd::IsPlatformShortcutKey(Mask<FWL_EVENTFLAG> nFlag) {
#if BUILDFLAG(IS_APPLE)
  return IsMETAKeyDown(nFlag);
#else
  return IsCTRLKeyDown(nFlag);
#endif
}

CPWL_Wnd::CPWL_Wnd(
    const CreateParams& cp,
    std::unique_ptr<IPWL_FillerNotify::PerWindowData> pAttachedData)
    : m_CreationParams(cp), m_pAttachedData(std::move(pAttachedData)) {}

CPWL_Wnd::~CPWL_Wnd() {
  DCHECK(!m_bCreated);
}

void CPWL_Wnd::Realize() {
  DCHECK(!m_bCreated);

  m_CreationParams.rcRectWnd.Normalize();
  m_rcWindow = m_CreationParams.rcRectWnd;
  m_rcClip = m_rcWindow;
  if (!m_rcClip.IsEmpty()) {
    m_rcClip.Inflate(1.0f, 1.0f);
    m_rcClip.Normalize();
  }
  CreateSharedCaptureFocusState();

  CreateParams ccp = m_CreationParams;
  ccp.dwFlags &= 0xFFFF0000L;  // remove sub styles
  CreateVScrollBar(ccp);
  CreateChildWnd(ccp);
  m_bVisible = HasFlag(PWS_VISIBLE);
  OnCreated();
  if (!RepositionChildWnd()) {
    return;
  }

  m_bCreated = true;
}

void CPWL_Wnd::OnCreated() {}

void CPWL_Wnd::OnDestroy() {}

void CPWL_Wnd::InvalidateProvider(ProviderIface* provider) {
  if (m_CreationParams.pProvider.Get() == provider)
    m_CreationParams.pProvider.Reset();
}

void CPWL_Wnd::Destroy() {
  KillFocus();
  OnDestroy();
  if (m_bCreated) {
    m_pVScrollBar = nullptr;
    while (!m_Children.empty()) {
      std::unique_ptr<CPWL_Wnd> pChild = std::move(m_Children.back());
      m_Children.pop_back();
      pChild->Destroy();
    }
    if (m_pParent)
      m_pParent->RemoveChild(this);
    m_bCreated = false;
  }
  DestroySharedCaptureFocusState();
}

bool CPWL_Wnd::Move(const CFX_FloatRect& rcNew, bool bReset, bool bRefresh) {
  if (!IsValid())
    return true;

  CFX_FloatRect rcOld = GetWindowRect();
  m_rcWindow = rcNew;
  m_rcWindow.Normalize();

  if (bReset) {
    if (rcOld.left != rcNew.left || rcOld.right != rcNew.right ||
        rcOld.top != rcNew.top || rcOld.bottom != rcNew.bottom) {
      if (!RepositionChildWnd()) {
        return false;
      }
    }
  }
  if (bRefresh && !InvalidateRectMove(rcOld, rcNew))
    return false;

  m_CreationParams.rcRectWnd = m_rcWindow;
  return true;
}

bool CPWL_Wnd::InvalidateRectMove(const CFX_FloatRect& rcOld,
                                  const CFX_FloatRect& rcNew) {
  CFX_FloatRect rcUnion = rcOld;
  rcUnion.Union(rcNew);

  return InvalidateRect(&rcUnion);
}

void CPWL_Wnd::DrawAppearance(CFX_RenderDevice* pDevice,
                              const CFX_Matrix& mtUser2Device) {
  if (IsValid() && IsVisible()) {
    DrawThisAppearance(pDevice, mtUser2Device);
    DrawChildAppearance(pDevice, mtUser2Device);
  }
}

void CPWL_Wnd::DrawThisAppearance(CFX_RenderDevice* pDevice,
                                  const CFX_Matrix& mtUser2Device) {
  CFX_FloatRect rectWnd = GetWindowRect();
  if (rectWnd.IsEmpty())
    return;

  if (HasFlag(PWS_BACKGROUND)) {
    float width = static_cast<float>(GetBorderWidth() + GetInnerBorderWidth());
    pDevice->DrawFillRect(&mtUser2Device, rectWnd.GetDeflated(width, width),
                          GetBackgroundColor(), GetTransparency());
  }

  if (HasFlag(PWS_BORDER)) {
    pDevice->DrawBorder(&mtUser2Device, rectWnd,
                        static_cast<float>(GetBorderWidth()), GetBorderColor(),
                        GetBorderLeftTopColor(GetBorderStyle()),
                        GetBorderRightBottomColor(GetBorderStyle()),
                        GetBorderStyle(), GetTransparency());
  }
}

void CPWL_Wnd::DrawChildAppearance(CFX_RenderDevice* pDevice,
                                   const CFX_Matrix& mtUser2Device) {
  for (const auto& pChild : m_Children) {
    pChild->DrawAppearance(pDevice, mtUser2Device);
  }
}

bool CPWL_Wnd::InvalidateRect(const CFX_FloatRect* pRect) {
  ObservedPtr<CPWL_Wnd> this_observed(this);
  if (!this_observed->IsValid()) {
    return true;
  }
  CFX_FloatRect rcRefresh = pRect ? *pRect : this_observed->GetWindowRect();
  if (!this_observed->HasFlag(PWS_NOREFRESHCLIP)) {
    CFX_FloatRect rcClip = this_observed->GetClipRect();
    if (!rcClip.IsEmpty())
      rcRefresh.Intersect(rcClip);
  }

  CFX_FloatRect rcWin = this_observed->PWLtoWnd(rcRefresh);
  rcWin.Inflate(1, 1);
  rcWin.Normalize();
  this_observed->GetFillerNotify()->InvalidateRect(
      this_observed->m_pAttachedData.get(), rcWin);
  return !!this_observed;
}

bool CPWL_Wnd::OnKeyDown(FWL_VKEYCODE nKeyCode, Mask<FWL_EVENTFLAG> nFlag) {
  if (!IsValid() || !IsVisible())
    return false;
  if (!IsWndCaptureKeyboard(this))
    return false;
  for (const auto& pChild : m_Children) {
    if (IsWndCaptureKeyboard(pChild.get()))
      return pChild->OnKeyDown(nKeyCode, nFlag);
  }
  return false;
}

bool CPWL_Wnd::OnChar(uint16_t nChar, Mask<FWL_EVENTFLAG> nFlag) {
  if (!IsValid() || !IsVisible())
    return false;
  if (!IsWndCaptureKeyboard(this))
    return false;
  for (const auto& pChild : m_Children) {
    if (IsWndCaptureKeyboard(pChild.get()))
      return pChild->OnChar(nChar, nFlag);
  }
  return false;
}

#define PWL_IMPLEMENT_MOUSE_METHOD(mouse_method_name)         \
  bool CPWL_Wnd::mouse_method_name(Mask<FWL_EVENTFLAG> nFlag, \
                                   const CFX_PointF& point) { \
    if (!IsValid() || !IsVisible())                           \
      return false;                                           \
    if (IsWndCaptureMouse(this)) {                            \
      for (const auto& pChild : m_Children) {                 \
        if (IsWndCaptureMouse(pChild.get())) {                \
          return pChild->mouse_method_name(nFlag, point);     \
        }                                                     \
      }                                                       \
      SetCursor();                                            \
      return false;                                           \
    }                                                         \
    for (const auto& pChild : m_Children) {                   \
      if (pChild->WndHitTest(point)) {                        \
        return pChild->mouse_method_name(nFlag, point);       \
      }                                                       \
    }                                                         \
    if (WndHitTest(point))                                    \
      SetCursor();                                            \
    return false;                                             \
  }

PWL_IMPLEMENT_MOUSE_METHOD(OnLButtonDblClk)
PWL_IMPLEMENT_MOUSE_METHOD(OnLButtonDown)
PWL_IMPLEMENT_MOUSE_METHOD(OnLButtonUp)
PWL_IMPLEMENT_MOUSE_METHOD(OnMouseMove)
#undef PWL_IMPLEMENT_MOUSE_METHOD

// Unlike their FWL counterparts, PWL windows don't handle right clicks.
bool CPWL_Wnd::OnRButtonDown(Mask<FWL_EVENTFLAG> nFlag,
                             const CFX_PointF& point) {
  return false;
}

bool CPWL_Wnd::OnRButtonUp(Mask<FWL_EVENTFLAG> nFlag, const CFX_PointF& point) {
  return false;
}

WideString CPWL_Wnd::GetText() {
  return WideString();
}

WideString CPWL_Wnd::GetSelectedText() {
  return WideString();
}

void CPWL_Wnd::ReplaceAndKeepSelection(const WideString& text) {}

void CPWL_Wnd::ReplaceSelection(const WideString& text) {}

bool CPWL_Wnd::SelectAllText() {
  return false;
}

bool CPWL_Wnd::CanUndo() {
  return false;
}

bool CPWL_Wnd::CanRedo() {
  return false;
}

bool CPWL_Wnd::Undo() {
  return false;
}

bool CPWL_Wnd::Redo() {
  return false;
}

bool CPWL_Wnd::OnMouseWheel(Mask<FWL_EVENTFLAG> nFlag,
                            const CFX_PointF& point,
                            const CFX_Vector& delta) {
  if (!IsValid() || !IsVisible())
    return false;

  SetCursor();
  if (!IsWndCaptureKeyboard(this))
    return false;

  for (const auto& pChild : m_Children) {
    if (IsWndCaptureKeyboard(pChild.get()))
      return pChild->OnMouseWheel(nFlag, point, delta);
  }
  return false;
}

void CPWL_Wnd::AddChild(std::unique_ptr<CPWL_Wnd> pWnd) {
  DCHECK(!pWnd->m_pParent);
  pWnd->m_pParent = this;
  m_Children.push_back(std::move(pWnd));
}

void CPWL_Wnd::RemoveChild(CPWL_Wnd* pWnd) {
  DCHECK_EQ(pWnd->m_pParent, this);
  auto it =
      std::find(m_Children.begin(), m_Children.end(), MakeFakeUniquePtr(pWnd));
  if (it == m_Children.end())
    return;

  // TODO(tsepez): murky ownership.
  it->release();
  m_Children.erase(it);
}

void CPWL_Wnd::SetScrollInfo(const PWL_SCROLL_INFO& info) {}

void CPWL_Wnd::SetScrollPosition(float pos) {}

void CPWL_Wnd::ScrollWindowVertically(float pos) {}

void CPWL_Wnd::NotifyLButtonDown(CPWL_Wnd* child, const CFX_PointF& pos) {}

void CPWL_Wnd::NotifyLButtonUp(CPWL_Wnd* child, const CFX_PointF& pos) {}

void CPWL_Wnd::NotifyMouseMove(CPWL_Wnd* child, const CFX_PointF& pos) {}

CFX_FloatRect CPWL_Wnd::GetWindowRect() const {
  return m_rcWindow;
}

CFX_FloatRect CPWL_Wnd::GetClientRect() const {
  CFX_FloatRect rcWindow = GetWindowRect();

  float width = static_cast<float>(GetBorderWidth() + GetInnerBorderWidth());
  CFX_FloatRect rcClient = rcWindow.GetDeflated(width, width);
  if (CPWL_ScrollBar* pVSB = GetVScrollBar())
    rcClient.right -= pVSB->GetScrollBarWidth();

  rcClient.Normalize();
  return rcWindow.Contains(rcClient) ? rcClient : CFX_FloatRect();
}

CFX_PointF CPWL_Wnd::GetCenterPoint() const {
  CFX_FloatRect rcClient = GetClientRect();
  return CFX_PointF((rcClient.left + rcClient.right) * 0.5f,
                    (rcClient.top + rcClient.bottom) * 0.5f);
}

bool CPWL_Wnd::HasFlag(uint32_t dwFlags) const {
  return (m_CreationParams.dwFlags & dwFlags) != 0;
}

void CPWL_Wnd::RemoveFlag(uint32_t dwFlags) {
  m_CreationParams.dwFlags &= ~dwFlags;
}

CFX_Color CPWL_Wnd::GetBackgroundColor() const {
  return m_CreationParams.sBackgroundColor;
}

CFX_Color CPWL_Wnd::GetTextColor() const {
  return m_CreationParams.sTextColor;
}

BorderStyle CPWL_Wnd::GetBorderStyle() const {
  return m_CreationParams.nBorderStyle;
}

int32_t CPWL_Wnd::GetBorderWidth() const {
  return HasFlag(PWS_BORDER) ? m_CreationParams.dwBorderWidth : 0;
}

int32_t CPWL_Wnd::GetInnerBorderWidth() const {
  return 0;
}

CFX_Color CPWL_Wnd::GetBorderColor() const {
  return HasFlag(PWS_BORDER) ? m_CreationParams.sBorderColor : CFX_Color();
}

const CPWL_Dash& CPWL_Wnd::GetBorderDash() const {
  return m_CreationParams.sDash;
}

CPWL_ScrollBar* CPWL_Wnd::GetVScrollBar() const {
  return HasFlag(PWS_VSCROLL) ? m_pVScrollBar : nullptr;
}

void CPWL_Wnd::CreateVScrollBar(const CreateParams& cp) {
  if (m_pVScrollBar || !HasFlag(PWS_VSCROLL))
    return;

  CreateParams scp = cp;
  scp.dwFlags = PWS_BACKGROUND | PWS_AUTOTRANSPARENT | PWS_NOREFRESHCLIP;
  scp.sBackgroundColor = kDefaultWhiteColor;
  scp.eCursorType = IPWL_FillerNotify::CursorStyle::kArrow;
  scp.nTransparency = CPWL_ScrollBar::kTransparency;

  auto pBar = std::make_unique<CPWL_ScrollBar>(scp, CloneAttachedData());
  m_pVScrollBar = pBar.get();
  AddChild(std::move(pBar));
  m_pVScrollBar->Realize();
}

void CPWL_Wnd::SetCapture() {
  if (SharedCaptureFocusState* pSharedState = GetSharedCaptureFocusState()) {
    pSharedState->SetCapture(this);
  }
}

void CPWL_Wnd::ReleaseCapture() {
  for (const auto& pChild : m_Children)
    pChild->ReleaseCapture();

  if (SharedCaptureFocusState* pSharedState = GetSharedCaptureFocusState()) {
    pSharedState->ReleaseCapture();
  }
}

void CPWL_Wnd::SetFocus() {
  if (SharedCaptureFocusState* pSharedState = GetSharedCaptureFocusState()) {
    if (!pSharedState->IsMainCaptureKeyboard(this)) {
      pSharedState->ReleaseFocus();
    }
    pSharedState->SetFocus(this);
  }
}

void CPWL_Wnd::KillFocus() {
  if (SharedCaptureFocusState* pSharedState = GetSharedCaptureFocusState()) {
    if (pSharedState->IsWndCaptureKeyboard(this)) {
      pSharedState->ReleaseFocus();
    }
  }
}

void CPWL_Wnd::OnSetFocus() {}

void CPWL_Wnd::OnKillFocus() {}

std::unique_ptr<IPWL_FillerNotify::PerWindowData> CPWL_Wnd::CloneAttachedData()
    const {
  return m_pAttachedData ? m_pAttachedData->Clone() : nullptr;
}

std::vector<UnownedPtr<CPWL_Wnd>> CPWL_Wnd::GetAncestors() {
  std::vector<UnownedPtr<CPWL_Wnd>> results;
  for (CPWL_Wnd* pWnd = this; pWnd; pWnd = pWnd->GetParentWindow()) {
    results.emplace_back(pWnd);
  }
  return results;
}

bool CPWL_Wnd::WndHitTest(const CFX_PointF& point) const {
  return IsValid() && IsVisible() && GetWindowRect().Contains(point);
}

bool CPWL_Wnd::ClientHitTest(const CFX_PointF& point) const {
  return IsValid() && IsVisible() && GetClientRect().Contains(point);
}

bool CPWL_Wnd::SetVisible(bool bVisible) {
  ObservedPtr<CPWL_Wnd> this_observed(this);
  if (!this_observed->IsValid()) {
    return true;
  }
  for (const auto& pChild : this_observed->m_Children) {
    if (!pChild->SetVisible(bVisible)) {
      return false;
    }
    if (!this_observed) {
      return false;
    }
  }
  if (bVisible != this_observed->m_bVisible) {
    this_observed->m_bVisible = bVisible;
    if (!this_observed->RepositionChildWnd()) {
      return false;
    }
    if (!this_observed->InvalidateRect(nullptr)) {
      return false;
    }
  }
  return true;
}

void CPWL_Wnd::SetClipRect(const CFX_FloatRect& rect) {
  m_rcClip = rect;
  m_rcClip.Normalize();
}

const CFX_FloatRect& CPWL_Wnd::GetClipRect() const {
  return m_rcClip;
}

bool CPWL_Wnd::IsReadOnly() const {
  return HasFlag(PWS_READONLY);
}

bool CPWL_Wnd::RepositionChildWnd() {
  ObservedPtr<CPWL_Wnd> this_observed(this);
  CPWL_ScrollBar* pVSB = this_observed->GetVScrollBar();
  if (!pVSB) {
    return true;
  }
  CFX_FloatRect rcContent = this_observed->GetWindowRect();
  if (!rcContent.IsEmpty()) {
    float width = static_cast<float>(this_observed->GetBorderWidth() +
                                     this_observed->GetInnerBorderWidth());
    rcContent.Deflate(width, width);
    rcContent.Normalize();
  }
  CFX_FloatRect rcVScroll =
      CFX_FloatRect(rcContent.right - CPWL_ScrollBar::kWidth, rcContent.bottom,
                    rcContent.right - 1.0f, rcContent.top);

  pVSB->Move(rcVScroll, true, false);
  return !!this_observed;
}

void CPWL_Wnd::CreateChildWnd(const CreateParams& cp) {}

void CPWL_Wnd::SetCursor() {
  if (IsValid())
    GetFillerNotify()->SetCursor(GetCreationParams()->eCursorType);
}

void CPWL_Wnd::CreateSharedCaptureFocusState() {
  if (!m_CreationParams.pSharedCaptureFocusState) {
    m_CreationParams.pSharedCaptureFocusState =
        new SharedCaptureFocusState(this);
  }
}

void CPWL_Wnd::DestroySharedCaptureFocusState() {
  SharedCaptureFocusState* pSharedCaptureFocusState =
      GetSharedCaptureFocusState();
  if (!pSharedCaptureFocusState) {
    return;
  }
  const bool owned = pSharedCaptureFocusState->IsOwnedByWnd(this);
  pSharedCaptureFocusState->RemoveWnd(this);
  if (owned) {
    delete pSharedCaptureFocusState;
  }
}

CPWL_Wnd::SharedCaptureFocusState* CPWL_Wnd::GetSharedCaptureFocusState()
    const {
  return m_CreationParams.pSharedCaptureFocusState;
}

bool CPWL_Wnd::IsCaptureMouse() const {
  return IsWndCaptureMouse(this);
}

bool CPWL_Wnd::IsWndCaptureMouse(const CPWL_Wnd* pWnd) const {
  SharedCaptureFocusState* pCtrl = GetSharedCaptureFocusState();
  return pCtrl && pCtrl->IsWndCaptureMouse(pWnd);
}

bool CPWL_Wnd::IsWndCaptureKeyboard(const CPWL_Wnd* pWnd) const {
  SharedCaptureFocusState* pCtrl = GetSharedCaptureFocusState();
  return pCtrl && pCtrl->IsWndCaptureKeyboard(pWnd);
}

bool CPWL_Wnd::IsFocused() const {
  SharedCaptureFocusState* pCtrl = GetSharedCaptureFocusState();
  return pCtrl && pCtrl->IsMainCaptureKeyboard(this);
}

CFX_FloatRect CPWL_Wnd::GetFocusRect() const {
  CFX_FloatRect rect = GetWindowRect();
  if (!rect.IsEmpty()) {
    rect.Inflate(1.0f, 1.0f);
    rect.Normalize();
  }
  return rect;
}

float CPWL_Wnd::GetFontSize() const {
  return m_CreationParams.fFontSize;
}

void CPWL_Wnd::SetFontSize(float fFontSize) {
  m_CreationParams.fFontSize = fFontSize;
}

CFX_Color CPWL_Wnd::GetBorderLeftTopColor(BorderStyle nBorderStyle) const {
  switch (nBorderStyle) {
    case BorderStyle::kBeveled:
      return CFX_Color(CFX_Color::Type::kGray, 1);
    case BorderStyle::kInset:
      return CFX_Color(CFX_Color::Type::kGray, 0.5f);
    default:
      return CFX_Color();
  }
}

CFX_Color CPWL_Wnd::GetBorderRightBottomColor(BorderStyle nBorderStyle) const {
  switch (nBorderStyle) {
    case BorderStyle::kBeveled:
      return GetBackgroundColor() / 2.0f;
    case BorderStyle::kInset:
      return CFX_Color(CFX_Color::Type::kGray, 0.75f);
    default:
      return CFX_Color();
  }
}

int32_t CPWL_Wnd::GetTransparency() {
  return m_CreationParams.nTransparency;
}

void CPWL_Wnd::SetTransparency(int32_t nTransparency) {
  for (const auto& pChild : m_Children)
    pChild->SetTransparency(nTransparency);

  m_CreationParams.nTransparency = nTransparency;
}

CFX_Matrix CPWL_Wnd::GetWindowMatrix() const {
  CFX_Matrix mt;
  if (ProviderIface* pProvider = GetProvider())
    mt.Concat(pProvider->GetWindowMatrix(GetAttachedData()));
  return mt;
}

CFX_FloatRect CPWL_Wnd::PWLtoWnd(const CFX_FloatRect& rect) const {
  CFX_Matrix mt = GetWindowMatrix();
  return mt.TransformRect(rect);
}
