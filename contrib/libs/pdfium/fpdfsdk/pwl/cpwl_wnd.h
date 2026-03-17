// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef FPDFSDK_PWL_CPWL_WND_H_
#define FPDFSDK_PWL_CPWL_WND_H_

#include <memory>
#include <vector>

#include "core/fxcrt/cfx_timer.h"
#include "core/fxcrt/mask.h"
#include "core/fxcrt/observed_ptr.h"
#include "core/fxcrt/unowned_ptr.h"
#include "core/fxcrt/unowned_ptr_exclusion.h"
#include "core/fxcrt/widestring.h"
#include "core/fxge/cfx_color.h"
#include "core/fxge/cfx_renderdevice.h"
#include "fpdfsdk/pwl/ipwl_fillernotify.h"
#include "public/fpdf_fwlevent.h"

class CPWL_Edit;
class CPWL_ScrollBar;
class IPVT_FontMap;
struct PWL_SCROLL_INFO;

// window styles
#define PWS_BORDER 0x40000000L
#define PWS_BACKGROUND 0x20000000L
#define PWS_VSCROLL 0x08000000L
#define PWS_VISIBLE 0x04000000L
#define PWS_READONLY 0x01000000L
#define PWS_AUTOFONTSIZE 0x00800000L
#define PWS_AUTOTRANSPARENT 0x00400000L
#define PWS_NOREFRESHCLIP 0x00200000L

// edit and label styles
#define PES_MULTILINE 0x0001L
#define PES_PASSWORD 0x0002L
#define PES_LEFT 0x0004L
#define PES_RIGHT 0x0008L
#define PES_MIDDLE 0x0010L
#define PES_TOP 0x0020L
#define PES_CENTER 0x0080L
#define PES_CHARARRAY 0x0100L
#define PES_AUTOSCROLL 0x0200L
#define PES_AUTORETURN 0x0400L
#define PES_UNDO 0x0800L
#define PES_RICH 0x1000L
#define PES_TEXTOVERFLOW 0x4000L

// listbox styles
#define PLBS_MULTIPLESEL 0x0001L
#define PLBS_HOVERSEL 0x0008L

// combobox styles
#define PCBS_ALLOWCUSTOMTEXT 0x0001L

struct CPWL_Dash {
  CPWL_Dash() : nDash(0), nGap(0), nPhase(0) {}
  CPWL_Dash(int32_t dash, int32_t gap, int32_t phase)
      : nDash(dash), nGap(gap), nPhase(phase) {}

  void Reset() {
    nDash = 0;
    nGap = 0;
    nPhase = 0;
  }

  int32_t nDash;
  int32_t nGap;
  int32_t nPhase;
};

class CPWL_Wnd : public Observable {
 public:
  static const CFX_Color kDefaultBlackColor;
  static const CFX_Color kDefaultWhiteColor;

  class SharedCaptureFocusState;

  class ProviderIface : public Observable {
   public:
    virtual ~ProviderIface() = default;

    // get a matrix which map user space to CWnd client space
    virtual CFX_Matrix GetWindowMatrix(
        const IPWL_FillerNotify::PerWindowData* pAttached) = 0;

    virtual void OnSetFocusForEdit(CPWL_Edit* pEdit) = 0;
  };

  // Caller-provided options for window creation.
  class CreateParams {
   public:
    CreateParams(CFX_Timer::HandlerIface* timer_handler,
                 IPWL_FillerNotify* filler_notify,
                 ProviderIface* provider);
    CreateParams(const CreateParams& other);
    ~CreateParams();

    // Required:
    CFX_FloatRect rcRectWnd;
    ObservedPtr<CFX_Timer::HandlerIface> const pTimerHandler;
    UnownedPtr<IPWL_FillerNotify> const pFillerNotify;
    UnownedPtr<IPVT_FontMap> pFontMap;
    ObservedPtr<ProviderIface> pProvider;

    // Optional:
    uint32_t dwFlags = 0;
    CFX_Color sBackgroundColor;
    BorderStyle nBorderStyle = BorderStyle::kSolid;
    int32_t dwBorderWidth = 1;
    CFX_Color sBorderColor;
    CFX_Color sTextColor;
    int32_t nTransparency = 255;
    float fFontSize;
    CPWL_Dash sDash;

    // Ignore, used internally only:
    // TODO(tsepez): fix murky ownership, bare delete.
    UNOWNED_PTR_EXCLUSION SharedCaptureFocusState* pSharedCaptureFocusState =
        nullptr;
    IPWL_FillerNotify::CursorStyle eCursorType =
        IPWL_FillerNotify::CursorStyle::kArrow;
  };

  static bool IsSHIFTKeyDown(Mask<FWL_EVENTFLAG> nFlag);
  static bool IsCTRLKeyDown(Mask<FWL_EVENTFLAG> nFlag);
  static bool IsALTKeyDown(Mask<FWL_EVENTFLAG> nFlag);
  static bool IsMETAKeyDown(Mask<FWL_EVENTFLAG> nFlag);

  // Selects between IsCTRLKeyDown() and IsMETAKeyDown() depending on platform.
  static bool IsPlatformShortcutKey(Mask<FWL_EVENTFLAG> nFlag);

  CPWL_Wnd(const CreateParams& cp,
           std::unique_ptr<IPWL_FillerNotify::PerWindowData> pAttachedData);
  virtual ~CPWL_Wnd();

  // Returns |true| iff this instance is still allocated.
  [[nodiscard]] virtual bool InvalidateRect(const CFX_FloatRect* pRect);

  virtual bool OnKeyDown(FWL_VKEYCODE nKeyCode, Mask<FWL_EVENTFLAG> nFlag);
  virtual bool OnChar(uint16_t nChar, Mask<FWL_EVENTFLAG> nFlag);
  virtual bool OnLButtonDblClk(Mask<FWL_EVENTFLAG> nFlag,
                               const CFX_PointF& point);
  virtual bool OnLButtonDown(Mask<FWL_EVENTFLAG> nFlag,
                             const CFX_PointF& point);
  virtual bool OnLButtonUp(Mask<FWL_EVENTFLAG> nFlag, const CFX_PointF& point);
  virtual bool OnRButtonDown(Mask<FWL_EVENTFLAG> nFlag,
                             const CFX_PointF& point);
  virtual bool OnRButtonUp(Mask<FWL_EVENTFLAG> nFlag, const CFX_PointF& point);
  virtual bool OnMouseMove(Mask<FWL_EVENTFLAG> nFlag, const CFX_PointF& point);
  virtual bool OnMouseWheel(Mask<FWL_EVENTFLAG> nFlag,
                            const CFX_PointF& point,
                            const CFX_Vector& delta);
  virtual void SetScrollInfo(const PWL_SCROLL_INFO& info);
  virtual void SetScrollPosition(float pos);
  virtual void ScrollWindowVertically(float pos);
  virtual void NotifyLButtonDown(CPWL_Wnd* child, const CFX_PointF& pos);
  virtual void NotifyLButtonUp(CPWL_Wnd* child, const CFX_PointF& pos);
  virtual void NotifyMouseMove(CPWL_Wnd* child, const CFX_PointF& pos);
  virtual void SetFocus();
  virtual void KillFocus();
  virtual void SetCursor();

  // Returns |true| iff this instance is still allocated.
  [[nodiscard]] virtual bool SetVisible(bool bVisible);
  virtual void SetFontSize(float fFontSize);
  virtual float GetFontSize() const;

  virtual WideString GetText();
  virtual WideString GetSelectedText();
  virtual void ReplaceAndKeepSelection(const WideString& text);
  virtual void ReplaceSelection(const WideString& text);
  virtual bool SelectAllText();

  virtual bool CanUndo();
  virtual bool CanRedo();
  virtual bool Undo();
  virtual bool Redo();

  virtual CFX_FloatRect GetFocusRect() const;
  virtual CFX_FloatRect GetClientRect() const;

  virtual void OnSetFocus();
  virtual void OnKillFocus();

  void AddChild(std::unique_ptr<CPWL_Wnd> pWnd);
  void RemoveChild(CPWL_Wnd* pWnd);
  void Realize();
  void Destroy();
  bool Move(const CFX_FloatRect& rcNew, bool bReset, bool bRefresh);

  void InvalidateProvider(ProviderIface* provider);
  void DrawAppearance(CFX_RenderDevice* pDevice,
                      const CFX_Matrix& mtUser2Device);

  int32_t GetBorderWidth() const;
  CFX_FloatRect GetWindowRect() const;

  bool IsVisible() const { return m_bVisible; }
  bool HasFlag(uint32_t dwFlags) const;
  void RemoveFlag(uint32_t dwFlags);
  void SetClipRect(const CFX_FloatRect& rect);

  IPWL_FillerNotify::PerWindowData* GetAttachedData() const {
    return m_pAttachedData.get();
  }
  std::unique_ptr<IPWL_FillerNotify::PerWindowData> CloneAttachedData() const;
  std::vector<UnownedPtr<CPWL_Wnd>> GetAncestors();

  bool WndHitTest(const CFX_PointF& point) const;
  bool ClientHitTest(const CFX_PointF& point) const;
  bool IsCaptureMouse() const;

  bool IsFocused() const;
  bool IsReadOnly() const;

  void SetTransparency(int32_t nTransparency);
  CFX_Matrix GetWindowMatrix() const;

 protected:
  virtual void CreateChildWnd(const CreateParams& cp);

  // Returns |true| iff this instance is still allocated.
  [[nodiscard]] virtual bool RepositionChildWnd();

  virtual void DrawThisAppearance(CFX_RenderDevice* pDevice,
                                  const CFX_Matrix& mtUser2Device);

  virtual void OnCreated();
  virtual void OnDestroy();

  bool IsValid() const { return m_bCreated; }
  CreateParams* GetCreationParams() { return &m_CreationParams; }
  ProviderIface* GetProvider() const {
    return m_CreationParams.pProvider.Get();
  }
  CFX_Timer::HandlerIface* GetTimerHandler() const {
    return m_CreationParams.pTimerHandler.Get();
  }
  IPWL_FillerNotify* GetFillerNotify() const {
    return m_CreationParams.pFillerNotify;
  }

  CPWL_Wnd* GetParentWindow() const { return m_pParent; }
  CPWL_ScrollBar* GetVScrollBar() const;

  // Returns |true| iff this instance is still allocated.
  [[nodiscard]] bool InvalidateRectMove(const CFX_FloatRect& rcOld,
                                        const CFX_FloatRect& rcNew);

  void SetCapture();
  void ReleaseCapture();
  bool IsWndCaptureMouse(const CPWL_Wnd* pWnd) const;
  bool IsWndCaptureKeyboard(const CPWL_Wnd* pWnd) const;

  CFX_Color GetBackgroundColor() const;
  CFX_Color GetBorderColor() const;
  CFX_Color GetTextColor() const;
  CFX_Color GetBorderLeftTopColor(BorderStyle nBorderStyle) const;
  CFX_Color GetBorderRightBottomColor(BorderStyle nBorderStyle) const;
  BorderStyle GetBorderStyle() const;
  const CPWL_Dash& GetBorderDash() const;

  int32_t GetTransparency();
  int32_t GetInnerBorderWidth() const;
  CFX_PointF GetCenterPoint() const;
  const CFX_FloatRect& GetClipRect() const;

  IPVT_FontMap* GetFontMap() const { return m_CreationParams.pFontMap; }

 private:
  void DrawChildAppearance(CFX_RenderDevice* pDevice,
                           const CFX_Matrix& mtUser2Device);

  CFX_FloatRect PWLtoWnd(const CFX_FloatRect& rect) const;

  void CreateVScrollBar(const CreateParams& cp);

  void AdjustStyle();
  void CreateSharedCaptureFocusState();
  void DestroySharedCaptureFocusState();
  SharedCaptureFocusState* GetSharedCaptureFocusState() const;

  CreateParams m_CreationParams;
  std::unique_ptr<IPWL_FillerNotify::PerWindowData> m_pAttachedData;
  UnownedPtr<CPWL_Wnd> m_pParent;
  std::vector<std::unique_ptr<CPWL_Wnd>> m_Children;
  UnownedPtr<CPWL_ScrollBar> m_pVScrollBar;
  CFX_FloatRect m_rcWindow;
  CFX_FloatRect m_rcClip;
  bool m_bCreated = false;
  bool m_bVisible = false;
};

#endif  // FPDFSDK_PWL_CPWL_WND_H_
