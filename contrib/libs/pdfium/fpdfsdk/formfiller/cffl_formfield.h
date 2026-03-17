// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef FPDFSDK_FORMFILLER_CFFL_FORMFIELD_H_
#define FPDFSDK_FORMFILLER_CFFL_FORMFIELD_H_

#include <map>
#include <memory>

#include "core/fpdfdoc/cpdf_aaction.h"
#include "core/fxcrt/cfx_timer.h"
#include "core/fxcrt/mask.h"
#include "core/fxcrt/unowned_ptr.h"
#include "fpdfsdk/formfiller/cffl_fieldaction.h"
#include "fpdfsdk/formfiller/cffl_interactiveformfiller.h"
#include "fpdfsdk/pwl/cpwl_wnd.h"
#include "fpdfsdk/pwl/ipwl_fillernotify.h"

class CFFL_PerWindowData;
class CPDFSDK_PageView;
class CPDFSDK_Widget;

class CFFL_FormField : public CPWL_Wnd::ProviderIface,
                       public CFX_Timer::CallbackIface {
 public:
  CFFL_FormField(CFFL_InteractiveFormFiller* pFormFiller,
                 CPDFSDK_Widget* pWidget);
  ~CFFL_FormField() override;

  virtual void OnDraw(CPDFSDK_PageView* pPageView,
                      CPDFSDK_Widget* pWidget,
                      CFX_RenderDevice* pDevice,
                      const CFX_Matrix& mtUser2Device);
  virtual void OnDrawDeactive(CPDFSDK_PageView* pPageView,
                              CPDFSDK_Widget* pWidget,
                              CFX_RenderDevice* pDevice,
                              const CFX_Matrix& mtUser2Device);

  virtual void OnMouseEnter(CPDFSDK_PageView* pPageView);
  virtual void OnMouseExit(CPDFSDK_PageView* pPageView);

  virtual bool OnLButtonDown(CPDFSDK_PageView* pPageView,
                             CPDFSDK_Widget* pAnnot,
                             Mask<FWL_EVENTFLAG> nFlags,
                             const CFX_PointF& point);
  virtual bool OnLButtonUp(CPDFSDK_PageView* pPageView,
                           CPDFSDK_Widget* pAnnot,
                           Mask<FWL_EVENTFLAG> nFlags,
                           const CFX_PointF& point);
  virtual bool OnLButtonDblClk(CPDFSDK_PageView* pPageView,
                               Mask<FWL_EVENTFLAG> nFlags,
                               const CFX_PointF& point);
  virtual bool OnMouseMove(CPDFSDK_PageView* pPageView,
                           Mask<FWL_EVENTFLAG> nFlags,
                           const CFX_PointF& point);
  virtual bool OnMouseWheel(CPDFSDK_PageView* pPageView,
                            Mask<FWL_EVENTFLAG> nFlags,
                            const CFX_PointF& point,
                            const CFX_Vector& delta);
  virtual bool OnRButtonDown(CPDFSDK_PageView* pPageView,
                             Mask<FWL_EVENTFLAG> nFlags,
                             const CFX_PointF& point);
  virtual bool OnRButtonUp(CPDFSDK_PageView* pPageView,
                           Mask<FWL_EVENTFLAG> nFlags,
                           const CFX_PointF& point);

  virtual bool OnKeyDown(FWL_VKEYCODE nKeyCode, Mask<FWL_EVENTFLAG> nFlags);
  virtual bool OnChar(CPDFSDK_Widget* pAnnot,
                      uint32_t nChar,
                      Mask<FWL_EVENTFLAG> nFlags);
  virtual bool SetIndexSelected(int index, bool selected);
  virtual bool IsIndexSelected(int index);

  FX_RECT GetViewBBox(const CPDFSDK_PageView* pPageView);

  WideString GetText();
  WideString GetSelectedText();
  void ReplaceAndKeepSelection(const WideString& text);
  void ReplaceSelection(const WideString& text);
  bool SelectAllText();

  bool CanUndo();
  bool CanRedo();
  bool Undo();
  bool Redo();

  void SetFocusForAnnot(CPDFSDK_Widget* pWidget, Mask<FWL_EVENTFLAG> nFlag);
  void KillFocusForAnnot(Mask<FWL_EVENTFLAG> nFlag);

  // CFX_Timer::CallbackIface:
  void OnTimerFired() override;

  // CPWL_Wnd::ProviderIface:
  CFX_Matrix GetWindowMatrix(
      const IPWL_FillerNotify::PerWindowData* pAttached) override;
  void OnSetFocusForEdit(CPWL_Edit* pEdit) override;

  virtual void GetActionData(const CPDFSDK_PageView* pPageView,
                             CPDF_AAction::AActionType type,
                             CFFL_FieldAction& fa);
  virtual void SetActionData(const CPDFSDK_PageView* pPageView,
                             CPDF_AAction::AActionType type,
                             const CFFL_FieldAction& fa);
  virtual CPWL_Wnd::CreateParams GetCreateParam();
  virtual std::unique_ptr<CPWL_Wnd> NewPWLWindow(
      const CPWL_Wnd::CreateParams& cp,
      std::unique_ptr<IPWL_FillerNotify::PerWindowData> pAttachedData) = 0;
  virtual void SavePWLWindowState(const CPDFSDK_PageView* pPageView);
  virtual void RecreatePWLWindowFromSavedState(
      const CPDFSDK_PageView* pPageView);
  virtual bool IsDataChanged(const CPDFSDK_PageView* pPageView);
  virtual void SaveData(const CPDFSDK_PageView* pPageView);
#ifdef PDF_ENABLE_XFA
  virtual bool IsFieldFull(const CPDFSDK_PageView* pPageView);
#endif  // PDF_ENABLE_XFA

  CFX_Matrix GetCurMatrix();
  CFX_FloatRect GetFocusBox(const CPDFSDK_PageView* pPageView);
  CFX_FloatRect FFLtoPWL(const CFX_FloatRect& rect);
  CFX_FloatRect PWLtoFFL(const CFX_FloatRect& rect);
  CFX_PointF FFLtoPWL(const CFX_PointF& point);
  CFX_PointF PWLtoFFL(const CFX_PointF& point);
  bool CommitData(const CPDFSDK_PageView* pPageView, Mask<FWL_EVENTFLAG> nFlag);
  void DestroyPWLWindow(const CPDFSDK_PageView* pPageView);
  void EscapeFiller(CPDFSDK_PageView* pPageView, bool bDestroyPWLWindow);

  bool IsValid() const;
  CFX_FloatRect GetPDFAnnotRect() const;

  CPDFSDK_PageView* GetCurPageView();
  void SetChangeMark();

  CPDFSDK_Widget* GetSDKWidget() const { return m_pWidget; }

  CFFL_PerWindowData* GetPerPWLWindowData(const CPDFSDK_PageView* pPageView);
  void ResetPWLWindowForValueAge(const CPDFSDK_PageView* pPageView,
                                 CPDFSDK_Widget* pWidget,
                                 uint32_t nValueAge);

 protected:
  friend class CPWLComboBoxEmbedderTest;
  friend class CPWLEditEmbedderTest;
  friend class CPWLSpecialButtonEmbedderTest;

  virtual CPWL_Wnd* ResetPWLWindow(const CPDFSDK_PageView* pPageView);
  virtual CPWL_Wnd* RestorePWLWindow(const CPDFSDK_PageView* pPageView);

  CPWL_Wnd* GetPWLWindow(const CPDFSDK_PageView* pPageView) const;
  CPWL_Wnd* CreateOrUpdatePWLWindow(const CPDFSDK_PageView* pPageView);
  CPWL_Wnd* ResetPWLWindowForValueAgeInternal(const CPDFSDK_PageView* pPageView,
                                              CPDFSDK_Widget* pWidget,
                                              uint32_t nValueAge);

  // If the inheriting widget has its own fontmap and a PWL_Edit widget that
  // access that fontmap then you have to call DestroyWindows before destroying
  // the font map in order to not get a use-after-free.
  //
  // The font map should be stored somewhere more appropriate so it will live
  // until the PWL_Edit is done with it. pdfium:566
  void DestroyWindows();

  void InvalidateRect(const FX_RECT& rect);

  bool m_bValid = false;
  UnownedPtr<CFFL_InteractiveFormFiller> const m_pFormFiller;
  UnownedPtr<CPDFSDK_Widget> m_pWidget;
  std::unique_ptr<CFX_Timer> m_pTimer;
  std::map<const CPDFSDK_PageView*, std::unique_ptr<CPWL_Wnd>> m_Maps;
};

#endif  // FPDFSDK_FORMFILLER_CFFL_FORMFIELD_H_
