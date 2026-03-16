// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef FPDFSDK_PWL_CPWL_LIST_BOX_H_
#define FPDFSDK_PWL_CPWL_LIST_BOX_H_

#include <memory>

#include "core/fxcrt/unowned_ptr.h"
#include "fpdfsdk/pwl/cpwl_list_ctrl.h"
#include "fpdfsdk/pwl/cpwl_wnd.h"

class IPWL_FillerNotify;

class CPWL_ListBox : public CPWL_Wnd, public CPWL_ListCtrl::NotifyIface {
 public:
  CPWL_ListBox(const CreateParams& cp,
               std::unique_ptr<IPWL_FillerNotify::PerWindowData> pAttachedData);
  ~CPWL_ListBox() override;

  // CPWL_Wnd:
  void OnCreated() override;
  void OnDestroy() override;
  void DrawThisAppearance(CFX_RenderDevice* pDevice,
                          const CFX_Matrix& mtUser2Device) override;
  bool OnKeyDown(FWL_VKEYCODE nKeyCode, Mask<FWL_EVENTFLAG> nFlag) override;
  bool OnChar(uint16_t nChar, Mask<FWL_EVENTFLAG> nFlag) override;
  bool OnLButtonDown(Mask<FWL_EVENTFLAG> nFlag,
                     const CFX_PointF& point) override;
  bool OnLButtonUp(Mask<FWL_EVENTFLAG> nFlag, const CFX_PointF& point) override;
  bool OnMouseMove(Mask<FWL_EVENTFLAG> nFlag, const CFX_PointF& point) override;
  bool OnMouseWheel(Mask<FWL_EVENTFLAG> nFlag,
                    const CFX_PointF& point,
                    const CFX_Vector& delta) override;
  WideString GetText() override;
  void SetScrollInfo(const PWL_SCROLL_INFO& info) override;
  void SetScrollPosition(float pos) override;
  void ScrollWindowVertically(float pos) override;
  bool RepositionChildWnd() override;
  CFX_FloatRect GetFocusRect() const override;
  void SetFontSize(float fFontSize) override;
  float GetFontSize() const override;

  // CPWL_ListCtrl::NotifyIface:
  void OnSetScrollInfoY(float fPlateMin,
                        float fPlateMax,
                        float fContentMin,
                        float fContentMax,
                        float fSmallStep,
                        float fBigStep) override;
  void OnSetScrollPosY(float fy) override;
  [[nodiscard]] bool OnInvalidateRect(const CFX_FloatRect& pRect) override;

  bool OnNotifySelectionChanged(bool bKeyDown, Mask<FWL_EVENTFLAG> nFlag);

  void AddString(const WideString& str);
  void SetTopVisibleIndex(int32_t nItemIndex);
  void ScrollToListItem(int32_t nItemIndex);

  void Select(int32_t nItemIndex);
  void Deselect(int32_t nItemIndex);
  void SetCaret(int32_t nItemIndex);
  void SetHoverSel(bool bHoverSel);

  int32_t GetCount() const;
  bool IsMultipleSel() const;
  int32_t GetCaretIndex() const;
  int32_t GetCurSel() const;
  bool IsItemSelected(int32_t nItemIndex) const;
  int32_t GetTopVisibleIndex() const;
  CFX_FloatRect GetContentRect() const;
  float GetFirstHeight() const;
  CFX_FloatRect GetListRect() const;

 protected:
  bool m_bMouseDown = false;
  bool m_bHoverSel = false;
  std::unique_ptr<CPWL_ListCtrl> m_pListCtrl;
};

#endif  // FPDFSDK_PWL_CPWL_LIST_BOX_H_
