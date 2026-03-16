// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef FPDFSDK_FORMFILLER_CFFL_CHECKBOX_H_
#define FPDFSDK_FORMFILLER_CFFL_CHECKBOX_H_

#include <memory>

#include "fpdfsdk/formfiller/cffl_button.h"

class CPWL_CheckBox;

class CFFL_CheckBox final : public CFFL_Button {
 public:
  CFFL_CheckBox(CFFL_InteractiveFormFiller* pFormFiller,
                CPDFSDK_Widget* pWidget);
  ~CFFL_CheckBox() override;

  // CFFL_Button:
  std::unique_ptr<CPWL_Wnd> NewPWLWindow(
      const CPWL_Wnd::CreateParams& cp,
      std::unique_ptr<IPWL_FillerNotify::PerWindowData> pAttachedData) override;
  bool OnKeyDown(FWL_VKEYCODE nKeyCode, Mask<FWL_EVENTFLAG> nFlags) override;
  bool OnChar(CPDFSDK_Widget* pWidget,
              uint32_t nChar,
              Mask<FWL_EVENTFLAG> nFlags) override;
  bool OnLButtonUp(CPDFSDK_PageView* pPageView,
                   CPDFSDK_Widget* pWidget,
                   Mask<FWL_EVENTFLAG> nFlags,
                   const CFX_PointF& point) override;
  bool IsDataChanged(const CPDFSDK_PageView* pPageView) override;
  void SaveData(const CPDFSDK_PageView* pPageView) override;

 private:
  CPWL_CheckBox* GetPWLCheckBox(const CPDFSDK_PageView* pPageView) const;
  CPWL_CheckBox* CreateOrUpdatePWLCheckBox(const CPDFSDK_PageView* pPageView);
};

#endif  // FPDFSDK_FORMFILLER_CFFL_CHECKBOX_H_
