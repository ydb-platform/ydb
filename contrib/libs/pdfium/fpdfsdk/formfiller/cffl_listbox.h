// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef FPDFSDK_FORMFILLER_CFFL_LISTBOX_H_
#define FPDFSDK_FORMFILLER_CFFL_LISTBOX_H_

#include <memory>
#include <set>
#include <vector>

#include "fpdfsdk/formfiller/cffl_textobject.h"

class CPWL_ListBox;

class CFFL_ListBox final : public CFFL_TextObject {
 public:
  CFFL_ListBox(CFFL_InteractiveFormFiller* pFormFiller,
               CPDFSDK_Widget* pWidget);
  ~CFFL_ListBox() override;

  // CFFL_TextObject:
  CPWL_Wnd::CreateParams GetCreateParam() override;
  std::unique_ptr<CPWL_Wnd> NewPWLWindow(
      const CPWL_Wnd::CreateParams& cp,
      std::unique_ptr<IPWL_FillerNotify::PerWindowData> pAttachedData) override;
  bool OnChar(CPDFSDK_Widget* pWidget,
              uint32_t nChar,
              Mask<FWL_EVENTFLAG> nFlags) override;
  bool IsDataChanged(const CPDFSDK_PageView* pPageView) override;
  void SaveData(const CPDFSDK_PageView* pPageView) override;
  void GetActionData(const CPDFSDK_PageView* pPageView,
                     CPDF_AAction::AActionType type,
                     CFFL_FieldAction& fa) override;
  void SavePWLWindowState(const CPDFSDK_PageView* pPageView) override;
  void RecreatePWLWindowFromSavedState(
      const CPDFSDK_PageView* pPageView) override;
  bool SetIndexSelected(int index, bool selected) override;
  bool IsIndexSelected(int index) override;

 private:
  CPWL_ListBox* GetPWLListBox(const CPDFSDK_PageView* pPageView) const;
  CPWL_ListBox* CreateOrUpdatePWLListBox(const CPDFSDK_PageView* pPageView);

  std::set<int> m_OriginSelections;
  std::vector<int> m_State;
};

#endif  // FPDFSDK_FORMFILLER_CFFL_LISTBOX_H_
