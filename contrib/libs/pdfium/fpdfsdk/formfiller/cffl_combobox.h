// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef FPDFSDK_FORMFILLER_CFFL_COMBOBOX_H_
#define FPDFSDK_FORMFILLER_CFFL_COMBOBOX_H_

#include <memory>

#include "core/fxcrt/widestring.h"
#include "fpdfsdk/formfiller/cffl_textobject.h"

class CPWL_ComboBox;

struct FFL_ComboBoxState {
  int nIndex = 0;
  int nStart = 0;
  int nEnd = 0;
  WideString sValue;
};

class CFFL_ComboBox final : public CFFL_TextObject {
 public:
  CFFL_ComboBox(CFFL_InteractiveFormFiller* pFormFiller,
                CPDFSDK_Widget* pWidget);
  ~CFFL_ComboBox() override;

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
  void SetActionData(const CPDFSDK_PageView* pPageView,
                     CPDF_AAction::AActionType type,
                     const CFFL_FieldAction& fa) override;
  void SavePWLWindowState(const CPDFSDK_PageView* pPageView) override;
  void RecreatePWLWindowFromSavedState(
      const CPDFSDK_PageView* pPageView) override;
  bool SetIndexSelected(int index, bool selected) override;
  bool IsIndexSelected(int index) override;
#ifdef PDF_ENABLE_XFA
  bool IsFieldFull(const CPDFSDK_PageView* pPageView) override;
#endif

  // CPWL_Wnd::ProviderIface:
  void OnSetFocusForEdit(CPWL_Edit* pEdit) override;

 private:
  WideString GetSelectExportText();
  CPWL_ComboBox* GetPWLComboBox(const CPDFSDK_PageView* pPageView) const;
  CPWL_ComboBox* CreateOrUpdatePWLComboBox(const CPDFSDK_PageView* pPageView);

  FFL_ComboBoxState m_State;
};

#endif  // FPDFSDK_FORMFILLER_CFFL_COMBOBOX_H_
