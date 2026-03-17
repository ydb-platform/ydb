// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef FPDFSDK_PWL_CPWL_SPECIAL_BUTTON_H_
#define FPDFSDK_PWL_CPWL_SPECIAL_BUTTON_H_

#include <memory>

#include "fpdfsdk/pwl/cpwl_button.h"

class CPWL_PushButton final : public CPWL_Button {
 public:
  CPWL_PushButton(
      const CreateParams& cp,
      std::unique_ptr<IPWL_FillerNotify::PerWindowData> pAttachedData);
  ~CPWL_PushButton() override;

  // CPWL_Button:
  CFX_FloatRect GetFocusRect() const override;
};

class CPWL_CheckBox final : public CPWL_Button {
 public:
  CPWL_CheckBox(
      const CreateParams& cp,
      std::unique_ptr<IPWL_FillerNotify::PerWindowData> pAttachedData);
  ~CPWL_CheckBox() override;

  // CPWL_Button:
  bool OnLButtonUp(Mask<FWL_EVENTFLAG> nFlag, const CFX_PointF& point) override;
  bool OnChar(uint16_t nChar, Mask<FWL_EVENTFLAG> nFlag) override;

  bool IsChecked() const { return m_bChecked; }
  void SetCheck(bool bCheck) { m_bChecked = bCheck; }

 private:
  bool m_bChecked = false;
};

class CPWL_RadioButton final : public CPWL_Button {
 public:
  CPWL_RadioButton(
      const CreateParams& cp,
      std::unique_ptr<IPWL_FillerNotify::PerWindowData> pAttachedData);
  ~CPWL_RadioButton() override;

  // CPWL_Button
  bool OnLButtonUp(Mask<FWL_EVENTFLAG> nFlag, const CFX_PointF& point) override;
  bool OnChar(uint16_t nChar, Mask<FWL_EVENTFLAG> nFlag) override;

  bool IsChecked() const { return m_bChecked; }
  void SetCheck(bool bCheck) { m_bChecked = bCheck; }

 private:
  bool m_bChecked = false;
};

#endif  // FPDFSDK_PWL_CPWL_SPECIAL_BUTTON_H_
