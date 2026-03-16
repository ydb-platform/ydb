// Copyright 2021 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef FPDFSDK_PWL_CPWL_SBBUTTON_H_
#define FPDFSDK_PWL_CPWL_SBBUTTON_H_

#include <memory>

#include "fpdfsdk/pwl/cpwl_wnd.h"
#include "fpdfsdk/pwl/ipwl_fillernotify.h"

class CPWL_SBButton final : public CPWL_Wnd {
 public:
  enum class Type : uint8_t { kMinButton, kMaxButton, kPosButton };

  CPWL_SBButton(const CreateParams& cp,
                std::unique_ptr<IPWL_FillerNotify::PerWindowData> pAttachedData,
                Type eButtonType);
  ~CPWL_SBButton() override;

  // CPWL_Wnd
  void DrawThisAppearance(CFX_RenderDevice* pDevice,
                          const CFX_Matrix& mtUser2Device) override;
  bool OnLButtonDown(Mask<FWL_EVENTFLAG> nFlag,
                     const CFX_PointF& point) override;
  bool OnLButtonUp(Mask<FWL_EVENTFLAG> nFlag, const CFX_PointF& point) override;
  bool OnMouseMove(Mask<FWL_EVENTFLAG> nFlag, const CFX_PointF& point) override;

 private:
  const Type m_eSBButtonType;
  bool m_bMouseDown = false;
};

#endif  // FPDFSDK_PWL_CPWL_SBBUTTON_H_
