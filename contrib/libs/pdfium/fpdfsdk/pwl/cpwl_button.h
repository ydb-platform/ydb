// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef FPDFSDK_PWL_CPWL_BUTTON_H_
#define FPDFSDK_PWL_CPWL_BUTTON_H_

#include <memory>

#include "fpdfsdk/pwl/cpwl_wnd.h"
#include "fpdfsdk/pwl/ipwl_fillernotify.h"

class CPWL_Button : public CPWL_Wnd {
 public:
  CPWL_Button(const CreateParams& cp,
              std::unique_ptr<IPWL_FillerNotify::PerWindowData> pAttachedData);
  ~CPWL_Button() override;

  // CPWL_Wnd
  bool OnLButtonDown(Mask<FWL_EVENTFLAG> nFlag,
                     const CFX_PointF& point) override;
  bool OnLButtonUp(Mask<FWL_EVENTFLAG> nFlag, const CFX_PointF& point) override;

 protected:
  bool m_bMouseDown = false;
};

#endif  // FPDFSDK_PWL_CPWL_BUTTON_H_
