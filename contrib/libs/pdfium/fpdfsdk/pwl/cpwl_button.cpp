// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "fpdfsdk/pwl/cpwl_button.h"

#include <utility>

CPWL_Button::CPWL_Button(
    const CreateParams& cp,
    std::unique_ptr<IPWL_FillerNotify::PerWindowData> pAttachedData)
    : CPWL_Wnd(cp, std::move(pAttachedData)) {
  GetCreationParams()->eCursorType = IPWL_FillerNotify::CursorStyle::kHand;
}

CPWL_Button::~CPWL_Button() = default;

bool CPWL_Button::OnLButtonDown(Mask<FWL_EVENTFLAG> nFlag,
                                const CFX_PointF& point) {
  CPWL_Wnd::OnLButtonDown(nFlag, point);
  m_bMouseDown = true;
  SetCapture();
  return true;
}

bool CPWL_Button::OnLButtonUp(Mask<FWL_EVENTFLAG> nFlag,
                              const CFX_PointF& point) {
  CPWL_Wnd::OnLButtonUp(nFlag, point);
  ReleaseCapture();
  m_bMouseDown = false;
  return true;
}
