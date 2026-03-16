// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef FPDFSDK_PWL_CPWL_CARET_H_
#define FPDFSDK_PWL_CPWL_CARET_H_

#include <memory>

#include "core/fxcrt/cfx_timer.h"
#include "fpdfsdk/pwl/cpwl_wnd.h"

class CPWL_Caret final : public CPWL_Wnd, public CFX_Timer::CallbackIface {
 public:
  CPWL_Caret(const CreateParams& cp,
             std::unique_ptr<IPWL_FillerNotify::PerWindowData> pAttachedData);
  ~CPWL_Caret() override;

  // CPWL_Wnd:
  void DrawThisAppearance(CFX_RenderDevice* pDevice,
                          const CFX_Matrix& mtUser2Device) override;
  bool InvalidateRect(const CFX_FloatRect* pRect) override;
  bool SetVisible(bool bVisible) override;

  // CFX_Timer::CallbackIface:
  void OnTimerFired() override;

  void SetCaret(bool bVisible,
                const CFX_PointF& ptHead,
                const CFX_PointF& ptFoot);
  void SetInvalidRect(const CFX_FloatRect& rc) { m_rcInvalid = rc; }

 private:
  CFX_FloatRect GetCaretRect() const;

  bool m_bFlash = false;
  CFX_PointF m_ptHead;
  CFX_PointF m_ptFoot;
  float m_fWidth = 0.4f;
  CFX_FloatRect m_rcInvalid;
  std::unique_ptr<CFX_Timer> m_pTimer;
};

#endif  // FPDFSDK_PWL_CPWL_CARET_H_
