// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef FPDFSDK_FORMFILLER_CFFL_BUTTON_H_
#define FPDFSDK_FORMFILLER_CFFL_BUTTON_H_

#include "core/fxcrt/fx_coordinates.h"
#include "fpdfsdk/formfiller/cffl_formfield.h"

class CFX_RenderDevice;
class CFX_Matrix;
class CPDFSDK_PageView;
class CPDFSDK_Widget;

class CFFL_Button : public CFFL_FormField {
 public:
  CFFL_Button(CFFL_InteractiveFormFiller* pFormFiller, CPDFSDK_Widget* pWidget);
  ~CFFL_Button() override;

  // CFFL_FormField
  void OnMouseEnter(CPDFSDK_PageView* pPageView) override;
  void OnMouseExit(CPDFSDK_PageView* pPageView) override;
  bool OnLButtonDown(CPDFSDK_PageView* pPageView,
                     CPDFSDK_Widget* pWidget,
                     Mask<FWL_EVENTFLAG> nFlags,
                     const CFX_PointF& point) override;
  bool OnLButtonUp(CPDFSDK_PageView* pPageView,
                   CPDFSDK_Widget* pWidget,
                   Mask<FWL_EVENTFLAG> nFlags,
                   const CFX_PointF& point) override;
  bool OnMouseMove(CPDFSDK_PageView* pPageView,
                   Mask<FWL_EVENTFLAG> nFlags,
                   const CFX_PointF& point) override;
  void OnDraw(CPDFSDK_PageView* pPageView,
              CPDFSDK_Widget* pWidget,
              CFX_RenderDevice* pDevice,
              const CFX_Matrix& mtUser2Device) override;
  void OnDrawDeactive(CPDFSDK_PageView* pPageView,
                      CPDFSDK_Widget* pWidget,
                      CFX_RenderDevice* pDevice,
                      const CFX_Matrix& mtUser2Device) override;

 private:
  bool m_bMouseIn = false;
  bool m_bMouseDown = false;
};

#endif  // FPDFSDK_FORMFILLER_CFFL_BUTTON_H_
