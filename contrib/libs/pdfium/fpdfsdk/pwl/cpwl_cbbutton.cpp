// Copyright 2020 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "fpdfsdk/pwl/cpwl_cbbutton.h"

#include <utility>

#include "core/fxge/cfx_fillrenderoptions.h"
#include "core/fxge/cfx_path.h"
#include "core/fxge/cfx_renderdevice.h"

CPWL_CBButton::CPWL_CBButton(
    const CreateParams& cp,
    std::unique_ptr<IPWL_FillerNotify::PerWindowData> pAttachedData)
    : CPWL_Wnd(cp, std::move(pAttachedData)) {}

CPWL_CBButton::~CPWL_CBButton() = default;

void CPWL_CBButton::DrawThisAppearance(CFX_RenderDevice* pDevice,
                                       const CFX_Matrix& mtUser2Device) {
  CPWL_Wnd::DrawThisAppearance(pDevice, mtUser2Device);

  if (!IsVisible())
    return;

  CFX_FloatRect window = CPWL_Wnd::GetWindowRect();
  if (window.IsEmpty())
    return;

  constexpr float kComboBoxTriangleLength = 6.0f;
  constexpr float kComboBoxTriangleHalfLength = kComboBoxTriangleLength / 2;
  constexpr float kComboBoxTriangleQuarterLength = kComboBoxTriangleLength / 4;
  if (!FXSYS_IsFloatBigger(window.right - window.left,
                           kComboBoxTriangleLength) ||
      !FXSYS_IsFloatBigger(window.top - window.bottom,
                           kComboBoxTriangleHalfLength)) {
    return;
  }

  CFX_PointF ptCenter = GetCenterPoint();
  CFX_PointF pt1(ptCenter.x - kComboBoxTriangleHalfLength,
                 ptCenter.y + kComboBoxTriangleQuarterLength);
  CFX_PointF pt2(ptCenter.x + kComboBoxTriangleHalfLength,
                 ptCenter.y + kComboBoxTriangleQuarterLength);
  CFX_PointF pt3(ptCenter.x, ptCenter.y - kComboBoxTriangleQuarterLength);

  CFX_Path path;
  path.AppendPoint(pt1, CFX_Path::Point::Type::kMove);
  path.AppendPoint(pt2, CFX_Path::Point::Type::kLine);
  path.AppendPoint(pt3, CFX_Path::Point::Type::kLine);
  path.AppendPoint(pt1, CFX_Path::Point::Type::kLine);

  pDevice->DrawPath(path, &mtUser2Device, nullptr,
                    kDefaultBlackColor.ToFXColor(GetTransparency()), 0,
                    CFX_FillRenderOptions::EvenOddOptions());
}

bool CPWL_CBButton::OnLButtonDown(Mask<FWL_EVENTFLAG> nFlag,
                                  const CFX_PointF& point) {
  CPWL_Wnd::OnLButtonDown(nFlag, point);

  SetCapture();
  CPWL_Wnd* pParent = GetParentWindow();
  if (pParent)
    pParent->NotifyLButtonDown(this, point);

  return true;
}

bool CPWL_CBButton::OnLButtonUp(Mask<FWL_EVENTFLAG> nFlag,
                                const CFX_PointF& point) {
  CPWL_Wnd::OnLButtonUp(nFlag, point);

  ReleaseCapture();
  return true;
}
