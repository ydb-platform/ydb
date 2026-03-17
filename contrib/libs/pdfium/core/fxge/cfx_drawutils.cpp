// Copyright 2020 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxge/cfx_drawutils.h"

#include "core/fxcrt/check.h"
#include "core/fxge/cfx_fillrenderoptions.h"
#include "core/fxge/cfx_graphstatedata.h"
#include "core/fxge/cfx_path.h"
#include "core/fxge/cfx_renderdevice.h"

// static
void CFX_DrawUtils::DrawFocusRect(CFX_RenderDevice* render_device,
                                  const CFX_Matrix& user_to_device,
                                  const CFX_FloatRect& view_bounding_box) {
  DCHECK(render_device);
  CFX_Path path;
  path.AppendPoint(CFX_PointF(view_bounding_box.left, view_bounding_box.top),
                   CFX_Path::Point::Type::kMove);
  path.AppendPoint(CFX_PointF(view_bounding_box.left, view_bounding_box.bottom),
                   CFX_Path::Point::Type::kLine);
  path.AppendPoint(
      CFX_PointF(view_bounding_box.right, view_bounding_box.bottom),
      CFX_Path::Point::Type::kLine);
  path.AppendPoint(CFX_PointF(view_bounding_box.right, view_bounding_box.top),
                   CFX_Path::Point::Type::kLine);
  path.AppendPoint(CFX_PointF(view_bounding_box.left, view_bounding_box.top),
                   CFX_Path::Point::Type::kLine);

  CFX_GraphStateData graph_state_data;
  graph_state_data.m_DashArray = {1.0f};
  graph_state_data.m_DashPhase = 0;
  graph_state_data.m_LineWidth = 1.0f;

  render_device->DrawPath(path, &user_to_device, &graph_state_data, 0,
                          ArgbEncode(255, 0, 0, 0),
                          CFX_FillRenderOptions::EvenOddOptions());
}
