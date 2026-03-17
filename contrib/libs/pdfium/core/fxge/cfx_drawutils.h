// Copyright 2020 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXGE_CFX_DRAWUTILS_H_
#define CORE_FXGE_CFX_DRAWUTILS_H_

class CFX_FloatRect;
class CFX_Matrix;
class CFX_RenderDevice;

class CFX_DrawUtils {
 public:
  CFX_DrawUtils() = delete;
  CFX_DrawUtils(const CFX_DrawUtils&) = delete;
  CFX_DrawUtils& operator=(const CFX_DrawUtils&) = delete;

  static void DrawFocusRect(CFX_RenderDevice* render_device,
                            const CFX_Matrix& user_to_device,
                            const CFX_FloatRect& view_bounding_box);
};
#endif  // CORE_FXGE_CFX_DRAWUTILS_H_
