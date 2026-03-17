// Copyright 2020 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_RENDER_CPDF_RENDERTILING_H_
#define CORE_FPDFAPI_RENDER_CPDF_RENDERTILING_H_

#include "core/fxcrt/retain_ptr.h"

class CFX_DIBitmap;
class CFX_Matrix;
class CPDF_Form;
class CPDF_PageObject;
class CPDF_RenderStatus;
class CPDF_TilingPattern;
struct FX_RECT;

class CPDF_RenderTiling {
 public:
  static RetainPtr<CFX_DIBitmap> Draw(CPDF_RenderStatus* pRenderStatus,
                                      CPDF_PageObject* pPageObj,
                                      CPDF_TilingPattern* pPattern,
                                      CPDF_Form* pPatternForm,
                                      const CFX_Matrix& mtObj2Device,
                                      const FX_RECT& clip_box,
                                      bool bStroke);

  CPDF_RenderTiling() = delete;
  CPDF_RenderTiling(const CPDF_RenderTiling&) = delete;
  CPDF_RenderTiling& operator=(const CPDF_RenderTiling&) = delete;
};

#endif  // CORE_FPDFAPI_RENDER_CPDF_RENDERTILING_H_
