// Copyright 2019 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_RENDER_CPDF_RENDERSHADING_H_
#define CORE_FPDFAPI_RENDER_CPDF_RENDERSHADING_H_

class CFX_Matrix;
class CFX_RenderDevice;
class CPDF_PageObject;
class CPDF_RenderContext;
class CPDF_RenderOptions;
class CPDF_ShadingPattern;
struct FX_RECT;

class CPDF_RenderShading {
 public:
  static void Draw(CFX_RenderDevice* pDevice,
                   CPDF_RenderContext* pContext,
                   const CPDF_PageObject* pCurObj,
                   const CPDF_ShadingPattern* pPattern,
                   const CFX_Matrix& mtMatrix,
                   const FX_RECT& clip_rect,
                   int alpha,
                   const CPDF_RenderOptions& options);

  CPDF_RenderShading() = delete;
  CPDF_RenderShading(const CPDF_RenderShading&) = delete;
  CPDF_RenderShading& operator=(const CPDF_RenderShading&) = delete;
};

#endif  // CORE_FPDFAPI_RENDER_CPDF_RENDERSHADING_H_
