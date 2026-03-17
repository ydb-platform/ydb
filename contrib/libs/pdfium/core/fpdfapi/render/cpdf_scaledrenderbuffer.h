// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_RENDER_CPDF_SCALEDRENDERBUFFER_H_
#define CORE_FPDFAPI_RENDER_CPDF_SCALEDRENDERBUFFER_H_

#include <memory>

#include "core/fxcrt/fx_coordinates.h"
#include "core/fxcrt/unowned_ptr.h"

class CFX_DefaultRenderDevice;
class CFX_RenderDevice;
class CPDF_PageObject;
class CPDF_RenderContext;
class CPDF_RenderOptions;

class CPDF_ScaledRenderBuffer {
 public:
  CPDF_ScaledRenderBuffer(CFX_RenderDevice* device, const FX_RECT& rect);
  ~CPDF_ScaledRenderBuffer();

  bool Initialize(CPDF_RenderContext* pContext,
                  const CPDF_PageObject* pObj,
                  const CPDF_RenderOptions& options,
                  int max_dpi);

  CFX_DefaultRenderDevice* GetDevice();
  const CFX_Matrix& GetMatrix() const { return matrix_; }
  void OutputToDevice();

 private:
  UnownedPtr<CFX_RenderDevice> const device_;
  std::unique_ptr<CFX_DefaultRenderDevice> const bitmap_device_;
  const FX_RECT rect_;
  CFX_Matrix matrix_;
};

#endif  // CORE_FPDFAPI_RENDER_CPDF_SCALEDRENDERBUFFER_H_
