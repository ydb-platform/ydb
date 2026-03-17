// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_RENDER_CPDF_PROGRESSIVERENDERER_H_
#define CORE_FPDFAPI_RENDER_CPDF_PROGRESSIVERENDERER_H_

#include <stdint.h>

#include <memory>

#include "core/fpdfapi/page/cpdf_pageobjectholder.h"
#include "core/fpdfapi/render/cpdf_rendercontext.h"
#include "core/fxcrt/fx_coordinates.h"
#include "core/fxcrt/unowned_ptr.h"

class CPDF_RenderOptions;
class CPDF_RenderStatus;
class CFX_RenderDevice;
class PauseIndicatorIface;

class CPDF_ProgressiveRenderer {
 public:
  // Must match FDF_RENDER_* definitions in public/fpdf_progressive.h, but
  // cannot #include that header. fpdfsdk/fpdf_progressive.cpp has
  // static_asserts to make sure the two sets of values match.
  enum Status {
    kReady,          // FPDF_RENDER_READY
    kToBeContinued,  // FPDF_RENDER_TOBECONTINUED
    kDone,           // FPDF_RENDER_DONE
    kFailed          // FPDF_RENDER_FAILED
  };

  CPDF_ProgressiveRenderer(CPDF_RenderContext* pContext,
                           CFX_RenderDevice* pDevice,
                           const CPDF_RenderOptions* pOptions);
  ~CPDF_ProgressiveRenderer();

  Status GetStatus() const { return m_Status; }
  void Start(PauseIndicatorIface* pPause);
  void Continue(PauseIndicatorIface* pPause);

 private:
  // Maximum page objects to render before checking for pause.
  static constexpr int kStepLimit = 100;

  Status m_Status = kReady;
  UnownedPtr<CPDF_RenderContext> const m_pContext;
  UnownedPtr<CFX_RenderDevice> const m_pDevice;
  UnownedPtr<const CPDF_RenderOptions> const m_pOptions;
  std::unique_ptr<CPDF_RenderStatus> m_pRenderStatus;
  CFX_FloatRect m_ClipRect;
  uint32_t m_LayerIndex = 0;
  UnownedPtr<CPDF_RenderContext::Layer> m_pCurrentLayer;
  CPDF_PageObjectHolder::const_iterator m_LastObjectRendered;
};

#endif  // CORE_FPDFAPI_RENDER_CPDF_PROGRESSIVERENDERER_H_
