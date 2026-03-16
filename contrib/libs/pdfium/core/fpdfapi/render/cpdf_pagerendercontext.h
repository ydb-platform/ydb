// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_RENDER_CPDF_PAGERENDERCONTEXT_H_
#define CORE_FPDFAPI_RENDER_CPDF_PAGERENDERCONTEXT_H_

#include <memory>

#include "core/fpdfapi/page/cpdf_page.h"

class CFX_RenderDevice;
class CPDF_ProgressiveRenderer;
class CPDF_RenderContext;
class CPDF_RenderOptions;

// Everything about rendering is put here: for OOM recovery
class CPDF_PageRenderContext final : public CPDF_Page::RenderContextIface {
 public:
  // Context merely manages the lifetime for callers.
  class AnnotListIface {
   public:
    virtual ~AnnotListIface() = default;
  };

  CPDF_PageRenderContext();
  ~CPDF_PageRenderContext() override;

  // Specific destruction order required.
  std::unique_ptr<AnnotListIface> m_pAnnots;
  std::unique_ptr<CPDF_RenderOptions> m_pOptions;
  std::unique_ptr<CFX_RenderDevice> m_pDevice;
  std::unique_ptr<CPDF_RenderContext> m_pContext;
  std::unique_ptr<CPDF_ProgressiveRenderer> m_pRenderer;
};

#endif  // CORE_FPDFAPI_RENDER_CPDF_PAGERENDERCONTEXT_H_
