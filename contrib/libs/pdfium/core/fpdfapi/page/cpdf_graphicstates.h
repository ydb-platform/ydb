// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PAGE_CPDF_GRAPHICSTATES_H_
#define CORE_FPDFAPI_PAGE_CPDF_GRAPHICSTATES_H_

#include "core/fpdfapi/page/cpdf_clippath.h"
#include "core/fpdfapi/page/cpdf_colorstate.h"
#include "core/fpdfapi/page/cpdf_generalstate.h"
#include "core/fpdfapi/page/cpdf_textstate.h"
#include "core/fxge/cfx_graphstate.h"

class CPDF_GraphicStates {
 public:
  CPDF_GraphicStates();
  CPDF_GraphicStates(const CPDF_GraphicStates& that);
  CPDF_GraphicStates& operator=(const CPDF_GraphicStates& that);
  ~CPDF_GraphicStates();

  void SetDefaultStates();

  const CPDF_ClipPath& clip_path() const { return m_ClipPath; }
  CPDF_ClipPath& mutable_clip_path() { return m_ClipPath; }

  const CFX_GraphState& graph_state() const { return m_GraphState; }
  CFX_GraphState& mutable_graph_state() { return m_GraphState; }

  const CPDF_ColorState& color_state() const { return m_ColorState; }
  CPDF_ColorState& mutable_color_state() { return m_ColorState; }

  const CPDF_TextState& text_state() const { return m_TextState; }
  CPDF_TextState& mutable_text_state() { return m_TextState; }

  const CPDF_GeneralState& general_state() const { return m_GeneralState; }
  CPDF_GeneralState& mutable_general_state() { return m_GeneralState; }

 private:
  CPDF_ClipPath m_ClipPath;
  CFX_GraphState m_GraphState;
  CPDF_ColorState m_ColorState;
  CPDF_TextState m_TextState;
  CPDF_GeneralState m_GeneralState;
};

#endif  // CORE_FPDFAPI_PAGE_CPDF_GRAPHICSTATES_H_
