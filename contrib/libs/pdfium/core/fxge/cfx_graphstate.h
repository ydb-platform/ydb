// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXGE_CFX_GRAPHSTATE_H_
#define CORE_FXGE_CFX_GRAPHSTATE_H_

#include <stddef.h>

#include <vector>

#include "core/fxcrt/shared_copy_on_write.h"
#include "core/fxge/cfx_graphstatedata.h"

class CFX_GraphState {
 public:
  CFX_GraphState();
  CFX_GraphState(const CFX_GraphState& that);
  ~CFX_GraphState();

  void Emplace();

  void SetLineDash(std::vector<float> dashes, float phase, float scale);
  void SetLineDashPhase(float phase);
  std::vector<float> GetLineDashArray() const;
  size_t GetLineDashSize() const;
  float GetLineDashPhase() const;

  float GetLineWidth() const;
  void SetLineWidth(float width);

  CFX_GraphStateData::LineCap GetLineCap() const;
  void SetLineCap(CFX_GraphStateData::LineCap cap);

  CFX_GraphStateData::LineJoin GetLineJoin() const;
  void SetLineJoin(CFX_GraphStateData::LineJoin join);

  float GetMiterLimit() const;
  void SetMiterLimit(float limit);

  // FIXME(tsepez): remove when all GraphStateData usage gone.
  const CFX_GraphStateData* GetObject() const { return m_Ref.GetObject(); }

 private:
  SharedCopyOnWrite<CFX_RetainableGraphStateData> m_Ref;
};

#endif  // CORE_FXGE_CFX_GRAPHSTATE_H_
