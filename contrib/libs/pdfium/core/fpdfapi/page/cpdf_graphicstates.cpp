// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/page/cpdf_graphicstates.h"

CPDF_GraphicStates::CPDF_GraphicStates() = default;

CPDF_GraphicStates::CPDF_GraphicStates(const CPDF_GraphicStates& that) =
    default;

CPDF_GraphicStates& CPDF_GraphicStates::operator=(
    const CPDF_GraphicStates& that) = default;

CPDF_GraphicStates::~CPDF_GraphicStates() = default;

void CPDF_GraphicStates::SetDefaultStates() {
  m_ColorState.Emplace();
  m_ColorState.SetDefault();
}
