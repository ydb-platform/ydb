// Copyright 2021 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/page/cpdf_basedcs.h"

CPDF_BasedCS::CPDF_BasedCS(Family family) : CPDF_ColorSpace(family) {}

CPDF_BasedCS::~CPDF_BasedCS() = default;

void CPDF_BasedCS::EnableStdConversion(bool bEnabled) {
  CPDF_ColorSpace::EnableStdConversion(bEnabled);
  if (m_pBaseCS)
    m_pBaseCS->EnableStdConversion(bEnabled);
}
