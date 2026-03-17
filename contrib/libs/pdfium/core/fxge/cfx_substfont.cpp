// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxge/cfx_substfont.h"

CFX_SubstFont::CFX_SubstFont() = default;

CFX_SubstFont::~CFX_SubstFont() = default;

#if defined(PDF_USE_SKIA)
int CFX_SubstFont::GetOriginalWeight() const {
  int weight = m_Weight;

  // Perform the inverse weight adjustment of UseChromeSerif() to get the
  // original font weight.
  if (m_Family == "Chrome Serif")
    weight = weight * 5 / 4;
  return weight;
}
#endif

void CFX_SubstFont::UseChromeSerif() {
  m_Weight = m_Weight * 4 / 5;
  m_Family = "Chrome Serif";
}
