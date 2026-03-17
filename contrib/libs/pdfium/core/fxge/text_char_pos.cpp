// Copyright 2019 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxge/text_char_pos.h"

TextCharPos::TextCharPos() = default;

TextCharPos::TextCharPos(const TextCharPos&) = default;

TextCharPos::~TextCharPos() = default;

CFX_Matrix TextCharPos::GetEffectiveMatrix(const CFX_Matrix& matrix) const {
  CFX_Matrix new_matrix;
  if (m_bGlyphAdjust) {
    new_matrix = CFX_Matrix(m_AdjustMatrix[0], m_AdjustMatrix[1],
                            m_AdjustMatrix[2], m_AdjustMatrix[3], 0, 0);
  }
  new_matrix.Concat(matrix);
  return new_matrix;
}
