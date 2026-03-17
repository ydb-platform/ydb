// Copyright 2019 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXGE_TEXT_CHAR_POS_H_
#define CORE_FXGE_TEXT_CHAR_POS_H_

#include <array>

#include "build/build_config.h"
#include "core/fxcrt/fx_coordinates.h"

class TextCharPos {
 public:
  TextCharPos();
  TextCharPos(const TextCharPos&);
  ~TextCharPos();

  CFX_Matrix GetEffectiveMatrix(const CFX_Matrix& matrix) const;

  CFX_PointF m_Origin;
  uint32_t m_Unicode = 0;
  uint32_t m_GlyphIndex = 0;
  int m_FontCharWidth = 0;
#if BUILDFLAG(IS_APPLE)
  uint32_t m_ExtGID = 0;
#endif
  int32_t m_FallbackFontPosition = 0;
  bool m_bGlyphAdjust = false;
  bool m_bFontStyle = false;
  std::array<float, 4> m_AdjustMatrix = {};
};

#endif  // CORE_FXGE_TEXT_CHAR_POS_H_
