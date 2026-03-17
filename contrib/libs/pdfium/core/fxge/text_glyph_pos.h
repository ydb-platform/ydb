// Copyright 2019 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXGE_TEXT_GLYPH_POS_H_
#define CORE_FXGE_TEXT_GLYPH_POS_H_

#include <optional>

#include "core/fxcrt/fx_coordinates.h"
#include "core/fxcrt/unowned_ptr.h"

class CFX_GlyphBitmap;

class TextGlyphPos {
 public:
  TextGlyphPos();
  TextGlyphPos(const TextGlyphPos&);
  ~TextGlyphPos();

  std::optional<CFX_Point> GetOrigin(const CFX_Point& offset) const;

  UnownedPtr<const CFX_GlyphBitmap> m_pGlyph;
  CFX_Point m_Origin;
  CFX_PointF m_fDeviceOrigin;
};

#endif  // CORE_FXGE_TEXT_GLYPH_POS_H_
