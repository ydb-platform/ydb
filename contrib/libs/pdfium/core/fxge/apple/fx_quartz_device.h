// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXGE_APPLE_FX_QUARTZ_DEVICE_H_
#define CORE_FXGE_APPLE_FX_QUARTZ_DEVICE_H_

#include <CoreGraphics/CoreGraphics.h>
#include <stdint.h>

#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/span.h"
#include "core/fxge/dib/fx_dib.h"

class CFX_DIBitmap;
class CFX_Matrix;

class CQuartz2D {
 public:
  void* CreateGraphics(const RetainPtr<CFX_DIBitmap>& bitmap);
  void DestroyGraphics(void* graphics);

  void* CreateFont(pdfium::span<const uint8_t> font_data);
  void DestroyFont(void* font);
  void SetGraphicsTextMatrix(void* graphics, const CFX_Matrix& matrix);
  bool DrawGraphicsString(void* graphics,
                          void* font,
                          float font_size,
                          pdfium::span<uint16_t> glyph_indices,
                          pdfium::span<CGPoint> glyph_positions,
                          FX_ARGB argb);
};

#endif  // CORE_FXGE_APPLE_FX_QUARTZ_DEVICE_H_
