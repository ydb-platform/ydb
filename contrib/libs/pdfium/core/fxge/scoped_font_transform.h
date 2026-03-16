// Copyright 2019 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXGE_SCOPED_FONT_TRANSFORM_H_
#define CORE_FXGE_SCOPED_FONT_TRANSFORM_H_

#include "core/fxcrt/fx_memory.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxge/cfx_face.h"
#include "core/fxge/freetype/fx_freetype.h"

// Sets the given transform on the font, and resets it to the identity when it
// goes out of scope.
class ScopedFontTransform {
 public:
  FX_STACK_ALLOCATED();

  ScopedFontTransform(RetainPtr<CFX_Face> face, FT_Matrix* matrix);
  ~ScopedFontTransform();

 private:
  RetainPtr<CFX_Face> m_Face;
};

#endif  // CORE_FXGE_SCOPED_FONT_TRANSFORM_H_
