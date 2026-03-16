// Copyright 2019 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxge/scoped_font_transform.h"

#include <utility>

namespace {

void ResetTransform(FT_Face face) {
  FT_Matrix matrix;
  matrix.xx = 0x10000L;
  matrix.xy = 0;
  matrix.yx = 0;
  matrix.yy = 0x10000L;
  FT_Set_Transform(face, &matrix, nullptr);
}

}  // namespace

ScopedFontTransform::ScopedFontTransform(RetainPtr<CFX_Face> face,
                                         FT_Matrix* matrix)
    : m_Face(std::move(face)) {
  FT_Set_Transform(m_Face->GetRec(), matrix, nullptr);
}

ScopedFontTransform::~ScopedFontTransform() {
  ResetTransform(m_Face->GetRec());
}
