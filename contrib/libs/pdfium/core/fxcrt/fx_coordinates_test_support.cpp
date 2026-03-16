// Copyright 2024 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "core/fxcrt/fx_coordinates_test_support.h"

#include <ostream>

#include "core/fxcrt/fx_coordinates.h"

std::ostream& operator<<(std::ostream& os, const CFX_FloatRect& rect) {
  os << "rect[w " << rect.Width() << " x h " << rect.Height() << " (left "
     << rect.left << ", bot " << rect.bottom << ")]";
  return os;
}

std::ostream& operator<<(std::ostream& os, const CFX_RectF& rect) {
  os << "rect[w " << rect.Width() << " x h " << rect.Height() << " (left "
     << rect.left << ", top " << rect.top << ")]";
  return os;
}
