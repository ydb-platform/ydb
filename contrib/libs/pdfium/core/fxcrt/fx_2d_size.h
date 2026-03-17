// Copyright 2022 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FXCRT_FX_2D_SIZE_H_
#define CORE_FXCRT_FX_2D_SIZE_H_

#include "core/fxcrt/fx_safe_types.h"

template <typename T, typename U>
size_t Fx2DSizeOrDie(const T& w, const U& h) {
  FX_SAFE_SIZE_T safe_size = w;
  safe_size *= h;
  return safe_size.ValueOrDie();
}

#endif  // CORE_FXCRT_FX_2D_SIZE_H_
