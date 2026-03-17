// Copyright 2022 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FXCRT_DATA_VECTOR_H_
#define CORE_FXCRT_DATA_VECTOR_H_

#include <vector>

#include "core/fxcrt/fx_memory_wrappers.h"

namespace fxcrt {

template <typename T>
using DataVector = std::vector<T, FxAllocAllocator<T>>;

}  // namespace fxcrt

using fxcrt::DataVector;

#endif  // CORE_FXCRT_DATA_VECTOR_H_
