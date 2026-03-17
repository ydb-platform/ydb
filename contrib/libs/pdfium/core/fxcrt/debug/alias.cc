// Copyright 2024 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "core/fxcrt/debug/alias.h"

#include "build/build_config.h"

namespace pdfium {

#if defined(COMPILER_MSVC)
#pragma optimize("", off)
#elif defined(__clang__)
#pragma clang optimize off
#endif

void Alias(const void* var) {
}

#if defined(COMPILER_MSVC)
#pragma optimize("", on)
#elif defined(__clang__)
#pragma clang optimize on
#endif

}  // namespace pdfium
