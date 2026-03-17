// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FXCRT_FX_SAFE_TYPES_H_
#define CORE_FXCRT_FX_SAFE_TYPES_H_

#include <stddef.h>
#include <stdint.h>

#include "core/fxcrt/fx_types.h"
#include "core/fxcrt/numerics/safe_math.h"

using FX_SAFE_UINT32 = pdfium::CheckedNumeric<uint32_t>;
using FX_SAFE_INT32 = pdfium::CheckedNumeric<int32_t>;
using FX_SAFE_SIZE_T = pdfium::CheckedNumeric<size_t>;
using FX_SAFE_FILESIZE = pdfium::CheckedNumeric<FX_FILESIZE>;

#endif  // CORE_FXCRT_FX_SAFE_TYPES_H_
