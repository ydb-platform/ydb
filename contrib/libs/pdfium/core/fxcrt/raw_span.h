// Copyright 2024 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FXCRT_RAW_SPAN_H_
#define CORE_FXCRT_RAW_SPAN_H_

#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/span.h"

#if defined(PDF_USE_PARTITION_ALLOC)
#include "partition_alloc/pointers/raw_ptr.h"
#else
#include "core/fxcrt/unowned_ptr_exclusion.h"
#endif

namespace pdfium {

#if defined(PDF_USE_PARTITION_ALLOC)
template <typename T>
using raw_span = span<T, dynamic_extent, raw_ptr<T, AllowPtrArithmetic>>;
#else
template <typename T>
using raw_span = span<T, dynamic_extent, UNOWNED_PTR_EXCLUSION T*>;
#endif

}  // namespace pdfium

#endif  // CORE_FXCRT_RAW_SPAN_H_
