// Copyright 2023 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FXCRT_UNOWNED_PTR_EXCLUSION_H_
#define CORE_FXCRT_UNOWNED_PTR_EXCLUSION_H_

#include "build/build_config.h"

#if defined(PDF_ENABLE_UNOWNED_PTR_EXCLUSION)
// TODO(tsepez): convert to PA copy of this code.
#define UNOWNED_PTR_EXCLUSION __attribute__((annotate("raw_ptr_exclusion")))
#else
#define UNOWNED_PTR_EXCLUSION
#endif

#endif  // CORE_FXCRT_UNOWNED_PTR_EXCLUSION_H_
