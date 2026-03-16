// Copyright 2024 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FXCRT_CHECK_H_
#define CORE_FXCRT_CHECK_H_

#include <assert.h>

#include "build/build_config.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/immediate_crash.h"

#define CHECK(condition)              \
  do {                                \
    if (UNLIKELY(!(condition))) {     \
      pdfium::ImmediateCrash();       \
    }                                 \
  } while (0)

#if defined(NDEBUG) && !defined(DCHECK_ALWAYS_ON)
#define DCHECK_IS_ON() 0
#else
#define DCHECK_IS_ON() 1
#endif

// Debug mode: Use assert() for better diagnostics
// Release mode, DCHECK_ALWAYS_ON: Use CHECK() since assert() is a no-op.
// Release mode, no DCHECK_ALWAYS_ON: Use assert(), which is a no-op.
#if defined(NDEBUG) && defined(DCHECK_ALWAYS_ON)
#define DCHECK CHECK
#else
#define DCHECK assert
#endif

#endif  // CORE_FXCRT_CHECK_H_
