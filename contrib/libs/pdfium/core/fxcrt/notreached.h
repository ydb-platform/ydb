// Copyright 2024 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FXCRT_NOTREACHED_H_
#define CORE_FXCRT_NOTREACHED_H_

#include <assert.h>

#include "core/fxcrt/check.h"

// TODO(crbug.com/pdfium/2008): Migrate NOTREACHED() callers to
// NOTREACHED_NORETURN() which is [[noreturn]] and always FATAL. Once that's
// done, rename NOTREACHED_NORETURN() back to NOTREACHED() and remove the
// non-FATAL version.
#define NOTREACHED() DCHECK(false)

// NOTREACHED_NORETURN() annotates paths that are supposed to be unreachable.
// They crash if they are ever hit.
// TODO(crbug.com/pdfium/2008): Rename back to NOTREACHED() once there are no
// callers of the old non-CHECK-fatal macro.
#define NOTREACHED_NORETURN() CHECK(false)

#endif  // CORE_FXCRT_NOTREACHED_H_
