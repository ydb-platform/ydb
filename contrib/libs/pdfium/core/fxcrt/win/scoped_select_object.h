// Copyright 2024 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FXCRT_WIN_SCOPED_SELECT_OBJECT_H_
#define CORE_FXCRT_WIN_SCOPED_SELECT_OBJECT_H_

#include <windows.h>

#include "core/fxcrt/check.h"

namespace pdfium {

// Helper class for deselecting object from DC.
class ScopedSelectObject {
 public:
  ScopedSelectObject(HDC hdc, HGDIOBJ object)
      : hdc_(hdc), oldobj_(SelectObject(hdc, object)) {
    DCHECK(hdc_);
    DCHECK(object);
    DCHECK(oldobj_);
    DCHECK(oldobj_ != HGDI_ERROR);
  }

  ScopedSelectObject(const ScopedSelectObject&) = delete;
  ScopedSelectObject& operator=(const ScopedSelectObject&) = delete;

  ~ScopedSelectObject() {
    [[maybe_unused]] HGDIOBJ object = SelectObject(hdc_, oldobj_);
    DCHECK((GetObjectType(oldobj_) != OBJ_REGION && object) ||
           (GetObjectType(oldobj_) == OBJ_REGION && object != HGDI_ERROR));
  }

 private:
  const HDC hdc_;
  const HGDIOBJ oldobj_;
};

}  // namespace pdfium

#endif  // CORE_FXCRT_WIN_SCOPED_SELECT_OBJECT_H_
