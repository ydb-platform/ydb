// Copyright 2024 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "core/fxcrt/code_point_view.h"

#include "core/fxcrt/utf16.h"

#if !defined(WCHAR_T_IS_16_BIT)
#error "Building on wrong platform".
#endif

namespace pdfium {

CodePointView::CodePointView(WideStringView backing) : backing_(backing) {}

CodePointView::~CodePointView() = default;

CodePointView::Iterator::Iterator(WideStringView str_view)
    : current_(str_view), code_point_(Decode()) {}

CodePointView::Iterator::~Iterator() = default;

char32_t CodePointView::Iterator::Decode() {
  if (current_.IsEmpty()) {
    return kSentinel;
  }
  char32_t code_point = current_.Front();
  next_ = current_.Substr(1);
  if (IsHighSurrogate(code_point)) {
    if (!next_.IsEmpty() && IsLowSurrogate(next_.Front())) {
      code_point = SurrogatePair(code_point, next_.Front()).ToCodePoint();
      next_ = next_.Substr(1);
    }
  }
  return code_point;
}

}  // namespace pdfium
