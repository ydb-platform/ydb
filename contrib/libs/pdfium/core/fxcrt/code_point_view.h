// Copyright 2023 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FXCRT_CODE_POINT_VIEW_H_
#define CORE_FXCRT_CODE_POINT_VIEW_H_

#include "build/build_config.h"
#include "core/fxcrt/check_op.h"
#include "core/fxcrt/string_view_template.h"
#include "core/fxcrt/utf16.h"

namespace pdfium {

#if defined(WCHAR_T_IS_16_BIT)
// A view over a UTF-16 `WideStringView` suitable for iterating by code point
// using a range-based `for` loop.
class CodePointView final {
 public:
  class Iterator {
   public:
    ~Iterator();

    bool operator==(const Iterator& other) const {
      return current_ == other.current_;
    }

    bool operator!=(const Iterator& other) const {
      return current_ != other.current_;
    }

    Iterator& operator++() {
      current_ = next_;
      code_point_ = Decode();
      return *this;
    }

    char32_t operator*() const {
      DCHECK_NE(kSentinel, code_point_);
      return code_point_;
    }

   private:
    friend class CodePointView;

    static constexpr char32_t kSentinel = -1;

    explicit Iterator(WideStringView str_view);

    char32_t Decode();

    WideStringView current_;
    WideStringView next_;
    char32_t code_point_;
  };

  explicit CodePointView(WideStringView backing);
  ~CodePointView();

  Iterator begin() const { return Iterator(backing_); }
  Iterator end() const { return Iterator(WideStringView()); }

 private:
  WideStringView backing_;
};
#else
using CodePointView = WideStringView;
#endif  // defined(WCHAR_T_IS_16_BIT)

}  // namespace pdfium

#endif  // CORE_FXCRT_CODE_POINT_VIEW_H_
