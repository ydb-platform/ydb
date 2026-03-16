// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCRT_WIDETEXT_BUFFER_H_
#define CORE_FXCRT_WIDETEXT_BUFFER_H_

#include <stddef.h>

#include "core/fxcrt/binary_buffer.h"
#include "core/fxcrt/fx_string.h"
#include "core/fxcrt/span.h"

namespace fxcrt {

class WideTextBuffer final : public BinaryBuffer {
 public:
  // BinaryBuffer:
  size_t GetLength() const override;

  pdfium::span<wchar_t> GetWideSpan();
  pdfium::span<const wchar_t> GetWideSpan() const;
  WideStringView AsStringView() const;
  WideString MakeString() const;

  void AppendChar(wchar_t wch);
  void Delete(size_t start_index, size_t count);

  WideTextBuffer& operator<<(ByteStringView ascii);
  WideTextBuffer& operator<<(const wchar_t* lpsz);
  WideTextBuffer& operator<<(WideStringView str);
  WideTextBuffer& operator<<(const WideString& str);
  WideTextBuffer& operator<<(const WideTextBuffer& buf);

 private:
  void AppendWideString(WideStringView str);

  // Returned span is the newly-expanded space.
  pdfium::span<wchar_t> ExpandWideBuf(size_t char_count);
};

}  // namespace fxcrt

using fxcrt::WideTextBuffer;

#endif  // CORE_FXCRT_WIDETEXT_BUFFER_H_
