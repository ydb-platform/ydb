// Copyright 2022 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "core/fxcrt/cfx_read_only_string_stream.h"

#include <utility>

#include "core/fxcrt/cfx_read_only_span_stream.h"
#include "core/fxcrt/span.h"

CFX_ReadOnlyStringStream::CFX_ReadOnlyStringStream(ByteString data)
    : data_(std::move(data)),
      stream_(
          pdfium::MakeRetain<CFX_ReadOnlySpanStream>(data_.unsigned_span())) {}

CFX_ReadOnlyStringStream::~CFX_ReadOnlyStringStream() = default;

FX_FILESIZE CFX_ReadOnlyStringStream::GetSize() {
  return stream_->GetSize();
}

bool CFX_ReadOnlyStringStream::ReadBlockAtOffset(pdfium::span<uint8_t> buffer,
                                                 FX_FILESIZE offset) {
  return stream_->ReadBlockAtOffset(buffer, offset);
}
