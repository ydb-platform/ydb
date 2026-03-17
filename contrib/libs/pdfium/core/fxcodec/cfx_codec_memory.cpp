// Copyright 2018 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "core/fxcodec/cfx_codec_memory.h"

#include <algorithm>

#include "core/fxcrt/span_util.h"
#include "core/fxcrt/stl_util.h"

CFX_CodecMemory::CFX_CodecMemory(size_t buffer_size)
    : buffer_(FX_Alloc(uint8_t, buffer_size)), size_(buffer_size) {}

CFX_CodecMemory::~CFX_CodecMemory() = default;

bool CFX_CodecMemory::Seek(size_t pos) {
  if (pos > size_)
    return false;

  pos_ = pos;
  return true;
}

size_t CFX_CodecMemory::ReadBlock(pdfium::span<uint8_t> buffer) {
  if (buffer.empty() || IsEOF())
    return 0;

  size_t bytes_to_read = std::min(buffer.size(), size_ - pos_);
  fxcrt::Copy(GetBufferSpan().subspan(pos_, bytes_to_read), buffer);
  pos_ += bytes_to_read;
  return bytes_to_read;
}

bool CFX_CodecMemory::TryResize(size_t new_buffer_size) {
  uint8_t* pOldBuf = buffer_.release();
  uint8_t* pNewBuf = FX_TryRealloc(uint8_t, pOldBuf, new_buffer_size);
  if (new_buffer_size && !pNewBuf) {
    buffer_.reset(pOldBuf);
    return false;
  }
  buffer_.reset(pNewBuf);
  size_ = new_buffer_size;
  return true;
}

void CFX_CodecMemory::Consume(size_t consumed) {
  fxcrt::spanmove(GetBufferSpan(), GetBufferSpan().subspan(consumed));
}
