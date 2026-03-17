// Copyright 2018 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FXCODEC_CFX_CODEC_MEMORY_H_
#define CORE_FXCODEC_CFX_CODEC_MEMORY_H_

#include <memory>

#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/fx_memory_wrappers.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/span.h"

class CFX_CodecMemory final : public Retainable {
 public:
  CONSTRUCT_VIA_MAKE_RETAIN;

  // Returns a span over the unconsumed contents of the buffer.
  pdfium::span<uint8_t> GetUnconsumedSpan() {
    return GetBufferSpan().subspan(pos_);
  }

  // SAFETY: `size_` must track `buffer_` allocations.
  pdfium::span<uint8_t> GetBufferSpan() {
    return UNSAFE_BUFFERS(pdfium::make_span(buffer_.get(), size_));
  }
  size_t GetSize() const { return size_; }
  size_t GetPosition() const { return pos_; }
  bool IsEOF() const { return pos_ >= size_; }
  size_t ReadBlock(pdfium::span<uint8_t> buffer);

  // Sets the cursor position to |pos| if possible.
  bool Seek(size_t pos);

  // Try to change the size of the buffer, keep the old one on failure.
  bool TryResize(size_t new_buffer_size);

  // Schlep the bytes down the buffer.
  void Consume(size_t consumed);

 private:
  explicit CFX_CodecMemory(size_t buffer_size);
  ~CFX_CodecMemory() override;

  // TODO(tsepez): convert to a fixed array type.
  std::unique_ptr<uint8_t, FxFreeDeleter> buffer_;
  size_t size_ = 0;
  size_t pos_ = 0;
};

#endif  // CORE_FXCODEC_CFX_CODEC_MEMORY_H_
