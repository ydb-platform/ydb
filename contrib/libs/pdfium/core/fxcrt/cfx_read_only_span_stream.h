// Copyright 2022 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCRT_CFX_READ_ONLY_SPAN_STREAM_H_
#define CORE_FXCRT_CFX_READ_ONLY_SPAN_STREAM_H_

#include "core/fxcrt/fx_stream.h"
#include "core/fxcrt/raw_span.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/span.h"

class CFX_ReadOnlySpanStream final : public IFX_SeekableReadStream {
 public:
  CONSTRUCT_VIA_MAKE_RETAIN;

  // IFX_SeekableReadStream:
  FX_FILESIZE GetSize() override;
  bool ReadBlockAtOffset(pdfium::span<uint8_t> buffer,
                         FX_FILESIZE offset) override;

 private:
  explicit CFX_ReadOnlySpanStream(pdfium::span<const uint8_t> span);
  ~CFX_ReadOnlySpanStream() override;

  const pdfium::raw_span<const uint8_t> span_;
};

#endif  // CORE_FXCRT_CFX_READ_ONLY_SPAN_STREAM_H_
