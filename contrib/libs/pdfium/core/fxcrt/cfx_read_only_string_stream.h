// Copyright 2022 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FXCRT_CFX_READ_ONLY_STRING_STREAM_H_
#define CORE_FXCRT_CFX_READ_ONLY_STRING_STREAM_H_

#include "core/fxcrt/bytestring.h"
#include "core/fxcrt/fx_stream.h"
#include "core/fxcrt/retain_ptr.h"

class CFX_ReadOnlySpanStream;

class CFX_ReadOnlyStringStream final : public IFX_SeekableReadStream {
 public:
  CONSTRUCT_VIA_MAKE_RETAIN;

  // IFX_SeekableReadStream:
  FX_FILESIZE GetSize() override;
  bool ReadBlockAtOffset(pdfium::span<uint8_t> buffer,
                         FX_FILESIZE offset) override;

 private:
  explicit CFX_ReadOnlyStringStream(ByteString data);
  ~CFX_ReadOnlyStringStream() override;

  const ByteString data_;
  const RetainPtr<CFX_ReadOnlySpanStream> stream_;
};

#endif  // CORE_FXCRT_CFX_READ_ONLY_STRING_STREAM_H_
