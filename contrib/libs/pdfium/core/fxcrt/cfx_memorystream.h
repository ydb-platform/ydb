// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCRT_CFX_MEMORYSTREAM_H_
#define CORE_FXCRT_CFX_MEMORYSTREAM_H_

#include "core/fxcrt/data_vector.h"
#include "core/fxcrt/fx_stream.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/span.h"

class CFX_MemoryStream final : public IFX_SeekableStream {
 public:
  CONSTRUCT_VIA_MAKE_RETAIN;

  // IFX_SeekableStream
  FX_FILESIZE GetSize() override;
  FX_FILESIZE GetPosition() override;
  bool IsEOF() override;
  bool ReadBlockAtOffset(pdfium::span<uint8_t> buffer,
                         FX_FILESIZE offset) override;
  bool WriteBlock(pdfium::span<const uint8_t> buffer) override;
  bool Flush() override;

  pdfium::span<const uint8_t> GetSpan() const;

 private:
  CFX_MemoryStream();
  ~CFX_MemoryStream() override;

  DataVector<uint8_t> m_data;
  size_t m_nCurSize = 0;
  size_t m_nCurPos = 0;
};

#endif  // CORE_FXCRT_CFX_MEMORYSTREAM_H_
