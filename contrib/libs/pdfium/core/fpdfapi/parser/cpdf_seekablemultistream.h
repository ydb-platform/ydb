// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PARSER_CPDF_SEEKABLEMULTISTREAM_H_
#define CORE_FPDFAPI_PARSER_CPDF_SEEKABLEMULTISTREAM_H_

#include <vector>

#include "core/fxcrt/fx_stream.h"
#include "core/fxcrt/retain_ptr.h"

class CPDF_Stream;
class CPDF_StreamAcc;

class CPDF_SeekableMultiStream final : public IFX_SeekableStream {
 public:
  explicit CPDF_SeekableMultiStream(
      std::vector<RetainPtr<const CPDF_Stream>> streams);
  ~CPDF_SeekableMultiStream() override;

  // IFX_SeekableStream:
  FX_FILESIZE GetPosition() override;
  FX_FILESIZE GetSize() override;
  bool IsEOF() override;
  bool ReadBlockAtOffset(pdfium::span<uint8_t> buffer,
                         FX_FILESIZE offset) override;
  bool WriteBlock(pdfium::span<const uint8_t> buffer) override;
  bool Flush() override;

 private:
  std::vector<RetainPtr<CPDF_StreamAcc>> m_Data;
};

#endif  // CORE_FPDFAPI_PARSER_CPDF_SEEKABLEMULTISTREAM_H_
