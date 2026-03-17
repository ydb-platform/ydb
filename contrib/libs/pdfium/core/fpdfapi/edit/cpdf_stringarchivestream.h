// Copyright 2018 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FPDFAPI_EDIT_CPDF_STRINGARCHIVESTREAM_H_
#define CORE_FPDFAPI_EDIT_CPDF_STRINGARCHIVESTREAM_H_

#include "core/fxcrt/fx_stream.h"
#include "core/fxcrt/fx_string_wrappers.h"
#include "core/fxcrt/unowned_ptr.h"

class CPDF_StringArchiveStream final : public IFX_ArchiveStream {
 public:
  explicit CPDF_StringArchiveStream(fxcrt::ostringstream* stream);
  ~CPDF_StringArchiveStream() override;

  // IFX_ArchiveStream:
  bool WriteBlock(pdfium::span<const uint8_t> buffer) override;
  FX_FILESIZE CurrentOffset() const override;

 private:
  UnownedPtr<fxcrt::ostringstream> stream_;
};

#endif  // CORE_FPDFAPI_EDIT_CPDF_STRINGARCHIVESTREAM_H_
