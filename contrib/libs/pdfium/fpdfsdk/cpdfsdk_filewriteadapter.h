// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef FPDFSDK_CPDFSDK_FILEWRITEADAPTER_H_
#define FPDFSDK_CPDFSDK_FILEWRITEADAPTER_H_

#include "core/fxcrt/fx_stream.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/unowned_ptr.h"
#include "public/fpdf_save.h"

class CPDFSDK_FileWriteAdapter final : public IFX_RetainableWriteStream {
 public:
  CONSTRUCT_VIA_MAKE_RETAIN;

  // IFX_WriteStream:
  bool WriteBlock(pdfium::span<const uint8_t> buffer) override;

 private:
  explicit CPDFSDK_FileWriteAdapter(FPDF_FILEWRITE* file_write);
  ~CPDFSDK_FileWriteAdapter() override;

  UnownedPtr<FPDF_FILEWRITE> file_write_;
};

#endif  // FPDFSDK_CPDFSDK_FILEWRITEADAPTER_H_
