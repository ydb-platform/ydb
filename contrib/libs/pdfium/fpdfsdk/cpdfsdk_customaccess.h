// Copyright 2018 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef FPDFSDK_CPDFSDK_CUSTOMACCESS_H_
#define FPDFSDK_CPDFSDK_CUSTOMACCESS_H_

#include "core/fxcrt/fx_stream.h"
#include "core/fxcrt/retain_ptr.h"
#include "public/fpdfview.h"

class CPDFSDK_CustomAccess final : public IFX_SeekableReadStream {
 public:
  CONSTRUCT_VIA_MAKE_RETAIN;

  // IFX_SeekableReadStream
  FX_FILESIZE GetSize() override;
  bool ReadBlockAtOffset(pdfium::span<uint8_t> buffer,
                         FX_FILESIZE offset) override;

 private:
  explicit CPDFSDK_CustomAccess(FPDF_FILEACCESS* pFileAccess);
  ~CPDFSDK_CustomAccess() override;

  FPDF_FILEACCESS m_FileAccess;
};

#endif  // FPDFSDK_CPDFSDK_CUSTOMACCESS_H_
