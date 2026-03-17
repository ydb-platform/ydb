// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "fpdfsdk/cpdfsdk_filewriteadapter.h"

#include "core/fxcrt/check.h"
#include "core/fxcrt/numerics/safe_conversions.h"

CPDFSDK_FileWriteAdapter::CPDFSDK_FileWriteAdapter(FPDF_FILEWRITE* file_write)
    : file_write_(file_write) {
  DCHECK(file_write_);
}

CPDFSDK_FileWriteAdapter::~CPDFSDK_FileWriteAdapter() = default;

bool CPDFSDK_FileWriteAdapter::WriteBlock(pdfium::span<const uint8_t> buffer) {
  return file_write_->WriteBlock(
             file_write_, buffer.data(),
             pdfium::checked_cast<unsigned long>(buffer.size())) != 0;
}
