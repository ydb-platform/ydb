// Copyright 2018 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "fpdfsdk/cpdfsdk_customaccess.h"

#include "core/fxcrt/fx_safe_types.h"
#include "core/fxcrt/numerics/safe_conversions.h"

CPDFSDK_CustomAccess::CPDFSDK_CustomAccess(FPDF_FILEACCESS* pFileAccess)
    : m_FileAccess(*pFileAccess) {}

CPDFSDK_CustomAccess::~CPDFSDK_CustomAccess() = default;

FX_FILESIZE CPDFSDK_CustomAccess::GetSize() {
  return m_FileAccess.m_FileLen;
}

bool CPDFSDK_CustomAccess::ReadBlockAtOffset(pdfium::span<uint8_t> buffer,
                                             FX_FILESIZE offset) {
  if (buffer.empty() || offset < 0)
    return false;

  if (!pdfium::IsValueInRangeForNumericType<FX_FILESIZE>(buffer.size())) {
    return false;
  }

  FX_SAFE_FILESIZE new_pos = buffer.size();
  new_pos += offset;
  return new_pos.IsValid() && new_pos.ValueOrDie() <= GetSize() &&
         m_FileAccess.m_GetBlock(
             m_FileAccess.m_Param, pdfium::checked_cast<unsigned long>(offset),
             buffer.data(), pdfium::checked_cast<unsigned long>(buffer.size()));
}
