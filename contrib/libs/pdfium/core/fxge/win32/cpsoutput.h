// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXGE_WIN32_CPSOUTPUT_H_
#define CORE_FXGE_WIN32_CPSOUTPUT_H_

#include <stddef.h>
#include <windows.h>

#include "core/fxcrt/fx_stream.h"

class CPSOutput final : public IFX_RetainableWriteStream {
 public:
  enum class OutputMode { kExtEscape, kGdiComment };

  CPSOutput(HDC hDC, OutputMode mode);
  ~CPSOutput() override;

  // IFX_Writestream:
  bool WriteBlock(pdfium::span<const uint8_t> input) override;

 private:
  const HDC m_hDC;
  const OutputMode m_mode;
};

#endif  // CORE_FXGE_WIN32_CPSOUTPUT_H_
