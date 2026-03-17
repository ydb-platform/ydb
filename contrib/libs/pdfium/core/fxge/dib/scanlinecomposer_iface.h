// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXGE_DIB_SCANLINECOMPOSER_IFACE_H_
#define CORE_FXGE_DIB_SCANLINECOMPOSER_IFACE_H_

#include "core/fxcrt/data_vector.h"
#include "core/fxcrt/span.h"
#include "core/fxge/dib/fx_dib.h"

class ScanlineComposerIface {
 public:
  virtual ~ScanlineComposerIface() = default;

  virtual void ComposeScanline(int line,
                               pdfium::span<const uint8_t> scanline) = 0;

  // `src_format` cannot be `FXDIB_Format::k1bppMask` or
  // `FXDIB_Format::k1bppRgb`.
  virtual bool SetInfo(int width,
                       int height,
                       FXDIB_Format src_format,
                       DataVector<uint32_t> src_palette) = 0;
};

#endif  // CORE_FXGE_DIB_SCANLINECOMPOSER_IFACE_H_
