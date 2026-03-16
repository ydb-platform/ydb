// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXGE_DIB_CFX_BITMAPSTORER_H_
#define CORE_FXGE_DIB_CFX_BITMAPSTORER_H_

#include <stdint.h>

#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/span.h"
#include "core/fxge/dib/scanlinecomposer_iface.h"

class CFX_DIBitmap;

class CFX_BitmapStorer final : public ScanlineComposerIface {
 public:
  CFX_BitmapStorer();
  ~CFX_BitmapStorer() override;

  // ScanlineComposerIface:
  void ComposeScanline(int line, pdfium::span<const uint8_t> scanline) override;
  bool SetInfo(int width,
               int height,
               FXDIB_Format src_format,
               DataVector<uint32_t> src_palette) override;

  RetainPtr<CFX_DIBitmap> GetBitmap() { return m_pBitmap; }
  RetainPtr<CFX_DIBitmap> Detach();
  void Replace(RetainPtr<CFX_DIBitmap>&& pBitmap);

 private:
  RetainPtr<CFX_DIBitmap> m_pBitmap;
};

#endif  // CORE_FXGE_DIB_CFX_BITMAPSTORER_H_
