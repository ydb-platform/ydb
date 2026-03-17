// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXGE_DIB_CFX_SCANLINECOMPOSITOR_H_
#define CORE_FXGE_DIB_CFX_SCANLINECOMPOSITOR_H_

#include <stddef.h>
#include <stdint.h>

#include <memory>

#include "core/fxcrt/fx_memory_wrappers.h"
#include "core/fxcrt/span.h"
#include "core/fxge/dib/fx_dib.h"

class CFX_ScanlineCompositor {
 public:
  CFX_ScanlineCompositor();
  ~CFX_ScanlineCompositor();

  bool Init(FXDIB_Format dest_format,
            FXDIB_Format src_format,
            pdfium::span<const uint32_t> src_palette,
            uint32_t mask_color,
            BlendMode blend_type,
            bool bRgbByteOrder);

  void CompositeRgbBitmapLine(pdfium::span<uint8_t> dest_scan,
                              pdfium::span<const uint8_t> src_scan,
                              int width,
                              pdfium::span<const uint8_t> clip_scan) const;

  void CompositePalBitmapLine(pdfium::span<uint8_t> dest_scan,
                              pdfium::span<const uint8_t> src_scan,
                              int src_left,
                              int width,
                              pdfium::span<const uint8_t> clip_scan) const;

  void CompositeByteMaskLine(pdfium::span<uint8_t> dest_scan,
                             pdfium::span<const uint8_t> src_scan,
                             int width,
                             pdfium::span<const uint8_t> clip_scan) const;

  void CompositeBitMaskLine(pdfium::span<uint8_t> dest_scan,
                            pdfium::span<const uint8_t> src_scan,
                            int src_left,
                            int width,
                            pdfium::span<const uint8_t> clip_scan) const;

 private:
  class Palette {
   public:
    Palette();
    ~Palette();

    void Reset();
    pdfium::span<uint8_t> Make8BitPalette(size_t nElements);
    pdfium::span<uint32_t> Make32BitPalette(size_t nElements);

    // Hard CHECK() if mismatch between created and requested widths.
    pdfium::span<const uint8_t> Get8BitPalette() const;
    pdfium::span<const uint32_t> Get32BitPalette() const;

   private:
    // If 0, then no |m_pData|.
    // If 1, then |m_pData| is really uint8_t* instead.
    // If 4, then |m_pData| is uint32_t* as expected.
    size_t m_Width = 0;
    size_t m_nElements = 0;

    // TODO(tsepez): convert to variant of FixedArray.
    std::unique_ptr<uint32_t, FxFreeDeleter> m_pData;
  };

  void InitSourcePalette(pdfium::span<const uint32_t> src_palette);

  void InitSourceMask(uint32_t mask_color);

  void CompositeRgbBitmapLineSrcBgrx(
      pdfium::span<uint8_t> dest_scan,
      pdfium::span<const uint8_t> src_scan,
      int width,
      pdfium::span<const uint8_t> clip_scan) const;
  void CompositeRgbBitmapLineSrcBgra(
      pdfium::span<uint8_t> dest_scan,
      pdfium::span<const uint8_t> src_scan,
      int width,
      pdfium::span<const uint8_t> clip_scan) const;
#if defined(PDF_USE_SKIA)
  void CompositeRgbBitmapLineSrcBgraPremul(pdfium::span<uint8_t> dest_scan,
                                           pdfium::span<const uint8_t> src_scan,
                                           int width) const;
#endif

  void CompositePalBitmapLineSrcBpp1(
      pdfium::span<uint8_t> dest_scan,
      pdfium::span<const uint8_t> src_scan,
      int src_left,
      int width,
      pdfium::span<const uint8_t> clip_scan) const;
  void CompositePalBitmapLineSrcBpp8(
      pdfium::span<uint8_t> dest_scan,
      pdfium::span<const uint8_t> src_scan,
      int src_left,
      int width,
      pdfium::span<const uint8_t> clip_scan) const;

  FXDIB_Format m_SrcFormat;
  FXDIB_Format m_DestFormat;
  Palette m_SrcPalette;
  int m_MaskAlpha;
  int m_MaskRed;
  int m_MaskGreen;
  int m_MaskBlue;
  BlendMode m_BlendType = BlendMode::kNormal;
  bool m_bRgbByteOrder = false;
};

#endif  // CORE_FXGE_DIB_CFX_SCANLINECOMPOSITOR_H_
