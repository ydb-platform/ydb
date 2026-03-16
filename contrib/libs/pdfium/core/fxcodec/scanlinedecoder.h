// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCODEC_SCANLINEDECODER_H_
#define CORE_FXCODEC_SCANLINEDECODER_H_

#include <stdint.h>

#include "core/fxcrt/raw_span.h"
#include "core/fxcrt/span.h"

class PauseIndicatorIface;

namespace fxcodec {

class ScanlineDecoder {
 public:
  ScanlineDecoder();
  ScanlineDecoder(int nOrigWidth,
                  int nOrigHeight,
                  int nOutputWidth,
                  int nOutputHeight,
                  int nComps,
                  int nBpc,
                  uint32_t nPitch);
  virtual ~ScanlineDecoder();

  pdfium::span<const uint8_t> GetScanline(int line);
  bool SkipToScanline(int line, PauseIndicatorIface* pPause);

  int GetWidth() const { return m_OutputWidth; }
  int GetHeight() const { return m_OutputHeight; }
  int CountComps() const { return m_nComps; }
  int GetBPC() const { return m_bpc; }

  virtual uint32_t GetSrcOffset() = 0;

 protected:
  virtual bool Rewind() = 0;
  virtual pdfium::span<uint8_t> GetNextLine() = 0;

  int m_OrigWidth;
  int m_OrigHeight;
  int m_OutputWidth;
  int m_OutputHeight;
  int m_nComps;
  int m_bpc;
  uint32_t m_Pitch;
  int m_NextLine = -1;
  pdfium::raw_span<uint8_t> m_pLastScanline;
};

}  // namespace fxcodec

using ScanlineDecoder = fxcodec::ScanlineDecoder;

#endif  // CORE_FXCODEC_SCANLINEDECODER_H_
