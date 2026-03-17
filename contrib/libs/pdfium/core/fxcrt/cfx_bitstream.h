// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCRT_CFX_BITSTREAM_H_
#define CORE_FXCRT_CFX_BITSTREAM_H_

#include <stddef.h>
#include <stdint.h>

#include "core/fxcrt/raw_span.h"
#include "core/fxcrt/span.h"

class CFX_BitStream {
 public:
  explicit CFX_BitStream(pdfium::span<const uint8_t> pData);
  ~CFX_BitStream();

  void ByteAlign();

  bool IsEOF() const { return m_BitPos >= m_BitSize; }
  size_t GetPos() const { return m_BitPos; }
  uint32_t GetBits(uint32_t nBits);

  void SkipBits(size_t nBits) { m_BitPos += nBits; }
  void Rewind() { m_BitPos = 0; }

  size_t BitsRemaining() const {
    return m_BitSize >= m_BitPos ? m_BitSize - m_BitPos : 0;
  }

 private:
  size_t m_BitPos = 0;
  const size_t m_BitSize;
  pdfium::raw_span<const uint8_t> const m_pData;
};

#endif  // CORE_FXCRT_CFX_BITSTREAM_H_
