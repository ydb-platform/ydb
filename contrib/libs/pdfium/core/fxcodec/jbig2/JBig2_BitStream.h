// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCODEC_JBIG2_JBIG2_BITSTREAM_H_
#define CORE_FXCODEC_JBIG2_JBIG2_BITSTREAM_H_

#include "core/fxcrt/raw_span.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/span.h"

class CJBig2_BitStream {
 public:
  CJBig2_BitStream(pdfium::span<const uint8_t> pSrcStream, uint64_t key);
  CJBig2_BitStream(const CJBig2_BitStream&) = delete;
  CJBig2_BitStream& operator=(const CJBig2_BitStream&) = delete;
  ~CJBig2_BitStream();

  // TODO(thestig): readFoo() should return bool.
  int32_t readNBits(uint32_t dwBits, uint32_t* dwResult);
  int32_t readNBits(uint32_t dwBits, int32_t* nResult);
  int32_t read1Bit(uint32_t* dwResult);
  int32_t read1Bit(bool* bResult);
  int32_t read1Byte(uint8_t* cResult);
  int32_t readInteger(uint32_t* dwResult);
  int32_t readShortInteger(uint16_t* wResult);
  void alignByte();
  uint8_t getCurByte() const;
  void incByteIdx();
  uint8_t getCurByte_arith() const;
  uint8_t getNextByte_arith() const;
  uint32_t getOffset() const;
  void setOffset(uint32_t dwOffset);
  void addOffset(uint32_t dwDelta);
  uint32_t getBitPos() const;
  void setBitPos(uint32_t dwBitPos);
  pdfium::span<const uint8_t> getBufSpan() const { return m_Span; }
  const uint8_t* getPointer() const;
  uint32_t getByteLeft() const;
  uint64_t getKey() const { return m_Key; }
  bool IsInBounds() const;

 private:
  void AdvanceBit();
  uint32_t LengthInBits() const;

  const pdfium::raw_span<const uint8_t> m_Span;
  uint32_t m_dwByteIdx = 0;  // Must always be <= `m_Span.size()`.
  uint32_t m_dwBitIdx = 0;   // Must Always be in [0..7].
  const uint64_t m_Key;
};

#endif  // CORE_FXCODEC_JBIG2_JBIG2_BITSTREAM_H_
