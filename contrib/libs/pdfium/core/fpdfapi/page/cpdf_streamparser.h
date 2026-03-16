// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PAGE_CPDF_STREAMPARSER_H_
#define CORE_FPDFAPI_PAGE_CPDF_STREAMPARSER_H_

#include <stdint.h>

#include <array>

#include "core/fxcrt/data_vector.h"
#include "core/fxcrt/raw_span.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/span.h"
#include "core/fxcrt/string_pool_template.h"
#include "core/fxcrt/weak_ptr.h"

class CPDF_Dictionary;
class CPDF_Document;
class CPDF_Object;
class CPDF_Stream;

class CPDF_StreamParser {
 public:
  enum ElementType { kEndOfData, kNumber, kKeyword, kName, kOther };

  explicit CPDF_StreamParser(pdfium::span<const uint8_t> span);
  CPDF_StreamParser(pdfium::span<const uint8_t> span,
                    const WeakPtr<ByteStringPool>& pPool);
  ~CPDF_StreamParser();

  ElementType ParseNextElement();
  ByteStringView GetWord() const {
    return ByteStringView(m_WordBuffer).First(m_WordSize);
  }
  uint32_t GetPos() const { return m_Pos; }
  void SetPos(uint32_t pos) { m_Pos = pos; }
  const RetainPtr<CPDF_Object>& GetObject() const { return m_pLastObj; }
  RetainPtr<CPDF_Object> ReadNextObject(bool bAllowNestedArray,
                                        bool bInArray,
                                        uint32_t dwRecursionLevel);
  RetainPtr<CPDF_Stream> ReadInlineStream(CPDF_Document* pDoc,
                                          RetainPtr<CPDF_Dictionary> pDict,
                                          const CPDF_Object* pCSObj);

 private:
  friend class cpdf_streamparser_ReadHexString_Test;
  static constexpr uint32_t kMaxWordLength = 255;

  void GetNextWord(bool& bIsNumber);
  ByteString ReadString();
  DataVector<uint8_t> ReadHexString();
  bool PositionIsInBounds() const;

  uint32_t m_Pos = 0;       // Current byte position within |m_pBuf|.
  uint32_t m_WordSize = 0;  // Current byte position within |m_WordBuffer|.
  WeakPtr<ByteStringPool> m_pPool;
  RetainPtr<CPDF_Object> m_pLastObj;
  pdfium::raw_span<const uint8_t> m_pBuf;
  // Include space for NUL.
  std::array<uint8_t, kMaxWordLength + 1> m_WordBuffer = {};
};

#endif  // CORE_FPDFAPI_PAGE_CPDF_STREAMPARSER_H_
