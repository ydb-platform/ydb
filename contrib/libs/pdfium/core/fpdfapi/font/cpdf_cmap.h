// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_FONT_CPDF_CMAP_H_
#define CORE_FPDFAPI_FONT_CPDF_CMAP_H_

#include <stdint.h>

#include <array>
#include <vector>

#include "core/fpdfapi/font/cpdf_cidfont.h"
#include "core/fxcrt/fixed_size_data_vector.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/span.h"
#include "core/fxcrt/unowned_ptr.h"

namespace fxcmap {
struct CMap;
}

enum class CIDCoding : uint8_t {
  kUNKNOWN = 0,
  kGB,
  kBIG5,
  kJIS,
  kKOREA,
  kUCS2,
  kCID,
  kUTF16,
};

class CPDF_CMap final : public Retainable {
 public:
  static constexpr size_t kDirectMapTableSize = 65536;

  enum CodingScheme : uint8_t {
    OneByte,
    TwoBytes,
    MixedTwoBytes,
    MixedFourBytes
  };

  struct CodeRange {
    size_t m_CharSize;
    std::array<uint8_t, 4> m_Lower;
    std::array<uint8_t, 4> m_Upper;
  };

  struct CIDRange {
    uint32_t m_StartCode;
    uint32_t m_EndCode;
    uint16_t m_StartCID;
  };

  CONSTRUCT_VIA_MAKE_RETAIN;

  bool IsLoaded() const { return m_bLoaded; }
  bool IsVertWriting() const { return m_bVertical; }

  uint16_t CIDFromCharCode(uint32_t charcode) const;

  int GetCharSize(uint32_t charcode) const;
  uint32_t GetNextChar(ByteStringView pString, size_t* pOffset) const;
  size_t CountChar(ByteStringView pString) const;
  void AppendChar(ByteString* str, uint32_t charcode) const;

  void SetVertical(bool vert) { m_bVertical = vert; }
  void SetCodingScheme(CodingScheme scheme) { m_CodingScheme = scheme; }
  void SetAdditionalMappings(std::vector<CIDRange> mappings);
  void SetMixedFourByteLeadingRanges(std::vector<CodeRange> ranges);

  CIDCoding GetCoding() const { return m_Coding; }
  const fxcmap::CMap* GetEmbedMap() const { return m_pEmbedMap; }
  CIDSet GetCharset() const { return m_Charset; }
  void SetCharset(CIDSet set) { m_Charset = set; }

  void SetDirectCharcodeToCIDTableRange(uint32_t start_code,
                                        uint32_t end_code,
                                        uint16_t start_cid);
  bool IsDirectCharcodeToCIDTableIsEmpty() const {
    return m_DirectCharcodeToCIDTable.empty();
  }

 private:
  explicit CPDF_CMap(ByteStringView bsPredefinedName);
  explicit CPDF_CMap(pdfium::span<const uint8_t> spEmbeddedData);
  ~CPDF_CMap() override;

  bool m_bLoaded = false;
  bool m_bVertical = false;
  CIDSet m_Charset = CIDSET_UNKNOWN;
  CodingScheme m_CodingScheme = TwoBytes;
  CIDCoding m_Coding = CIDCoding::kUNKNOWN;
  std::vector<bool> m_MixedTwoByteLeadingBytes;
  std::vector<CodeRange> m_MixedFourByteLeadingRanges;
  FixedSizeDataVector<uint16_t> m_DirectCharcodeToCIDTable;
  std::vector<CIDRange> m_AdditionalCharcodeToCIDMappings;
  UnownedPtr<const fxcmap::CMap> m_pEmbedMap;
};

#endif  // CORE_FPDFAPI_FONT_CPDF_CMAP_H_
