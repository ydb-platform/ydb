// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PARSER_FPDF_PARSER_UTILITY_H_
#define CORE_FPDFAPI_PARSER_FPDF_PARSER_UTILITY_H_

#include <iosfwd>
#include <limits>
#include <optional>
#include <vector>

#include "core/fxcrt/bytestring.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/retain_ptr.h"

class CPDF_Array;
class CPDF_Dictionary;
class CPDF_Object;
class IFX_SeekableReadStream;

// Use the accessors below instead of directly accessing kPDFCharTypes.
extern const char kPDFCharTypes[256];

inline uint8_t GetPDFCharTypeFromArray(uint8_t c) {
  static_assert(std::numeric_limits<decltype(c)>::min() == 0);
  static_assert(std::numeric_limits<decltype(c)>::max() <
                std::size(kPDFCharTypes));

  // SAFETY: previous static_asserts show table covers entire range
  // of the index's type.
  return UNSAFE_BUFFERS(kPDFCharTypes[c]);
}

inline bool PDFCharIsWhitespace(uint8_t c) {
  return GetPDFCharTypeFromArray(c) == 'W';
}

inline bool PDFCharIsNumeric(uint8_t c) {
  return GetPDFCharTypeFromArray(c) == 'N';
}

inline bool PDFCharIsDelimiter(uint8_t c) {
  return GetPDFCharTypeFromArray(c) == 'D';
}

inline bool PDFCharIsOther(uint8_t c) {
  return GetPDFCharTypeFromArray(c) == 'R';
}

inline bool PDFCharIsLineEnding(uint8_t c) {
  return c == '\r' || c == '\n';
}

// On success, return a positive offset value to the PDF header. If the header
// cannot be found, or if there is an error reading from |pFile|, then return
// nullopt.
std::optional<FX_FILESIZE> GetHeaderOffset(
    const RetainPtr<IFX_SeekableReadStream>& pFile);

ByteString PDF_NameDecode(ByteStringView orig);
ByteString PDF_NameEncode(const ByteString& orig);

// Return |nCount| elements from |pArray| as a vector of floats. |pArray| must
// have at least |nCount| elements.
std::vector<float> ReadArrayElementsToVector(const CPDF_Array* pArray,
                                             size_t nCount);

// Returns true if |dict| is non-null and has a /Type name entry that matches
// |type|.
bool ValidateDictType(const CPDF_Dictionary* dict, ByteStringView type);

// Returns true if |dict| is non-null and all entries in |dict| are dictionaries
// of |type|.
bool ValidateDictAllResourcesOfType(const CPDF_Dictionary* dict,
                                    ByteStringView type);

// Shorthand for ValidateDictAllResourcesOfType(dict, "Font").
bool ValidateFontResourceDict(const CPDF_Dictionary* dict);

// Like ValidateDictType(), but /Type can also not exist.
bool ValidateDictOptionalType(const CPDF_Dictionary* dict, ByteStringView type);

std::ostream& operator<<(std::ostream& buf, const CPDF_Object* pObj);

#endif  // CORE_FPDFAPI_PARSER_FPDF_PARSER_UTILITY_H_
