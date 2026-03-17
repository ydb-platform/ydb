// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCRT_BYTESTRING_H_
#define CORE_FXCRT_BYTESTRING_H_

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>

#include <iosfwd>
#include <utility>

#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/fx_string_wrappers.h"
#include "core/fxcrt/string_template.h"

namespace fxcrt {

// A mutable string with shared buffers using copy-on-write semantics that
// avoids the cost of std::string's iterator stability guarantees.
class ByteString : public StringTemplate<char> {
 public:
  [[nodiscard]] static ByteString FormatInteger(int i);
  [[nodiscard]] static ByteString Format(const char* pFormat, ...);
  [[nodiscard]] static ByteString FormatV(const char* pFormat, va_list argList);

  ByteString() = default;
  ByteString(const ByteString& other) = default;

  // Move-construct a ByteString. After construction, |other| is empty.
  ByteString(ByteString&& other) noexcept = default;

  ~ByteString() = default;

  UNSAFE_BUFFER_USAGE ByteString(const char* pStr, size_t len);
  UNSAFE_BUFFER_USAGE ByteString(const uint8_t* pStr, size_t len);

  // Make a one-character string from a char.
  explicit ByteString(char ch);

  // Deliberately implicit to avoid calling on every string literal.
  // NOLINTNEXTLINE(runtime/explicit)
  ByteString(const char* ptr);

  // No implicit conversions from wide strings.
  // NOLINTNEXTLINE(runtime/explicit)
  ByteString(wchar_t) = delete;

  explicit ByteString(ByteStringView bstrc);
  ByteString(ByteStringView str1, ByteStringView str2);
  ByteString(const std::initializer_list<ByteStringView>& list);
  explicit ByteString(const fxcrt::ostringstream& outStream);

  int Compare(ByteStringView str) const;
  bool EqualNoCase(ByteStringView str) const;

  bool operator==(const char* ptr) const;
  bool operator==(ByteStringView str) const;
  bool operator==(const ByteString& other) const;

  bool operator!=(const char* ptr) const { return !(*this == ptr); }
  bool operator!=(ByteStringView str) const { return !(*this == str); }
  bool operator!=(const ByteString& other) const { return !(*this == other); }

  bool operator<(const char* ptr) const;
  bool operator<(ByteStringView str) const;
  bool operator<(const ByteString& other) const;

  ByteString& operator=(const char* str);
  ByteString& operator=(ByteStringView str);
  ByteString& operator=(const ByteString& that);

  // Move-assign a ByteString. After assignment, |that| is empty.
  ByteString& operator=(ByteString&& that) noexcept;

  ByteString& operator+=(char ch);
  ByteString& operator+=(const char* str);
  ByteString& operator+=(const ByteString& str);
  ByteString& operator+=(ByteStringView str);

  ByteString Substr(size_t offset) const;
  ByteString Substr(size_t first, size_t count) const;
  ByteString First(size_t count) const;
  ByteString Last(size_t count) const;

  void MakeLower();
  void MakeUpper();

  // Remove a canonical set of characters from the string.
  void TrimWhitespace();
  void TrimWhitespaceBack();
  void TrimWhitespaceFront();

  uint32_t GetID() const { return AsStringView().GetID(); }

 protected:
  intptr_t ReferenceCountForTesting() const;

  friend class ByteString_Assign_Test;
  friend class ByteString_Concat_Test;
  friend class ByteString_Construct_Test;
  friend class StringPool_ByteString_Test;
};

inline bool operator==(const char* lhs, const ByteString& rhs) {
  return rhs == lhs;
}
inline bool operator==(ByteStringView lhs, const ByteString& rhs) {
  return rhs == lhs;
}
inline bool operator!=(const char* lhs, const ByteString& rhs) {
  return rhs != lhs;
}
inline bool operator!=(ByteStringView lhs, const ByteString& rhs) {
  return rhs != lhs;
}
inline bool operator<(const char* lhs, const ByteString& rhs) {
  return rhs.Compare(lhs) > 0;
}
inline bool operator<(const ByteStringView& lhs, const ByteString& rhs) {
  return rhs.Compare(lhs) > 0;
}
inline bool operator<(const ByteStringView& lhs, const char* rhs) {
  return lhs < ByteStringView(rhs);
}

inline ByteString operator+(ByteStringView str1, ByteStringView str2) {
  return ByteString(str1, str2);
}
inline ByteString operator+(ByteStringView str1, const char* str2) {
  return ByteString(str1, str2);
}
inline ByteString operator+(const char* str1, ByteStringView str2) {
  return ByteString(str1, str2);
}
inline ByteString operator+(ByteStringView str1, char ch) {
  return ByteString(str1, ByteStringView(ch));
}
inline ByteString operator+(char ch, ByteStringView str2) {
  return ByteString(ByteStringView(ch), str2);
}
inline ByteString operator+(const ByteString& str1, const ByteString& str2) {
  return ByteString(str1.AsStringView(), str2.AsStringView());
}
inline ByteString operator+(const ByteString& str1, char ch) {
  return ByteString(str1.AsStringView(), ByteStringView(ch));
}
inline ByteString operator+(char ch, const ByteString& str2) {
  return ByteString(ByteStringView(ch), str2.AsStringView());
}
inline ByteString operator+(const ByteString& str1, const char* str2) {
  return ByteString(str1.AsStringView(), str2);
}
inline ByteString operator+(const char* str1, const ByteString& str2) {
  return ByteString(str1, str2.AsStringView());
}
inline ByteString operator+(const ByteString& str1, ByteStringView str2) {
  return ByteString(str1.AsStringView(), str2);
}
inline ByteString operator+(ByteStringView str1, const ByteString& str2) {
  return ByteString(str1, str2.AsStringView());
}

std::ostream& operator<<(std::ostream& os, const ByteString& str);
std::ostream& operator<<(std::ostream& os, ByteStringView str);

// This is declared here for use in gtest-based tests but is defined in a test
// support target. This should not be used in production code. Just use
// operator<< from above instead.
// In some cases, gtest will automatically use operator<< as well, but in this
// case, it needs PrintTo() because ByteString looks like a container to gtest.
void PrintTo(const ByteString& str, std::ostream* os);

}  // namespace fxcrt

using ByteString = fxcrt::ByteString;

uint32_t FX_HashCode_GetA(ByteStringView str);
uint32_t FX_HashCode_GetLoweredA(ByteStringView str);
uint32_t FX_HashCode_GetAsIfW(ByteStringView str);
uint32_t FX_HashCode_GetLoweredAsIfW(ByteStringView str);

namespace std {

template <>
struct hash<ByteString> {
  size_t operator()(const ByteString& str) const {
    return FX_HashCode_GetA(str.AsStringView());
  }
};

}  // namespace std

extern template struct std::hash<ByteString>;

#endif  // CORE_FXCRT_BYTESTRING_H_
