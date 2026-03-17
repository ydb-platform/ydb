// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCRT_WIDESTRING_H_
#define CORE_FXCRT_WIDESTRING_H_

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>

#include <iosfwd>
#include <utility>

#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/span.h"
#include "core/fxcrt/string_template.h"

namespace fxcrt {

class ByteString;

// A mutable string with shared buffers using copy-on-write semantics that
// avoids the cost of std::string's iterator stability guarantees.
// TODO(crbug.com/pdfium/2031): Consider switching to `char16_t` instead.
class WideString : public StringTemplate<wchar_t> {
 public:
  [[nodiscard]] static WideString FormatInteger(int i);
  [[nodiscard]] static WideString Format(const wchar_t* pFormat, ...);
  [[nodiscard]] static WideString FormatV(const wchar_t* lpszFormat,
                                          va_list argList);

  WideString() = default;
  WideString(const WideString& other) = default;

  // Move-construct a WideString. After construction, |other| is empty.
  WideString(WideString&& other) noexcept = default;

  ~WideString() = default;

  UNSAFE_BUFFER_USAGE WideString(const wchar_t* pStr, size_t len);

  // Make a one-character string from one wide char.
  explicit WideString(wchar_t ch);

  // Deliberately implicit to avoid calling on every string literal.
  // NOLINTNEXTLINE(runtime/explicit)
  WideString(const wchar_t* ptr);

  // No implicit conversions from byte strings.
  // NOLINTNEXTLINE(runtime/explicit)
  WideString(char) = delete;

  explicit WideString(WideStringView str);
  WideString(WideStringView str1, WideStringView str2);
  WideString(const std::initializer_list<WideStringView>& list);

  [[nodiscard]] static WideString FromASCII(ByteStringView str);
  [[nodiscard]] static WideString FromLatin1(ByteStringView str);
  [[nodiscard]] static WideString FromDefANSI(ByteStringView str);
  [[nodiscard]] static WideString FromUTF8(ByteStringView str);
  [[nodiscard]] static WideString FromUTF16LE(pdfium::span<const uint8_t> data);
  [[nodiscard]] static WideString FromUTF16BE(pdfium::span<const uint8_t> data);

  WideString& operator=(const wchar_t* str);
  WideString& operator=(WideStringView str);
  WideString& operator=(const WideString& that);

  // Move-assign a WideString. After assignment, |that| is empty.
  WideString& operator=(WideString&& that) noexcept;

  WideString& operator+=(const wchar_t* str);
  WideString& operator+=(wchar_t ch);
  WideString& operator+=(const WideString& str);
  WideString& operator+=(WideStringView str);

  bool operator==(const wchar_t* ptr) const;
  bool operator==(WideStringView str) const;
  bool operator==(const WideString& other) const;

  bool operator!=(const wchar_t* ptr) const { return !(*this == ptr); }
  bool operator!=(WideStringView str) const { return !(*this == str); }
  bool operator!=(const WideString& other) const { return !(*this == other); }

  bool operator<(const wchar_t* ptr) const;
  bool operator<(WideStringView str) const;
  bool operator<(const WideString& other) const;

  int Compare(const wchar_t* str) const;
  int Compare(const WideString& str) const;
  int CompareNoCase(const wchar_t* str) const;

  WideString Substr(size_t offset) const;
  WideString Substr(size_t first, size_t count) const;
  WideString First(size_t count) const;
  WideString Last(size_t count) const;

  void MakeLower();
  void MakeUpper();

  // Trim a canonical set of characters from the widestring.
  void TrimWhitespace();
  void TrimWhitespaceFront();
  void TrimWhitespaceBack();

  int GetInteger() const;

  bool IsASCII() const { return AsStringView().IsASCII(); }
  bool EqualsASCII(ByteStringView that) const {
    return AsStringView().EqualsASCII(that);
  }
  bool EqualsASCIINoCase(ByteStringView that) const {
    return AsStringView().EqualsASCIINoCase(that);
  }

  ByteString ToASCII() const;
  ByteString ToLatin1() const;
  ByteString ToDefANSI() const;
  ByteString ToUTF8() const;

  // These methods will add \0\0 to the end of the string to represent the
  // two-byte terminator. These values are part of the string itself, so
  // GetLength() will include them.
  ByteString ToUTF16LE() const;
  ByteString ToUCS2LE() const;

  // Replace the characters &<>'" with HTML entities.
  WideString EncodeEntities() const;

 protected:
  intptr_t ReferenceCountForTesting() const;

  friend class WideString_Assign_Test;
  friend class WideString_ConcatInPlace_Test;
  friend class WideString_Construct_Test;
  friend class StringPool_WideString_Test;
};

inline WideString operator+(WideStringView str1, WideStringView str2) {
  return WideString(str1, str2);
}
inline WideString operator+(WideStringView str1, const wchar_t* str2) {
  return WideString(str1, str2);
}
inline WideString operator+(const wchar_t* str1, WideStringView str2) {
  return WideString(str1, str2);
}
inline WideString operator+(WideStringView str1, wchar_t ch) {
  return WideString(str1, WideStringView(ch));
}
inline WideString operator+(wchar_t ch, WideStringView str2) {
  return WideString(WideStringView(ch), str2);
}
inline WideString operator+(const WideString& str1, const WideString& str2) {
  return WideString(str1.AsStringView(), str2.AsStringView());
}
inline WideString operator+(const WideString& str1, wchar_t ch) {
  return WideString(str1.AsStringView(), WideStringView(ch));
}
inline WideString operator+(wchar_t ch, const WideString& str2) {
  return WideString(WideStringView(ch), str2.AsStringView());
}
inline WideString operator+(const WideString& str1, const wchar_t* str2) {
  return WideString(str1.AsStringView(), str2);
}
inline WideString operator+(const wchar_t* str1, const WideString& str2) {
  return WideString(str1, str2.AsStringView());
}
inline WideString operator+(const WideString& str1, WideStringView str2) {
  return WideString(str1.AsStringView(), str2);
}
inline WideString operator+(WideStringView str1, const WideString& str2) {
  return WideString(str1, str2.AsStringView());
}
inline bool operator==(const wchar_t* lhs, const WideString& rhs) {
  return rhs == lhs;
}
inline bool operator==(WideStringView lhs, const WideString& rhs) {
  return rhs == lhs;
}
inline bool operator!=(const wchar_t* lhs, const WideString& rhs) {
  return rhs != lhs;
}
inline bool operator!=(WideStringView lhs, const WideString& rhs) {
  return rhs != lhs;
}
inline bool operator<(const wchar_t* lhs, const WideString& rhs) {
  return rhs.Compare(lhs) > 0;
}

std::wostream& operator<<(std::wostream& os, const WideString& str);
std::ostream& operator<<(std::ostream& os, const WideString& str);
std::wostream& operator<<(std::wostream& os, WideStringView str);
std::ostream& operator<<(std::ostream& os, WideStringView str);

// This is declared here for use in gtest-based tests but is defined in a test
// support target. This should not be used in production code. Just use
// operator<< from above instead.
// In some cases, gtest will automatically use operator<< as well, but in this
// case, it needs PrintTo() because WideString looks like a container to gtest.
void PrintTo(const WideString& str, std::ostream* os);

}  // namespace fxcrt

using WideString = fxcrt::WideString;

uint32_t FX_HashCode_GetW(WideStringView str);
uint32_t FX_HashCode_GetLoweredW(WideStringView str);

namespace std {

template <>
struct hash<WideString> {
  size_t operator()(const WideString& str) const {
    return FX_HashCode_GetW(str.AsStringView());
  }
};

}  // namespace std

extern template struct std::hash<WideString>;

#endif  // CORE_FXCRT_WIDESTRING_H_
