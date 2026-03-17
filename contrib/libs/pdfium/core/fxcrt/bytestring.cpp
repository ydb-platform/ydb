// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcrt/bytestring.h"

#include <ctype.h>
#include <stddef.h>

#include <algorithm>
#include <sstream>
#include <string>
#include <utility>

#include "core/fxcrt/check.h"
#include "core/fxcrt/check_op.h"
#include "core/fxcrt/fx_codepage.h"
#include "core/fxcrt/fx_extension.h"
#include "core/fxcrt/fx_memcpy_wrappers.h"
#include "core/fxcrt/fx_safe_types.h"
#include "core/fxcrt/fx_system.h"
#include "core/fxcrt/span.h"
#include "core/fxcrt/span_util.h"
#include "core/fxcrt/string_pool_template.h"

// Instantiate.
template class fxcrt::StringViewTemplate<char>;
template class fxcrt::StringPoolTemplate<ByteString>;
template struct std::hash<ByteString>;

namespace {

constexpr char kTrimChars[] = "\x09\x0a\x0b\x0c\x0d\x20";

}  // namespace

namespace fxcrt {

static_assert(sizeof(ByteString) <= sizeof(char*),
              "Strings must not require more space than pointers");

// static
ByteString ByteString::FormatInteger(int i) {
  char buf[32];
  FXSYS_snprintf(buf, sizeof(buf), "%d", i);
  return ByteString(buf);
}

// static
ByteString ByteString::FormatV(const char* pFormat, va_list argList) {
  va_list argListCopy;
  va_copy(argListCopy, argList);
  int nMaxLen = vsnprintf(nullptr, 0, pFormat, argListCopy);
  va_end(argListCopy);

  if (nMaxLen <= 0)
    return ByteString();

  ByteString ret;
  {
    // Span's lifetime must end before ReleaseBuffer() below.
    pdfium::span<char> buf = ret.GetBuffer(nMaxLen);

    // SAFETY: In the following two calls, there's always space in the buffer
    // for a terminating NUL that's not included in nMaxLen, and hence not
    // included in the span.
    UNSAFE_BUFFERS(FXSYS_memset(buf.data(), 0, nMaxLen + 1));
    va_copy(argListCopy, argList);
    vsnprintf(buf.data(), nMaxLen + 1, pFormat, argListCopy);
    va_end(argListCopy);
  }
  ret.ReleaseBuffer(ret.GetStringLength());
  return ret;
}

// static
ByteString ByteString::Format(const char* pFormat, ...) {
  va_list argList;
  va_start(argList, pFormat);
  ByteString ret = FormatV(pFormat, argList);
  va_end(argList);

  return ret;
}

ByteString::ByteString(const char* pStr, size_t nLen) {
  if (nLen) {
    // SAFETY: caller ensures `pStr` points to at least `nLen` chars.
    m_pData = StringData::Create(UNSAFE_BUFFERS(pdfium::make_span(pStr, nLen)));
  }
}

ByteString::ByteString(const uint8_t* pStr, size_t nLen)
    : ByteString(reinterpret_cast<const char*>(pStr), nLen) {}

ByteString::ByteString(char ch) {
  m_pData = StringData::Create(1);
  m_pData->m_String[0] = ch;
}

ByteString::ByteString(const char* ptr)
    : ByteString(ptr, ptr ? strlen(ptr) : 0) {}

ByteString::ByteString(ByteStringView bstrc) {
  if (!bstrc.IsEmpty()) {
    m_pData = StringData::Create(bstrc.span());
  }
}

ByteString::ByteString(ByteStringView str1, ByteStringView str2) {
  FX_SAFE_SIZE_T nSafeLen = str1.GetLength();
  nSafeLen += str2.GetLength();

  size_t nNewLen = nSafeLen.ValueOrDie();
  if (nNewLen == 0)
    return;

  m_pData = StringData::Create(nNewLen);
  m_pData->CopyContents(str1.span());
  m_pData->CopyContentsAt(str1.GetLength(), str2.span());
}

ByteString::ByteString(const std::initializer_list<ByteStringView>& list) {
  FX_SAFE_SIZE_T nSafeLen = 0;
  for (const auto& item : list)
    nSafeLen += item.GetLength();

  size_t nNewLen = nSafeLen.ValueOrDie();
  if (nNewLen == 0)
    return;

  m_pData = StringData::Create(nNewLen);

  size_t nOffset = 0;
  for (const auto& item : list) {
    m_pData->CopyContentsAt(nOffset, item.span());
    nOffset += item.GetLength();
  }
}

ByteString::ByteString(const fxcrt::ostringstream& outStream) {
  auto str = outStream.str();
  if (!str.empty()) {
    m_pData = StringData::Create(pdfium::make_span(str));
  }
}

ByteString& ByteString::operator=(const char* str) {
  if (!str || !str[0])
    clear();
  else
    AssignCopy(str, strlen(str));

  return *this;
}

ByteString& ByteString::operator=(ByteStringView str) {
  if (str.IsEmpty())
    clear();
  else
    AssignCopy(str.unterminated_c_str(), str.GetLength());

  return *this;
}

ByteString& ByteString::operator=(const ByteString& that) {
  if (m_pData != that.m_pData)
    m_pData = that.m_pData;

  return *this;
}

ByteString& ByteString::operator=(ByteString&& that) noexcept {
  if (m_pData != that.m_pData)
    m_pData = std::move(that.m_pData);

  return *this;
}

ByteString& ByteString::operator+=(const char* str) {
  if (str)
    Concat(str, strlen(str));

  return *this;
}

ByteString& ByteString::operator+=(char ch) {
  Concat(&ch, 1);
  return *this;
}

ByteString& ByteString::operator+=(const ByteString& str) {
  if (str.m_pData)
    Concat(str.m_pData->m_String, str.m_pData->m_nDataLength);

  return *this;
}

ByteString& ByteString::operator+=(ByteStringView str) {
  if (!str.IsEmpty())
    Concat(str.unterminated_c_str(), str.GetLength());

  return *this;
}

bool ByteString::operator==(const char* ptr) const {
  if (!m_pData)
    return !ptr || !ptr[0];

  if (!ptr)
    return m_pData->m_nDataLength == 0;

  // SAFETY: `m_nDataLength` is within `m_String`, and the strlen() call
  // ensures there are `m_nDataLength` bytes at `ptr` before the terminator.
  return strlen(ptr) == m_pData->m_nDataLength &&
         UNSAFE_BUFFERS(
             FXSYS_memcmp(ptr, m_pData->m_String, m_pData->m_nDataLength)) == 0;
}

bool ByteString::operator==(ByteStringView str) const {
  if (!m_pData)
    return str.IsEmpty();

  // SAFETY: `str` has `GetLength()` valid bytes in `unterminated_c_str()`,
  // `m_nDataLength` is within `m_String`, and equality comparison.
  return m_pData->m_nDataLength == str.GetLength() &&
         UNSAFE_BUFFERS(FXSYS_memcmp(
             m_pData->m_String, str.unterminated_c_str(), str.GetLength())) ==
             0;
}

bool ByteString::operator==(const ByteString& other) const {
  if (m_pData == other.m_pData)
    return true;

  if (IsEmpty())
    return other.IsEmpty();

  if (other.IsEmpty())
    return false;

  return other.m_pData->m_nDataLength == m_pData->m_nDataLength &&
         memcmp(other.m_pData->m_String, m_pData->m_String,
                m_pData->m_nDataLength) == 0;
}

bool ByteString::operator<(const char* ptr) const {
  if (!m_pData && !ptr)
    return false;
  if (c_str() == ptr)
    return false;

  size_t len = GetLength();
  size_t other_len = ptr ? strlen(ptr) : 0;

  // SAFETY: Comparison limited to minimum valid length of either argument.
  int result =
      UNSAFE_BUFFERS(FXSYS_memcmp(c_str(), ptr, std::min(len, other_len)));
  return result < 0 || (result == 0 && len < other_len);
}

bool ByteString::operator<(ByteStringView str) const {
  return Compare(str) < 0;
}

bool ByteString::operator<(const ByteString& other) const {
  if (m_pData == other.m_pData)
    return false;

  size_t len = GetLength();
  size_t other_len = other.GetLength();

  // SAFETY: Comparison limited to minimum valid length of either argument.
  int result = UNSAFE_BUFFERS(
      FXSYS_memcmp(c_str(), other.c_str(), std::min(len, other_len)));
  return result < 0 || (result == 0 && len < other_len);
}

bool ByteString::EqualNoCase(ByteStringView str) const {
  if (!m_pData) {
    return str.IsEmpty();
  }
  if (m_pData->m_nDataLength != str.GetLength()) {
    return false;
  }
  pdfium::span<const uint8_t> this_span = pdfium::as_bytes(m_pData->span());
  pdfium::span<const uint8_t> that_span = str.unsigned_span();
  while (!this_span.empty()) {
    uint8_t this_char = this_span.front();
    uint8_t that_char = that_span.front();
    if (this_char != that_char && tolower(this_char) != tolower(that_char)) {
      return false;
    }
    this_span = this_span.subspan(1);
    that_span = that_span.subspan(1);
  }
  return true;
}

intptr_t ByteString::ReferenceCountForTesting() const {
  return m_pData ? m_pData->m_nRefs : 0;
}

ByteString ByteString::Substr(size_t offset) const {
  // Unsigned underflow is well-defined and out-of-range is handled by Substr().
  return Substr(offset, GetLength() - offset);
}

ByteString ByteString::Substr(size_t first, size_t count) const {
  if (!m_pData) {
    return ByteString();
  }
  if (first == 0 && count == m_pData->m_nDataLength) {
    return *this;
  }
  return ByteString(AsStringView().Substr(first, count));
}

ByteString ByteString::First(size_t count) const {
  return Substr(0, count);
}

ByteString ByteString::Last(size_t count) const {
  // Unsigned underflow is well-defined and out-of-range is handled by Substr().
  return Substr(GetLength() - count, count);
}

void ByteString::MakeLower() {
  if (IsEmpty())
    return;

  ReallocBeforeWrite(m_pData->m_nDataLength);
  FXSYS_strlwr(m_pData->m_String);
}

void ByteString::MakeUpper() {
  if (IsEmpty())
    return;

  ReallocBeforeWrite(m_pData->m_nDataLength);
  FXSYS_strupr(m_pData->m_String);
}

int ByteString::Compare(ByteStringView str) const {
  if (!m_pData)
    return str.IsEmpty() ? 0 : -1;

  size_t this_len = m_pData->m_nDataLength;
  size_t that_len = str.GetLength();
  size_t min_len = std::min(this_len, that_len);

  // SAFETY: Comparison limited to minimum valid length of either argument.
  int result = UNSAFE_BUFFERS(
      FXSYS_memcmp(m_pData->m_String, str.unterminated_c_str(), min_len));
  if (result != 0)
    return result;
  if (this_len == that_len)
    return 0;
  return this_len < that_len ? -1 : 1;
}

void ByteString::TrimWhitespace() {
  TrimWhitespaceBack();
  TrimWhitespaceFront();
}

void ByteString::TrimWhitespaceFront() {
  TrimFront(kTrimChars);
}

void ByteString::TrimWhitespaceBack() {
  TrimBack(kTrimChars);
}

std::ostream& operator<<(std::ostream& os, const ByteString& str) {
  return os.write(str.c_str(), str.GetLength());
}

std::ostream& operator<<(std::ostream& os, ByteStringView str) {
  return os.write(str.unterminated_c_str(), str.GetLength());
}

}  // namespace fxcrt

uint32_t FX_HashCode_GetA(ByteStringView str) {
  uint32_t dwHashCode = 0;
  for (ByteStringView::UnsignedType c : str)
    dwHashCode = 31 * dwHashCode + c;
  return dwHashCode;
}

uint32_t FX_HashCode_GetLoweredA(ByteStringView str) {
  uint32_t dwHashCode = 0;
  for (ByteStringView::UnsignedType c : str)
    dwHashCode = 31 * dwHashCode + tolower(c);
  return dwHashCode;
}

uint32_t FX_HashCode_GetAsIfW(ByteStringView str) {
  uint32_t dwHashCode = 0;
  for (ByteStringView::UnsignedType c : str)
    dwHashCode = 1313 * dwHashCode + c;
  return dwHashCode;
}

uint32_t FX_HashCode_GetLoweredAsIfW(ByteStringView str) {
  uint32_t dwHashCode = 0;
  for (ByteStringView::UnsignedType c : str)
    dwHashCode = 1313 * dwHashCode + FXSYS_towlower(c);
  return dwHashCode;
}
