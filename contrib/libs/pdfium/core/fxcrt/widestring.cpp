// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcrt/widestring.h"

#include <stddef.h>
#include <string.h>
#include <wchar.h>

#include <algorithm>
#include <sstream>

#include "core/fxcrt/check.h"
#include "core/fxcrt/check_op.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/fx_codepage.h"
#include "core/fxcrt/fx_extension.h"
#include "core/fxcrt/fx_memcpy_wrappers.h"
#include "core/fxcrt/fx_safe_types.h"
#include "core/fxcrt/fx_system.h"
#include "core/fxcrt/numerics/safe_math.h"
#include "core/fxcrt/span_util.h"
#include "core/fxcrt/string_pool_template.h"
#include "core/fxcrt/utf16.h"

// Instantiate.
template class fxcrt::StringViewTemplate<wchar_t>;
template class fxcrt::StringPoolTemplate<WideString>;
template struct std::hash<WideString>;

#define FORCE_ANSI 0x10000
#define FORCE_UNICODE 0x20000
#define FORCE_INT64 0x40000

namespace {

#if defined(WCHAR_T_IS_32_BIT)
size_t FuseSurrogates(pdfium::span<wchar_t> s) {
  size_t dest_pos = 0;
  for (size_t i = 0; i < s.size(); ++i) {
    // TODO(crbug.com/pdfium/2031): Always use UTF-16.
    if (pdfium::IsHighSurrogate(s[i]) && i + 1 < s.size() &&
        pdfium::IsLowSurrogate(s[i + 1])) {
      s[dest_pos++] = pdfium::SurrogatePair(s[i], s[i + 1]).ToCodePoint();
      ++i;
      continue;
    }
    s[dest_pos++] = s[i];
  }
  return dest_pos;
}
#endif  // defined(WCHAR_T_IS_32_BIT)

constexpr wchar_t kWideTrimChars[] = L"\x09\x0a\x0b\x0c\x0d\x20";

std::optional<size_t> GuessSizeForVSWPrintf(const wchar_t* pFormat,
                                            va_list argList) {
  size_t nMaxLen = 0;
  UNSAFE_TODO({
    for (const wchar_t* pStr = pFormat; *pStr != 0; pStr++) {
      if (*pStr != '%' || *(pStr = pStr + 1) == '%') {
        ++nMaxLen;
        continue;
      }
      int iWidth = 0;
      for (; *pStr != 0; pStr++) {
        if (*pStr == '#') {
          nMaxLen += 2;
        } else if (*pStr == '*') {
          iWidth = va_arg(argList, int);
        } else if (*pStr != '-' && *pStr != '+' && *pStr != '0' &&
                   *pStr != ' ') {
          break;
        }
      }
      if (iWidth == 0) {
        iWidth = FXSYS_wtoi(pStr);
        while (FXSYS_IsDecimalDigit(*pStr))
          ++pStr;
      }
      if (iWidth < 0 || iWidth > 128 * 1024) {
        return std::nullopt;
      }
      uint32_t nWidth = static_cast<uint32_t>(iWidth);
      int iPrecision = 0;
      if (*pStr == '.') {
        pStr++;
        if (*pStr == '*') {
          iPrecision = va_arg(argList, int);
          pStr++;
        } else {
          iPrecision = FXSYS_wtoi(pStr);
          while (FXSYS_IsDecimalDigit(*pStr)) {
            ++pStr;
          }
        }
      }
      if (iPrecision < 0 || iPrecision > 128 * 1024) {
        return std::nullopt;
      }
      uint32_t nPrecision = static_cast<uint32_t>(iPrecision);
      int nModifier = 0;
      if (*pStr == L'I' && *(pStr + 1) == L'6' && *(pStr + 2) == L'4') {
        pStr += 3;
        nModifier = FORCE_INT64;
      } else {
        switch (*pStr) {
          case 'h':
            nModifier = FORCE_ANSI;
            pStr++;
            break;
          case 'l':
            nModifier = FORCE_UNICODE;
            pStr++;
            break;
          case 'F':
          case 'N':
          case 'L':
            pStr++;
            break;
        }
      }
      size_t nItemLen = 0;
      switch (*pStr | nModifier) {
        case 'c':
        case 'C':
          nItemLen = 2;
          va_arg(argList, int);
          break;
        case 'c' | FORCE_ANSI:
        case 'C' | FORCE_ANSI:
          nItemLen = 2;
          va_arg(argList, int);
          break;
        case 'c' | FORCE_UNICODE:
        case 'C' | FORCE_UNICODE:
          nItemLen = 2;
          va_arg(argList, int);
          break;
        case 's': {
          const wchar_t* pstrNextArg = va_arg(argList, const wchar_t*);
          if (pstrNextArg) {
            nItemLen = wcslen(pstrNextArg);
            if (nItemLen < 1) {
              nItemLen = 1;
            }
          } else {
            nItemLen = 6;
          }
        } break;
        case 'S': {
          const char* pstrNextArg = va_arg(argList, const char*);
          if (pstrNextArg) {
            nItemLen = strlen(pstrNextArg);
            if (nItemLen < 1) {
              nItemLen = 1;
            }
          } else {
            nItemLen = 6;
          }
        } break;
        case 's' | FORCE_ANSI:
        case 'S' | FORCE_ANSI: {
          const char* pstrNextArg = va_arg(argList, const char*);
          if (pstrNextArg) {
            nItemLen = strlen(pstrNextArg);
            if (nItemLen < 1) {
              nItemLen = 1;
            }
          } else {
            nItemLen = 6;
          }
        } break;
        case 's' | FORCE_UNICODE:
        case 'S' | FORCE_UNICODE: {
          const wchar_t* pstrNextArg = va_arg(argList, wchar_t*);
          if (pstrNextArg) {
            nItemLen = wcslen(pstrNextArg);
            if (nItemLen < 1) {
              nItemLen = 1;
            }
          } else {
            nItemLen = 6;
          }
        } break;
      }
      if (nItemLen != 0) {
        if (nPrecision != 0 && nItemLen > nPrecision) {
          nItemLen = nPrecision;
        }
        if (nItemLen < nWidth) {
          nItemLen = nWidth;
        }
      } else {
        switch (*pStr) {
          case 'd':
          case 'i':
          case 'u':
          case 'x':
          case 'X':
          case 'o':
            if (nModifier & FORCE_INT64) {
              va_arg(argList, int64_t);
            } else {
              va_arg(argList, int);
            }
            nItemLen = 32;
            if (nItemLen < nWidth + nPrecision) {
              nItemLen = nWidth + nPrecision;
            }
            break;
          case 'a':
          case 'A':
          case 'e':
          case 'E':
          case 'g':
          case 'G':
            va_arg(argList, double);
            nItemLen = 128;
            if (nItemLen < nWidth + nPrecision) {
              nItemLen = nWidth + nPrecision;
            }
            break;
          case 'f':
            if (nWidth + nPrecision > 100) {
              nItemLen = nPrecision + nWidth + 128;
            } else {
              double f;
              char pszTemp[256];
              f = va_arg(argList, double);
              FXSYS_snprintf(pszTemp, sizeof(pszTemp), "%*.*f", nWidth,
                             nPrecision + 6, f);
              nItemLen = strlen(pszTemp);
            }
            break;
          case 'p':
            va_arg(argList, void*);
            nItemLen = 32;
            if (nItemLen < nWidth + nPrecision) {
              nItemLen = nWidth + nPrecision;
            }
            break;
          case 'n':
            va_arg(argList, int*);
            break;
        }
      }
      nMaxLen += nItemLen;
    }
  });
  nMaxLen += 32;  // Fudge factor.
  return nMaxLen;
}

// Returns string unless we ran out of space.
std::optional<WideString> TryVSWPrintf(size_t size,
                                       const wchar_t* pFormat,
                                       va_list argList) {
  if (!size)
    return std::nullopt;

  WideString str;
  {
    // Span's lifetime must end before ReleaseBuffer() below.
    pdfium::span<wchar_t> buffer = str.GetBuffer(size);

    // SAFETY: In the following two calls, there's always space in the
    // WideString for a terminating NUL that's not included in the span.
    // For vswprintf(), MSAN won't untaint the buffer on a truncated write's
    // -1 return code even though the buffer is written. Probably just as well
    // not to trust the vendor's implementation to write anything anyways.
    // See https://crbug.com/705912.
    UNSAFE_BUFFERS(
        FXSYS_memset(buffer.data(), 0, (size + 1) * sizeof(wchar_t)));
    int ret = vswprintf(buffer.data(), size + 1, pFormat, argList);
    bool bSufficientBuffer = ret >= 0 || buffer[size - 1] == 0;
    if (!bSufficientBuffer)
      return std::nullopt;
  }
  str.ReleaseBuffer(str.GetStringLength());
  return str;
}

// Appends a Unicode code point to a `WideString` using either UTF-16 or UTF-32,
// depending on the platform's definition of `wchar_t`.
//
// TODO(crbug.com/pdfium/2031): Always use UTF-16.
// TODO(crbug.com/pdfium/2041): Migrate to `WideString`.
void AppendCodePointToWideString(char32_t code_point, WideString& buffer) {
  if (code_point > pdfium::kMaximumSupplementaryCodePoint) {
    // Invalid code point above U+10FFFF.
    return;
  }

#if defined(WCHAR_T_IS_16_BIT)
  if (code_point < pdfium::kMinimumSupplementaryCodePoint) {
    buffer += static_cast<wchar_t>(code_point);
  } else {
    // Encode as UTF-16 surrogate pair.
    pdfium::SurrogatePair surrogate_pair(code_point);
    buffer += surrogate_pair.high();
    buffer += surrogate_pair.low();
  }
#else
  buffer += static_cast<wchar_t>(code_point);
#endif  // defined(WCHAR_T_IS_16_BIT)
}

WideString UTF8Decode(ByteStringView bsStr) {
  WideString buffer;

  int remaining = 0;
  char32_t code_point = 0;
  for (char byte : bsStr) {
    uint8_t code_unit = static_cast<uint8_t>(byte);
    if (code_unit < 0x80) {
      remaining = 0;
      AppendCodePointToWideString(code_unit, buffer);
    } else if (code_unit < 0xc0) {
      if (remaining > 0) {
        --remaining;
        code_point = (code_point << 6) | (code_unit & 0x3f);
        if (remaining == 0) {
          AppendCodePointToWideString(code_point, buffer);
        }
      }
    } else if (code_unit < 0xe0) {
      remaining = 1;
      code_point = code_unit & 0x1f;
    } else if (code_unit < 0xf0) {
      remaining = 2;
      code_point = code_unit & 0x0f;
    } else if (code_unit < 0xf8) {
      remaining = 3;
      code_point = code_unit & 0x07;
    } else {
      remaining = 0;
    }
  }

  return buffer;
}

}  // namespace

namespace fxcrt {

static_assert(sizeof(WideString) <= sizeof(wchar_t*),
              "Strings must not require more space than pointers");

// static
WideString WideString::FormatInteger(int i) {
  wchar_t wbuf[32];
  swprintf(wbuf, std::size(wbuf), L"%d", i);
  return WideString(wbuf);
}

// static
WideString WideString::FormatV(const wchar_t* format, va_list argList) {
  va_list argListCopy;
  va_copy(argListCopy, argList);
  auto guess = GuessSizeForVSWPrintf(format, argListCopy);
  va_end(argListCopy);

  if (!guess.has_value()) {
    return WideString();
  }
  int maxLen = pdfium::checked_cast<int>(guess.value());

  while (maxLen < 32 * 1024) {
    va_copy(argListCopy, argList);
    std::optional<WideString> ret =
        TryVSWPrintf(static_cast<size_t>(maxLen), format, argListCopy);
    va_end(argListCopy);
    if (ret.has_value())
      return ret.value();

    maxLen *= 2;
  }
  return WideString();
}

// static
WideString WideString::Format(const wchar_t* pFormat, ...) {
  va_list argList;
  va_start(argList, pFormat);
  WideString ret = FormatV(pFormat, argList);
  va_end(argList);
  return ret;
}

WideString::WideString(const wchar_t* pStr, size_t nLen) {
  if (nLen) {
    // SAFETY: caller ensures `pStr` points to al least `nLen` wchar_t.
    m_pData = StringData::Create(UNSAFE_BUFFERS(pdfium::make_span(pStr, nLen)));
  }
}

WideString::WideString(wchar_t ch) {
  m_pData = StringData::Create(1);
  m_pData->m_String[0] = ch;
}

WideString::WideString(const wchar_t* ptr)
    : WideString(ptr, ptr ? wcslen(ptr) : 0) {}

WideString::WideString(WideStringView stringSrc) {
  if (!stringSrc.IsEmpty()) {
    m_pData = StringData::Create(stringSrc.span());
  }
}

WideString::WideString(WideStringView str1, WideStringView str2) {
  FX_SAFE_SIZE_T nSafeLen = str1.GetLength();
  nSafeLen += str2.GetLength();

  size_t nNewLen = nSafeLen.ValueOrDie();
  if (nNewLen == 0)
    return;

  m_pData = StringData::Create(nNewLen);
  m_pData->CopyContents(str1.span());
  m_pData->CopyContentsAt(str1.GetLength(), str2.span());
}

WideString::WideString(const std::initializer_list<WideStringView>& list) {
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

WideString& WideString::operator=(const wchar_t* str) {
  if (!str || !str[0])
    clear();
  else
    AssignCopy(str, wcslen(str));

  return *this;
}

WideString& WideString::operator=(WideStringView str) {
  if (str.IsEmpty())
    clear();
  else
    AssignCopy(str.unterminated_c_str(), str.GetLength());

  return *this;
}

WideString& WideString::operator=(const WideString& that) {
  if (m_pData != that.m_pData)
    m_pData = that.m_pData;

  return *this;
}

WideString& WideString::operator=(WideString&& that) noexcept {
  if (m_pData != that.m_pData)
    m_pData = std::move(that.m_pData);

  return *this;
}

WideString& WideString::operator+=(const wchar_t* str) {
  if (str)
    Concat(str, wcslen(str));

  return *this;
}

WideString& WideString::operator+=(wchar_t ch) {
  Concat(&ch, 1);
  return *this;
}

WideString& WideString::operator+=(const WideString& str) {
  if (str.m_pData)
    Concat(str.m_pData->m_String, str.m_pData->m_nDataLength);

  return *this;
}

WideString& WideString::operator+=(WideStringView str) {
  if (!str.IsEmpty())
    Concat(str.unterminated_c_str(), str.GetLength());

  return *this;
}

bool WideString::operator==(const wchar_t* ptr) const {
  if (!m_pData)
    return !ptr || !ptr[0];

  if (!ptr)
    return m_pData->m_nDataLength == 0;

  // SAFTEY: `wsclen()` comparison ensures there are `m_nDataLength` wchars at
  // `ptr` before the terminator, and `m_nDataLength` is within `m_String`.
  return wcslen(ptr) == m_pData->m_nDataLength &&
         UNSAFE_BUFFERS(FXSYS_wmemcmp(ptr, m_pData->m_String,
                                      m_pData->m_nDataLength)) == 0;
}

bool WideString::operator==(WideStringView str) const {
  if (!m_pData)
    return str.IsEmpty();

  // SAFTEY: Comparison ensure there are `m_nDataLength` wchars in `str`
  // and `m_nDataLength is within `m_String`.
  return m_pData->m_nDataLength == str.GetLength() &&
         UNSAFE_BUFFERS(FXSYS_wmemcmp(
             m_pData->m_String, str.unterminated_c_str(), str.GetLength())) ==
             0;
}

bool WideString::operator==(const WideString& other) const {
  if (m_pData == other.m_pData)
    return true;

  if (IsEmpty())
    return other.IsEmpty();

  if (other.IsEmpty())
    return false;

  return other.m_pData->m_nDataLength == m_pData->m_nDataLength &&
         wmemcmp(other.m_pData->m_String, m_pData->m_String,
                 m_pData->m_nDataLength) == 0;
}

bool WideString::operator<(const wchar_t* ptr) const {
  return Compare(ptr) < 0;
}

bool WideString::operator<(WideStringView str) const {
  if (!m_pData && !str.unterminated_c_str())
    return false;
  if (c_str() == str.unterminated_c_str())
    return false;

  size_t len = GetLength();
  size_t other_len = str.GetLength();

  // SAFETY: Comparison limited to minimum valid length of either argument.
  int result = UNSAFE_BUFFERS(FXSYS_wmemcmp(c_str(), str.unterminated_c_str(),
                                            std::min(len, other_len)));
  return result < 0 || (result == 0 && len < other_len);
}

bool WideString::operator<(const WideString& other) const {
  return Compare(other) < 0;
}

intptr_t WideString::ReferenceCountForTesting() const {
  return m_pData ? m_pData->m_nRefs : 0;
}

ByteString WideString::ToASCII() const {
  ByteString result;
  result.Reserve(GetLength());
  for (wchar_t wc : *this)
    result.InsertAtBack(static_cast<char>(wc & 0x7f));
  return result;
}

ByteString WideString::ToLatin1() const {
  ByteString result;
  result.Reserve(GetLength());
  for (wchar_t wc : *this)
    result.InsertAtBack(static_cast<char>(wc & 0xff));
  return result;
}

ByteString WideString::ToDefANSI() const {
  size_t dest_len =
      FX_WideCharToMultiByte(FX_CodePage::kDefANSI, AsStringView(), {});
  if (!dest_len)
    return ByteString();

  ByteString bstr;
  {
    // Span's lifetime must end before ReleaseBuffer() below.
    pdfium::span<char> dest_buf = bstr.GetBuffer(dest_len);
    FX_WideCharToMultiByte(FX_CodePage::kDefANSI, AsStringView(), dest_buf);
  }
  bstr.ReleaseBuffer(dest_len);
  return bstr;
}

ByteString WideString::ToUTF8() const {
  return FX_UTF8Encode(AsStringView());
}

ByteString WideString::ToUTF16LE() const {
  std::u16string utf16 = FX_UTF16Encode(AsStringView());
  ByteString result;
  size_t output_length = 0;
  {
    // Span's lifetime must end before ReleaseBuffer() below.
    // 2 bytes required per UTF-16 code unit.
    pdfium::span<uint8_t> buffer =
        pdfium::as_writable_bytes(result.GetBuffer(utf16.size() * 2 + 2));
    for (char16_t c : utf16) {
      buffer[output_length++] = c & 0xff;
      buffer[output_length++] = c >> 8;
    }
    buffer[output_length++] = 0;
    buffer[output_length++] = 0;
  }
  result.ReleaseBuffer(output_length);
  return result;
}

ByteString WideString::ToUCS2LE() const {
  ByteString result;
  size_t output_length = 0;
  {
    // Span's lifetime must end before ReleaseBuffer() below.
    // 2 bytes required per UTF-16 code unit.
    pdfium::span<uint8_t> buffer =
        pdfium::as_writable_bytes(result.GetBuffer(GetLength() * 2 + 2));
    for (wchar_t wc : AsStringView()) {
#if defined(WCHAR_T_IS_32_BIT)
      if (pdfium::IsSupplementary(wc)) {
        continue;
      }
#endif
      buffer[output_length++] = wc & 0xff;
      buffer[output_length++] = wc >> 8;
    }
    buffer[output_length++] = 0;
    buffer[output_length++] = 0;
  }
  result.ReleaseBuffer(output_length);
  return result;
}

WideString WideString::EncodeEntities() const {
  WideString ret = *this;
  ret.Replace(L"&", L"&amp;");
  ret.Replace(L"<", L"&lt;");
  ret.Replace(L">", L"&gt;");
  ret.Replace(L"\'", L"&apos;");
  ret.Replace(L"\"", L"&quot;");
  return ret;
}

WideString WideString::Substr(size_t offset) const {
  // Unsigned underflow is well-defined and out-of-range is handled by Substr().
  return Substr(offset, GetLength() - offset);
}

WideString WideString::Substr(size_t first, size_t count) const {
  if (!m_pData) {
    return WideString();
  }
  if (first == 0 && count == GetLength()) {
    return *this;
  }
  return WideString(AsStringView().Substr(first, count));
}

WideString WideString::First(size_t count) const {
  return Substr(0, count);
}

WideString WideString::Last(size_t count) const {
  // Unsigned underflow is well-defined and out-of-range is handled by Substr().
  return Substr(GetLength() - count, count);
}

void WideString::MakeLower() {
  if (IsEmpty())
    return;

  ReallocBeforeWrite(m_pData->m_nDataLength);
  FXSYS_wcslwr(m_pData->m_String);
}

void WideString::MakeUpper() {
  if (IsEmpty())
    return;

  ReallocBeforeWrite(m_pData->m_nDataLength);
  FXSYS_wcsupr(m_pData->m_String);
}

// static
WideString WideString::FromASCII(ByteStringView bstr) {
  WideString result;
  result.Reserve(bstr.GetLength());
  for (char c : bstr)
    result.InsertAtBack(static_cast<wchar_t>(c & 0x7f));
  return result;
}

// static
WideString WideString::FromLatin1(ByteStringView bstr) {
  WideString result;
  result.Reserve(bstr.GetLength());
  for (char c : bstr)
    result.InsertAtBack(static_cast<wchar_t>(c & 0xff));
  return result;
}

// static
WideString WideString::FromDefANSI(ByteStringView bstr) {
  size_t dest_len = FX_MultiByteToWideChar(FX_CodePage::kDefANSI, bstr, {});
  if (!dest_len)
    return WideString();

  WideString wstr;
  {
    // Span's lifetime must end before ReleaseBuffer() below.
    pdfium::span<wchar_t> dest_buf = wstr.GetBuffer(dest_len);
    FX_MultiByteToWideChar(FX_CodePage::kDefANSI, bstr, dest_buf);
  }
  wstr.ReleaseBuffer(dest_len);
  return wstr;
}

// static
WideString WideString::FromUTF8(ByteStringView str) {
  return UTF8Decode(str);
}

// static
WideString WideString::FromUTF16LE(pdfium::span<const uint8_t> data) {
  if (data.empty()) {
    return WideString();
  }

  WideString result;
  size_t length = 0;
  {
    // Span's lifetime must end before ReleaseBuffer() below.
    pdfium::span<wchar_t> buf = result.GetBuffer(data.size() / 2);
    for (size_t i = 0; i + 1 < data.size(); i += 2) {
      buf[length++] = data[i] | data[i + 1] << 8;
    }

#if defined(WCHAR_T_IS_32_BIT)
    length = FuseSurrogates(buf.first(length));
#endif
  }
  result.ReleaseBuffer(length);
  return result;
}

WideString WideString::FromUTF16BE(pdfium::span<const uint8_t> data) {
  if (data.empty()) {
    return WideString();
  }

  WideString result;
  size_t length = 0;
  {
    // Span's lifetime must end before ReleaseBuffer() below.
    pdfium::span<wchar_t> buf = result.GetBuffer(data.size() / 2);
    for (size_t i = 0; i + 1 < data.size(); i += 2) {
      buf[length++] = data[i] << 8 | data[i + 1];
    }

#if defined(WCHAR_T_IS_32_BIT)
    length = FuseSurrogates(buf.first(length));
#endif
  }
  result.ReleaseBuffer(length);
  return result;
}

int WideString::Compare(const wchar_t* str) const {
  if (m_pData)
    return str ? wcscmp(m_pData->m_String, str) : 1;
  return (!str || str[0] == 0) ? 0 : -1;
}

int WideString::Compare(const WideString& str) const {
  if (!m_pData)
    return str.m_pData ? -1 : 0;
  if (!str.m_pData)
    return 1;

  size_t this_len = m_pData->m_nDataLength;
  size_t that_len = str.m_pData->m_nDataLength;
  size_t min_len = std::min(this_len, that_len);

  // SAFTEY: Comparison limited to minimum length of either argument.
  int result = UNSAFE_BUFFERS(
      FXSYS_wmemcmp(m_pData->m_String, str.m_pData->m_String, min_len));
  if (result != 0)
    return result;
  if (this_len == that_len)
    return 0;
  return this_len < that_len ? -1 : 1;
}

int WideString::CompareNoCase(const wchar_t* str) const {
  if (m_pData)
    return str ? FXSYS_wcsicmp(m_pData->m_String, str) : 1;
  return (!str || str[0] == 0) ? 0 : -1;
}

void WideString::TrimWhitespace() {
  TrimWhitespaceBack();
  TrimWhitespaceFront();
}

void WideString::TrimWhitespaceFront() {
  TrimFront(kWideTrimChars);
}

void WideString::TrimWhitespaceBack() {
  TrimBack(kWideTrimChars);
}
int WideString::GetInteger() const {
  return m_pData ? FXSYS_wtoi(m_pData->m_String) : 0;
}

std::wostream& operator<<(std::wostream& os, const WideString& str) {
  return os.write(str.c_str(), str.GetLength());
}

std::ostream& operator<<(std::ostream& os, const WideString& str) {
  os << str.ToUTF8();
  return os;
}

std::wostream& operator<<(std::wostream& os, WideStringView str) {
  return os.write(str.unterminated_c_str(), str.GetLength());
}

std::ostream& operator<<(std::ostream& os, WideStringView str) {
  os << FX_UTF8Encode(str);
  return os;
}

}  // namespace fxcrt

uint32_t FX_HashCode_GetW(WideStringView str) {
  uint32_t dwHashCode = 0;
  for (WideStringView::UnsignedType c : str)
    dwHashCode = 1313 * dwHashCode + c;
  return dwHashCode;
}

uint32_t FX_HashCode_GetLoweredW(WideStringView str) {
  uint32_t dwHashCode = 0;
  for (wchar_t c : str)  // match FXSYS_towlower() arg type.
    dwHashCode = 1313 * dwHashCode + FXSYS_towlower(c);
  return dwHashCode;
}
