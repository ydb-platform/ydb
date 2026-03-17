// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCRT_FX_EXTENSION_H_
#define CORE_FXCRT_FX_EXTENSION_H_

#include <ctype.h>
#include <math.h>
#include <time.h>
#include <wctype.h>

#include "build/build_config.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/widestring.h"

#if defined(USE_SYSTEM_ICUUC)
#include <unicode/uchar.h>
#else
#include <unicode/uchar.h>
#endif

#define FX_INVALID_OFFSET static_cast<uint32_t>(-1)

#define FX_IsOdd(a) ((a)&1)

float FXSYS_wcstof(WideStringView pwsStr, size_t* pUsedLen);

UNSAFE_BUFFER_USAGE wchar_t* FXSYS_wcsncpy(wchar_t* dstStr,
                                           const wchar_t* srcStr,
                                           size_t count);

inline bool FXSYS_iswlower(int32_t c) {
  return u_islower(c);
}

inline bool FXSYS_iswupper(int32_t c) {
  return u_isupper(c);
}

inline int32_t FXSYS_towlower(wchar_t c) {
  return u_tolower(c);
}

inline int32_t FXSYS_towupper(wchar_t c) {
  return u_toupper(c);
}

inline bool FXSYS_IsLowerASCII(int32_t c) {
  return c >= 'a' && c <= 'z';
}

inline bool FXSYS_IsUpperASCII(int32_t c) {
  return c >= 'A' && c <= 'Z';
}

inline char FXSYS_ToUpperASCII(char c) {
  return FXSYS_IsLowerASCII(c) ? (c + ('A' - 'a')) : c;
}

inline bool FXSYS_iswalpha(wchar_t c) {
  return u_isalpha(c);
}

inline bool FXSYS_iswalnum(wchar_t c) {
  return u_isalnum(c);
}

inline bool FXSYS_iswspace(wchar_t c) {
  return u_isspace(c);
}

inline bool FXSYS_IsOctalDigit(char c) {
  return c >= '0' && c <= '7';
}

inline bool FXSYS_IsHexDigit(char c) {
  return !((c & 0x80) || !isxdigit(c));
}

inline bool FXSYS_IsWideHexDigit(wchar_t c) {
  return !((c & 0xFFFFFF80) || !isxdigit(c));
}

inline int FXSYS_HexCharToInt(char c) {
  if (!FXSYS_IsHexDigit(c))
    return 0;
  char upchar = FXSYS_ToUpperASCII(c);
  return upchar > '9' ? upchar - 'A' + 10 : upchar - '0';
}

inline int FXSYS_WideHexCharToInt(wchar_t c) {
  if (!FXSYS_IsWideHexDigit(c))
    return 0;
  char upchar = toupper(static_cast<char>(c));
  return upchar > '9' ? upchar - 'A' + 10 : upchar - '0';
}

inline bool FXSYS_IsDecimalDigit(char c) {
  return !((c & 0x80) || !isdigit(c));
}

inline bool FXSYS_IsDecimalDigit(wchar_t c) {
  return !((c & 0xFFFFFF80) || !iswdigit(c));
}

inline int FXSYS_DecimalCharToInt(char c) {
  return FXSYS_IsDecimalDigit(c) ? c - '0' : 0;
}

inline int FXSYS_DecimalCharToInt(wchar_t c) {
  return FXSYS_IsDecimalDigit(c) ? c - L'0' : 0;
}

void FXSYS_IntToTwoHexChars(uint8_t n, char* buf);
void FXSYS_IntToFourHexChars(uint16_t n, char* buf);

size_t FXSYS_ToUTF16BE(uint32_t unicode, char* buf);

// Strict order over floating types where NaNs may be present.
// All NaNs are treated as equal to each other and greater than infinity.
template <typename T>
bool FXSYS_SafeEQ(const T& lhs, const T& rhs) {
  return (isnan(lhs) && isnan(rhs)) ||
         (!isnan(lhs) && !isnan(rhs) && lhs == rhs);
}

template <typename T>
bool FXSYS_SafeLT(const T& lhs, const T& rhs) {
  if (isnan(lhs) && isnan(rhs))
    return false;
  if (isnan(lhs) || isnan(rhs))
    return isnan(lhs) < isnan(rhs);
  return lhs < rhs;
}

// Override time/localtime functions for test consistency.
void FXSYS_SetTimeFunction(time_t (*func)());
void FXSYS_SetLocaltimeFunction(struct tm* (*func)(const time_t*));

// Replacements for time/localtime that respect overrides.
time_t FXSYS_time(time_t* tloc);
struct tm* FXSYS_localtime(const time_t* tp);

#endif  // CORE_FXCRT_FX_EXTENSION_H_
