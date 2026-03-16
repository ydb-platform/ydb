// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcrt/fx_extension.h"

#include <wchar.h>

#include "core/fxcrt/check.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/fx_system.h"
#include "core/fxcrt/utf16.h"
#include "core/fxcrt/widestring.h"

namespace {

time_t DefaultTimeFunction() {
  return time(nullptr);
}

struct tm* DefaultLocaltimeFunction(const time_t* tp) {
  return localtime(tp);
}

time_t (*g_time_func)() = DefaultTimeFunction;
struct tm* (*g_localtime_func)(const time_t*) = DefaultLocaltimeFunction;

}  // namespace

float FXSYS_wcstof(WideStringView pwsStr, size_t* pUsedLen) {
  // Force NUL-termination via copied buffer.
  auto copied = WideString(pwsStr);
  wchar_t* endptr = nullptr;
  float result = wcstof(copied.c_str(), &endptr);
  if (result != result) {
    result = 0.0f;  // Convert NAN to 0.0f;
  }
  if (pUsedLen) {
    *pUsedLen = endptr - copied.c_str();
  }
  return result;
}

wchar_t* FXSYS_wcsncpy(wchar_t* dstStr, const wchar_t* srcStr, size_t count) {
  DCHECK(dstStr);
  DCHECK(srcStr);
  DCHECK(count > 0);

  // SAFETY: required from caller, enforced by UNSAFE_BUFFER_USAGE in header.
  UNSAFE_BUFFERS({
    for (size_t i = 0; i < count; ++i) {
      dstStr[i] = srcStr[i];
      if (dstStr[i] == L'\0') {
        break;
      }
    }
  });
  return dstStr;
}

// TODO(tsepez): should be UNSAFE_BUFFER_USAGE.
void FXSYS_IntToTwoHexChars(uint8_t n, char* buf) {
  static const char kHex[] = "0123456789ABCDEF";
  // SAFETY: range of uint8_t keeps indices in bound.
  UNSAFE_BUFFERS({
    buf[0] = kHex[n / 16];
    buf[1] = kHex[n % 16];
  });
}

// TODO(tsepez): This is UNSAFE_BUFFER_USAGE as well.
void FXSYS_IntToFourHexChars(uint16_t n, char* buf) {
  // SAFETY: required from caller.
  UNSAFE_BUFFERS({
    FXSYS_IntToTwoHexChars(n / 256, buf);
    FXSYS_IntToTwoHexChars(n % 256, buf + 2);
  });
}

// TODO(tsepez): This is UNSAFE_BUFFER_USAGE as well.
size_t FXSYS_ToUTF16BE(uint32_t unicode, char* buf) {
  DCHECK(unicode <= pdfium::kMaximumSupplementaryCodePoint);
  DCHECK(!pdfium::IsHighSurrogate(unicode));
  DCHECK(!pdfium::IsLowSurrogate(unicode));

  if (unicode <= 0xFFFF) {
    FXSYS_IntToFourHexChars(unicode, buf);
    return 4;
  }
  // SAFETY: required from caller.
  UNSAFE_BUFFERS({
    pdfium::SurrogatePair surrogate_pair(unicode);
    FXSYS_IntToFourHexChars(surrogate_pair.high(), buf);
    FXSYS_IntToFourHexChars(surrogate_pair.low(), buf + 4);
  });
  return 8;
}

void FXSYS_SetTimeFunction(time_t (*func)()) {
  g_time_func = func ? func : DefaultTimeFunction;
}

void FXSYS_SetLocaltimeFunction(struct tm* (*func)(const time_t*)) {
  g_localtime_func = func ? func : DefaultLocaltimeFunction;
}

time_t FXSYS_time(time_t* tloc) {
  time_t ret_val = g_time_func();
  if (tloc)
    *tloc = ret_val;
  return ret_val;
}

struct tm* FXSYS_localtime(const time_t* tp) {
  return g_localtime_func(tp);
}
