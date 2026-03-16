// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCRT_FX_SYSTEM_H_
#define CORE_FXCRT_FX_SYSTEM_H_

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <wchar.h>

#include "build/build_config.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/fx_types.h"

#if defined(_MSC_VER) && _MSC_VER < 1900
#error Sorry, VC++ 2015 or later is required to compile PDFium.
#endif  // defined(_MSC_VER) && _MSC_VER < 1900

#if defined(__wasm__) && defined(PDF_ENABLE_V8)
#error Cannot compile v8 with wasm.
#endif  // PDF_ENABLE_V8

#if BUILDFLAG(IS_WIN)
#include <windows.h>
#endif  // BUILDFLAG(IS_WIN)

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus

#define FXSYS_IsFloatZero(f) ((f) < 0.0001 && (f) > -0.0001)
#define FXSYS_IsFloatBigger(fa, fb) \
  ((fa) > (fb) && !FXSYS_IsFloatZero((fa) - (fb)))
#define FXSYS_IsFloatSmaller(fa, fb) \
  ((fa) < (fb) && !FXSYS_IsFloatZero((fa) - (fb)))
#define FXSYS_IsFloatEqual(fa, fb) FXSYS_IsFloatZero((fa) - (fb))

// M_PI not universally present on all platforms.
#define FXSYS_PI 3.1415926535897932384626433832795f
#define FXSYS_BEZIER 0.5522847498308f

// NOTE: prevent use of the return value from snprintf() since some platforms
// have different return values.
#define FXSYS_snprintf (void)snprintf
#define FXSYS_vsnprintf (void)vsnprintf
#define FXSYS_sprintf DO_NOT_USE_SPRINTF_DIE_DIE_DIE
#define FXSYS_vsprintf DO_NOT_USE_VSPRINTF_DIE_DIE_DIE

#if BUILDFLAG(IS_WIN)
#define FXSYS_itoa _itoa
#define FXSYS_strlwr _strlwr
#define FXSYS_strupr _strupr
#define FXSYS_wcslwr _wcslwr
#define FXSYS_wcsupr _wcsupr
#define FXSYS_SetLastError SetLastError
#define FXSYS_GetLastError GetLastError
UNSAFE_BUFFER_USAGE size_t FXSYS_wcsftime(wchar_t* strDest,
                                          size_t maxsize,
                                          const wchar_t* format,
                                          const struct tm* timeptr);
#else  // BUILDFLAG(IS_WIN)
char* FXSYS_itoa(int value, char* str, int radix);
char* FXSYS_strlwr(char* str);
char* FXSYS_strupr(char* str);
wchar_t* FXSYS_wcslwr(wchar_t* str);
wchar_t* FXSYS_wcsupr(wchar_t* str);
void FXSYS_SetLastError(uint32_t err);
uint32_t FXSYS_GetLastError();
#define FXSYS_wcsftime wcsftime
#endif  // BUILDFLAG(IS_WIN)

const char* FXSYS_i64toa(int64_t value, char* str, int radix);
int FXSYS_roundf(float f);
int FXSYS_round(double d);
float FXSYS_sqrt2(float a, float b);

#ifdef __cplusplus
}  // extern "C"

// C++-only section to allow future use of TerminatedPtr<>.
int FXSYS_stricmp(const char* str1, const char* str2);
int FXSYS_wcsicmp(const wchar_t* str1, const wchar_t* str2);
int32_t FXSYS_atoi(const char* str);
int32_t FXSYS_wtoi(const wchar_t* str);
uint32_t FXSYS_atoui(const char* str);
int64_t FXSYS_atoi64(const char* str);

#endif  // __cplusplus

#endif  // CORE_FXCRT_FX_SYSTEM_H_
