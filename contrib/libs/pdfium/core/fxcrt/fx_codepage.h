// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCRT_FX_CODEPAGE_H_
#define CORE_FXCRT_FX_CODEPAGE_H_

#include <stdint.h>

#include <array>

// Prove consistency with incomplete forward definitions.
#include "core/fxcrt/fx_codepage_forward.h"
#include "core/fxcrt/fx_string.h"
#include "core/fxcrt/span.h"

enum class FX_CodePage : uint16_t {
  kDefANSI = 0,
  kSymbol = 42,
  kMSDOS_US = 437,
  kArabic_ASMO708 = 708,
  kMSDOS_Greek1 = 737,
  kMSDOS_Baltic = 775,
  kMSDOS_WesternEuropean = 850,
  kMSDOS_EasternEuropean = 852,
  kMSDOS_Cyrillic = 855,
  kMSDOS_Turkish = 857,
  kMSDOS_Portuguese = 860,
  kMSDOS_Icelandic = 861,
  kMSDOS_Hebrew = 862,
  kMSDOS_FrenchCanadian = 863,
  kMSDOS_Arabic = 864,
  kMSDOS_Norwegian = 865,
  kMSDOS_Russian = 866,
  kMSDOS_Greek2 = 869,
  kMSDOS_Thai = 874,
  kShiftJIS = 932,
  kChineseSimplified = 936,
  kHangul = 949,
  kChineseTraditional = 950,
  kUTF16LE = 1200,
  kUTF16BE = 1201,
  kMSWin_EasternEuropean = 1250,
  kMSWin_Cyrillic = 1251,
  kMSWin_WesternEuropean = 1252,
  kMSWin_Greek = 1253,
  kMSWin_Turkish = 1254,
  kMSWin_Hebrew = 1255,
  kMSWin_Arabic = 1256,
  kMSWin_Baltic = 1257,
  kMSWin_Vietnamese = 1258,
  kJohab = 1361,
  kMAC_Roman = 10000,
  kMAC_ShiftJIS = 10001,
  kMAC_ChineseTraditional = 10002,
  kMAC_Korean = 10003,
  kMAC_Arabic = 10004,
  kMAC_Hebrew = 10005,
  kMAC_Greek = 10006,
  kMAC_Cyrillic = 10007,
  kMAC_ChineseSimplified = 10008,
  kMAC_Thai = 10021,
  kMAC_EasternEuropean = 10029,
  kMAC_Turkish = 10081,
  kUTF8 = 65001,
  kFailure = 65535,
};

enum class FX_Charset : uint8_t {
  kANSI = 0,
  kDefault = 1,
  kSymbol = 2,
  kMAC_Roman = 77,
  kMAC_ShiftJIS = 78,
  kMAC_Korean = 79,
  kMAC_ChineseSimplified = 80,
  kMAC_ChineseTraditional = 81,
  kMAC_Hebrew = 83,
  kMAC_Arabic = 84,
  kMAC_Greek = 85,
  kMAC_Turkish = 86,
  kMAC_Thai = 87,
  kMAC_EasternEuropean = 88,
  kMAC_Cyrillic = 89,
  kShiftJIS = 128,
  kHangul = 129,
  kJohab = 130,
  kChineseSimplified = 134,
  kChineseTraditional = 136,
  kMSWin_Greek = 161,
  kMSWin_Turkish = 162,
  kMSWin_Vietnamese = 163,
  kMSWin_Hebrew = 177,
  kMSWin_Arabic = 178,
  kMSWin_Baltic = 186,
  kMSWin_Cyrillic = 204,
  kThai = 222,
  kMSWin_EasternEuropean = 238,
  kUS = 254,
  kOEM = 255,
};

// Hi-bytes to unicode codepoint mapping for various code pages.
struct FX_CharsetUnicodes {
  FX_Charset m_Charset;
  pdfium::span<const uint16_t> m_pUnicodes;
};

extern const std::array<FX_CharsetUnicodes, 8> kFX_CharsetUnicodes;

FX_CodePage FX_GetACP();
FX_CodePage FX_GetCodePageFromCharset(FX_Charset charset);
FX_Charset FX_GetCharsetFromCodePage(FX_CodePage codepage);
FX_Charset FX_GetCharsetFromInt(int value);
bool FX_CharSetIsCJK(FX_Charset uCharset);
size_t FX_WideCharToMultiByte(FX_CodePage codepage,
                              WideStringView wstr,
                              pdfium::span<char> buf);
size_t FX_MultiByteToWideChar(FX_CodePage codepage,
                              ByteStringView bstr,
                              pdfium::span<wchar_t> buf);

#endif  // CORE_FXCRT_FX_CODEPAGE_H_
