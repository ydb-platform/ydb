// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include <array>
#include <iterator>
#include <memory>
#include <utility>

#include "build/build_config.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/fx_codepage.h"
#include "core/fxcrt/stl_util.h"
#include "core/fxge/cfx_folderfontinfo.h"
#include "core/fxge/cfx_fontmgr.h"
#include "core/fxge/cfx_gemodule.h"
#include "core/fxge/fx_font.h"
#include "core/fxge/systemfontinfo_iface.h"

#if !BUILDFLAG(IS_LINUX) && !BUILDFLAG(IS_CHROMEOS) && !defined(OS_ASMJS)
#error "Included on the wrong platform"
#endif

namespace {

enum JpFontFamilyRowIndex : uint8_t {
  kJpFontPGothic,
  kJpFontGothic,
  kJpFontPMincho,
  kJpFontMincho,
};

constexpr size_t kJpFontFamilyColumnCount = 5;
using JpFontFamilyRow = std::array<const char*, kJpFontFamilyColumnCount>;
constexpr auto kJpFontTable = fxcrt::ToArray<const JpFontFamilyRow>({
    {{"MS PGothic", "TakaoPGothic", "VL PGothic", "IPAPGothic", "VL Gothic"}},
    {{"MS Gothic", "TakaoGothic", "VL Gothic", "IPAGothic", "Kochi Gothic"}},
    {{"MS PMincho", "TakaoPMincho", "IPAPMincho", "VL Gothic", "Kochi Mincho"}},
    {{"MS Mincho", "TakaoMincho", "IPAMincho", "VL Gothic", "Kochi Mincho"}},
});

const char* const kGbFontList[] = {
    "AR PL UMing CN Light",
    "WenQuanYi Micro Hei",
    "AR PL UKai CN",
};

const char* const kB5FontList[] = {
    "AR PL UMing TW Light",
    "WenQuanYi Micro Hei",
    "AR PL UKai TW",
};

const char* const kHGFontList[] = {
    "UnDotum",
};

JpFontFamilyRowIndex GetJapanesePreference(const ByteString& face,
                                           int weight,
                                           int pitch_family) {
  if (face.Contains("Gothic") ||
      face.Contains("\x83\x53\x83\x56\x83\x62\x83\x4e")) {
    if (face.Contains("PGothic") ||
        face.Contains("\x82\x6f\x83\x53\x83\x56\x83\x62\x83\x4e")) {
      return kJpFontPGothic;
    }
    return kJpFontGothic;
  }
  if (face.Contains("Mincho") || face.Contains("\x96\xbe\x92\xa9")) {
    if (face.Contains("PMincho") || face.Contains("\x82\x6f\x96\xbe\x92\xa9")) {
      return kJpFontPMincho;
    }
    return kJpFontMincho;
  }
  if (!FontFamilyIsRoman(pitch_family) && weight > 400)
    return kJpFontPGothic;

  return kJpFontPMincho;
}

class CFX_LinuxFontInfo final : public CFX_FolderFontInfo {
 public:
  CFX_LinuxFontInfo() = default;
  ~CFX_LinuxFontInfo() override = default;

  // CFX_FolderFontInfo:
  void* MapFont(int weight,
                bool bItalic,
                FX_Charset charset,
                int pitch_family,
                const ByteString& face) override;

  bool ParseFontCfg(const char** pUserPaths);
};

void* CFX_LinuxFontInfo::MapFont(int weight,
                                 bool bItalic,
                                 FX_Charset charset,
                                 int pitch_family,
                                 const ByteString& face) {
  void* font = GetSubstFont(face);
  if (font)
    return font;

  bool bCJK = true;
  switch (charset) {
    case FX_Charset::kShiftJIS: {
      JpFontFamilyRowIndex index =
          GetJapanesePreference(face, weight, pitch_family);
      for (const char* name : kJpFontTable[index]) {
        auto it = m_FontList.find(name);
        if (it != m_FontList.end())
          return it->second.get();
      }
      break;
    }
    case FX_Charset::kChineseSimplified: {
      for (const char* name : kGbFontList) {
        auto it = m_FontList.find(name);
        if (it != m_FontList.end())
          return it->second.get();
      }
      break;
    }
    case FX_Charset::kChineseTraditional: {
      for (const char* name : kB5FontList) {
        auto it = m_FontList.find(name);
        if (it != m_FontList.end())
          return it->second.get();
      }
      break;
    }
    case FX_Charset::kHangul: {
      for (const char* name : kHGFontList) {
        auto it = m_FontList.find(name);
        if (it != m_FontList.end())
          return it->second.get();
      }
      break;
    }
    default:
      bCJK = false;
      break;
  }
  return FindFont(weight, bItalic, charset, pitch_family, face, !bCJK);
}

bool CFX_LinuxFontInfo::ParseFontCfg(const char** pUserPaths) {
  if (!pUserPaths)
    return false;

  // SAFETY: nullptr-terminated array required from caller.
  UNSAFE_BUFFERS({
    for (const char** pPath = pUserPaths; *pPath; ++pPath) {
      AddPath(*pPath);
    }
  });
  return true;
}

}  // namespace

class CLinuxPlatform : public CFX_GEModule::PlatformIface {
 public:
  CLinuxPlatform() = default;
  ~CLinuxPlatform() override = default;

  void Init() override {}

  std::unique_ptr<SystemFontInfoIface> CreateDefaultSystemFontInfo() override {
    auto pInfo = std::make_unique<CFX_LinuxFontInfo>();
    if (!pInfo->ParseFontCfg(CFX_GEModule::Get()->GetUserFontPaths())) {
      pInfo->AddPath("/usr/share/fonts");
      pInfo->AddPath("/usr/share/X11/fonts/Type1");
      pInfo->AddPath("/usr/share/X11/fonts/TTF");
      pInfo->AddPath("/usr/local/share/fonts");
    }
    return pInfo;
  }
};

// static
std::unique_ptr<CFX_GEModule::PlatformIface>
CFX_GEModule::PlatformIface::Create() {
  return std::make_unique<CLinuxPlatform>();
}
