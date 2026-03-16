// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxge/apple/fx_apple_platform.h"

#include <memory>
#include <utility>

#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/fx_codepage.h"
#include "core/fxge/cfx_folderfontinfo.h"
#include "core/fxge/cfx_fontmgr.h"
#include "core/fxge/fx_font.h"
#include "core/fxge/systemfontinfo_iface.h"

namespace {

struct Substs {
  const char* m_pName;
  const char* m_pSubstName;
};

constexpr Substs kBase14Substs[] = {
    {"Courier", "Courier New"},
    {"Courier-Bold", "Courier New Bold"},
    {"Courier-BoldOblique", "Courier New Bold Italic"},
    {"Courier-Oblique", "Courier New Italic"},
    {"Helvetica", "Arial"},
    {"Helvetica-Bold", "Arial Bold"},
    {"Helvetica-BoldOblique", "Arial Bold Italic"},
    {"Helvetica-Oblique", "Arial Italic"},
    {"Times-Roman", "Times New Roman"},
    {"Times-Bold", "Times New Roman Bold"},
    {"Times-BoldItalic", "Times New Roman Bold Italic"},
    {"Times-Italic", "Times New Roman Italic"},
};

class CFX_MacFontInfo final : public CFX_FolderFontInfo {
 public:
  CFX_MacFontInfo() = default;
  ~CFX_MacFontInfo() override = default;

  // CFX_FolderFontInfo
  void* MapFont(int weight,
                bool bItalic,
                FX_Charset charset,
                int pitch_family,
                const ByteString& face) override;

  bool ParseFontCfg(const char** pUserPaths);
};

constexpr char kJapanGothic[] = "Hiragino Kaku Gothic Pro W6";
constexpr char kJapanMincho[] = "Hiragino Mincho Pro W6";

ByteString GetJapanesePreference(const ByteString& face,
                                 int weight,
                                 int pitch_family) {
  if (face.Contains("Gothic"))
    return kJapanGothic;
  if (FontFamilyIsRoman(pitch_family) || weight <= 400)
    return kJapanMincho;
  return kJapanGothic;
}

void* CFX_MacFontInfo::MapFont(int weight,
                               bool bItalic,
                               FX_Charset charset,
                               int pitch_family,
                               const ByteString& face) {
  for (const auto& sub : kBase14Substs) {
    if (face == ByteStringView(sub.m_pName))
      return GetFont(sub.m_pSubstName);
  }

  // The request may not ask for the bold and/or italic version of a font by
  // name. So try to construct the appropriate name. This is not 100% foolproof
  // as there are fonts that have "Oblique" or "BoldOblique" or "Heavy" in their
  // names instead. But this at least works for common fonts like Arial and
  // Times New Roman. A more sophisticated approach would be to find all the
  // fonts in |m_FontList| with |face| in the name, and examine the fonts to
  // see which best matches the requested characteristics.
  if (!face.Contains("Bold") && !face.Contains("Italic")) {
    ByteString new_face = face;
    if (weight > 400)
      new_face += " Bold";
    if (bItalic)
      new_face += " Italic";
    auto it = m_FontList.find(new_face);
    if (it != m_FontList.end())
      return it->second.get();
  }

  auto it = m_FontList.find(face);
  if (it != m_FontList.end())
    return it->second.get();

  if (charset == FX_Charset::kANSI && FontFamilyIsFixedPitch(pitch_family))
    return GetFont("Courier New");

  if (charset == FX_Charset::kANSI || charset == FX_Charset::kSymbol)
    return nullptr;

  ByteString other_face;
  switch (charset) {
    case FX_Charset::kShiftJIS:
      other_face = GetJapanesePreference(face, weight, pitch_family);
      break;
    case FX_Charset::kChineseSimplified:
      other_face = "STSong";
      break;
    case FX_Charset::kHangul:
      other_face = "AppleMyungjo";
      break;
    case FX_Charset::kChineseTraditional:
      other_face = "LiSong Pro Light";
      break;
    default:
      other_face = face;
      break;
  }
  it = m_FontList.find(other_face);
  return it != m_FontList.end() ? it->second.get() : nullptr;
}

bool CFX_MacFontInfo::ParseFontCfg(const char** pUserPaths) {
  if (!pUserPaths) {
    return false;
  }
  UNSAFE_TODO({
    for (const char** pPath = pUserPaths; *pPath; ++pPath) {
      AddPath(*pPath);
    }
  });
  return true;
}

}  // namespace

CApplePlatform::CApplePlatform() = default;

CApplePlatform::~CApplePlatform() = default;

void CApplePlatform::Init() {}

std::unique_ptr<SystemFontInfoIface>
CApplePlatform::CreateDefaultSystemFontInfo() {
  auto pInfo = std::make_unique<CFX_MacFontInfo>();
  if (!pInfo->ParseFontCfg(CFX_GEModule::Get()->GetUserFontPaths())) {
    pInfo->AddPath("~/Library/Fonts");
    pInfo->AddPath("/Library/Fonts");
    pInfo->AddPath("/System/Library/Fonts");
  }
  return pInfo;
}

void* CApplePlatform::CreatePlatformFont(
    pdfium::span<const uint8_t> font_span) {
  return m_quartz2d.CreateFont(font_span);
}

// static
std::unique_ptr<CFX_GEModule::PlatformIface>
CFX_GEModule::PlatformIface::Create() {
  return std::make_unique<CApplePlatform>();
}
