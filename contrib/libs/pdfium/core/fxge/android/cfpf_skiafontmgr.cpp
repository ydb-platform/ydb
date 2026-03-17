// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxge/android/cfpf_skiafontmgr.h"

#include <algorithm>
#include <array>
#include <iterator>
#include <utility>

#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/containers/adapters.h"
#include "core/fxcrt/fx_codepage.h"
#include "core/fxcrt/fx_extension.h"
#include "core/fxcrt/fx_folder.h"
#include "core/fxcrt/fx_system.h"
#include "core/fxcrt/stl_util.h"
#include "core/fxge/android/cfpf_skiafont.h"
#include "core/fxge/android/cfpf_skiapathfont.h"
#include "core/fxge/freetype/fx_freetype.h"
#include "core/fxge/fx_font.h"

namespace {

constexpr int kSkiaMatchNameWeight = 62;
constexpr int kSkiaMatchSystemNameWeight = 60;
constexpr int kSkiaMatchSerifStyleWeight = 16;
constexpr int kSkiaMatchScriptStyleWeight = 8;

struct SkiaFontMap {
  uint32_t family;
  uint32_t subst;
};

const SkiaFontMap kSkiaFontmap[] = {
    {0x58c5083, 0xc8d2e345},  {0x5dfade2, 0xe1633081},
    {0x684317d, 0xe1633081},  {0x14ee2d13, 0xc8d2e345},
    {0x3918fe2d, 0xbbeeec72}, {0x3b98b31c, 0xe1633081},
    {0x3d49f40e, 0xe1633081}, {0x432c41c5, 0xe1633081},
    {0x491b6ad0, 0xe1633081}, {0x5612cab1, 0x59b9f8f1},
    {0x779ce19d, 0xc8d2e345}, {0x7cc9510b, 0x59b9f8f1},
    {0x83746053, 0xbbeeec72}, {0xaaa60c03, 0xbbeeec72},
    {0xbf85ff26, 0xe1633081}, {0xc04fe601, 0xbbeeec72},
    {0xca3812d5, 0x59b9f8f1}, {0xca383e15, 0x59b9f8f1},
    {0xcad5eaf6, 0x59b9f8f1}, {0xcb7a04c8, 0xc8d2e345},
    {0xfb4ce0de, 0xe1633081},
};

const SkiaFontMap kSkiaSansFontMap[] = {
    {0x58c5083, 0xd5b8d10f},  {0x14ee2d13, 0xd5b8d10f},
    {0x779ce19d, 0xd5b8d10f}, {0xcb7a04c8, 0xd5b8d10f},
    {0xfb4ce0de, 0xd5b8d10f},
};

uint32_t SkiaGetSubstFont(uint32_t hash,
                          pdfium::span<const SkiaFontMap> font_map) {
  const SkiaFontMap* it =
      std::lower_bound(font_map.begin(), font_map.end(), hash,
                       [](const SkiaFontMap& item, uint32_t hash) {
                         return item.family < hash;
                       });
  if (it != font_map.end() && it->family == hash) {
    return it->subst;
  }
  return 0;
}

enum SKIACHARSET {
  SKIACHARSET_Ansi = 1 << 0,
  SKIACHARSET_Default = 1 << 1,
  SKIACHARSET_Symbol = 1 << 2,
  SKIACHARSET_ShiftJIS = 1 << 3,
  SKIACHARSET_Korean = 1 << 4,
  SKIACHARSET_Johab = 1 << 5,
  SKIACHARSET_GB2312 = 1 << 6,
  SKIACHARSET_BIG5 = 1 << 7,
  SKIACHARSET_Greek = 1 << 8,
  SKIACHARSET_Turkish = 1 << 9,
  SKIACHARSET_Vietnamese = 1 << 10,
  SKIACHARSET_Hebrew = 1 << 11,
  SKIACHARSET_Arabic = 1 << 12,
  SKIACHARSET_Baltic = 1 << 13,
  SKIACHARSET_Cyrillic = 1 << 14,
  SKIACHARSET_Thai = 1 << 15,
  SKIACHARSET_EeasternEuropean = 1 << 16,
  SKIACHARSET_PC = 1 << 17,
  SKIACHARSET_OEM = 1 << 18,
};

uint32_t SkiaGetCharset(FX_Charset charset) {
  switch (charset) {
    case FX_Charset::kANSI:
      return SKIACHARSET_Ansi;
    case FX_Charset::kDefault:
      return SKIACHARSET_Default;
    case FX_Charset::kSymbol:
      return SKIACHARSET_Symbol;
    case FX_Charset::kShiftJIS:
      return SKIACHARSET_ShiftJIS;
    case FX_Charset::kHangul:
      return SKIACHARSET_Korean;
    case FX_Charset::kChineseSimplified:
      return SKIACHARSET_GB2312;
    case FX_Charset::kChineseTraditional:
      return SKIACHARSET_BIG5;
    case FX_Charset::kMSWin_Greek:
      return SKIACHARSET_Greek;
    case FX_Charset::kMSWin_Turkish:
      return SKIACHARSET_Turkish;
    case FX_Charset::kMSWin_Hebrew:
      return SKIACHARSET_Hebrew;
    case FX_Charset::kMSWin_Arabic:
      return SKIACHARSET_Arabic;
    case FX_Charset::kMSWin_Baltic:
      return SKIACHARSET_Baltic;
    case FX_Charset::kMSWin_Cyrillic:
      return SKIACHARSET_Cyrillic;
    case FX_Charset::kThai:
      return SKIACHARSET_Thai;
    case FX_Charset::kMSWin_EasternEuropean:
      return SKIACHARSET_EeasternEuropean;
    default:
      return SKIACHARSET_Default;
  }
}

uint32_t SkiaNormalizeFontName(ByteStringView family) {
  uint32_t hash_code = 0;
  for (unsigned char ch : family) {
    if (ch == ' ' || ch == '-' || ch == ',')
      continue;
    hash_code = 31 * hash_code + tolower(ch);
  }
  return hash_code;
}

uint32_t GetFamilyHash(ByteStringView family,
                       uint32_t style,
                       FX_Charset charset) {
  ByteString font(family);
  if (FontStyleIsForceBold(style)) {
    font += "Bold";
  }
  if (FontStyleIsItalic(style)) {
    font += "Italic";
  }
  if (FontStyleIsSerif(style)) {
    font += "Serif";
  }
  font += static_cast<uint8_t>(charset);
  return FX_HashCode_GetA(font.AsStringView());
}

bool SkiaIsCJK(FX_Charset charset) {
  return FX_CharSetIsCJK(charset);
}

bool SkiaMaybeSymbol(ByteStringView facename) {
  ByteString name(facename);
  name.MakeLower();
  return name.Contains("symbol");
}

bool SkiaMaybeArabic(ByteStringView facename) {
  ByteString name(facename);
  name.MakeLower();
  return name.Contains("arabic");
}

constexpr auto kFPFSkiaFontCharsets = fxcrt::ToArray<const uint32_t>({
    SKIACHARSET_Ansi,
    SKIACHARSET_EeasternEuropean,
    SKIACHARSET_Cyrillic,
    SKIACHARSET_Greek,
    SKIACHARSET_Turkish,
    SKIACHARSET_Hebrew,
    SKIACHARSET_Arabic,
    SKIACHARSET_Baltic,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    SKIACHARSET_Thai,
    SKIACHARSET_ShiftJIS,
    SKIACHARSET_GB2312,
    SKIACHARSET_Korean,
    SKIACHARSET_BIG5,
    SKIACHARSET_Johab,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    SKIACHARSET_OEM,
    SKIACHARSET_Symbol,
});

uint32_t SkiaGetFaceCharset(uint32_t code_range) {
  uint32_t charset = 0;
  for (int32_t i = 0; i < 32; i++) {
    if (code_range & (1 << i)) {
      charset |= kFPFSkiaFontCharsets[i];
    }
  }
  return charset;
}

}  // namespace

CFPF_SkiaFontMgr::CFPF_SkiaFontMgr() = default;

CFPF_SkiaFontMgr::~CFPF_SkiaFontMgr() = default;

bool CFPF_SkiaFontMgr::InitFTLibrary() {
  if (ft_library_) {
    return true;
  }

  FXFT_LibraryRec* library = nullptr;
  FT_Init_FreeType(&library);
  if (!library) {
    return false;
  }

  ft_library_.reset(library);
  return true;
}

void CFPF_SkiaFontMgr::LoadFonts(const char** user_paths) {
  if (loaded_fonts_) {
    return;
  }

  ScanPath("/system/fonts");

  if (user_paths) {
    // SAFETY: nullptr-terminated array required from caller.
    UNSAFE_BUFFERS({
      for (const char** path = user_paths; *path; ++path) {
        ScanPath(*path);
      }
    });
  }

  loaded_fonts_ = true;
}

CFPF_SkiaFont* CFPF_SkiaFontMgr::CreateFont(ByteStringView family_name,
                                            FX_Charset charset,
                                            uint32_t style) {
  const uint32_t hash = GetFamilyHash(family_name, style, charset);
  auto family_iter = family_font_map_.find(hash);
  if (family_iter != family_font_map_.end()) {
    return family_iter->second.get();
  }

  const uint32_t face_name_hash = SkiaNormalizeFontName(family_name);
  const uint32_t subst_hash = SkiaGetSubstFont(face_name_hash, kSkiaFontmap);
  const uint32_t subst_sans_hash =
      SkiaGetSubstFont(face_name_hash, kSkiaSansFontMap);
  const bool maybe_symbol = SkiaMaybeSymbol(family_name);
  if (charset != FX_Charset::kMSWin_Arabic && SkiaMaybeArabic(family_name)) {
    charset = FX_Charset::kMSWin_Arabic;
  } else if (charset == FX_Charset::kANSI) {
    charset = FX_Charset::kDefault;
  }
  int32_t expected_score = kSkiaMatchNameWeight +
                           kSkiaMatchSerifStyleWeight * 3 +
                           kSkiaMatchScriptStyleWeight * 2;
  const CFPF_SkiaPathFont* best_font = nullptr;
  int32_t best_score = -1;
  int32_t best_glyph_num = 0;
  for (const std::unique_ptr<CFPF_SkiaPathFont>& font :
       pdfium::Reversed(font_faces_)) {
    if (!(font->charsets() & SkiaGetCharset(charset))) {
      continue;
    }
    int32_t score = 0;
    const uint32_t sys_font_name_hash = SkiaNormalizeFontName(font->family());
    if (face_name_hash == sys_font_name_hash) {
      score += kSkiaMatchNameWeight;
    }
    bool matches_name = (score == kSkiaMatchNameWeight);
    if (FontStyleIsForceBold(style) == FontStyleIsForceBold(font->style())) {
      score += kSkiaMatchSerifStyleWeight;
    }
    if (FontStyleIsItalic(style) == FontStyleIsItalic(font->style())) {
      score += kSkiaMatchSerifStyleWeight;
    }
    if (FontStyleIsFixedPitch(style) == FontStyleIsFixedPitch(font->style())) {
      score += kSkiaMatchScriptStyleWeight;
    }
    if (FontStyleIsSerif(style) == FontStyleIsSerif(font->style())) {
      score += kSkiaMatchSerifStyleWeight;
    }
    if (FontStyleIsScript(style) == FontStyleIsScript(font->style())) {
      score += kSkiaMatchScriptStyleWeight;
    }
    if (subst_hash == sys_font_name_hash ||
        subst_sans_hash == sys_font_name_hash) {
      score += kSkiaMatchSystemNameWeight;
      matches_name = true;
    }
    if (charset == FX_Charset::kDefault || maybe_symbol) {
      if (score > best_score && matches_name) {
        best_score = score;
        best_font = font.get();
      }
    } else if (SkiaIsCJK(charset)) {
      if (matches_name || font->glyph_num() > best_glyph_num) {
        best_font = font.get();
        best_glyph_num = font->glyph_num();
      }
    } else if (score > best_score) {
      best_score = score;
      best_font = font.get();
    }
    if (score >= expected_score) {
      best_font = font.get();
      break;
    }
  }
  if (!best_font) {
    return nullptr;
  }

  auto font = std::make_unique<CFPF_SkiaFont>(this, best_font, charset);
  if (!font->IsValid())
    return nullptr;

  CFPF_SkiaFont* ret = font.get();
  family_font_map_[hash] = std::move(font);
  return ret;
}

RetainPtr<CFX_Face> CFPF_SkiaFontMgr::GetFontFace(ByteStringView path,
                                                  int32_t face_index) {
  if (path.IsEmpty()) {
    return nullptr;
  }

  if (face_index < 0) {
    return nullptr;
  }

  FT_Open_Args args;
  args.flags = FT_OPEN_PATHNAME;
  args.pathname = const_cast<FT_String*>(path.unterminated_c_str());
  RetainPtr<CFX_Face> face =
      CFX_Face::Open(ft_library_.get(), &args, face_index);
  if (!face)
    return nullptr;

  face->SetPixelSize(0, 64);
  return face;
}

void CFPF_SkiaFontMgr::ScanPath(const ByteString& path) {
  std::unique_ptr<FX_Folder> handle = FX_Folder::OpenFolder(path);
  if (!handle)
    return;

  ByteString filename;
  bool is_folder = false;
  while (handle->GetNextFile(&filename, &is_folder)) {
    if (is_folder) {
      if (filename == "." || filename == "..")
        continue;
    } else {
      ByteString ext = filename.Last(4);
      ext.MakeLower();
      if (ext != ".ttf" && ext != ".ttc" && ext != ".otf")
        continue;
    }
    ByteString fullpath(path);
    fullpath += "/";
    fullpath += filename;
    if (is_folder) {
      ScanPath(fullpath);
    } else {
      ScanFile(fullpath);
    }
  }
}

void CFPF_SkiaFontMgr::ScanFile(const ByteString& file) {
  RetainPtr<CFX_Face> face = GetFontFace(file.AsStringView(), 0);
  if (!face)
    return;

  font_faces_.push_back(ReportFace(face, file));
}

std::unique_ptr<CFPF_SkiaPathFont> CFPF_SkiaFontMgr::ReportFace(
    RetainPtr<CFX_Face> face,
    const ByteString& file) {
  uint32_t style = 0;
  if (face->IsBold()) {
    style |= FXFONT_FORCE_BOLD;
  }
  if (face->IsItalic()) {
    style |= FXFONT_ITALIC;
  }
  if (face->IsFixedWidth()) {
    style |= FXFONT_FIXED_PITCH;
  }

  uint32_t charset = SKIACHARSET_Default;
  std::optional<std::array<uint32_t, 2>> code_page_range =
      face->GetOs2CodePageRange();
  if (code_page_range.has_value()) {
    if (code_page_range.value()[0] & (1 << 31)) {
      style |= FXFONT_SYMBOLIC;
    }
    charset |= SkiaGetFaceCharset(code_page_range.value()[0]);
  }

  std::optional<std::array<uint8_t, 2>> panose = face->GetOs2Panose();
  if (panose.has_value() && panose.value()[0] == 2) {
    uint8_t serif = panose.value()[1];
    if ((serif > 1 && serif < 10) || serif > 13) {
      style |= FXFONT_SERIF;
    }
  }

  return std::make_unique<CFPF_SkiaPathFont>(file, face->GetFamilyName(), style,
                                             face->GetRec()->face_index,
                                             charset, face->GetGlyphCount());
}
