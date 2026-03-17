// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxge/cfx_font.h"

#include <stdint.h>

#include <algorithm>
#include <limits>
#include <memory>
#include <utility>

#include "build/build_config.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/data_vector.h"
#include "core/fxcrt/fx_codepage.h"
#include "core/fxcrt/fx_stream.h"
#include "core/fxcrt/numerics/safe_conversions.h"
#include "core/fxcrt/span.h"
#include "core/fxcrt/unowned_ptr.h"
#include "core/fxge/cfx_fontcache.h"
#include "core/fxge/cfx_fontmapper.h"
#include "core/fxge/cfx_fontmgr.h"
#include "core/fxge/cfx_gemodule.h"
#include "core/fxge/cfx_glyphcache.h"
#include "core/fxge/cfx_path.h"
#include "core/fxge/cfx_substfont.h"
#include "core/fxge/fx_font.h"
#include "core/fxge/scoped_font_transform.h"

namespace {

const CFX_Font::CharsetFontMap kDefaultTTFMap[] = {
    {static_cast<int>(FX_Charset::kANSI), CFX_Font::kDefaultAnsiFontName},
    {static_cast<int>(FX_Charset::kChineseSimplified), "SimSun"},
    {static_cast<int>(FX_Charset::kChineseTraditional), "MingLiU"},
    {static_cast<int>(FX_Charset::kShiftJIS), "MS Gothic"},
    {static_cast<int>(FX_Charset::kHangul), "Batang"},
    {static_cast<int>(FX_Charset::kMSWin_Cyrillic), "Arial"},
#if BUILDFLAG(IS_WIN)
    {static_cast<int>(FX_Charset::kMSWin_EasternEuropean), "Tahoma"},
#else
    {static_cast<int>(FX_Charset::kMSWin_EasternEuropean), "Arial"},
#endif
    {static_cast<int>(FX_Charset::kMSWin_Arabic), "Arial"},
    // TODO(crbug.com/348468114): Remove sentinel value when
    // FPDF_GetDefaultTTFMap() gets removed.
    {-1, nullptr}};

FX_RECT FXRectFromFTPos(FT_Pos left, FT_Pos top, FT_Pos right, FT_Pos bottom) {
  return FX_RECT(pdfium::checked_cast<int32_t>(left),
                 pdfium::checked_cast<int32_t>(top),
                 pdfium::checked_cast<int32_t>(right),
                 pdfium::checked_cast<int32_t>(bottom));
}

FX_RECT ScaledFXRectFromFTPos(FT_Pos left,
                              FT_Pos top,
                              FT_Pos right,
                              FT_Pos bottom,
                              int x_scale,
                              int y_scale) {
  if (x_scale == 0 || y_scale == 0)
    return FXRectFromFTPos(left, top, right, bottom);

  return FXRectFromFTPos(left * 1000 / x_scale, top * 1000 / y_scale,
                         right * 1000 / x_scale, bottom * 1000 / y_scale);
}

#ifdef PDF_ENABLE_XFA
unsigned long FTStreamRead(FXFT_StreamRec* stream,
                           unsigned long offset,
                           unsigned char* buffer,
                           unsigned long count) {
  if (count == 0)
    return 0;

  IFX_SeekableReadStream* pFile =
      static_cast<IFX_SeekableReadStream*>(stream->descriptor.pointer);

  // SAFETY: caller ensures `buffer` points to at least `count` bytes.
  return pFile && pFile->ReadBlockAtOffset(
                      UNSAFE_BUFFERS(pdfium::make_span(buffer, count)), offset)
             ? count
             : 0;
}

void FTStreamClose(FXFT_StreamRec* stream) {}
#endif  // PDF_ENABLE_XFA

bool ShouldAppendStyle(const ByteString& style) {
  return !style.IsEmpty() && style != "Regular";
}

}  // namespace

// static
const char CFX_Font::kUntitledFontName[] = "Untitled";

// static
const char CFX_Font::kDefaultAnsiFontName[] = "Helvetica";

// static
const char CFX_Font::kUniversalDefaultFontName[] = "Arial Unicode MS";

// static
pdfium::span<const CFX_Font::CharsetFontMap> CFX_Font::GetDefaultTTFMapSpan() {
  auto map_with_sentinel_value = pdfium::make_span(kDefaultTTFMap);
  return map_with_sentinel_value.first(map_with_sentinel_value.size() - 1);
}

// static
ByteString CFX_Font::GetDefaultFontNameByCharset(FX_Charset nCharset) {
  for (const auto& entry : GetDefaultTTFMapSpan()) {
    if (static_cast<int>(nCharset) == entry.charset) {
      return entry.fontname;
    }
  }
  return kUniversalDefaultFontName;
}

// static
FX_Charset CFX_Font::GetCharSetFromUnicode(uint16_t word) {
  // to avoid CJK Font to show ASCII
  if (word < 0x7F)
    return FX_Charset::kANSI;

  // find new charset
  if ((word >= 0x4E00 && word <= 0x9FA5) ||
      (word >= 0xE7C7 && word <= 0xE7F3) ||
      (word >= 0x3000 && word <= 0x303F) ||
      (word >= 0x2000 && word <= 0x206F)) {
    return FX_Charset::kChineseSimplified;
  }

  if (((word >= 0x3040) && (word <= 0x309F)) ||
      ((word >= 0x30A0) && (word <= 0x30FF)) ||
      ((word >= 0x31F0) && (word <= 0x31FF)) ||
      ((word >= 0xFF00) && (word <= 0xFFEF))) {
    return FX_Charset::kShiftJIS;
  }

  if (((word >= 0xAC00) && (word <= 0xD7AF)) ||
      ((word >= 0x1100) && (word <= 0x11FF)) ||
      ((word >= 0x3130) && (word <= 0x318F))) {
    return FX_Charset::kHangul;
  }

  if (word >= 0x0E00 && word <= 0x0E7F)
    return FX_Charset::kThai;

  if ((word >= 0x0370 && word <= 0x03FF) || (word >= 0x1F00 && word <= 0x1FFF))
    return FX_Charset::kMSWin_Greek;

  if ((word >= 0x0600 && word <= 0x06FF) || (word >= 0xFB50 && word <= 0xFEFC))
    return FX_Charset::kMSWin_Arabic;

  if (word >= 0x0590 && word <= 0x05FF)
    return FX_Charset::kMSWin_Hebrew;

  if (word >= 0x0400 && word <= 0x04FF)
    return FX_Charset::kMSWin_Cyrillic;

  if (word >= 0x0100 && word <= 0x024F)
    return FX_Charset::kMSWin_EasternEuropean;

  if (word >= 0x1E00 && word <= 0x1EFF)
    return FX_Charset::kMSWin_Vietnamese;

  return FX_Charset::kANSI;
}

CFX_Font::CFX_Font() = default;

int CFX_Font::GetSubstFontItalicAngle() const {
  CFX_SubstFont* subst_font = GetSubstFont();
  return subst_font ? subst_font->m_ItalicAngle : 0;
}

#ifdef PDF_ENABLE_XFA
bool CFX_Font::LoadFile(RetainPtr<IFX_SeekableReadStream> pFile,
                        int nFaceIndex) {
  m_ObjectTag = 0;

  auto pStreamRec = std::make_unique<FXFT_StreamRec>();
  pStreamRec->base = nullptr;
  pStreamRec->size = static_cast<unsigned long>(pFile->GetSize());
  pStreamRec->pos = 0;
  pStreamRec->descriptor.pointer = static_cast<void*>(pFile.Get());
  pStreamRec->close = FTStreamClose;
  pStreamRec->read = FTStreamRead;

  FT_Open_Args args;
  args.flags = FT_OPEN_STREAM;
  args.stream = pStreamRec.get();

  m_Face = CFX_Face::Open(CFX_GEModule::Get()->GetFontMgr()->GetFTLibrary(),
                          &args, nFaceIndex);
  if (!m_Face)
    return false;

  m_pOwnedFile = std::move(pFile);
  m_pOwnedStreamRec = std::move(pStreamRec);
  m_Face->SetPixelSize(0, 64);
  return true;
}

#if !BUILDFLAG(IS_WIN)
void CFX_Font::SetFace(RetainPtr<CFX_Face> face) {
  ClearGlyphCache();
  m_ObjectTag = 0;
  m_Face = face;
}

void CFX_Font::SetSubstFont(std::unique_ptr<CFX_SubstFont> subst) {
  m_pSubstFont = std::move(subst);
}
#endif  // !BUILDFLAG(IS_WIN)
#endif  // PDF_ENABLE_XFA

CFX_Font::~CFX_Font() {
  m_FontData = {};  // m_FontData can't outive m_Face.
  m_Face.Reset();

#if BUILDFLAG(IS_APPLE)
  ReleasePlatformResource();
#endif
}

void CFX_Font::LoadSubst(const ByteString& face_name,
                         bool bTrueType,
                         uint32_t flags,
                         int weight,
                         int italic_angle,
                         FX_CodePage code_page,
                         bool bVertical) {
  m_bVertical = bVertical;
  m_ObjectTag = 0;
  m_pSubstFont = std::make_unique<CFX_SubstFont>();
  m_Face = CFX_GEModule::Get()->GetFontMgr()->GetBuiltinMapper()->FindSubstFont(
      face_name, bTrueType, flags, weight, italic_angle, code_page,
      m_pSubstFont.get());
  if (m_Face) {
    m_FontData = m_Face->GetData();
  }
}

int CFX_Font::GetGlyphWidth(uint32_t glyph_index) const {
  return GetGlyphWidth(glyph_index, 0, 0);
}

int CFX_Font::GetGlyphWidth(uint32_t glyph_index,
                            int dest_width,
                            int weight) const {
  return GetOrCreateGlyphCache()->GetGlyphWidth(this, glyph_index, dest_width,
                                                weight);
}

int CFX_Font::GetGlyphWidthImpl(uint32_t glyph_index,
                                int dest_width,
                                int weight) const {
  if (!m_Face)
    return 0;

  return m_Face->GetGlyphWidth(glyph_index, dest_width, weight,
                               m_pSubstFont.get());
}

bool CFX_Font::LoadEmbedded(pdfium::span<const uint8_t> src_span,
                            bool force_vertical,
                            uint64_t object_tag) {
  m_bVertical = force_vertical;
  m_ObjectTag = object_tag;
  m_FontDataAllocation = DataVector<uint8_t>(src_span.begin(), src_span.end());
  m_Face = CFX_GEModule::Get()->GetFontMgr()->NewFixedFace(
      nullptr, m_FontDataAllocation, 0);
  m_FontData = m_FontDataAllocation;
  return !!m_Face;
}

bool CFX_Font::IsTTFont() const {
  return m_Face && m_Face->IsTtOt();
}

int CFX_Font::GetAscent() const {
  if (!m_Face) {
    return 0;
  }
  return NormalizeFontMetric(m_Face->GetAscender(), m_Face->GetUnitsPerEm());
}

int CFX_Font::GetDescent() const {
  if (!m_Face) {
    return 0;
  }
  return NormalizeFontMetric(m_Face->GetDescender(), m_Face->GetUnitsPerEm());
}

std::optional<FX_RECT> CFX_Font::GetGlyphBBox(uint32_t glyph_index) {
  if (!m_Face)
    return std::nullopt;

  if (m_Face->IsTricky()) {
    int error = FT_Set_Char_Size(m_Face->GetRec(), 0, 1000 * 64, 72, 72);
    if (error)
      return std::nullopt;

    error = FT_Load_Glyph(m_Face->GetRec(), glyph_index,
                          FT_LOAD_IGNORE_GLOBAL_ADVANCE_WIDTH);
    if (error)
      return std::nullopt;

    FT_Glyph glyph;
    error = FT_Get_Glyph(m_Face->GetRec()->glyph, &glyph);
    if (error)
      return std::nullopt;

    FT_BBox cbox;
    FT_Glyph_Get_CBox(glyph, FT_GLYPH_BBOX_PIXELS, &cbox);
    int pixel_size_x = m_Face->GetRec()->size->metrics.x_ppem;
    int pixel_size_y = m_Face->GetRec()->size->metrics.y_ppem;
    FX_RECT result = ScaledFXRectFromFTPos(
        cbox.xMin, cbox.yMax, cbox.xMax, cbox.yMin, pixel_size_x, pixel_size_y);
    result.top = std::min(result.top, static_cast<int>(m_Face->GetAscender()));
    result.bottom =
        std::max(result.bottom, static_cast<int>(m_Face->GetDescender()));
    FT_Done_Glyph(glyph);
    if (!m_Face->SetPixelSize(0, 64)) {
      return std::nullopt;
    }
    return result;
  }
  constexpr int kFlag = FT_LOAD_NO_SCALE | FT_LOAD_IGNORE_GLOBAL_ADVANCE_WIDTH;
  if (FT_Load_Glyph(m_Face->GetRec(), glyph_index, kFlag) != 0)
    return std::nullopt;
  int em = m_Face->GetUnitsPerEm();
  return ScaledFXRectFromFTPos(FXFT_Get_Glyph_HoriBearingX(m_Face->GetRec()),
                               FXFT_Get_Glyph_HoriBearingY(m_Face->GetRec()) -
                                   FXFT_Get_Glyph_Height(m_Face->GetRec()),
                               FXFT_Get_Glyph_HoriBearingX(m_Face->GetRec()) +
                                   FXFT_Get_Glyph_Width(m_Face->GetRec()),
                               FXFT_Get_Glyph_HoriBearingY(m_Face->GetRec()),
                               em, em);
}

bool CFX_Font::IsItalic() const {
  if (!m_Face)
    return false;
  if (m_Face->IsItalic()) {
    return true;
  }

  ByteString str = m_Face->GetStyleName();
  str.MakeLower();
  return str.Contains("italic");
}

bool CFX_Font::IsBold() const {
  return m_Face && m_Face->IsBold();
}

bool CFX_Font::IsFixedWidth() const {
  return m_Face && m_Face->IsFixedWidth();
}

#if defined(PDF_USE_SKIA)
bool CFX_Font::IsSubstFontBold() const {
  CFX_SubstFont* subst_font = GetSubstFont();
  return subst_font && subst_font->GetOriginalWeight() >= FXFONT_FW_BOLD;
}
#endif

ByteString CFX_Font::GetPsName() const {
  if (!m_Face)
    return ByteString();

  ByteString psName = FT_Get_Postscript_Name(m_Face->GetRec());
  if (psName.IsEmpty())
    psName = kUntitledFontName;
  return psName;
}

ByteString CFX_Font::GetFamilyName() const {
  if (!m_Face && !m_pSubstFont)
    return ByteString();
  if (m_Face)
    return m_Face->GetFamilyName();
  return m_pSubstFont->m_Family;
}

ByteString CFX_Font::GetFamilyNameOrUntitled() const {
  ByteString facename = GetFamilyName();
  return facename.IsEmpty() ? kUntitledFontName : facename;
}

ByteString CFX_Font::GetBaseFontName() const {
  ByteString psname = GetPsName();
  if (!psname.IsEmpty() && psname != kUntitledFontName)
    return psname;
  if (m_Face) {
    ByteString style = m_Face->GetStyleName();
    ByteString facename = GetFamilyNameOrUntitled();
    if (IsTTFont())
      facename.Remove(' ');
    if (ShouldAppendStyle(style))
      facename += (IsTTFont() ? "," : " ") + style;
    return facename;
  }
  if (m_pSubstFont)
    return m_pSubstFont->m_Family;
  return ByteString();
}

std::optional<FX_RECT> CFX_Font::GetRawBBox() const {
  if (!m_Face)
    return std::nullopt;
  return m_Face->GetBBox();
}

std::optional<FX_RECT> CFX_Font::GetBBox() const {
  std::optional<FX_RECT> result = GetRawBBox();
  if (!result.has_value())
    return result;

  int em = m_Face->GetUnitsPerEm();
  if (em != 0) {
    FX_RECT& bbox = result.value();
    bbox.left = pdfium::saturated_cast<int32_t>((bbox.left * 1000.0f) / em);
    bbox.top = pdfium::saturated_cast<int32_t>((bbox.top * 1000.0f) / em);
    bbox.right = pdfium::saturated_cast<int32_t>((bbox.right * 1000.0f) / em);
    bbox.bottom = pdfium::saturated_cast<int32_t>((bbox.bottom * 1000.0f) / em);
  }
  return result;
}

RetainPtr<CFX_GlyphCache> CFX_Font::GetOrCreateGlyphCache() const {
  if (!m_GlyphCache)
    m_GlyphCache = CFX_GEModule::Get()->GetFontCache()->GetGlyphCache(this);
  return m_GlyphCache;
}

void CFX_Font::ClearGlyphCache() {
  m_GlyphCache = nullptr;
}

std::unique_ptr<CFX_Path> CFX_Font::LoadGlyphPathImpl(uint32_t glyph_index,
                                                      int dest_width) const {
  if (!m_Face)
    return nullptr;

  return m_Face->LoadGlyphPath(glyph_index, dest_width, m_bVertical,
                               m_pSubstFont.get());
}

const CFX_GlyphBitmap* CFX_Font::LoadGlyphBitmap(
    uint32_t glyph_index,
    bool bFontStyle,
    const CFX_Matrix& matrix,
    int dest_width,
    int anti_alias,
    CFX_TextRenderOptions* text_options) const {
  return GetOrCreateGlyphCache()->LoadGlyphBitmap(this, glyph_index, bFontStyle,
                                                  matrix, dest_width,
                                                  anti_alias, text_options);
}

const CFX_Path* CFX_Font::LoadGlyphPath(uint32_t glyph_index,
                                        int dest_width) const {
  return GetOrCreateGlyphCache()->LoadGlyphPath(this, glyph_index, dest_width);
}

#if defined(PDF_USE_SKIA)
CFX_TypeFace* CFX_Font::GetDeviceCache() const {
  return GetOrCreateGlyphCache()->GetDeviceCache(this);
}
#endif
