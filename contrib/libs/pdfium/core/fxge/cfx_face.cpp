// Copyright 2019 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "core/fxge/cfx_face.h"

#include <algorithm>
#include <array>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "core/fxcrt/check.h"
#include "core/fxcrt/check_op.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/notreached.h"
#include "core/fxcrt/numerics/clamped_math.h"
#include "core/fxcrt/numerics/safe_conversions.h"
#include "core/fxcrt/numerics/safe_math.h"
#include "core/fxcrt/span.h"
#include "core/fxcrt/stl_util.h"
#include "core/fxge/cfx_font.h"
#include "core/fxge/cfx_fontmgr.h"
#include "core/fxge/cfx_gemodule.h"
#include "core/fxge/cfx_glyphbitmap.h"
#include "core/fxge/cfx_path.h"
#include "core/fxge/cfx_substfont.h"
#include "core/fxge/dib/cfx_dibitmap.h"
#include "core/fxge/dib/fx_dib.h"
#include "core/fxge/fx_font.h"
#include "core/fxge/fx_fontencoding.h"
#include "core/fxge/scoped_font_transform.h"

#define EM_ADJUST(em, a) (em == 0 ? (a) : (a) * 1000 / em)

namespace {

struct OUTLINE_PARAMS {
  UnownedPtr<CFX_Path> m_pPath;
  FT_Pos m_CurX;
  FT_Pos m_CurY;
  float m_CoordUnit;
};

constexpr int kThousandthMinInt = std::numeric_limits<int>::min() / 1000;
constexpr int kThousandthMaxInt = std::numeric_limits<int>::max() / 1000;

constexpr int kMaxGlyphDimension = 2048;

// Boundary value to avoid integer overflow when adding 1/64th of the value.
constexpr int kMaxRectTop = 2114445437;

constexpr auto kWeightPow = fxcrt::ToArray<const uint8_t>({
    0,   6,   12,  14,  16,  18,  22,  24,  28,  30,  32,  34,  36,  38,  40,
    42,  44,  46,  48,  50,  52,  54,  56,  58,  60,  62,  64,  66,  68,  70,
    70,  72,  72,  74,  74,  74,  76,  76,  76,  78,  78,  78,  80,  80,  80,
    82,  82,  82,  84,  84,  84,  84,  86,  86,  86,  88,  88,  88,  88,  90,
    90,  90,  90,  92,  92,  92,  92,  94,  94,  94,  94,  96,  96,  96,  96,
    96,  98,  98,  98,  98,  100, 100, 100, 100, 100, 102, 102, 102, 102, 102,
    104, 104, 104, 104, 104, 106, 106, 106, 106, 106,
});

constexpr auto kWeightPow11 = fxcrt::ToArray<const uint8_t>({
    0,  4,  7,  8,  9,  10, 12, 13, 15, 17, 18, 19, 20, 21, 22, 23, 24,
    25, 26, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 39, 39, 40, 40, 41,
    41, 41, 42, 42, 42, 43, 43, 43, 44, 44, 44, 45, 45, 45, 46, 46, 46,
    46, 43, 47, 47, 48, 48, 48, 48, 45, 50, 50, 50, 46, 51, 51, 51, 52,
    52, 52, 52, 53, 53, 53, 53, 53, 54, 54, 54, 54, 55, 55, 55, 55, 55,
    56, 56, 56, 56, 56, 57, 57, 57, 57, 57, 58, 58, 58, 58, 58,
});

constexpr auto kWeightPowShiftJis = fxcrt::ToArray<const uint8_t>({
    0,   0,   2,   4,   6,   8,   10,  14,  16,  20,  22,  26,  28,  32,  34,
    38,  42,  44,  48,  52,  56,  60,  64,  66,  70,  74,  78,  82,  86,  90,
    96,  96,  96,  96,  98,  98,  98,  100, 100, 100, 100, 102, 102, 102, 102,
    104, 104, 104, 104, 104, 106, 106, 106, 106, 106, 108, 108, 108, 108, 108,
    110, 110, 110, 110, 110, 112, 112, 112, 112, 112, 112, 114, 114, 114, 114,
    114, 114, 114, 116, 116, 116, 116, 116, 116, 116, 118, 118, 118, 118, 118,
    118, 118, 120, 120, 120, 120, 120, 120, 120, 120,
});

constexpr size_t kWeightPowArraySize = 100;
static_assert(kWeightPowArraySize == std::size(kWeightPow), "Wrong size");
static_assert(kWeightPowArraySize == std::size(kWeightPow11), "Wrong size");
static_assert(kWeightPowArraySize == std::size(kWeightPowShiftJis),
              "Wrong size");

constexpr auto kAngleSkew = fxcrt::ToArray<const int8_t>({
    -0,  -2,  -3,  -5,  -7,  -9,  -11, -12, -14, -16, -18, -19, -21, -23, -25,
    -27, -29, -31, -32, -34, -36, -38, -40, -42, -45, -47, -49, -51, -53, -55,
});

// Returns negative values on failure.
int GetWeightLevel(FX_Charset charset, size_t index) {
  if (index >= kWeightPowArraySize) {
    return -1;
  }

  if (charset == FX_Charset::kShiftJIS) {
    return kWeightPowShiftJis[index];
  }
  return kWeightPow11[index];
}

int GetSkewFromAngle(int angle) {
  // |angle| is non-positive so |-angle| is used as the index. Need to make sure
  // |angle| != INT_MIN since -INT_MIN is undefined.
  if (angle > 0 || angle == std::numeric_limits<int>::min() ||
      static_cast<size_t>(-angle) >= std::size(kAngleSkew)) {
    return -58;
  }
  return kAngleSkew[-angle];
}

int FTPosToCBoxInt(FT_Pos pos) {
  // Boundary values to avoid integer overflow when multiplied by 1000.
  constexpr FT_Pos kMinCBox = -2147483;
  constexpr FT_Pos kMaxCBox = 2147483;
  return static_cast<int>(std::clamp(pos, kMinCBox, kMaxCBox));
}

void Outline_CheckEmptyContour(OUTLINE_PARAMS* param) {
  size_t size;
  {
    pdfium::span<const CFX_Path::Point> points = param->m_pPath->GetPoints();
    size = points.size();

    if (size >= 2 &&
        points[size - 2].IsTypeAndOpen(CFX_Path::Point::Type::kMove) &&
        points[size - 2].m_Point == points[size - 1].m_Point) {
      size -= 2;
    }
    if (size >= 4 &&
        points[size - 4].IsTypeAndOpen(CFX_Path::Point::Type::kMove) &&
        points[size - 3].IsTypeAndOpen(CFX_Path::Point::Type::kBezier) &&
        points[size - 3].m_Point == points[size - 4].m_Point &&
        points[size - 2].m_Point == points[size - 4].m_Point &&
        points[size - 1].m_Point == points[size - 4].m_Point) {
      size -= 4;
    }
  }
  // Only safe after |points| has been destroyed.
  param->m_pPath->GetPoints().resize(size);
}

int Outline_MoveTo(const FT_Vector* to, void* user) {
  OUTLINE_PARAMS* param = static_cast<OUTLINE_PARAMS*>(user);

  Outline_CheckEmptyContour(param);

  param->m_pPath->ClosePath();
  param->m_pPath->AppendPoint(
      CFX_PointF(to->x / param->m_CoordUnit, to->y / param->m_CoordUnit),
      CFX_Path::Point::Type::kMove);

  param->m_CurX = to->x;
  param->m_CurY = to->y;
  return 0;
}

int Outline_LineTo(const FT_Vector* to, void* user) {
  OUTLINE_PARAMS* param = static_cast<OUTLINE_PARAMS*>(user);

  param->m_pPath->AppendPoint(
      CFX_PointF(to->x / param->m_CoordUnit, to->y / param->m_CoordUnit),
      CFX_Path::Point::Type::kLine);

  param->m_CurX = to->x;
  param->m_CurY = to->y;
  return 0;
}

int Outline_ConicTo(const FT_Vector* control, const FT_Vector* to, void* user) {
  OUTLINE_PARAMS* param = static_cast<OUTLINE_PARAMS*>(user);

  param->m_pPath->AppendPoint(
      CFX_PointF((param->m_CurX + (control->x - param->m_CurX) * 2 / 3) /
                     param->m_CoordUnit,
                 (param->m_CurY + (control->y - param->m_CurY) * 2 / 3) /
                     param->m_CoordUnit),
      CFX_Path::Point::Type::kBezier);

  param->m_pPath->AppendPoint(
      CFX_PointF((control->x + (to->x - control->x) / 3) / param->m_CoordUnit,
                 (control->y + (to->y - control->y) / 3) / param->m_CoordUnit),
      CFX_Path::Point::Type::kBezier);

  param->m_pPath->AppendPoint(
      CFX_PointF(to->x / param->m_CoordUnit, to->y / param->m_CoordUnit),
      CFX_Path::Point::Type::kBezier);

  param->m_CurX = to->x;
  param->m_CurY = to->y;
  return 0;
}

int Outline_CubicTo(const FT_Vector* control1,
                    const FT_Vector* control2,
                    const FT_Vector* to,
                    void* user) {
  OUTLINE_PARAMS* param = static_cast<OUTLINE_PARAMS*>(user);

  param->m_pPath->AppendPoint(CFX_PointF(control1->x / param->m_CoordUnit,
                                         control1->y / param->m_CoordUnit),
                              CFX_Path::Point::Type::kBezier);

  param->m_pPath->AppendPoint(CFX_PointF(control2->x / param->m_CoordUnit,
                                         control2->y / param->m_CoordUnit),
                              CFX_Path::Point::Type::kBezier);

  param->m_pPath->AppendPoint(
      CFX_PointF(to->x / param->m_CoordUnit, to->y / param->m_CoordUnit),
      CFX_Path::Point::Type::kBezier);

  param->m_CurX = to->x;
  param->m_CurY = to->y;
  return 0;
}

FT_Encoding ToFTEncoding(fxge::FontEncoding encoding) {
  switch (encoding) {
    case fxge::FontEncoding::kAdobeCustom:
      return FT_ENCODING_ADOBE_CUSTOM;
    case fxge::FontEncoding::kAdobeExpert:
      return FT_ENCODING_ADOBE_EXPERT;
    case fxge::FontEncoding::kAdobeStandard:
      return FT_ENCODING_ADOBE_STANDARD;
    case fxge::FontEncoding::kAppleRoman:
      return FT_ENCODING_APPLE_ROMAN;
    case fxge::FontEncoding::kBig5:
      return FT_ENCODING_BIG5;
    case fxge::FontEncoding::kGB2312:
      return FT_ENCODING_PRC;
    case fxge::FontEncoding::kJohab:
      return FT_ENCODING_JOHAB;
    case fxge::FontEncoding::kLatin1:
      return FT_ENCODING_ADOBE_LATIN_1;
    case fxge::FontEncoding::kNone:
      return FT_ENCODING_NONE;
    case fxge::FontEncoding::kOldLatin2:
      return FT_ENCODING_OLD_LATIN_2;
    case fxge::FontEncoding::kSjis:
      return FT_ENCODING_SJIS;
    case fxge::FontEncoding::kSymbol:
      return FT_ENCODING_MS_SYMBOL;
    case fxge::FontEncoding::kUnicode:
      return FT_ENCODING_UNICODE;
    case fxge::FontEncoding::kWansung:
      return FT_ENCODING_WANSUNG;
  }
}

fxge::FontEncoding ToFontEncoding(uint32_t ft_encoding) {
  switch (ft_encoding) {
    case FT_ENCODING_ADOBE_CUSTOM:
      return fxge::FontEncoding::kAdobeCustom;
    case FT_ENCODING_ADOBE_EXPERT:
      return fxge::FontEncoding::kAdobeExpert;
    case FT_ENCODING_ADOBE_STANDARD:
      return fxge::FontEncoding::kAdobeStandard;
    case FT_ENCODING_APPLE_ROMAN:
      return fxge::FontEncoding::kAppleRoman;
    case FT_ENCODING_BIG5:
      return fxge::FontEncoding::kBig5;
    case FT_ENCODING_PRC:
      return fxge::FontEncoding::kGB2312;
    case FT_ENCODING_JOHAB:
      return fxge::FontEncoding::kJohab;
    case FT_ENCODING_ADOBE_LATIN_1:
      return fxge::FontEncoding::kLatin1;
    case FT_ENCODING_NONE:
      return fxge::FontEncoding::kNone;
    case FT_ENCODING_OLD_LATIN_2:
      return fxge::FontEncoding::kOldLatin2;
    case FT_ENCODING_SJIS:
      return fxge::FontEncoding::kSjis;
    case FT_ENCODING_MS_SYMBOL:
      return fxge::FontEncoding::kSymbol;
    case FT_ENCODING_UNICODE:
      return fxge::FontEncoding::kUnicode;
    case FT_ENCODING_WANSUNG:
      return fxge::FontEncoding::kWansung;
  }
  NOTREACHED_NORETURN();
}

}  // namespace

// static
RetainPtr<CFX_Face> CFX_Face::New(FT_Library library,
                                  RetainPtr<Retainable> pDesc,
                                  pdfium::span<const FT_Byte> data,
                                  FT_Long face_index) {
  FXFT_FaceRec* pRec = nullptr;
  if (FT_New_Memory_Face(library, data.data(),
                         pdfium::checked_cast<FT_Long>(data.size()), face_index,
                         &pRec) != 0) {
    return nullptr;
  }
  // Private ctor.
  return pdfium::WrapRetain(new CFX_Face(pRec, std::move(pDesc)));
}

// static
RetainPtr<CFX_Face> CFX_Face::Open(FT_Library library,
                                   const FT_Open_Args* args,
                                   FT_Long face_index) {
  FXFT_FaceRec* pRec = nullptr;
  if (FT_Open_Face(library, args, face_index, &pRec) != 0)
    return nullptr;

  // Private ctor.
  return pdfium::WrapRetain(new CFX_Face(pRec, nullptr));
}

bool CFX_Face::HasGlyphNames() const {
  return !!(GetRec()->face_flags & FT_FACE_FLAG_GLYPH_NAMES);
}

bool CFX_Face::IsTtOt() const {
  return !!(GetRec()->face_flags & FT_FACE_FLAG_SFNT);
}

bool CFX_Face::IsTricky() const {
  return !!(GetRec()->face_flags & FT_FACE_FLAG_TRICKY);
}

bool CFX_Face::IsFixedWidth() const {
  return !!(GetRec()->face_flags & FT_FACE_FLAG_FIXED_WIDTH);
}

#if defined(PDF_ENABLE_XFA)
bool CFX_Face::IsScalable() const {
  return !!(GetRec()->face_flags & FT_FACE_FLAG_SCALABLE);
}

void CFX_Face::ClearExternalStream() {
  GetRec()->face_flags &= ~FT_FACE_FLAG_EXTERNAL_STREAM;
}
#endif

bool CFX_Face::IsItalic() const {
  return !!(GetRec()->style_flags & FT_STYLE_FLAG_ITALIC);
}

bool CFX_Face::IsBold() const {
  return !!(GetRec()->style_flags & FT_STYLE_FLAG_BOLD);
}

ByteString CFX_Face::GetFamilyName() const {
  return ByteString(GetRec()->family_name);
}

ByteString CFX_Face::GetStyleName() const {
  return ByteString(GetRec()->style_name);
}

FX_RECT CFX_Face::GetBBox() const {
  return FX_RECT(pdfium::checked_cast<int32_t>(GetRec()->bbox.xMin),
                 pdfium::checked_cast<int32_t>(GetRec()->bbox.yMin),
                 pdfium::checked_cast<int32_t>(GetRec()->bbox.xMax),
                 pdfium::checked_cast<int32_t>(GetRec()->bbox.yMax));
}

uint16_t CFX_Face::GetUnitsPerEm() const {
  return pdfium::checked_cast<uint16_t>(GetRec()->units_per_EM);
}

int16_t CFX_Face::GetAscender() const {
  return pdfium::checked_cast<int16_t>(GetRec()->ascender);
}

int16_t CFX_Face::GetDescender() const {
  return pdfium::checked_cast<int16_t>(GetRec()->descender);
}

#if BUILDFLAG(IS_ANDROID)
int16_t CFX_Face::GetHeight() const {
  return pdfium::checked_cast<int16_t>(GetRec()->height);
}
#endif

pdfium::span<uint8_t> CFX_Face::GetData() const {
  // TODO(tsepez): justify safety from library API.
  return UNSAFE_BUFFERS(
      pdfium::make_span(GetRec()->stream->base, GetRec()->stream->size));
}

size_t CFX_Face::GetSfntTable(uint32_t table, pdfium::span<uint8_t> buffer) {
  unsigned long length = pdfium::checked_cast<unsigned long>(buffer.size());
  if (length) {
    int error = FT_Load_Sfnt_Table(GetRec(), table, 0, buffer.data(), &length);
    if (error || length != buffer.size()) {
      return 0;
    }
    return buffer.size();
  }

  int error = FT_Load_Sfnt_Table(GetRec(), table, 0, nullptr, &length);
  if (error || !length) {
    return 0;
  }
  return pdfium::checked_cast<size_t>(length);
}

std::optional<std::array<uint32_t, 4>> CFX_Face::GetOs2UnicodeRange() {
  auto* os2 = static_cast<TT_OS2*>(FT_Get_Sfnt_Table(GetRec(), FT_SFNT_OS2));
  if (!os2) {
    return std::nullopt;
  }
  return std::array<uint32_t, 4>{static_cast<uint32_t>(os2->ulUnicodeRange1),
                                 static_cast<uint32_t>(os2->ulUnicodeRange2),
                                 static_cast<uint32_t>(os2->ulUnicodeRange3),
                                 static_cast<uint32_t>(os2->ulUnicodeRange4)};
}

std::optional<std::array<uint32_t, 2>> CFX_Face::GetOs2CodePageRange() {
  auto* os2 = static_cast<TT_OS2*>(FT_Get_Sfnt_Table(GetRec(), FT_SFNT_OS2));
  if (!os2) {
    return std::nullopt;
  }
  return std::array<uint32_t, 2>{static_cast<uint32_t>(os2->ulCodePageRange1),
                                 static_cast<uint32_t>(os2->ulCodePageRange2)};
}

std::optional<std::array<uint8_t, 2>> CFX_Face::GetOs2Panose() {
  auto* os2 = static_cast<TT_OS2*>(FT_Get_Sfnt_Table(GetRec(), FT_SFNT_OS2));
  if (!os2) {
    return std::nullopt;
  }
  // SAFETY: required from library.
  return UNSAFE_BUFFERS(std::array<uint8_t, 2>{os2->panose[0], os2->panose[1]});
}

int CFX_Face::GetGlyphCount() const {
  return pdfium::checked_cast<int>(GetRec()->num_glyphs);
}

std::unique_ptr<CFX_GlyphBitmap> CFX_Face::RenderGlyph(const CFX_Font* pFont,
                                                       uint32_t glyph_index,
                                                       bool bFontStyle,
                                                       const CFX_Matrix& matrix,
                                                       int dest_width,
                                                       int anti_alias) {
  FT_Matrix ft_matrix;
  ft_matrix.xx = matrix.a / 64 * 65536;
  ft_matrix.xy = matrix.c / 64 * 65536;
  ft_matrix.yx = matrix.b / 64 * 65536;
  ft_matrix.yy = matrix.d / 64 * 65536;
  bool bUseCJKSubFont = false;
  const CFX_SubstFont* pSubstFont = pFont->GetSubstFont();
  if (pSubstFont) {
    bUseCJKSubFont = pSubstFont->m_bSubstCJK && bFontStyle;
    int angle;
    if (bUseCJKSubFont) {
      angle = pSubstFont->m_bItalicCJK ? -15 : 0;
    } else {
      angle = pSubstFont->m_ItalicAngle;
    }
    if (angle) {
      int skew = GetSkewFromAngle(angle);
      if (pFont->IsVertical()) {
        ft_matrix.yx += ft_matrix.yy * skew / 100;
      } else {
        ft_matrix.xy -= ft_matrix.xx * skew / 100;
      }
    }
    if (pSubstFont->IsBuiltInGenericFont()) {
      pFont->GetFace()->AdjustVariationParams(glyph_index, dest_width,
                                              pFont->GetSubstFont()->m_Weight);
    }
  }

  ScopedFontTransform scoped_transform(pdfium::WrapRetain(this), &ft_matrix);
  int load_flags = FT_LOAD_NO_BITMAP | FT_LOAD_PEDANTIC;
  if (!IsTtOt()) {
    load_flags |= FT_LOAD_NO_HINTING;
  }
  FXFT_FaceRec* rec = GetRec();
  int error = FT_Load_Glyph(rec, glyph_index, load_flags);
  if (error) {
    // if an error is returned, try to reload glyphs without hinting.
    if (load_flags & FT_LOAD_NO_HINTING) {
      return nullptr;
    }

    load_flags |= FT_LOAD_NO_HINTING;
    load_flags &= ~FT_LOAD_PEDANTIC;
    error = FT_Load_Glyph(rec, glyph_index, load_flags);
    if (error) {
      return nullptr;
    }
  }

  auto* glyph = rec->glyph;
  int weight;
  if (bUseCJKSubFont) {
    weight = pSubstFont->m_WeightCJK;
  } else {
    weight = pSubstFont ? pSubstFont->m_Weight : 0;
  }
  if (pSubstFont && !pSubstFont->IsBuiltInGenericFont() && weight > 400) {
    uint32_t index = (weight - 400) / 10;
    pdfium::CheckedNumeric<signed long> level =
        GetWeightLevel(pSubstFont->m_Charset, index);
    if (level.ValueOrDefault(-1) < 0) {
      return nullptr;
    }

    level = level *
            (abs(static_cast<int>(ft_matrix.xx)) +
             abs(static_cast<int>(ft_matrix.xy))) /
            36655;
    FT_Outline_Embolden(&glyph->outline, level.ValueOrDefault(0));
  }
  FT_Library_SetLcdFilter(CFX_GEModule::Get()->GetFontMgr()->GetFTLibrary(),
                          FT_LCD_FILTER_DEFAULT);
  error = FT_Render_Glyph(glyph, static_cast<FT_Render_Mode>(anti_alias));
  if (error) {
    return nullptr;
  }

  const FT_Bitmap& bitmap = glyph->bitmap;
  if (bitmap.width > kMaxGlyphDimension || bitmap.rows > kMaxGlyphDimension) {
    return nullptr;
  }
  int dib_width = bitmap.width;
  auto pGlyphBitmap =
      std::make_unique<CFX_GlyphBitmap>(glyph->bitmap_left, glyph->bitmap_top);
  const FXDIB_Format format = anti_alias == FT_RENDER_MODE_MONO
                                  ? FXDIB_Format::k1bppMask
                                  : FXDIB_Format::k8bppMask;
  if (!pGlyphBitmap->GetBitmap()->Create(dib_width, bitmap.rows, format)) {
    return nullptr;
  }

  int dest_pitch = pGlyphBitmap->GetBitmap()->GetPitch();
  uint8_t* pDestBuf = pGlyphBitmap->GetBitmap()->GetWritableBuffer().data();
  const uint8_t* pSrcBuf = bitmap.buffer;
  UNSAFE_TODO({
    if (anti_alias != FT_RENDER_MODE_MONO &&
        bitmap.pixel_mode == FT_PIXEL_MODE_MONO) {
      unsigned int bytes = anti_alias == FT_RENDER_MODE_LCD ? 3 : 1;
      for (unsigned int i = 0; i < bitmap.rows; i++) {
        for (unsigned int n = 0; n < bitmap.width; n++) {
          uint8_t data =
              (pSrcBuf[i * bitmap.pitch + n / 8] & (0x80 >> (n % 8))) ? 255 : 0;
          for (unsigned int b = 0; b < bytes; b++) {
            pDestBuf[i * dest_pitch + n * bytes + b] = data;
          }
        }
      }
    } else {
      FXSYS_memset(pDestBuf, 0, dest_pitch * bitmap.rows);
      int rowbytes = std::min(abs(bitmap.pitch), dest_pitch);
      for (unsigned int row = 0; row < bitmap.rows; row++) {
        FXSYS_memcpy(pDestBuf + row * dest_pitch, pSrcBuf + row * bitmap.pitch,
                     rowbytes);
      }
    }
  });
  return pGlyphBitmap;
}

std::unique_ptr<CFX_Path> CFX_Face::LoadGlyphPath(
    uint32_t glyph_index,
    int dest_width,
    bool is_vertical,
    const CFX_SubstFont* subst_font) {
  FXFT_FaceRec* rec = GetRec();
  FT_Set_Pixel_Sizes(rec, 0, 64);
  FT_Matrix ft_matrix = {65536, 0, 0, 65536};
  if (subst_font) {
    if (subst_font->m_ItalicAngle) {
      int skew = GetSkewFromAngle(subst_font->m_ItalicAngle);
      if (is_vertical) {
        ft_matrix.yx += ft_matrix.yy * skew / 100;
      } else {
        ft_matrix.xy -= ft_matrix.xx * skew / 100;
      }
    }
    if (subst_font->IsBuiltInGenericFont()) {
      AdjustVariationParams(glyph_index, dest_width, subst_font->m_Weight);
    }
  }
  ScopedFontTransform scoped_transform(pdfium::WrapRetain(this), &ft_matrix);
  int load_flags = FT_LOAD_NO_BITMAP;
  if (!IsTtOt() || !IsTricky()) {
    load_flags |= FT_LOAD_NO_HINTING;
  }
  if (FT_Load_Glyph(rec, glyph_index, load_flags)) {
    return nullptr;
  }
  if (subst_font && !subst_font->IsBuiltInGenericFont() &&
      subst_font->m_Weight > 400) {
    uint32_t index = std::min<uint32_t>((subst_font->m_Weight - 400) / 10,
                                        kWeightPowArraySize - 1);
    int level;
    if (subst_font->m_Charset == FX_Charset::kShiftJIS) {
      level = kWeightPowShiftJis[index] * 65536 / 36655;
    } else {
      level = kWeightPow[index];
    }
    FT_Outline_Embolden(&rec->glyph->outline, level);
  }

  FT_Outline_Funcs funcs;
  funcs.move_to = Outline_MoveTo;
  funcs.line_to = Outline_LineTo;
  funcs.conic_to = Outline_ConicTo;
  funcs.cubic_to = Outline_CubicTo;
  funcs.shift = 0;
  funcs.delta = 0;

  auto pPath = std::make_unique<CFX_Path>();
  OUTLINE_PARAMS params;
  params.m_pPath = pPath.get();
  params.m_CurX = params.m_CurY = 0;
  params.m_CoordUnit = 64 * 64.0;

  FT_Outline_Decompose(&rec->glyph->outline, &funcs, &params);
  if (pPath->GetPoints().empty()) {
    return nullptr;
  }

  Outline_CheckEmptyContour(&params);
  pPath->ClosePath();
  return pPath;
}

int CFX_Face::GetGlyphWidth(uint32_t glyph_index,
                            int dest_width,
                            int weight,
                            const CFX_SubstFont* subst_font) {
  if (subst_font && subst_font->IsBuiltInGenericFont()) {
    AdjustVariationParams(glyph_index, dest_width, weight);
  }

  FXFT_FaceRec* rec = GetRec();
  int err = FT_Load_Glyph(
      rec, glyph_index, FT_LOAD_NO_SCALE | FT_LOAD_IGNORE_GLOBAL_ADVANCE_WIDTH);
  if (err) {
    return 0;
  }

  FT_Pos horizontal_advance = rec->glyph->metrics.horiAdvance;
  if (horizontal_advance < kThousandthMinInt ||
      horizontal_advance > kThousandthMaxInt) {
    return 0;
  }

  return static_cast<int>(EM_ADJUST(GetUnitsPerEm(), horizontal_advance));
}

ByteString CFX_Face::GetGlyphName(uint32_t glyph_index) {
  char name[256] = {};
  FT_Get_Glyph_Name(GetRec(), glyph_index, name, sizeof(name));
  name[255] = 0;
  return ByteString(name);
}

int CFX_Face::GetCharIndex(uint32_t code) {
  return FT_Get_Char_Index(GetRec(), code);
}

int CFX_Face::GetNameIndex(const char* name) {
  return FT_Get_Name_Index(GetRec(), name);
}

FX_RECT CFX_Face::GetCharBBox(uint32_t code, int glyph_index) {
  FX_RECT rect;
  FXFT_FaceRec* rec = GetRec();
  if (IsTricky()) {
    int err =
        FT_Load_Glyph(rec, glyph_index, FT_LOAD_IGNORE_GLOBAL_ADVANCE_WIDTH);
    if (!err) {
      FT_Glyph glyph;
      err = FT_Get_Glyph(rec->glyph, &glyph);
      if (!err) {
        FT_BBox cbox;
        FT_Glyph_Get_CBox(glyph, FT_GLYPH_BBOX_PIXELS, &cbox);
        const int xMin = FTPosToCBoxInt(cbox.xMin);
        const int xMax = FTPosToCBoxInt(cbox.xMax);
        const int yMin = FTPosToCBoxInt(cbox.yMin);
        const int yMax = FTPosToCBoxInt(cbox.yMax);
        const int pixel_size_x = rec->size->metrics.x_ppem;
        const int pixel_size_y = rec->size->metrics.y_ppem;
        if (pixel_size_x == 0 || pixel_size_y == 0) {
          rect = FX_RECT(xMin, yMax, xMax, yMin);
        } else {
          rect =
              FX_RECT(xMin * 1000 / pixel_size_x, yMax * 1000 / pixel_size_y,
                      xMax * 1000 / pixel_size_x, yMin * 1000 / pixel_size_y);
        }
        rect.top = std::min(rect.top, static_cast<int>(GetAscender()));
        rect.bottom = std::max(rect.bottom, static_cast<int>(GetDescender()));
        FT_Done_Glyph(glyph);
      }
    }
  } else {
    int err = FT_Load_Glyph(rec, glyph_index, FT_LOAD_NO_SCALE);
    if (err == 0) {
      rect = GetGlyphBBox();
      if (rect.top <= kMaxRectTop) {
        rect.top += rect.top / 64;
      } else {
        rect.top = std::numeric_limits<int>::max();
      }
    }
  }
  return rect;
}

FX_RECT CFX_Face::GetGlyphBBox() const {
  const auto* glyph = GetRec()->glyph;
  pdfium::ClampedNumeric<FT_Pos> left = glyph->metrics.horiBearingX;
  pdfium::ClampedNumeric<FT_Pos> top = glyph->metrics.horiBearingY;
  const uint16_t upem = GetUnitsPerEm();
  return FX_RECT(NormalizeFontMetric(left, upem),
                 NormalizeFontMetric(top, upem),
                 NormalizeFontMetric(left + glyph->metrics.width, upem),
                 NormalizeFontMetric(top - glyph->metrics.height, upem));
}

std::vector<CFX_Face::CharCodeAndIndex> CFX_Face::GetCharCodesAndIndices(
    char32_t max_char) {
  CharCodeAndIndex char_code_and_index;
  char_code_and_index.char_code = static_cast<uint32_t>(
      FT_Get_First_Char(GetRec(), &char_code_and_index.glyph_index));
  if (char_code_and_index.char_code > max_char) {
    return {};
  }
  std::vector<CharCodeAndIndex> results = {char_code_and_index};
  while (true) {
    char_code_and_index.char_code = static_cast<uint32_t>(FT_Get_Next_Char(
        GetRec(), results.back().char_code, &char_code_and_index.glyph_index));
    if (char_code_and_index.char_code > max_char ||
        char_code_and_index.glyph_index == 0) {
      return results;
    }
    results.push_back(char_code_and_index);
  }
}

CFX_Face::CharMap CFX_Face::GetCurrentCharMap() const {
  return GetRec()->charmap;
}

std::optional<fxge::FontEncoding> CFX_Face::GetCurrentCharMapEncoding() const {
  if (!GetRec()->charmap) {
    return std::nullopt;
  }
  return ToFontEncoding(GetRec()->charmap->encoding);
}

int CFX_Face::GetCharMapPlatformIdByIndex(size_t index) const {
  CHECK_LT(index, GetCharMapCount());
  // SAFETY: required from library as enforced by check above.
  return UNSAFE_BUFFERS(GetRec()->charmaps[index]->platform_id);
}

int CFX_Face::GetCharMapEncodingIdByIndex(size_t index) const {
  CHECK_LT(index, GetCharMapCount());
  // SAFETY: required from library as enforced by check above.
  return UNSAFE_BUFFERS(GetRec()->charmaps[index]->encoding_id);
}

fxge::FontEncoding CFX_Face::GetCharMapEncodingByIndex(size_t index) const {
  CHECK_LT(index, GetCharMapCount());
  // SAFETY: required from library as enforced by check above.
  return ToFontEncoding(UNSAFE_BUFFERS(GetRec()->charmaps[index]->encoding));
}

size_t CFX_Face::GetCharMapCount() const {
  return GetRec()->charmaps
             ? pdfium::checked_cast<size_t>(GetRec()->num_charmaps)
             : 0;
}

void CFX_Face::SetCharMap(CharMap map) {
  FT_Set_Charmap(GetRec(), static_cast<FT_CharMap>(map));
}

void CFX_Face::SetCharMapByIndex(size_t index) {
  CHECK_LT(index, GetCharMapCount());
  // SAFETY: required from library as enforced by check above.
  SetCharMap(UNSAFE_BUFFERS(GetRec()->charmaps[index]));
}

bool CFX_Face::SelectCharMap(fxge::FontEncoding encoding) {
  FT_Error error = FT_Select_Charmap(GetRec(), ToFTEncoding(encoding));
  return !error;
}

bool CFX_Face::SetPixelSize(uint32_t width, uint32_t height) {
  FT_Error error = FT_Set_Pixel_Sizes(GetRec(), width, height);
  return !error;
}

#if BUILDFLAG(IS_WIN)
bool CFX_Face::CanEmbed() {
  FT_UShort fstype = FT_Get_FSType_Flags(GetRec());
  return (fstype & (FT_FSTYPE_RESTRICTED_LICENSE_EMBEDDING |
                    FT_FSTYPE_BITMAP_EMBEDDING_ONLY)) == 0;
}
#endif

CFX_Face::CFX_Face(FXFT_FaceRec* rec, RetainPtr<Retainable> pDesc)
    : m_pRec(rec), m_pDesc(std::move(pDesc)) {
  DCHECK(m_pRec);
}

CFX_Face::~CFX_Face() = default;

void CFX_Face::AdjustVariationParams(int glyph_index,
                                     int dest_width,
                                     int weight) {
  DCHECK(dest_width >= 0);

  FXFT_FaceRec* rec = GetRec();
  ScopedFXFTMMVar variation_desc(rec);
  if (!variation_desc) {
    return;
  }

  FT_Pos coords[2];
  if (weight == 0) {
    coords[0] = variation_desc.GetAxisDefault(0) / 65536;
  } else {
    coords[0] = weight;
  }

  if (dest_width == 0) {
    coords[1] = variation_desc.GetAxisDefault(1) / 65536;
  } else {
    FT_Long min_param = variation_desc.GetAxisMin(1) / 65536;
    FT_Long max_param = variation_desc.GetAxisMax(1) / 65536;
    coords[1] = min_param;
    FT_Set_MM_Design_Coordinates(rec, 2, coords);
    FT_Load_Glyph(rec, glyph_index,
                  FT_LOAD_NO_SCALE | FT_LOAD_IGNORE_GLOBAL_ADVANCE_WIDTH);
    FT_Pos min_width = rec->glyph->metrics.horiAdvance * 1000 / GetUnitsPerEm();
    coords[1] = max_param;
    FT_Set_MM_Design_Coordinates(rec, 2, coords);
    FT_Load_Glyph(rec, glyph_index,
                  FT_LOAD_NO_SCALE | FT_LOAD_IGNORE_GLOBAL_ADVANCE_WIDTH);
    FT_Pos max_width = rec->glyph->metrics.horiAdvance * 1000 / GetUnitsPerEm();
    if (max_width == min_width) {
      return;
    }
    FT_Pos param = min_param + (max_param - min_param) *
                                   (dest_width - min_width) /
                                   (max_width - min_width);
    coords[1] = param;
  }
  FT_Set_MM_Design_Coordinates(rec, 2, coords);
}
