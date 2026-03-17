// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxge/cfx_glyphcache.h"

#include <initializer_list>
#include <memory>
#include <utility>

#include "build/build_config.h"
#include "core/fxcrt/fx_codepage.h"
#include "core/fxcrt/fx_memcpy_wrappers.h"
#include "core/fxcrt/span.h"
#include "core/fxge/cfx_defaultrenderdevice.h"
#include "core/fxge/cfx_font.h"
#include "core/fxge/cfx_glyphbitmap.h"
#include "core/fxge/cfx_path.h"
#include "core/fxge/cfx_substfont.h"

#if defined(PDF_USE_SKIA)
#error #include "third_party/skia/include/core/SkStream.h"
#error #include "third_party/skia/include/core/SkTypeface.h"
#error #include "third_party/skia/include/core/SkFontMgr.h"
#error #include "third_party/skia/include/ports/SkFontMgr_empty.h"

#if BUILDFLAG(IS_WIN)
#error #include "third_party/skia/include/ports/SkTypeface_win.h"
#elif BUILDFLAG(IS_APPLE)
#error #include "third_party/skia/include/ports/SkFontMgr_mac_ct.h"
#endif

#endif

#if BUILDFLAG(IS_APPLE)
#include "core/fxge/cfx_textrenderoptions.h"
#endif

namespace {

constexpr uint32_t kInvalidGlyphIndex = static_cast<uint32_t>(-1);

class UniqueKeyGen {
 public:
  UniqueKeyGen(const CFX_Font* pFont,
               const CFX_Matrix& matrix,
               int dest_width,
               int anti_alias,
               bool bNative);

  pdfium::span<const uint8_t> span() const;

 private:
  void Initialize(std::initializer_list<const int> args);

  size_t key_len_;
  uint32_t key_[32];
};

void UniqueKeyGen::Initialize(std::initializer_list<const int32_t> args) {
  auto key_span = pdfium::make_span(key_);
  for (const auto& arg : args) {
    key_span.front() = arg;
    key_span = key_span.subspan(1);
  }
  key_len_ = args.size();
}

pdfium::span<const uint8_t> UniqueKeyGen::span() const {
  return pdfium::as_bytes(pdfium::make_span(key_).first(key_len_));
}

UniqueKeyGen::UniqueKeyGen(const CFX_Font* pFont,
                           const CFX_Matrix& matrix,
                           int dest_width,
                           int anti_alias,
                           bool bNative) {
  int nMatrixA = static_cast<int>(matrix.a * 10000);
  int nMatrixB = static_cast<int>(matrix.b * 10000);
  int nMatrixC = static_cast<int>(matrix.c * 10000);
  int nMatrixD = static_cast<int>(matrix.d * 10000);

#if BUILDFLAG(IS_APPLE)
  if (bNative) {
    if (pFont->GetSubstFont()) {
      Initialize({nMatrixA, nMatrixB, nMatrixC, nMatrixD, dest_width,
                  anti_alias, pFont->GetSubstFont()->m_Weight,
                  pFont->GetSubstFont()->m_ItalicAngle, pFont->IsVertical(),
                  3});
    } else {
      Initialize(
          {nMatrixA, nMatrixB, nMatrixC, nMatrixD, dest_width, anti_alias, 3});
    }
    return;
  }
#endif

  CHECK(!bNative);
  if (pFont->GetSubstFont()) {
    Initialize({nMatrixA, nMatrixB, nMatrixC, nMatrixD, dest_width, anti_alias,
                pFont->GetSubstFont()->m_Weight,
                pFont->GetSubstFont()->m_ItalicAngle, pFont->IsVertical()});
  } else {
    Initialize(
        {nMatrixA, nMatrixB, nMatrixC, nMatrixD, dest_width, anti_alias});
  }
}

}  // namespace

CFX_GlyphCache::CFX_GlyphCache(RetainPtr<CFX_Face> face)
    : m_Face(std::move(face)) {}

CFX_GlyphCache::~CFX_GlyphCache() = default;

std::unique_ptr<CFX_GlyphBitmap> CFX_GlyphCache::RenderGlyph(
    const CFX_Font* pFont,
    uint32_t glyph_index,
    bool bFontStyle,
    const CFX_Matrix& matrix,
    int dest_width,
    int anti_alias) {
  if (!m_Face) {
    return nullptr;
  }

  return m_Face->RenderGlyph(pFont, glyph_index, bFontStyle, matrix, dest_width,
                             anti_alias);
}

const CFX_Path* CFX_GlyphCache::LoadGlyphPath(const CFX_Font* pFont,
                                              uint32_t glyph_index,
                                              int dest_width) {
  if (!GetFace() || glyph_index == kInvalidGlyphIndex) {
    return nullptr;
  }

  const auto* pSubstFont = pFont->GetSubstFont();
  int weight = pSubstFont ? pSubstFont->m_Weight : 0;
  int angle = pSubstFont ? pSubstFont->m_ItalicAngle : 0;
  bool vertical = pSubstFont && pFont->IsVertical();
  const PathMapKey key =
      std::make_tuple(glyph_index, dest_width, weight, angle, vertical);
  auto it = m_PathMap.find(key);
  if (it != m_PathMap.end())
    return it->second.get();

  m_PathMap[key] = pFont->LoadGlyphPathImpl(glyph_index, dest_width);
  return m_PathMap[key].get();
}

const CFX_GlyphBitmap* CFX_GlyphCache::LoadGlyphBitmap(
    const CFX_Font* pFont,
    uint32_t glyph_index,
    bool bFontStyle,
    const CFX_Matrix& matrix,
    int dest_width,
    int anti_alias,
    CFX_TextRenderOptions* text_options) {
  if (glyph_index == kInvalidGlyphIndex)
    return nullptr;

#if BUILDFLAG(IS_APPLE)
  const bool bNative = text_options->native_text;
#else
  const bool bNative = false;
#endif
  UniqueKeyGen keygen(pFont, matrix, dest_width, anti_alias, bNative);
  auto FaceGlyphsKey = ByteString(ByteStringView(keygen.span()));

#if BUILDFLAG(IS_APPLE)
  const bool bDoLookUp =
      !text_options->native_text || CFX_DefaultRenderDevice::UseSkiaRenderer();
#else
  const bool bDoLookUp = true;
#endif
  if (bDoLookUp) {
    return LookUpGlyphBitmap(pFont, matrix, FaceGlyphsKey, glyph_index,
                             bFontStyle, dest_width, anti_alias);
  }

#if BUILDFLAG(IS_APPLE)
  DCHECK(!CFX_DefaultRenderDevice::UseSkiaRenderer());

  std::unique_ptr<CFX_GlyphBitmap> pGlyphBitmap;
  auto it = m_SizeMap.find(FaceGlyphsKey);
  if (it != m_SizeMap.end()) {
    SizeGlyphCache* pSizeCache = &(it->second);
    auto it2 = pSizeCache->find(glyph_index);
    if (it2 != pSizeCache->end())
      return it2->second.get();

    pGlyphBitmap = RenderGlyph_Nativetext(pFont, glyph_index, matrix,
                                          dest_width, anti_alias);
    if (pGlyphBitmap) {
      CFX_GlyphBitmap* pResult = pGlyphBitmap.get();
      (*pSizeCache)[glyph_index] = std::move(pGlyphBitmap);
      return pResult;
    }
  } else {
    pGlyphBitmap = RenderGlyph_Nativetext(pFont, glyph_index, matrix,
                                          dest_width, anti_alias);
    if (pGlyphBitmap) {
      CFX_GlyphBitmap* pResult = pGlyphBitmap.get();

      SizeGlyphCache cache;
      cache[glyph_index] = std::move(pGlyphBitmap);

      m_SizeMap[FaceGlyphsKey] = std::move(cache);
      return pResult;
    }
  }
  UniqueKeyGen keygen2(pFont, matrix, dest_width, anti_alias,
                       /*bNative=*/false);
  auto FaceGlyphsKey2 = ByteString(ByteStringView(keygen2.span()));
  text_options->native_text = false;
  return LookUpGlyphBitmap(pFont, matrix, FaceGlyphsKey2, glyph_index,
                           bFontStyle, dest_width, anti_alias);
#endif  // BUILDFLAG(IS_APPLE)
}

int CFX_GlyphCache::GetGlyphWidth(const CFX_Font* font,
                                  uint32_t glyph_index,
                                  int dest_width,
                                  int weight) {
  const WidthMapKey key = std::make_tuple(glyph_index, dest_width, weight);
  auto it = m_WidthMap.find(key);
  if (it != m_WidthMap.end()) {
    return it->second;
  }

  m_WidthMap[key] = font->GetGlyphWidthImpl(glyph_index, dest_width, weight);
  return m_WidthMap[key];
}

#if defined(PDF_USE_SKIA)

namespace {
// A singleton SkFontMgr which can be used to decode raw font data or
// otherwise get access to system fonts.
SkFontMgr* g_fontmgr = nullptr;
}  // namespace

// static
void CFX_GlyphCache::InitializeGlobals() {
  CHECK(!g_fontmgr);
#if BUILDFLAG(IS_WIN)
  g_fontmgr = SkFontMgr_New_DirectWrite().release();
#elif BUILDFLAG(IS_APPLE)
  g_fontmgr = SkFontMgr_New_CoreText(nullptr).release();
#else
  // This is a SkFontMgr which will use FreeType to decode font data.
  g_fontmgr = SkFontMgr_New_Custom_Empty().release();
#endif
}

// static
void CFX_GlyphCache::DestroyGlobals() {
  CHECK(g_fontmgr);
  delete g_fontmgr;
  g_fontmgr = nullptr;
}

CFX_TypeFace* CFX_GlyphCache::GetDeviceCache(const CFX_Font* pFont) {
  if (!m_pTypeface && g_fontmgr) {
    pdfium::span<const uint8_t> span = pFont->GetFontSpan();
    m_pTypeface = g_fontmgr->makeFromStream(
        std::make_unique<SkMemoryStream>(span.data(), span.size()));
  }
#if BUILDFLAG(IS_WIN) || BUILDFLAG(IS_APPLE)
  // If DirectWrite or CoreText didn't work, try FreeType.
  if (!m_pTypeface) {
    sk_sp<SkFontMgr> freetype_mgr = SkFontMgr_New_Custom_Empty();
    pdfium::span<const uint8_t> span = pFont->GetFontSpan();
    m_pTypeface = freetype_mgr->makeFromStream(
        std::make_unique<SkMemoryStream>(span.data(), span.size()));
  }
#endif  // BUILDFLAG(IS_WIN) || BUILDFLAG(IS_APPLE)
  return m_pTypeface.get();
}
#endif  // defined(PDF_USE_SKIA)

CFX_GlyphBitmap* CFX_GlyphCache::LookUpGlyphBitmap(
    const CFX_Font* pFont,
    const CFX_Matrix& matrix,
    const ByteString& FaceGlyphsKey,
    uint32_t glyph_index,
    bool bFontStyle,
    int dest_width,
    int anti_alias) {
  SizeGlyphCache* pSizeCache;
  auto it = m_SizeMap.find(FaceGlyphsKey);
  if (it == m_SizeMap.end()) {
    m_SizeMap[FaceGlyphsKey] = SizeGlyphCache();
    pSizeCache = &(m_SizeMap[FaceGlyphsKey]);
  } else {
    pSizeCache = &(it->second);
  }

  auto it2 = pSizeCache->find(glyph_index);
  if (it2 != pSizeCache->end())
    return it2->second.get();

  std::unique_ptr<CFX_GlyphBitmap> pGlyphBitmap = RenderGlyph(
      pFont, glyph_index, bFontStyle, matrix, dest_width, anti_alias);
  CFX_GlyphBitmap* pResult = pGlyphBitmap.get();
  (*pSizeCache)[glyph_index] = std::move(pGlyphBitmap);
  return pResult;
}
