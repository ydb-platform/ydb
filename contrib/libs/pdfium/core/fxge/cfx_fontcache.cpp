// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxge/cfx_fontcache.h"

#include "core/fxge/cfx_font.h"
#include "core/fxge/cfx_glyphcache.h"
#include "core/fxge/fx_font.h"

CFX_FontCache::CFX_FontCache() = default;

CFX_FontCache::~CFX_FontCache() = default;

RetainPtr<CFX_GlyphCache> CFX_FontCache::GetGlyphCache(const CFX_Font* pFont) {
  RetainPtr<CFX_Face> face = pFont->GetFace();
  const bool bExternal = !face;
  auto& map = bExternal ? m_ExtGlyphCacheMap : m_GlyphCacheMap;
  auto it = map.find(face.Get());
  if (it != map.end() && it->second)
    return pdfium::WrapRetain(it->second.Get());

  auto new_cache = pdfium::MakeRetain<CFX_GlyphCache>(face);
  map[face.Get()].Reset(new_cache.Get());
  return new_cache;
}

#if defined(PDF_USE_SKIA)
CFX_TypeFace* CFX_FontCache::GetDeviceCache(const CFX_Font* pFont) {
  return GetGlyphCache(pFont)->GetDeviceCache(pFont);
}
#endif
