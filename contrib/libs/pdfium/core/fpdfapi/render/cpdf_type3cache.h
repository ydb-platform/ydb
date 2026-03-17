// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_RENDER_CPDF_TYPE3CACHE_H_
#define CORE_FPDFAPI_RENDER_CPDF_TYPE3CACHE_H_

#include <stdint.h>

#include <map>
#include <memory>
#include <tuple>

#include "core/fxcrt/bytestring.h"
#include "core/fxcrt/observed_ptr.h"
#include "core/fxcrt/retain_ptr.h"

class CFX_GlyphBitmap;
class CFX_Matrix;
class CPDF_Type3Font;
class CPDF_Type3GlyphMap;

class CPDF_Type3Cache final : public Retainable, public Observable {
 public:
  CONSTRUCT_VIA_MAKE_RETAIN;

  const CFX_GlyphBitmap* LoadGlyph(uint32_t charcode,
                                   const CFX_Matrix& mtMatrix);

 private:
  using SizeKey = std::tuple<int, int, int, int>;

  explicit CPDF_Type3Cache(CPDF_Type3Font* pFont);
  ~CPDF_Type3Cache() override;

  std::unique_ptr<CFX_GlyphBitmap> RenderGlyph(CPDF_Type3GlyphMap* pSize,
                                               uint32_t charcode,
                                               const CFX_Matrix& mtMatrix);

  RetainPtr<CPDF_Type3Font> const m_pFont;
  std::map<SizeKey, std::unique_ptr<CPDF_Type3GlyphMap>> m_SizeMap;
};

#endif  // CORE_FPDFAPI_RENDER_CPDF_TYPE3CACHE_H_
