// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_RENDER_CPDF_TEXTRENDERER_H_
#define CORE_FPDFAPI_RENDER_CPDF_TEXTRENDERER_H_

#include <stdint.h>

#include "core/fxcrt/bytestring.h"
#include "core/fxcrt/fx_coordinates.h"
#include "core/fxcrt/span.h"
#include "core/fxge/dib/fx_dib.h"

class CFX_RenderDevice;
class CFX_GraphStateData;
class CFX_Path;
class CPDF_RenderOptions;
class CPDF_Font;
struct CFX_FillRenderOptions;

class CPDF_TextRenderer {
 public:
  static void DrawTextString(CFX_RenderDevice* pDevice,
                             float origin_x,
                             float origin_y,
                             CPDF_Font* pFont,
                             float font_size,
                             const CFX_Matrix& matrix,
                             const ByteString& str,
                             FX_ARGB fill_argb,
                             const CPDF_RenderOptions& options);

  static bool DrawTextPath(CFX_RenderDevice* pDevice,
                           pdfium::span<const uint32_t> char_codes,
                           pdfium::span<const float> char_pos,
                           CPDF_Font* pFont,
                           float font_size,
                           const CFX_Matrix& mtText2User,
                           const CFX_Matrix* pUser2Device,
                           const CFX_GraphStateData* pGraphState,
                           FX_ARGB fill_argb,
                           FX_ARGB stroke_argb,
                           CFX_Path* pClippingPath,
                           const CFX_FillRenderOptions& fill_options);

  static bool DrawNormalText(CFX_RenderDevice* pDevice,
                             pdfium::span<const uint32_t> char_codes,
                             pdfium::span<const float> char_pos,
                             CPDF_Font* pFont,
                             float font_size,
                             const CFX_Matrix& mtText2Device,
                             FX_ARGB fill_argb,
                             const CPDF_RenderOptions& options);

  CPDF_TextRenderer() = delete;
  CPDF_TextRenderer(const CPDF_TextRenderer&) = delete;
  CPDF_TextRenderer& operator=(const CPDF_TextRenderer&) = delete;
};

#endif  // CORE_FPDFAPI_RENDER_CPDF_TEXTRENDERER_H_
