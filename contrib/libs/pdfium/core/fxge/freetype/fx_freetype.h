// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXGE_FREETYPE_FX_FREETYPE_H_
#define CORE_FXGE_FREETYPE_FX_FREETYPE_H_

#include <ft2build.h>

#include <memory>

#include "core/fxcrt/span.h"

#include FT_FREETYPE_H
#include FT_GLYPH_H
#include FT_LCD_FILTER_H
#include FT_MULTIPLE_MASTERS_H
#include FT_OUTLINE_H
#include FT_TRUETYPE_TABLES_H

using FXFT_LibraryRec = struct FT_LibraryRec_;
using FXFT_FaceRec = struct FT_FaceRec_;
using FXFT_StreamRec = struct FT_StreamRec_;

struct FXFTFaceRecDeleter {
  inline void operator()(FXFT_FaceRec* pRec) { FT_Done_Face(pRec); }
};

struct FXFTLibraryRecDeleter {
  inline void operator()(FXFT_LibraryRec* pRec) { FT_Done_FreeType(pRec); }
};

struct FXFTMMVarDeleter {
  void operator()(FT_MM_Var* variation_desc);
};

using ScopedFXFTFaceRec = std::unique_ptr<FXFT_FaceRec, FXFTFaceRecDeleter>;
using ScopedFXFTLibraryRec =
    std::unique_ptr<FXFT_LibraryRec, FXFTLibraryRecDeleter>;

class ScopedFXFTMMVar {
 public:
  explicit ScopedFXFTMMVar(FXFT_FaceRec* face);
  ~ScopedFXFTMMVar();

  explicit operator bool() const { return !!variation_desc_; }

  FT_Pos GetAxisDefault(size_t index) const;
  FT_Long GetAxisMin(size_t index) const;
  FT_Long GetAxisMax(size_t index) const;

 private:
  std::unique_ptr<FT_MM_Var, FXFTMMVarDeleter> const variation_desc_;
  // Points into `variation_desc_`.
  const pdfium::span<const FT_Var_Axis> axis_;
};

#define FXFT_Get_Glyph_HoriBearingX(face) (face)->glyph->metrics.horiBearingX
#define FXFT_Get_Glyph_HoriBearingY(face) (face)->glyph->metrics.horiBearingY
#define FXFT_Get_Glyph_Width(face) (face)->glyph->metrics.width
#define FXFT_Get_Glyph_Height(face) (face)->glyph->metrics.height
#define FXFT_Get_Glyph_HoriAdvance(face) (face)->glyph->metrics.horiAdvance

int FXFT_unicode_from_adobe_name(const char* glyph_name);
void FXFT_adobe_name_from_unicode(pdfium::span<char> name_buf, wchar_t unicode);

#endif  // CORE_FXGE_FREETYPE_FX_FREETYPE_H_
