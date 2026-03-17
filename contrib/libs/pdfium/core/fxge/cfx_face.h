// Copyright 2019 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FXGE_CFX_FACE_H_
#define CORE_FXGE_CFX_FACE_H_

#include <stdint.h>

#include <array>
#include <memory>
#include <optional>
#include <vector>

#include "build/build_config.h"
#include "core/fxcrt/bytestring.h"
#include "core/fxcrt/fx_coordinates.h"
#include "core/fxcrt/observed_ptr.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/span.h"
#include "core/fxge/freetype/fx_freetype.h"

namespace fxge {
enum class FontEncoding : uint32_t;
}

class CFX_Font;
class CFX_GlyphBitmap;
class CFX_Path;
class CFX_SubstFont;

class CFX_Face final : public Retainable, public Observable {
 public:
  using CharMap = void*;

  struct CharCodeAndIndex {
    uint32_t char_code;
    uint32_t glyph_index;
  };

  static RetainPtr<CFX_Face> New(FT_Library library,
                                 RetainPtr<Retainable> pDesc,
                                 pdfium::span<const FT_Byte> data,
                                 FT_Long face_index);

  static RetainPtr<CFX_Face> Open(FT_Library library,
                                  const FT_Open_Args* args,
                                  FT_Long face_index);

  bool HasGlyphNames() const;
  bool IsTtOt() const;
  bool IsTricky() const;
  bool IsFixedWidth() const;

#if defined(PDF_ENABLE_XFA)
  bool IsScalable() const;
  void ClearExternalStream();
#endif

  bool IsItalic() const;
  bool IsBold() const;

  ByteString GetFamilyName() const;
  ByteString GetStyleName() const;

  FX_RECT GetBBox() const;
  uint16_t GetUnitsPerEm() const;
  int16_t GetAscender() const;
  int16_t GetDescender() const;
#if BUILDFLAG(IS_ANDROID)
  int16_t GetHeight() const;
#endif

  pdfium::span<uint8_t> GetData() const;

  // Returns the size of the data, or 0 on failure. Only write into `buffer` if
  // it is large enough to hold the data.
  size_t GetSfntTable(uint32_t table, pdfium::span<uint8_t> buffer);

  std::optional<std::array<uint32_t, 4>> GetOs2UnicodeRange();
  std::optional<std::array<uint32_t, 2>> GetOs2CodePageRange();
  std::optional<std::array<uint8_t, 2>> GetOs2Panose();

  int GetGlyphCount() const;
  // TODO(crbug.com/pdfium/2037): Can this method be private?
  FX_RECT GetGlyphBBox() const;
  std::unique_ptr<CFX_GlyphBitmap> RenderGlyph(const CFX_Font* pFont,
                                               uint32_t glyph_index,
                                               bool bFontStyle,
                                               const CFX_Matrix& matrix,
                                               int dest_width,
                                               int anti_alias);
  std::unique_ptr<CFX_Path> LoadGlyphPath(uint32_t glyph_index,
                                          int dest_width,
                                          bool is_vertical,
                                          const CFX_SubstFont* subst_font);
  int GetGlyphWidth(uint32_t glyph_index,
                    int dest_width,
                    int weight,
                    const CFX_SubstFont* subst_font);
  ByteString GetGlyphName(uint32_t glyph_index);

  int GetCharIndex(uint32_t code);
  int GetNameIndex(const char* name);

  FX_RECT GetCharBBox(uint32_t code, int glyph_index);

  std::vector<CharCodeAndIndex> GetCharCodesAndIndices(char32_t max_char);

  CharMap GetCurrentCharMap() const;
  std::optional<fxge::FontEncoding> GetCurrentCharMapEncoding() const;
  int GetCharMapPlatformIdByIndex(size_t index) const;
  int GetCharMapEncodingIdByIndex(size_t index) const;
  fxge::FontEncoding GetCharMapEncodingByIndex(size_t index) const;
  size_t GetCharMapCount() const;
  void SetCharMap(CharMap map);
  void SetCharMapByIndex(size_t index);
  bool SelectCharMap(fxge::FontEncoding encoding);

  bool SetPixelSize(uint32_t width, uint32_t height);

#if BUILDFLAG(IS_WIN)
  bool CanEmbed();
#endif

  FXFT_FaceRec* GetRec() { return m_pRec.get(); }
  const FXFT_FaceRec* GetRec() const { return m_pRec.get(); }

 private:
  CFX_Face(FXFT_FaceRec* pRec, RetainPtr<Retainable> pDesc);
  ~CFX_Face() override;

  void AdjustVariationParams(int glyph_index, int dest_width, int weight);

  ScopedFXFTFaceRec const m_pRec;
  RetainPtr<Retainable> const m_pDesc;
};

#endif  // CORE_FXGE_CFX_FACE_H_
