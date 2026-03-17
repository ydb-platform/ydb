// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXGE_WIN32_CFX_PSRENDERER_H_
#define CORE_FXGE_WIN32_CFX_PSRENDERER_H_

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <optional>
#include <sstream>
#include <vector>

#include "core/fxcrt/bytestring.h"
#include "core/fxcrt/data_vector.h"
#include "core/fxcrt/fx_coordinates.h"
#include "core/fxcrt/fx_stream.h"
#include "core/fxcrt/fx_string_wrappers.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/span.h"
#include "core/fxcrt/unowned_ptr.h"
#include "core/fxge/cfx_graphstatedata.h"

class CFX_DIBBase;
class CFX_Font;
class CFX_GlyphCache;
class CFX_PSFontTracker;
class CFX_Path;
class TextCharPos;
struct CFX_FillRenderOptions;
struct FXDIB_ResampleOptions;

struct EncoderIface {
  DataVector<uint8_t> (*pA85EncodeFunc)(pdfium::span<const uint8_t> src_span);
  DataVector<uint8_t> (*pFaxEncodeFunc)(RetainPtr<const CFX_DIBBase> src);
  DataVector<uint8_t> (*pFlateEncodeFunc)(pdfium::span<const uint8_t> src_span);
  bool (*pJpegEncodeFunc)(const RetainPtr<const CFX_DIBBase>& pSource,
                          uint8_t** dest_buf,
                          size_t* dest_size);
  DataVector<uint8_t> (*pRunLengthEncodeFunc)(
      pdfium::span<const uint8_t> src_span);
};

class CFX_PSRenderer {
 public:
  enum class RenderingLevel {
    kLevel2,
    kLevel3,
    kLevel3Type42,
  };

  CFX_PSRenderer(CFX_PSFontTracker* font_tracker,
                 const EncoderIface* encoder_iface);
  ~CFX_PSRenderer();

  void Init(const RetainPtr<IFX_RetainableWriteStream>& stream,
            RenderingLevel level,
            int width,
            int height);
  void SaveState();
  void RestoreState(bool bKeepSaved);
  void SetClip_PathFill(const CFX_Path& path,
                        const CFX_Matrix* pObject2Device,
                        const CFX_FillRenderOptions& fill_options);
  void SetClip_PathStroke(const CFX_Path& path,
                          const CFX_Matrix* pObject2Device,
                          const CFX_GraphStateData* pGraphState);
  FX_RECT GetClipBox() const { return m_ClipBox; }
  bool DrawPath(const CFX_Path& path,
                const CFX_Matrix* pObject2Device,
                const CFX_GraphStateData* pGraphState,
                uint32_t fill_color,
                uint32_t stroke_color,
                const CFX_FillRenderOptions& fill_options);
  bool SetDIBits(RetainPtr<const CFX_DIBBase> bitmap,
                 uint32_t color,
                 int dest_left,
                 int dest_top);
  bool StretchDIBits(RetainPtr<const CFX_DIBBase> bitmap,
                     uint32_t color,
                     int dest_left,
                     int dest_top,
                     int dest_width,
                     int dest_height,
                     const FXDIB_ResampleOptions& options);
  bool DrawDIBits(RetainPtr<const CFX_DIBBase> bitmap,
                  uint32_t color,
                  const CFX_Matrix& matrix,
                  const FXDIB_ResampleOptions& options);
  bool DrawText(int nChars,
                const TextCharPos* pCharPos,
                CFX_Font* pFont,
                const CFX_Matrix& mtObject2Device,
                float font_size,
                uint32_t color);

  static std::optional<ByteString> GenerateType42SfntDataForTesting(
      const ByteString& psname,
      pdfium::span<const uint8_t> font_data);

  static ByteString GenerateType42FontDictionaryForTesting(
      const ByteString& psname,
      const FX_RECT& bbox,
      size_t num_glyphs,
      size_t glyphs_per_descendant_font);

 private:
  struct Glyph;

  struct FaxCompressResult {
    FaxCompressResult();
    FaxCompressResult(const FaxCompressResult&) = delete;
    FaxCompressResult& operator=(const FaxCompressResult&) = delete;
    FaxCompressResult(FaxCompressResult&&) noexcept;
    FaxCompressResult& operator=(FaxCompressResult&&) noexcept;
    ~FaxCompressResult();

    DataVector<uint8_t> data;
    bool compressed = false;
  };

  struct PSCompressResult {
    PSCompressResult();
    PSCompressResult(const PSCompressResult&) = delete;
    PSCompressResult& operator=(const PSCompressResult&) = delete;
    PSCompressResult(PSCompressResult&&) noexcept;
    PSCompressResult& operator=(PSCompressResult&&) noexcept;
    ~PSCompressResult();

    DataVector<uint8_t> data;
    ByteString filter;
  };

  void StartRendering();
  void EndRendering();
  void OutputPath(const CFX_Path& path, const CFX_Matrix* pObject2Device);
  void SetGraphState(const CFX_GraphStateData* pGraphState);
  void SetColor(uint32_t color);
  void FindPSFontGlyph(CFX_GlyphCache* pGlyphCache,
                       CFX_Font* pFont,
                       const TextCharPos& charpos,
                       int* ps_fontnum,
                       int* ps_glyphindex);
  void DrawTextAsType3Font(int char_count,
                           const TextCharPos* char_pos,
                           CFX_Font* font,
                           float font_size,
                           fxcrt::ostringstream& buf);
  bool DrawTextAsType42Font(int char_count,
                            const TextCharPos* char_pos,
                            CFX_Font* font,
                            float font_size,
                            fxcrt::ostringstream& buf);
  FaxCompressResult FaxCompressData(RetainPtr<const CFX_DIBBase> src) const;
  std::optional<PSCompressResult> PSCompressData(
      pdfium::span<const uint8_t> src_span) const;
  void WritePreambleString(ByteStringView str);
  void WritePSBinary(pdfium::span<const uint8_t> data);
  void WriteStream(fxcrt::ostringstream& stream);
  void WriteString(ByteStringView str);

  bool m_bInited = false;
  bool m_bGraphStateSet = false;
  bool m_bColorSet = false;
  std::optional<RenderingLevel> m_Level;
  uint32_t m_LastColor = 0;
  FX_RECT m_ClipBox;
  CFX_GraphStateData m_CurGraphState;
  UnownedPtr<CFX_PSFontTracker> const m_pFontTracker;
  UnownedPtr<const EncoderIface> const m_pEncoderIface;
  RetainPtr<IFX_RetainableWriteStream> m_pStream;
  std::vector<std::unique_ptr<Glyph>> m_PSFontList;
  fxcrt::ostringstream m_PreambleOutput;
  fxcrt::ostringstream m_Output;
  std::vector<FX_RECT> m_ClipBoxStack;
};

#endif  // CORE_FXGE_WIN32_CFX_PSRENDERER_H_
