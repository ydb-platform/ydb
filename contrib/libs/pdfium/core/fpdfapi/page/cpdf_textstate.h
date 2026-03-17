// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PAGE_CPDF_TEXTSTATE_H_
#define CORE_FPDFAPI_PAGE_CPDF_TEXTSTATE_H_

#include <array>

#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/shared_copy_on_write.h"
#include "core/fxcrt/span.h"
#include "core/fxcrt/unowned_ptr.h"

class CPDF_Document;
class CPDF_Font;

// See PDF Reference 1.7, page 402, table 5.3.
enum class TextRenderingMode {
  MODE_UNKNOWN = -1,
  MODE_FILL = 0,
  MODE_STROKE = 1,
  MODE_FILL_STROKE = 2,
  MODE_INVISIBLE = 3,
  MODE_FILL_CLIP = 4,
  MODE_STROKE_CLIP = 5,
  MODE_FILL_STROKE_CLIP = 6,
  MODE_CLIP = 7,
  MODE_LAST = MODE_CLIP,
};

class CPDF_TextState {
 public:
  CPDF_TextState();
  CPDF_TextState(const CPDF_TextState& that);
  CPDF_TextState& operator=(const CPDF_TextState& that);
  ~CPDF_TextState();

  void Emplace();

  RetainPtr<CPDF_Font> GetFont() const;
  void SetFont(RetainPtr<CPDF_Font> pFont);

  float GetFontSize() const;
  void SetFontSize(float size);

  pdfium::span<const float> GetMatrix() const;
  pdfium::span<float> GetMutableMatrix();

  float GetCharSpace() const;
  void SetCharSpace(float sp);

  float GetWordSpace() const;
  void SetWordSpace(float sp);

  float GetFontSizeH() const;

  TextRenderingMode GetTextMode() const;
  void SetTextMode(TextRenderingMode mode);

  pdfium::span<const float> GetCTM() const;
  pdfium::span<float> GetMutableCTM();

 private:
  class TextData final : public Retainable {
   public:
    CONSTRUCT_VIA_MAKE_RETAIN;

    RetainPtr<TextData> Clone() const;

    void SetFont(RetainPtr<CPDF_Font> pFont);
    float GetFontSizeV() const;
    float GetFontSizeH() const;

    RetainPtr<CPDF_Font> font_;
    UnownedPtr<const CPDF_Document> document_;
    float font_size_ = 1.0f;
    float char_space_ = 0.0f;
    float word_space_ = 0.0f;
    TextRenderingMode text_rendering_mode_ = TextRenderingMode::MODE_FILL;
    std::array<float, 4> matrix_ = {1.0f, 0.0f, 0.0f, 1.0f};
    std::array<float, 4> ctm_ = {1.0f, 0.0f, 0.0f, 1.0f};

   private:
    TextData();
    TextData(const TextData& that);
    ~TextData() override;
  };

  SharedCopyOnWrite<TextData> m_Ref;
};

bool SetTextRenderingModeFromInt(int iMode, TextRenderingMode* mode);
bool TextRenderingModeIsClipMode(const TextRenderingMode& mode);
bool TextRenderingModeIsStrokeMode(const TextRenderingMode& mode);

#endif  // CORE_FPDFAPI_PAGE_CPDF_TEXTSTATE_H_
