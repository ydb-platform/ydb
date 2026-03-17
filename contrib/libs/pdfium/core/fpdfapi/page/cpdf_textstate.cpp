// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/page/cpdf_textstate.h"

#include <math.h>

#include <utility>

#include "core/fpdfapi/font/cpdf_font.h"
#include "core/fpdfapi/page/cpdf_docpagedata.h"

CPDF_TextState::CPDF_TextState() = default;

CPDF_TextState::CPDF_TextState(const CPDF_TextState&) = default;

CPDF_TextState& CPDF_TextState::operator=(const CPDF_TextState&) = default;

CPDF_TextState::~CPDF_TextState() = default;

void CPDF_TextState::Emplace() {
  m_Ref.Emplace();
}

RetainPtr<CPDF_Font> CPDF_TextState::GetFont() const {
  return m_Ref.GetObject()->font_;
}

void CPDF_TextState::SetFont(RetainPtr<CPDF_Font> pFont) {
  m_Ref.GetPrivateCopy()->SetFont(std::move(pFont));
}

float CPDF_TextState::GetFontSize() const {
  return m_Ref.GetObject()->font_size_;
}

void CPDF_TextState::SetFontSize(float size) {
  if (!m_Ref || GetFontSize() != size) {
    m_Ref.GetPrivateCopy()->font_size_ = size;
  }
}

pdfium::span<const float> CPDF_TextState::GetMatrix() const {
  return m_Ref.GetObject()->matrix_;
}

pdfium::span<float> CPDF_TextState::GetMutableMatrix() {
  return m_Ref.GetPrivateCopy()->matrix_;
}

float CPDF_TextState::GetCharSpace() const {
  return m_Ref.GetObject()->char_space_;
}

void CPDF_TextState::SetCharSpace(float sp) {
  if (!m_Ref || GetCharSpace() != sp) {
    m_Ref.GetPrivateCopy()->char_space_ = sp;
  }
}

float CPDF_TextState::GetWordSpace() const {
  return m_Ref.GetObject()->word_space_;
}

void CPDF_TextState::SetWordSpace(float sp) {
  if (!m_Ref || GetWordSpace() != sp) {
    m_Ref.GetPrivateCopy()->word_space_ = sp;
  }
}

float CPDF_TextState::GetFontSizeH() const {
  return m_Ref.GetObject()->GetFontSizeH();
}

TextRenderingMode CPDF_TextState::GetTextMode() const {
  return m_Ref.GetObject()->text_rendering_mode_;
}

void CPDF_TextState::SetTextMode(TextRenderingMode mode) {
  if (!m_Ref || GetTextMode() != mode) {
    m_Ref.GetPrivateCopy()->text_rendering_mode_ = mode;
  }
}

pdfium::span<const float> CPDF_TextState::GetCTM() const {
  return m_Ref.GetObject()->ctm_;
}

pdfium::span<float> CPDF_TextState::GetMutableCTM() {
  return m_Ref.GetPrivateCopy()->ctm_;
}

CPDF_TextState::TextData::TextData() = default;

CPDF_TextState::TextData::TextData(const TextData& that)
    : font_(that.font_),
      document_(that.document_),
      font_size_(that.font_size_),
      char_space_(that.char_space_),
      word_space_(that.word_space_),
      text_rendering_mode_(that.text_rendering_mode_),
      matrix_(that.matrix_),
      ctm_(that.ctm_) {
  if (document_ && font_) {
    auto* page_data = CPDF_DocPageData::FromDocument(document_);
    font_ = page_data->GetFont(font_->GetMutableFontDict());
  }
}

CPDF_TextState::TextData::~TextData() = default;

RetainPtr<CPDF_TextState::TextData> CPDF_TextState::TextData::Clone() const {
  return pdfium::MakeRetain<CPDF_TextState::TextData>(*this);
}

void CPDF_TextState::TextData::SetFont(RetainPtr<CPDF_Font> pFont) {
  document_ = pFont ? pFont->GetDocument() : nullptr;
  font_ = std::move(pFont);
}

float CPDF_TextState::TextData::GetFontSizeH() const {
  return fabs(hypotf(matrix_[0], matrix_[2]) * font_size_);
}

bool SetTextRenderingModeFromInt(int iMode, TextRenderingMode* mode) {
  if (iMode < 0 || iMode > 7)
    return false;
  *mode = static_cast<TextRenderingMode>(iMode);
  return true;
}

bool TextRenderingModeIsClipMode(const TextRenderingMode& mode) {
  switch (mode) {
    case TextRenderingMode::MODE_FILL_CLIP:
    case TextRenderingMode::MODE_STROKE_CLIP:
    case TextRenderingMode::MODE_FILL_STROKE_CLIP:
    case TextRenderingMode::MODE_CLIP:
      return true;
    default:
      return false;
  }
}

bool TextRenderingModeIsStrokeMode(const TextRenderingMode& mode) {
  switch (mode) {
    case TextRenderingMode::MODE_STROKE:
    case TextRenderingMode::MODE_FILL_STROKE:
    case TextRenderingMode::MODE_STROKE_CLIP:
    case TextRenderingMode::MODE_FILL_STROKE_CLIP:
      return true;
    default:
      return false;
  }
}
