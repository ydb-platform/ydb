// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/page/cpdf_textobject.h"

#include <algorithm>

#include "core/fpdfapi/font/cpdf_cidfont.h"
#include "core/fpdfapi/font/cpdf_font.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/fx_coordinates.h"
#include "core/fxcrt/span.h"
#include "core/fxcrt/span_util.h"

#define ISLATINWORD(u) (u != 0x20 && u <= 0x28FF)

namespace {

bool IsVertWritingCIDFont(const CPDF_CIDFont* font) {
  return font && font->IsVertWriting();
}

}  // namespace

CPDF_TextObject::Item::Item() = default;

CPDF_TextObject::Item::Item(const Item& that) = default;

CPDF_TextObject::Item::~Item() = default;

CPDF_TextObject::CPDF_TextObject(int32_t content_stream)
    : CPDF_PageObject(content_stream) {}

CPDF_TextObject::CPDF_TextObject() : CPDF_TextObject(kNoContentStream) {}

CPDF_TextObject::~CPDF_TextObject() = default;

size_t CPDF_TextObject::CountItems() const {
  return m_CharCodes.size();
}

CPDF_TextObject::Item CPDF_TextObject::GetItemInfo(size_t index) const {
  DCHECK(index < m_CharCodes.size());

  Item info;
  info.m_CharCode = m_CharCodes[index];
  info.m_Origin = CFX_PointF(index > 0 ? m_CharPos[index - 1] : 0, 0);
  if (info.m_CharCode == CPDF_Font::kInvalidCharCode)
    return info;

  RetainPtr<CPDF_Font> pFont = GetFont();
  const CPDF_CIDFont* pCIDFont = pFont->AsCIDFont();
  if (!IsVertWritingCIDFont(pCIDFont))
    return info;

  uint16_t cid = pCIDFont->CIDFromCharCode(info.m_CharCode);
  info.m_Origin = CFX_PointF(0, info.m_Origin.x);

  CFX_Point16 vertical_origin = pCIDFont->GetVertOrigin(cid);
  float fontsize = GetFontSize();
  info.m_Origin.x -= fontsize * vertical_origin.x / 1000;
  info.m_Origin.y -= fontsize * vertical_origin.y / 1000;
  return info;
}

size_t CPDF_TextObject::CountChars() const {
  size_t count = 0;
  for (uint32_t charcode : m_CharCodes) {
    if (charcode != CPDF_Font::kInvalidCharCode)
      ++count;
  }
  return count;
}

uint32_t CPDF_TextObject::GetCharCode(size_t index) const {
  size_t count = 0;
  for (uint32_t code : m_CharCodes) {
    if (code == CPDF_Font::kInvalidCharCode)
      continue;
    if (count++ != index)
      continue;
    return code;
  }
  return CPDF_Font::kInvalidCharCode;
}

CPDF_TextObject::Item CPDF_TextObject::GetCharInfo(size_t index) const {
  size_t count = 0;
  for (size_t i = 0; i < m_CharCodes.size(); ++i) {
    uint32_t charcode = m_CharCodes[i];
    if (charcode == CPDF_Font::kInvalidCharCode)
      continue;
    if (count++ == index)
      return GetItemInfo(i);
  }
  return Item();
}

int CPDF_TextObject::CountWords() const {
  RetainPtr<CPDF_Font> pFont = GetFont();
  bool bInLatinWord = false;
  int nWords = 0;
  for (size_t i = 0, sz = CountChars(); i < sz; ++i) {
    uint32_t charcode = GetCharCode(i);

    WideString swUnicode = pFont->UnicodeFromCharCode(charcode);
    uint16_t unicode = 0;
    if (swUnicode.GetLength() > 0)
      unicode = swUnicode[0];

    bool bIsLatin = ISLATINWORD(unicode);
    if (bIsLatin && bInLatinWord)
      continue;

    bInLatinWord = bIsLatin;
    if (unicode != 0x20)
      nWords++;
  }

  return nWords;
}

WideString CPDF_TextObject::GetWordString(int nWordIndex) const {
  RetainPtr<CPDF_Font> pFont = GetFont();
  WideString swRet;
  int nWords = 0;
  bool bInLatinWord = false;
  for (size_t i = 0, sz = CountChars(); i < sz; ++i) {
    uint32_t charcode = GetCharCode(i);

    WideString swUnicode = pFont->UnicodeFromCharCode(charcode);
    uint16_t unicode = 0;
    if (swUnicode.GetLength() > 0)
      unicode = swUnicode[0];

    bool bIsLatin = ISLATINWORD(unicode);
    if (!bIsLatin || !bInLatinWord) {
      bInLatinWord = bIsLatin;
      if (unicode != 0x20)
        nWords++;
    }
    if (nWords - 1 == nWordIndex)
      swRet += unicode;
  }
  return swRet;
}

std::unique_ptr<CPDF_TextObject> CPDF_TextObject::Clone() const {
  auto obj = std::make_unique<CPDF_TextObject>();
  obj->CopyData(this);
  obj->m_CharCodes = m_CharCodes;
  obj->m_CharPos = m_CharPos;
  obj->m_Pos = m_Pos;
  return obj;
}

CPDF_PageObject::Type CPDF_TextObject::GetType() const {
  return Type::kText;
}

void CPDF_TextObject::Transform(const CFX_Matrix& matrix) {
  SetTextMatrix(GetTextMatrix() * matrix);
  SetDirty(true);
}

bool CPDF_TextObject::IsText() const {
  return true;
}

CPDF_TextObject* CPDF_TextObject::AsText() {
  return this;
}

const CPDF_TextObject* CPDF_TextObject::AsText() const {
  return this;
}

CFX_Matrix CPDF_TextObject::GetTextMatrix() const {
  pdfium::span<const float> pTextMatrix = text_state().GetMatrix();
  return CFX_Matrix(pTextMatrix[0], pTextMatrix[2], pTextMatrix[1],
                    pTextMatrix[3], m_Pos.x, m_Pos.y);
}

void CPDF_TextObject::SetTextMatrix(const CFX_Matrix& matrix) {
  pdfium::span<float> pTextMatrix = mutable_text_state().GetMutableMatrix();
  pTextMatrix[0] = matrix.a;
  pTextMatrix[1] = matrix.c;
  pTextMatrix[2] = matrix.b;
  pTextMatrix[3] = matrix.d;
  m_Pos = CFX_PointF(matrix.e, matrix.f);
  CalcPositionDataInternal(GetFont());
}

void CPDF_TextObject::SetSegments(pdfium::span<const ByteString> strings,
                                  pdfium::span<const float> kernings) {
  size_t nSegs = strings.size();
  CHECK(nSegs);
  m_CharCodes.clear();
  m_CharPos.clear();
  RetainPtr<CPDF_Font> pFont = GetFont();
  size_t nChars = nSegs - 1;
  for (const auto& str : strings) {
    nChars += pFont->CountChar(str.AsStringView());
  }
  CHECK(nChars);
  m_CharCodes.resize(nChars);
  m_CharPos.resize(nChars - 1);
  size_t index = 0;
  for (size_t i = 0; i < nSegs; ++i) {
    ByteStringView segment = strings[i].AsStringView();
    size_t offset = 0;
    while (offset < segment.GetLength()) {
      DCHECK(index < m_CharCodes.size());
      m_CharCodes[index++] = pFont->GetNextChar(segment, &offset);
    }
    if (i != nSegs - 1) {
      m_CharPos[index - 1] = kernings[i];
      m_CharCodes[index++] = CPDF_Font::kInvalidCharCode;
    }
  }
}

void CPDF_TextObject::SetText(const ByteString& str) {
  SetSegments(pdfium::span_from_ref(str), pdfium::span<float>());
  CalcPositionDataInternal(GetFont());
  SetDirty(true);
}

float CPDF_TextObject::GetCharWidth(uint32_t charcode) const {
  const float fontsize = GetFontSize() / 1000;
  RetainPtr<CPDF_Font> pFont = GetFont();
  const CPDF_CIDFont* pCIDFont = pFont->AsCIDFont();
  if (!IsVertWritingCIDFont(pCIDFont))
    return pFont->GetCharWidthF(charcode) * fontsize;

  uint16_t cid = pCIDFont->CIDFromCharCode(charcode);
  return pCIDFont->GetVertWidth(cid) * fontsize;
}

RetainPtr<CPDF_Font> CPDF_TextObject::GetFont() const {
  return text_state().GetFont();
}

float CPDF_TextObject::GetFontSize() const {
  return text_state().GetFontSize();
}

TextRenderingMode CPDF_TextObject::GetTextRenderMode() const {
  return text_state().GetTextMode();
}

void CPDF_TextObject::SetTextRenderMode(TextRenderingMode mode) {
  mutable_text_state().SetTextMode(mode);
  SetDirty(true);
}

CFX_PointF CPDF_TextObject::CalcPositionData(float horz_scale) {
  RetainPtr<CPDF_Font> pFont = GetFont();
  const float curpos = CalcPositionDataInternal(pFont);
  if (IsVertWritingCIDFont(pFont->AsCIDFont()))
    return {0, curpos};
  return {curpos * horz_scale, 0};
}

float CPDF_TextObject::CalcPositionDataInternal(
    const RetainPtr<CPDF_Font>& pFont) {
  float curpos = 0;
  float min_x = 10000.0f;
  float max_x = -10000.0f;
  float min_y = 10000.0f;
  float max_y = -10000.0f;
  const CPDF_CIDFont* pCIDFont = pFont->AsCIDFont();
  const bool bVertWriting = IsVertWritingCIDFont(pCIDFont);
  const float fontsize = GetFontSize();

  for (size_t i = 0; i < m_CharCodes.size(); ++i) {
    const uint32_t charcode = m_CharCodes[i];
    if (i > 0) {
      if (charcode == CPDF_Font::kInvalidCharCode) {
        curpos -= (m_CharPos[i - 1] * fontsize) / 1000;
        continue;
      }
      m_CharPos[i - 1] = curpos;
    }

    FX_RECT char_rect = pFont->GetCharBBox(charcode);
    float charwidth;
    if (bVertWriting) {
      uint16_t cid = pCIDFont->CIDFromCharCode(charcode);
      CFX_Point16 vertical_origin = pCIDFont->GetVertOrigin(cid);
      char_rect.Offset(-vertical_origin.x, -vertical_origin.y);
      min_x = std::min({min_x, static_cast<float>(char_rect.left),
                        static_cast<float>(char_rect.right)});
      max_x = std::max({max_x, static_cast<float>(char_rect.left),
                        static_cast<float>(char_rect.right)});
      const float char_top = curpos + char_rect.top * fontsize / 1000;
      const float char_bottom = curpos + char_rect.bottom * fontsize / 1000;
      min_y = std::min({min_y, char_top, char_bottom});
      max_y = std::max({max_y, char_top, char_bottom});
      charwidth = pCIDFont->GetVertWidth(cid) * fontsize / 1000;
    } else {
      min_y = std::min({min_y, static_cast<float>(char_rect.top),
                        static_cast<float>(char_rect.bottom)});
      max_y = std::max({max_y, static_cast<float>(char_rect.top),
                        static_cast<float>(char_rect.bottom)});
      const float char_left = curpos + char_rect.left * fontsize / 1000;
      const float char_right = curpos + char_rect.right * fontsize / 1000;
      min_x = std::min({min_x, char_left, char_right});
      max_x = std::max({max_x, char_left, char_right});
      charwidth = pFont->GetCharWidthF(charcode) * fontsize / 1000;
    }
    curpos += charwidth;
    if (charcode == ' ' && (!pCIDFont || pCIDFont->GetCharSize(' ') == 1))
      curpos += text_state().GetWordSpace();

    curpos += text_state().GetCharSpace();
  }

  if (bVertWriting) {
    min_x = min_x * fontsize / 1000;
    max_x = max_x * fontsize / 1000;
  } else {
    min_y = min_y * fontsize / 1000;
    max_y = max_y * fontsize / 1000;
  }

  SetOriginalRect(CFX_FloatRect(min_x, min_y, max_x, max_y));
  CFX_FloatRect rect = GetTextMatrix().TransformRect(GetOriginalRect());
  if (TextRenderingModeIsStrokeMode(text_state().GetTextMode())) {
    // TODO(crbug.com/pdfium/1840): Does the original rect need a similar
    // adjustment?
    const float half_width = graph_state().GetLineWidth() / 2;
    rect.Inflate(half_width, half_width);
  }
  SetRect(rect);

  return curpos;
}
