// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_FONT_CPDF_TYPE3CHAR_H_
#define CORE_FPDFAPI_FONT_CPDF_TYPE3CHAR_H_

#include <memory>
#include <optional>
#include <utility>

#include "core/fpdfapi/font/cpdf_font.h"
#include "core/fxcrt/fx_coordinates.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/span.h"

class CFX_DIBitmap;

class CPDF_Type3Char {
 public:
  CPDF_Type3Char();
  ~CPDF_Type3Char();

  static float TextUnitToGlyphUnit(float fTextUnit);
  static void TextUnitRectToGlyphUnitRect(CFX_FloatRect* pRect);

  bool LoadBitmapFromSoleImageOfForm();
  void InitializeFromStreamData(bool bColored, pdfium::span<const float> pData);
  void Transform(CPDF_Font::FormIface* pForm, const CFX_Matrix& matrix);
  void WillBeDestroyed();

  RetainPtr<CFX_DIBitmap> GetBitmap();

  bool colored() const { return m_bColored; }
  int width() const { return m_Width; }
  const CFX_Matrix& matrix() const { return m_ImageMatrix; }
  const FX_RECT& bbox() const { return m_BBox; }

  const CPDF_Font::FormIface* form() const { return m_pForm.get(); }
  void SetForm(std::unique_ptr<CPDF_Font::FormIface> pForm);

 private:
  std::unique_ptr<CPDF_Font::FormIface> m_pForm;
  RetainPtr<CFX_DIBitmap> m_pBitmap;
  bool m_bColored = false;
  int m_Width = 0;
  CFX_Matrix m_ImageMatrix;
  FX_RECT m_BBox;
};

#endif  // CORE_FPDFAPI_FONT_CPDF_TYPE3CHAR_H_
