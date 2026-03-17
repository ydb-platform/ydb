// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXGE_CFX_SUBSTFONT_H_
#define CORE_FXGE_CFX_SUBSTFONT_H_

#include "core/fxcrt/bytestring.h"
#include "core/fxcrt/fx_codepage.h"

class CFX_SubstFont {
 public:
  CFX_SubstFont();
  ~CFX_SubstFont();

#if defined(PDF_USE_SKIA)
  int GetOriginalWeight() const;
#endif
  void UseChromeSerif();

  void SetIsBuiltInGenericFont() { m_bFlagMM = true; }
  bool IsBuiltInGenericFont() const { return m_bFlagMM; }

  ByteString m_Family;
  FX_Charset m_Charset = FX_Charset::kANSI;
  int m_Weight = 0;
  int m_ItalicAngle = 0;
  int m_WeightCJK = 0;

  bool m_bSubstCJK = false;
  bool m_bItalicCJK = false;

 private:
  bool m_bFlagMM = false;
};

#endif  // CORE_FXGE_CFX_SUBSTFONT_H_
