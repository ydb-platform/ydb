// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFDOC_CPDF_APSETTINGS_H_
#define CORE_FPDFDOC_CPDF_APSETTINGS_H_

#include "core/fpdfdoc/cpdf_iconfit.h"
#include "core/fxcrt/fx_string.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxge/cfx_color.h"
#include "core/fxge/dib/fx_dib.h"

class CPDF_Dictionary;
class CPDF_Stream;

// Corresponds to PDF spec section 12.5.6.19 (Widget annotation TP dictionary).
#define TEXTPOS_CAPTION 0
#define TEXTPOS_ICON 1
#define TEXTPOS_BELOW 2
#define TEXTPOS_ABOVE 3
#define TEXTPOS_RIGHT 4
#define TEXTPOS_LEFT 5
#define TEXTPOS_OVERLAID 6

class CPDF_ApSettings {
 public:
  explicit CPDF_ApSettings(RetainPtr<CPDF_Dictionary> pDict);
  CPDF_ApSettings(const CPDF_ApSettings& that);
  ~CPDF_ApSettings();

  bool HasMKEntry(const ByteString& csEntry) const;
  int GetRotation() const;

  CPDF_IconFit GetIconFit() const;

  // Returns one of the TEXTPOS_* values above.
  int GetTextPosition() const;

  CFX_Color::TypeAndARGB GetColorARGB(const ByteString& csEntry) const;

  float GetOriginalColorComponent(int index, const ByteString& csEntry) const;
  CFX_Color GetOriginalColor(const ByteString& csEntry) const;

  WideString GetCaption(const ByteString& csEntry) const;
  RetainPtr<CPDF_Stream> GetIcon(const ByteString& csEntry) const;

 private:
  RetainPtr<CPDF_Dictionary> const m_pDict;
};

#endif  // CORE_FPDFDOC_CPDF_APSETTINGS_H_
