// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfdoc/cpdf_apsettings.h"

#include <algorithm>
#include <utility>

#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fpdfdoc/cpdf_formcontrol.h"

CPDF_ApSettings::CPDF_ApSettings(RetainPtr<CPDF_Dictionary> pDict)
    : m_pDict(std::move(pDict)) {}

CPDF_ApSettings::CPDF_ApSettings(const CPDF_ApSettings& that) = default;

CPDF_ApSettings::~CPDF_ApSettings() = default;

bool CPDF_ApSettings::HasMKEntry(const ByteString& csEntry) const {
  return m_pDict && m_pDict->KeyExist(csEntry);
}

int CPDF_ApSettings::GetRotation() const {
  return m_pDict ? m_pDict->GetIntegerFor("R") : 0;
}

CFX_Color::TypeAndARGB CPDF_ApSettings::GetColorARGB(
    const ByteString& csEntry) const {
  if (!m_pDict)
    return {CFX_Color::Type::kTransparent, 0};

  RetainPtr<const CPDF_Array> pEntry = m_pDict->GetArrayFor(csEntry);
  if (!pEntry)
    return {CFX_Color::Type::kTransparent, 0};

  const size_t dwCount = pEntry->size();
  if (dwCount == 1) {
    const float g = pEntry->GetFloatAt(0) * 255;
    return {CFX_Color::Type::kGray, ArgbEncode(255, (int)g, (int)g, (int)g)};
  }
  if (dwCount == 3) {
    float r = pEntry->GetFloatAt(0) * 255;
    float g = pEntry->GetFloatAt(1) * 255;
    float b = pEntry->GetFloatAt(2) * 255;
    return {CFX_Color::Type::kRGB, ArgbEncode(255, (int)r, (int)g, (int)b)};
  }
  if (dwCount == 4) {
    float c = pEntry->GetFloatAt(0);
    float m = pEntry->GetFloatAt(1);
    float y = pEntry->GetFloatAt(2);
    float k = pEntry->GetFloatAt(3);
    float r = (1.0f - std::min(1.0f, c + k)) * 255;
    float g = (1.0f - std::min(1.0f, m + k)) * 255;
    float b = (1.0f - std::min(1.0f, y + k)) * 255;
    return {CFX_Color::Type::kCMYK, ArgbEncode(255, (int)r, (int)g, (int)b)};
  }
  return {CFX_Color::Type::kTransparent, 0};
}

float CPDF_ApSettings::GetOriginalColorComponent(
    int index,
    const ByteString& csEntry) const {
  if (!m_pDict)
    return 0;

  RetainPtr<const CPDF_Array> pEntry = m_pDict->GetArrayFor(csEntry);
  return pEntry ? pEntry->GetFloatAt(index) : 0;
}

CFX_Color CPDF_ApSettings::GetOriginalColor(const ByteString& csEntry) const {
  if (!m_pDict)
    return CFX_Color();

  RetainPtr<const CPDF_Array> pEntry = m_pDict->GetArrayFor(csEntry);
  if (!pEntry)
    return CFX_Color();

  size_t dwCount = pEntry->size();
  if (dwCount == 1) {
    return CFX_Color(CFX_Color::Type::kGray, pEntry->GetFloatAt(0));
  }
  if (dwCount == 3) {
    return CFX_Color(CFX_Color::Type::kRGB, pEntry->GetFloatAt(0),
                     pEntry->GetFloatAt(1), pEntry->GetFloatAt(2));
  }
  if (dwCount == 4) {
    return CFX_Color(CFX_Color::Type::kCMYK, pEntry->GetFloatAt(0),
                     pEntry->GetFloatAt(1), pEntry->GetFloatAt(2),
                     pEntry->GetFloatAt(3));
  }
  return CFX_Color();
}

WideString CPDF_ApSettings::GetCaption(const ByteString& csEntry) const {
  return m_pDict ? m_pDict->GetUnicodeTextFor(csEntry) : WideString();
}

RetainPtr<CPDF_Stream> CPDF_ApSettings::GetIcon(
    const ByteString& csEntry) const {
  return m_pDict ? m_pDict->GetMutableStreamFor(csEntry) : nullptr;
}

CPDF_IconFit CPDF_ApSettings::GetIconFit() const {
  return CPDF_IconFit(m_pDict ? m_pDict->GetDictFor("IF") : nullptr);
}

int CPDF_ApSettings::GetTextPosition() const {
  return m_pDict ? m_pDict->GetIntegerFor("TP", TEXTPOS_CAPTION)
                 : TEXTPOS_CAPTION;
}
