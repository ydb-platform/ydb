// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/parser/cpdf_name.h"

#include "core/fpdfapi/parser/fpdf_parser_decode.h"
#include "core/fpdfapi/parser/fpdf_parser_utility.h"
#include "core/fxcrt/fx_stream.h"

CPDF_Name::CPDF_Name(WeakPtr<ByteStringPool> pPool, const ByteString& str)
    : m_Name(str) {
  if (pPool)
    m_Name = pPool->Intern(m_Name);
}

CPDF_Name::~CPDF_Name() = default;

CPDF_Object::Type CPDF_Name::GetType() const {
  return kName;
}

RetainPtr<CPDF_Object> CPDF_Name::Clone() const {
  return pdfium::MakeRetain<CPDF_Name>(nullptr, m_Name);
}

ByteString CPDF_Name::GetString() const {
  return m_Name;
}

void CPDF_Name::SetString(const ByteString& str) {
  m_Name = str;
}

CPDF_Name* CPDF_Name::AsMutableName() {
  return this;
}

WideString CPDF_Name::GetUnicodeText() const {
  return PDF_DecodeText(m_Name.unsigned_span());
}

bool CPDF_Name::WriteTo(IFX_ArchiveStream* archive,
                        const CPDF_Encryptor* encryptor) const {
  if (!archive->WriteString("/"))
    return false;

  const ByteString name = PDF_NameEncode(GetString());
  return name.IsEmpty() || archive->WriteString(name.AsStringView());
}
