// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/parser/cpdf_number.h"

#include <sstream>

#include "core/fpdfapi/edit/cpdf_contentstream_write_utils.h"
#include "core/fxcrt/fx_stream.h"
#include "core/fxcrt/fx_string_wrappers.h"

namespace {

ByteString FloatToString(float value) {
  fxcrt::ostringstream sstream;
  WriteFloat(sstream, value);
  return ByteString(sstream);
}

}  // namespace

CPDF_Number::CPDF_Number() = default;

CPDF_Number::CPDF_Number(int value) : number_(value) {}

CPDF_Number::CPDF_Number(float value) : number_(value) {}

CPDF_Number::CPDF_Number(ByteStringView str) : number_(str) {}

CPDF_Number::~CPDF_Number() = default;

CPDF_Object::Type CPDF_Number::GetType() const {
  return kNumber;
}

RetainPtr<CPDF_Object> CPDF_Number::Clone() const {
  return number_.IsInteger()
             ? pdfium::MakeRetain<CPDF_Number>(number_.GetSigned())
             : pdfium::MakeRetain<CPDF_Number>(number_.GetFloat());
}

float CPDF_Number::GetNumber() const {
  return number_.GetFloat();
}

int CPDF_Number::GetInteger() const {
  return number_.GetSigned();
}

CPDF_Number* CPDF_Number::AsMutableNumber() {
  return this;
}

void CPDF_Number::SetString(const ByteString& str) {
  number_ = FX_Number(str.AsStringView());
}

ByteString CPDF_Number::GetString() const {
  return number_.IsInteger() ? ByteString::FormatInteger(number_.GetSigned())
                             : FloatToString(GetNumber());
}

bool CPDF_Number::WriteTo(IFX_ArchiveStream* archive,
                          const CPDF_Encryptor* encryptor) const {
  return archive->WriteString(" ") &&
         archive->WriteString(GetString().AsStringView());
}
