// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/parser/cpdf_string.h"

#include <stdint.h>

#include <utility>

#include "core/fpdfapi/parser/cpdf_encryptor.h"
#include "core/fpdfapi/parser/fpdf_parser_decode.h"
#include "core/fxcrt/data_vector.h"
#include "core/fxcrt/fx_stream.h"

CPDF_String::CPDF_String() = default;

CPDF_String::CPDF_String(WeakPtr<ByteStringPool> pool,
                         pdfium::span<const uint8_t> data,
                         DataType is_hex)
    : data_(ByteStringView(data)), output_is_hex_(true) {
  if (pool) {
    data_ = pool->Intern(data_);
  }
}

CPDF_String::CPDF_String(WeakPtr<ByteStringPool> pool, const ByteString& str)
    : data_(str) {
  if (pool) {
    data_ = pool->Intern(data_);
  }
}

CPDF_String::CPDF_String(WeakPtr<ByteStringPool> pool, WideStringView str)
    : CPDF_String(pool, PDF_EncodeText(str)) {
  // Delegates to ctor above.
}

CPDF_String::~CPDF_String() = default;

CPDF_Object::Type CPDF_String::GetType() const {
  return kString;
}

RetainPtr<CPDF_Object> CPDF_String::Clone() const {
  auto clone = pdfium::MakeRetain<CPDF_String>();
  clone->data_ = data_;
  clone->output_is_hex_ = output_is_hex_;
  return clone;
}

ByteString CPDF_String::GetString() const {
  return data_;
}

void CPDF_String::SetString(const ByteString& str) {
  data_ = str;
}

CPDF_String* CPDF_String::AsMutableString() {
  return this;
}

WideString CPDF_String::GetUnicodeText() const {
  return PDF_DecodeText(data_.unsigned_span());
}

bool CPDF_String::WriteTo(IFX_ArchiveStream* archive,
                          const CPDF_Encryptor* encryptor) const {
  DataVector<uint8_t> encrypted_data;
  pdfium::span<const uint8_t> data = data_.unsigned_span();
  if (encryptor) {
    encrypted_data = encryptor->Encrypt(data);
    data = encrypted_data;
  }
  ByteStringView raw(data);
  ByteString content =
      output_is_hex_ ? PDF_HexEncodeString(raw) : PDF_EncodeString(raw);
  return archive->WriteString(content.AsStringView());
}

ByteString CPDF_String::EncodeString() const {
  return output_is_hex_ ? PDF_HexEncodeString(data_.AsStringView())
                        : PDF_EncodeString(data_.AsStringView());
}
