// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PARSER_CPDF_STRING_H_
#define CORE_FPDFAPI_PARSER_CPDF_STRING_H_

#include "core/fpdfapi/parser/cpdf_object.h"
#include "core/fxcrt/fx_string.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/span.h"
#include "core/fxcrt/string_pool_template.h"
#include "core/fxcrt/weak_ptr.h"

class CPDF_String final : public CPDF_Object {
 public:
  CONSTRUCT_VIA_MAKE_RETAIN;

  // Used as a placeholder to differentiate constructors.
  enum class DataType { kIsHex };

  // CPDF_Object:
  Type GetType() const override;
  RetainPtr<CPDF_Object> Clone() const override;
  ByteString GetString() const override;
  WideString GetUnicodeText() const override;
  void SetString(const ByteString& str) override;
  CPDF_String* AsMutableString() override;
  bool WriteTo(IFX_ArchiveStream* archive,
               const CPDF_Encryptor* encryptor) const override;

  bool IsHex() const { return output_is_hex_; }
  ByteString EncodeString() const;

 private:
  CPDF_String();
  CPDF_String(WeakPtr<ByteStringPool> pool,
              pdfium::span<const uint8_t> data,
              DataType is_hex);
  CPDF_String(WeakPtr<ByteStringPool> pool, const ByteString& str);
  CPDF_String(WeakPtr<ByteStringPool> pool, WideStringView str);
  ~CPDF_String() override;

  ByteString data_;
  bool output_is_hex_ = false;
};

inline CPDF_String* ToString(CPDF_Object* obj) {
  return obj ? obj->AsMutableString() : nullptr;
}

inline const CPDF_String* ToString(const CPDF_Object* obj) {
  return obj ? obj->AsString() : nullptr;
}

inline RetainPtr<const CPDF_String> ToString(RetainPtr<const CPDF_Object> obj) {
  return RetainPtr<const CPDF_String>(ToString(obj.Get()));
}

#endif  // CORE_FPDFAPI_PARSER_CPDF_STRING_H_
