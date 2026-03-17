// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PARSER_CPDF_NUMBER_H_
#define CORE_FPDFAPI_PARSER_CPDF_NUMBER_H_

#include "core/fpdfapi/parser/cpdf_object.h"
#include "core/fxcrt/bytestring.h"
#include "core/fxcrt/fx_number.h"
#include "core/fxcrt/retain_ptr.h"

class CPDF_Number final : public CPDF_Object {
 public:
  CONSTRUCT_VIA_MAKE_RETAIN;

  // CPDF_Object:
  Type GetType() const override;
  RetainPtr<CPDF_Object> Clone() const override;
  ByteString GetString() const override;
  float GetNumber() const override;
  int GetInteger() const override;
  void SetString(const ByteString& str) override;
  CPDF_Number* AsMutableNumber() override;
  bool WriteTo(IFX_ArchiveStream* archive,
               const CPDF_Encryptor* encryptor) const override;

  bool IsInteger() const { return number_.IsInteger(); }

 private:
  CPDF_Number();
  explicit CPDF_Number(int value);
  explicit CPDF_Number(float value);
  explicit CPDF_Number(ByteStringView str);
  ~CPDF_Number() override;

  FX_Number number_;
};

inline CPDF_Number* ToNumber(CPDF_Object* obj) {
  return obj ? obj->AsMutableNumber() : nullptr;
}

inline const CPDF_Number* ToNumber(const CPDF_Object* obj) {
  return obj ? obj->AsNumber() : nullptr;
}

inline RetainPtr<CPDF_Number> ToNumber(RetainPtr<CPDF_Object> obj) {
  return RetainPtr<CPDF_Number>(ToNumber(obj.Get()));
}

inline RetainPtr<const CPDF_Number> ToNumber(RetainPtr<const CPDF_Object> obj) {
  return RetainPtr<const CPDF_Number>(ToNumber(obj.Get()));
}

#endif  // CORE_FPDFAPI_PARSER_CPDF_NUMBER_H_
