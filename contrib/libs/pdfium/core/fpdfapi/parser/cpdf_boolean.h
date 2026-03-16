// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PARSER_CPDF_BOOLEAN_H_
#define CORE_FPDFAPI_PARSER_CPDF_BOOLEAN_H_

#include "core/fpdfapi/parser/cpdf_object.h"
#include "core/fxcrt/bytestring.h"
#include "core/fxcrt/retain_ptr.h"

class CPDF_Boolean final : public CPDF_Object {
 public:
  CONSTRUCT_VIA_MAKE_RETAIN;

  // CPDF_Object:
  Type GetType() const override;
  RetainPtr<CPDF_Object> Clone() const override;
  ByteString GetString() const override;
  int GetInteger() const override;
  void SetString(const ByteString& str) override;
  CPDF_Boolean* AsMutableBoolean() override;
  bool WriteTo(IFX_ArchiveStream* archive,
               const CPDF_Encryptor* encryptor) const override;

 private:
  CPDF_Boolean();
  explicit CPDF_Boolean(bool value);
  ~CPDF_Boolean() override;

  bool m_bValue = false;
};

inline CPDF_Boolean* ToBoolean(CPDF_Object* obj) {
  return obj ? obj->AsMutableBoolean() : nullptr;
}

inline const CPDF_Boolean* ToBoolean(const CPDF_Object* obj) {
  return obj ? obj->AsBoolean() : nullptr;
}

#endif  // CORE_FPDFAPI_PARSER_CPDF_BOOLEAN_H_
