// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PARSER_CPDF_NAME_H_
#define CORE_FPDFAPI_PARSER_CPDF_NAME_H_

#include "core/fpdfapi/parser/cpdf_object.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/string_pool_template.h"
#include "core/fxcrt/weak_ptr.h"

class CPDF_Name final : public CPDF_Object {
 public:
  CONSTRUCT_VIA_MAKE_RETAIN;

  // CPDF_Object:
  Type GetType() const override;
  RetainPtr<CPDF_Object> Clone() const override;
  ByteString GetString() const override;
  WideString GetUnicodeText() const override;
  void SetString(const ByteString& str) override;
  CPDF_Name* AsMutableName() override;
  bool WriteTo(IFX_ArchiveStream* archive,
               const CPDF_Encryptor* encryptor) const override;

 private:
  CPDF_Name(WeakPtr<ByteStringPool> pPool, const ByteString& str);
  ~CPDF_Name() override;

  ByteString m_Name;
};

inline CPDF_Name* ToName(CPDF_Object* obj) {
  return obj ? obj->AsMutableName() : nullptr;
}

inline const CPDF_Name* ToName(const CPDF_Object* obj) {
  return obj ? obj->AsName() : nullptr;
}

inline RetainPtr<const CPDF_Name> ToName(RetainPtr<CPDF_Object> obj) {
  return RetainPtr<CPDF_Name>(ToName(obj.Get()));
}

inline RetainPtr<const CPDF_Name> ToName(RetainPtr<const CPDF_Object> obj) {
  return RetainPtr<const CPDF_Name>(ToName(obj.Get()));
}

#endif  // CORE_FPDFAPI_PARSER_CPDF_NAME_H_
