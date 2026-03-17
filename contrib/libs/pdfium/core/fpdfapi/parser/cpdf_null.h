// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PARSER_CPDF_NULL_H_
#define CORE_FPDFAPI_PARSER_CPDF_NULL_H_

#include "core/fpdfapi/parser/cpdf_object.h"
#include "core/fxcrt/retain_ptr.h"

class CPDF_Null final : public CPDF_Object {
 public:
  CONSTRUCT_VIA_MAKE_RETAIN;

  // CPDF_Object.
  Type GetType() const override;
  RetainPtr<CPDF_Object> Clone() const override;
  CPDF_Null* AsMutableNull() override;
  bool WriteTo(IFX_ArchiveStream* archive,
               const CPDF_Encryptor* encryptor) const override;

 private:
  CPDF_Null();
};

#endif  // CORE_FPDFAPI_PARSER_CPDF_NULL_H_
