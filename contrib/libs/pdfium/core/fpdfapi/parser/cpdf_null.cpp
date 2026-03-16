// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/parser/cpdf_null.h"

#include "core/fxcrt/fx_stream.h"

CPDF_Null::CPDF_Null() = default;

CPDF_Object::Type CPDF_Null::GetType() const {
  return kNullobj;
}

RetainPtr<CPDF_Object> CPDF_Null::Clone() const {
  return pdfium::MakeRetain<CPDF_Null>();
}

CPDF_Null* CPDF_Null::AsMutableNull() {
  return this;
}

bool CPDF_Null::WriteTo(IFX_ArchiveStream* archive,
                        const CPDF_Encryptor* encryptor) const {
  return archive->WriteString(" null");
}
