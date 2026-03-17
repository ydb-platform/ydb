// Copyright 2022 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FPDFAPI_PARSER_CPDF_TEST_DOCUMENT_H_
#define CORE_FPDFAPI_PARSER_CPDF_TEST_DOCUMENT_H_

#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fxcrt/retain_ptr.h"

class CPDF_Dictionary;

class CPDF_TestDocument : public CPDF_Document {
 public:
  CPDF_TestDocument();

  void SetRoot(RetainPtr<CPDF_Dictionary> root);
};

#endif  // CORE_FPDFAPI_PARSER_CPDF_TEST_DOCUMENT_H_
