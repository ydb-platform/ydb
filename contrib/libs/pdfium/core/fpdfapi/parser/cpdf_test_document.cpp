// Copyright 2022 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "core/fpdfapi/parser/cpdf_test_document.h"

#include <memory>
#include <utility>

#include "core/fpdfapi/page/cpdf_docpagedata.h"
#include "core/fpdfapi/render/cpdf_docrenderdata.h"

CPDF_TestDocument::CPDF_TestDocument()
    : CPDF_Document(std::make_unique<CPDF_DocRenderData>(),
                    std::make_unique<CPDF_DocPageData>()) {}

void CPDF_TestDocument::SetRoot(RetainPtr<CPDF_Dictionary> root) {
  SetRootForTesting(std::move(root));
}
