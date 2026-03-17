// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "public/fpdf_catalog.h"

#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fpdfapi/parser/cpdf_string.h"
#include "core/fxcrt/retain_ptr.h"
#include "fpdfsdk/cpdfsdk_helpers.h"

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFCatalog_IsTagged(FPDF_DOCUMENT document) {
  CPDF_Document* pDoc = CPDFDocumentFromFPDFDocument(document);
  if (!pDoc)
    return false;

  const CPDF_Dictionary* pCatalog = pDoc->GetRoot();
  if (!pCatalog)
    return false;

  RetainPtr<const CPDF_Dictionary> pMarkInfo = pCatalog->GetDictFor("MarkInfo");
  return pMarkInfo && pMarkInfo->GetIntegerFor("Marked") != 0;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFCatalog_SetLanguage(FPDF_DOCUMENT document, FPDF_BYTESTRING language) {
  if (!language) {
    return false;
  }

  CPDF_Document* doc = CPDFDocumentFromFPDFDocument(document);
  if (!doc) {
    return false;
  }

  RetainPtr<CPDF_Dictionary> catalog = doc->GetMutableRoot();
  if (!catalog) {
    return false;
  }

  catalog->SetNewFor<CPDF_String>("Lang", language);
  return true;
}
