// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "public/fpdf_ppo.h"

#include <memory>
#include <numeric>
#include <utility>
#include <vector>

#include "core/fpdfapi/edit/cpdf_npagetooneexporter.h"
#include "core/fpdfapi/edit/cpdf_pageexporter.h"
#include "core/fpdfapi/page/cpdf_form.h"
#include "core/fpdfapi/page/cpdf_formobject.h"
#include "core/fpdfapi/page/cpdf_page.h"
#include "core/fpdfapi/page/cpdf_pageimagecache.h"
#include "core/fpdfapi/page/cpdf_pageobject.h"
#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fpdfapi/parser/cpdf_object.h"
#include "core/fpdfapi/parser/fpdf_parser_utility.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/span.h"
#include "fpdfsdk/cpdfsdk_helpers.h"
#include "public/cpp/fpdf_scopers.h"

namespace {

std::vector<uint32_t> GetPageIndices(const CPDF_Document& doc,
                                     const ByteString& page_range) {
  uint32_t count = doc.GetPageCount();
  if (!page_range.IsEmpty()) {
    return ParsePageRangeString(page_range, count);
  }

  std::vector<uint32_t> page_indices(count);
  std::iota(page_indices.begin(), page_indices.end(), 0);
  return page_indices;
}


// Make sure arrays only contain objects of basic types.
bool IsValidViewerPreferencesArray(const CPDF_Array* array) {
  CPDF_ArrayLocker locker(array);
  for (const auto& obj : locker) {
    if (obj->IsArray() || obj->IsDictionary() || obj->IsReference() ||
        obj->IsStream()) {
      return false;
    }
  }
  return true;
}

bool IsValidViewerPreferencesObject(const CPDF_Object* obj) {
  // Per spec, there are no valid entries of these types.
  if (obj->IsDictionary() || obj->IsNull() || obj->IsReference() ||
      obj->IsStream()) {
    return false;
  }

  const CPDF_Array* array = obj->AsArray();
  if (!array) {
    return true;
  }

  return IsValidViewerPreferencesArray(array);
}

}  // namespace

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDF_ImportPagesByIndex(FPDF_DOCUMENT dest_doc,
                        FPDF_DOCUMENT src_doc,
                        const int* page_indices,
                        unsigned long length,
                        int index) {
  CPDF_Document* cdest_doc = CPDFDocumentFromFPDFDocument(dest_doc);
  if (!cdest_doc) {
    return false;
  }

  CPDF_Document* csrc_doc = CPDFDocumentFromFPDFDocument(src_doc);
  if (!csrc_doc) {
    return false;
  }

  CPDF_PageExporter exporter(cdest_doc, csrc_doc);

  if (!page_indices) {
    std::vector<uint32_t> page_indices_vec(csrc_doc->GetPageCount());
    std::iota(page_indices_vec.begin(), page_indices_vec.end(), 0);
    return exporter.ExportPages(page_indices_vec, index);
  }
  if (length == 0) {
    return false;
  }

  // SAFETY: required from caller.
  auto page_span = UNSAFE_BUFFERS(pdfium::make_span(page_indices, length));
  for (int page_index : page_span) {
    if (page_index < 0) {
      return false;
    }
  }
  return exporter.ExportPages(
      fxcrt::reinterpret_span<const uint32_t>(page_span), index);
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDF_ImportPages(FPDF_DOCUMENT dest_doc,
                                                     FPDF_DOCUMENT src_doc,
                                                     FPDF_BYTESTRING pagerange,
                                                     int index) {
  CPDF_Document* cdest_doc = CPDFDocumentFromFPDFDocument(dest_doc);
  if (!cdest_doc) {
    return false;
  }

  CPDF_Document* csrc_doc = CPDFDocumentFromFPDFDocument(src_doc);
  if (!csrc_doc) {
    return false;
  }

  std::vector<uint32_t> page_indices = GetPageIndices(*csrc_doc, pagerange);
  if (page_indices.empty())
    return false;

  CPDF_PageExporter exporter(cdest_doc, csrc_doc);
  return exporter.ExportPages(page_indices, index);
}

FPDF_EXPORT FPDF_DOCUMENT FPDF_CALLCONV
FPDF_ImportNPagesToOne(FPDF_DOCUMENT src_doc,
                       float output_width,
                       float output_height,
                       size_t pages_on_x_axis,
                       size_t pages_on_y_axis) {
  CPDF_Document* csrc_doc = CPDFDocumentFromFPDFDocument(src_doc);
  if (!csrc_doc) {
    return nullptr;
  }

  if (output_width <= 0 || output_height <= 0 || pages_on_x_axis <= 0 ||
      pages_on_y_axis <= 0) {
    return nullptr;
  }

  ScopedFPDFDocument output_doc(FPDF_CreateNewDocument());
  if (!output_doc)
    return nullptr;

  CPDF_Document* dest_doc = CPDFDocumentFromFPDFDocument(output_doc.get());
  DCHECK(dest_doc);

  std::vector<uint32_t> page_indices = GetPageIndices(*csrc_doc, ByteString());
  if (page_indices.empty())
    return nullptr;

  if (pages_on_x_axis == 1 && pages_on_y_axis == 1) {
    CPDF_PageExporter exporter(dest_doc, csrc_doc);
    if (!exporter.ExportPages(page_indices, 0)) {
      return nullptr;
    }
    return output_doc.release();
  }

  CPDF_NPageToOneExporter exporter(dest_doc, csrc_doc);
  if (!exporter.ExportNPagesToOne(page_indices,
                                  CFX_SizeF(output_width, output_height),
                                  pages_on_x_axis, pages_on_y_axis)) {
    return nullptr;
  }
  return output_doc.release();
}

FPDF_EXPORT FPDF_XOBJECT FPDF_CALLCONV
FPDF_NewXObjectFromPage(FPDF_DOCUMENT dest_doc,
                        FPDF_DOCUMENT src_doc,
                        int src_page_index) {
  CPDF_Document* dest = CPDFDocumentFromFPDFDocument(dest_doc);
  if (!dest)
    return nullptr;

  CPDF_Document* src = CPDFDocumentFromFPDFDocument(src_doc);
  if (!src)
    return nullptr;

  CPDF_NPageToOneExporter exporter(dest, src);
  std::unique_ptr<XObjectContext> xobject =
      exporter.CreateXObjectContextFromPage(src_page_index);
  return FPDFXObjectFromXObjectContext(xobject.release());
}

FPDF_EXPORT void FPDF_CALLCONV FPDF_CloseXObject(FPDF_XOBJECT xobject) {
  std::unique_ptr<XObjectContext> xobject_deleter(
      XObjectContextFromFPDFXObject(xobject));
}

FPDF_EXPORT FPDF_PAGEOBJECT FPDF_CALLCONV
FPDF_NewFormObjectFromXObject(FPDF_XOBJECT xobject) {
  XObjectContext* xobj = XObjectContextFromFPDFXObject(xobject);
  if (!xobj)
    return nullptr;

  auto form = std::make_unique<CPDF_Form>(xobj->dest_doc, nullptr,
                                          xobj->xobject, nullptr);
  form->ParseContent(nullptr, nullptr, nullptr);
  auto form_object = std::make_unique<CPDF_FormObject>(
      CPDF_PageObject::kNoContentStream, std::move(form), CFX_Matrix());
  return FPDFPageObjectFromCPDFPageObject(form_object.release());
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDF_CopyViewerPreferences(FPDF_DOCUMENT dest_doc, FPDF_DOCUMENT src_doc) {
  CPDF_Document* cdest_doc = CPDFDocumentFromFPDFDocument(dest_doc);
  if (!cdest_doc) {
    return false;
  }

  CPDF_Document* csrc_doc = CPDFDocumentFromFPDFDocument(src_doc);
  if (!csrc_doc) {
    return false;
  }

  RetainPtr<const CPDF_Dictionary> pref_dict =
      csrc_doc->GetRoot()->GetDictFor("ViewerPreferences");
  if (!pref_dict) {
    return false;
  }

  RetainPtr<CPDF_Dictionary> dest_dict = cdest_doc->GetMutableRoot();
  if (!dest_dict) {
    return false;
  }

  auto cloned_dict = pdfium::MakeRetain<CPDF_Dictionary>();
  CPDF_DictionaryLocker locker(pref_dict);
  for (const auto& it : locker) {
    if (IsValidViewerPreferencesObject(it.second)) {
      cloned_dict->SetFor(it.first, it.second->Clone());
    }
  }

  dest_dict->SetFor("ViewerPreferences", std::move(cloned_dict));
  return true;
}
