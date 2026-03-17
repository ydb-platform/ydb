// Copyright 2024 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/edit/cpdf_pageexporter.h"

#include "constants/page_object.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fpdfapi/parser/cpdf_object.h"

CPDF_PageExporter::CPDF_PageExporter(CPDF_Document* dest_doc,
                                     CPDF_Document* src_doc)
    : CPDF_PageOrganizer(dest_doc, src_doc) {}

CPDF_PageExporter::~CPDF_PageExporter() = default;

bool CPDF_PageExporter::ExportPages(pdfium::span<const uint32_t> page_indices,
                                    int index) {
  if (!Init()) {
    return false;
  }

  int curpage = index;
  for (uint32_t pageIndex : page_indices) {
    RetainPtr<CPDF_Dictionary> dest_page_dict = dest()->CreateNewPage(curpage);
    RetainPtr<const CPDF_Dictionary> src_page_dict =
        src()->GetPageDictionary(pageIndex);
    if (!src_page_dict || !dest_page_dict) {
      return false;
    }

    // Clone the page dictionary
    CPDF_DictionaryLocker locker(src_page_dict);
    for (const auto& it : locker) {
      const ByteString& src_key = it.first;
      const RetainPtr<CPDF_Object>& obj = it.second;
      if (src_key == pdfium::page_object::kType ||
          src_key == pdfium::page_object::kParent) {
        continue;
      }
      dest_page_dict->SetFor(src_key, obj->Clone());
    }

    // inheritable item
    // Even though some entries are required by the PDF spec, there exist
    // PDFs that omit them. Set some defaults in this case.
    // 1 MediaBox - required
    if (!CopyInheritable(dest_page_dict, src_page_dict,
                         pdfium::page_object::kMediaBox)) {
      // Search for "CropBox" in the source page dictionary.
      // If it does not exist, use the default letter size.
      RetainPtr<const CPDF_Object> inheritable = PageDictGetInheritableTag(
          src_page_dict, pdfium::page_object::kCropBox);
      if (inheritable) {
        dest_page_dict->SetFor(pdfium::page_object::kMediaBox,
                               inheritable->Clone());
      } else {
        // Make the default size letter size (8.5"x11")
        static const CFX_FloatRect kDefaultLetterRect(0, 0, 612, 792);
        dest_page_dict->SetRectFor(pdfium::page_object::kMediaBox,
                                   kDefaultLetterRect);
      }
    }

    // 2 Resources - required
    if (!CopyInheritable(dest_page_dict, src_page_dict,
                         pdfium::page_object::kResources)) {
      // Use a default empty resources if it does not exist.
      dest_page_dict->SetNewFor<CPDF_Dictionary>(
          pdfium::page_object::kResources);
    }

    // 3 CropBox - optional
    CopyInheritable(dest_page_dict, src_page_dict,
                    pdfium::page_object::kCropBox);
    // 4 Rotate - optional
    CopyInheritable(dest_page_dict, src_page_dict,
                    pdfium::page_object::kRotate);

    // Update the reference
    uint32_t old_page_obj_num = src_page_dict->GetObjNum();
    uint32_t new_page_obj_num = dest_page_dict->GetObjNum();
    AddObjectMapping(old_page_obj_num, new_page_obj_num);
    UpdateReference(dest_page_dict);
    ++curpage;
  }

  return true;
}
