// Copyright 2024 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_EDIT_CPDF_NPAGETOONEEXPORTER_H_
#define CORE_FPDFAPI_EDIT_CPDF_NPAGETOONEEXPORTER_H_

#include <stddef.h>
#include <stdint.h>

#include <map>
#include <memory>

#include "core/fpdfapi/edit/cpdf_pageorganizer.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fxcrt/bytestring.h"
#include "core/fxcrt/fx_coordinates.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/span.h"
#include "core/fxcrt/unowned_ptr.h"

class CPDF_Document;
class CPDF_Page;

struct XObjectContext {
  XObjectContext();
  ~XObjectContext();

  UnownedPtr<CPDF_Document> dest_doc;
  RetainPtr<CPDF_Stream> xobject;
};

// Copies pages from a source document into a destination document. Creates 1
// page in the destination document for every N source pages. This class is
// intended to be used once via ExportNPagesToOne() and then destroyed.
class CPDF_NPageToOneExporter final : public CPDF_PageOrganizer {
 public:
  // Struct that stores sub page origin and scale information.  When importing
  // more than one pages onto the same page, most likely the pages will need to
  // be scaled down, and scale is in range of (0, 1) exclusive.
  struct NupPageSettings {
    CFX_PointF sub_page_start_point;
    float scale = 0.0f;
  };

  CPDF_NPageToOneExporter(CPDF_Document* dest_doc, CPDF_Document* src_doc);
  ~CPDF_NPageToOneExporter();

  // For the pages from the source document with `page_indices` as their page
  // indices, insert them into the destination document, starting at page index
  // 0.
  // `page_indices` is 0-based.
  // `dest_page_size` is the destination document page dimensions, measured in
  // PDF "user space" units.
  // `pages_on_x_axis` and `nPagesOnXAxis` together defines how many source
  // pages fit on one destination page.
  bool ExportNPagesToOne(pdfium::span<const uint32_t> page_indices,
                         const CFX_SizeF& dest_page_size,
                         size_t pages_on_x_axis,
                         size_t pages_on_y_axis);

  std::unique_ptr<XObjectContext> CreateXObjectContextFromPage(
      int src_page_index);

  // Helper that generates the content stream for a sub-page. Exposed for
  // testing.
  static ByteString GenerateSubPageContentStreamForTesting(
      const ByteString& xobject_name,
      const NupPageSettings& settings);

 private:
  // Map page object number to XObject object name.
  using PageXObjectMap = std::map<uint32_t, ByteString>;

  // Creates an XObject from `src_page`, or find an existing XObject that
  // represents `src_page`. The transformation matrix is specified in
  // `settings`.
  // Returns the XObject reference surrounded by the transformation matrix.
  ByteString AddSubPage(const RetainPtr<CPDF_Page>& src_page,
                        const NupPageSettings& settings);

  // Creates an XObject from `src_page`. Updates mapping as needed.
  // Returns the name of the newly created XObject.
  ByteString MakeXObjectFromPage(RetainPtr<CPDF_Page> src_page);
  RetainPtr<CPDF_Stream> MakeXObjectFromPageRaw(RetainPtr<CPDF_Page> src_page);

  // Adds `content` as the Contents key in `dest_page_dict`.
  // Adds the objects in `xobject_name_to_number_map_` to the XObject
  // dictionary in `dest_page_dict`'s Resources dictionary.
  void FinishPage(RetainPtr<CPDF_Dictionary> dest_page_dict,
                  const ByteString& content);

  // Counter for giving new XObjects unique names.
  uint32_t object_number_ = 0;

  // Keeps track of created XObjects in the current page.
  // Map XObject's object name to it's object number.
  std::map<ByteString, uint32_t> xobject_name_to_number_map_;

  // Mapping of source page object number and XObject name of the entire doc.
  // If there are multiple source pages that reference the same object number,
  // they can also share the same created XObject.
  PageXObjectMap src_page_xobject_map_;
};

#endif  // CORE_FPDFAPI_EDIT_CPDF_NPAGETOONEEXPORTER_H_
