// Copyright 2024 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/edit/cpdf_npagetooneexporter.h"

#include <algorithm>
#include <memory>
#include <sstream>
#include <utility>

#include "constants/page_object.h"
#include "core/fpdfapi/edit/cpdf_contentstream_write_utils.h"
#include "core/fpdfapi/page/cpdf_page.h"
#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fpdfapi/parser/cpdf_name.h"
#include "core/fpdfapi/parser/cpdf_number.h"
#include "core/fpdfapi/parser/cpdf_object.h"
#include "core/fpdfapi/parser/cpdf_reference.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fpdfapi/parser/cpdf_stream_acc.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/fx_safe_types.h"
#include "core/fxcrt/fx_string_wrappers.h"
#include "core/fxcrt/span.h"

namespace {

// Calculates the N-up parameters.  When importing multiple pages into one page.
// The space of output page is evenly divided along the X axis and Y axis based
// on the input `pages_on_x_axis` and `pages_on_y_axis`.
class NupState {
 public:
  NupState(const CFX_SizeF& pagesize,
           size_t pages_on_x_axis,
           size_t pages_on_y_axis);

  // Calculate sub page origin and scale with the source page of `pagesize` and
  // new page of `sub_page_size_`.
  CPDF_NPageToOneExporter::NupPageSettings CalculateNewPagePosition(
      const CFX_SizeF& pagesize);

 private:
  // Helper function to get the `sub_x`, `sub_y` pair based on
  // `sub_page_index_`. The space of output page is evenly divided into slots
  // along x and y axis. `sub_x` and `sub_y` are 0-based indices that indicate
  // which allocation slot to use.
  std::pair<size_t, size_t> ConvertPageOrder() const;

  // Given the `sub_x` and `sub_y` subpage position within a page, and a source
  // page with dimensions of `pagesize`, calculate the sub page's origin and
  // scale.
  CPDF_NPageToOneExporter::NupPageSettings CalculatePageEdit(
      size_t sub_x,
      size_t sub_y,
      const CFX_SizeF& pagesize) const;

  const CFX_SizeF dest_page_size_;
  const size_t pages_on_x_axis_;
  const size_t pages_on_y_axis_;
  const size_t pages_per_sheet_;
  CFX_SizeF sub_page_size_;

  // A 0-based index, in range of [0, pages_per_sheet_ - 1).
  size_t sub_page_index_ = 0;
};

NupState::NupState(const CFX_SizeF& pagesize,
                   size_t pages_on_x_axis,
                   size_t pages_on_y_axis)
    : dest_page_size_(pagesize),
      pages_on_x_axis_(pages_on_x_axis),
      pages_on_y_axis_(pages_on_y_axis),
      pages_per_sheet_(pages_on_x_axis * pages_on_y_axis) {
  DCHECK(pages_on_x_axis_ > 0);
  DCHECK(pages_on_y_axis_ > 0);
  DCHECK(dest_page_size_.width > 0);
  DCHECK(dest_page_size_.height > 0);

  sub_page_size_.width = dest_page_size_.width / pages_on_x_axis_;
  sub_page_size_.height = dest_page_size_.height / pages_on_y_axis_;
}

std::pair<size_t, size_t> NupState::ConvertPageOrder() const {
  size_t sub_x = sub_page_index_ % pages_on_x_axis_;
  size_t sub_y = sub_page_index_ / pages_on_x_axis_;

  // Y Axis, pages start from the top of the output page.
  sub_y = pages_on_y_axis_ - sub_y - 1;

  return {sub_x, sub_y};
}

CPDF_NPageToOneExporter::NupPageSettings NupState::CalculatePageEdit(
    size_t sub_x,
    size_t sub_y,
    const CFX_SizeF& pagesize) const {
  CPDF_NPageToOneExporter::NupPageSettings settings;
  settings.sub_page_start_point.x = sub_x * sub_page_size_.width;
  settings.sub_page_start_point.y = sub_y * sub_page_size_.height;

  const float x_scale = sub_page_size_.width / pagesize.width;
  const float y_scale = sub_page_size_.height / pagesize.height;
  settings.scale = std::min(x_scale, y_scale);

  float sub_width = pagesize.width * settings.scale;
  float sub_height = pagesize.height * settings.scale;
  if (x_scale > y_scale) {
    settings.sub_page_start_point.x += (sub_page_size_.width - sub_width) / 2;
  } else {
    settings.sub_page_start_point.y += (sub_page_size_.height - sub_height) / 2;
  }
  return settings;
}

CPDF_NPageToOneExporter::NupPageSettings NupState::CalculateNewPagePosition(
    const CFX_SizeF& pagesize) {
  if (sub_page_index_ >= pages_per_sheet_) {
    sub_page_index_ = 0;
  }

  auto [sub_x, sub_y] = ConvertPageOrder();
  ++sub_page_index_;
  return CalculatePageEdit(sub_x, sub_y, pagesize);
}

// Helper that generates the content stream for a sub-page.
ByteString GenerateSubPageContentStream(
    const ByteString& xobject_name,
    const CPDF_NPageToOneExporter::NupPageSettings& settings) {
  CFX_Matrix matrix;
  matrix.Scale(settings.scale, settings.scale);
  matrix.Translate(settings.sub_page_start_point.x,
                   settings.sub_page_start_point.y);

  fxcrt::ostringstream content_stream;
  content_stream << "q\n";
  WriteMatrix(content_stream, matrix) << " cm\n"
                                      << "/" << xobject_name << " Do Q\n";
  return ByteString(content_stream);
}

}  // namespace

CPDF_NPageToOneExporter::CPDF_NPageToOneExporter(CPDF_Document* dest_doc,
                                                 CPDF_Document* src_doc)
    : CPDF_PageOrganizer(dest_doc, src_doc) {}

CPDF_NPageToOneExporter::~CPDF_NPageToOneExporter() = default;

bool CPDF_NPageToOneExporter::ExportNPagesToOne(
    pdfium::span<const uint32_t> page_indices,
    const CFX_SizeF& dest_page_size,
    size_t pages_on_x_axis,
    size_t pages_on_y_axis) {
  if (!Init()) {
    return false;
  }

  FX_SAFE_SIZE_T safe_pages_per_sheet = pages_on_x_axis;
  safe_pages_per_sheet *= pages_on_y_axis;
  if (!safe_pages_per_sheet.IsValid()) {
    return false;
  }

  ClearObjectNumberMap();
  src_page_xobject_map_.clear();
  size_t pages_per_sheet = safe_pages_per_sheet.ValueOrDie();
  NupState n_up_state(dest_page_size, pages_on_x_axis, pages_on_y_axis);

  FX_SAFE_INT32 curpage = 0;
  const CFX_FloatRect dest_page_rect(0, 0, dest_page_size.width,
                                     dest_page_size.height);
  for (size_t outer_page_index = 0; outer_page_index < page_indices.size();
       outer_page_index += pages_per_sheet) {
    xobject_name_to_number_map_.clear();

    RetainPtr<CPDF_Dictionary> dest_page_dict =
        dest()->CreateNewPage(curpage.ValueOrDie());
    if (!dest_page_dict) {
      return false;
    }

    dest_page_dict->SetRectFor(pdfium::page_object::kMediaBox, dest_page_rect);
    ByteString content;
    size_t inner_page_max =
        std::min(outer_page_index + pages_per_sheet, page_indices.size());
    for (size_t i = outer_page_index; i < inner_page_max; ++i) {
      RetainPtr<CPDF_Dictionary> src_page_dict =
          src()->GetMutablePageDictionary(page_indices[i]);
      if (!src_page_dict) {
        return false;
      }

      auto src_page = pdfium::MakeRetain<CPDF_Page>(src(), src_page_dict);
      src_page->AddPageImageCache();
      NupPageSettings settings =
          n_up_state.CalculateNewPagePosition(src_page->GetPageSize());
      content += AddSubPage(src_page, settings);
    }

    FinishPage(dest_page_dict, content);
    ++curpage;
  }

  return true;
}

// static
ByteString CPDF_NPageToOneExporter::GenerateSubPageContentStreamForTesting(
    const ByteString& xobject_name,
    const NupPageSettings& settings) {
  return GenerateSubPageContentStream(xobject_name, settings);
}

ByteString CPDF_NPageToOneExporter::AddSubPage(
    const RetainPtr<CPDF_Page>& src_page,
    const NupPageSettings& settings) {
  uint32_t src_page_obj_num = src_page->GetDict()->GetObjNum();
  const auto it = src_page_xobject_map_.find(src_page_obj_num);
  ByteString xobject_name = it != src_page_xobject_map_.end()
                                ? it->second
                                : MakeXObjectFromPage(src_page);
  return GenerateSubPageContentStream(xobject_name, settings);
}

RetainPtr<CPDF_Stream> CPDF_NPageToOneExporter::MakeXObjectFromPageRaw(
    RetainPtr<CPDF_Page> src_page) {
  RetainPtr<const CPDF_Dictionary> src_page_dict = src_page->GetDict();
  RetainPtr<const CPDF_Object> src_contents =
      src_page_dict->GetDirectObjectFor(pdfium::page_object::kContents);

  auto new_xobject =
      dest()->NewIndirect<CPDF_Stream>(dest()->New<CPDF_Dictionary>());
  RetainPtr<CPDF_Dictionary> new_xobject_dict = new_xobject->GetMutableDict();
  static const char kResourceString[] = "Resources";
  if (!CPDF_PageOrganizer::CopyInheritable(new_xobject_dict, src_page_dict,
                                           kResourceString)) {
    // Use a default empty resources if it does not exist.
    new_xobject_dict->SetNewFor<CPDF_Dictionary>(kResourceString);
  }
  uint32_t src_page_obj_num = src_page_dict->GetObjNum();
  uint32_t new_xobject_obj_num = new_xobject_dict->GetObjNum();
  AddObjectMapping(src_page_obj_num, new_xobject_obj_num);
  UpdateReference(new_xobject_dict);
  new_xobject_dict->SetNewFor<CPDF_Name>("Type", "XObject");
  new_xobject_dict->SetNewFor<CPDF_Name>("Subtype", "Form");
  new_xobject_dict->SetNewFor<CPDF_Number>("FormType", 1);
  new_xobject_dict->SetRectFor("BBox", src_page->GetBBox());
  new_xobject_dict->SetMatrixFor("Matrix", src_page->GetPageMatrix());
  if (!src_contents) {
    return new_xobject;
  }
  const CPDF_Array* src_contents_array = src_contents->AsArray();
  if (!src_contents_array) {
    RetainPtr<const CPDF_Stream> stream(src_contents->AsStream());
    auto acc = pdfium::MakeRetain<CPDF_StreamAcc>(std::move(stream));
    acc->LoadAllDataFiltered();
    new_xobject->SetDataAndRemoveFilter(acc->GetSpan());
    return new_xobject;
  }
  ByteString src_content_stream;
  for (size_t i = 0; i < src_contents_array->size(); ++i) {
    RetainPtr<const CPDF_Stream> stream = src_contents_array->GetStreamAt(i);
    auto acc = pdfium::MakeRetain<CPDF_StreamAcc>(std::move(stream));
    acc->LoadAllDataFiltered();
    src_content_stream += ByteStringView(acc->GetSpan());
    src_content_stream += "\n";
  }
  new_xobject->SetDataAndRemoveFilter(src_content_stream.unsigned_span());
  return new_xobject;
}

ByteString CPDF_NPageToOneExporter::MakeXObjectFromPage(
    RetainPtr<CPDF_Page> src_page) {
  RetainPtr<CPDF_Stream> new_xobject = MakeXObjectFromPageRaw(src_page);

  // TODO(xlou): A better name schema to avoid possible object name collision.
  ByteString xobject_name = ByteString::Format("X%d", ++object_number_);
  xobject_name_to_number_map_[xobject_name] = new_xobject->GetObjNum();
  src_page_xobject_map_[src_page->GetDict()->GetObjNum()] = xobject_name;
  return xobject_name;
}

std::unique_ptr<XObjectContext>
CPDF_NPageToOneExporter::CreateXObjectContextFromPage(int src_page_index) {
  RetainPtr<CPDF_Dictionary> src_page_dict =
      src()->GetMutablePageDictionary(src_page_index);
  if (!src_page_dict) {
    return nullptr;
  }

  auto src_page = pdfium::MakeRetain<CPDF_Page>(src(), src_page_dict);
  auto xobject = std::make_unique<XObjectContext>();
  xobject->dest_doc = dest();
  xobject->xobject.Reset(MakeXObjectFromPageRaw(src_page));
  return xobject;
}

void CPDF_NPageToOneExporter::FinishPage(
    RetainPtr<CPDF_Dictionary> dest_page_dict,
    const ByteString& content) {
  RetainPtr<CPDF_Dictionary> resources =
      dest_page_dict->GetOrCreateDictFor(pdfium::page_object::kResources);
  RetainPtr<CPDF_Dictionary> xobject = resources->GetOrCreateDictFor("XObject");
  for (auto& it : xobject_name_to_number_map_) {
    xobject->SetNewFor<CPDF_Reference>(it.first, dest(), it.second);
  }

  auto stream =
      dest()->NewIndirect<CPDF_Stream>(dest()->New<CPDF_Dictionary>());
  stream->SetData(content.unsigned_span());
  dest_page_dict->SetNewFor<CPDF_Reference>(pdfium::page_object::kContents,
                                            dest(), stream->GetObjNum());
}

XObjectContext::XObjectContext() = default;

XObjectContext::~XObjectContext() = default;
