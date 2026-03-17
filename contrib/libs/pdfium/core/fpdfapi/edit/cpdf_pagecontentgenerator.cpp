// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/edit/cpdf_pagecontentgenerator.h"

#include <map>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <tuple>
#include <utility>

#include "constants/page_object.h"
#include "core/fpdfapi/edit/cpdf_contentstream_write_utils.h"
#include "core/fpdfapi/edit/cpdf_pagecontentmanager.h"
#include "core/fpdfapi/edit/cpdf_stringarchivestream.h"
#include "core/fpdfapi/font/cpdf_truetypefont.h"
#include "core/fpdfapi/font/cpdf_type1font.h"
#include "core/fpdfapi/page/cpdf_contentmarks.h"
#include "core/fpdfapi/page/cpdf_docpagedata.h"
#include "core/fpdfapi/page/cpdf_form.h"
#include "core/fpdfapi/page/cpdf_formobject.h"
#include "core/fpdfapi/page/cpdf_image.h"
#include "core/fpdfapi/page/cpdf_imageobject.h"
#include "core/fpdfapi/page/cpdf_page.h"
#include "core/fpdfapi/page/cpdf_path.h"
#include "core/fpdfapi/page/cpdf_pathobject.h"
#include "core/fpdfapi/page/cpdf_textobject.h"
#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fpdfapi/parser/cpdf_name.h"
#include "core/fpdfapi/parser/cpdf_number.h"
#include "core/fpdfapi/parser/cpdf_reference.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fpdfapi/parser/fpdf_parser_decode.h"
#include "core/fpdfapi/parser/fpdf_parser_utility.h"
#include "core/fpdfapi/parser/object_tree_traversal_util.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/check_op.h"
#include "core/fxcrt/containers/contains.h"
#include "core/fxcrt/notreached.h"
#include "core/fxcrt/numerics/safe_conversions.h"
#include "core/fxcrt/span.h"

namespace {

// Key: The resource type.
// Value: The resource names of a given type.
using ResourcesMap = std::map<ByteString, std::set<ByteString>>;

// Returns whether it wrote to `buf` or not.
bool WriteColorToStream(fxcrt::ostringstream& buf, const CPDF_Color* color) {
  if (!color || (!color->IsColorSpaceRGB() && !color->IsColorSpaceGray())) {
    return false;
  }

  std::optional<FX_RGB_STRUCT<float>> colors = color->GetRGB();
  if (!colors.has_value()) {
    return false;
  }

  WriteFloat(buf, colors.value().red) << " ";
  WriteFloat(buf, colors.value().green) << " ";
  WriteFloat(buf, colors.value().blue);
  return true;
}

void RecordPageObjectResourceUsage(const CPDF_PageObject* page_object,
                                   ResourcesMap& seen_resources) {
  const ByteString& resource_name = page_object->GetResourceName();
  if (!resource_name.IsEmpty()) {
    switch (page_object->GetType()) {
      case CPDF_PageObject::Type::kText:
        seen_resources["Font"].insert(resource_name);
        break;
      case CPDF_PageObject::Type::kImage:
      case CPDF_PageObject::Type::kForm:
        seen_resources["XObject"].insert(resource_name);
        break;
      case CPDF_PageObject::Type::kPath:
        break;
      case CPDF_PageObject::Type::kShading:
        break;
    }
  }
  for (const auto& name : page_object->GetGraphicsResourceNames()) {
    CHECK(!name.IsEmpty());
    seen_resources["ExtGState"].insert(name);
  }
}

void RemoveUnusedResources(RetainPtr<CPDF_Dictionary> resources_dict,
                           const ResourcesMap& resources_in_use) {
  // TODO(thestig): Remove other unused resource types:
  // - ColorSpace
  // - Pattern
  // - Shading
  static constexpr const char* kResourceKeys[] = {"ExtGState", "Font",
                                                  "XObject"};
  for (const char* resource_key : kResourceKeys) {
    RetainPtr<CPDF_Dictionary> resource_dict =
        resources_dict->GetMutableDictFor(resource_key);
    if (!resource_dict) {
      continue;
    }

    std::vector<ByteString> keys;
    {
      CPDF_DictionaryLocker resource_dict_locker(resource_dict);
      for (auto& it : resource_dict_locker) {
        keys.push_back(it.first);
      }
    }

    auto it = resources_in_use.find(resource_key);
    const std::set<ByteString>* resource_in_use_of_current_type =
        it != resources_in_use.end() ? &it->second : nullptr;
    for (const ByteString& key : keys) {
      if (resource_in_use_of_current_type &&
          pdfium::Contains(*resource_in_use_of_current_type, key)) {
        continue;
      }

      resource_dict->RemoveFor(key.AsStringView());
    }
  }
}

}  // namespace

CPDF_PageContentGenerator::CPDF_PageContentGenerator(
    CPDF_PageObjectHolder* pObjHolder)
    : m_pObjHolder(pObjHolder), m_pDocument(pObjHolder->GetDocument()) {
  for (const auto& pObj : *pObjHolder) {
    m_pageObjects.emplace_back(pObj.get());
  }
}

CPDF_PageContentGenerator::~CPDF_PageContentGenerator() = default;

void CPDF_PageContentGenerator::GenerateContent() {
  DCHECK(m_pObjHolder->IsPage());
  std::map<int32_t, fxcrt::ostringstream> new_stream_data =
      GenerateModifiedStreams();
  // If no streams were regenerated or removed, nothing to do here.
  if (new_stream_data.empty()) {
    return;
  }

  UpdateContentStreams(std::move(new_stream_data));
  UpdateResourcesDict();
}

std::map<int32_t, fxcrt::ostringstream>
CPDF_PageContentGenerator::GenerateModifiedStreams() {
  // Figure out which streams are dirty.
  std::set<int32_t> all_dirty_streams;
  for (auto& pPageObj : m_pageObjects) {
    if (pPageObj->IsDirty())
      all_dirty_streams.insert(pPageObj->GetContentStream());
  }
  std::set<int32_t> marked_dirty_streams = m_pObjHolder->TakeDirtyStreams();
  all_dirty_streams.insert(marked_dirty_streams.begin(),
                           marked_dirty_streams.end());

  // Start regenerating dirty streams.
  std::map<int32_t, fxcrt::ostringstream> streams;
  std::set<int32_t> empty_streams;
  std::unique_ptr<const CPDF_ContentMarks> empty_content_marks =
      std::make_unique<CPDF_ContentMarks>();
  std::map<int32_t, const CPDF_ContentMarks*> current_content_marks;

  for (int32_t dirty_stream : all_dirty_streams) {
    fxcrt::ostringstream buf;

    // Set the default graphic state values. Update CTM to be the identity
    // matrix for the duration of this stream, if it is not already.
    buf << "q\n";
    const CFX_Matrix ctm =
        m_pObjHolder->GetCTMAtBeginningOfStream(dirty_stream);
    if (!ctm.IsIdentity()) {
      WriteMatrix(buf, ctm.GetInverse()) << " cm\n";
    }

    ProcessDefaultGraphics(&buf);
    streams[dirty_stream] = std::move(buf);
    empty_streams.insert(dirty_stream);
    current_content_marks[dirty_stream] = empty_content_marks.get();
  }

  // Process the page objects, write into each dirty stream.
  for (auto& pPageObj : m_pageObjects) {
    int stream_index = pPageObj->GetContentStream();
    auto it = streams.find(stream_index);
    if (it == streams.end())
      continue;

    fxcrt::ostringstream* buf = &it->second;
    empty_streams.erase(stream_index);
    current_content_marks[stream_index] =
        ProcessContentMarks(buf, pPageObj, current_content_marks[stream_index]);
    ProcessPageObject(buf, pPageObj);
  }

  // Finish dirty streams.
  for (int32_t dirty_stream : all_dirty_streams) {
    CFX_Matrix prev_ctm;
    CFX_Matrix ctm;
    bool affects_ctm;
    if (dirty_stream == 0) {
      // For the first stream, `prev_ctm` is the identity matrix.
      ctm = m_pObjHolder->GetCTMAtEndOfStream(dirty_stream);
      affects_ctm = !ctm.IsIdentity();
    } else if (dirty_stream > 0) {
      prev_ctm = m_pObjHolder->GetCTMAtEndOfStream(dirty_stream - 1);
      ctm = m_pObjHolder->GetCTMAtEndOfStream(dirty_stream);
      affects_ctm = prev_ctm != ctm;
    } else {
      CHECK_EQ(CPDF_PageObject::kNoContentStream, dirty_stream);
      // This is the last stream, so there is no subsequent stream that it can
      // affect.
      affects_ctm = false;
    }

    const bool is_empty = pdfium::Contains(empty_streams, dirty_stream);

    fxcrt::ostringstream* buf = &streams[dirty_stream];
    if (is_empty && !affects_ctm) {
      // Clear to show that this stream needs to be deleted.
      buf->str("");
      continue;
    }

    if (!is_empty) {
      FinishMarks(buf, current_content_marks[dirty_stream]);
    }

    // Return graphics to original state.
    *buf << "Q\n";

    if (affects_ctm) {
      // Update CTM so the next stream gets the expected value.
      CFX_Matrix ctm_difference = prev_ctm.GetInverse() * ctm;
      if (!ctm_difference.IsIdentity()) {
        WriteMatrix(*buf, ctm_difference) << " cm\n";
      }
    }
  }

  return streams;
}

void CPDF_PageContentGenerator::UpdateContentStreams(
    std::map<int32_t, fxcrt::ostringstream>&& new_stream_data) {
  CHECK(!new_stream_data.empty());

  // Make sure default graphics are created.
  m_DefaultGraphicsName = GetOrCreateDefaultGraphics();

  CPDF_PageContentManager page_content_manager(m_pObjHolder, m_pDocument);
  for (auto& pair : new_stream_data) {
    int32_t stream_index = pair.first;
    fxcrt::ostringstream* buf = &pair.second;

    if (stream_index == CPDF_PageObject::kNoContentStream) {
      int new_stream_index =
          pdfium::checked_cast<int>(page_content_manager.AddStream(buf));
      UpdateStreamlessPageObjects(new_stream_index);
      continue;
    }

    page_content_manager.UpdateStream(stream_index, buf);
  }
}

void CPDF_PageContentGenerator::UpdateResourcesDict() {
  RetainPtr<CPDF_Dictionary> resources = m_pObjHolder->GetMutableResources();
  if (!resources) {
    return;
  }

  const uint32_t resources_object_number = resources->GetObjNum();
  if (resources_object_number) {
    // If `resources` is not an inline object, then do not modify it directly if
    // it has multiple references.
    if (pdfium::Contains(GetObjectsWithMultipleReferences(m_pDocument),
                         resources_object_number)) {
      resources = pdfium::WrapRetain(resources->Clone()->AsMutableDictionary());
      const uint32_t clone_object_number =
          m_pDocument->AddIndirectObject(resources);
      m_pObjHolder->SetResources(resources);
      m_pObjHolder->GetMutableDict()->SetNewFor<CPDF_Reference>(
          pdfium::page_object::kResources, m_pDocument, clone_object_number);
    }
  }

  ResourcesMap seen_resources;
  for (auto& page_object : m_pageObjects) {
    RecordPageObjectResourceUsage(page_object, seen_resources);
  }
  if (!m_DefaultGraphicsName.IsEmpty()) {
    seen_resources["ExtGState"].insert(m_DefaultGraphicsName);
  }

  RemoveUnusedResources(std::move(resources), seen_resources);
}

ByteString CPDF_PageContentGenerator::RealizeResource(
    const CPDF_Object* pResource,
    const ByteString& bsType) const {
  DCHECK(pResource);
  if (!m_pObjHolder->GetResources()) {
    m_pObjHolder->SetResources(m_pDocument->NewIndirect<CPDF_Dictionary>());
    m_pObjHolder->GetMutableDict()->SetNewFor<CPDF_Reference>(
        pdfium::page_object::kResources, m_pDocument,
        m_pObjHolder->GetResources()->GetObjNum());
  }

  RetainPtr<CPDF_Dictionary> pResList =
      m_pObjHolder->GetMutableResources()->GetOrCreateDictFor(bsType);
  ByteString name;
  int idnum = 1;
  while (true) {
    name = ByteString::Format("FX%c%d", bsType[0], idnum);
    if (!pResList->KeyExist(name))
      break;

    idnum++;
  }
  pResList->SetNewFor<CPDF_Reference>(name, m_pDocument,
                                      pResource->GetObjNum());
  return name;
}

bool CPDF_PageContentGenerator::ProcessPageObjects(fxcrt::ostringstream* buf) {
  bool bDirty = false;
  std::unique_ptr<const CPDF_ContentMarks> empty_content_marks =
      std::make_unique<CPDF_ContentMarks>();
  const CPDF_ContentMarks* content_marks = empty_content_marks.get();

  for (auto& pPageObj : m_pageObjects) {
    if (m_pObjHolder->IsPage() && !pPageObj->IsDirty())
      continue;

    bDirty = true;
    content_marks = ProcessContentMarks(buf, pPageObj, content_marks);
    ProcessPageObject(buf, pPageObj);
  }
  FinishMarks(buf, content_marks);
  return bDirty;
}

void CPDF_PageContentGenerator::UpdateStreamlessPageObjects(
    int new_content_stream_index) {
  for (auto& pPageObj : m_pageObjects) {
    if (pPageObj->GetContentStream() == CPDF_PageObject::kNoContentStream)
      pPageObj->SetContentStream(new_content_stream_index);
  }
}

const CPDF_ContentMarks* CPDF_PageContentGenerator::ProcessContentMarks(
    fxcrt::ostringstream* buf,
    const CPDF_PageObject* pPageObj,
    const CPDF_ContentMarks* pPrev) {
  const CPDF_ContentMarks* pNext = pPageObj->GetContentMarks();
  const size_t first_different = pPrev->FindFirstDifference(pNext);

  // Close all marks that are in prev but not in next.
  // Technically we should iterate backwards to close from the top to the
  // bottom, but since the EMC operators do not identify which mark they are
  // closing, it does not matter.
  for (size_t i = first_different; i < pPrev->CountItems(); ++i)
    *buf << "EMC\n";

  // Open all marks that are in next but not in prev.
  for (size_t i = first_different; i < pNext->CountItems(); ++i) {
    const CPDF_ContentMarkItem* item = pNext->GetItem(i);

    // Write mark tag.
    *buf << "/" << PDF_NameEncode(item->GetName()) << " ";

    // If there are no parameters, write a BMC (begin marked content) operator.
    if (item->GetParamType() == CPDF_ContentMarkItem::kNone) {
      *buf << "BMC\n";
      continue;
    }

    // If there are parameters, write properties, direct or indirect.
    switch (item->GetParamType()) {
      case CPDF_ContentMarkItem::kDirectDict: {
        CPDF_StringArchiveStream archive_stream(buf);
        item->GetParam()->WriteTo(&archive_stream, nullptr);
        *buf << " ";
        break;
      }
      case CPDF_ContentMarkItem::kPropertiesDict: {
        *buf << "/" << item->GetPropertyName() << " ";
        break;
      }
      case CPDF_ContentMarkItem::kNone:
        NOTREACHED_NORETURN();
    }

    // Write BDC (begin dictionary content) operator.
    *buf << "BDC\n";
  }

  return pNext;
}

void CPDF_PageContentGenerator::FinishMarks(
    fxcrt::ostringstream* buf,
    const CPDF_ContentMarks* pContentMarks) {
  // Technically we should iterate backwards to close from the top to the
  // bottom, but since the EMC operators do not identify which mark they are
  // closing, it does not matter.
  for (size_t i = 0; i < pContentMarks->CountItems(); ++i)
    *buf << "EMC\n";
}

void CPDF_PageContentGenerator::ProcessPageObject(fxcrt::ostringstream* buf,
                                                  CPDF_PageObject* pPageObj) {
  if (CPDF_ImageObject* pImageObject = pPageObj->AsImage())
    ProcessImage(buf, pImageObject);
  else if (CPDF_FormObject* pFormObj = pPageObj->AsForm())
    ProcessForm(buf, pFormObj);
  else if (CPDF_PathObject* pPathObj = pPageObj->AsPath())
    ProcessPath(buf, pPathObj);
  else if (CPDF_TextObject* pTextObj = pPageObj->AsText())
    ProcessText(buf, pTextObj);
  pPageObj->SetDirty(false);
}

void CPDF_PageContentGenerator::ProcessImage(fxcrt::ostringstream* buf,
                                             CPDF_ImageObject* pImageObj) {
  const CFX_Matrix& matrix = pImageObj->matrix();
  if ((matrix.a == 0 && matrix.b == 0) || (matrix.c == 0 && matrix.d == 0)) {
    return;
  }

  RetainPtr<CPDF_Image> pImage = pImageObj->GetImage();
  if (pImage->IsInline())
    return;

  RetainPtr<const CPDF_Stream> pStream = pImage->GetStream();
  if (!pStream)
    return;

  *buf << "q ";

  if (!matrix.IsIdentity()) {
    WriteMatrix(*buf, matrix) << " cm ";
  }

  bool bWasInline = pStream->IsInline();
  if (bWasInline)
    pImage->ConvertStreamToIndirectObject();

  ByteString name = RealizeResource(pStream, "XObject");
  pImageObj->SetResourceName(name);

  if (bWasInline) {
    auto* pPageData = CPDF_DocPageData::FromDocument(m_pDocument);
    pImageObj->SetImage(pPageData->GetImage(pStream->GetObjNum()));
  }

  *buf << "/" << PDF_NameEncode(name) << " Do Q\n";
}

void CPDF_PageContentGenerator::ProcessForm(fxcrt::ostringstream* buf,
                                            CPDF_FormObject* pFormObj) {
  const CFX_Matrix& matrix = pFormObj->form_matrix();
  if ((matrix.a == 0 && matrix.b == 0) || (matrix.c == 0 && matrix.d == 0)) {
    return;
  }

  RetainPtr<const CPDF_Stream> pStream = pFormObj->form()->GetStream();
  if (!pStream)
    return;

  ByteString name = RealizeResource(pStream.Get(), "XObject");
  pFormObj->SetResourceName(name);

  *buf << "q\n";

  if (!matrix.IsIdentity()) {
    WriteMatrix(*buf, matrix) << " cm ";
  }

  *buf << "/" << PDF_NameEncode(name) << " Do Q\n";
}

// Processing path construction with operators from Table 4.9 of PDF spec 1.7:
// "re" appends a rectangle (here, used only if the whole path is a rectangle)
// "m" moves current point to the given coordinates
// "l" creates a line from current point to the new point
// "c" adds a Bezier curve from current to last point, using the two other
// points as the Bezier control points
// Note: "l", "c" change the current point
// "h" closes the subpath (appends a line from current to starting point)
void CPDF_PageContentGenerator::ProcessPathPoints(fxcrt::ostringstream* buf,
                                                  CPDF_Path* pPath) {
  pdfium::span<const CFX_Path::Point> points = pPath->GetPoints();
  if (pPath->IsRect()) {
    CFX_PointF diff = points[2].m_Point - points[0].m_Point;
    WritePoint(*buf, points[0].m_Point) << " ";
    WritePoint(*buf, diff) << " re";
    return;
  }
  for (size_t i = 0; i < points.size(); ++i) {
    if (i > 0)
      *buf << " ";

    WritePoint(*buf, points[i].m_Point);

    CFX_Path::Point::Type point_type = points[i].m_Type;
    if (point_type == CFX_Path::Point::Type::kMove) {
      *buf << " m";
    } else if (point_type == CFX_Path::Point::Type::kLine) {
      *buf << " l";
    } else if (point_type == CFX_Path::Point::Type::kBezier) {
      if (i + 2 >= points.size() ||
          !points[i].IsTypeAndOpen(CFX_Path::Point::Type::kBezier) ||
          !points[i + 1].IsTypeAndOpen(CFX_Path::Point::Type::kBezier) ||
          points[i + 2].m_Type != CFX_Path::Point::Type::kBezier) {
        // If format is not supported, close the path and paint
        *buf << " h";
        break;
      }
      *buf << " ";
      WritePoint(*buf, points[i + 1].m_Point) << " ";
      WritePoint(*buf, points[i + 2].m_Point) << " c";
      i += 2;
    }
    if (points[i].m_CloseFigure)
      *buf << " h";
  }
}

// Processing path painting with operators from Table 4.10 of PDF spec 1.7:
// Path painting operators: "S", "n", "B", "f", "B*", "f*", depending on
// the filling mode and whether we want stroking the path or not.
// "Q" restores the graphics state imposed by the ProcessGraphics method.
void CPDF_PageContentGenerator::ProcessPath(fxcrt::ostringstream* buf,
                                            CPDF_PathObject* pPathObj) {
  ProcessGraphics(buf, pPathObj);

  const CFX_Matrix& matrix = pPathObj->matrix();
  if (!matrix.IsIdentity()) {
    WriteMatrix(*buf, matrix) << " cm ";
  }

  ProcessPathPoints(buf, &pPathObj->path());

  if (pPathObj->has_no_filltype())
    *buf << (pPathObj->stroke() ? " S" : " n");
  else if (pPathObj->has_winding_filltype())
    *buf << (pPathObj->stroke() ? " B" : " f");
  else if (pPathObj->has_alternate_filltype())
    *buf << (pPathObj->stroke() ? " B*" : " f*");
  *buf << " Q\n";
}

// This method supports color operators rg and RGB from Table 4.24 of PDF spec
// 1.7. A color will not be set if the colorspace is not DefaultRGB or the RGB
// values cannot be obtained. The method also adds an external graphics
// dictionary, as described in Section 4.3.4.
// "rg" sets the fill color, "RG" sets the stroke color (using DefaultRGB)
// "w" sets the stroke line width.
// "ca" sets the fill alpha, "CA" sets the stroke alpha.
// "W" and "W*" modify the clipping path using the nonzero winding rule and
// even-odd rules, respectively.
// "q" saves the graphics state, so that the settings can later be reversed
void CPDF_PageContentGenerator::ProcessGraphics(fxcrt::ostringstream* buf,
                                                CPDF_PageObject* pPageObj) {
  *buf << "q ";
  if (WriteColorToStream(*buf, pPageObj->color_state().GetFillColor())) {
    *buf << " rg ";
  }
  if (WriteColorToStream(*buf, pPageObj->color_state().GetStrokeColor())) {
    *buf << " RG ";
  }
  float line_width = pPageObj->graph_state().GetLineWidth();
  if (line_width != 1.0f) {
    WriteFloat(*buf, line_width) << " w ";
  }
  CFX_GraphStateData::LineCap lineCap = pPageObj->graph_state().GetLineCap();
  if (lineCap != CFX_GraphStateData::LineCap::kButt)
    *buf << static_cast<int>(lineCap) << " J ";
  CFX_GraphStateData::LineJoin lineJoin = pPageObj->graph_state().GetLineJoin();
  if (lineJoin != CFX_GraphStateData::LineJoin::kMiter)
    *buf << static_cast<int>(lineJoin) << " j ";
  std::vector<float> dash_array = pPageObj->graph_state().GetLineDashArray();
  if (dash_array.size()) {
    *buf << "[";
    for (size_t i = 0; i < dash_array.size(); ++i) {
      if (i > 0) {
        *buf << " ";
      }
      WriteFloat(*buf, dash_array[i]);
    }
    *buf << "] ";
    WriteFloat(*buf, pPageObj->graph_state().GetLineDashPhase()) << " d ";
  }

  const CPDF_ClipPath& clip_path = pPageObj->clip_path();
  if (clip_path.HasRef()) {
    for (size_t i = 0; i < clip_path.GetPathCount(); ++i) {
      CPDF_Path path = clip_path.GetPath(i);
      ProcessPathPoints(buf, &path);
      switch (clip_path.GetClipType(i)) {
        case CFX_FillRenderOptions::FillType::kWinding:
          *buf << " W ";
          break;
        case CFX_FillRenderOptions::FillType::kEvenOdd:
          *buf << " W* ";
          break;
        case CFX_FillRenderOptions::FillType::kNoFill:
          NOTREACHED_NORETURN();
      }

      // Use a no-op path-painting operator to terminate the path without
      // causing any marks to be placed on the page.
      *buf << "n ";
    }
  }

  GraphicsData graphD;
  graphD.fillAlpha = pPageObj->general_state().GetFillAlpha();
  graphD.strokeAlpha = pPageObj->general_state().GetStrokeAlpha();
  graphD.blendType = pPageObj->general_state().GetBlendType();
  if (graphD.fillAlpha == 1.0f && graphD.strokeAlpha == 1.0f &&
      graphD.blendType == BlendMode::kNormal) {
    return;
  }

  ByteString name;
  std::optional<ByteString> maybe_name =
      m_pObjHolder->GraphicsMapSearch(graphD);
  if (maybe_name.has_value()) {
    name = std::move(maybe_name.value());
  } else {
    auto gsDict = pdfium::MakeRetain<CPDF_Dictionary>();
    if (graphD.fillAlpha != 1.0f)
      gsDict->SetNewFor<CPDF_Number>("ca", graphD.fillAlpha);

    if (graphD.strokeAlpha != 1.0f)
      gsDict->SetNewFor<CPDF_Number>("CA", graphD.strokeAlpha);

    if (graphD.blendType != BlendMode::kNormal) {
      gsDict->SetNewFor<CPDF_Name>("BM",
                                   pPageObj->general_state().GetBlendMode());
    }
    m_pDocument->AddIndirectObject(gsDict);
    name = RealizeResource(std::move(gsDict), "ExtGState");
    pPageObj->mutable_general_state().SetGraphicsResourceNames({name});
    m_pObjHolder->GraphicsMapInsert(graphD, name);
  }
  *buf << "/" << PDF_NameEncode(name) << " gs ";
}

void CPDF_PageContentGenerator::ProcessDefaultGraphics(
    fxcrt::ostringstream* buf) {
  *buf << "0 0 0 RG 0 0 0 rg 1 w "
       << static_cast<int>(CFX_GraphStateData::LineCap::kButt) << " J "
       << static_cast<int>(CFX_GraphStateData::LineJoin::kMiter) << " j\n";
  m_DefaultGraphicsName = GetOrCreateDefaultGraphics();
  *buf << "/" << PDF_NameEncode(m_DefaultGraphicsName) << " gs ";
}

ByteString CPDF_PageContentGenerator::GetOrCreateDefaultGraphics() const {
  GraphicsData defaultGraphics;
  defaultGraphics.fillAlpha = 1.0f;
  defaultGraphics.strokeAlpha = 1.0f;
  defaultGraphics.blendType = BlendMode::kNormal;

  std::optional<ByteString> maybe_name =
      m_pObjHolder->GraphicsMapSearch(defaultGraphics);
  if (maybe_name.has_value())
    return maybe_name.value();

  auto gsDict = pdfium::MakeRetain<CPDF_Dictionary>();
  gsDict->SetNewFor<CPDF_Number>("ca", defaultGraphics.fillAlpha);
  gsDict->SetNewFor<CPDF_Number>("CA", defaultGraphics.strokeAlpha);
  gsDict->SetNewFor<CPDF_Name>("BM", "Normal");
  m_pDocument->AddIndirectObject(gsDict);
  ByteString name = RealizeResource(std::move(gsDict), "ExtGState");
  m_pObjHolder->GraphicsMapInsert(defaultGraphics, name);
  return name;
}

// This method adds text to the buffer, BT begins the text object, ET ends it.
// Tm sets the text matrix (allows positioning and transforming text).
// Tf sets the font name (from Font in Resources) and font size.
// Tr sets the text rendering mode.
// Tj sets the actual text, <####...> is used when specifying charcodes.
void CPDF_PageContentGenerator::ProcessText(fxcrt::ostringstream* buf,
                                            CPDF_TextObject* pTextObj) {
  ProcessGraphics(buf, pTextObj);
  *buf << "BT ";

  const CFX_Matrix& matrix = pTextObj->GetTextMatrix();
  if (!matrix.IsIdentity()) {
    WriteMatrix(*buf, matrix) << " Tm ";
  }

  RetainPtr<CPDF_Font> pFont(pTextObj->GetFont());
  if (!pFont)
    pFont = CPDF_Font::GetStockFont(m_pDocument, "Helvetica");

  FontData data;
  const CPDF_FontEncoding* pEncoding = nullptr;
  if (pFont->IsType1Font()) {
    data.type = "Type1";
    pEncoding = pFont->AsType1Font()->GetEncoding();
  } else if (pFont->IsTrueTypeFont()) {
    data.type = "TrueType";
    pEncoding = pFont->AsTrueTypeFont()->GetEncoding();
  } else if (pFont->IsCIDFont()) {
    data.type = "Type0";
  } else {
    return;
  }
  data.baseFont = pFont->GetBaseFontName();

  ByteString dict_name;
  std::optional<ByteString> maybe_name = m_pObjHolder->FontsMapSearch(data);
  if (maybe_name.has_value()) {
    dict_name = std::move(maybe_name.value());
  } else {
    RetainPtr<const CPDF_Object> pIndirectFont = pFont->GetFontDict();
    if (pIndirectFont->IsInline()) {
      // In this case we assume it must be a standard font
      auto pFontDict = pdfium::MakeRetain<CPDF_Dictionary>();
      pFontDict->SetNewFor<CPDF_Name>("Type", "Font");
      pFontDict->SetNewFor<CPDF_Name>("Subtype", data.type);
      pFontDict->SetNewFor<CPDF_Name>("BaseFont", data.baseFont);
      if (pEncoding) {
        pFontDict->SetFor("Encoding",
                          pEncoding->Realize(m_pDocument->GetByteStringPool()));
      }
      m_pDocument->AddIndirectObject(pFontDict);
      pIndirectFont = std::move(pFontDict);
    }
    dict_name = RealizeResource(std::move(pIndirectFont), "Font");
    m_pObjHolder->FontsMapInsert(data, dict_name);
  }
  pTextObj->SetResourceName(dict_name);

  *buf << "/" << PDF_NameEncode(dict_name) << " ";
  WriteFloat(*buf, pTextObj->GetFontSize()) << " Tf ";
  *buf << static_cast<int>(pTextObj->GetTextRenderMode()) << " Tr ";
  ByteString text;
  for (uint32_t charcode : pTextObj->GetCharCodes()) {
    if (charcode != CPDF_Font::kInvalidCharCode) {
      pFont->AppendChar(&text, charcode);
    }
  }
  *buf << PDF_HexEncodeString(text.AsStringView()) << " Tj ET";
  *buf << " Q\n";
}
