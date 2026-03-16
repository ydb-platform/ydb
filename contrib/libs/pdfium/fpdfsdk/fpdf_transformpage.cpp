// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "public/fpdf_transformpage.h"

#include <memory>
#include <sstream>

#include "constants/page_object.h"
#include "core/fpdfapi/edit/cpdf_contentstream_write_utils.h"
#include "core/fpdfapi/page/cpdf_clippath.h"
#include "core/fpdfapi/page/cpdf_page.h"
#include "core/fpdfapi/page/cpdf_pageobject.h"
#include "core/fpdfapi/page/cpdf_path.h"
#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fpdfapi/parser/cpdf_number.h"
#include "core/fpdfapi/parser/cpdf_reference.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fxcrt/fx_string_wrappers.h"
#include "core/fxcrt/numerics/safe_conversions.h"
#include "core/fxcrt/span.h"
#include "core/fxcrt/stl_util.h"
#include "core/fxge/cfx_fillrenderoptions.h"
#include "core/fxge/cfx_path.h"
#include "fpdfsdk/cpdfsdk_helpers.h"

namespace {

void SetBoundingBox(CPDF_Page* page,
                    const ByteString& key,
                    const CFX_FloatRect& rect) {
  if (!page)
    return;

  page->GetMutableDict()->SetRectFor(key, rect);
  page->UpdateDimensions();
}

bool GetBoundingBox(const CPDF_Page* page,
                    const ByteString& key,
                    float* left,
                    float* bottom,
                    float* right,
                    float* top) {
  if (!page || !left || !bottom || !right || !top)
    return false;

  RetainPtr<const CPDF_Array> pArray = page->GetDict()->GetArrayFor(key);
  if (!pArray)
    return false;

  *left = pArray->GetFloatAt(0);
  *bottom = pArray->GetFloatAt(1);
  *right = pArray->GetFloatAt(2);
  *top = pArray->GetFloatAt(3);
  return true;
}

RetainPtr<CPDF_Object> GetPageContent(CPDF_Dictionary* pPageDict) {
  return pPageDict->GetMutableDirectObjectFor(pdfium::page_object::kContents);
}

void OutputPath(fxcrt::ostringstream& buf, CPDF_Path path) {
  const CFX_Path* pPath = path.GetObject();
  if (!pPath)
    return;

  pdfium::span<const CFX_Path::Point> points = pPath->GetPoints();
  if (path.IsRect()) {
    CFX_PointF diff = points[2].m_Point - points[0].m_Point;
    buf << points[0].m_Point.x << " " << points[0].m_Point.y << " " << diff.x
        << " " << diff.y << " re\n";
    return;
  }

  for (size_t i = 0; i < points.size(); ++i) {
    buf << points[i].m_Point.x << " " << points[i].m_Point.y;
    CFX_Path::Point::Type point_type = points[i].m_Type;
    if (point_type == CFX_Path::Point::Type::kMove) {
      buf << " m\n";
    } else if (point_type == CFX_Path::Point::Type::kBezier) {
      buf << " " << points[i + 1].m_Point.x << " " << points[i + 1].m_Point.y
          << " " << points[i + 2].m_Point.x << " " << points[i + 2].m_Point.y;
      buf << " c";
      if (points[i + 2].m_CloseFigure)
        buf << " h";
      buf << "\n";

      i += 2;
    } else if (point_type == CFX_Path::Point::Type::kLine) {
      buf << " l";
      if (points[i].m_CloseFigure)
        buf << " h";
      buf << "\n";
    }
  }
}

}  // namespace

FPDF_EXPORT void FPDF_CALLCONV FPDFPage_SetMediaBox(FPDF_PAGE page,
                                                    float left,
                                                    float bottom,
                                                    float right,
                                                    float top) {
  SetBoundingBox(CPDFPageFromFPDFPage(page), pdfium::page_object::kMediaBox,
                 CFX_FloatRect(left, bottom, right, top));
}

FPDF_EXPORT void FPDF_CALLCONV FPDFPage_SetCropBox(FPDF_PAGE page,
                                                   float left,
                                                   float bottom,
                                                   float right,
                                                   float top) {
  SetBoundingBox(CPDFPageFromFPDFPage(page), pdfium::page_object::kCropBox,
                 CFX_FloatRect(left, bottom, right, top));
}

FPDF_EXPORT void FPDF_CALLCONV FPDFPage_SetBleedBox(FPDF_PAGE page,
                                                    float left,
                                                    float bottom,
                                                    float right,
                                                    float top) {
  SetBoundingBox(CPDFPageFromFPDFPage(page), pdfium::page_object::kBleedBox,
                 CFX_FloatRect(left, bottom, right, top));
}

FPDF_EXPORT void FPDF_CALLCONV FPDFPage_SetTrimBox(FPDF_PAGE page,
                                                   float left,
                                                   float bottom,
                                                   float right,
                                                   float top) {
  SetBoundingBox(CPDFPageFromFPDFPage(page), pdfium::page_object::kTrimBox,
                 CFX_FloatRect(left, bottom, right, top));
}

FPDF_EXPORT void FPDF_CALLCONV FPDFPage_SetArtBox(FPDF_PAGE page,
                                                  float left,
                                                  float bottom,
                                                  float right,
                                                  float top) {
  SetBoundingBox(CPDFPageFromFPDFPage(page), pdfium::page_object::kArtBox,
                 CFX_FloatRect(left, bottom, right, top));
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDFPage_GetMediaBox(FPDF_PAGE page,
                                                         float* left,
                                                         float* bottom,
                                                         float* right,
                                                         float* top) {
  return GetBoundingBox(CPDFPageFromFPDFPage(page),
                        pdfium::page_object::kMediaBox, left, bottom, right,
                        top);
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDFPage_GetCropBox(FPDF_PAGE page,
                                                        float* left,
                                                        float* bottom,
                                                        float* right,
                                                        float* top) {
  return GetBoundingBox(CPDFPageFromFPDFPage(page),
                        pdfium::page_object::kCropBox, left, bottom, right,
                        top);
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDFPage_GetBleedBox(FPDF_PAGE page,
                                                         float* left,
                                                         float* bottom,
                                                         float* right,
                                                         float* top) {
  return GetBoundingBox(CPDFPageFromFPDFPage(page),
                        pdfium::page_object::kBleedBox, left, bottom, right,
                        top);
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDFPage_GetTrimBox(FPDF_PAGE page,
                                                        float* left,
                                                        float* bottom,
                                                        float* right,
                                                        float* top) {
  return GetBoundingBox(CPDFPageFromFPDFPage(page),
                        pdfium::page_object::kTrimBox, left, bottom, right,
                        top);
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDFPage_GetArtBox(FPDF_PAGE page,
                                                       float* left,
                                                       float* bottom,
                                                       float* right,
                                                       float* top) {
  return GetBoundingBox(CPDFPageFromFPDFPage(page),
                        pdfium::page_object::kArtBox, left, bottom, right, top);
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFPage_TransFormWithClip(FPDF_PAGE page,
                           const FS_MATRIX* matrix,
                           const FS_RECTF* clipRect) {
  if (!matrix && !clipRect)
    return false;

  CPDF_Page* pPage = CPDFPageFromFPDFPage(page);
  if (!pPage)
    return false;

  RetainPtr<CPDF_Dictionary> pPageDict = pPage->GetMutableDict();
  RetainPtr<CPDF_Object> pContentObj = GetPageContent(pPageDict.Get());
  if (!pContentObj)
    return false;

  CPDF_Document* pDoc = pPage->GetDocument();
  if (!pDoc)
    return false;

  fxcrt::ostringstream text_buf;
  text_buf << "q ";

  if (clipRect) {
    CFX_FloatRect rect = CFXFloatRectFromFSRectF(*clipRect);
    rect.Normalize();
    WriteRect(text_buf, rect) << " re W* n ";
  }
  if (matrix)
    WriteMatrix(text_buf, CFXMatrixFromFSMatrix(*matrix)) << " cm ";

  auto pStream = pDoc->NewIndirect<CPDF_Stream>(pDoc->New<CPDF_Dictionary>());
  pStream->SetDataFromStringstream(&text_buf);

  auto pEndStream =
      pDoc->NewIndirect<CPDF_Stream>(pDoc->New<CPDF_Dictionary>());
  pEndStream->SetData(ByteStringView(" Q").unsigned_span());

  RetainPtr<CPDF_Array> pContentArray = ToArray(pContentObj);
  if (pContentArray) {
    pContentArray->InsertNewAt<CPDF_Reference>(0, pDoc, pStream->GetObjNum());
    pContentArray->AppendNew<CPDF_Reference>(pDoc, pEndStream->GetObjNum());
  } else if (pContentObj->IsStream() && !pContentObj->IsInline()) {
    pContentArray = pDoc->NewIndirect<CPDF_Array>();
    pContentArray->AppendNew<CPDF_Reference>(pDoc, pStream->GetObjNum());
    pContentArray->AppendNew<CPDF_Reference>(pDoc, pContentObj->GetObjNum());
    pContentArray->AppendNew<CPDF_Reference>(pDoc, pEndStream->GetObjNum());
    pPageDict->SetNewFor<CPDF_Reference>(pdfium::page_object::kContents, pDoc,
                                         pContentArray->GetObjNum());
  }

  // Need to transform the patterns as well.
  RetainPtr<const CPDF_Dictionary> pRes =
      pPageDict->GetDictFor(pdfium::page_object::kResources);
  if (!pRes)
    return true;

  RetainPtr<const CPDF_Dictionary> pPatternDict = pRes->GetDictFor("Pattern");
  if (!pPatternDict)
    return true;

  CPDF_DictionaryLocker locker(pPatternDict);
  for (const auto& it : locker) {
    RetainPtr<CPDF_Object> pObj = it.second;
    if (pObj->IsReference())
      pObj = pObj->GetMutableDirect();

    RetainPtr<CPDF_Dictionary> pDict;
    if (pObj->IsDictionary())
      pDict.Reset(pObj->AsMutableDictionary());
    else if (CPDF_Stream* pObjStream = pObj->AsMutableStream())
      pDict = pObjStream->GetMutableDict();
    else
      continue;

    if (matrix) {
      CFX_Matrix m = CFXMatrixFromFSMatrix(*matrix);
      pDict->SetMatrixFor("Matrix", pDict->GetMatrixFor("Matrix") * m);
    }
  }

  return true;
}

FPDF_EXPORT void FPDF_CALLCONV
FPDFPageObj_TransformClipPath(FPDF_PAGEOBJECT page_object,
                              double a,
                              double b,
                              double c,
                              double d,
                              double e,
                              double f) {
  CPDF_PageObject* pPageObj = CPDFPageObjectFromFPDFPageObject(page_object);
  if (!pPageObj)
    return;

  CFX_Matrix matrix((float)a, (float)b, (float)c, (float)d, (float)e, (float)f);

  // Special treatment to shading object, because the ClipPath for shading
  // object is already transformed.
  if (!pPageObj->IsShading())
    pPageObj->TransformClipPath(matrix);
}

FPDF_EXPORT FPDF_CLIPPATH FPDF_CALLCONV
FPDFPageObj_GetClipPath(FPDF_PAGEOBJECT page_object) {
  CPDF_PageObject* pPageObj = CPDFPageObjectFromFPDFPageObject(page_object);
  if (!pPageObj)
    return nullptr;

  return FPDFClipPathFromCPDFClipPath(&pPageObj->mutable_clip_path());
}

FPDF_EXPORT int FPDF_CALLCONV FPDFClipPath_CountPaths(FPDF_CLIPPATH clip_path) {
  CPDF_ClipPath* pClipPath = CPDFClipPathFromFPDFClipPath(clip_path);
  if (!pClipPath || !pClipPath->HasRef())
    return -1;

  return pdfium::checked_cast<int>(pClipPath->GetPathCount());
}

FPDF_EXPORT int FPDF_CALLCONV
FPDFClipPath_CountPathSegments(FPDF_CLIPPATH clip_path, int path_index) {
  CPDF_ClipPath* pClipPath = CPDFClipPathFromFPDFClipPath(clip_path);
  if (!pClipPath || !pClipPath->HasRef())
    return -1;

  if (path_index < 0 ||
      static_cast<size_t>(path_index) >= pClipPath->GetPathCount()) {
    return -1;
  }

  return fxcrt::CollectionSize<int>(pClipPath->GetPath(path_index).GetPoints());
}

FPDF_EXPORT FPDF_PATHSEGMENT FPDF_CALLCONV
FPDFClipPath_GetPathSegment(FPDF_CLIPPATH clip_path,
                            int path_index,
                            int segment_index) {
  CPDF_ClipPath* pClipPath = CPDFClipPathFromFPDFClipPath(clip_path);
  if (!pClipPath || !pClipPath->HasRef())
    return nullptr;

  if (path_index < 0 ||
      static_cast<size_t>(path_index) >= pClipPath->GetPathCount()) {
    return nullptr;
  }

  pdfium::span<const CFX_Path::Point> points =
      pClipPath->GetPath(path_index).GetPoints();
  if (!fxcrt::IndexInBounds(points, segment_index))
    return nullptr;

  return FPDFPathSegmentFromFXPathPoint(&points[segment_index]);
}

FPDF_EXPORT FPDF_CLIPPATH FPDF_CALLCONV FPDF_CreateClipPath(float left,
                                                            float bottom,
                                                            float right,
                                                            float top) {
  CPDF_Path Path;
  Path.AppendRect(left, bottom, right, top);

  auto pNewClipPath = std::make_unique<CPDF_ClipPath>();
  pNewClipPath->AppendPath(Path, CFX_FillRenderOptions::FillType::kEvenOdd);

  // Caller takes ownership.
  return FPDFClipPathFromCPDFClipPath(pNewClipPath.release());
}

FPDF_EXPORT void FPDF_CALLCONV FPDF_DestroyClipPath(FPDF_CLIPPATH clipPath) {
  // Take ownership back from caller and destroy.
  std::unique_ptr<CPDF_ClipPath>(CPDFClipPathFromFPDFClipPath(clipPath));
}

FPDF_EXPORT void FPDF_CALLCONV FPDFPage_InsertClipPath(FPDF_PAGE page,
                                                       FPDF_CLIPPATH clipPath) {
  CPDF_Page* pPage = CPDFPageFromFPDFPage(page);
  if (!pPage)
    return;

  RetainPtr<CPDF_Dictionary> pPageDict = pPage->GetMutableDict();
  RetainPtr<CPDF_Object> pContentObj = GetPageContent(pPageDict.Get());
  if (!pContentObj)
    return;

  fxcrt::ostringstream strClip;
  CPDF_ClipPath* pClipPath = CPDFClipPathFromFPDFClipPath(clipPath);
  for (size_t i = 0; i < pClipPath->GetPathCount(); ++i) {
    CPDF_Path path = pClipPath->GetPath(i);
    if (path.GetPoints().empty()) {
      // Empty clipping (totally clipped out)
      strClip << "0 0 m W n ";
    } else {
      OutputPath(strClip, path);
      if (pClipPath->GetClipType(i) ==
          CFX_FillRenderOptions::FillType::kWinding) {
        strClip << "W n\n";
      } else {
        strClip << "W* n\n";
      }
    }
  }
  CPDF_Document* pDoc = pPage->GetDocument();
  if (!pDoc)
    return;

  auto pStream = pDoc->NewIndirect<CPDF_Stream>(pDoc->New<CPDF_Dictionary>());
  pStream->SetDataFromStringstream(&strClip);

  RetainPtr<CPDF_Array> pArray = ToArray(pContentObj);
  if (pArray) {
    pArray->InsertNewAt<CPDF_Reference>(0, pDoc, pStream->GetObjNum());
  } else if (pContentObj->IsStream() && !pContentObj->IsInline()) {
    auto pContentArray = pDoc->NewIndirect<CPDF_Array>();
    pContentArray->AppendNew<CPDF_Reference>(pDoc, pStream->GetObjNum());
    pContentArray->AppendNew<CPDF_Reference>(pDoc, pContentObj->GetObjNum());
    pPageDict->SetNewFor<CPDF_Reference>(pdfium::page_object::kContents, pDoc,
                                         pContentArray->GetObjNum());
  }
}
