// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "public/fpdf_edit.h"

#include <memory>
#include <utility>

#include "core/fpdfapi/page/cpdf_path.h"
#include "core/fpdfapi/page/cpdf_pathobject.h"
#include "core/fxcrt/fx_system.h"
#include "core/fxcrt/span.h"
#include "core/fxcrt/stl_util.h"
#include "fpdfsdk/cpdfsdk_helpers.h"

// These checks are here because core/ and public/ cannot depend on each other.
static_assert(static_cast<int>(CFX_GraphStateData::LineCap::kButt) ==
                  FPDF_LINECAP_BUTT,
              "CFX_GraphStateData::LineCap::kButt value mismatch");
static_assert(static_cast<int>(CFX_GraphStateData::LineCap::kRound) ==
                  FPDF_LINECAP_ROUND,
              "CFX_GraphStateData::LineCap::kRound value mismatch");
static_assert(static_cast<int>(CFX_GraphStateData::LineCap::kSquare) ==
                  FPDF_LINECAP_PROJECTING_SQUARE,
              "CFX_GraphStateData::LineCap::kSquare value mismatch");

static_assert(static_cast<int>(CFX_GraphStateData::LineJoin::kMiter) ==
                  FPDF_LINEJOIN_MITER,
              "CFX_GraphStateData::LineJoin::kMiter value mismatch");
static_assert(static_cast<int>(CFX_GraphStateData::LineJoin::kRound) ==
                  FPDF_LINEJOIN_ROUND,
              "CFX_GraphStateData::LineJoin::kRound value mismatch");
static_assert(static_cast<int>(CFX_GraphStateData::LineJoin::kBevel) ==
                  FPDF_LINEJOIN_BEVEL,
              "CFX_GraphStateData::LineJoin::kBevel value mismatch");

static_assert(static_cast<int>(CFX_Path::Point::Type::kLine) ==
                  FPDF_SEGMENT_LINETO,
              "CFX_Path::Point::Type::kLine value mismatch");
static_assert(static_cast<int>(CFX_Path::Point::Type::kBezier) ==
                  FPDF_SEGMENT_BEZIERTO,
              "CFX_Path::Point::Type::kBezier value mismatch");
static_assert(static_cast<int>(CFX_Path::Point::Type::kMove) ==
                  FPDF_SEGMENT_MOVETO,
              "CFX_Path::Point::Type::kMove value mismatch");

namespace {

CPDF_PathObject* CPDFPathObjectFromFPDFPageObject(FPDF_PAGEOBJECT page_object) {
  auto* obj = CPDFPageObjectFromFPDFPageObject(page_object);
  return obj ? obj->AsPath() : nullptr;
}

}  // namespace

FPDF_EXPORT FPDF_PAGEOBJECT FPDF_CALLCONV FPDFPageObj_CreateNewPath(float x,
                                                                    float y) {
  auto pPathObj = std::make_unique<CPDF_PathObject>();
  pPathObj->path().AppendPoint(CFX_PointF(x, y), CFX_Path::Point::Type::kMove);
  pPathObj->SetDefaultStates();

  // Caller takes ownership.
  return FPDFPageObjectFromCPDFPageObject(pPathObj.release());
}

FPDF_EXPORT FPDF_PAGEOBJECT FPDF_CALLCONV FPDFPageObj_CreateNewRect(float x,
                                                                    float y,
                                                                    float w,
                                                                    float h) {
  auto pPathObj = std::make_unique<CPDF_PathObject>();
  pPathObj->path().AppendRect(x, y, x + w, y + h);
  pPathObj->SetDefaultStates();

  // Caller takes ownership.
  return FPDFPageObjectFromCPDFPageObject(pPathObj.release());
}

FPDF_EXPORT int FPDF_CALLCONV FPDFPath_CountSegments(FPDF_PAGEOBJECT path) {
  auto* pPathObj = CPDFPathObjectFromFPDFPageObject(path);
  if (!pPathObj)
    return -1;
  return fxcrt::CollectionSize<int>(pPathObj->path().GetPoints());
}

FPDF_EXPORT FPDF_PATHSEGMENT FPDF_CALLCONV
FPDFPath_GetPathSegment(FPDF_PAGEOBJECT path, int index) {
  auto* pPathObj = CPDFPathObjectFromFPDFPageObject(path);
  if (!pPathObj)
    return nullptr;

  pdfium::span<const CFX_Path::Point> points = pPathObj->path().GetPoints();
  if (!fxcrt::IndexInBounds(points, index))
    return nullptr;

  return FPDFPathSegmentFromFXPathPoint(&points[index]);
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDFPath_MoveTo(FPDF_PAGEOBJECT path,
                                                    float x,
                                                    float y) {
  auto* pPathObj = CPDFPathObjectFromFPDFPageObject(path);
  if (!pPathObj)
    return false;

  pPathObj->path().AppendPoint(CFX_PointF(x, y), CFX_Path::Point::Type::kMove);
  pPathObj->SetDirty(true);
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDFPath_LineTo(FPDF_PAGEOBJECT path,
                                                    float x,
                                                    float y) {
  auto* pPathObj = CPDFPathObjectFromFPDFPageObject(path);
  if (!pPathObj)
    return false;

  pPathObj->path().AppendPoint(CFX_PointF(x, y), CFX_Path::Point::Type::kLine);
  pPathObj->SetDirty(true);
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDFPath_BezierTo(FPDF_PAGEOBJECT path,
                                                      float x1,
                                                      float y1,
                                                      float x2,
                                                      float y2,
                                                      float x3,
                                                      float y3) {
  auto* pPathObj = CPDFPathObjectFromFPDFPageObject(path);
  if (!pPathObj)
    return false;

  CPDF_Path& cpath = pPathObj->path();
  cpath.AppendPoint(CFX_PointF(x1, y1), CFX_Path::Point::Type::kBezier);
  cpath.AppendPoint(CFX_PointF(x2, y2), CFX_Path::Point::Type::kBezier);
  cpath.AppendPoint(CFX_PointF(x3, y3), CFX_Path::Point::Type::kBezier);
  pPathObj->SetDirty(true);
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDFPath_Close(FPDF_PAGEOBJECT path) {
  auto* pPathObj = CPDFPathObjectFromFPDFPageObject(path);
  if (!pPathObj)
    return false;

  CPDF_Path& cpath = pPathObj->path();
  if (cpath.GetPoints().empty())
    return false;

  cpath.ClosePath();
  pPathObj->SetDirty(true);
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDFPath_SetDrawMode(FPDF_PAGEOBJECT path,
                                                         int fillmode,
                                                         FPDF_BOOL stroke) {
  auto* pPathObj = CPDFPathObjectFromFPDFPageObject(path);
  if (!pPathObj)
    return false;

  pPathObj->set_stroke(!!stroke);
  if (fillmode == FPDF_FILLMODE_ALTERNATE)
    pPathObj->set_alternate_filltype();
  else if (fillmode == FPDF_FILLMODE_WINDING)
    pPathObj->set_winding_filltype();
  else
    pPathObj->set_no_filltype();
  pPathObj->SetDirty(true);
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDFPath_GetDrawMode(FPDF_PAGEOBJECT path,
                                                         int* fillmode,
                                                         FPDF_BOOL* stroke) {
  auto* pPathObj = CPDFPathObjectFromFPDFPageObject(path);
  if (!pPathObj || !fillmode || !stroke)
    return false;

  if (pPathObj->has_alternate_filltype())
    *fillmode = FPDF_FILLMODE_ALTERNATE;
  else if (pPathObj->has_winding_filltype())
    *fillmode = FPDF_FILLMODE_WINDING;
  else
    *fillmode = FPDF_FILLMODE_NONE;

  *stroke = pPathObj->stroke();
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFPathSegment_GetPoint(FPDF_PATHSEGMENT segment, float* x, float* y) {
  auto* pPathPoint = FXPathPointFromFPDFPathSegment(segment);
  if (!pPathPoint || !x || !y)
    return false;

  *x = pPathPoint->m_Point.x;
  *y = pPathPoint->m_Point.y;
  return true;
}

FPDF_EXPORT int FPDF_CALLCONV
FPDFPathSegment_GetType(FPDF_PATHSEGMENT segment) {
  auto* pPathPoint = FXPathPointFromFPDFPathSegment(segment);
  return pPathPoint ? static_cast<int>(pPathPoint->m_Type)
                    : FPDF_SEGMENT_UNKNOWN;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFPathSegment_GetClose(FPDF_PATHSEGMENT segment) {
  auto* pPathPoint = FXPathPointFromFPDFPathSegment(segment);
  return pPathPoint && pPathPoint->m_CloseFigure;
}
