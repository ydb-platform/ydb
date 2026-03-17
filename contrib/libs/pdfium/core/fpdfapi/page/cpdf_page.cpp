// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/page/cpdf_page.h"

#include <set>
#include <utility>

#include "constants/page_object.h"
#include "core/fpdfapi/page/cpdf_contentparser.h"
#include "core/fpdfapi/page/cpdf_pageimagecache.h"
#include "core/fpdfapi/page/cpdf_pageobject.h"
#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_object.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/check_op.h"
#include "core/fxcrt/containers/contains.h"

CPDF_Page::CPDF_Page(CPDF_Document* pDocument,
                     RetainPtr<CPDF_Dictionary> pPageDict)
    : CPDF_PageObjectHolder(pDocument, std::move(pPageDict), nullptr, nullptr),
      m_PageSize(100, 100),
      m_pPDFDocument(pDocument) {
  // Cannot initialize |m_pResources| and |m_pPageResources| via the
  // CPDF_PageObjectHolder ctor because GetPageAttr() requires
  // CPDF_PageObjectHolder to finish initializing first.
  RetainPtr<CPDF_Object> pPageAttr =
      GetMutablePageAttr(pdfium::page_object::kResources);
  m_pResources = pPageAttr ? pPageAttr->GetMutableDict() : nullptr;
  m_pPageResources = m_pResources;

  UpdateDimensions();
  m_Transparency.SetIsolated();
  LoadTransparencyInfo();
}

CPDF_Page::~CPDF_Page() = default;

CPDF_Page* CPDF_Page::AsPDFPage() {
  return this;
}

CPDFXFA_Page* CPDF_Page::AsXFAPage() {
  return nullptr;
}

CPDF_Document* CPDF_Page::GetDocument() const {
  return m_pPDFDocument;
}

float CPDF_Page::GetPageWidth() const {
  return m_PageSize.width;
}

float CPDF_Page::GetPageHeight() const {
  return m_PageSize.height;
}

bool CPDF_Page::IsPage() const {
  return true;
}

void CPDF_Page::ParseContent() {
  if (GetParseState() == ParseState::kParsed)
    return;

  if (GetParseState() == ParseState::kNotParsed)
    StartParse(std::make_unique<CPDF_ContentParser>(this));

  DCHECK_EQ(GetParseState(), ParseState::kParsing);
  ContinueParse(nullptr);
}

RetainPtr<CPDF_Object> CPDF_Page::GetMutablePageAttr(const ByteString& name) {
  return pdfium::WrapRetain(const_cast<CPDF_Object*>(GetPageAttr(name).Get()));
}

RetainPtr<const CPDF_Object> CPDF_Page::GetPageAttr(
    const ByteString& name) const {
  std::set<RetainPtr<const CPDF_Dictionary>> visited;
  RetainPtr<const CPDF_Dictionary> pPageDict = GetDict();
  while (pPageDict && !pdfium::Contains(visited, pPageDict)) {
    RetainPtr<const CPDF_Object> pObj = pPageDict->GetDirectObjectFor(name);
    if (pObj)
      return pObj;

    visited.insert(pPageDict);
    pPageDict = pPageDict->GetDictFor(pdfium::page_object::kParent);
  }
  return nullptr;
}

CFX_FloatRect CPDF_Page::GetBox(const ByteString& name) const {
  CFX_FloatRect box;
  RetainPtr<const CPDF_Array> pBox = ToArray(GetPageAttr(name));
  if (pBox) {
    box = pBox->GetRect();
    box.Normalize();
  }
  return box;
}

std::optional<CFX_PointF> CPDF_Page::DeviceToPage(
    const FX_RECT& rect,
    int rotate,
    const CFX_PointF& device_point) const {
  CFX_Matrix page2device = GetDisplayMatrix(rect, rotate);
  return page2device.GetInverse().Transform(device_point);
}

std::optional<CFX_PointF> CPDF_Page::PageToDevice(
    const FX_RECT& rect,
    int rotate,
    const CFX_PointF& page_point) const {
  CFX_Matrix page2device = GetDisplayMatrix(rect, rotate);
  return page2device.Transform(page_point);
}

CFX_Matrix CPDF_Page::GetDisplayMatrix(const FX_RECT& rect, int iRotate) const {
  if (m_PageSize.width == 0 || m_PageSize.height == 0)
    return CFX_Matrix();

  float x0 = 0;
  float y0 = 0;
  float x1 = 0;
  float y1 = 0;
  float x2 = 0;
  float y2 = 0;
  iRotate %= 4;
  // This code implicitly inverts the y-axis to account for page coordinates
  // pointing up and bitmap coordinates pointing down. (x0, y0) is the base
  // point, (x1, y1) is that point translated on y and (x2, y2) is the point
  // translated on x. On iRotate = 0, y0 is rect.bottom and the translation
  // to get y1 is performed as negative. This results in the desired
  // transformation.
  switch (iRotate) {
    case 0:
      x0 = rect.left;
      y0 = rect.bottom;
      x1 = rect.left;
      y1 = rect.top;
      x2 = rect.right;
      y2 = rect.bottom;
      break;
    case 1:
      x0 = rect.left;
      y0 = rect.top;
      x1 = rect.right;
      y1 = rect.top;
      x2 = rect.left;
      y2 = rect.bottom;
      break;
    case 2:
      x0 = rect.right;
      y0 = rect.top;
      x1 = rect.right;
      y1 = rect.bottom;
      x2 = rect.left;
      y2 = rect.top;
      break;
    case 3:
      x0 = rect.right;
      y0 = rect.bottom;
      x1 = rect.left;
      y1 = rect.bottom;
      x2 = rect.right;
      y2 = rect.top;
      break;
  }
  CFX_Matrix matrix((x2 - x0) / m_PageSize.width, (y2 - y0) / m_PageSize.width,
                    (x1 - x0) / m_PageSize.height,
                    (y1 - y0) / m_PageSize.height, x0, y0);
  return m_PageMatrix * matrix;
}

int CPDF_Page::GetPageRotation() const {
  RetainPtr<const CPDF_Object> pRotate =
      GetPageAttr(pdfium::page_object::kRotate);
  int rotate = pRotate ? (pRotate->GetInteger() / 90) % 4 : 0;
  return (rotate < 0) ? (rotate + 4) : rotate;
}

RetainPtr<CPDF_Array> CPDF_Page::GetOrCreateAnnotsArray() {
  return GetMutableDict()->GetOrCreateArrayFor("Annots");
}

RetainPtr<CPDF_Array> CPDF_Page::GetMutableAnnotsArray() {
  return GetMutableDict()->GetMutableArrayFor("Annots");
}

RetainPtr<const CPDF_Array> CPDF_Page::GetAnnotsArray() const {
  return GetDict()->GetArrayFor("Annots");
}

void CPDF_Page::AddPageImageCache() {
  m_pPageImageCache = std::make_unique<CPDF_PageImageCache>(this);
}

void CPDF_Page::SetRenderContext(std::unique_ptr<RenderContextIface> pContext) {
  DCHECK(!m_pRenderContext);
  DCHECK(pContext);
  m_pRenderContext = std::move(pContext);
}

void CPDF_Page::ClearRenderContext() {
  m_pRenderContext.reset();
}

void CPDF_Page::ClearView() {
  if (m_pView)
    m_pView->ClearPage(this);
}

void CPDF_Page::UpdateDimensions() {
  CFX_FloatRect mediabox = GetBox(pdfium::page_object::kMediaBox);
  if (mediabox.IsEmpty())
    mediabox = CFX_FloatRect(0, 0, 612, 792);

  m_BBox = GetBox(pdfium::page_object::kCropBox);
  if (m_BBox.IsEmpty())
    m_BBox = mediabox;
  else
    m_BBox.Intersect(mediabox);

  m_PageSize.width = m_BBox.Width();
  m_PageSize.height = m_BBox.Height();

  switch (GetPageRotation()) {
    case 0:
      m_PageMatrix = CFX_Matrix(1.0f, 0, 0, 1.0f, -m_BBox.left, -m_BBox.bottom);
      break;
    case 1:
      std::swap(m_PageSize.width, m_PageSize.height);
      m_PageMatrix =
          CFX_Matrix(0, -1.0f, 1.0f, 0, -m_BBox.bottom, m_BBox.right);
      break;
    case 2:
      m_PageMatrix = CFX_Matrix(-1.0f, 0, 0, -1.0f, m_BBox.right, m_BBox.top);
      break;
    case 3:
      std::swap(m_PageSize.width, m_PageSize.height);
      m_PageMatrix = CFX_Matrix(0, 1.0f, -1.0f, 0, m_BBox.top, -m_BBox.left);
      break;
  }
}

CPDF_Page::RenderContextClearer::RenderContextClearer(CPDF_Page* pPage)
    : m_pPage(pPage) {}

CPDF_Page::RenderContextClearer::~RenderContextClearer() {
  if (m_pPage)
    m_pPage->ClearRenderContext();
}
