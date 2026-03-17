// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/page/cpdf_imageobject.h"

#include <utility>

#include "core/fpdfapi/page/cpdf_docpagedata.h"
#include "core/fpdfapi/page/cpdf_image.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fxcrt/fx_coordinates.h"
#include "core/fxge/dib/cfx_dibbase.h"
#include "core/fxge/dib/cfx_dibitmap.h"

CPDF_ImageObject::CPDF_ImageObject(int32_t content_stream)
    : CPDF_PageObject(content_stream) {}

CPDF_ImageObject::CPDF_ImageObject() : CPDF_ImageObject(kNoContentStream) {}

CPDF_ImageObject::~CPDF_ImageObject() {
  MaybePurgeCache();
}

CPDF_PageObject::Type CPDF_ImageObject::GetType() const {
  return Type::kImage;
}

void CPDF_ImageObject::Transform(const CFX_Matrix& matrix) {
  m_Matrix.Concat(matrix);
  CalcBoundingBox();
  SetDirty(true);
}

bool CPDF_ImageObject::IsImage() const {
  return true;
}

CPDF_ImageObject* CPDF_ImageObject::AsImage() {
  return this;
}

const CPDF_ImageObject* CPDF_ImageObject::AsImage() const {
  return this;
}

void CPDF_ImageObject::CalcBoundingBox() {
  static constexpr CFX_FloatRect kRect(0.0f, 0.0f, 1.0f, 1.0f);
  SetOriginalRect(kRect);
  SetRect(m_Matrix.TransformRect(kRect));
}

void CPDF_ImageObject::SetImage(RetainPtr<CPDF_Image> pImage) {
  MaybePurgeCache();
  m_pImage = std::move(pImage);
}

RetainPtr<CPDF_Image> CPDF_ImageObject::GetImage() const {
  return m_pImage;
}

RetainPtr<CFX_DIBitmap> CPDF_ImageObject::GetIndependentBitmap() const {
  RetainPtr<CFX_DIBBase> pSource = GetImage()->LoadDIBBase();

  // Realize() is non-virtual, and can't be overloaded by CPDF_DIB to
  // return a full-up CPDF_DIB subclass. Instead, it only works upon the
  // CFX_DIBBase, which is convenient since none of its members point to
  // objects owned by |this| or the form containing |this|. As a result,
  // the new bitmap may outlive them, giving the "independent" property
  // this method is named after.
  return pSource ? pSource->Realize() : nullptr;
}

void CPDF_ImageObject::SetImageMatrix(const CFX_Matrix& matrix) {
  m_Matrix = matrix;
  CalcBoundingBox();
}

void CPDF_ImageObject::MaybePurgeCache() {
  if (!m_pImage || m_pImage->IsGoingToBeDestroyed()) {
    return;
  }

  RetainPtr<const CPDF_Stream> pStream = m_pImage->GetStream();
  if (!pStream)
    return;

  uint32_t objnum = pStream->GetObjNum();
  if (!objnum)
    return;

  auto* pDoc = m_pImage->GetDocument();
  CHECK(pDoc);

  m_pImage.Reset();  // Clear my reference before asking the cache.
  pDoc->MaybePurgeImage(objnum);
}
