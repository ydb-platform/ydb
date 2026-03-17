// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFDOC_CPDF_DEST_H_
#define CORE_FPDFDOC_CPDF_DEST_H_

#include <vector>

#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fxcrt/retain_ptr.h"

class CPDF_Document;
class CPDF_Object;

class CPDF_Dest {
 public:
  explicit CPDF_Dest(RetainPtr<const CPDF_Array> pArray);
  CPDF_Dest(const CPDF_Dest& that);
  ~CPDF_Dest();

  // Use when |pDest| is an object of an unknown type. Can pass in nullptr.
  static CPDF_Dest Create(CPDF_Document* pDoc,
                          RetainPtr<const CPDF_Object> pDest);

  const CPDF_Array* GetArray() const { return m_pArray.Get(); }

  int GetDestPageIndex(CPDF_Document* pDoc) const;
  std::vector<float> GetScrollPositionArray() const;

  // Returns the zoom mode, as one of the PDFDEST_VIEW_* values in fpdf_doc.h.
  int GetZoomMode() const;

  size_t GetNumParams() const;
  float GetParam(size_t index) const;
  bool GetXYZ(bool* pHasX,
              bool* pHasY,
              bool* pHasZoom,
              float* pX,
              float* pY,
              float* pZoom) const;

 private:
  RetainPtr<const CPDF_Array> const m_pArray;
};

#endif  // CORE_FPDFDOC_CPDF_DEST_H_
