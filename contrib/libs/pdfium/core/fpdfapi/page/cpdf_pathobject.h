// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PAGE_CPDF_PATHOBJECT_H_
#define CORE_FPDFAPI_PAGE_CPDF_PATHOBJECT_H_

#include <stdint.h>

#include "core/fpdfapi/page/cpdf_pageobject.h"
#include "core/fpdfapi/page/cpdf_path.h"
#include "core/fxcrt/fx_coordinates.h"
#include "core/fxge/cfx_fillrenderoptions.h"

class CPDF_PathObject final : public CPDF_PageObject {
 public:
  explicit CPDF_PathObject(int32_t content_stream);
  CPDF_PathObject();
  ~CPDF_PathObject() override;

  // CPDF_PageObject
  Type GetType() const override;
  void Transform(const CFX_Matrix& matrix) override;
  bool IsPath() const override;
  CPDF_PathObject* AsPath() override;
  const CPDF_PathObject* AsPath() const override;

  void CalcBoundingBox();

  bool stroke() const { return m_bStroke; }
  void set_stroke(bool stroke) { m_bStroke = stroke; }

  // Layering, avoid caller knowledge of CFX_FillRenderOptions::FillType values.
  bool has_no_filltype() const {
    return m_FillType == CFX_FillRenderOptions::FillType::kNoFill;
  }
  bool has_winding_filltype() const {
    return m_FillType == CFX_FillRenderOptions::FillType::kWinding;
  }
  bool has_alternate_filltype() const {
    return m_FillType == CFX_FillRenderOptions::FillType::kEvenOdd;
  }
  void set_no_filltype() {
    m_FillType = CFX_FillRenderOptions::FillType::kNoFill;
  }
  void set_winding_filltype() {
    m_FillType = CFX_FillRenderOptions::FillType::kWinding;
  }
  void set_alternate_filltype() {
    m_FillType = CFX_FillRenderOptions::FillType::kEvenOdd;
  }

  CFX_FillRenderOptions::FillType filltype() const { return m_FillType; }
  void set_filltype(CFX_FillRenderOptions::FillType fill_type) {
    m_FillType = fill_type;
  }

  CPDF_Path& path() { return m_Path; }
  const CPDF_Path& path() const { return m_Path; }

  const CFX_Matrix& matrix() const { return m_Matrix; }
  void SetPathMatrix(const CFX_Matrix& matrix);

 private:
  bool m_bStroke = false;
  CFX_FillRenderOptions::FillType m_FillType =
      CFX_FillRenderOptions::FillType::kNoFill;
  CPDF_Path m_Path;
  CFX_Matrix m_Matrix;
};

#endif  // CORE_FPDFAPI_PAGE_CPDF_PATHOBJECT_H_
