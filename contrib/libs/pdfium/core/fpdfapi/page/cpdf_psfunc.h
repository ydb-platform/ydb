// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PAGE_CPDF_PSFUNC_H_
#define CORE_FPDFAPI_PAGE_CPDF_PSFUNC_H_

#include "core/fpdfapi/page/cpdf_function.h"
#include "core/fpdfapi/page/cpdf_psengine.h"

class CPDF_Object;

class CPDF_PSFunc final : public CPDF_Function {
 public:
  CPDF_PSFunc();
  ~CPDF_PSFunc() override;

  // CPDF_Function:
  bool v_Init(const CPDF_Object* pObj, VisitedSet* pVisited) override;
  bool v_Call(pdfium::span<const float> inputs,
              pdfium::span<float> results) const override;

 private:
  mutable CPDF_PSEngine m_PS;  // Pre-initialized scratch space for v_Call().
};

#endif  // CORE_FPDFAPI_PAGE_CPDF_PSFUNC_H_
