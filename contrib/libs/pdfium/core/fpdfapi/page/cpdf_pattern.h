// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PAGE_CPDF_PATTERN_H_
#define CORE_FPDFAPI_PAGE_CPDF_PATTERN_H_

#include "core/fpdfapi/parser/cpdf_object.h"
#include "core/fxcrt/fx_coordinates.h"
#include "core/fxcrt/observed_ptr.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/unowned_ptr.h"

class CPDF_Document;
class CPDF_ShadingPattern;
class CPDF_TilingPattern;

class CPDF_Pattern : public Retainable, public Observable {
 public:
  // Values used in PDFs. Do not change.
  enum PatternType { kTiling = 1, kShading = 2 };

  virtual CPDF_TilingPattern* AsTilingPattern();
  virtual CPDF_ShadingPattern* AsShadingPattern();

  const CFX_Matrix& pattern_to_form() const { return m_Pattern2Form; }

 protected:
  CPDF_Pattern(CPDF_Document* pDoc,
               RetainPtr<CPDF_Object> pObj,
               const CFX_Matrix& parentMatrix);
  ~CPDF_Pattern() override;

  // All the getters that return pointers return non-NULL pointers.
  CPDF_Document* document() const { return m_pDocument; }
  RetainPtr<CPDF_Object> pattern_obj() const { return m_pPatternObj; }
  const CFX_Matrix& parent_matrix() const { return m_ParentMatrix; }

  void SetPatternToFormMatrix();

 private:
  UnownedPtr<CPDF_Document> const m_pDocument;
  RetainPtr<CPDF_Object> const m_pPatternObj;
  CFX_Matrix m_Pattern2Form;
  const CFX_Matrix m_ParentMatrix;
};

#endif  // CORE_FPDFAPI_PAGE_CPDF_PATTERN_H_
