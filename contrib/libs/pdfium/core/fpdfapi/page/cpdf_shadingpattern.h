// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PAGE_CPDF_SHADINGPATTERN_H_
#define CORE_FPDFAPI_PAGE_CPDF_SHADINGPATTERN_H_

#include <stdint.h>

#include <memory>
#include <vector>

#include "core/fpdfapi/page/cpdf_colorspace.h"
#include "core/fpdfapi/page/cpdf_pattern.h"
#include "core/fxcrt/retain_ptr.h"

// Values used in PDFs except for |kInvalidShading| and |kMaxShading|.
// Do not change.
enum ShadingType {
  kInvalidShading = 0,
  kFunctionBasedShading = 1,
  kAxialShading = 2,
  kRadialShading = 3,
  kFreeFormGouraudTriangleMeshShading = 4,
  kLatticeFormGouraudTriangleMeshShading = 5,
  kCoonsPatchMeshShading = 6,
  kTensorProductPatchMeshShading = 7,
  kMaxShading = 8
};

class CFX_Matrix;
class CPDF_ColorSpace;
class CPDF_Document;
class CPDF_Function;
class CPDF_Object;

class CPDF_ShadingPattern final : public CPDF_Pattern {
 public:
  CONSTRUCT_VIA_MAKE_RETAIN;
  ~CPDF_ShadingPattern() override;

  // CPDF_Pattern:
  CPDF_ShadingPattern* AsShadingPattern() override;

  bool IsMeshShading() const {
    return m_ShadingType == kFreeFormGouraudTriangleMeshShading ||
           m_ShadingType == kLatticeFormGouraudTriangleMeshShading ||
           m_ShadingType == kCoonsPatchMeshShading ||
           m_ShadingType == kTensorProductPatchMeshShading;
  }
  bool Load();

  ShadingType GetShadingType() const { return m_ShadingType; }
  bool IsShadingObject() const { return m_bShading; }
  RetainPtr<const CPDF_Object> GetShadingObject() const;
  RetainPtr<CPDF_ColorSpace> GetCS() const { return m_pCS; }
  const std::vector<std::unique_ptr<CPDF_Function>>& GetFuncs() const {
    return m_pFunctions;
  }

 private:
  CPDF_ShadingPattern(CPDF_Document* pDoc,
                      RetainPtr<CPDF_Object> pPatternObj,
                      bool bShading,
                      const CFX_Matrix& parentMatrix);
  CPDF_ShadingPattern(const CPDF_ShadingPattern&) = delete;
  CPDF_ShadingPattern& operator=(const CPDF_ShadingPattern&) = delete;

  // Constraints in PDF 1.7 spec, 4.6.3 Shading Patterns, pages 308-331.
  bool Validate() const;
  bool ValidateFunctions(uint32_t nExpectedNumFunctions,
                         uint32_t nExpectedNumInputs,
                         uint32_t nExpectedNumOutputs) const;

  ShadingType m_ShadingType = kInvalidShading;
  const bool m_bShading;
  RetainPtr<CPDF_ColorSpace> m_pCS;
  std::vector<std::unique_ptr<CPDF_Function>> m_pFunctions;
};

#endif  // CORE_FPDFAPI_PAGE_CPDF_SHADINGPATTERN_H_
