// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PAGE_CPDF_FORM_H_
#define CORE_FPDFAPI_PAGE_CPDF_FORM_H_

#include <set>
#include <utility>

#include "core/fpdfapi/font/cpdf_font.h"
#include "core/fpdfapi/page/cpdf_pageobjectholder.h"
#include "core/fxcrt/retain_ptr.h"

class CFX_Matrix;
class CPDF_AllStates;
class CPDF_Dictionary;
class CPDF_Document;
class CPDF_Stream;
class CPDF_Type3Char;

class CPDF_Form final : public CPDF_PageObjectHolder,
                        public CPDF_Font::FormIface {
 public:
  struct RecursionState {
    RecursionState();
    ~RecursionState();

    std::set<const uint8_t*> parsed_set;
  };

  // Helper method to choose the first non-null resources dictionary.
  static CPDF_Dictionary* ChooseResourcesDict(CPDF_Dictionary* pResources,
                                              CPDF_Dictionary* pParentResources,
                                              CPDF_Dictionary* pPageResources);

  CPDF_Form(CPDF_Document* pDocument,
            RetainPtr<CPDF_Dictionary> pPageResources,
            RetainPtr<CPDF_Stream> pFormStream);
  CPDF_Form(CPDF_Document* pDocument,
            RetainPtr<CPDF_Dictionary> pPageResources,
            RetainPtr<CPDF_Stream> pFormStream,
            CPDF_Dictionary* pParentResources);
  ~CPDF_Form() override;

  // CPDF_Font::FormIface:
  void ParseContentForType3Char(CPDF_Type3Char* pType3Char) override;
  bool HasPageObjects() const override;
  CFX_FloatRect CalcBoundingBox() const override;
  std::optional<std::pair<RetainPtr<CFX_DIBitmap>, CFX_Matrix>>
  GetBitmapAndMatrixFromSoleImageOfForm() const override;

  void ParseContent();
  void ParseContent(const CPDF_AllStates* pGraphicStates,
                    const CFX_Matrix* pParentMatrix,
                    RecursionState* recursion_state);

  RetainPtr<const CPDF_Stream> GetStream() const;

 private:
  void ParseContentInternal(const CPDF_AllStates* pGraphicStates,
                            const CFX_Matrix* pParentMatrix,
                            CPDF_Type3Char* pType3Char,
                            RecursionState* recursion_state);

  RecursionState m_RecursionState;
  RetainPtr<CPDF_Stream> const m_pFormStream;
};

#endif  // CORE_FPDFAPI_PAGE_CPDF_FORM_H_
