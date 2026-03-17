// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef FPDFSDK_FORMFILLER_CFFL_TEXTOBJECT_H_
#define FPDFSDK_FORMFILLER_CFFL_TEXTOBJECT_H_

#include <memory>

#include "fpdfsdk/formfiller/cffl_formfield.h"

class CPDF_BAFontMap;

// Class to implement common functionality for CFFL_FormField sub-classes with
// text fields.
class CFFL_TextObject : public CFFL_FormField {
 public:
  // CFFL_FormField:
  CPWL_Wnd* ResetPWLWindow(const CPDFSDK_PageView* pPageView) override;
  CPWL_Wnd* RestorePWLWindow(const CPDFSDK_PageView* pPageView) override;

 protected:
  CFFL_TextObject(CFFL_InteractiveFormFiller* pFormFiller,
                  CPDFSDK_Widget* pWidget);
  ~CFFL_TextObject() override;

  CPDF_BAFontMap* GetOrCreateFontMap();

 private:
  std::unique_ptr<CPDF_BAFontMap> m_pFontMap;
};

#endif  // FPDFSDK_FORMFILLER_CFFL_TEXTOBJECT_H_
