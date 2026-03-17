// Copyright 2021 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PAGE_CPDF_BASEDCS_H_
#define CORE_FPDFAPI_PAGE_CPDF_BASEDCS_H_

#include "core/fpdfapi/page/cpdf_colorspace.h"
#include "core/fxcrt/retain_ptr.h"

// Represents a color space that is based on another color space. This includes
// all the special color spaces in ISO 32000-1:2008, table 62, as well as the
// ICCBased color space.
class CPDF_BasedCS : public CPDF_ColorSpace {
 public:
  CONSTRUCT_VIA_MAKE_RETAIN;
  ~CPDF_BasedCS() override;

  void EnableStdConversion(bool bEnabled) final;

 protected:
  explicit CPDF_BasedCS(Family family);

  RetainPtr<CPDF_ColorSpace> m_pBaseCS;  // May be fallback CS in some cases.
};

#endif  // CORE_FPDFAPI_PAGE_CPDF_BASEDCS_H_
