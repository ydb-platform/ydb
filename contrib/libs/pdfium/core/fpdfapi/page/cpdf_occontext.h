// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PAGE_CPDF_OCCONTEXT_H_
#define CORE_FPDFAPI_PAGE_CPDF_OCCONTEXT_H_

#include <functional>
#include <map>

#include "core/fxcrt/bytestring.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/unowned_ptr.h"

class CPDF_Array;
class CPDF_Dictionary;
class CPDF_Document;
class CPDF_PageObject;

class CPDF_OCContext final : public Retainable {
 public:
  enum UsageType { kView = 0, kDesign, kPrint, kExport };

  CONSTRUCT_VIA_MAKE_RETAIN;

  bool CheckOCGDictVisible(const CPDF_Dictionary* pOCGDict) const;
  bool CheckPageObjectVisible(const CPDF_PageObject* pObj) const;

 private:
  CPDF_OCContext(CPDF_Document* pDoc, UsageType eUsageType);
  ~CPDF_OCContext() override;

  bool LoadOCGStateFromConfig(const ByteString& csConfig,
                              const CPDF_Dictionary* pOCGDict) const;
  bool LoadOCGState(const CPDF_Dictionary* pOCGDict) const;
  bool GetOCGVisible(const CPDF_Dictionary* pOCGDict) const;
  bool GetOCGVE(const CPDF_Array* pExpression, int nLevel) const;
  bool LoadOCMDState(const CPDF_Dictionary* pOCMDDict) const;

  UnownedPtr<CPDF_Document> const m_pDocument;
  const UsageType m_eUsageType;
  mutable std::map<RetainPtr<const CPDF_Dictionary>, bool, std::less<>>
      m_OGCStateCache;
};

#endif  // CORE_FPDFAPI_PAGE_CPDF_OCCONTEXT_H_
