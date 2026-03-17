// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFDOC_CPDF_NAMETREE_H_
#define CORE_FPDFDOC_CPDF_NAMETREE_H_

#include <stddef.h>

#include <memory>

#include "core/fxcrt/fx_string.h"
#include "core/fxcrt/retain_ptr.h"

class CPDF_Array;
class CPDF_Dictionary;
class CPDF_Document;
class CPDF_Object;

class CPDF_NameTree {
 public:
  CPDF_NameTree(const CPDF_NameTree&) = delete;
  CPDF_NameTree& operator=(const CPDF_NameTree&) = delete;
  ~CPDF_NameTree();

  static std::unique_ptr<CPDF_NameTree> Create(CPDF_Document* pDoc,
                                               const ByteString& category);

  // If necessary, create missing Names dictionary in |pDoc|, and/or missing
  // Names array in the dictionary that corresponds to |category|, if necessary.
  // Returns nullptr on failure.
  static std::unique_ptr<CPDF_NameTree> CreateWithRootNameArray(
      CPDF_Document* pDoc,
      const ByteString& category);

  static std::unique_ptr<CPDF_NameTree> CreateForTesting(
      CPDF_Dictionary* pRoot);

  static RetainPtr<const CPDF_Array> LookupNamedDest(CPDF_Document* doc,
                                                     const ByteString& name);

  bool AddValueAndName(RetainPtr<CPDF_Object> pObj, const WideString& name);
  bool DeleteValueAndName(size_t nIndex);

  RetainPtr<CPDF_Object> LookupValueAndName(size_t nIndex,
                                            WideString* csName) const;
  RetainPtr<const CPDF_Object> LookupValue(const WideString& csName) const;

  size_t GetCount() const;
  CPDF_Dictionary* GetRootForTesting() const { return m_pRoot.Get(); }

 private:
  explicit CPDF_NameTree(RetainPtr<CPDF_Dictionary> pRoot);

  RetainPtr<const CPDF_Array> LookupNewStyleNamedDest(const ByteString& name);

  RetainPtr<CPDF_Dictionary> const m_pRoot;
};

#endif  // CORE_FPDFDOC_CPDF_NAMETREE_H_
