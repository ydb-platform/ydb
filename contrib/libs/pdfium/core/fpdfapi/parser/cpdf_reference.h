// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PARSER_CPDF_REFERENCE_H_
#define CORE_FPDFAPI_PARSER_CPDF_REFERENCE_H_

#include <set>

#include "core/fpdfapi/parser/cpdf_object.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/unowned_ptr.h"

class CPDF_IndirectObjectHolder;

class CPDF_Reference final : public CPDF_Object {
 public:
  CONSTRUCT_VIA_MAKE_RETAIN;

  // CPDF_Object:
  Type GetType() const override;
  RetainPtr<CPDF_Object> Clone() const override;
  ByteString GetString() const override;
  float GetNumber() const override;
  int GetInteger() const override;
  CPDF_Reference* AsMutableReference() override;
  bool WriteTo(IFX_ArchiveStream* archive,
               const CPDF_Encryptor* encryptor) const override;
  RetainPtr<CPDF_Reference> MakeReference(
      CPDF_IndirectObjectHolder* holder) const override;

  uint32_t GetRefObjNum() const { return m_RefObjNum; }
  bool HasIndirectObjectHolder() const { return !!m_pObjList; }
  void SetRef(CPDF_IndirectObjectHolder* pDoc, uint32_t objnum);

 private:
  friend class CPDF_Dictionary;

  CPDF_Reference(CPDF_IndirectObjectHolder* pDoc, uint32_t objnum);
  ~CPDF_Reference() override;

  const CPDF_Object* GetDirectInternal() const override;
  const CPDF_Dictionary* GetDictInternal() const override;
  RetainPtr<CPDF_Object> CloneNonCyclic(
      bool bDirect,
      std::set<const CPDF_Object*>* pVisited) const override;

  const CPDF_Object* FastGetDirect() const;

  UnownedPtr<CPDF_IndirectObjectHolder> m_pObjList;
  uint32_t m_RefObjNum;
};

inline CPDF_Reference* ToReference(CPDF_Object* obj) {
  return obj ? obj->AsMutableReference() : nullptr;
}

inline const CPDF_Reference* ToReference(const CPDF_Object* obj) {
  return obj ? obj->AsReference() : nullptr;
}

inline RetainPtr<CPDF_Reference> ToReference(RetainPtr<CPDF_Object> obj) {
  return RetainPtr<CPDF_Reference>(ToReference(obj.Get()));
}

inline RetainPtr<const CPDF_Reference> ToReference(
    RetainPtr<const CPDF_Object> obj) {
  return RetainPtr<const CPDF_Reference>(ToReference(obj.Get()));
}

#endif  // CORE_FPDFAPI_PARSER_CPDF_REFERENCE_H_
