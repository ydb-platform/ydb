// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfdoc/cpdf_structelement.h"

#include <utility>

#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_name.h"
#include "core/fpdfapi/parser/cpdf_number.h"
#include "core/fpdfapi/parser/cpdf_object.h"
#include "core/fpdfapi/parser/cpdf_reference.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fpdfdoc/cpdf_structtree.h"
#include "core/fxcrt/check.h"

CPDF_StructElement::Kid::Kid() = default;

CPDF_StructElement::Kid::Kid(const Kid& that) = default;

CPDF_StructElement::Kid::~Kid() = default;

CPDF_StructElement::CPDF_StructElement(const CPDF_StructTree* pTree,
                                       RetainPtr<const CPDF_Dictionary> pDict)
    : m_pTree(pTree),
      m_pDict(std::move(pDict)),
      m_Type(m_pTree->GetRoleMapNameFor(m_pDict->GetNameFor("S"))) {
  LoadKids();
}

CPDF_StructElement::~CPDF_StructElement() {
  for (auto& kid : m_Kids) {
    if (kid.m_Type == Kid::kElement && kid.m_pElement) {
      kid.m_pElement->SetParent(nullptr);
    }
  }
}

ByteString CPDF_StructElement::GetObjType() const {
  return m_pDict->GetByteStringFor("Type");
}

WideString CPDF_StructElement::GetAltText() const {
  return m_pDict->GetUnicodeTextFor("Alt");
}

WideString CPDF_StructElement::GetActualText() const {
  return m_pDict->GetUnicodeTextFor("ActualText");
}

WideString CPDF_StructElement::GetTitle() const {
  return m_pDict->GetUnicodeTextFor("T");
}

std::optional<WideString> CPDF_StructElement::GetID() const {
  RetainPtr<const CPDF_Object> obj = m_pDict->GetObjectFor("ID");
  if (!obj || !obj->IsString())
    return std::nullopt;
  return obj->GetUnicodeText();
}

std::optional<WideString> CPDF_StructElement::GetLang() const {
  RetainPtr<const CPDF_Object> obj = m_pDict->GetObjectFor("Lang");
  if (!obj || !obj->IsString())
    return std::nullopt;
  return obj->GetUnicodeText();
}

RetainPtr<const CPDF_Object> CPDF_StructElement::GetA() const {
  return m_pDict->GetObjectFor("A");
}

RetainPtr<const CPDF_Object> CPDF_StructElement::GetK() const {
  return m_pDict->GetObjectFor("K");
}

size_t CPDF_StructElement::CountKids() const {
  return m_Kids.size();
}

CPDF_StructElement* CPDF_StructElement::GetKidIfElement(size_t index) const {
  return m_Kids[index].m_Type == Kid::kElement ? m_Kids[index].m_pElement.Get()
                                               : nullptr;
}

int CPDF_StructElement::GetKidContentId(size_t index) const {
  return m_Kids[index].m_Type == Kid::kStreamContent ||
                 m_Kids[index].m_Type == Kid::kPageContent
             ? m_Kids[index].m_ContentId
             : -1;
}

bool CPDF_StructElement::UpdateKidIfElement(const CPDF_Dictionary* pDict,
                                            CPDF_StructElement* pElement) {
  bool bSave = false;
  for (auto& kid : m_Kids) {
    if (kid.m_Type == Kid::kElement && kid.m_pDict == pDict) {
      kid.m_pElement.Reset(pElement);
      bSave = true;
    }
  }
  return bSave;
}

void CPDF_StructElement::LoadKids() {
  RetainPtr<const CPDF_Object> pObj = m_pDict->GetObjectFor("Pg");
  const CPDF_Reference* pRef = ToReference(pObj.Get());
  const uint32_t page_obj_num = pRef ? pRef->GetRefObjNum() : 0;
  RetainPtr<const CPDF_Object> pKids = m_pDict->GetDirectObjectFor("K");
  if (!pKids)
    return;

  DCHECK(m_Kids.empty());
  if (const CPDF_Array* pArray = pKids->AsArray()) {
    m_Kids.resize(pArray->size());
    for (size_t i = 0; i < pArray->size(); ++i) {
      LoadKid(page_obj_num, pArray->GetDirectObjectAt(i), m_Kids[i]);
    }
    return;
  }

  m_Kids.resize(1);
  LoadKid(page_obj_num, std::move(pKids), m_Kids[0]);
}

void CPDF_StructElement::LoadKid(uint32_t page_obj_num,
                                 RetainPtr<const CPDF_Object> pKidObj,
                                 Kid& kid) {
  if (!pKidObj)
    return;

  if (pKidObj->IsNumber()) {
    if (m_pTree->GetPageObjNum() != page_obj_num) {
      return;
    }

    kid.m_Type = Kid::kPageContent;
    kid.m_ContentId = pKidObj->GetInteger();
    kid.m_PageObjNum = page_obj_num;
    return;
  }

  const CPDF_Dictionary* pKidDict = pKidObj->AsDictionary();
  if (!pKidDict)
    return;

  if (RetainPtr<const CPDF_Reference> pRef =
          ToReference(pKidDict->GetObjectFor("Pg"))) {
    page_obj_num = pRef->GetRefObjNum();
  }
  ByteString type = pKidDict->GetNameFor("Type");
  if ((type == "MCR" || type == "OBJR") &&
      m_pTree->GetPageObjNum() != page_obj_num) {
    return;
  }

  if (type == "MCR") {
    kid.m_Type = Kid::kStreamContent;
    RetainPtr<const CPDF_Reference> pRef =
        ToReference(pKidDict->GetObjectFor("Stm"));
    kid.m_RefObjNum = pRef ? pRef->GetRefObjNum() : 0;
    kid.m_PageObjNum = page_obj_num;
    kid.m_ContentId = pKidDict->GetIntegerFor("MCID");
    return;
  }

  if (type == "OBJR") {
    kid.m_Type = Kid::kObject;
    RetainPtr<const CPDF_Reference> pObj =
        ToReference(pKidDict->GetObjectFor("Obj"));
    kid.m_RefObjNum = pObj ? pObj->GetRefObjNum() : 0;
    kid.m_PageObjNum = page_obj_num;
    return;
  }

  kid.m_Type = Kid::kElement;
  kid.m_pDict.Reset(pKidDict);
}
