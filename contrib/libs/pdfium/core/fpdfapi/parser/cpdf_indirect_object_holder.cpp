// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/parser/cpdf_indirect_object_holder.h"

#include <algorithm>
#include <memory>
#include <utility>

#include "core/fpdfapi/parser/cpdf_object.h"
#include "core/fpdfapi/parser/cpdf_parser.h"
#include "core/fxcrt/check.h"

namespace {

const CPDF_Object* FilterInvalidObjNum(const CPDF_Object* obj) {
  return obj && obj->GetObjNum() != CPDF_Object::kInvalidObjNum ? obj : nullptr;
}

}  // namespace

CPDF_IndirectObjectHolder::CPDF_IndirectObjectHolder()
    : m_pByteStringPool(std::make_unique<ByteStringPool>()) {}

CPDF_IndirectObjectHolder::~CPDF_IndirectObjectHolder() {
  m_pByteStringPool.DeleteObject();  // Make weak.
}

RetainPtr<const CPDF_Object> CPDF_IndirectObjectHolder::GetIndirectObject(
    uint32_t objnum) const {
  return pdfium::WrapRetain(GetIndirectObjectInternal(objnum));
}

RetainPtr<CPDF_Object> CPDF_IndirectObjectHolder::GetMutableIndirectObject(
    uint32_t objnum) {
  return pdfium::WrapRetain(
      const_cast<CPDF_Object*>(GetIndirectObjectInternal(objnum)));
}

const CPDF_Object* CPDF_IndirectObjectHolder::GetIndirectObjectInternal(
    uint32_t objnum) const {
  auto it = m_IndirectObjs.find(objnum);
  if (it == m_IndirectObjs.end())
    return nullptr;

  return FilterInvalidObjNum(it->second.Get());
}

RetainPtr<CPDF_Object> CPDF_IndirectObjectHolder::GetOrParseIndirectObject(
    uint32_t objnum) {
  return pdfium::WrapRetain(GetOrParseIndirectObjectInternal(objnum));
}

CPDF_Object* CPDF_IndirectObjectHolder::GetOrParseIndirectObjectInternal(
    uint32_t objnum) {
  if (objnum == 0 || objnum == CPDF_Object::kInvalidObjNum)
    return nullptr;

  // Add item anyway to prevent recursively parsing of same object.
  auto insert_result = m_IndirectObjs.insert(std::make_pair(objnum, nullptr));
  if (!insert_result.second) {
    return const_cast<CPDF_Object*>(
        FilterInvalidObjNum(insert_result.first->second.Get()));
  }
  RetainPtr<CPDF_Object> pNewObj = ParseIndirectObject(objnum);
  if (!pNewObj) {
    m_IndirectObjs.erase(insert_result.first);
    return nullptr;
  }

  pNewObj->SetObjNum(objnum);
  m_LastObjNum = std::max(m_LastObjNum, objnum);

  CPDF_Object* result = pNewObj.Get();
  insert_result.first->second = std::move(pNewObj);
  return result;
}

RetainPtr<CPDF_Object> CPDF_IndirectObjectHolder::ParseIndirectObject(
    uint32_t objnum) {
  return nullptr;
}

uint32_t CPDF_IndirectObjectHolder::AddIndirectObject(
    RetainPtr<CPDF_Object> pObj) {
  CHECK(!pObj->GetObjNum());
  pObj->SetObjNum(++m_LastObjNum);
  m_IndirectObjs[m_LastObjNum] = std::move(pObj);
  return m_LastObjNum;
}

bool CPDF_IndirectObjectHolder::ReplaceIndirectObjectIfHigherGeneration(
    uint32_t objnum,
    RetainPtr<CPDF_Object> pObj) {
  DCHECK(objnum);
  if (!pObj || objnum == CPDF_Object::kInvalidObjNum)
    return false;

  auto& obj_holder = m_IndirectObjs[objnum];
  const CPDF_Object* old_object = FilterInvalidObjNum(obj_holder.Get());
  if (old_object && pObj->GetGenNum() <= old_object->GetGenNum())
    return false;

  pObj->SetObjNum(objnum);
  obj_holder = std::move(pObj);
  m_LastObjNum = std::max(m_LastObjNum, objnum);
  return true;
}

void CPDF_IndirectObjectHolder::DeleteIndirectObject(uint32_t objnum) {
  auto it = m_IndirectObjs.find(objnum);
  if (it == m_IndirectObjs.end() || !FilterInvalidObjNum(it->second.Get()))
    return;

  m_IndirectObjs.erase(it);
}
