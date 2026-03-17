// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/parser/cpdf_dictionary.h"

#include <set>
#include <utility>

#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_boolean.h"
#include "core/fpdfapi/parser/cpdf_crypto_handler.h"
#include "core/fpdfapi/parser/cpdf_name.h"
#include "core/fpdfapi/parser/cpdf_number.h"
#include "core/fpdfapi/parser/cpdf_reference.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fpdfapi/parser/cpdf_string.h"
#include "core/fpdfapi/parser/fpdf_parser_utility.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/containers/contains.h"
#include "core/fxcrt/fx_stream.h"

CPDF_Dictionary::CPDF_Dictionary()
    : CPDF_Dictionary(WeakPtr<ByteStringPool>()) {}

CPDF_Dictionary::CPDF_Dictionary(const WeakPtr<ByteStringPool>& pPool)
    : m_pPool(pPool) {}

CPDF_Dictionary::~CPDF_Dictionary() {
  // Mark the object as deleted so that it will not be deleted again,
  // and break cyclic references.
  m_ObjNum = kInvalidObjNum;
  for (auto& it : m_Map) {
    if (it.second->GetObjNum() == kInvalidObjNum)
      it.second.Leak();
  }
}

CPDF_Object::Type CPDF_Dictionary::GetType() const {
  return kDictionary;
}

CPDF_Dictionary* CPDF_Dictionary::AsMutableDictionary() {
  return this;
}

RetainPtr<CPDF_Object> CPDF_Dictionary::Clone() const {
  return CloneObjectNonCyclic(false);
}

RetainPtr<CPDF_Object> CPDF_Dictionary::CloneNonCyclic(
    bool bDirect,
    std::set<const CPDF_Object*>* pVisited) const {
  pVisited->insert(this);
  auto pCopy = pdfium::MakeRetain<CPDF_Dictionary>(m_pPool);
  CPDF_DictionaryLocker locker(this);
  for (const auto& it : locker) {
    if (!pdfium::Contains(*pVisited, it.second.Get())) {
      std::set<const CPDF_Object*> visited(*pVisited);
      auto obj = it.second->CloneNonCyclic(bDirect, &visited);
      if (obj)
        pCopy->m_Map.insert(std::make_pair(it.first, std::move(obj)));
    }
  }
  return pCopy;
}

const CPDF_Object* CPDF_Dictionary::GetObjectForInternal(
    const ByteString& key) const {
  auto it = m_Map.find(key);
  return it != m_Map.end() ? it->second.Get() : nullptr;
}

RetainPtr<const CPDF_Object> CPDF_Dictionary::GetObjectFor(
    const ByteString& key) const {
  return pdfium::WrapRetain(GetObjectForInternal(key));
}

RetainPtr<CPDF_Object> CPDF_Dictionary::GetMutableObjectFor(
    const ByteString& key) {
  return pdfium::WrapRetain(
      const_cast<CPDF_Object*>(GetObjectForInternal(key)));
}

const CPDF_Object* CPDF_Dictionary::GetDirectObjectForInternal(
    const ByteString& key) const {
  const CPDF_Object* p = GetObjectForInternal(key);
  return p ? p->GetDirectInternal() : nullptr;
}

RetainPtr<const CPDF_Object> CPDF_Dictionary::GetDirectObjectFor(
    const ByteString& key) const {
  return pdfium::WrapRetain(GetDirectObjectForInternal(key));
}

RetainPtr<CPDF_Object> CPDF_Dictionary::GetMutableDirectObjectFor(
    const ByteString& key) {
  return pdfium::WrapRetain(
      const_cast<CPDF_Object*>(GetDirectObjectForInternal(key)));
}

ByteString CPDF_Dictionary::GetByteStringFor(const ByteString& key) const {
  const CPDF_Object* p = GetObjectForInternal(key);
  return p ? p->GetString() : ByteString();
}

ByteString CPDF_Dictionary::GetByteStringFor(const ByteString& key,
                                             const ByteString& def) const {
  const CPDF_Object* p = GetObjectForInternal(key);
  return p ? p->GetString() : ByteString(def);
}

WideString CPDF_Dictionary::GetUnicodeTextFor(const ByteString& key) const {
  const CPDF_Object* p = GetObjectForInternal(key);
  if (const CPDF_Reference* pRef = ToReference(p))
    p = pRef->GetDirectInternal();
  return p ? p->GetUnicodeText() : WideString();
}

ByteString CPDF_Dictionary::GetNameFor(const ByteString& key) const {
  const CPDF_Name* p = ToName(GetObjectForInternal(key));
  return p ? p->GetString() : ByteString();
}

bool CPDF_Dictionary::GetBooleanFor(const ByteString& key,
                                    bool bDefault) const {
  const CPDF_Object* p = GetObjectForInternal(key);
  return ToBoolean(p) ? p->GetInteger() != 0 : bDefault;
}

int CPDF_Dictionary::GetIntegerFor(const ByteString& key) const {
  const CPDF_Object* p = GetObjectForInternal(key);
  return p ? p->GetInteger() : 0;
}

int CPDF_Dictionary::GetIntegerFor(const ByteString& key, int def) const {
  const CPDF_Object* p = GetObjectForInternal(key);
  return p ? p->GetInteger() : def;
}

int CPDF_Dictionary::GetDirectIntegerFor(const ByteString& key) const {
  const CPDF_Number* p = ToNumber(GetObjectForInternal(key));
  return p ? p->GetInteger() : 0;
}

float CPDF_Dictionary::GetFloatFor(const ByteString& key) const {
  const CPDF_Object* p = GetObjectForInternal(key);
  return p ? p->GetNumber() : 0;
}

const CPDF_Dictionary* CPDF_Dictionary::GetDictInternal() const {
  return this;
}

const CPDF_Dictionary* CPDF_Dictionary::GetDictForInternal(
    const ByteString& key) const {
  const CPDF_Object* p = GetDirectObjectForInternal(key);
  return p ? p->GetDictInternal() : nullptr;
}

RetainPtr<const CPDF_Dictionary> CPDF_Dictionary::GetDictFor(
    const ByteString& key) const {
  return pdfium::WrapRetain(GetDictForInternal(key));
}

RetainPtr<CPDF_Dictionary> CPDF_Dictionary::GetMutableDictFor(
    const ByteString& key) {
  return pdfium::WrapRetain(
      const_cast<CPDF_Dictionary*>(GetDictForInternal(key)));
}

RetainPtr<CPDF_Dictionary> CPDF_Dictionary::GetOrCreateDictFor(
    const ByteString& key) {
  RetainPtr<CPDF_Dictionary> result = GetMutableDictFor(key);
  if (result)
    return result;
  return SetNewFor<CPDF_Dictionary>(key);
}

const CPDF_Array* CPDF_Dictionary::GetArrayForInternal(
    const ByteString& key) const {
  return ToArray(GetDirectObjectForInternal(key));
}

RetainPtr<const CPDF_Array> CPDF_Dictionary::GetArrayFor(
    const ByteString& key) const {
  return pdfium::WrapRetain(GetArrayForInternal(key));
}

RetainPtr<CPDF_Array> CPDF_Dictionary::GetMutableArrayFor(
    const ByteString& key) {
  return pdfium::WrapRetain(const_cast<CPDF_Array*>(GetArrayForInternal(key)));
}

RetainPtr<CPDF_Array> CPDF_Dictionary::GetOrCreateArrayFor(
    const ByteString& key) {
  RetainPtr<CPDF_Array> result = GetMutableArrayFor(key);
  if (result)
    return result;
  return SetNewFor<CPDF_Array>(key);
}

const CPDF_Stream* CPDF_Dictionary::GetStreamForInternal(
    const ByteString& key) const {
  return ToStream(GetDirectObjectForInternal(key));
}

RetainPtr<const CPDF_Stream> CPDF_Dictionary::GetStreamFor(
    const ByteString& key) const {
  return pdfium::WrapRetain(GetStreamForInternal(key));
}

RetainPtr<CPDF_Stream> CPDF_Dictionary::GetMutableStreamFor(
    const ByteString& key) {
  return pdfium::WrapRetain(
      const_cast<CPDF_Stream*>(GetStreamForInternal(key)));
}

const CPDF_Number* CPDF_Dictionary::GetNumberForInternal(
    const ByteString& key) const {
  return ToNumber(GetObjectForInternal(key));
}

RetainPtr<const CPDF_Number> CPDF_Dictionary::GetNumberFor(
    const ByteString& key) const {
  return pdfium::WrapRetain(GetNumberForInternal(key));
}

const CPDF_String* CPDF_Dictionary::GetStringForInternal(
    const ByteString& key) const {
  return ToString(GetObjectForInternal(key));
}

RetainPtr<const CPDF_String> CPDF_Dictionary::GetStringFor(
    const ByteString& key) const {
  return pdfium::WrapRetain(GetStringForInternal(key));
}

CFX_FloatRect CPDF_Dictionary::GetRectFor(const ByteString& key) const {
  const CPDF_Array* pArray = GetArrayForInternal(key);
  if (pArray)
    return pArray->GetRect();
  return CFX_FloatRect();
}

CFX_Matrix CPDF_Dictionary::GetMatrixFor(const ByteString& key) const {
  const CPDF_Array* pArray = GetArrayForInternal(key);
  if (pArray)
    return pArray->GetMatrix();
  return CFX_Matrix();
}

bool CPDF_Dictionary::KeyExist(const ByteString& key) const {
  return pdfium::Contains(m_Map, key);
}

std::vector<ByteString> CPDF_Dictionary::GetKeys() const {
  std::vector<ByteString> result;
  CPDF_DictionaryLocker locker(this);
  for (const auto& item : locker)
    result.push_back(item.first);
  return result;
}

void CPDF_Dictionary::SetFor(const ByteString& key,
                             RetainPtr<CPDF_Object> object) {
  (void)SetForInternal(key, std::move(object));
}

CPDF_Object* CPDF_Dictionary::SetForInternal(const ByteString& key,
                                             RetainPtr<CPDF_Object> pObj) {
  CHECK(!IsLocked());
  if (!pObj) {
    m_Map.erase(key);
    return nullptr;
  }
  CHECK(pObj->IsInline());
  CHECK(!pObj->IsStream());
  CPDF_Object* pRet = pObj.Get();
  m_Map[MaybeIntern(key)] = std::move(pObj);
  return pRet;
}

void CPDF_Dictionary::ConvertToIndirectObjectFor(
    const ByteString& key,
    CPDF_IndirectObjectHolder* pHolder) {
  CHECK(!IsLocked());
  auto it = m_Map.find(key);
  if (it == m_Map.end() || it->second->IsReference())
    return;

  pHolder->AddIndirectObject(it->second);
  it->second = it->second->MakeReference(pHolder);
}

RetainPtr<CPDF_Object> CPDF_Dictionary::RemoveFor(ByteStringView key) {
  CHECK(!IsLocked());
  RetainPtr<CPDF_Object> result;
  auto it = m_Map.find(key);
  if (it != m_Map.end()) {
    result = std::move(it->second);
    m_Map.erase(it);
  }
  return result;
}

void CPDF_Dictionary::ReplaceKey(const ByteString& oldkey,
                                 const ByteString& newkey) {
  CHECK(!IsLocked());
  auto old_it = m_Map.find(oldkey);
  if (old_it == m_Map.end())
    return;

  auto new_it = m_Map.find(newkey);
  if (new_it == old_it)
    return;

  m_Map[MaybeIntern(newkey)] = std::move(old_it->second);
  m_Map.erase(old_it);
}

void CPDF_Dictionary::SetRectFor(const ByteString& key,
                                 const CFX_FloatRect& rect) {
  auto pArray = SetNewFor<CPDF_Array>(key);
  pArray->AppendNew<CPDF_Number>(rect.left);
  pArray->AppendNew<CPDF_Number>(rect.bottom);
  pArray->AppendNew<CPDF_Number>(rect.right);
  pArray->AppendNew<CPDF_Number>(rect.top);
}

void CPDF_Dictionary::SetMatrixFor(const ByteString& key,
                                   const CFX_Matrix& matrix) {
  auto pArray = SetNewFor<CPDF_Array>(key);
  pArray->AppendNew<CPDF_Number>(matrix.a);
  pArray->AppendNew<CPDF_Number>(matrix.b);
  pArray->AppendNew<CPDF_Number>(matrix.c);
  pArray->AppendNew<CPDF_Number>(matrix.d);
  pArray->AppendNew<CPDF_Number>(matrix.e);
  pArray->AppendNew<CPDF_Number>(matrix.f);
}

ByteString CPDF_Dictionary::MaybeIntern(const ByteString& str) {
  return m_pPool ? m_pPool->Intern(str) : str;
}

bool CPDF_Dictionary::WriteTo(IFX_ArchiveStream* archive,
                              const CPDF_Encryptor* encryptor) const {
  if (!archive->WriteString("<<"))
    return false;

  const bool is_signature = CPDF_CryptoHandler::IsSignatureDictionary(this);

  CPDF_DictionaryLocker locker(this);
  for (const auto& it : locker) {
    const ByteString& key = it.first;
    const RetainPtr<CPDF_Object>& pValue = it.second;
    if (!archive->WriteString("/") ||
        !archive->WriteString(PDF_NameEncode(key).AsStringView())) {
      return false;
    }
    if (!pValue->WriteTo(archive, !is_signature || key != "Contents"
                                      ? encryptor
                                      : nullptr)) {
      return false;
    }
  }
  return archive->WriteString(">>");
}

CPDF_DictionaryLocker::CPDF_DictionaryLocker(const CPDF_Dictionary* pDictionary)
    : m_pDictionary(pDictionary) {
  m_pDictionary->m_LockCount++;
}

CPDF_DictionaryLocker::CPDF_DictionaryLocker(
    RetainPtr<CPDF_Dictionary> pDictionary)
    : m_pDictionary(std::move(pDictionary)) {
  m_pDictionary->m_LockCount++;
}

CPDF_DictionaryLocker::CPDF_DictionaryLocker(
    RetainPtr<const CPDF_Dictionary> pDictionary)
    : m_pDictionary(std::move(pDictionary)) {
  m_pDictionary->m_LockCount++;
}

CPDF_DictionaryLocker::~CPDF_DictionaryLocker() {
  m_pDictionary->m_LockCount--;
}
