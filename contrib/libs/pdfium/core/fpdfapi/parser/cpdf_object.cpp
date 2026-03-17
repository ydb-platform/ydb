// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/parser/cpdf_object.h"

#include <algorithm>

#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_indirect_object_holder.h"
#include "core/fpdfapi/parser/cpdf_parser.h"
#include "core/fpdfapi/parser/cpdf_reference.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/fx_string.h"
#include "core/fxcrt/notreached.h"

CPDF_Object::~CPDF_Object() = default;

static_assert(sizeof(uint64_t) >= sizeof(CPDF_Object*),
              "Need a bigger type for cache keys");

static_assert(CPDF_Parser::kMaxObjectNumber < static_cast<uint32_t>(1) << 31,
              "Need a smaller kMaxObjNumber for cache keys");

uint64_t CPDF_Object::KeyForCache() const {
  if (IsInline())
    return (static_cast<uint64_t>(1) << 63) | reinterpret_cast<uint64_t>(this);

  return (static_cast<uint64_t>(m_ObjNum) << 32) |
         static_cast<uint64_t>(m_GenNum);
}

RetainPtr<CPDF_Object> CPDF_Object::GetMutableDirect() {
  return pdfium::WrapRetain(const_cast<CPDF_Object*>(GetDirectInternal()));
}

RetainPtr<const CPDF_Object> CPDF_Object::GetDirect() const {
  return pdfium::WrapRetain(GetDirectInternal());
}

const CPDF_Object* CPDF_Object::GetDirectInternal() const {
  return this;
}

RetainPtr<CPDF_Object> CPDF_Object::CloneObjectNonCyclic(bool bDirect) const {
  std::set<const CPDF_Object*> visited_objs;
  return CloneNonCyclic(bDirect, &visited_objs);
}

RetainPtr<CPDF_Object> CPDF_Object::CloneDirectObject() const {
  return CloneObjectNonCyclic(true);
}

RetainPtr<CPDF_Object> CPDF_Object::CloneNonCyclic(
    bool bDirect,
    std::set<const CPDF_Object*>* pVisited) const {
  return Clone();
}

ByteString CPDF_Object::GetString() const {
  return ByteString();
}

WideString CPDF_Object::GetUnicodeText() const {
  return WideString();
}

float CPDF_Object::GetNumber() const {
  return 0;
}

int CPDF_Object::GetInteger() const {
  return 0;
}

RetainPtr<const CPDF_Dictionary> CPDF_Object::GetDict() const {
  return pdfium::WrapRetain(GetDictInternal());
}

RetainPtr<CPDF_Dictionary> CPDF_Object::GetMutableDict() {
  return pdfium::WrapRetain(const_cast<CPDF_Dictionary*>(GetDictInternal()));
}

const CPDF_Dictionary* CPDF_Object::GetDictInternal() const {
  return nullptr;
}

void CPDF_Object::SetString(const ByteString& str) {
  NOTREACHED_NORETURN();
}

CPDF_Array* CPDF_Object::AsMutableArray() {
  return nullptr;
}

const CPDF_Array* CPDF_Object::AsArray() const {
  return const_cast<CPDF_Object*>(this)->AsMutableArray();
}

CPDF_Boolean* CPDF_Object::AsMutableBoolean() {
  return nullptr;
}

const CPDF_Boolean* CPDF_Object::AsBoolean() const {
  return const_cast<CPDF_Object*>(this)->AsMutableBoolean();
}

CPDF_Dictionary* CPDF_Object::AsMutableDictionary() {
  return nullptr;
}

const CPDF_Dictionary* CPDF_Object::AsDictionary() const {
  return const_cast<CPDF_Object*>(this)->AsMutableDictionary();
}

CPDF_Name* CPDF_Object::AsMutableName() {
  return nullptr;
}

const CPDF_Name* CPDF_Object::AsName() const {
  return const_cast<CPDF_Object*>(this)->AsMutableName();
}

CPDF_Null* CPDF_Object::AsMutableNull() {
  return nullptr;
}

const CPDF_Null* CPDF_Object::AsNull() const {
  return const_cast<CPDF_Object*>(this)->AsMutableNull();
}

CPDF_Number* CPDF_Object::AsMutableNumber() {
  return nullptr;
}

const CPDF_Number* CPDF_Object::AsNumber() const {
  return const_cast<CPDF_Object*>(this)->AsMutableNumber();
}

CPDF_Reference* CPDF_Object::AsMutableReference() {
  return nullptr;
}

const CPDF_Reference* CPDF_Object::AsReference() const {
  return const_cast<CPDF_Object*>(this)->AsMutableReference();
}

CPDF_Stream* CPDF_Object::AsMutableStream() {
  return nullptr;
}

const CPDF_Stream* CPDF_Object::AsStream() const {
  return const_cast<CPDF_Object*>(this)->AsMutableStream();
}

CPDF_String* CPDF_Object::AsMutableString() {
  return nullptr;
}

const CPDF_String* CPDF_Object::AsString() const {
  return const_cast<CPDF_Object*>(this)->AsMutableString();
}

RetainPtr<CPDF_Reference> CPDF_Object::MakeReference(
    CPDF_IndirectObjectHolder* holder) const {
  CHECK(!IsInline());
  return pdfium::MakeRetain<CPDF_Reference>(holder, GetObjNum());
}
