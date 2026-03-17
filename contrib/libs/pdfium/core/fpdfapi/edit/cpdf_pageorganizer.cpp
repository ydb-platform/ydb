// Copyright 2024 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/edit/cpdf_pageorganizer.h"

#include <utility>
#include <vector>

#include "constants/page_object.h"
#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fpdfapi/parser/cpdf_name.h"
#include "core/fpdfapi/parser/cpdf_number.h"
#include "core/fpdfapi/parser/cpdf_object.h"
#include "core/fpdfapi/parser/cpdf_reference.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fpdfapi/parser/cpdf_string.h"
#include "core/fxcrt/bytestring.h"
#include "core/fxcrt/check.h"

CPDF_PageOrganizer::CPDF_PageOrganizer(CPDF_Document* dest_doc,
                                       CPDF_Document* src_doc)
    : dest_doc_(dest_doc), src_doc_(src_doc) {}

CPDF_PageOrganizer::~CPDF_PageOrganizer() = default;

bool CPDF_PageOrganizer::Init() {
  DCHECK(dest_doc_);
  DCHECK(src_doc_);

  return InitDestDoc();
}

bool CPDF_PageOrganizer::InitDestDoc() {
  RetainPtr<CPDF_Dictionary> root = dest()->GetMutableRoot();
  if (!root) {
    return false;
  }

  RetainPtr<CPDF_Dictionary> info = dest()->GetInfo();
  if (info) {
    info->SetNewFor<CPDF_String>("Producer", "PDFium");
  }

  if (root->GetByteStringFor("Type", ByteString()).IsEmpty()) {
    root->SetNewFor<CPDF_Name>("Type", "Catalog");
  }

  RetainPtr<CPDF_Dictionary> pages;
  if (RetainPtr<CPDF_Object> current_pages = root->GetMutableObjectFor("Pages");
      current_pages) {
    pages = ToDictionary(current_pages->GetMutableDirect());
  }
  if (!pages) {
    pages = dest()->NewIndirect<CPDF_Dictionary>();
    root->SetNewFor<CPDF_Reference>("Pages", dest(), pages->GetObjNum());
  }
  if (pages->GetByteStringFor("Type", ByteString()).IsEmpty()) {
    pages->SetNewFor<CPDF_Name>("Type", "Pages");
  }

  if (!pages->GetArrayFor("Kids")) {
    auto kids_array = dest()->NewIndirect<CPDF_Array>();
    pages->SetNewFor<CPDF_Number>("Count", 0);
    pages->SetNewFor<CPDF_Reference>("Kids", dest(), kids_array->GetObjNum());
  }
  return true;
}

bool CPDF_PageOrganizer::UpdateReference(RetainPtr<CPDF_Object> obj) {
  switch (obj->GetType()) {
    case CPDF_Object::kReference: {
      CPDF_Reference* reference = obj->AsMutableReference();
      uint32_t newobjnum = GetNewObjId(reference);
      if (newobjnum == 0) {
        return false;
      }
      reference->SetRef(dest(), newobjnum);
      return true;
    }
    case CPDF_Object::kDictionary: {
      CPDF_Dictionary* dict = obj->AsMutableDictionary();
      std::vector<ByteString> bad_keys;
      {
        CPDF_DictionaryLocker locker(dict);
        for (const auto& it : locker) {
          const ByteString& key = it.first;
          if (key == "Parent" || key == "Prev" || key == "First") {
            continue;
          }
          RetainPtr<CPDF_Object> next_obj = it.second;
          if (!UpdateReference(next_obj)) {
            bad_keys.push_back(key);
          }
        }
      }
      for (const auto& key : bad_keys) {
        dict->RemoveFor(key.AsStringView());
      }
      return true;
    }
    case CPDF_Object::kArray: {
      CPDF_Array* array = obj->AsMutableArray();
      for (size_t i = 0; i < array->size(); ++i) {
        if (!UpdateReference(array->GetMutableObjectAt(i))) {
          return false;
        }
      }
      return true;
    }
    case CPDF_Object::kStream: {
      return UpdateReference(obj->AsMutableStream()->GetMutableDict());
    }
    default:
      return true;
  }
}

uint32_t CPDF_PageOrganizer::GetNewObjId(CPDF_Reference* ref) {
  if (!ref) {
    return 0;
  }

  uint32_t obj_num = ref->GetRefObjNum();
  uint32_t new_obj_num = 0;
  const auto it = object_number_map_.find(obj_num);
  if (it != object_number_map_.end()) {
    new_obj_num = it->second;
  }
  if (new_obj_num) {
    return new_obj_num;
  }

  RetainPtr<const CPDF_Object> direct = ref->GetDirect();
  if (!direct) {
    return 0;
  }

  RetainPtr<CPDF_Object> clone = direct->Clone();
  const CPDF_Dictionary* dict_clone = clone->AsDictionary();
  if (dict_clone && dict_clone->KeyExist("Type")) {
    ByteString type = dict_clone->GetByteStringFor("Type");
    if (type.EqualNoCase("Pages")) {
      return 4;
    }
    if (type.EqualNoCase("Page")) {
      return 0;
    }
  }

  new_obj_num = dest()->AddIndirectObject(clone);
  AddObjectMapping(obj_num, new_obj_num);
  if (!UpdateReference(std::move(clone))) {
    return 0;
  }

  return new_obj_num;
}

// static
bool CPDF_PageOrganizer::CopyInheritable(
    RetainPtr<CPDF_Dictionary> dest_page_dict,
    RetainPtr<const CPDF_Dictionary> src_page_dict,
    const ByteString& key) {
  if (dest_page_dict->KeyExist(key)) {
    return true;
  }

  RetainPtr<const CPDF_Object> inheritable =
      PageDictGetInheritableTag(std::move(src_page_dict), key);
  if (!inheritable) {
    return false;
  }

  dest_page_dict->SetFor(key, inheritable->Clone());
  return true;
}

// static
RetainPtr<const CPDF_Object> CPDF_PageOrganizer::PageDictGetInheritableTag(
    RetainPtr<const CPDF_Dictionary> dict,
    const ByteString& src_tag) {
  if (!dict || src_tag.IsEmpty()) {
    return nullptr;
  }
  if (!dict->KeyExist(pdfium::page_object::kParent) ||
      !dict->KeyExist(pdfium::page_object::kType)) {
    return nullptr;
  }

  RetainPtr<const CPDF_Name> name =
      ToName(dict->GetObjectFor(pdfium::page_object::kType)->GetDirect());
  if (!name || name->GetString() != "Page") {
    return nullptr;
  }

  RetainPtr<const CPDF_Dictionary> pp = ToDictionary(
      dict->GetObjectFor(pdfium::page_object::kParent)->GetDirect());
  if (!pp) {
    return nullptr;
  }

  if (dict->KeyExist(src_tag)) {
    return dict->GetObjectFor(src_tag);
  }

  while (pp) {
    if (pp->KeyExist(src_tag)) {
      return pp->GetObjectFor(src_tag);
    }
    if (!pp->KeyExist(pdfium::page_object::kParent)) {
      break;
    }

    pp = ToDictionary(
        pp->GetObjectFor(pdfium::page_object::kParent)->GetDirect());
  }
  return nullptr;
}
